// In consensus/src/lib.rs

// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, error, info, log_enabled, trace, warn};
use primary::{Certificate, Epoch, ReconfigureNotification, Round}; // Import ReconfigureNotification
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use store::Store;
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration}; // <-- THÊM sleep, Duration

// Key used to store the consensus state in the persistent store.
pub const STATE_KEY: &'static [u8] = b"consensus_state";

// Errors that can occur within the consensus module.
#[derive(Error, Debug)]
pub enum ConsensusError {
    #[error("Failed to serialize state: {0}")]
    SerializationError(#[from] bincode::Error),

    #[error("Store operation failed: {0}")]
    StoreError(String), // More specific store errors could be propagated if needed.

    #[error("Missing round {0} in DAG")]
    MissingRound(Round),

    #[error("Leader not found for round {0}")]
    LeaderNotFound(Round),

    #[error("Channel send failed: {0}")]
    ChannelError(String), // Error sending messages to other components.

    #[error("State validation failed: {0}")]
    ValidationError(String), // Error found during state integrity checks.
}

// Represents the Directed Acyclic Graph (DAG) of certificates.
// Key: Round number
// Value: Map from Authority's PublicKey to their (Certificate Digest, Certificate) tuple.
pub type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

// Metrics tracked by the consensus engine.
#[derive(Debug, Clone, Default)]
pub struct ConsensusMetrics {
    // Total number of certificates processed since startup/reset.
    pub total_certificates_processed: u64,
    // Total number of certificates included in committed sub-DAGs.
    pub total_certificates_committed: u64,
    // The highest round number for which a leader has been committed globally.
    pub last_committed_round: Round,
    // Count of rounds where the designated leader's certificate was missing when checked.
    pub failed_leader_elections: u64,
}

// Represents the persistent state of the consensus engine.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConsensusState {
    // The highest round number globally committed across all authorities.
    pub last_committed_round: Round,
    // Tracks the highest committed round for each individual authority. Used for GC.
    pub last_committed: HashMap<PublicKey, Round>,
    // The DAG structure containing certificates received so far.
    pub dag: Dag,
    // The epoch number this state belongs to. Crucial for validation and reset logic.
    pub epoch: Epoch,
}

// Represents a committed sub-DAG, typically outputted for execution or analysis.
#[derive(Clone, Debug)]
pub struct CommittedSubDag {
    // The leader certificate that triggered the commit of this sub-DAG.
    pub leader: Certificate,
    // All certificates included in the causal history of the leader (up to the previous commit), ordered by round.
    pub certificates: Vec<Certificate>,
}

impl ConsensusState {
    // Creates a new initial state for a given epoch using genesis certificates.
    pub fn new(genesis: Vec<Certificate>, epoch: Epoch) -> Self {
        // Build the map for Round 0 using the provided genesis certificates.
        // It's assumed genesis() function in primary::messages correctly calculates digests.
        let genesis_map = genesis
            .into_iter()
            .map(|cert| {
                // Sanity check: ensure genesis certs have the correct epoch.
                if cert.epoch() != epoch {
                    warn!(
                        "Genesis certificate for {} has wrong epoch {} (expected {})",
                        cert.origin(),
                        cert.epoch(),
                        epoch
                    );
                    // Depending on severity, could panic or try to recover.
                    // Assuming genesis() generates correct certs.
                }
                (cert.origin(), (cert.digest(), cert))
            })
            .collect::<HashMap<_, _>>();

        // Initialize the last_committed map for all authorities present in genesis to round 0.
        let last_committed_map = genesis_map.keys().map(|pk| (*pk, 0 as Round)).collect();

        info!(
            "Creating new ConsensusState for epoch {}, with {} genesis certificates.",
            epoch,
            genesis_map.len()
        );

        Self {
            last_committed_round: 0,            // Global commit starts at 0.
            last_committed: last_committed_map, // Per-authority commit starts at 0.
            // Initialize the DAG with only Round 0 containing the genesis certificates.
            dag: [(0, genesis_map)].iter().cloned().collect(),
            epoch, // Store the epoch number associated with this state.
        }
    }

    // Updates the state after a certificate is committed, performing garbage collection.
    pub fn update(&mut self, committed_certificate: &Certificate, gc_depth: Round) {
        let cert_round = committed_certificate.round();
        let cert_origin = committed_certificate.origin();
        let cert_epoch = committed_certificate.epoch();

        // Safety check: Only process updates if the certificate belongs to the current state's epoch.
        if cert_epoch != self.epoch {
            warn!(
                "[StateUpdate][E{}] Ignoring update from certificate C{}({}) with wrong epoch {}!",
                self.epoch, cert_round, cert_origin, cert_epoch
            );
            return;
        }

        // Update the last committed round for the specific authority that created the certificate.
        // `entry().and_modify().or_insert()` ensures the map contains the authority.
        self.last_committed
            .entry(cert_origin)
            .and_modify(|current_round| *current_round = max(*current_round, cert_round))
            .or_insert(cert_round); // Insert if the authority was somehow missing.
        trace!(
            "[StateUpdate][E{}] Updated last_committed for {}: {}",
            self.epoch,
            cert_origin,
            self.last_committed[&cert_origin]
        );

        // Determine the new highest globally committed round based on the maximum of per-authority commits.
        let new_overall_last_committed = *self.last_committed.values().max().unwrap_or(&0);

        // Perform Garbage Collection (GC) only if the global commit round has advanced.
        if new_overall_last_committed > self.last_committed_round {
            let old_overall_last_committed = self.last_committed_round;
            self.last_committed_round = new_overall_last_committed;
            debug!(
                "[StateUpdate][E{}] Global last_committed_round advanced from {} to {}",
                self.epoch, old_overall_last_committed, new_overall_last_committed
            );

            // Calculate the minimum round number to keep in the DAG.
            // Rounds strictly below this threshold will be pruned.
            let minimum_round_to_keep = self.last_committed_round.saturating_sub(gc_depth);
            debug!(
                   "[GC][E{}] Performing GC: Keeping rounds >= {}. (Current last_committed_round: {}, gc_depth: {})",
                    self.epoch, minimum_round_to_keep, self.last_committed_round, gc_depth
              );

            let mut removed_count = 0;
            let initial_rounds = self.dag.len();

            // Retain rounds in the DAG based on the calculated minimum threshold,
            // **but always explicitly keep Round 0 (Genesis)**.
            self.dag.retain(|r, certs_in_round| {
                // *** FIX: Explicitly keep Round 0 (Genesis) ***
                if r == &0 {
                    trace!("[GC][E{}] Keeping genesis round 0 explicitly.", self.epoch);
                    true // Always keep genesis round.
                } else if r < &minimum_round_to_keep {
                    // If the round is older than the minimum threshold, remove it.
                    removed_count += certs_in_round.len();
                    trace!("[GC][E{}] Removing entire round {}", self.epoch, r);
                    false // Remove this round entry from the DAG.
                } else {
                    // Keep rounds that are recent enough (>= minimum_round_to_keep).
                    true
                }
            });

            // Log GC results if any certificates or rounds were removed.
            let final_rounds = self.dag.len();
            if removed_count > 0 || initial_rounds != final_rounds {
                debug!(
                    "[GC][E{}] finished: Removed {} certificates. {} rounds remaining in DAG.",
                    self.epoch, removed_count, final_rounds
                );
            }
        } else {
            // Log if GC was skipped because the global commit round didn't advance.
            trace!(
                "[StateUpdate][E{}] Global last_committed_round ({}) did not advance. Skipping GC.",
                self.epoch,
                self.last_committed_round
            );
        }
    }

    // Validates the basic integrity of the consensus state.
    pub fn validate(&self) -> Result<(), ConsensusError> {
        // Critical validation: Ensure Round 0 (Genesis) exists in the DAG.
        // Its absence indicates a corrupted or improperly initialized state.
        if !self.dag.contains_key(&0) {
            return Err(ConsensusError::ValidationError(format!(
                "State for epoch {} is invalid: Genesis round (round 0) is missing.",
                self.epoch
            )));
        }
        // Additional validation logic can be added here, e.g.:
        // - Check if all certificates within the state belong to `self.epoch`.
        // - Perform basic connectivity checks on the DAG if necessary.
        Ok(())
    }
}

// Provides a default, empty consensus state (used before loading from store).
impl Default for ConsensusState {
    fn default() -> Self {
        Self {
            last_committed_round: 0,
            last_committed: HashMap::new(),
            dag: HashMap::new(),
            epoch: 0, // Default to epoch 0, will be overwritten on load or reset.
        }
    }
}

// Helper functions for consensus logic.
mod utils {
    use super::*;

    // Orders a sequence of causally linked leaders backwards from a given leader.
    pub fn order_leaders<'a, LeaderElector>(
        leader: &Certificate, // The starting leader (usually triggers the commit check).
        state: &'a ConsensusState, // Current consensus state.
        get_leader: LeaderElector, // Function to find the leader for a round.
    ) -> Vec<Certificate>
    where
        LeaderElector: Fn(Round, &'a Dag) -> Option<&'a (Digest, Certificate)>,
    {
        // Initialize the sequence with the starting leader.
        let mut leader_sequence = vec![leader.clone()];
        // Track the latest leader added to the sequence for backward linking.
        let mut current_leader_in_sequence = leader;

        // Sanity check: Leaders should ideally be on even rounds (except genesis).
        if leader.round() > 0 && leader.round() % 2 != 0 {
            warn!(
                "[order_leaders][E{}] Initiated with an ODD-numbered leader: round {}",
                state.epoch,
                leader.round()
            );
        } else {
            debug!(
                "[order_leaders][E{}] Initiated with leader at round {}",
                state.epoch,
                leader.round()
            );
        }

        // Iterate backwards from the round *before* the current leader, down to just above the last globally committed round.
        // Only consider even rounds > 0.
        for r in (state.last_committed_round + 1..current_leader_in_sequence.round())
            .rev() // Iterate backwards.
            .filter(|round| round % 2 == 0)
        // Only consider even rounds.
        {
            debug!(
                "[order_leaders][E{}] Checking for linked leader at even round {}",
                state.epoch, r
            );

            // Try to find the designated leader for round 'r'.
            if let Some((_prev_leader_digest, prev_leader_cert)) = get_leader(r, &state.dag) {
                // Check if the current leader in the sequence has a causal path back to this previous leader.
                if linked(current_leader_in_sequence, prev_leader_cert, &state.dag) {
                    // If linked, add the previous leader to the sequence and update the tracker.
                    debug!(
                        "[order_leaders][E{}] Found linked leader at round {} (linked back from round {})",
                        state.epoch, prev_leader_cert.round(), current_leader_in_sequence.round()
                    );
                    leader_sequence.push(prev_leader_cert.clone());
                    current_leader_in_sequence = prev_leader_cert;
                } else {
                    // If not linked, the causal chain is broken. Stop searching further back.
                    debug!(
                        "[order_leaders][E{}] Leader found at round {} but not linked back from round {}. Stopping search.",
                        state.epoch, r, current_leader_in_sequence.round()
                    );
                    break;
                }
            } else {
                // If no leader certificate exists for round 'r', the sequence is broken. Stop searching.
                debug!(
                    "[order_leaders][E{}] No designated leader certificate found in DAG for round {}. Stopping search.",
                    state.epoch, r
                );
                break;
            }
        }

        debug!(
            "[order_leaders][E{}] Found sequence of {} leaders: rounds {:?}",
            state.epoch,
            leader_sequence.len(),
            // Log the rounds of the leaders found in the sequence.
            leader_sequence
                .iter()
                .map(|c| c.round())
                .collect::<Vec<_>>()
        );
        // Return the sequence (note: newest leader is first, commit logic might reverse).
        leader_sequence
    }

    // Checks if there is a causal path from certificate `cert_new` back to `cert_old` in the DAG.
    fn linked(cert_new: &Certificate, cert_old: &Certificate, dag: &Dag) -> bool {
        // Base case: Cannot be linked if 'new' is older or same round as 'old'.
        if cert_new.round() <= cert_old.round() {
            return false;
        }

        // Use Breadth-First Search (BFS) starting from cert_new.
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(cert_new.clone());
        let mut visited = HashSet::new(); // Keep track of visited certificate digests.
        visited.insert(cert_new.digest());

        while let Some(current_cert) = queue.pop_front() {
            // Success condition: We reached the target certificate.
            if current_cert.digest() == cert_old.digest() {
                debug!(
                    "linked check: Path found from round {} ({}) to {} ({})",
                    cert_new.round(),
                    cert_new.digest(),
                    cert_old.round(),
                    cert_old.digest()
                );
                return true;
            }

            // Optimization: If current cert is already at or below the target round,
            // no path can exist through this branch.
            if current_cert.round() <= cert_old.round() {
                continue;
            }

            // Get the round number of the parents.
            let parent_round = current_cert.round() - 1;

            // Look up certificates in the parent round within the DAG.
            if let Some(certs_in_parent_round) = dag.get(&parent_round) {
                // Iterate through the parent digests listed in the current certificate's header.
                for parent_digest in &current_cert.header.parents {
                    // Explore only unvisited parents.
                    if !visited.contains(parent_digest) {
                        // Find the actual parent certificate in the DAG using its digest.
                        if let Some((_, parent_cert)) = certs_in_parent_round
                            .values()
                            .find(|(d, _)| d == parent_digest)
                        {
                            // Mark as visited and add to the queue for exploration.
                            visited.insert(parent_digest.clone());
                            queue.push_back(parent_cert.clone());
                        } else {
                            // Log if a listed parent is unexpectedly missing from the DAG structure.
                            trace!(
                                "linked check: Parent {} of cert {} (round {}) missing in DAG at round {}",
                                parent_digest, current_cert.digest(), current_cert.round(), parent_round
                            );
                        }
                    }
                }
            } else {
                // Log if the entire parent round is missing from the DAG structure.
                trace!(
                    "linked check: Parent round {} missing in DAG while traversing from cert {}",
                    parent_round,
                    current_cert.digest()
                );
            }
        }

        // If the queue becomes empty and we haven't found cert_old, no path exists.
        debug!(
            "linked check: Path NOT found from round {} ({}) to {} ({})",
            cert_new.round(),
            cert_new.digest(),
            cert_old.round(),
            cert_old.digest()
        );
        false
    }

    // Orders the certificates in the sub-DAG representing the causal history of a leader.
    pub fn order_dag(
        gc_depth: Round,        // Garbage collection depth parameter.
        leader: &Certificate,   // The leader certificate defining the sub-DAG.
        state: &ConsensusState, // The current consensus state containing the DAG.
    ) -> Vec<Certificate> {
        debug!(
            "[order_dag][E{}] Ordering sub-dag starting from leader L{}({}) digest {}",
            state.epoch,
            leader.round(),
            leader.origin(),
            leader.digest()
        );

        // Use Depth-First Search (DFS) starting from the leader to traverse causal history.
        let mut ordered = Vec::new(); // Stores the final ordered list of certificates.
        let mut buffer = vec![leader.clone()]; // Acts as the DFS stack, initialized with the leader.
        let mut processed_digests = HashSet::new(); // Tracks digests added to buffer or ordered list to avoid cycles/duplicates.
        processed_digests.insert(leader.digest());

        // While the stack is not empty...
        while let Some(cert) = buffer.pop() {
            trace!(
                "[order_dag][E{}] Processing cert C{}({}) digest {}",
                state.epoch,
                cert.round(),
                cert.origin(),
                cert.digest()
            );
            // Add the currently processed certificate to the final ordered list.
            ordered.push(cert.clone());

            // Base case: Genesis certificates (Round 0) have no parents.
            if cert.round() == 0 {
                continue;
            }

            // Determine the round number of the parents.
            let parent_round = cert.round() - 1;

            // Check if the parent round exists in the DAG.
            if let Some(certs_in_parent_round) = state.dag.get(&parent_round) {
                // Iterate through the parent digests listed in the certificate's header.
                for parent_digest in &cert.header.parents {
                    // Only process parents that haven't been visited/added to the stack yet.
                    if !processed_digests.contains(parent_digest) {
                        // Find the actual parent certificate in the DAG using its digest.
                        if let Some((_, parent_cert)) = certs_in_parent_round
                            .values()
                            .find(|(d, _)| d == parent_digest)
                        {
                            let parent_origin = parent_cert.origin();
                            let parent_cert_round = parent_cert.round();
                            // Crucial check: Only explore parents that have NOT been committed yet *for their specific origin*.
                            // We use `state.last_committed` which tracks the highest committed round per authority.
                            let is_parent_committed = state
                                .last_committed
                                .get(&parent_origin)
                                .map_or(false, |last_committed_for_origin| {
                                    // If the parent's round is <= the last committed round for that authority, skip it.
                                    parent_cert_round <= *last_committed_for_origin
                                });

                            // If the parent certificate is not yet committed for its origin...
                            if !is_parent_committed {
                                // Mark it as processed (will be added to stack) and push it onto the DFS stack.
                                trace!(
                                    "[order_dag][E{}] Adding parent C{}({}) digest {} to buffer",
                                    state.epoch,
                                    parent_cert.round(),
                                    parent_origin,
                                    parent_digest
                                );
                                processed_digests.insert(parent_digest.clone());
                                buffer.push(parent_cert.clone());
                            } else {
                                // Log skipping due to being already committed for that authority.
                                trace!(
                                    "[order_dag][E{}] Skipping parent {} of {}: Already committed for origin {} at or before round {}",
                                    state.epoch, parent_digest, cert.digest(), parent_origin, parent_cert_round
                                );
                            }
                        } else {
                            // Log if a listed parent certificate is missing from the DAG structure.
                            debug!(
                                "[order_dag][E{}] Parent certificate {} for child {} (round {}) not found in DAG at round {}",
                                state.epoch, parent_digest, cert.digest(), cert.round(), parent_round
                            );
                        }
                    } else {
                        // Log skipping because the parent is already in the stack or has been ordered.
                        trace!(
                            "[order_dag][E{}] Skipping parent {} of {}: Already in buffer/processed",
                            state.epoch, parent_digest, cert.digest()
                        );
                    }
                }
            } else {
                // Log if the entire parent round is missing (unless it's round 0).
                if parent_round > 0 {
                    debug!(
                        "[order_dag][E{}] Parent round {} missing in DAG while processing cert {}",
                        state.epoch,
                        parent_round,
                        cert.digest()
                    );
                }
            }
        } // End DFS while loop.

        // After traversal, filter out certificates that fall below the garbage collection threshold
        // based on the *global* last committed round.
        let before_gc = ordered.len();
        // Calculate the minimum round to keep based on global commit and GC depth.
        let min_round_to_keep = state.last_committed_round.saturating_sub(gc_depth);
        ordered.retain(|cert| cert.round() >= min_round_to_keep); // Keep certs at or above the threshold.
        let after_gc = ordered.len();

        // Log if any certificates were removed by GC filtering.
        if before_gc != after_gc {
            debug!(
                "[order_dag][E{}] Filtered out {} GC'd certificates (rounds < {}). {} remaining.",
                state.epoch,
                before_gc - after_gc,
                min_round_to_keep,
                after_gc
            );
        }

        // Sort the remaining certificates by round number. This provides a deterministic causal order for execution.
        ordered.sort_by_key(|cert| cert.round());

        debug!(
            "[order_dag][E{}] Ordered {} certificates in sub-dag for leader L{}({})",
            state.epoch,
            ordered.len(),
            leader.round(),
            leader.origin()
        );
        ordered // Return the final ordered list of certificates.
    }
}

// Trait defining the interface for different consensus algorithms (Tusk, Bullshark, etc.).
pub trait ConsensusAlgorithm: Send + Sync {
    // Updates the committee information within the consensus protocol instance.
    fn update_committee(&mut self, new_committee: Committee);

    // Processes a received certificate, updates the internal state, and determines if any sub-DAGs should be committed.
    fn process_certificate(
        &self,
        state: &mut ConsensusState, // The mutable consensus state (DAG, commit info).
        certificate: Certificate,   // The certificate to process.
        metrics: &mut ConsensusMetrics, // Mutable metrics to update.
    ) -> Result<(Vec<CommittedSubDag>, bool), ConsensusError>; // Returns committed sub-DAGs and a flag indicating if a commit occurred.

    // Returns the name of the consensus algorithm (e.g., "Tusk", "Bullshark").
    fn name(&self) -> &'static str;
}

// --- Tusk implementation ---
// Tusk consensus protocol logic.
pub struct Tusk {
    // Committee information for the current epoch.
    pub committee: Committee,
    // Garbage collection depth (rounds).
    pub gc_depth: Round,
}

impl Tusk {
    // Creates a new Tusk instance.
    pub fn new(committee: Committee, gc_depth: Round) -> Self {
        info!(
            "[Tusk] Initializing Tusk consensus for epoch {} with gc_depth={}",
            committee.epoch, gc_depth
        );
        Self {
            committee,
            gc_depth,
        }
    }

    // Determines the leader for a given round based on round-robin selection.
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // Use round number for pseudo-random leader selection in production.
        // For testing, predictable selection might be used (e.g., cfg(test) coin = 0).
        #[cfg(test)]
        let coin = 0; // Predictable leader for tests
        #[cfg(not(test))]
        let coin = round; // Round-based selection for production

        // Get a sorted list of authority public keys. Sorting ensures deterministic leader selection across nodes.
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        // Select the leader based on the round number modulo the committee size.
        let leader_pk = keys[coin as usize % self.committee.size()];
        trace!(
            "[Tusk][E{}] Designated leader for round {}: {:?}",
            self.committee.epoch,
            round,
            leader_pk
        );

        // Find the certificate corresponding to the selected leader in the given round's data within the DAG.
        dag.get(&round) // Get the map of certificates for the specified round.
            .and_then(|round_certs| round_certs.get(&leader_pk)) // Get the specific leader's certificate from that map.
    }
}

impl ConsensusAlgorithm for Tusk {
    // Updates the internal committee for Tusk.
    fn update_committee(&mut self, new_committee: Committee) {
        info!(
            "[Tusk][E->{}] Committee updated. Old epoch: {}, Size: {}, New epoch: {}, Size: {}",
            new_committee.epoch,
            self.committee.epoch,
            self.committee.size(),
            new_committee.epoch,
            new_committee.size()
        );
        self.committee = new_committee;
    }

    // Processes a certificate according to the Tusk commit rule.
    fn process_certificate(
        &self,
        state: &mut ConsensusState, // State is mutable
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<CommittedSubDag>, bool), ConsensusError> {
        // Basic validation and state update
        let round = certificate.round();
        let epoch = certificate.epoch();

        // Ensure the certificate belongs to the current epoch being processed by this state.
        if epoch != state.epoch {
            warn!(
                "[Tusk][E{}] Discarding certificate C{}({}) with wrong epoch {}!",
                state.epoch,
                round,
                certificate.origin(),
                epoch
            );
            return Ok((Vec::new(), false)); // Ignore certificates from wrong epochs.
        }

        metrics.total_certificates_processed += 1;
        debug!(
            "[Tusk][E{}] Processing certificate C{}({}) digest {}",
            epoch,
            round,
            certificate.origin(),
            certificate.digest()
        );

        // Add the certificate to the DAG structure.
        state.dag.entry(round).or_insert_with(HashMap::new).insert(
            certificate.origin(),
            (certificate.digest(), certificate.clone()),
        );

        // Tusk commit rule parameters:
        let vote_delay = 1; // Check support at leader_round + 1
        let lookback_depth = 2; // Leader is at potential_support_round - 2 (R-2)

        // Determine the round where we look for support (relative to the leader).
        let potential_support_round = round; // The round of the certificate just processed (R).
                                             // Determine the round where the leader we *might* commit resides.
        let leader_round_to_check = potential_support_round.saturating_sub(lookback_depth); // Round R-2.

        debug!(
            "[Tusk][E{}] Cert round {}. Checking potential commit for leader round {}",
            epoch, round, leader_round_to_check
        );

        // --- Basic Commit Eligibility Checks ---

        // Don't check for commits based on genesis (round 0) or odd-numbered leader rounds.
        if leader_round_to_check == 0 || leader_round_to_check % 2 != 0 {
            debug!(
                "[Tusk][E{}] Skipping commit check: Leader round {} is genesis or odd.",
                epoch, leader_round_to_check
            );
            return Ok((Vec::new(), false)); // No commit possible.
        }

        // Don't re-commit leaders from rounds already globally committed.
        if leader_round_to_check <= state.last_committed_round {
            debug!(
                "[Tusk][E{}] Skipping commit check: Leader round {} already committed or earlier (last_committed={})",
                epoch, leader_round_to_check, state.last_committed_round
            );
            return Ok((Vec::new(), false)); // No commit needed.
        }

        // --- Leader Identification ---

        // Try to find the designated leader certificate for the calculated leader round (R-2).
        let (leader_digest, leader_cert) = match self.leader(leader_round_to_check, &state.dag) {
            Some(leader_entry) => {
                debug!(
                    "[Tusk][E{}] Found potential leader {} at round {}",
                    epoch, leader_entry.0, leader_round_to_check
                );
                leader_entry.clone() // Clone the digest and certificate reference.
            }
            None => {
                // If no leader certificate exists in the DAG for this round, we can't commit it.
                metrics.failed_leader_elections += 1;
                debug!(
                    "[Tusk][E{}] No leader certificate found in DAG for round {}",
                    epoch, leader_round_to_check
                );
                return Ok((Vec::new(), false)); // No commit possible.
            }
        };

        // --- Support Calculation (Quorum Check) ---

        // Determine the round where supporting certificates should exist (leader_round + vote_delay = R-1).
        let support_round = leader_round_to_check + vote_delay; // Round R-1.
                                                                // Calculate the total stake of authorities in the support round (R-1) whose certificates
                                                                // include the leader's (from R-2) digest in their parents list.
        let supporting_stake: Stake = match state.dag.get(&support_round) {
            Some(certs_in_support_round) => certs_in_support_round
                .values() // Iterate over (digest, certificate) tuples.
                // Filter certificates that have the leader's digest as a parent.
                .filter(|(_, cert)| cert.header.parents.contains(&leader_digest))
                // Map each supporting certificate's origin (PublicKey) to its stake in the committee.
                .map(|(_, cert)| self.committee.stake(&cert.origin()))
                .sum(), // Sum the stakes.
            None => {
                // If the support round doesn't exist in the DAG yet, support is 0.
                debug!(
                    "[Tusk][E{}] Support round {} missing from DAG while checking leader {}",
                    epoch, support_round, leader_round_to_check
                );
                0 // No stake if the round is missing.
            }
        };

        // Get the quorum threshold (typically 2f+1) from the committee.
        let required_stake = self.committee.quorum_threshold();

        // If the calculated support stake is less than the required quorum, we cannot commit this leader yet.
        if supporting_stake < required_stake {
            debug!(
                "[Tusk][E{}] Leader {} at round {} has insufficient support stake ({}/{}) from round {}",
                epoch, leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
            );
            return Ok((Vec::new(), false)); // No commit possible yet.
        }

        // --- Commit Sequence ---

        info!(
            "[Tusk][E{}] Leader {} at round {} has ENOUGH support stake ({}/{}) from round {}. Initiating commit sequence.",
            epoch, leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
        );

        let mut committed_sub_dags_result = Vec::new(); // Store the results of this commit sequence.

        // Find the sequence of causally linked leaders ending at the current leader (R-2).
        // This traces back through previous even rounds.
        let leaders_in_sequence =
            utils::order_leaders(&leader_cert, state, |r, d| self.leader(r, d));
        debug!(
            "[Tusk][E{}] Found sequence of {} leaders ending at round {}: {:?}",
            epoch,
            leaders_in_sequence.len(),
            leader_round_to_check,
            // Log the rounds of the leaders found in the sequence.
            leaders_in_sequence
                .iter()
                .map(|l| l.round())
                .collect::<Vec<_>>()
        );

        // Iterate through the identified leader sequence in reverse order (oldest potential commit to newest).
        for leader_to_commit in leaders_in_sequence.iter().rev() {
            // Skip leaders whose rounds are already globally committed.
            if leader_to_commit.round() <= state.last_committed_round {
                trace!(
                    "[Tusk][E{}] Skipping leader round {}: already globally committed (last_committed={}).",
                    epoch, leader_to_commit.round(), state.last_committed_round
                );
                continue;
            }

            info!(
                "[Tusk][E{}] Processing commit for leader round {}",
                epoch,
                leader_to_commit.round()
            );

            // Order the sub-DAG (causal history) for the current leader being committed.
            // This gathers all certificates causally linked to the leader up to the previous commit state.
            let sub_dag_certificates = utils::order_dag(self.gc_depth, leader_to_commit, state);

            // If the ordered sub-DAG is not empty (i.e., not entirely GC'd)...
            if !sub_dag_certificates.is_empty() {
                debug!(
                    "[Tusk][E{}] Ordered {} certificates for sub-dag of leader round {}",
                    epoch,
                    sub_dag_certificates.len(),
                    leader_to_commit.round()
                );

                // Update the commit state (last_committed per authority and global last_committed_round)
                // for each certificate in the ordered sub-DAG. This also triggers GC within the state.
                for cert in &sub_dag_certificates {
                    state.update(cert, self.gc_depth);
                }

                // Add the committed sub-DAG information (leader + ordered certificates) to the results.
                committed_sub_dags_result.push(CommittedSubDag {
                    leader: leader_to_commit.clone(),
                    certificates: sub_dag_certificates, // The ordered list.
                });
            } else {
                // This might happen if the sub-DAG was entirely garbage collected before ordering,
                // or if the DAG is significantly incomplete.
                warn!(
                    "[Tusk][E{}] order_dag returned empty list for leader round {}. Was it fully GC'd or is DAG incomplete?",
                    epoch, leader_to_commit.round()
                );
            }
        } // End loop through leaders_in_sequence.

        // Update metrics if any commits occurred during this call.
        let commit_occurred = !committed_sub_dags_result.is_empty();
        if commit_occurred {
            // Calculate total certificates committed in this batch.
            let total_certs_committed_this_call: u64 = committed_sub_dags_result
                .iter()
                .map(|d| d.certificates.len() as u64)
                .sum();
            metrics.total_certificates_committed += total_certs_committed_this_call;
            // Update the metric with the final global commit round after processing the sequence.
            metrics.last_committed_round = state.last_committed_round;
            info!(
                "[Tusk][E{}] Commit sequence finished. {} certificates committed in total this call. New global last_committed_round: {}",
                epoch, total_certs_committed_this_call, state.last_committed_round
            );
        } else {
            debug!(
                "[Tusk][E{}] No new leaders were committed in this call.",
                epoch
            );
        }

        // Return the list of committed sub-DAGs and a flag indicating if any commit happened.
        Ok((committed_sub_dags_result, commit_occurred))
    }

    // Returns the name of the protocol.
    fn name(&self) -> &'static str {
        "Tusk"
    }
}

// --- Bullshark implementation ---
// Bullshark consensus protocol logic.
pub struct Bullshark {
    // Committee information for the current epoch.
    pub committee: Committee,
    // Garbage collection depth (rounds).
    pub gc_depth: Round,
}

impl Bullshark {
    // Creates a new Bullshark instance.
    pub fn new(committee: Committee, gc_depth: Round) -> Self {
        info!(
            "[Bullshark] Initializing Bullshark consensus for epoch {} with gc_depth={}",
            committee.epoch, gc_depth
        );
        Self {
            committee,
            gc_depth,
        }
    }

    // Determines the leader for a given round based on round-robin selection.
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // Get a sorted list of authority public keys for deterministic selection.
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        // Select the leader based on the round number modulo committee size.
        let leader_pk = &keys[round as usize % self.committee.size()];
        trace!(
            "[Bullshark][E{}] Designated leader for round {}: {:?}",
            self.committee.epoch,
            round,
            leader_pk
        );
        // Find the leader's certificate in the DAG for the specified round.
        dag.get(&round).and_then(|x| x.get(leader_pk))
    }
}

impl ConsensusAlgorithm for Bullshark {
    // Updates the internal committee for Bullshark.
    fn update_committee(&mut self, new_committee: Committee) {
        info!(
            "[Bullshark][E->{}] Committee updated. Old epoch: {}, Size: {}, New epoch: {}, Size: {}",
            new_committee.epoch, self.committee.epoch, self.committee.size(), new_committee.epoch, new_committee.size()
        );
        self.committee = new_committee;
    }

    // Processes a certificate according to the Bullshark commit rule.
    fn process_certificate(
        &self,
        state: &mut ConsensusState, // State is mutable
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<CommittedSubDag>, bool), ConsensusError> {
        // Basic validation and state update
        let round = certificate.round();
        let epoch = certificate.epoch();

        // Ensure the certificate belongs to the current epoch being processed by this state.
        if epoch != state.epoch {
            warn!(
                "[Bullshark][E{}] Discarding certificate C{}({}) with wrong epoch {}!",
                state.epoch,
                round,
                certificate.origin(),
                epoch
            );
            return Ok((Vec::new(), false)); // Ignore certificates from wrong epochs.
        }

        metrics.total_certificates_processed += 1;

        debug!(
            "[Bullshark][E{}] Processing certificate C{}({}) digest {}",
            epoch,
            round,
            certificate.origin(),
            certificate.digest()
        );

        // Add the certificate to the DAG structure.
        state.dag.entry(round).or_insert_with(HashMap::new).insert(
            certificate.origin(),
            (certificate.digest(), certificate.clone()),
        );

        // Bullshark commit rule: Check leader of round R-1 based on support in round R.
        let leader_round_to_check = round.saturating_sub(1); // Round R-1.

        debug!(
            "[Bullshark][E{}] Cert round {}. Checking potential commit for leader round {}",
            epoch, round, leader_round_to_check
        );

        // --- Basic Commit Eligibility Checks ---

        // Don't check for commits based on genesis (round 0) or odd-numbered leader rounds.
        if leader_round_to_check == 0 || leader_round_to_check % 2 != 0 {
            debug!(
                "[Bullshark][E{}] Skipping commit check: Leader round {} is genesis or odd.",
                epoch, leader_round_to_check
            );
            return Ok((Vec::new(), false)); // No commit possible.
        }
        // Don't re-commit leaders from rounds already globally committed.
        if leader_round_to_check <= state.last_committed_round {
            debug!(
                "[Bullshark][E{}] Skipping commit check: Leader round {} already committed or earlier (last_committed={})",
                epoch, leader_round_to_check, state.last_committed_round
            );
            return Ok((Vec::new(), false)); // No commit needed.
        }

        // --- Leader Identification ---

        // Try to find the designated leader certificate for the calculated leader round (R-1).
        let (leader_digest, leader_cert) = match self.leader(leader_round_to_check, &state.dag) {
            Some(leader_entry) => {
                debug!(
                    "[Bullshark][E{}] Found potential leader {} at round {}",
                    epoch, leader_entry.0, leader_round_to_check
                );
                leader_entry.clone() // Clone the digest and certificate reference.
            }
            None => {
                // If no leader certificate exists in the DAG for this round, we can't commit it.
                metrics.failed_leader_elections += 1;
                debug!(
                    "[Bullshark][E{}] No leader certificate found in DAG for round {}",
                    epoch, leader_round_to_check
                );
                // Return false indicating no commit occurred, BUT ALSO signal that this round might be skipped
                return Ok((Vec::new(), false));
            }
        };

        // --- Support Calculation (Validity Check) ---

        // The support round is the current certificate's round (R).
        let support_round = round;
        // Calculate the total stake of authorities in the support round (R) whose certificates
        // include the leader's (from R-1) digest in their parents list.
        let supporting_stake: Stake = match state.dag.get(&support_round) {
            Some(certs_in_support_round) => certs_in_support_round
                .values() // Iterate over (digest, certificate) tuples.
                // Filter certificates that have the leader's digest as a parent.
                .filter(|(_, cert)| cert.header.parents.contains(&leader_digest))
                // Map each supporting certificate's origin to its stake.
                .map(|(_, cert)| self.committee.stake(&cert.origin()))
                .sum(), // Sum the stakes.
            None => {
                // This case should ideally not happen if we just added the certificate of round R.
                error!(
                    "[Bullshark][E{}] CRITICAL: Support round (current round) {} missing after adding cert!",
                    epoch, support_round
                );
                0 // Return 0 stake if the round is unexpectedly missing.
            }
        };

        // Get the validity threshold (typically f+1) from the committee.
        let required_stake = self.committee.validity_threshold();

        // If the calculated support stake is less than the required validity threshold,
        // we cannot commit this leader yet.
        if supporting_stake < required_stake {
            debug!(
                "[Bullshark][E{}] Leader {} at round {} has insufficient support stake ({}/{}) from round {}",
                 epoch, leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
            );
            // Return false indicating no commit occurred, BUT ALSO signal that this round might be skipped
            return Ok((Vec::new(), false));
        }

        // --- Commit Sequence ---

        info!(
            "[Bullshark][E{}] Leader {} at round {} has ENOUGH support stake ({}/{}) from round {}. Initiating commit sequence.",
             epoch, leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
        );

        let mut committed_sub_dags_result = Vec::new(); // Store the results (usually just one sub-DAG for Bullshark).

        // Find the sequence of causally linked leaders ending at the current leader (R-1).
        let leaders_in_sequence =
            utils::order_leaders(&leader_cert, state, |r, d| self.leader(r, d));
        debug!(
            "[Bullshark][E{}] Found sequence of {} leaders ending at round {}: {:?}",
            epoch,
            leaders_in_sequence.len(),
            leader_round_to_check,
            // Log the rounds of the leaders found.
            leaders_in_sequence
                .iter()
                .map(|l| l.round())
                .collect::<Vec<_>>()
        );

        // Bullshark commits only the *newest* uncommitted leader from the sequence.
        // Find the first (newest, since the list is newest-first) leader in the sequence
        // whose round is greater than the last globally committed round.
        if let Some(leader_to_commit) = leaders_in_sequence
            .iter() // Iterate from newest leader backwards.
            .find(|l| l.round() > state.last_committed_round)
        {
            info!(
                "[Bullshark][E{}] Attempting to commit newest uncommitted leader: round {}",
                epoch,
                leader_to_commit.round()
            );

            // Sanity check: leaders should always be on even rounds. Log critically if not.
            if leader_to_commit.round() % 2 != 0 {
                warn!("[BUG_TRACE][E{}] CRITICAL: An ODD-numbered leader (round {}) chosen as newest uncommitted is about to be processed!", epoch, leader_to_commit.round());
            }

            // Order the sub-DAG (causal history) for this chosen leader.
            let sub_dag_certificates = utils::order_dag(self.gc_depth, leader_to_commit, state);

            // If the ordered sub-DAG is not empty (i.e., not entirely GC'd)...
            if !sub_dag_certificates.is_empty() {
                debug!(
                    "[Bullshark][E{}] Ordered {} certificates for sub-dag of leader {}",
                    epoch,
                    sub_dag_certificates.len(),
                    leader_to_commit.round()
                );

                // Update the commit state for each certificate in the ordered sub-DAG.
                // This updates last_committed per authority and the global last_committed_round.
                for cert in &sub_dag_certificates {
                    state.update(cert, self.gc_depth);
                }

                // Add the committed sub-DAG information to the results.
                committed_sub_dags_result.push(CommittedSubDag {
                    leader: leader_to_commit.clone(),
                    certificates: sub_dag_certificates,
                });
                info!(
                    "[Bullshark][E{}] Successfully committed leader {} at round {}. New last_committed_round: {}",
                    epoch, leader_to_commit.digest(), leader_to_commit.round(), state.last_committed_round
                );
            } else {
                // Log if ordering returned an empty list, potentially due to GC or incomplete DAG.
                warn!(
                    "[Bullshark][E{}] Skipping commit for leader {} at round {}: order_dag returned empty list (GC'd or incomplete DAG?)",
                    epoch, leader_to_commit.digest(), leader_to_commit.round()
                );
            }
        } else {
            // Log if all leaders in the found sequence were apparently already committed.
            debug!(
                "[Bullshark][E{}] Found leader sequence for round {}, but all leaders in it seem already committed (last_committed={}).",
                epoch, leader_round_to_check, state.last_committed_round
            );
        }

        // Update metrics if a commit occurred.
        let commit_occurred = !committed_sub_dags_result.is_empty();
        if commit_occurred {
            let total_certs_committed_this_call: u64 = committed_sub_dags_result
                .iter()
                .map(|d| d.certificates.len() as u64)
                .sum();
            metrics.total_certificates_committed += total_certs_committed_this_call;
            // Update the metric with the final global commit round.
            metrics.last_committed_round = state.last_committed_round;
        }

        // Return the committed sub-DAGs (likely just one for Bullshark) and the commit flag.
        Ok((committed_sub_dags_result, commit_occurred))
    }

    // Returns the name of the protocol.
    fn name(&self) -> &'static str {
        "Bullshark"
    }
}

// --- ConsensusProtocol enum ---
// Enum to wrap different consensus algorithm implementations.
// Allows switching protocols possibly at runtime or compile time.
pub enum ConsensusProtocol {
    Tusk(Tusk),
    Bullshark(Bullshark),
    // Potentially add other protocols here like Mysticeti, etc.
}

// Implement the ConsensusAlgorithm trait for the enum, dispatching calls to the inner instance.
impl ConsensusAlgorithm for ConsensusProtocol {
    fn update_committee(&mut self, new_committee: Committee) {
        match self {
            ConsensusProtocol::Tusk(t) => t.update_committee(new_committee),
            ConsensusProtocol::Bullshark(b) => b.update_committee(new_committee),
        }
    }

    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<CommittedSubDag>, bool), ConsensusError> {
        match self {
            ConsensusProtocol::Tusk(tusk) => tusk.process_certificate(state, certificate, metrics),
            ConsensusProtocol::Bullshark(bullshark) => {
                bullshark.process_certificate(state, certificate, metrics)
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            ConsensusProtocol::Tusk(t) => t.name(),
            ConsensusProtocol::Bullshark(b) => b.name(),
        }
    }
}

// --- Consensus struct ---
// The main struct managing the consensus process.
pub struct Consensus {
    // Persistent storage for the consensus state.
    store: Store,
    // Channel to receive new certificates from the primary core.
    rx_primary: Receiver<Certificate>,
    // Broadcast channel to send committed sub-DAGs to listeners (e.g., executor, analyzer).
    // Tuple contains: (Committed SubDAGs, Committee for that epoch, Optional<Skipped Round>)
    tx_output: broadcast::Sender<(Vec<CommittedSubDag>, Committee, Option<Round>)>,
    // Shared, mutable reference to the current committee information.
    committee: Arc<RwLock<Committee>>,
    // The specific consensus algorithm implementation being used (e.g., Tusk, Bullshark).
    protocol: Box<dyn ConsensusAlgorithm>,
    // Metrics for monitoring consensus performance.
    metrics: Arc<RwLock<ConsensusMetrics>>,
    // Internal tracker for the current epoch the consensus engine is operating on.
    // Used for state validation and handling reconfiguration race conditions.
    current_protocol_epoch: Epoch,
    // Channel to receive reconfiguration notifications triggered by the primary core.
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    // Garbage collection depth, stored locally for use in state updates.
    gc_depth: Round,
}

impl Consensus {
    // Static method potentially used elsewhere to quickly check the last committed round from storage.
    pub async fn load_last_committed_round(store: &mut Store) -> Round {
        match store.read(STATE_KEY.to_vec()).await {
            Ok(Some(bytes)) => match bincode::deserialize::<ConsensusState>(&bytes) {
                Ok(state) => {
                    log::info!(
                        "Consensus::load_last_committed_round: Loaded last committed round {} (epoch {}) from store.",
                        state.last_committed_round, state.epoch
                    );
                    state.last_committed_round
                }
                Err(e) => {
                    log::error!(
                        "Consensus::load_last_committed_round: Failed to deserialize state: {}. Defaulting to 0.",
                        e
                    );
                    0 // Default to 0 on deserialization error.
                }
            },
            Ok(None) => {
                log::info!(
                    "Consensus::load_last_committed_round: No state found. Defaulting to 0."
                );
                0 // Default to 0 if no state exists.
            }
            Err(e) => {
                log::error!(
                    "Consensus::load_last_committed_round: Failed to read state: {:?}. Defaulting to 0.",
                    e
                );
                0 // Default to 0 on store read error.
            }
        }
    }

    // Spawns the main consensus task.
    pub fn spawn(
        committee_arc: Arc<RwLock<Committee>>, // Shared committee.
        parameters: config::Parameters,        // Node parameters.
        store: Store,                          // Persistent store.
        rx_primary: Receiver<Certificate>,     // Input channel for certificates.
        _tx_primary: Sender<Certificate>,      // Output channel (usually unused).
        tx_output: broadcast::Sender<(Vec<CommittedSubDag>, Committee, Option<Round>)>, // Output channel for commits.
        rx_reconfigure: broadcast::Receiver<ReconfigureNotification>, // Input channel for reconfig signals.
    ) {
        let gc_depth = parameters.gc_depth; // Extract GC depth.
                                            // Clone Arcs needed for the async task.
        let committee_clone_for_spawn = committee_arc.clone();
        let metrics = Arc::new(RwLock::new(ConsensusMetrics::default()));

        // Spawn the asynchronous task that runs the consensus engine.
        tokio::spawn(async move {
            // Read the initial committee state within the spawned task.
            let initial_committee = committee_clone_for_spawn.read().await.clone();
            let initial_epoch = initial_committee.epoch;

            // Initialize the chosen consensus protocol (currently hardcoded to Bullshark).
            // Pass the initial committee and GC depth.
            let protocol: Box<dyn ConsensusAlgorithm> =
                Box::new(Bullshark::new(initial_committee.clone(), gc_depth));

            info!(
                "Spawning Consensus task with {} protocol, gc_depth={}, initial epoch {}",
                protocol.name(),
                gc_depth,
                initial_epoch
            );

            // Create the Consensus struct instance.
            Self {
                store,
                rx_primary,
                tx_output,
                committee: committee_arc, // Use the original Arc passed to spawn.
                protocol,
                metrics,
                current_protocol_epoch: initial_epoch, // Initialize internal epoch tracker.
                rx_reconfigure,
                gc_depth, // Store gc_depth locally.
            }
            .run() // Start the main execution loop.
            .await;
        });
    }

    // Loads the consensus state from storage, resetting to genesis if loading fails,
    // the state is invalid, or belongs to a previous epoch.
    async fn load_state(&mut self) -> ConsensusState {
        match self.store.read(STATE_KEY.to_vec()).await {
            Ok(Some(bytes)) => {
                // Attempt to deserialize the loaded state.
                match bincode::deserialize::<ConsensusState>(&bytes) {
                    Ok(mut state) => {
                        info!(
                            "Successfully loaded consensus state from store. Epoch: {}, Last committed round: {}",
                            state.epoch, state.last_committed_round
                        );
                        // Get the expected current epoch from the shared committee Arc.
                        let current_committee = self.committee.read().await;
                        let expected_epoch = current_committee.epoch;
                        // Get genesis certs for the current epoch, needed for potential reset.
                        let current_genesis = Certificate::genesis(&*current_committee);
                        drop(current_committee); // Release read lock.

                        // Validate the loaded state.
                        let validation_result = state.validate();

                        // Reset logic: Reset if validation fails OR if the loaded state's epoch is older than expected.
                        if validation_result.is_err() || state.epoch < expected_epoch {
                            if let Err(e) = validation_result {
                                error!("Loaded consensus state failed validation: {}. Re-initializing from genesis for epoch {}.", e, expected_epoch);
                            } else {
                                info!("Loaded state from old epoch {} is outdated (expected {}). Resetting state.", state.epoch, expected_epoch);
                            }
                            // Reset state using current epoch's genesis.
                            state = ConsensusState::new(current_genesis, expected_epoch);
                        } else if state.epoch > expected_epoch {
                            // This indicates a potential inconsistency between stored state and the current committee info.
                            error!("CRITICAL: Loaded consensus state epoch {} is NEWER than committee epoch {}. Resetting state, but this might indicate a problem.", state.epoch, expected_epoch);
                            state = ConsensusState::new(current_genesis, expected_epoch);
                        }
                        // If epochs match and validation passes, return the loaded state.
                        state
                    }
                    Err(e) => {
                        // Deserialization failed. Reset to genesis of the current committee epoch.
                        let current_committee = self.committee.read().await;
                        let current_epoch = current_committee.epoch;
                        let current_genesis = Certificate::genesis(&*current_committee);
                        drop(current_committee);
                        error!(
                            "Failed to deserialize consensus state from store: {}. Re-initializing from genesis for epoch {}.",
                            e, current_epoch
                        );
                        ConsensusState::new(current_genesis, current_epoch)
                    }
                }
            }
            Ok(None) | Err(_) => {
                // No state found in store or error reading from store. Reset to genesis.
                let (log_message, error_context) =
                    if let Err(e) = self.store.read(STATE_KEY.to_vec()).await {
                        (
                            format!("Failed to read consensus state from store: {:?}", e),
                            "read error",
                        )
                    } else {
                        (
                            "No consensus state found in store.".to_string(),
                            "no state found",
                        )
                    };

                // Get current epoch info for reset.
                let current_committee = self.committee.read().await;
                let current_epoch = current_committee.epoch;
                let current_genesis = Certificate::genesis(&*current_committee);
                drop(current_committee);
                info!(
                    "{}. Initializing from genesis for epoch {} ({})",
                    log_message, current_epoch, error_context
                );
                // Create new state using current epoch's genesis.
                ConsensusState::new(current_genesis, current_epoch)
            }
        }
    }

    // Saves the current consensus state to persistent storage.
    async fn save_state(&mut self, state: &ConsensusState) -> Result<(), ConsensusError> {
        // Optional: Validate state integrity before saving.
        state.validate()?;
        // Serialize the state using bincode.
        let serialized_state = bincode::serialize(state)?;
        // Write the serialized state to the store using the predefined key.
        self.store.write(STATE_KEY.to_vec(), serialized_state).await;
        debug!(
            "Successfully saved consensus state for epoch {}. Last committed round: {}",
            state.epoch, state.last_committed_round
        );
        Ok(())
    }

    // The main execution loop of the consensus engine task.
    pub async fn run(&mut self) {
        // Load the initial state from storage or reset to genesis.
        let mut state = self.load_state().await;
        // Ensure the internal epoch tracker matches the loaded/reset state's epoch.
        self.current_protocol_epoch = state.epoch;
        info!(
            "Consensus engine starting for epoch {}. Initial last committed round: {}",
            self.current_protocol_epoch, state.last_committed_round
        );

        // Main event loop.
        loop {
            tokio::select! {
                // Prioritize reconfiguration signals over processing certificates.
                biased;

                // Handle reconfiguration signals received from the broadcast channel.
                result = self.rx_reconfigure.recv() => {
                    match result {
                        Ok(notification) => {
                            // --- Anti-Race Condition Logic ---
                            // Check 1: Is this signal for the *end* of our current epoch?
                            // The notification carries the committee *of the epoch that just ended*.
                            if notification.committee.epoch != self.current_protocol_epoch {
                                warn!(
                                    "[Consensus] Ignoring stale reconfigure signal for epoch {} (current is {})",
                                    notification.committee.epoch, self.current_protocol_epoch
                                );
                                continue; // Ignore signal if it's for a different epoch.
                            }

                            // Check 2: Wait until the shared Committee Arc has actually been updated to the *next* epoch.
                            info!("[Consensus] Received reconfigure signal for end of epoch {}. Waiting for committee update...", self.current_protocol_epoch);
                            let mut updated_committee_epoch = self.current_protocol_epoch;
                            let mut new_committee_for_reset = None; // Temporarily store the new committee data.

                            // Loop until the epoch in the shared Arc is greater than our current epoch.
                            while updated_committee_epoch <= self.current_protocol_epoch {
                                sleep(Duration::from_millis(100)).await; // Wait briefly before checking again.
                                let committee_guard = self.committee.read().await; // Acquire read lock.
                                updated_committee_epoch = committee_guard.epoch;
                                // If the epoch has updated, clone the new committee data to use after releasing the lock.
                                if updated_committee_epoch > self.current_protocol_epoch {
                                    new_committee_for_reset = Some(committee_guard.clone());
                                }
                                drop(committee_guard); // Release read lock promptly.
                            }

                            // Perform the reset only if we successfully captured the new committee data.
                            if let Some(new_committee) = new_committee_for_reset {
                                info!(
                                    "[Consensus] Committee Arc updated to epoch {}. Resetting state and updating protocol.",
                                    updated_committee_epoch
                                );

                                // 1. Update internal epoch tracker.
                                self.current_protocol_epoch = updated_committee_epoch;
                                // 2. Update the committee within the consensus protocol instance (e.g., Bullshark).
                                self.protocol.update_committee(new_committee.clone());
                                // 3. Create a completely new genesis state for the new epoch.
                                let new_genesis = Certificate::genesis(&new_committee);
                                state = ConsensusState::new(new_genesis.clone(), updated_committee_epoch);
                                info!(
                                    "[Consensus] State fully reset for epoch {}. Starting from new genesis. Last committed round reset to 0.",
                                    updated_committee_epoch
                                );

                                // 4. Persist the newly created initial state for the new epoch. This is crucial for recovery.
                                info!("[Consensus] Attempting to save initial state for new epoch {}", updated_committee_epoch);
                                if let Err(e) = self.save_state(&state).await {
                                    error!("CRITICAL: Failed to save initial state for new epoch {}: {}", updated_committee_epoch, e);
                                    // Consider shutting down if saving the initial state fails, as recovery might be impossible.
                                    break; // Exit the main loop on critical save error.
                                }
                                info!("[Consensus] Successfully saved initial state for new epoch {}", updated_committee_epoch);

                            } else {
                                // This indicates an internal logic error if the loop exited without capturing the new committee.
                                error!("[Consensus] Logic error during reconfigure: Committee epoch updated but data not captured.");
                            }
                            // --- End Anti-Race Condition Logic ---
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            // If the receiver lagged, it missed reconfiguration signals. This is serious.
                            warn!("[Consensus] Reconfigure receiver lagged by {}. Missed epoch transitions!", n);
                            // Consider more drastic action like requesting a state sync or shutdown.
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            // If the broadcast channel closes, it likely means the main node loop terminated.
                            error!("[Consensus] Reconfigure channel closed unexpectedly. Shutting down.");
                            break; // Exit the loop.
                        }
                    }
                },

                // Handle incoming certificates from the primary core.
                Some(certificate) = self.rx_primary.recv() => {
                    // Get a snapshot of the current committee for outputting alongside commits.
                    // Read lock is held briefly.
                    let current_committee_for_output = self.committee.read().await.clone();

                    // Check if the certificate's epoch matches the consensus engine's current operating epoch.
                    if certificate.epoch() != self.current_protocol_epoch {
                        warn!(
                            "[Consensus] Received certificate from wrong epoch {} (current is {}). Discarding C{}({}).",
                            certificate.epoch(), self.current_protocol_epoch, certificate.round(), certificate.origin()
                        );
                        continue; // Skip processing this certificate.
                    }

                    let cert_round = certificate.round();
                    let cert_digest = certificate.digest();
                    trace!(
                        "[Consensus][E{}] Received certificate C{}({}) digest {}",
                        self.current_protocol_epoch, cert_round, certificate.origin(), cert_digest
                    );

                    // Acquire write lock to update metrics.
                    let mut metrics_guard = self.metrics.write().await;
                    // Determine the potential leader round Bullshark might try to commit based on this certificate (R-1).
                    let potential_commit_leader_round = cert_round.saturating_sub(1);

                    // Process the certificate using the configured consensus protocol (e.g., Bullshark).
                    // The protocol implementation modifies the `state` directly.
                    match self.protocol.process_certificate(&mut state, certificate, &mut *metrics_guard) {
                        Ok((committed_sub_dags, commit_occurred)) => {
                            // Release metrics lock.
                            drop(metrics_guard);

                            // Actions to take if the protocol reported that a commit occurred.
                            if commit_occurred {
                                debug!(
                                    "[Consensus][E{}] Commit occurred. New last committed round: {}. Saving state.",
                                    self.current_protocol_epoch, state.last_committed_round
                                );
                                // Persist the updated state to storage. Failure here is critical.
                                if let Err(e) = self.save_state(&state).await {
                                    error!("CRITICAL: Failed to save consensus state after commit for epoch {}: {}", self.current_protocol_epoch, e);
                                    break; // Exit loop on critical save error.
                                }

                                // If sub-DAGs were actually generated by the commit logic...
                                if !committed_sub_dags.is_empty() {
                                    // Log committed leaders if info level is enabled.
                                    if log_enabled!(log::Level::Info) {
                                        for dag in &committed_sub_dags {
                                            info!("[Consensus][E{}] Committed leader L{}({}) digest {}", self.current_protocol_epoch, dag.leader.round(), dag.leader.origin(), dag.leader.digest());
                                            // Optional benchmark logging
                                            #[cfg(feature = "benchmark")]
                                            for cert in &dag.certificates {
                                                for digest in cert.header.payload.keys() {
                                                    info!("Committed {} -> {:?}", cert.header, digest);
                                                }
                                            }
                                        }
                                    }

                                    // Broadcast the committed data (sub-DAGs, current committee, no skipped round).
                                    let output_tuple = (committed_sub_dags.clone(), current_committee_for_output.clone(), None);
                                    // Send to output channel, ignoring error if no listeners.
                                    if self.tx_output.send(output_tuple).is_err() {
                                        debug!("[Consensus][E{}] No receivers for committed dags (this is okay).", self.current_protocol_epoch);
                                    }
                                } else {
                                    // This indicates a potential logic error in the protocol implementation.
                                    warn!("[Consensus][E{}] Commit reported by protocol, but committed_sub_dags list is empty!", self.current_protocol_epoch);
                                }
                            } else {
                                // No commit occurred for this certificate.
                                trace!("[Consensus][E{}] No commit occurred for certificate round {}", self.current_protocol_epoch, cert_round);

                                // Specific logic for Bullshark: If a potential even leader round (R-1) was NOT committed,
                                // notify listeners about the skipped round. This helps the executor know the round is finalized without commits.
                                if potential_commit_leader_round > state.last_committed_round // Check if the leader round is newer than last commit.
                                    && potential_commit_leader_round % 2 == 0 // Check if it's an even round.
                                    && self.protocol.name() == "Bullshark" // Only applies to Bullshark's commit rule.
                                {
                                    debug!(
                                        "[Consensus][E{}] Potential leader round {} was not committed (last committed is {}). Sending skipped round notification.",
                                        self.current_protocol_epoch, potential_commit_leader_round, state.last_committed_round
                                    );
                                    // Send an empty Vec of dags, the current committee, and the skipped round number.
                                    let output_tuple = (Vec::new(), current_committee_for_output, Some(potential_commit_leader_round));
                                    // Send to output channel, ignoring error if no listeners.
                                    if self.tx_output.send(output_tuple).is_err() {
                                        debug!("[Consensus][E{}] No receivers for skipped round {} (this is okay).", self.current_protocol_epoch, potential_commit_leader_round);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // Release metrics lock in case of error during processing.
                            drop(metrics_guard);
                            // Log errors encountered during certificate processing by the protocol.
                            error!("[Consensus][E{}] Error processing certificate {}: {}", self.current_protocol_epoch, cert_digest, e);
                        }
                    }
                },

                // Handle the case where the primary core channel closes (e.g., primary shutting down).
                else => {
                    info!("[Consensus] Primary Core channel closed. Consensus engine shutting down.");
                    break; // Exit the loop.
                }
            }
        } // End main loop.

        info!(
            "[Consensus] Engine main loop for epoch {} finished.",
            self.current_protocol_epoch
        );
    } // End fn run.

    // Helper function (can be called externally) to get a clone of the current metrics.
    pub async fn get_metrics(metrics: &Arc<RwLock<ConsensusMetrics>>) -> ConsensusMetrics {
        metrics.read().await.clone()
    }
} // End impl Consensus.
