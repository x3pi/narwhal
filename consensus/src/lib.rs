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
use std::collections::{HashMap, HashSet}; // Đảm bảo HashSet được import
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

/// Trạng thái cần được lưu trữ để phục hồi sau sự cố.
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

    // [SỬA LỖI KIẾN TRÚC]
    // Dùng HashMap để lưu (Digest -> Round) thay vì HashSet.
    // Điều này cho phép chúng ta "đánh dấu" và dọn dẹp (GC) nó.
    pub committed_certificates: HashMap<Digest, Round>,
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
        let genesis_map = genesis
            .into_iter()
            .map(|cert| {
                if cert.epoch() != epoch {
                    warn!(
                        "Genesis certificate for {} has wrong epoch {} (expected {})",
                        cert.origin(),
                        cert.epoch(),
                        epoch
                    );
                }
                (cert.origin(), (cert.digest(), cert))
            })
            .collect::<HashMap<_, _>>();

        // Initialize the last_committed map for all authorities present in genesis to round 0.
        let last_committed_map = genesis_map.keys().map(|pk| (*pk, 0 as Round)).collect();

        // Khởi tạo bộ lọc với (Digest, Round) của genesis.
        let committed_certificates = genesis_map
            .values()
            .map(|(digest, cert)| (digest.clone(), cert.round()))
            .collect();

        info!(
            "Creating new ConsensusState for epoch {}, with {} genesis certificates.",
            epoch,
            genesis_map.len()
        );

        Self {
            last_committed_round: 0,
            last_committed: last_committed_map,
            dag: [(0, genesis_map)].iter().cloned().collect(),
            epoch,
            committed_certificates, // Khởi tạo trường mới
        }
    }

    // Updates the state after a certificate is committed, performing garbage collection.
    pub fn update(&mut self, committed_certificate: &Certificate, gc_depth: Round) {
        let cert_round = committed_certificate.round();
        let cert_origin = committed_certificate.origin();
        let cert_epoch = committed_certificate.epoch();

        if cert_epoch != self.epoch {
            warn!(
                "[StateUpdate][E{}] Ignoring update from certificate C{}({}) with wrong epoch {}!",
                self.epoch, cert_round, cert_origin, cert_epoch
            );
            return;
        }

        // Cập nhật last committed round cho tác giả
        self.last_committed
            .entry(cert_origin)
            .and_modify(|current_round| *current_round = max(*current_round, cert_round))
            .or_insert(cert_round);
        trace!(
            "[StateUpdate][E{}] Updated last_committed for {}: {}",
            self.epoch,
            cert_origin,
            self.last_committed[&cert_origin]
        );

        // Ghi lại digest và round của certificate đã commit.
        self.committed_certificates
            .insert(committed_certificate.digest(), cert_round);

        // Xác định round commit toàn cục mới
        let new_overall_last_committed = *self.last_committed.values().max().unwrap_or(&0);

        // Chỉ thực hiện GC nếu round commit toàn cục tăng lên.
        if new_overall_last_committed > self.last_committed_round {
            let old_overall_last_committed = self.last_committed_round;
            self.last_committed_round = new_overall_last_committed;
            debug!(
                "[StateUpdate][E{}] Global last_committed_round advanced from {} to {}",
                self.epoch, old_overall_last_committed, new_overall_last_committed
            );

            // Tính toán round để dọn dẹp (GC)
            let minimum_round_to_keep = self.last_committed_round.saturating_sub(gc_depth);
            debug!(
                   "[GC][E{}] Performing GC: Keeping rounds >= {}. (Current last_committed_round: {}, gc_depth: {})",
                    self.epoch, minimum_round_to_keep, self.last_committed_round, gc_depth
              );

            let mut removed_count = 0;
            let initial_rounds = self.dag.len();

            // 1. Dọn dẹp DAG
            self.dag.retain(|r, certs_in_round| {
                if r == &0 {
                    trace!("[GC][E{}] Keeping genesis round 0 explicitly.", self.epoch);
                    true
                } else if *r < minimum_round_to_keep {
                    // <-- SỬA LỖI: Dùng *r
                    removed_count += certs_in_round.len();
                    trace!("[GC][E{}] Removing entire round {}", self.epoch, r);
                    false
                } else {
                    true
                }
            });

            // 2. Dọn dẹp bộ lọc (HashMap)
            let set_before = self.committed_certificates.len();
            self.committed_certificates
                .retain(|_digest, round| *round >= minimum_round_to_keep); // <-- SỬA LỖI: Dùng *round
            let set_after = self.committed_certificates.len();

            // Log GC results
            let final_rounds = self.dag.len();
            if removed_count > 0 || initial_rounds != final_rounds || set_before != set_after {
                debug!(
                    "[GC][E{}] finished: Removed {} certificates from DAG ({} rounds). Removed {} digests from committed_certificates set.",
                    self.epoch, removed_count, initial_rounds - final_rounds, set_before - set_after
                );
            }
        } else {
            trace!(
                "[StateUpdate][E{}] Global last_committed_round ({}) did not advance. Skipping GC.",
                self.epoch,
                self.last_committed_round
            );
        }
    }

    // Validates the basic integrity of the consensus state.
    pub fn validate(&self) -> Result<(), ConsensusError> {
        if !self.dag.contains_key(&0) {
            return Err(ConsensusError::ValidationError(format!(
                "State for epoch {} is invalid: Genesis round (round 0) is missing.",
                self.epoch
            )));
        }
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
            epoch: 0,
            committed_certificates: HashMap::new(),
        }
    }
}

// Helper functions for consensus logic.
mod utils {
    use super::*;

    // Orders a sequence of causally linked leaders backwards from a given leader.
    pub fn order_leaders<'a, LeaderElector>(
        leader: &Certificate,
        state: &'a ConsensusState,
        get_leader: LeaderElector,
    ) -> Vec<Certificate>
    where
        LeaderElector: Fn(Round, &'a Dag) -> Option<&'a (Digest, Certificate)>,
    {
        let mut leader_sequence = vec![leader.clone()];
        let mut current_leader_in_sequence = leader;

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

        for r in (state.last_committed_round + 1..current_leader_in_sequence.round())
            .rev()
            .filter(|round| round % 2 == 0)
        {
            debug!(
                "[order_leaders][E{}] Checking for linked leader at even round {}",
                state.epoch, r
            );

            if let Some((_prev_leader_digest, prev_leader_cert)) = get_leader(r, &state.dag) {
                if linked(current_leader_in_sequence, prev_leader_cert, &state.dag) {
                    debug!(
                        "[order_leaders][E{}] Found linked leader at round {} (linked back from round {})",
                        state.epoch, prev_leader_cert.round(), current_leader_in_sequence.round()
                    );
                    leader_sequence.push(prev_leader_cert.clone());
                    current_leader_in_sequence = prev_leader_cert;
                } else {
                    debug!(
                        "[order_leaders][E{}] Leader found at round {} but not linked back from round {}. Stopping search.",
                        state.epoch, r, current_leader_in_sequence.round()
                    );
                    break;
                }
            } else {
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
            leader_sequence
                .iter()
                .map(|c| c.round())
                .collect::<Vec<_>>()
        );
        leader_sequence
    }

    // Checks if there is a causal path from certificate `cert_new` back to `cert_old` in the DAG.
    fn linked(cert_new: &Certificate, cert_old: &Certificate, dag: &Dag) -> bool {
        if cert_new.round() <= cert_old.round() {
            return false;
        }

        let mut queue = std::collections::VecDeque::new();
        queue.push_back(cert_new.clone());
        let mut visited = HashSet::new();
        visited.insert(cert_new.digest());

        while let Some(current_cert) = queue.pop_front() {
            if current_cert.digest() == cert_old.digest() {
                debug!(
                    "linked check: Path found from round {} ({}) to {} ({})",
                    cert_new.round(),
                    current_cert.digest(),
                    cert_old.round(),
                    cert_old.digest()
                );
                return true;
            }
            if current_cert.round() <= cert_old.round() {
                continue;
            }
            let parent_round = current_cert.round() - 1;

            if let Some(certs_in_parent_round) = dag.get(&parent_round) {
                for parent_digest in &current_cert.header.parents {
                    if !visited.contains(parent_digest) {
                        if let Some((_, parent_cert)) = certs_in_parent_round
                            .values()
                            .find(|(d, _)| d == parent_digest)
                        {
                            visited.insert(parent_digest.clone());
                            queue.push_back(parent_cert.clone());
                        } else {
                            trace!(
                                "linked check: Parent {} of cert {} (round {}) missing in DAG at round {}",
                                parent_digest, current_cert.digest(), current_cert.round(), parent_round
                            );
                        }
                    }
                }
            } else {
                if parent_round > 0 {
                    trace!(
                        "linked check: Parent round {} missing in DAG while traversing from cert {}",
                        parent_round,
                        current_cert.digest()
                    );
                }
            }
        }

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
        gc_depth: Round,
        leader: &Certificate,
        state: &ConsensusState,
    ) -> Vec<Certificate> {
        debug!(
            "[order_dag][E{}] Ordering sub-dag starting from leader L{}({}) digest {}",
            state.epoch,
            leader.round(),
            leader.origin(),
            leader.digest()
        );

        let mut ordered = Vec::new();
        let mut buffer = vec![leader.clone()];
        let mut processed_digests = HashSet::new();
        processed_digests.insert(leader.digest());

        while let Some(cert) = buffer.pop() {
            trace!(
                "[order_dag][E{}] Processing cert C{}({}) digest {}",
                state.epoch,
                cert.round(),
                cert.origin(),
                cert.digest()
            );

            // [SỬA LỖI] Bỏ qua certificate nếu nó đã được commit TRƯỚC ĐÓ.
            if state.committed_certificates.contains_key(&cert.digest()) {
                trace!(
                    "[order_dag][E{}] Skipping cert {}: Already in committed_certificates.",
                    state.epoch,
                    cert.digest()
                );
                continue;
            }

            ordered.push(cert.clone());

            if cert.round() == 0 {
                continue;
            }

            let parent_round = cert.round() - 1;

            if let Some(certs_in_parent_round) = state.dag.get(&parent_round) {
                for parent_digest in &cert.header.parents {
                    if !processed_digests.contains(parent_digest) {
                        if let Some((_, parent_cert)) = certs_in_parent_round
                            .values()
                            .find(|(d, _)| d == parent_digest)
                        {
                            // [SỬA LỖI] Bỏ qua nếu đã xử lý trong CÁC LẦN TRƯỚC (committed_certificates)
                            if state.committed_certificates.contains_key(parent_digest) {
                                trace!("[order_dag][E{}] Skipping parent {}: Already in committed_certificates.", state.epoch, parent_digest);
                                continue;
                            }

                            trace!(
                                "[order_dag][E{}] Adding parent C{}({}) digest {} to buffer",
                                state.epoch,
                                parent_cert.round(),
                                parent_cert.origin(),
                                parent_digest
                            );
                            processed_digests.insert(parent_digest.clone());
                            buffer.push(parent_cert.clone());
                        } else {
                            debug!(
                                "[order_dag][E{}] Parent certificate {} for child {} (round {}) not found in DAG at round {}",
                                state.epoch, parent_digest, cert.digest(), cert.round(), parent_round
                            );
                        }
                    } else {
                        trace!(
                            "[order_dag][E{}] Skipping parent {} of {}: Already in buffer/processed",
                            state.epoch, parent_digest, cert.digest()
                        );
                    }
                }
            } else {
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
        let before_gc = ordered.len();
        let min_round_to_keep = state.last_committed_round.saturating_sub(gc_depth);
        ordered.retain(|cert| cert.round() >= min_round_to_keep);
        let after_gc = ordered.len();

        if before_gc != after_gc {
            debug!(
                "[order_dag][E{}] Filtered out {} GC'd certificates (rounds < {}). {} remaining.",
                state.epoch,
                before_gc - after_gc,
                min_round_to_keep,
                after_gc
            );
        }

        // Sort the remaining certificates by round number.
        ordered.sort_by_key(|cert| cert.round());

        debug!(
            "[order_dag][E{}] Ordered {} certificates in sub-dag for leader L{}({})",
            state.epoch,
            ordered.len(),
            leader.round(),
            leader.origin()
        );
        ordered
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
        // (Giữ nguyên logic)
        #[cfg(test)]
        let coin = 0;
        #[cfg(not(test))]
        let coin = round;

        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader_pk = keys[coin as usize % self.committee.size()];
        trace!(
            "[Tusk][E{}] Designated leader for round {}: {:?}",
            self.committee.epoch,
            round,
            leader_pk
        );

        dag.get(&round)
            .and_then(|round_certs| round_certs.get(&leader_pk))
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
        let round = certificate.round();
        let epoch = certificate.epoch();

        if epoch != state.epoch {
            warn!(
                "[Tusk][E{}] Discarding certificate C{}({}) with wrong epoch {}!",
                state.epoch,
                round,
                certificate.origin(),
                epoch
            );
            return Ok((Vec::new(), false));
        }

        metrics.total_certificates_processed += 1;

        // [SỬA LỖI] Bỏ qua ngay nếu certificate này đã được commit
        if state
            .committed_certificates
            .contains_key(&certificate.digest())
        {
            debug!(
                "Certificate {} already committed, skipping processing.",
                certificate.digest()
            );
            return Ok((Vec::new(), false));
        }

        debug!(
            "[Tusk][E{}] Processing certificate C{}({}) digest {}",
            epoch,
            round,
            certificate.origin(),
            certificate.digest()
        );

        // Add to DAG (chỉ thêm nếu chưa commit)
        state.dag.entry(round).or_insert_with(HashMap::new).insert(
            certificate.origin(),
            (certificate.digest(), certificate.clone()),
        );

        // ... (Logic Tusk giữ nguyên) ...
        let vote_delay = 1;
        let lookback_depth = 2;
        let potential_support_round = round;
        let leader_round_to_check = potential_support_round.saturating_sub(lookback_depth);

        debug!(
            "[Tusk][E{}] Cert round {}. Checking potential commit for leader round {}",
            epoch, round, leader_round_to_check
        );

        if leader_round_to_check == 0 || leader_round_to_check % 2 != 0 {
            debug!(
                "[Tusk][E{}] Skipping commit check: Leader round {} is genesis or odd.",
                epoch, leader_round_to_check
            );
            return Ok((Vec::new(), false));
        }
        if leader_round_to_check <= state.last_committed_round {
            debug!(
                "[Tusk][E{}] Skipping commit check: Leader round {} already committed or earlier (last_committed={})",
                epoch, leader_round_to_check, state.last_committed_round
            );
            return Ok((Vec::new(), false));
        }

        let (leader_digest, leader_cert) = match self.leader(leader_round_to_check, &state.dag) {
            Some(leader_entry) => {
                debug!(
                    "[Tusk][E{}] Found potential leader {} at round {}",
                    epoch, leader_entry.0, leader_round_to_check
                );
                leader_entry.clone()
            }
            None => {
                metrics.failed_leader_elections += 1;
                debug!(
                    "[Tusk][E{}] No leader certificate found in DAG for round {}",
                    epoch, leader_round_to_check
                );
                return Ok((Vec::new(), false));
            }
        };

        let support_round = leader_round_to_check + vote_delay;
        let supporting_stake: Stake = match state.dag.get(&support_round) {
            Some(certs_in_support_round) => certs_in_support_round
                .values()
                .filter(|(_, cert)| cert.header.parents.contains(&leader_digest))
                .map(|(_, cert)| self.committee.stake(&cert.origin()))
                .sum(),
            None => {
                debug!(
                    "[Tusk][E{}] Support round {} missing from DAG while checking leader {}",
                    epoch, support_round, leader_round_to_check
                );
                0
            }
        };

        let required_stake = self.committee.quorum_threshold();

        if supporting_stake < required_stake {
            debug!(
                "[Tusk][E{}] Leader {} at round {} has insufficient support stake ({}/{}) from round {}",
                epoch, leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
            );
            return Ok((Vec::new(), false));
        }

        info!(
            "[Tusk][E{}] Leader {} at round {} has ENOUGH support stake ({}/{}) from round {}. Initiating commit sequence.",
            epoch, leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
        );

        let mut committed_sub_dags_result = Vec::new();
        let leaders_in_sequence =
            utils::order_leaders(&leader_cert, state, |r, d| self.leader(r, d));

        for leader_to_commit in leaders_in_sequence.iter().rev() {
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

            // [SỬA LỖI] `order_dag` bây giờ đã lọc chính xác
            let sub_dag_certificates = utils::order_dag(self.gc_depth, leader_to_commit, state);

            if !sub_dag_certificates.is_empty() {
                debug!(
                    "[Tusk][E{}] Ordered {} certificates for sub-dag of leader round {}",
                    epoch,
                    sub_dag_certificates.len(),
                    leader_to_commit.round()
                );

                // [SỬA LỖI] `state.update` sẽ thêm vào `committed_certificates`
                for cert in &sub_dag_certificates {
                    state.update(cert, self.gc_depth);
                }

                committed_sub_dags_result.push(CommittedSubDag {
                    leader: leader_to_commit.clone(),
                    certificates: sub_dag_certificates,
                });
            } else {
                warn!(
                    "[Tusk][E{}] order_dag returned empty list for leader round {}. Was it fully GC'd or is DAG incomplete?",
                    epoch, leader_to_commit.round()
                );
            }
        }

        let commit_occurred = !committed_sub_dags_result.is_empty();
        if commit_occurred {
            let total_certs_committed_this_call: u64 = committed_sub_dags_result
                .iter()
                .map(|d| d.certificates.len() as u64)
                .sum();
            metrics.total_certificates_committed += total_certs_committed_this_call;
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
        // (Giữ nguyên logic)
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader_pk = &keys[round as usize % self.committee.size()];
        trace!(
            "[Bullshark][E{}] Designated leader for round {}: {:?}",
            self.committee.epoch,
            round,
            leader_pk
        );
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
        let round = certificate.round();
        let epoch = certificate.epoch();

        if epoch != state.epoch {
            warn!(
                "[Bullshark][E{}] Discarding certificate C{}({}) with wrong epoch {}!",
                state.epoch,
                round,
                certificate.origin(),
                epoch
            );
            return Ok((Vec::new(), false));
        }

        metrics.total_certificates_processed += 1;

        // [SỬA LỖI] Bỏ qua ngay nếu certificate này đã được commit
        if state
            .committed_certificates
            .contains_key(&certificate.digest())
        {
            debug!(
                "Certificate {} already committed, skipping processing.",
                certificate.digest()
            );
            return Ok((Vec::new(), false));
        }

        debug!(
            "[Bullshark][E{}] Processing certificate C{}({}) digest {}",
            epoch,
            round,
            certificate.origin(),
            certificate.digest()
        );

        // Add to DAG (chỉ thêm nếu chưa commit)
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
        if leader_round_to_check == 0 || leader_round_to_check % 2 != 0 {
            debug!(
                "[Bullshark][E{}] Skipping commit check: Leader round {} is genesis or odd.",
                epoch, leader_round_to_check
            );
            return Ok((Vec::new(), false));
        }
        if leader_round_to_check <= state.last_committed_round {
            debug!(
                "[Bullshark][E{}] Skipping commit check: Leader round {} already committed or earlier (last_committed={})",
                epoch, leader_round_to_check, state.last_committed_round
            );
            return Ok((Vec::new(), false));
        }

        // --- Leader Identification ---
        let (leader_digest, leader_cert) = match self.leader(leader_round_to_check, &state.dag) {
            Some(leader_entry) => {
                debug!(
                    "[Bullshark][E{}] Found potential leader {} at round {}",
                    epoch, leader_entry.0, leader_round_to_check
                );
                leader_entry.clone()
            }
            None => {
                metrics.failed_leader_elections += 1;
                debug!(
                    "[Bullshark][E{}] No leader certificate found in DAG for round {}",
                    epoch, leader_round_to_check
                );
                return Ok((Vec::new(), false));
            }
        };

        // --- Support Calculation (Validity Check) ---
        let support_round = round;
        let supporting_stake: Stake = match state.dag.get(&support_round) {
            Some(certs_in_support_round) => certs_in_support_round
                .values()
                .filter(|(_, cert)| cert.header.parents.contains(&leader_digest))
                .map(|(_, cert)| self.committee.stake(&cert.origin()))
                .sum(),
            None => {
                error!(
                    "[Bullshark][E{}] CRITICAL: Support round (current round) {} missing after adding cert!",
                    epoch, support_round
                );
                0
            }
        };

        let required_stake = self.committee.validity_threshold();

        if supporting_stake < required_stake {
            debug!(
                "[Bullshark][E{}] Leader {} at round {} has insufficient support stake ({}/{}) from round {}",
                 epoch, leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
            );
            return Ok((Vec::new(), false));
        }

        // --- Commit Sequence ---
        info!(
            "[Bullshark][E{}] Leader {} at round {} has ENOUGH support stake ({}/{}) from round {}. Initiating commit sequence.",
             epoch, leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
        );

        let mut committed_sub_dags_result = Vec::new();
        let leaders_in_sequence =
            utils::order_leaders(&leader_cert, state, |r, d| self.leader(r, d));

        debug!(
            "[Bullshark][E{}] Found sequence of {} leaders ending at round {}: {:?}",
            epoch,
            leaders_in_sequence.len(),
            leader_round_to_check,
            leaders_in_sequence
                .iter()
                .map(|l| l.round())
                .collect::<Vec<_>>()
        );

        if let Some(leader_to_commit) = leaders_in_sequence
            .iter()
            .find(|l| l.round() > state.last_committed_round)
        {
            info!(
                "[Bullshark][E{}] Attempting to commit newest uncommitted leader: round {}",
                epoch,
                leader_to_commit.round()
            );

            if leader_to_commit.round() % 2 != 0 {
                warn!("[BUG_TRACE][E{}] CRITICAL: An ODD-numbered leader (round {}) chosen as newest uncommitted is about to be processed!", epoch, leader_to_commit.round());
            }

            // [SỬA LỖI] `order_dag` (đã sửa) sẽ lọc ra các certificate đã commit
            let sub_dag_certificates = utils::order_dag(self.gc_depth, leader_to_commit, state);

            if !sub_dag_certificates.is_empty() {
                debug!(
                    "[Bullshark][E{}] Ordered {} certificates for sub-dag of leader {}",
                    epoch,
                    sub_dag_certificates.len(),
                    leader_to_commit.round()
                );

                // [SỬA LỖI] `state.update` sẽ thêm vào `committed_certificates`
                for cert in &sub_dag_certificates {
                    state.update(cert, self.gc_depth);
                }

                committed_sub_dags_result.push(CommittedSubDag {
                    leader: leader_to_commit.clone(),
                    certificates: sub_dag_certificates,
                });
                info!(
                    "[Bullshark][E{}] Successfully committed leader {} at round {}. New last_committed_round: {}",
                    epoch, leader_to_commit.digest(), leader_to_commit.round(), state.last_committed_round
                );
            } else {
                warn!(
                    "[Bullshark][E{}] Skipping commit for leader {} at round {}: order_dag returned empty list (GC'd or incomplete DAG?)",
                    epoch, leader_to_commit.digest(), leader_to_commit.round()
                );
            }
        } else {
            debug!(
                "[Bullshark][E{}] Found leader sequence for round {}, but all leaders in it seem already committed (last_committed={}).",
                epoch, leader_round_to_check, state.last_committed_round
            );
        }

        let commit_occurred = !committed_sub_dags_result.is_empty();
        if commit_occurred {
            let total_certs_committed_this_call: u64 = committed_sub_dags_result
                .iter()
                .map(|d| d.certificates.len() as u64)
                .sum();
            metrics.total_certificates_committed += total_certs_committed_this_call;
            metrics.last_committed_round = state.last_committed_round;
        }

        Ok((committed_sub_dags_result, commit_occurred))
    }

    // Returns the name of the protocol.
    fn name(&self) -> &'static str {
        "Bullshark"
    }
}

// --- ConsensusProtocol enum ---
pub enum ConsensusProtocol {
    Tusk(Tusk),
    Bullshark(Bullshark),
}

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
pub struct Consensus {
    store: Store,
    rx_primary: Receiver<Certificate>,
    // Chỉ gửi khi commit thành công
    tx_output: broadcast::Sender<(Vec<CommittedSubDag>, Committee, Option<Round>)>, // Giữ Option<Round> để tương thích type, nhưng luôn là None
    committee: Arc<RwLock<Committee>>,
    protocol: Box<dyn ConsensusAlgorithm>,
    metrics: Arc<RwLock<ConsensusMetrics>>,
    current_protocol_epoch: Epoch,
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    // processed_consensus_rounds: HashSet<Round>, // <-- XÓA BỎ
}

impl Consensus {
    // Static method (không thay đổi)
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
                    0
                }
            },
            Ok(None) => {
                log::info!(
                    "Consensus::load_last_committed_round: No state found. Defaulting to 0."
                );
                0
            }
            Err(e) => {
                log::error!(
                    "Consensus::load_last_committed_round: Failed to read state: {:?}. Defaulting to 0.",
                    e
                );
                0
            }
        }
    }

    // Spawns the main consensus task.
    pub fn spawn(
        committee_arc: Arc<RwLock<Committee>>,
        parameters: config::Parameters,
        store: Store,
        rx_primary: Receiver<Certificate>,
        _tx_primary: Sender<Certificate>,
        tx_output: broadcast::Sender<(Vec<CommittedSubDag>, Committee, Option<Round>)>,
        rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    ) {
        let gc_depth = parameters.gc_depth;
        let committee_clone_for_spawn = committee_arc.clone();
        let metrics = Arc::new(RwLock::new(ConsensusMetrics::default()));

        tokio::spawn(async move {
            let initial_committee = committee_clone_for_spawn.read().await.clone();
            let initial_epoch = initial_committee.epoch;
            let protocol: Box<dyn ConsensusAlgorithm> =
                Box::new(Bullshark::new(initial_committee.clone(), gc_depth));

            info!(
                "Spawning Consensus task with {} protocol, gc_depth={}, initial epoch {}",
                protocol.name(),
                gc_depth,
                initial_epoch
            );

            Self {
                store,
                rx_primary,
                tx_output,
                committee: committee_arc,
                protocol,
                metrics,
                current_protocol_epoch: initial_epoch,
                rx_reconfigure,
                // processed_consensus_rounds: HashSet::new(), // <-- XÓA BỎ
            }
            .run()
            .await;
        });
    }

    // Loads the consensus state from storage (đã sửa ở trên)
    async fn load_state(&mut self) -> ConsensusState {
        // self.processed_consensus_rounds.clear(); // <-- XÓA BỎ
        match self.store.read(STATE_KEY.to_vec()).await {
            Ok(Some(bytes)) => match bincode::deserialize::<ConsensusState>(&bytes) {
                Ok(mut state) => {
                    info!(
                            "Successfully loaded consensus state from store. Epoch: {}, Last committed round: {}",
                            state.epoch, state.last_committed_round
                        );
                    let current_committee = self.committee.read().await;
                    let expected_epoch = current_committee.epoch;
                    let current_genesis = Certificate::genesis(&*current_committee);
                    drop(current_committee);

                    let validation_result = state.validate();

                    if validation_result.is_err() || state.epoch < expected_epoch {
                        if let Err(e) = validation_result {
                            error!("Loaded consensus state failed validation: {}. Re-initializing from genesis for epoch {}.", e, expected_epoch);
                        } else {
                            info!("Loaded state from old epoch {} is outdated (expected {}). Resetting state.", state.epoch, expected_epoch);
                        }
                        state = ConsensusState::new(current_genesis, expected_epoch);
                        // self.processed_consensus_rounds.clear(); // <-- XÓA BỎ
                    } else if state.epoch > expected_epoch {
                        error!("CRITICAL: Loaded consensus state epoch {} is NEWER than committee epoch {}. Resetting state, but this might indicate a problem.", state.epoch, expected_epoch);
                        state = ConsensusState::new(current_genesis, expected_epoch);
                        // self.processed_consensus_rounds.clear(); // <-- XÓA BỎ
                    }
                    state
                }
                Err(e) => {
                    let current_committee = self.committee.read().await;
                    let current_epoch = current_committee.epoch;
                    let current_genesis = Certificate::genesis(&*current_committee);
                    drop(current_committee);
                    error!(
                            "Failed to deserialize consensus state from store: {}. Re-initializing from genesis for epoch {}.",
                            e, current_epoch
                        );
                    // self.processed_consensus_rounds.clear(); // <-- XÓA BỎ
                    ConsensusState::new(current_genesis, current_epoch)
                }
            },
            Ok(None) | Err(_) => {
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

                let current_committee = self.committee.read().await;
                let current_epoch = current_committee.epoch;
                let current_genesis = Certificate::genesis(&*current_committee);
                drop(current_committee);
                info!(
                    "{}. Initializing from genesis for epoch {} ({})",
                    log_message, current_epoch, error_context
                );
                // self.processed_consensus_rounds.clear(); // <-- XÓA BỎ
                ConsensusState::new(current_genesis, current_epoch)
            }
        }
    }

    // Saves the current consensus state to persistent storage.
    async fn save_state(&mut self, state: &ConsensusState) -> Result<(), ConsensusError> {
        state.validate()?;
        let serialized_state = bincode::serialize(state)?;
        self.store.write(STATE_KEY.to_vec(), serialized_state).await;
        debug!(
            "Successfully saved consensus state for epoch {}. Last committed round: {}",
            state.epoch, state.last_committed_round
        );
        Ok(())
    }

    // The main execution loop of the consensus engine task.
    pub async fn run(&mut self) {
        let mut state = self.load_state().await;
        self.current_protocol_epoch = state.epoch;
        info!(
            "Consensus engine starting for epoch {}. Initial last committed round: {}",
            self.current_protocol_epoch, state.last_committed_round
        );

        // Main event loop.
        loop {
            tokio::select! {
                // Prioritize reconfiguration signals.
                biased;

                // Handle reconfiguration signals.
                result = self.rx_reconfigure.recv() => {
                    match result {
                        Ok(notification) => {
                            // --- Anti-Race Condition Logic ---
                            if notification.committee.epoch != self.current_protocol_epoch {
                                warn!(
                                    "[Consensus] Ignoring stale reconfigure signal for epoch {} (current is {})",
                                    notification.committee.epoch, self.current_protocol_epoch
                                );
                                continue;
                            }

                            info!("[Consensus] Received reconfigure signal for end of epoch {}. Waiting for committee update...", self.current_protocol_epoch);
                            let mut updated_committee_epoch = self.current_protocol_epoch;
                            let mut new_committee_for_reset = None;

                            while updated_committee_epoch <= self.current_protocol_epoch {
                                sleep(Duration::from_millis(100)).await;
                                let committee_guard = self.committee.read().await;
                                updated_committee_epoch = committee_guard.epoch;
                                if updated_committee_epoch > self.current_protocol_epoch {
                                    new_committee_for_reset = Some(committee_guard.clone());
                                }
                                drop(committee_guard);
                            }

                            if let Some(new_committee) = new_committee_for_reset {
                                info!(
                                    "[Consensus] Committee Arc updated to epoch {}. Resetting state and updating protocol.",
                                    updated_committee_epoch
                                );

                                self.current_protocol_epoch = updated_committee_epoch;
                                self.protocol.update_committee(new_committee.clone());
                                let new_genesis = Certificate::genesis(&new_committee);

                                // *** XÓA SET KHI TÁI CẤU HÌNH ***
                                // self.processed_consensus_rounds.clear(); // <-- XÓA BỎ
                                state = ConsensusState::new(new_genesis.clone(), updated_committee_epoch);

                                info!(
                                    "[Consensus] State fully reset for epoch {}. Starting from new genesis. Last committed round reset to 0.",
                                    updated_committee_epoch
                                );

                                info!("[Consensus] Attempting to save initial state for new epoch {}", updated_committee_epoch);
                                if let Err(e) = self.save_state(&state).await {
                                    error!("CRITICAL: Failed to save initial state for new epoch {}: {}", updated_committee_epoch, e);
                                    break;
                                }
                                info!("[Consensus] Successfully saved initial state for new epoch {}", updated_committee_epoch);

                            } else {
                                error!("[Consensus] Logic error during reconfigure: Committee epoch updated but data not captured.");
                            }
                            // --- End Anti-Race Condition Logic ---
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("[Consensus] Reconfigure receiver lagged by {}. Missed epoch transitions!", n);
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            error!("[Consensus] Reconfigure channel closed unexpectedly. Shutting down.");
                            break;
                        }
                    }
                },

                // Handle incoming certificates from the primary core.
                Some(certificate) = self.rx_primary.recv() => {
                    let current_committee_for_output = self.committee.read().await.clone();
                    if certificate.epoch() != self.current_protocol_epoch { continue; }
                    let cert_digest = certificate.digest();
                    let mut metrics_guard = self.metrics.write().await;
                    // Bỏ potential_commit_leader_round nếu không dùng nữa


                    match self.protocol.process_certificate(&mut state, certificate, &mut *metrics_guard) {
                        // *** === THAY ĐỔI LOGIC GỬI === ***
                        Ok((committed_sub_dags, commit_occurred)) => { // Quay lại dùng kiểu trả về cũ
                            drop(metrics_guard);

                            if commit_occurred { // Chỉ gửi khi CÓ commit
                                debug!(
                                    "[Consensus][E{}] Commit occurred. New last committed round: {}.",
                                    self.current_protocol_epoch, state.last_committed_round
                                );
                                if let Err(e) = self.save_state(&state).await {
                                    error!("CRITICAL: Failed to save consensus state after commit: {}", e);
                                    break;
                                 }

                                // Chỉ gửi nếu danh sách dags commit không rỗng
                                if !committed_sub_dags.is_empty() {
                                    if log_enabled!(log::Level::Info) {
                                        for dag in &committed_sub_dags {
                                            info!("[Consensus][E{}] Committed leader L{}({}) digest {}", self.current_protocol_epoch, dag.leader.round(), dag.leader.origin(), dag.leader.digest());
                                        }
                                    }

                                    // Luôn gửi None cho skipped_round khi commit
                                    let output_tuple = (committed_sub_dags.clone(), current_committee_for_output.clone(), None);
                                    if self.tx_output.send(output_tuple).is_err() {
                                         debug!("[Consensus][E{}] No receivers for committed dags (this is okay).", self.current_protocol_epoch);
                                     }
                                } else {
                                    debug!("[Consensus][E{}] Commit reported by protocol, but generated sequence is empty (likely filtered). No message sent.", self.current_protocol_epoch);
                                }
                            }
                            // BỎ HOÀN TOÀN NHÁNH 'else' (khi commit_occurred == false)
                            // Không gửi gì cả nếu không có commit
                        }
                        Err(e) => {
                            drop(metrics_guard);
                            error!("[Consensus][E{}] Error processing certificate {}: {}", self.current_protocol_epoch, cert_digest, e);
                         }
                    }
                },
                // ... (nhánh else) ...
                 else => {
                    info!("[Consensus] Primary Core channel closed. Consensus engine shutting down.");
                    break; // Exit loop if primary channel closes
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

// ====================
// TESTS
// ====================

#[cfg(test)]
mod tests {
    use super::*;
    use config::{Authority, PrimaryAddresses, WorkerAddresses}; // Thêm WorkerAddresses
    use crypto::{
        generate_consensus_keypair, generate_keypair, ConsensusPublicKey, ConsensusSecretKey,
        SecretKey,
    }; // Thêm SecretKey và consensus keys
    use primary::Header; // Thêm Header
    use rand::rngs::StdRng; // Thêm StdRng
    use rand::SeedableRng as _; // Thêm SeedableRng
    use std::collections::BTreeSet;
    use std::fs;
    use tokio::sync::mpsc::channel; // Thêm fs

    // Fixture
    fn keys() -> Vec<(PublicKey, SecretKey, ConsensusPublicKey, ConsensusSecretKey)> {
        // Sửa signature
        let mut rng = StdRng::from_seed([0; 32]);
        (0..4)
            .map(|_| {
                let (pk, sk) = generate_keypair(&mut rng);
                let (cpk, csk) = generate_consensus_keypair(&mut rng); // Tạo cả consensus keys
                (pk, sk, cpk, csk)
            })
            .collect()
    }

    // Fixture
    pub fn mock_committee() -> Committee {
        // Bỏ tham số keys
        let keys_data = keys(); // Gọi hàm keys() bên trong
        let epoch = 1; // Test epoch
        Committee {
            authorities: keys_data // Sử dụng keys_data đã tạo
                .iter()
                .map(|(id, _, consensus_key, _)| {
                    // Lấy consensus_key
                    (
                        *id,
                        Authority {
                            stake: 1,
                            consensus_key: consensus_key.clone(), // Clone consensus_key
                            primary: PrimaryAddresses {
                                primary_to_primary: "0.0.0.0:0".parse().unwrap(),
                                worker_to_primary: "0.0.0.0:0".parse().unwrap(),
                            },
                            workers: HashMap::default(), // Giữ workers rỗng cho test đơn giản
                            p2p_address: "0.0.0.0:0".to_string(),
                        },
                    )
                })
                .collect(),
            epoch,
        }
    }

    // Fixture
    fn mock_certificate(
        origin: PublicKey,
        round: Round,
        epoch: Epoch, // SỬA: Thêm epoch
        parents: BTreeSet<Digest>,
    ) -> (Digest, Certificate) {
        let certificate = Certificate {
            header: Header {
                author: origin,
                round,
                epoch, // SỬA: Thêm epoch
                parents,
                ..Header::default()
            },
            ..Certificate::default()
        };
        (certificate.digest(), certificate)
    }

    // Creates one certificate per authority starting and finishing at the specified rounds (inclusive).
    fn make_certificates(
        start: Round,
        stop: Round,
        epoch: Epoch, // SỬA: Thêm epoch
        initial_parents: &BTreeSet<Digest>,
        keys: &[PublicKey],
    ) -> (std::collections::VecDeque<Certificate>, BTreeSet<Digest>) {
        // Sửa: Thêm std::collections::
        let mut certificates = std::collections::VecDeque::new(); // Sửa: Thêm std::collections::
        let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
        let mut next_parents = BTreeSet::new();

        for round in start..=stop {
            next_parents.clear();
            for name in keys {
                let (digest, certificate) = mock_certificate(*name, round, epoch, parents.clone()); // SỬA: Truyền epoch
                certificates.push_back(certificate);
                next_parents.insert(digest);
            }
            parents = next_parents.clone();
        }
        (certificates, next_parents)
    }

    // SỬA: Cập nhật các bài test để dùng (Vec<CommittedSubDag>, ...)
    // và truyền rx_reconfigure

    #[tokio::test]
    async fn commit_one() {
        let keys_data = keys(); // Lấy dữ liệu keys
        let keys_pks: Vec<_> = keys_data.iter().map(|(pk, _, _, _)| *pk).collect(); // Lấy public keys
        let committee = mock_committee(); // Tạo committee từ keys
        let epoch = committee.epoch; // Lấy epoch từ committee

        let genesis = Certificate::genesis(&committee)
            .iter()
            .map(|x| x.digest())
            .collect::<BTreeSet<_>>();
        let (mut certificates, next_parents) = make_certificates(1, 4, epoch, &genesis, &keys_pks); // SỬA: Truyền epoch và keys_pks

        let (_, certificate) = mock_certificate(keys_pks[0], 5, epoch, next_parents); // SỬA: Truyền epoch
        certificates.push_back(certificate);

        let (tx_waiter, rx_waiter) = channel(1);
        let (tx_primary, mut rx_primary) = channel(1);
        // SỬA: Dùng broadcast và đúng kiểu tuple
        let (tx_output, mut rx_output) =
            broadcast::channel::<(Vec<CommittedSubDag>, Committee, Option<Round>)>(10);
        let (_tx_reconfigure, rx_reconfigure) = broadcast::channel(1); // SỬA: Thêm kênh reconfigure (dùng _tx vì không gửi)

        let path = ".db_test_consensus_commit_one"; // Sửa tên path
        let _ = fs::remove_dir_all(path); // Dùng fs thay vì std::fs
        let store = Store::new(path).unwrap();

        Consensus::spawn(
            Arc::new(RwLock::new(committee.clone())), // SỬA: Truyền Arc<RwLock<Committee>>
            config::Parameters {
                gc_depth: 50,
                ..Default::default()
            }, // SỬA: Truyền Parameters
            store,
            rx_waiter,
            tx_primary,
            tx_output,
            rx_reconfigure, // SỬA: Truyền rx_reconfigure
        );
        tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

        while let Some(certificate) = certificates.pop_front() {
            tx_waiter.send(certificate).await.unwrap();
        }

        // SỬA: Nhận (Vec<CommittedSubDag>, Committee, Option<Round>)
        // Chúng ta mong đợi commit ở round 5 (khi C5 được xử lý)
        // Commit này sẽ chứa L2 và các cert liên quan
        let (dags, _, skipped_round) = rx_output.recv().await.unwrap();
        assert!(
            skipped_round.is_none(),
            "Expected no skipped round, but got {:?}",
            skipped_round
        );
        assert!(!dags.is_empty(), "Expected at least one CommittedSubDag");

        // Lấy dag đầu tiên (và duy nhất trong trường hợp này)
        let dag = &dags[0];
        assert_eq!(
            dag.leader.round(),
            2,
            "Expected leader of round 2 to be committed"
        );

        let mut committed_rounds: Vec<Round> = dag.certificates.iter().map(|c| c.round()).collect();
        committed_rounds.sort_unstable();

        // Kiểm tra xem các cert của round 1 và leader round 2 có trong đó không
        let expected_rounds: Vec<Round> = vec![1, 1, 1, 1, 2]; // 4 certs R1 + 1 leader R2
        assert_eq!(
            committed_rounds, expected_rounds,
            "Mismatch in committed rounds"
        );
    }

    // Thêm các bài test khác tương tự nếu cần
}
