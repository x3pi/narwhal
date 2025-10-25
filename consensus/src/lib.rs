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

pub const STATE_KEY: &'static [u8] = b"consensus_state";

#[derive(Error, Debug)]
pub enum ConsensusError {
    #[error("Failed to serialize state: {0}")]
    SerializationError(#[from] bincode::Error),

    #[error("Store operation failed: {0}")]
    StoreError(String),

    #[error("Missing round {0} in DAG")]
    MissingRound(Round),

    #[error("Leader not found for round {0}")]
    LeaderNotFound(Round),

    #[error("Channel send failed: {0}")]
    ChannelError(String),

    #[error("State validation failed: {0}")]
    ValidationError(String),
}

pub type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

#[derive(Debug, Clone, Default)]
pub struct ConsensusMetrics {
    pub total_certificates_processed: u64,
    pub total_certificates_committed: u64,
    pub last_committed_round: Round,
    pub failed_leader_elections: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConsensusState {
    pub last_committed_round: Round,
    pub last_committed: HashMap<PublicKey, Round>,
    pub dag: Dag,
}

#[derive(Clone, Debug)]
pub struct CommittedSubDag {
    pub leader: Certificate,
    pub certificates: Vec<Certificate>,
}

impl ConsensusState {
    pub fn new(genesis: Vec<Certificate>) -> Self {
        let genesis_map = genesis
            .into_iter()
            .map(|cert| (cert.origin(), (cert.digest(), cert)))
            .collect::<HashMap<_, _>>();

        let last_committed_map = genesis_map.keys().map(|pk| (*pk, 0 as Round)).collect();

        Self {
            last_committed_round: 0,
            last_committed: last_committed_map,
            dag: [(0, genesis_map)].iter().cloned().collect(),
        }
    }

    pub fn update(&mut self, committed_certificate: &Certificate, gc_depth: Round) {
        let cert_round = committed_certificate.round();
        let cert_origin = committed_certificate.origin();

        self.last_committed
            .entry(cert_origin)
            .and_modify(|current_round| *current_round = max(*current_round, cert_round))
            .or_insert(cert_round);
        trace!(
            "Updated last_committed for {}: {}",
            cert_origin,
            self.last_committed[&cert_origin]
        );

        let new_overall_last_committed = *self.last_committed.values().max().unwrap_or(&0);

        if new_overall_last_committed > self.last_committed_round {
            let old_overall_last_committed = self.last_committed_round;
            self.last_committed_round = new_overall_last_committed;
            debug!(
                "Global last_committed_round advanced from {} to {}",
                old_overall_last_committed, new_overall_last_committed
            );

            let minimum_round_to_keep = self.last_committed_round.saturating_sub(gc_depth);
            debug!(
                   "Performing GC: Keeping rounds >= {}. (Current last_committed_round: {}, gc_depth: {})",
                    minimum_round_to_keep, self.last_committed_round, gc_depth
              );

            let mut removed_count = 0;
            let initial_rounds = self.dag.len();

            self.dag.retain(|r, certs_in_round| {
                  if r < &minimum_round_to_keep {
                      removed_count += certs_in_round.len();
                      trace!("GC: Removing entire round {}", r);
                      false
                  } else {
                       let round_size_before = certs_in_round.len();
                       certs_in_round.retain(|origin_pk, (_digest, cert)| {
                            let authority_last_committed = self.last_committed.get(origin_pk).cloned().unwrap_or(0);
                            let keep = cert.round() >= authority_last_committed;
                             if !keep {
                                 trace!("GC: Removing cert from {} at round {} (authority last commit: {}) within round {}", origin_pk, cert.round(), authority_last_committed, r);
                             }
                            keep
                       });
                       let round_removed_count = round_size_before - certs_in_round.len();
                        if round_removed_count > 0 {
                             trace!("GC: Removed {} individual certs within round {}", round_removed_count, r);
                             removed_count += round_removed_count;
                        }
                      !certs_in_round.is_empty()
                  }
              });

            let final_rounds = self.dag.len();
            if removed_count > 0 || initial_rounds != final_rounds {
                debug!(
                    "GC finished: Removed {} certificates. {} rounds remaining in DAG.",
                    removed_count, final_rounds
                );
            }
        } else {
            trace!(
                "Global last_committed_round ({}) did not advance. Skipping GC.",
                self.last_committed_round
            );
        }
    }

    pub fn validate(&self) -> Result<(), ConsensusError> {
        // Validation logic remains the same
        Ok(())
    }
}

impl Default for ConsensusState {
    fn default() -> Self {
        Self {
            last_committed_round: 0,
            last_committed: HashMap::new(),
            dag: HashMap::new(),
        }
    }
}

// --- mod utils, ConsensusAlgorithm trait, Tusk, Bullshark implementations remain the same ---
mod utils {
    use super::*;

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

        if leader.round() % 2 != 0 {
            warn!(
                "[order_leaders] Initiated with an ODD-numbered leader: round {}",
                leader.round()
            );
        } else {
            debug!(
                "[order_leaders] Initiated with leader at round {}",
                leader.round()
            );
        }

        for r in (state.last_committed_round..current_leader_in_sequence.round())
            .rev()
            .filter(|round| round % 2 == 0 && *round > 0)
        {
            debug!(
                "[order_leaders] Checking for linked leader at even round {}",
                r
            );

            if let Some((_prev_leader_digest, prev_leader_cert)) = get_leader(r, &state.dag) {
                if linked(current_leader_in_sequence, prev_leader_cert, &state.dag) {
                    debug!(
                        "[order_leaders] Found linked leader at round {} (linked back from round {})",
                        prev_leader_cert.round(), current_leader_in_sequence.round()
                    );
                    leader_sequence.push(prev_leader_cert.clone());
                    current_leader_in_sequence = prev_leader_cert;
                } else {
                    debug!(
                        "[order_leaders] Leader found at round {} but not linked back from round {}. Stopping search.",
                         r, current_leader_in_sequence.round()
                    );
                    break;
                }
            } else {
                debug!(
                    "[order_leaders] No designated leader certificate found in DAG for round {}. Stopping search.",
                    r
                );
                break;
            }
        }

        debug!(
            "[order_leaders] Found sequence of {} leaders: rounds {:?}",
            leader_sequence.len(),
            leader_sequence
                .iter()
                .map(|c| c.round())
                .collect::<Vec<_>>()
        );
        leader_sequence
    }

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
                    "linked check: Path found from round {} to {}",
                    cert_new.round(),
                    cert_old.round()
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
                trace!(
                    "linked check: Parent round {} missing in DAG while traversing from cert {}",
                    parent_round,
                    current_cert.digest()
                );
            }
        }

        debug!(
            "linked check: Path NOT found from round {} to {}",
            cert_new.round(),
            cert_old.round()
        );
        false
    }

    pub fn order_dag(
        gc_depth: Round,
        leader: &Certificate,
        state: &ConsensusState,
    ) -> Vec<Certificate> {
        debug!(
            "[order_dag] Ordering sub-dag starting from leader L{}({}) digest {}",
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
                "[order_dag] Processing cert C{}({}) digest {}",
                cert.round(),
                cert.origin(),
                cert.digest()
            );
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
                            let parent_origin = parent_cert.origin();
                            let parent_cert_round = parent_cert.round();
                            let is_parent_committed = state
                                .last_committed
                                .get(&parent_origin)
                                .map_or(false, |last_committed_for_origin| {
                                    last_committed_for_origin >= &parent_cert_round
                                });

                            if !is_parent_committed {
                                trace!(
                                    "[order_dag] Adding parent C{}({}) digest {} to buffer",
                                    parent_cert.round(),
                                    parent_origin,
                                    parent_digest
                                );
                                processed_digests.insert(parent_digest.clone());
                                buffer.push(parent_cert.clone());
                            } else {
                                trace!(
                                         "[order_dag] Skipping parent {} of {}: Already committed for origin {} at or before round {}",
                                         parent_digest, cert.digest(), parent_origin, parent_cert_round
                                     );
                            }
                        } else {
                            debug!(
                                   "[order_dag] Parent certificate {} for child {} (round {}) not found in DAG at round {}",
                                   parent_digest, cert.digest(), cert.round(), parent_round
                               );
                        }
                    } else {
                        trace!(
                            "[order_dag] Skipping parent {} of {}: Already in buffer/processed",
                            parent_digest,
                            cert.digest()
                        );
                    }
                }
            } else {
                if parent_round > 0 {
                    debug!(
                        "[order_dag] Parent round {} missing in DAG while processing cert {}",
                        parent_round,
                        cert.digest()
                    );
                }
            }
        } // End while let Some(cert) = buffer.pop()

        let before_gc = ordered.len();
        let min_round_to_keep = state.last_committed_round.saturating_sub(gc_depth);
        ordered.retain(|cert| cert.round() >= min_round_to_keep);
        let after_gc = ordered.len();

        if before_gc != after_gc {
            debug!(
                "[order_dag] Filtered out {} GC'd certificates (rounds < {}). {} remaining.",
                before_gc - after_gc,
                min_round_to_keep,
                after_gc
            );
        }

        ordered.sort_by_key(|cert| cert.round());

        debug!(
            "[order_dag] Ordered {} certificates in sub-dag for leader L{}({})",
            ordered.len(),
            leader.round(),
            leader.origin()
        );
        ordered
    }
}

pub trait ConsensusAlgorithm: Send + Sync {
    fn update_committee(&mut self, new_committee: Committee);

    fn process_certificate(
        &self,
        state: &mut ConsensusState, // State is mutable here
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<CommittedSubDag>, bool), ConsensusError>;

    fn name(&self) -> &'static str;
}

// --- Tusk implementation ---
pub struct Tusk {
    pub committee: Committee,
    pub gc_depth: Round,
}

impl Tusk {
    pub fn new(committee: Committee, gc_depth: Round) -> Self {
        info!("Initializing Tusk consensus with gc_depth={}", gc_depth);
        Self {
            committee,
            gc_depth,
        }
    }

    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        #[cfg(test)]
        let coin = 0;
        #[cfg(not(test))]
        let coin = round;

        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader_pk = keys[coin as usize % self.committee.size()];

        dag.get(&round)
            .and_then(|round_certs| round_certs.get(&leader_pk))
    }
}

impl ConsensusAlgorithm for Tusk {
    fn update_committee(&mut self, new_committee: Committee) {
        self.committee = new_committee;
        info!(
            "[Tusk] Committee updated for epoch {}",
            self.committee.epoch
        );
    }

    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<CommittedSubDag>, bool), ConsensusError> {
        let round = certificate.round();
        metrics.total_certificates_processed += 1;
        debug!(
            "[Tusk] Processing certificate C{}({}) digest {}",
            round,
            certificate.origin(),
            certificate.digest()
        );

        state.dag.entry(round).or_insert_with(HashMap::new).insert(
            certificate.origin(),
            (certificate.digest(), certificate.clone()),
        );

        let vote_delay = 1;
        let lookback_depth = 2;

        let potential_support_round = round;
        let leader_round_to_check = potential_support_round.saturating_sub(lookback_depth);

        debug!(
            "[Tusk] Cert round {}. Checking potential commit for leader round {}",
            round, leader_round_to_check
        );

        if leader_round_to_check == 0 || leader_round_to_check % 2 != 0 {
            debug!(
                "[Tusk] Skipping commit check: Leader round {} is genesis or odd.",
                leader_round_to_check
            );
            return Ok((Vec::new(), false));
        }

        if leader_round_to_check <= state.last_committed_round {
            debug!(
                "[Tusk] Skipping commit check: Leader round {} already committed or earlier (last_committed={})",
                leader_round_to_check, state.last_committed_round
            );
            return Ok((Vec::new(), false));
        }

        let (leader_digest, leader_cert) = match self.leader(leader_round_to_check, &state.dag) {
            Some(leader_entry) => {
                debug!(
                    "[Tusk] Found potential leader {} at round {}",
                    leader_entry.0, leader_round_to_check
                );
                leader_entry.clone()
            }
            None => {
                metrics.failed_leader_elections += 1;
                debug!(
                    "[Tusk] No leader certificate found in DAG for round {}",
                    leader_round_to_check
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
                    "[Tusk] Support round {} missing from DAG while checking leader {}",
                    support_round, leader_round_to_check
                );
                0
            }
        };

        let required_stake = self.committee.quorum_threshold();

        if supporting_stake < required_stake {
            debug!(
                "[Tusk] Leader {} at round {} has insufficient support stake ({}/{}) from round {}",
                leader_digest,
                leader_round_to_check,
                supporting_stake,
                required_stake,
                support_round
            );
            return Ok((Vec::new(), false));
        }

        info!(
            "[Tusk] Leader {} at round {} has ENOUGH support stake ({}/{}) from round {}. Initiating commit sequence.",
            leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
        );

        let mut committed_sub_dags_result = Vec::new();

        let leaders_in_sequence =
            utils::order_leaders(&leader_cert, state, |r, d| self.leader(r, d));
        debug!(
            "[Tusk] Found sequence of {} leaders ending at round {}: {:?}",
            leaders_in_sequence.len(),
            leader_round_to_check,
            leaders_in_sequence
                .iter()
                .map(|l| l.round())
                .collect::<Vec<_>>()
        );

        for leader_to_commit in leaders_in_sequence.iter().rev() {
            if leader_to_commit.round() <= state.last_committed_round {
                trace!(
                    "[Tusk] Skipping leader round {}: already globally committed.",
                    leader_to_commit.round()
                );
                continue;
            }

            info!(
                "[Tusk] Processing commit for leader round {}",
                leader_to_commit.round()
            );

            let sub_dag_certificates = utils::order_dag(self.gc_depth, leader_to_commit, state);

            if !sub_dag_certificates.is_empty() {
                debug!(
                    "[Tusk] Ordered {} certificates for sub-dag of leader round {}",
                    sub_dag_certificates.len(),
                    leader_to_commit.round()
                );

                for cert in &sub_dag_certificates {
                    state.update(cert, self.gc_depth);
                }

                committed_sub_dags_result.push(CommittedSubDag {
                    leader: leader_to_commit.clone(),
                    certificates: sub_dag_certificates,
                });
            } else {
                warn!(
                     "[Tusk] order_dag returned empty list for leader round {}. Was it fully GC'd or is DAG incomplete?",
                     leader_to_commit.round()
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
                 "[Tusk] Commit sequence finished. {} certificates committed in total this call. New global last_committed_round: {}",
                  total_certs_committed_this_call, state.last_committed_round
             );
        } else {
            debug!("[Tusk] No new leaders were committed in this call.");
        }

        Ok((committed_sub_dags_result, commit_occurred))
    }

    fn name(&self) -> &'static str {
        "Tusk"
    }
}

// --- Bullshark implementation ---
pub struct Bullshark {
    pub committee: Committee,
    pub gc_depth: Round,
}

impl Bullshark {
    pub fn new(committee: Committee, gc_depth: Round) -> Self {
        info!(
            "Initializing Bullshark consensus with gc_depth={}",
            gc_depth
        );
        Self {
            committee,
            gc_depth,
        }
    }

    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader_pk = &keys[round as usize % self.committee.size()];
        trace!(
            "[Bullshark] Designated leader for round {}: {:?}",
            round,
            leader_pk
        );
        dag.get(&round).and_then(|x| x.get(leader_pk))
    }
}

impl ConsensusAlgorithm for Bullshark {
    fn update_committee(&mut self, new_committee: Committee) {
        self.committee = new_committee;
        info!(
            // Thêm log ở đây để xác nhận
            "[Bullshark] Committee updated for epoch {}",
            self.committee.epoch
        );
    }

    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<CommittedSubDag>, bool), ConsensusError> {
        let round = certificate.round();
        metrics.total_certificates_processed += 1;

        debug!(
            "[Bullshark] Processing certificate C{}({}) digest {}",
            round,
            certificate.origin(),
            certificate.digest()
        );

        state.dag.entry(round).or_insert_with(HashMap::new).insert(
            certificate.origin(),
            (certificate.digest(), certificate.clone()),
        );

        let leader_round_to_check = round.saturating_sub(1);

        debug!(
            "[Bullshark] Cert round {}. Checking potential commit for leader round {}",
            round, leader_round_to_check
        );

        if leader_round_to_check == 0 || leader_round_to_check % 2 != 0 {
            debug!(
                "[Bullshark] Skipping commit check: Leader round {} is genesis or odd.",
                leader_round_to_check
            );
            return Ok((Vec::new(), false));
        }
        if leader_round_to_check <= state.last_committed_round {
            debug!(
                "[Bullshark] Skipping commit check: Leader round {} already committed or earlier (last_committed={})",
                leader_round_to_check, state.last_committed_round
            );
            return Ok((Vec::new(), false));
        }

        let (leader_digest, leader_cert) = match self.leader(leader_round_to_check, &state.dag) {
            Some(leader_entry) => {
                debug!(
                    "[Bullshark] Found potential leader {} at round {}",
                    leader_entry.0, leader_round_to_check
                );
                leader_entry.clone()
            }
            None => {
                metrics.failed_leader_elections += 1;
                debug!(
                    "[Bullshark] No leader certificate found in DAG for round {}",
                    leader_round_to_check
                );
                return Ok((Vec::new(), false));
            }
        };

        let support_round = round;
        let supporting_stake: Stake = match state.dag.get(&support_round) {
            Some(certs_in_support_round) => certs_in_support_round
                .values()
                .filter(|(_, cert)| cert.header.parents.contains(&leader_digest))
                .map(|(_, cert)| self.committee.stake(&cert.origin()))
                .sum(),
            None => {
                error!("[Bullshark] CRITICAL: Support round (current round) {} missing after adding cert!", support_round);
                0
            }
        };

        let required_stake = self.committee.validity_threshold();

        if supporting_stake < required_stake {
            debug!(
                "[Bullshark] Leader {} at round {} has insufficient support stake ({}/{}) from round {}",
                leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
            );
            return Ok((Vec::new(), false));
        }

        info!(
            "[Bullshark] Leader {} at round {} has ENOUGH support stake ({}/{}) from round {}. Initiating commit sequence.",
            leader_digest, leader_round_to_check, supporting_stake, required_stake, support_round
        );

        let mut committed_sub_dags_result = Vec::new();

        let leaders_in_sequence =
            utils::order_leaders(&leader_cert, state, |r, d| self.leader(r, d));
        debug!(
            "[Bullshark] Found sequence of {} leaders ending at round {}: {:?}",
            leaders_in_sequence.len(),
            leader_round_to_check,
            leaders_in_sequence
                .iter()
                .map(|l| l.round())
                .collect::<Vec<_>>()
        );

        // Find the newest leader in the sequence that hasn't been committed yet.
        // We iterate from newest to oldest.
        if let Some(leader_to_commit) = leaders_in_sequence
            .iter()
            .find(|l| l.round() > state.last_committed_round)
        {
            info!(
                "[Bullshark] Attempting to commit newest uncommitted leader: round {}",
                leader_to_commit.round()
            );

            if leader_to_commit.round() % 2 != 0 {
                warn!("[BUG_TRACE] CRITICAL: An ODD-numbered leader (round {}) chosen as newest uncommitted is about to be processed!", leader_to_commit.round());
            }

            let sub_dag_certificates = utils::order_dag(self.gc_depth, leader_to_commit, state);

            if !sub_dag_certificates.is_empty() {
                debug!(
                    "[Bullshark] Ordered {} certificates for sub-dag of leader {}",
                    sub_dag_certificates.len(),
                    leader_to_commit.round()
                );

                for cert in &sub_dag_certificates {
                    state.update(cert, self.gc_depth);
                }

                committed_sub_dags_result.push(CommittedSubDag {
                    leader: leader_to_commit.clone(),
                    certificates: sub_dag_certificates,
                });
                info!(
                      "[Bullshark] Successfully committed leader {} at round {}. New last_committed_round: {}",
                      leader_to_commit.digest(), leader_to_commit.round(), state.last_committed_round
                  );
            } else {
                warn!(
                      "[Bullshark] Skipping commit for leader {} at round {}: order_dag returned empty list (GC'd or incomplete DAG?)",
                      leader_to_commit.digest(), leader_to_commit.round()
                  );
            }
        } else {
            debug!(
                   "[Bullshark] Found leader sequence for round {}, but all leaders in it seem already committed (last_committed={}).",
                    leader_round_to_check, state.last_committed_round
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
    tx_output: broadcast::Sender<(Vec<CommittedSubDag>, Committee, Option<Round>)>,
    committee: Arc<RwLock<Committee>>,
    protocol: Box<dyn ConsensusAlgorithm>,
    genesis: Vec<Certificate>,
    metrics: Arc<RwLock<ConsensusMetrics>>,
    // THAY ĐỔI: Thêm trường này
    current_protocol_epoch: Epoch,
    // THAY ĐỔI: Thêm trường này
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
}

impl Consensus {
    pub async fn load_last_committed_round(store: &mut Store) -> Round {
        match store.read(STATE_KEY.to_vec()).await {
            Ok(Some(bytes)) => match bincode::deserialize::<ConsensusState>(&bytes) {
                Ok(state) => {
                    log::info!(
                         "Consensus::load_last_committed_round: Loaded last committed round {} from store.",
                         state.last_committed_round
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

    pub fn spawn(
        committee_arc: Arc<RwLock<Committee>>,
        parameters: config::Parameters,
        store: Store,
        rx_primary: Receiver<Certificate>,
        _tx_primary: Sender<Certificate>,
        tx_output: broadcast::Sender<(Vec<CommittedSubDag>, Committee, Option<Round>)>,
        mut rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    ) {
        let gc_depth = parameters.gc_depth;
        let committee_clone_for_spawn = committee_arc.clone();
        let metrics = Arc::new(RwLock::new(ConsensusMetrics::default()));

        tokio::spawn(async move {
            let initial_committee = committee_clone_for_spawn.read().await.clone();
            let initial_epoch = initial_committee.epoch;

            let protocol: Box<dyn ConsensusAlgorithm> =
                Box::new(Bullshark::new(initial_committee, gc_depth));

            info!(
                "Spawning Consensus task with {} protocol and gc_depth={}",
                protocol.name(),
                gc_depth
            );

            // Lấy genesis của ủy ban ban đầu
            let initial_genesis = Certificate::genesis(&*committee_clone_for_spawn.read().await);

            Self {
                store,
                rx_primary,
                tx_output,
                committee: committee_arc,
                protocol,
                genesis: initial_genesis, // Khởi tạo genesis ban đầu
                metrics,
                current_protocol_epoch: initial_epoch,
                rx_reconfigure,
            }
            .run()
            .await;
        });
    }

    async fn load_state(&mut self) -> ConsensusState {
        match self.store.read(STATE_KEY.to_vec()).await {
            Ok(Some(bytes)) => match bincode::deserialize::<ConsensusState>(&bytes) {
                Ok(mut state) => {
                    info!(
                        "Successfully loaded consensus state from store. Last committed round: {}",
                        state.last_committed_round
                    );
                    // Lấy epoch từ committee hiện tại để xác thực state
                    let current_committee = self.committee.read().await;
                    let current_epoch = current_committee.epoch;
                    // Lấy genesis certs cho epoch hiện tại để có thể reset nếu cần
                    let current_genesis = Certificate::genesis(&*current_committee);
                    drop(current_committee);

                    // Xác thực state đã tải, nếu lỗi hoặc quá cũ thì reset
                    // (Ví dụ: Nếu state tải được từ epoch < current_epoch, nên reset)
                    // Logic xác thực chi tiết hơn có thể cần thiết ở đây
                    if let Err(e) = state.validate() {
                        error!("Loaded consensus state failed validation: {}. Re-initializing from genesis for epoch {}.", e, current_epoch);
                        state = ConsensusState::new(current_genesis); // Reset dùng genesis hiện tại
                    }
                    // Thêm kiểm tra epoch nếu cần thiết, ví dụ:
                    // let state_epoch = state.dag.get(&0).map(|certs| certs.values().next().map(|(_,c)| c.epoch())).flatten().unwrap_or(0);
                    // if state_epoch < current_epoch {
                    //    info!("Loaded state from old epoch {}. Resetting state for current epoch {}.", state_epoch, current_epoch);
                    //    state = ConsensusState::new(current_genesis);
                    //}
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
                    ConsensusState::new(current_genesis)
                }
            },
            Ok(None) => {
                let current_committee = self.committee.read().await;
                let current_epoch = current_committee.epoch;
                let current_genesis = Certificate::genesis(&*current_committee);
                drop(current_committee);
                info!(
                    "No consensus state found in store. Initializing from genesis for epoch {}.",
                    current_epoch
                );
                ConsensusState::new(current_genesis)
            }
            Err(e) => {
                let current_committee = self.committee.read().await;
                let current_epoch = current_committee.epoch;
                let current_genesis = Certificate::genesis(&*current_committee);
                drop(current_committee);
                error!(
                     "Failed to read consensus state from store: {:?}. Re-initializing from genesis for epoch {}.",
                      e, current_epoch
                 );
                ConsensusState::new(current_genesis)
            }
        }
    }

    async fn save_state(&mut self, state: &ConsensusState) -> Result<(), ConsensusError> {
        state.validate()?;
        let serialized_state = bincode::serialize(state)?;
        self.store.write(STATE_KEY.to_vec(), serialized_state).await;
        debug!(
            "Successfully saved consensus state. Last committed round: {}",
            state.last_committed_round
        );
        Ok(())
    }

    async fn run(&mut self) {
        let mut state = self.load_state().await;
        info!(
            "Consensus engine starting for epoch {}. Initial last committed round: {}",
            self.current_protocol_epoch, // Log epoch hiện tại khi bắt đầu
            state.last_committed_round
        );

        loop {
            tokio::select! {
                biased;

                // *** LOGIC RECONFIGURE ĐÃ SỬA ***
                result = self.rx_reconfigure.recv() => {
                    match result {
                        Ok(_notification) => { // Trigger only, ignore old committee inside
                            // 1. Đọc ủy ban MỚI NHẤT từ Arc
                            let new_committee = self.committee.read().await.clone(); // Clone để sử dụng sau khi drop lock
                            let new_epoch = new_committee.epoch;

                            // 2. So sánh epoch MỚI TỪ ARC với epoch NỘI BỘ
                            if new_epoch > self.current_protocol_epoch {
                                info!(
                                    "Consensus detected epoch change from {} to {}. Resetting state and updating protocol.",
                                    self.current_protocol_epoch, new_epoch
                                );

                                // 3. Cập nhật epoch nội bộ
                                self.current_protocol_epoch = new_epoch;

                                // 4. Cập nhật committee cho protocol bên trong
                                self.protocol.update_committee(new_committee.clone()); // Dùng committee đã clone

                                // 5. Lấy genesis certificates của ủy ban MỚI
                                let new_genesis = Certificate::genesis(&new_committee); // Dùng committee đã clone
                                // 6. Khởi tạo lại HOÀN TOÀN trạng thái DAG và commit
                                state = ConsensusState::new(new_genesis.clone()); // <-- SỬ DỤNG NEW GENESIS
                                info!(
                                    "Consensus state fully reset for epoch {}. Starting from new genesis.",
                                    new_epoch
                                );

                                // 7. Cập nhật genesis nội bộ (để load_state dùng nếu restart)
                                self.genesis = new_genesis; // <-- CẬP NHẬT GENESIS NỘI BỘ

                                // 8. Lưu trạng thái mới (quan trọng!)
                                if let Err(e) = self.save_state(&state).await {
                                    error!("CRITICAL: Failed to save initial state for new epoch {}: {}", new_epoch, e);
                                    // Consider shutting down the node here if state cannot be saved
                                }

                            } else {
                                // Epoch không mới hơn, log debug thay vì warn
                                debug!(
                                    "Consensus received reconfigure signal for epoch {} but current epoch is already {}. Ignoring reset.",
                                    new_committee.epoch, // Log epoch từ Arc để debug
                                    self.current_protocol_epoch
                                );
                            }
                        },
                        Err(e) => {
                            warn!("Reconfigure channel error in Consensus: {}", e);
                            if e == broadcast::error::RecvError::Closed {
                                break; // Exit loop if channel closed
                            }
                            // Ignore Lagged errors
                        }
                    }
                },
                // *** KẾT THÚC SỬA LOGIC RECONFIGURE ***

                Some(certificate) = self.rx_primary.recv() => {
                    // Lấy committee hiện tại từ Arc (chỉ để gửi ra ngoài khi commit)
                    let current_committee_for_output = self.committee.read().await.clone();

                    // An toàn: bỏ qua certificate từ epoch cũ hoặc tương lai so với epoch nội bộ ĐÃ ĐƯỢC ĐỒNG BỘ
                    if certificate.epoch() != self.current_protocol_epoch {
                        warn!(
                            "Consensus received certificate from wrong epoch {} (current is {}), discarding.",
                            certificate.epoch(), self.current_protocol_epoch
                        );
                        continue; // Bỏ qua certificate này
                    }


                    let cert_round = certificate.round();
                    let cert_digest = certificate.digest();
                    trace!(
                        "Received certificate C{}({}) digest {}",
                        cert_round, certificate.origin(), cert_digest
                    );

                    let mut metrics_guard = self.metrics.write().await;
                    let potential_commit_leader_round = cert_round.saturating_sub(1); // Bullshark specific logic

                    match self.protocol.process_certificate(&mut state, certificate, &mut *metrics_guard) {
                        Ok((committed_sub_dags, commit_occurred)) => {
                            drop(metrics_guard); // Drop metrics lock

                            if commit_occurred {
                                debug!(
                                    "Commit occurred. New last committed round: {}. Saving state.",
                                    state.last_committed_round
                                );
                                if let Err(e) = self.save_state(&state).await {
                                    error!("CRITICAL: Failed to save consensus state after commit: {}", e);
                                    // Consider shutting down the node here
                                }

                                if !committed_sub_dags.is_empty() {
                                     if log_enabled!(log::Level::Info) {
                                         for dag in &committed_sub_dags {
                                              info!("Committed leader L{}({}) digest {}", dag.leader.round(), dag.leader.origin(), dag.leader.digest());
                                              #[cfg(feature = "benchmark")]
                                              for cert in &dag.certificates {
                                                   for digest in cert.header.payload.keys() {
                                                        info!("Committed {} -> {:?}", cert.header, digest);
                                                    }
                                               }
                                         }
                                     }

                                    // Gửi kèm committee của epoch mà commit này xảy ra
                                    let output_tuple = (committed_sub_dags.clone(), current_committee_for_output, None);
                                    if self.tx_output.send(output_tuple).is_err() {
                                        debug!("No receivers for committed dags (this is okay).");
                                    }
                                } else {
                                     warn!("Commit reported by protocol, but committed_sub_dags list is empty!");
                                }
                            } else {
                                 trace!("No commit occurred for certificate round {}", cert_round);

                                // Logic gửi thông báo skipped round (cho Bullshark)
                                if potential_commit_leader_round > state.last_committed_round
                                       && potential_commit_leader_round % 2 == 0
                                       && self.protocol.name() == "Bullshark" // Check protocol type
                                   {
                                    debug!(
                                         "Potential leader round {} was not committed (last committed is {}). Sending skipped round notification.",
                                         potential_commit_leader_round, state.last_committed_round
                                    );
                                    // Gửi kèm committee của epoch hiện tại
                                    let output_tuple = (Vec::new(), current_committee_for_output, Some(potential_commit_leader_round));
                                    if self.tx_output.send(output_tuple).is_err() {
                                        debug!("No receivers for skipped round {} (this is okay).", potential_commit_leader_round);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            drop(metrics_guard); // Drop metrics lock in case of error too
                            error!("Error processing certificate {}: {}", cert_digest, e);
                        }
                    }
                },
                else => {
                    info!("Primary Core channel closed. Consensus engine shutting down.");
                    break; // Exit loop
                }
            }
        } // end loop

        info!("Consensus engine main loop finished.");
    } // end run

    pub async fn get_metrics(metrics: &Arc<RwLock<ConsensusMetrics>>) -> ConsensusMetrics {
        metrics.read().await.clone()
    }
}
