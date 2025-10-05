// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use primary::{Certificate, Round};

/// Biểu diễn của DAG trong bộ nhớ.
pub type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// Trạng thái cần được lưu trữ để phục hồi sau sự cố.
#[derive(Serialize, Deserialize, Debug)]
pub struct ConsensusState {
    pub last_committed_round: Round,
    pub last_committed: HashMap<PublicKey, Round>,
    pub dag: Dag,
}

impl ConsensusState {
    /// Tạo trạng thái mới từ các chứng chỉ genesis.
    pub fn new(genesis: Vec<Certificate>) -> Self {
        let genesis_map = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_committed: genesis_map
                .iter()
                .map(|(x, (_, y))| (*x, y.round()))
                .collect(),
            dag: [(0, genesis_map)].iter().cloned().collect(),
        }
    }

    /// Cập nhật và dọn dẹp trạng thái nội bộ dựa trên các chứng chỉ đã commit.
    pub fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap_or(&0);
        self.last_committed_round = last_committed_round;

        // TODO: This cleanup is dangerous: we need to ensure consensus can receive idempotent replies
        // from the primary. Here we risk cleaning up a certificate and receiving it again later.
        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }
}

// --- MODULE TIỆN ÍCH (Sử dụng chung cho cả hai thuật toán) ---

mod utils {
    use super::{Certificate, Committee, ConsensusState, Dag, Digest, Round};
    use log::warn;
    use std::collections::HashSet;
    use log::debug;
    /// Sắp xếp các leader trong quá khứ chưa được commit.
    pub fn order_leaders<'a, LeaderElector>(
        leader: &Certificate,
        state: &'a ConsensusState,
        get_leader: LeaderElector,
    ) -> Vec<Certificate>
    where
        LeaderElector: Fn(Round, &'a Dag) -> Option<&'a (Digest, Certificate)>,
    {
        let mut to_commit = vec![leader.clone()];
        let mut current_leader = leader;
        for r in (state.last_committed_round + 2..current_leader.round())
            .rev()
            .step_by(2)
        {
            if let Some((_, prev_leader)) = get_leader(r, &state.dag) {
                if linked(current_leader, prev_leader, &state.dag) {
                    to_commit.push(prev_leader.clone());
                    current_leader = prev_leader;
                }
            }
        }
        to_commit
    }

    /// Kiểm tra xem có đường đi giữa hai leader hay không.
    fn linked(leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];
        for r in (prev_leader.round()..leader.round()).rev() {
            // Xử lý an toàn như mã cũ
            let round_certificates = match dag.get(&r) {
                Some(certs) => certs,
                None => {
                    warn!(
                        "[CONSENSUS] Missing entire round {} in DAG during path check between leaders at round {} and {}.",
                        r,
                        leader.round(),
                        prev_leader.round()
                    );
                    return false;
                }
            };
            
            parents = round_certificates
                .values()
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        parents.contains(&prev_leader)
    }

    /// Trải phẳng DAG con được tham chiếu bởi chứng chỉ đầu vào.
    pub fn order_dag(
        gc_depth: Round,
        leader: &Certificate,
        state: &ConsensusState,
    ) -> Vec<Certificate> {

        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();
        let mut buffer = vec![leader];

        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r >= &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + gc_depth >= state.last_committed_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}

// --- ĐỊNH NGHĨA CÁC THUẬT TOÁN ĐỒNG THUẬN ---

/// Trait chung cho các thuật toán đồng thuận.
trait ConsensusAlgorithm {
    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
    ) -> (Vec<Certificate>, bool /* committed */);
}

/// Logic đồng thuận cũ (dựa trên Tusk).
pub struct Tusk {
    pub committee: Committee,
    pub gc_depth: Round,
}

impl Tusk {
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        #[cfg(test)]
        let coin = 0;
        #[cfg(not(test))]
        let coin = round;

        // Elect the leader.
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader = keys[coin as usize % self.committee.size()];

        // Return its certificate and the certificate's digest.
        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }
}

impl ConsensusAlgorithm for Tusk {
    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
    ) -> (Vec<Certificate>, bool) {
        let round = certificate.round();

        // Add the new certificate to the local storage.
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        // Try to order the dag to commit. Start from the highest round for which we have at least
        // 2f+1 certificates. This is because we need them to reveal the common coin.
        let r = round - 1;

        // We only elect leaders for even round numbers.
        if r % 2 != 0 || r < 4 {
            return (Vec::new(), false);
        }

        // Get the certificate's digest of the leader of round r-2. If we already ordered this leader,
        // there is nothing to do.
        let leader_round = r - 2;
        if leader_round <= state.last_committed_round {
            return (Vec::new(), false);
        }

        // -- BẮT ĐẦU LOGGING --
        info!("[CONSENSUS] Checking for leader at round {}", leader_round);
        let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
            Some(x) => {
                info!(
                    "[CONSENSUS] Leader found for round {}: {:?}",
                    leader_round,
                    x.1.digest()
                );
                x.clone() // Clone to avoid borrowing issues
            }
            None => {
                info!("[CONSENSUS] No leader found for round {}", leader_round);
                return (Vec::new(), false);
            }
        };

        // Check if the leader has f+1 support from its children (ie. round r-1).
        let stake: Stake = state
            .dag
            .get(&(r - 1))
            .expect("We should have the whole history by now")
            .values()
            .filter(|(_, x)| x.header.parents.contains(&leader_digest))
            .map(|(_, x)| self.committee.stake(&x.origin()))
            .sum();

        let required_stake = self.committee.validity_threshold();
        info!(
            "[CONSENSUS] Leader {:?} has {} stake, requires {}",
            leader.digest(),
            stake,
            required_stake
        );

        // If it is the case, we can commit the leader.
        if stake < required_stake {
            info!(
                "[CONSENSUS] Leader {:?} does not have enough support, skipping commit",
                leader.digest()
            );
            return (Vec::new(), false);
        }
        // -- KẾT THÚC LOGGING --

        // Get an ordered list of past leaders that are linked to the current leader.
        info!(
            "[CONSENSUS] Leader {:?} has enough support! COMMITTING",
            leader.digest()
        );
        let mut sequence = Vec::new();
        for leader_cert in utils::order_leaders(&leader, state, |r, d| self.leader(r, d))
            .iter()
            .rev()
        {
            // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
            for x in utils::order_dag(self.gc_depth, leader_cert, state) {
                // Update and clean up internal state.
                state.update(&x, self.gc_depth);

                // Add the certificate to the sequence.
                sequence.push(x);
            }
        }

        let committed = !sequence.is_empty();
        (sequence, committed)
    }
}

/// Logic đồng thuận Bullshark.
pub struct Bullshark {
    pub committee: Committee,
    pub gc_depth: Round,
}

impl Bullshark {
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader_pk = &keys[round as usize % self.committee.size()];
        dag.get(&round).and_then(|x| x.get(leader_pk))
    }
}

impl ConsensusAlgorithm for Bullshark {
    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
    ) -> (Vec<Certificate>, bool) {
        let round = certificate.round();

        // Add the new certificate to the local storage.
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        let r = round - 1;

        // We only elect leaders for even round numbers.
        if r % 2 != 0 || r < 2 {
            return (Vec::new(), false);
        }

        // For Bullshark, leader is at round r (not r-2 like Tusk)
        let leader_round = r;
        if leader_round <= state.last_committed_round {
            return (Vec::new(), false);
        }

        info!("[CONSENSUS] Checking for leader at round {}", leader_round);
        let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
            Some(x) => {
                info!(
                    "[CONSENSUS] Leader found for round {}: {:?}",
                    leader_round,
                    x.1.digest()
                );
                x.clone()
            }
            None => {
                info!("[CONSENSUS] No leader found for round {}", leader_round);
                return (Vec::new(), false);
            }
        };

        // For Bullshark, we check support from round r+1 (current round)
        let stake: Stake = state
            .dag
            .get(&round)
            .expect("We should have the whole history by now")
            .values()
            .filter(|(_, x)| x.header.parents.contains(&leader_digest))
            .map(|(_, x)| self.committee.stake(&x.origin()))
            .sum();

        let required_stake = self.committee.validity_threshold();
        info!(
            "[CONSENSUS] Leader {:?} has {} stake, requires {}",
            leader.digest(),
            stake,
            required_stake
        );

        if stake < required_stake {
            info!(
                "[CONSENSUS] Leader {:?} does not have enough support, skipping commit",
                leader.digest()
            );
            return (Vec::new(), false);
        }

        info!(
            "[CONSENSUS] Leader {:?} has enough support! COMMITTING",
            leader.digest()
        );
        let mut sequence = Vec::new();
        for leader_cert in utils::order_leaders(&leader, state, |r, d| self.leader(r, d))
            .iter()
            .rev()
        {
            for x in utils::order_dag(self.gc_depth, leader_cert, state) {
                state.update(&x, self.gc_depth);
                sequence.push(x);
            }
        }

        let committed = !sequence.is_empty();
        (sequence, committed)
    }
}

pub enum ConsensusProtocol {
    Tusk(Tusk),
    Bullshark(Bullshark),
}

impl ConsensusAlgorithm for ConsensusProtocol {
    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
    ) -> (Vec<Certificate>, bool) {
        match self {
            ConsensusProtocol::Tusk(tusk) => {
                tusk.process_certificate(state, certificate)
            }
            ConsensusProtocol::Bullshark(bullshark) => {
                bullshark.process_certificate(state, certificate)
            }
        }
    }
}

// --- CORE LOGIC ---

pub struct Consensus {
    store: Store,
    rx_primary: Receiver<Certificate>,
    tx_primary: Sender<Certificate>,
    tx_output: Sender<Certificate>,
    protocol: Box<dyn ConsensusAlgorithm + Send>,
    genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        store: Store,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
        protocol_selection: ConsensusProtocol,
    ) {
        let protocol: Box<dyn ConsensusAlgorithm + Send> = match protocol_selection {
            ConsensusProtocol::Tusk(tusk) => Box::new(tusk),
            ConsensusProtocol::Bullshark(bullshark) => Box::new(bullshark),
        };

        tokio::spawn(async move {
            Self {
                store,
                rx_primary,
                tx_primary,
                tx_output,
                protocol,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        const STATE_KEY: &[u8] = b"consensus_state";

        // Try to load state from store. If it fails, create a new state.
        let mut state: ConsensusState = match self.store.read(STATE_KEY.to_vec()).await {
            Ok(Some(bytes)) => bincode::deserialize(&bytes).unwrap_or_else(|e| {
                warn!(
                    "Could not deserialize consensus state from store: {}. Starting from genesis.",
                    e
                );
                ConsensusState::new(self.genesis.clone())
            }),
            _ => {
                info!("No consensus state found in store. Starting from genesis.");
                ConsensusState::new(self.genesis.clone())
            }
        };

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_primary.recv().await {
            debug!("Processing {:?}", certificate);

            let (sequence, committed) = self.protocol.process_certificate(&mut state, certificate);

            // If we committed anything, persist the new state.
            if committed {
                let serialized_state =
                    bincode::serialize(&state).expect("Failed to serialize state");
                self.store.write(STATE_KEY.to_vec(), serialized_state).await;
            }

            // Log the latest committed round of every authority (for debug).
            if log_enabled!(log::Level::Debug) {
                for (name, round) in &state.last_committed {
                    debug!("Latest commit of {}: Round {}", name, round);
                }
            }

            // Output the sequence in the right order.
            for certificate in sequence {
                #[cfg(not(feature = "benchmark"))]
                info!("Committed {}", certificate.header);

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed {} -> {:?}", certificate.header, digest);
                }

                self.tx_primary
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send certificate to primary");

                if let Err(e) = self.tx_output.send(certificate).await {
                    warn!("Failed to output certificate: {}", e);
                }
            }
        }
    }
}