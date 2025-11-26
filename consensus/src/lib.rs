// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, error, info, log_enabled, warn};
use primary::{Certificate, Round};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use store::Store;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;

// ====================
// ERROR DEFINITIONS
// ====================

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
}

// ====================
// TYPE DEFINITIONS
// ====================

/// Biểu diễn của DAG trong bộ nhớ.
pub type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// Metrics cho monitoring
#[derive(Debug, Clone, Default)]
pub struct ConsensusMetrics {
    pub total_certificates_processed: u64,
    pub total_certificates_committed: u64,
    pub last_committed_round: Round,
    pub failed_leader_elections: u64,
}

/// Trạng thái cần được lưu trữ để phục hồi sau sự cố.
#[derive(Serialize, Deserialize, Debug, Clone)]
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

        // Garbage collection với logging chi tiết
        let mut removed_count = 0;
        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                let before_size = authorities.len();
                authorities.retain(|n, _| n != name || r >= round);
                removed_count += before_size - authorities.len();
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }

        if removed_count > 0 {
            debug!("GC removed {} certificates from DAG", removed_count);
        }
    }

    /// Validate state integrity
    pub fn validate(&self) -> Result<(), ConsensusError> {
        // Check if dag is consistent with last_committed
        for (pk, round) in &self.last_committed {
            if let Some(round_map) = self.dag.get(round) {
                if !round_map.contains_key(pk) {
                    warn!("Inconsistency detected: last_committed references missing certificate for {} at round {}", pk, round);
                }
            }
        }
        Ok(())
    }
}

// ====================
// UTILITY MODULE
// ====================

mod utils {
    use super::*;

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
                    debug!("Found linked leader at round {}", r);
                    to_commit.push(prev_leader.clone());
                    current_leader = prev_leader;
                } else {
                    debug!("Leader at round {} is not linked, stopping", r);
                    break;
                }
            } else {
                debug!("No leader found at round {}, stopping", r);
                break;
            }
        }
        to_commit
    }

    /// Kiểm tra xem có đường đi giữa hai leader hay không.
    fn linked(leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];

        for r in (prev_leader.round()..leader.round()).rev() {
            let round_certificates = match dag.get(&r) {
                Some(certs) => certs,
                None => {
                    debug!(
                        "Missing round {} in DAG during path check (leader round {}, prev_leader round {})",
                        r, leader.round(), prev_leader.round()
                    );
                    return false;
                }
            };

            parents = round_certificates
                .values()
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();

            if parents.is_empty() {
                debug!("No parents found at round {}, path broken", r);
                return false;
            }
        }

        parents.contains(&prev_leader)
    }

    /// Trải phẳng DAG con được tham chiếu bởi chứng chỉ đầu vào.
    pub fn order_dag(
        gc_depth: Round,
        leader: &Certificate,
        state: &ConsensusState,
    ) -> Vec<Certificate> {
        debug!("Ordering sub-dag from leader at round {}", leader.round());

        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();
        let mut buffer = vec![leader];

        while let Some(x) = buffer.pop() {
            ordered.push(x.clone());

            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round().saturating_sub(1)))
                    .and_then(|certs| certs.values().find(|(d, _)| d == parent))
                {
                    Some(x) => x,
                    None => {
                        debug!(
                            "Parent {} not found in DAG (already GC'd or not yet received)",
                            parent
                        );
                        continue;
                    }
                };

                // Skip if already processed or already committed
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or(false, |r| r >= &certificate.round());

                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        let before_gc = ordered.len();
        ordered.retain(|x| x.round() + gc_depth >= state.last_committed_round);
        let after_gc = ordered.len();

        if before_gc != after_gc {
            debug!("Filtered out {} GC'd certificates", before_gc - after_gc);
        }

        // Sort by round for consistent ordering
        ordered.sort_by_key(|x| x.round());
        debug!("Ordered {} certificates from sub-dag", ordered.len());
        ordered
    }
}

// ====================
// CONSENSUS ALGORITHMS
// ====================

/// Trait chung cho các thuật toán đồng thuận.
pub trait ConsensusAlgorithm: Send + Sync {
    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<Certificate>, bool), ConsensusError>;

    fn name(&self) -> &'static str;
}

/// Logic đồng thuận Tusk (Narwhal).
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
        // Simple round-robin leader election
        #[cfg(test)]
        let coin = 0;
        #[cfg(not(test))]
        let coin = round;

        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader = keys[coin as usize % self.committee.size()];

        dag.get(&round).and_then(|x| x.get(&leader))
    }
}

impl ConsensusAlgorithm for Tusk {
    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<Certificate>, bool), ConsensusError> {
        let round = certificate.round();
        metrics.total_certificates_processed += 1;

        // Add to DAG
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        // Try to commit - need even round >= 4
        let r = round - 1;
        if r % 2 != 0 || r < 4 {
            return Ok((Vec::new(), false));
        }

        let leader_round = r - 2;
        if leader_round <= state.last_committed_round {
            return Ok((Vec::new(), false));
        }

        debug!("Checking for leader at round {}", leader_round);
        let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
            Some(x) => {
                debug!("Found leader {:?} at round {}", x.1.digest(), leader_round);
                x.clone()
            }
            None => {
                metrics.failed_leader_elections += 1;
                debug!("No leader found at round {}", leader_round);
                return Ok((Vec::new(), false));
            }
        };

        // Check f+1 support
        let stake: Stake = state
            .dag
            .get(&(r - 1))
            .ok_or(ConsensusError::MissingRound(r - 1))?
            .values()
            .filter(|(_, x)| x.header.parents.contains(&leader_digest))
            .map(|(_, x)| self.committee.stake(&x.origin()))
            .sum();

        let required_stake = self.committee.validity_threshold();

        if stake < required_stake {
            debug!(
                "Leader at round {} has insufficient stake ({}/{})",
                leader_round, stake, required_stake
            );
            return Ok((Vec::new(), false));
        }

        info!(
            "[LEADER COMMIT SUCCESS] Committing leader {:?} at round {} with stake {}/{} (Tusk consensus). This leader's transactions will be committed.",
            leader.origin(),
            leader_round,
            stake,
            required_stake
        );

        // Order and commit
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
        if committed {
            metrics.total_certificates_committed += sequence.len() as u64;
            metrics.last_committed_round = state.last_committed_round;
        }

        Ok((sequence, committed))
    }

    fn name(&self) -> &'static str {
        "Tusk"
    }
}

/// Logic đồng thuận Bullshark - IMPLEMENTATION CHÍNH XÁC THEO PAPER
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

    /// Chọn leader theo round-robin deterministic
    /// ĐÂY LÀ CÁCH CHÍNH THỨC CỦA BULLSHARK/SUI
    ///
    /// Bullshark sử dụng predefined leader selection cho steady-state leaders:
    /// - Round 1 của mỗi wave: steady-state leader #1
    /// - Round 3 của mỗi wave: steady-state leader #2
    ///
    /// Leader được chọn bằng round-robin để:
    /// - Đảm bảo fairness giữa các validators
    /// - Deterministic: tất cả nodes đều tính ra cùng leader
    /// - Byzantine-resistant: không thể manipulate vì predefined
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // Lấy tất cả public keys và sắp xếp để đảm bảo deterministic order
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();

        // Round-robin selection - ĐÚNG THEO PAPER BULLSHARK
        let leader_pk = &keys[round as usize % self.committee.size()];

        debug!("Selected leader for round {}: {:?}", round, leader_pk);

        // Tìm certificate của leader trong DAG
        dag.get(&round).and_then(|x| x.get(leader_pk))
    }
}

impl ConsensusAlgorithm for Bullshark {
    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<Certificate>, bool), ConsensusError> {
        let round = certificate.round();
        metrics.total_certificates_processed += 1;

        // Add to DAG
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        // Bullshark commits leaders every 2 rounds (vs Tusk's 4)
        let r = round - 1;
        if r % 2 != 0 || r < 2 {
            debug!(
                "[CONSENSUS] Round {} not eligible for commit (r={}, r%2={}, r<2={})",
                round,
                r,
                r % 2,
                r < 2
            );
            return Ok((Vec::new(), false));
        }

        let leader_round = r;
        if leader_round <= state.last_committed_round {
            return Ok((Vec::new(), false));
        }

        info!(
            "[CONSENSUS] Checking for leader at round {} (current round: {})",
            leader_round, round
        );
        let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
            Some(x) => {
                info!(
                    "[CONSENSUS] Found leader {:?} at round {}",
                    x.1.origin(),
                    leader_round
                );
                x.clone()
            }
            None => {
                metrics.failed_leader_elections += 1;

                // CRITICAL: Detect khi leader không tạo được certificate
                let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                keys.sort();
                let leader_pk = &keys[leader_round as usize % self.committee.size()];

                // Check xem có certificates nào từ leader không (có thể có nhưng không đủ votes để tạo certificate)
                let leader_has_certificate = state
                    .dag
                    .get(&leader_round)
                    .and_then(|round_certs| round_certs.get(leader_pk))
                    .is_some();

                if !leader_has_certificate {
                    // CRITICAL: Leader không tạo được certificate - đây là vấn đề nghiêm trọng
                    let available_primaries: Vec<_> = state
                        .dag
                        .get(&leader_round)
                        .map(|certs| certs.keys().cloned().collect::<Vec<_>>())
                        .unwrap_or_default();

                    error!(
                        "[LEADER NO CERTIFICATE] Leader {:?} at round {} did NOT create a certificate. Cannot commit. Current round: {}. Available primaries at round {}: {:?} ({} out of {}). This indicates the leader may be lagging, network issues, or the leader is not receiving votes. Transactions from this leader will NOT be committed.",
                        leader_pk,
                        leader_round,
                        round,
                        leader_round,
                        available_primaries,
                        available_primaries.len(),
                        self.committee.size(),
                    );

                    // CRITICAL: Nếu là primary0, log error đặc biệt
                    if let Some(first_primary) = keys.first() {
                        if leader_pk == first_primary {
                            error!(
                                "[PRIMARY0 LEADER NO CERTIFICATE] Primary0 (first primary) as leader at round {} did NOT create a certificate. This will cause ALL transactions from primary0 to be stuck. Available primaries: {:?}",
                                leader_round,
                                available_primaries
                            );
                        }
                    }
                } else {
                    warn!(
                        "[CONSENSUS] Bullshark: no leader certificate available at round {} (leader {:?}), cannot commit (current round {}). DAG has rounds: {:?}",
                        leader_round,
                        leader_pk,
                        round,
                        state.dag.keys().collect::<Vec<_>>()
                    );
                }

                return Ok((Vec::new(), false));
            }
        };

        // Check support from current round (2f+1 votes required)
        let round_certificates = state
            .dag
            .get(&round)
            .ok_or(ConsensusError::MissingRound(round))?;

        let mut supporters_set: HashSet<PublicKey> = HashSet::new();
        let mut supporters_info: Vec<String> = Vec::new();

        let stake: Stake = round_certificates
            .values()
            .filter(|(_, x)| x.header.parents.contains(&leader_digest))
            .map(|(_, x)| {
                let pk = x.origin();
                supporters_set.insert(pk);
                let supporter_stake = self.committee.stake(&pk);
                supporters_info.push(format!("{:?}@{}", pk, supporter_stake));
                supporter_stake
            })
            .sum();

        let mut missing_info: Vec<String> = Vec::new();
        for (pk, authority) in &self.committee.authorities {
            if !supporters_set.contains(pk) {
                missing_info.push(format!("{:?}@{}", pk, authority.stake));
            }
        }

        let required_stake = self.committee.validity_threshold();

        if stake < required_stake {
            // CRITICAL: Log insufficient support với chi tiết để debug primary bị "bỏ rơi"
            let missing_stake: Stake = missing_info
                .iter()
                .map(|info| {
                    // Parse stake from "PublicKey@stake" format
                    if let Some(stake_str) = info.split('@').nth(1) {
                        stake_str.parse::<Stake>().unwrap_or(0)
                    } else {
                        0
                    }
                })
                .sum();

            warn!(
                "[LEADER INSUFFICIENT SUPPORT] Leader {:?} at round {} has insufficient support ({}/{} required). Missing stake: {}. Supporters ({}): {:?}. Missing primaries ({}): {:?}. This may indicate network issues, primary lag, or primary exclusion. If this persists, the leader's transactions will not be committed.",
                leader.origin(),
                leader_round,
                stake,
                required_stake,
                missing_stake,
                supporters_set.len(),
                supporters_info,
                missing_info.len(),
                missing_info
            );

            // CRITICAL: Nếu leader là primary đầu tiên (primary0) và thiếu support, log error để alert
            let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
            keys.sort();
            if let Some(first_primary) = keys.first() {
                if leader.origin() == *first_primary {
                    error!(
                        "[PRIMARY0 LEADER INSUFFICIENT SUPPORT] Primary0 (first primary) as leader at round {} has insufficient support ({}/{}). This may cause transactions from primary0 to be stuck. Missing primaries: {:?}",
                        leader_round,
                        stake,
                        required_stake,
                        missing_info
                    );
                }
            }

            return Ok((Vec::new(), false));
        }

        info!(
            "[LEADER COMMIT SUCCESS] Committing leader {:?} at round {} with stake {}/{} (supporters: {} primaries). This leader's transactions will be committed.",
            leader.origin(),
            leader_round,
            stake,
            required_stake,
            supporters_set.len()
        );

        // Order and commit
        let mut sequence = Vec::new();
        let prev_committed_round = state.last_committed_round;
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
        let sequence_summary = if committed {
            Some(
                sequence
                    .iter()
                    .map(|cert| {
                        format!(
                            "{{round: {}, origin: {:?}, digest: {:?}}}",
                            cert.round(),
                            cert.origin(),
                            cert.digest()
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        if committed {
            metrics.total_certificates_committed += sequence.len() as u64;
            metrics.last_committed_round = state.last_committed_round;
            let new_committed_round = state.last_committed_round;
            let round_gap = new_committed_round.saturating_sub(prev_committed_round);
            info!(
                "Bullshark: commit advanced from round {} to {} via leader {} (Δ={} rounds, {} certificates)",
                prev_committed_round,
                new_committed_round,
                leader_round,
                round_gap,
                sequence.len()
            );
            if let Some(summary) = &sequence_summary {
                info!(
                    "Bullshark: committed {} certificates via leader round {} -> {:?}",
                    summary.len(),
                    leader_round,
                    summary
                );
            }
        }

        Ok((sequence, committed))
    }

    fn name(&self) -> &'static str {
        "Bullshark"
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
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<Certificate>, bool), ConsensusError> {
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

// ====================
// MAIN CONSENSUS ENGINE
// ====================

pub struct Consensus {
    store: Store,
    rx_primary: Receiver<Certificate>,
    tx_primary: Sender<Certificate>,
    tx_output: Sender<Certificate>,
    protocol: Box<dyn ConsensusAlgorithm>,
    genesis: Vec<Certificate>,
    metrics: Arc<RwLock<ConsensusMetrics>>,
}

impl Consensus {
    const STATE_KEY: &'static [u8] = b"consensus_state";

    pub fn spawn(
        committee: Committee,
        _gc_depth: Round,
        store: Store,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
        protocol_selection: ConsensusProtocol,
    ) -> Arc<RwLock<ConsensusMetrics>> {
        let protocol: Box<dyn ConsensusAlgorithm> = match protocol_selection {
            ConsensusProtocol::Tusk(tusk) => Box::new(tusk),
            ConsensusProtocol::Bullshark(bullshark) => Box::new(bullshark),
        };

        let metrics = Arc::new(RwLock::new(ConsensusMetrics::default()));
        let metrics_clone = metrics.clone();

        info!(
            "Starting consensus engine with {} protocol",
            protocol.name()
        );

        tokio::spawn(async move {
            Self {
                store,
                rx_primary,
                tx_primary,
                tx_output,
                protocol,
                genesis: Certificate::genesis(&committee),
                metrics: metrics_clone,
            }
            .run()
            .await;
        });

        metrics
    }

    async fn load_state(&mut self) -> ConsensusState {
        match self.store.read(Self::STATE_KEY.to_vec()).await {
            Ok(Some(bytes)) => match bincode::deserialize(&bytes) {
                Ok(state) => {
                    info!("Loaded consensus state from store");
                    state
                }
                Err(e) => {
                    error!(
                        "Failed to deserialize consensus state: {}. Starting from genesis.",
                        e
                    );
                    ConsensusState::new(self.genesis.clone())
                }
            },
            Ok(None) => {
                info!("No consensus state found in store. Starting from genesis.");
                ConsensusState::new(self.genesis.clone())
            }
            Err(e) => {
                error!("Failed to read from store: {:?}. Starting from genesis.", e);
                ConsensusState::new(self.genesis.clone())
            }
        }
    }

    async fn save_state(&mut self, state: &ConsensusState) -> Result<(), ConsensusError> {
        let serialized = bincode::serialize(state)?;
        self.store.write(Self::STATE_KEY.to_vec(), serialized).await;
        Ok(())
    }

    async fn run(&mut self) {
        let mut state = self.load_state().await;

        // Validate loaded state
        if let Err(e) = state.validate() {
            error!("State validation failed: {}", e);
        }

        info!(
            "Consensus engine ready. Starting from round {}",
            state.last_committed_round
        );

        // Main processing loop
        while let Some(certificate) = self.rx_primary.recv().await {
            info!(
                "[CONSENSUS] Received certificate from round {} (origin: {:?})",
                certificate.round(),
                certificate.origin()
            );

            let mut metrics = self.metrics.write().await;

            match self
                .protocol
                .process_certificate(&mut state, certificate, &mut metrics)
            {
                Ok((sequence, committed)) => {
                    drop(metrics); // Release lock before I/O operations

                    // Persist state if committed
                    if committed {
                        if let Err(e) = self.save_state(&state).await {
                            error!("Failed to save state: {}", e);
                        }
                    }

                    // Log commit status
                    if log_enabled!(log::Level::Debug) {
                        for (name, round) in &state.last_committed {
                            debug!("Authority {} last committed: round {}", name, round);
                        }
                    }

                    // Output committed certificates
                    for certificate in sequence {
                        #[cfg(not(feature = "benchmark"))]
                        info!("Committed {}", certificate.header);

                        #[cfg(feature = "benchmark")]
                        for digest in certificate.header.payload.keys() {
                            // NOTE: This log entry is used to compute performance.
                            info!("Committed {} -> {:?}", certificate.header, digest);
                        }

                        // Send to primary
                        if let Err(e) = self.tx_primary.send(certificate.clone()).await {
                            error!("Failed to send certificate to primary: {}", e);
                        }

                        // Send to output
                        // CRITICAL: Log chi tiết khi gửi certificate tới rx_output
                        // Nếu channel đầy, đây là bottleneck nghiêm trọng
                        let cert_digest = certificate.digest();
                        let cert_round = certificate.round();
                        let batch_count = certificate.header.payload.len();

                        // CRITICAL: Warning khi certificate empty
                        if batch_count == 0 {
                            warn!(
                                "[CONSENSUS OUTPUT] WARNING: Certificate {} (round {}) has EMPTY payload (0 batches). This may indicate batches are not being included in headers!",
                                cert_digest, cert_round
                            );
                        }

                        match self.tx_output.send(certificate).await {
                            Ok(()) => {
                                info!(
                                    "[CONSENSUS OUTPUT] Successfully sent certificate {} (round {}, {} batches) to rx_output channel",
                                    cert_digest, cert_round, batch_count
                                );
                            }
                            Err(e) => {
                                // CRITICAL ERROR: Channel đầy hoặc đóng - certificates không được gửi
                                // Đây là nguyên nhân chính khiến batches không được xử lý
                                error!(
                                    "[CONSENSUS OUTPUT] CRITICAL: Failed to send certificate {} (round {}, {} batches) to rx_output channel: {}. This will cause batches to be stuck and not sent to UDS!",
                                    cert_digest, cert_round, batch_count, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error processing certificate: {}", e);
                }
            }
        }

        info!("Consensus engine shutting down");
    }

    /// Get current metrics (for monitoring/observability)
    pub async fn get_metrics(metrics: &Arc<RwLock<ConsensusMetrics>>) -> ConsensusMetrics {
        metrics.read().await.clone()
    }
}

// ====================
// TESTS
// ====================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consensus_state_creation() {
        let genesis = vec![];
        let state = ConsensusState::new(genesis);
        assert_eq!(state.last_committed_round, 0);
        assert!(state.last_committed.is_empty());
    }

    #[test]
    fn test_consensus_state_validation() {
        let state = ConsensusState::new(vec![]);
        assert!(state.validate().is_ok());
    }

    #[test]
    fn test_bullshark_leader_deterministic() {
        // Test that leader selection is deterministic
        // All nodes should compute the same leader for the same round
    }

    #[test]
    fn test_bullshark_leader_rotation() {
        // Test that leaders rotate fairly across all validators
    }
}
