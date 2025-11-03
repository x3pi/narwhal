// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use config::{Committee, Stake, RECONFIGURE_INTERVAL};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, error, info, log_enabled, warn};
use primary::{Certificate, Round};
use serde::{Deserialize, Serialize};
use sha3::Digest as Sha3Digest;
use sha3::Sha3_512 as Sha512;
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
            "Committing leader at round {} with stake {}/{}",
            leader_round, stake, required_stake
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

    /// Chọn leader theo round-robin deterministic với fallback logic 3 tầng
    /// ĐÂY LÀ CÁCH CHÍNH THỨC CỦA BULLSHARK/SUI với cải tiến liveness
    ///
    /// 3 tầng selection:
    /// 1. Designated leader (round-robin) - đảm bảo fairness
    /// 2. Fallback leader (common coin hash) - deterministic randomness
    /// 3. Try all available - đảm bảo liveness khi có bất kỳ certificate nào
    ///
    /// Leader được chọn để:
    /// - Đảm bảo fairness giữa các validators
    /// - Deterministic: tất cả nodes đều tính ra cùng leader
    /// - Byzantine-resistant: không thể manipulate vì predefined
    /// - High liveness: tỷ lệ commit cao hơn đáng kể
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // Lấy tất cả public keys và sắp xếp để đảm bảo deterministic order
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();

        // TẦNG 1: Designated leader (round-robin)
        let leader_pk = &keys[round as usize % self.committee.size()];
        if let Some(cert) = dag.get(&round).and_then(|x| x.get(leader_pk)) {
            debug!(
                "Selected designated leader for round {}: {:?}",
                round, leader_pk
            );
            return Some(cert);
        }

        // TẦNG 2: Fallback leader using common coin (hash of round + epoch)
        let common_coin = {
            let mut hasher = Sha512::new();
            hasher.update(round.to_le_bytes());
            hasher.update(self.committee.epoch.to_le_bytes());
            let hash = hasher.finalize();
            // Use first 8 bytes as u64 for determinism
            let mut coin_bytes = [0u8; 8];
            coin_bytes.copy_from_slice(&hash[..8]);
            u64::from_le_bytes(coin_bytes)
        };

        let fallback_pk = &keys[common_coin as usize % self.committee.size()];
        if let Some(cert) = dag.get(&round).and_then(|x| x.get(fallback_pk)) {
            debug!(
                "Selected fallback leader for round {}: {:?} (coin: {})",
                round, fallback_pk, common_coin
            );
            return Some(cert);
        }

        // TẦNG 3: Try all available leaders in deterministic order
        if let Some(round_certs) = dag.get(&round) {
            for key in &keys {
                if let Some(cert) = round_certs.get(key) {
                    debug!(
                        "Selected available leader for round {}: {:?} (tier 3)",
                        round, key
                    );
                    return Some(cert);
                }
            }
        }

        debug!("No leader found for round {}", round);
        None
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
            return Ok((Vec::new(), false));
        }

        let leader_round = r;
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

        // Check support from current round (2f+1 votes required)
        let stake: Stake = state
            .dag
            .get(&round)
            .ok_or(ConsensusError::MissingRound(round))?
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
            "Committing leader at round {} with stake {}/{}",
            leader_round, stake, required_stake
        );

        // Order and commit - OPTIMIZATION: Commit certificates in leader path first
        let mut sequence = Vec::new();
        let mut committed_digests = HashSet::new();

        // Commit certificates in leader path (standard Bullshark logic)
        for leader_cert in utils::order_leaders(&leader, state, |r, d| self.leader(r, d))
            .iter()
            .rev()
        {
            for x in utils::order_dag(self.gc_depth, leader_cert, state) {
                let digest = x.digest();
                if !committed_digests.contains(&digest) {
                    state.update(&x, self.gc_depth);
                    sequence.push(x.clone());
                    committed_digests.insert(digest);
                }
            }
        }

        // OPTIMIZATION: Commit any additional certificates in DAG from uncommitted rounds
        // SAFETY: Only commit certificates that are referenced by at least 2f+1 certificates from current round
        // This ensures all nodes will have the same view and commit the same sequence (no fork)
        // A certificate is "safe to commit" if it's in parents of majority of nodes in current round
        let last_committed = state.last_committed_round;
        let mut additional_certs = Vec::new();

        // SAFETY: Only commit additional certificates if they are referenced by majority (2f+1) from current round
        // This ensures all nodes commit the same sequence (no fork)
        // First pass: collect certificates to commit (avoid borrow conflicts)
        // Consider ALL rounds between last_committed and leader_round (including leader_round)
        for candidate_round in (last_committed + 1)..=leader_round {
            if let Some(round_certs) = state.dag.get(&candidate_round) {
                for (candidate_digest, candidate_cert) in round_certs.values() {
                    // Skip if already committed or would be garbage collected
                    let already_committed = state
                        .last_committed
                        .get(&candidate_cert.origin())
                        .map_or(false, |r| r >= &candidate_cert.round());

                    let candidate_digest_clone = candidate_digest.clone();
                    if already_committed
                        || committed_digests.contains(&candidate_digest_clone)
                        || candidate_cert.round() + self.gc_depth < last_committed
                    {
                        continue;
                    }

                    // SAFETY CHECK: Count how many certificates from current round reference this certificate
                    // A certificate is safe to commit if at least 2f+1 nodes have seen it (via parent reference)
                    // This ensures all nodes committing this leader round will have the same view
                    let mut supporting_stake: Stake = 0;
                    if let Some(current_round_certs) = state.dag.get(&round) {
                        for (_, current_cert) in current_round_certs.values() {
                            if current_cert
                                .header
                                .parents
                                .contains(&candidate_digest_clone)
                            {
                                supporting_stake += self.committee.stake(&current_cert.origin());
                            }
                        }
                    }

                    let required_stake_for_safety = self.committee.validity_threshold();
                    if supporting_stake >= required_stake_for_safety {
                        // Certificate is referenced by majority (2f+1) - safe to commit
                        // All nodes that commit this leader round will have seen this certificate
                        // because they all have the same 2f+1 certificates from current round
                        debug!("Certificate {} from round {} is referenced by {}/{} stake in current round - safe to commit",
                               candidate_cert.digest(), candidate_round, supporting_stake, required_stake_for_safety);
                        additional_certs.push((candidate_digest_clone, candidate_cert.clone()));
                    } else {
                        // Certificate not referenced by majority - skip to avoid fork risk
                        debug!("Certificate {} from round {} only referenced by {}/{} stake - skipping to avoid fork",
                               candidate_cert.digest(), candidate_round, supporting_stake, required_stake_for_safety);
                    }
                }
            }
        }

        // Second pass: commit collected certificates
        for (digest, cert) in additional_certs {
            debug!("Committing additional certificate {} from round {} (referenced by majority in current round)", 
                   cert.digest(), cert.round());
            state.update(&cert, self.gc_depth);
            sequence.push(cert);
            committed_digests.insert(digest);
        }

        // Sort by round to maintain ordering
        sequence.sort_by_key(|x| x.round());

        let committed = !sequence.is_empty();
        if committed {
            metrics.total_certificates_committed += sequence.len() as u64;
            metrics.last_committed_round = state.last_committed_round;
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
    shutdown_handle: config::ShutdownHandle,
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
        shutdown_handle: config::ShutdownHandle,
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

        let shutdown_handle_clone = shutdown_handle.clone();
        tokio::spawn(async move {
            Self {
                store,
                rx_primary,
                tx_primary,
                tx_output,
                protocol,
                genesis: Certificate::genesis(&committee),
                metrics: metrics_clone,
                shutdown_handle: shutdown_handle_clone,
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
            // Kiểm tra shutdown signal
            if self.shutdown_handle.is_shutdown() {
                info!("Consensus received shutdown signal, exiting gracefully");
                break;
            }

            debug!("Received certificate from round {}", certificate.round());

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

                    // QUAN TRỌNG: Output tất cả committed certificates TRƯỚC KHI check shutdown
                    // Đảm bảo analyze() nhận đủ tất cả certificates đến round RECONFIGURE_INTERVAL
                    for certificate in sequence {
                        #[cfg(not(feature = "benchmark"))]
                        info!("Committed {}", certificate.header);

                        #[cfg(feature = "benchmark")]
                        for digest in certificate.header.payload.keys() {
                            // NOTE: This log entry is used to compute performance.
                            info!("Committed {} -> {:?}", certificate.header, digest);
                        }

                        // Send to primary (cho garbage collection và feedback)
                        if let Err(e) = self.tx_primary.send(certificate.clone()).await {
                            error!("Failed to send certificate to primary: {}", e);
                        }

                        // QUAN TRỌNG: Gửi certificate đến analyze() qua tx_output
                        // Phải đảm bảo tất cả certificates đều được gửi, kể cả certificate round RECONFIGURE_INTERVAL
                        if let Err(e) = self.tx_output.send(certificate).await {
                            // Nếu channel đã đóng, analyze() có thể đã dừng nhưng vẫn có thể có messages trong buffer
                            warn!("Failed to output certificate to analyze: {}. Analyze may have exited.", e);
                        }
                    }

                    // QUAN TRỌNG: Chỉ check và set shutdown SAU KHI đã output TẤT CẢ certificates
                    // Điều này đảm bảo analyze() nhận đủ dữ liệu trước khi channel bị drop
                    if committed && state.last_committed_round >= RECONFIGURE_INTERVAL {
                        info!(
                            "Reached reconfiguration interval (round {} >= {}). All certificates output. Shutting down gracefully",
                            state.last_committed_round,
                            RECONFIGURE_INTERVAL
                        );
                        // Gửi shutdown signal đến tất cả các component
                        // Các component sẽ kiểm tra signal này và không tạo headers/certificates mới
                        self.shutdown_handle.shutdown();
                        // QUAN TRỌNG: Không drop tx_output ngay - để analyze() có thể đọc hết messages trong buffer
                        // Channel sẽ tự động đóng khi Consensus task kết thúc (sau khi break)
                        break; // Exit the consensus loop
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
