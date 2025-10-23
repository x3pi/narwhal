// In consensus/src/lib.rs

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
use tokio::task::JoinHandle;

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

    pub fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap_or(&0);
        self.last_committed_round = last_committed_round;

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

    pub fn validate(&self) -> Result<(), ConsensusError> {
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

impl Default for ConsensusState {
    fn default() -> Self {
        Self {
            last_committed_round: 0,
            last_committed: HashMap::new(),
            dag: HashMap::new(),
        }
    }
}

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
        let mut to_commit = vec![leader.clone()];
        let mut current_leader = leader;

        if leader.round() % 2 != 0 {
            warn!(
                "[BUG_TRACE] order_leaders initiated with an ODD-numbered leader: round {}",
                leader.round()
            );
        } else {
            debug!(
                "[BUG_TRACE] order_leaders initiated with leader at round {}",
                leader.round()
            );
        }

        // --- START FIX ---
        // The previous loop `(..).rev().step_by(2)` was buggy and iterated on odd numbers.
        // This new loop correctly iterates backwards over even-numbered rounds only.
        for r in (state.last_committed_round + 1..current_leader.round())
            .rev()
            .filter(|r| r % 2 == 0)
        // --- END FIX ---
        {
            if let Some((_, prev_leader)) = get_leader(r, &state.dag) {
                if linked(current_leader, prev_leader, &state.dag) {
                    if prev_leader.round() % 2 != 0 {
                        warn!(
                            "[BUG_TRACE] Found ODD-numbered previous leader at round {} linked to current leader at round {}",
                            prev_leader.round(),
                            current_leader.round()
                        );
                    } else {
                        debug!(
                            "[BUG_TRACE] Found linked leader at round {}",
                            prev_leader.round()
                        );
                    }
                    to_commit.push(prev_leader.clone());
                    current_leader = prev_leader;
                } else {
                    debug!("[BUG_TRACE] Leader at round {} is not linked, stopping", r);
                    break;
                }
            } else {
                debug!("[BUG_TRACE] No leader found at round {}, stopping", r);
                break;
            }
        }
        to_commit
    }

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

            if x.round() == 0 {
                continue;
            }

            let parent_round = x.round() - 1;

            for parent_digest in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&parent_round)
                    .and_then(|certs| certs.values().find(|(d, _)| d == parent_digest))
                {
                    Some(x) => x,
                    None => {
                        debug!(
                            "Parent {} not found in DAG (already GC'd or not yet received)",
                            parent_digest
                        );
                        continue;
                    }
                };

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

        let before_gc = ordered.len();
        ordered.retain(|x| x.round() + gc_depth >= state.last_committed_round);
        let after_gc = ordered.len();

        if before_gc != after_gc {
            debug!("Filtered out {} GC'd certificates", before_gc - after_gc);
        }

        ordered.sort_by_key(|x| x.round());
        debug!("Ordered {} certificates from sub-dag", ordered.len());
        ordered
    }
}

pub trait ConsensusAlgorithm: Send + Sync {
    fn update_committee(&mut self, new_committee: Committee);

    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<CommittedSubDag>, bool), ConsensusError>;

    fn name(&self) -> &'static str;
}

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
        let leader = keys[coin as usize % self.committee.size()];

        dag.get(&round).and_then(|x| x.get(&leader))
    }
}

impl ConsensusAlgorithm for Tusk {
    fn update_committee(&mut self, new_committee: Committee) {
        self.committee = new_committee;
        info!("[Tusk] Committee updated");
    }

    fn process_certificate(
        &self,
        state: &mut ConsensusState,
        certificate: Certificate,
        metrics: &mut ConsensusMetrics,
    ) -> Result<(Vec<CommittedSubDag>, bool), ConsensusError> {
        let round = certificate.round();
        metrics.total_certificates_processed += 1;

        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

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

        let mut committed_sub_dags = Vec::new();
        for leader_cert in utils::order_leaders(&leader, state, |r, d| self.leader(r, d))
            .iter()
            .rev()
        {
            let sub_dag_certificates = utils::order_dag(self.gc_depth, leader_cert, state);
            for x in &sub_dag_certificates {
                state.update(x, self.gc_depth);
            }
            if !sub_dag_certificates.is_empty() {
                committed_sub_dags.push(CommittedSubDag {
                    leader: leader_cert.clone(),
                    certificates: sub_dag_certificates,
                });
            }
        }

        let committed = !committed_sub_dags.is_empty();
        if committed {
            let total_certs: u64 = committed_sub_dags
                .iter()
                .map(|d| d.certificates.len() as u64)
                .sum();
            metrics.total_certificates_committed += total_certs;
            metrics.last_committed_round = state.last_committed_round;
        }

        Ok((committed_sub_dags, committed))
    }

    fn name(&self) -> &'static str {
        "Tusk"
    }
}

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
        debug!("Selected leader for round {}: {:?}", round, leader_pk);
        dag.get(&round).and_then(|x| x.get(leader_pk))
    }
}

impl ConsensusAlgorithm for Bullshark {
    fn update_committee(&mut self, new_committee: Committee) {
        self.committee = new_committee;
        info!("[Bullshark] Committee updated");
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
            "[BUG_TRACE] Bullshark processing certificate from round {}",
            round
        );

        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        let r = round - 1;

        debug!("[BUG_TRACE] Calculated potential leader round r = {}", r);
        if r % 2 != 0 || r < 2 {
            debug!(
                "[BUG_TRACE] Skipping commit check: r is odd or too small. r%2={}, r<2={}",
                r % 2,
                r < 2
            );
            return Ok((Vec::new(), false));
        }

        let leader_round = r;
        if leader_round <= state.last_committed_round {
            debug!(
                "[BUG_TRACE] Skipping commit check: leader_round {} <= last_committed_round {}",
                leader_round, state.last_committed_round
            );
            return Ok((Vec::new(), false));
        }

        debug!("[BUG_TRACE] Checking for leader at round {}", leader_round);
        let (leader_digest, leader) = match self.leader(leader_round, &state.dag) {
            Some(x) => {
                debug!(
                    "[BUG_TRACE] Found leader {:?} at round {}",
                    x.1.digest(),
                    leader_round
                );
                x.clone()
            }
            None => {
                metrics.failed_leader_elections += 1;
                debug!("[BUG_TRACE] No leader found at round {}", leader_round);
                return Ok((Vec::new(), false));
            }
        };

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
                "[BUG_TRACE] Leader at round {} has insufficient stake ({}/{})",
                leader_round, stake, required_stake
            );
            return Ok((Vec::new(), false));
        }

        warn!(
            "[BUG_TRACE] Leader at round {} has ENOUGH stake ({}/{}) to be committed.",
            leader_round, stake, required_stake
        );

        let mut committed_sub_dags = Vec::new();
        let ordered_leaders = utils::order_leaders(&leader, state, |r, d| self.leader(r, d));

        // SỬA ĐỔI: Chỉ commit leader cũ nhất trong chuỗi để tránh "nhảy vòng".
        if let Some(oldest_leader_to_commit) = ordered_leaders.last() {
            let leader_cert = oldest_leader_to_commit.clone();

            if leader_cert.round() % 2 != 0 {
                warn!("[BUG_TRACE] CRITICAL: An ODD-numbered leader (round {}) is about to be committed!", leader_cert.round());
            }

            let sub_dag_certificates = utils::order_dag(self.gc_depth, &leader_cert, state);
            for x in &sub_dag_certificates {
                state.update(x, self.gc_depth);
            }
            if !sub_dag_certificates.is_empty() {
                committed_sub_dags.push(CommittedSubDag {
                    leader: leader_cert.clone(),
                    certificates: sub_dag_certificates,
                });
            }
        }

        let committed = !committed_sub_dags.is_empty();
        if committed {
            let total_certs: u64 = committed_sub_dags
                .iter()
                .map(|d| d.certificates.len() as u64)
                .sum();
            metrics.total_certificates_committed += total_certs;
            metrics.last_committed_round = state.last_committed_round;
        }

        Ok((committed_sub_dags, committed))
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

pub struct Consensus {
    store: Store,
    rx_primary: Receiver<Certificate>,
    tx_primary: Sender<Certificate>,
    tx_output: Sender<(Vec<CommittedSubDag>, Committee)>,
    committee: Arc<RwLock<Committee>>,
    protocol: Box<dyn ConsensusAlgorithm>,
    genesis: Vec<Certificate>,
    metrics: Arc<RwLock<ConsensusMetrics>>,
}

impl Consensus {
    pub async fn load_last_committed_round(store: &mut Store) -> Round {
        match store.read(STATE_KEY.to_vec()).await {
            Ok(Some(bytes)) => match bincode::deserialize::<ConsensusState>(&bytes) {
                Ok(state) => {
                    log::info!(
                        "Loaded last committed round {} from store.",
                        state.last_committed_round
                    );
                    state.last_committed_round
                }
                Err(e) => {
                    log::error!(
                        "Failed to deserialize consensus state from store: {}. Starting from genesis (Round 0).",
                        e
                    );
                    0
                }
            },
            Ok(None) => 0,
            Err(e) => {
                log::error!("Failed to read consensus state from store: {:?}. Starting from genesis (Round 0).", e);
                0
            }
        }
    }

    pub fn spawn(
        committee: Arc<RwLock<Committee>>,
        _gc_depth: Round,
        store: Store,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<(Vec<CommittedSubDag>, Committee)>,
        protocol_selection: ConsensusProtocol,
    ) {
        let protocol: Box<dyn ConsensusAlgorithm> = match protocol_selection {
            ConsensusProtocol::Tusk(tusk) => Box::new(tusk),
            ConsensusProtocol::Bullshark(bullshark) => Box::new(bullshark),
        };

        let metrics = Arc::new(RwLock::new(ConsensusMetrics::default()));

        info!(
            "Starting consensus engine with {} protocol",
            protocol.name()
        );

        tokio::spawn(async move {
            let genesis = Certificate::genesis(&*committee.read().await);

            Self {
                store,
                rx_primary,
                tx_primary,
                tx_output,
                committee,
                protocol,
                genesis,
                metrics,
            }
            .run()
            .await;
        });
    }

    async fn load_state(&mut self) -> ConsensusState {
        match self.store.read(STATE_KEY.to_vec()).await {
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
        self.store.write(STATE_KEY.to_vec(), serialized).await;
        Ok(())
    }

    async fn run(&mut self) {
        let mut state = self.load_state().await;

        if let Err(e) = state.validate() {
            error!("State validation failed: {}", e);
        }

        info!(
            "Consensus engine ready. Starting from round {}",
            state.last_committed_round
        );

        loop {
            let committee = self.committee.read().await.clone();
            self.protocol.update_committee(committee.clone());

            tokio::select! {
                Some(certificate) = self.rx_primary.recv() => {
                    debug!("Received certificate from round {}", certificate.round());

                    let mut metrics_guard = self.metrics.write().await;

                    match self
                        .protocol
                        .process_certificate(&mut state, certificate, &mut metrics_guard)
                    {
                        Ok((sub_dags, committed)) => {
                            drop(metrics_guard);

                            if committed {
                                if let Err(e) = self.save_state(&state).await {
                                    error!("Failed to save state: {}", e);
                                }
                            }

                            if log_enabled!(log::Level::Debug) {
                                for (name, round) in &state.last_committed {
                                    debug!("Authority {} last committed: round {}", name, round);
                                }
                            }

                            if !sub_dags.is_empty() {
                                for sub_dag in &sub_dags {

                                    if sub_dag.leader.round() % 2 != 0 {
                                        warn!("[BUG_TRACE] About to send a committed sub-dag with an ODD-numbered leader (height {}) to the analyze function.", sub_dag.leader.round());
                                    }

                                    for certificate in &sub_dag.certificates {
                                        #[cfg(not(feature = "benchmark"))]
                                        info!("Committed {}", certificate.header);

                                        #[cfg(feature = "benchmark")]
                                        for digest in certificate.header.payload.keys() {
                                            info!("Committed {} -> {:?}", certificate.header, digest);
                                        }
                                    }
                                }

                                if let Some(last_dag) = sub_dags.last() {
                                    if let Some(last_cert) = last_dag.certificates.last() {
                                        if let Err(e) = self.tx_primary.send(last_cert.clone()).await {
                                            error!("Failed to send certificate to primary for GC: {}", e);
                                        }
                                    }
                                }

                                let output_tuple = (sub_dags, committee.clone());
                                if let Err(e) = self.tx_output.send(output_tuple).await {
                                    warn!("Failed to output certificate sequence: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error processing certificate: {}", e);
                        }
                    }
                },
                else => {
                    break;
                }
            }
        }

        info!("Consensus engine shutting down");
    }

    pub async fn get_metrics(metrics: &Arc<RwLock<ConsensusMetrics>>) -> ConsensusMetrics {
        metrics.read().await.clone()
    }
}
