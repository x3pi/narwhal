// In primary/src/core.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, VotesAggregator};
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use crate::primary::{PrimaryMessage, ReconfigureNotification};
use crate::synchronizer::Synchronizer;
use crate::{Epoch, Round};
use async_recursion::async_recursion;
use bytes::Bytes;
use config::{Committee, PrimaryAddresses, Stake}; // <-- THÊM PrimaryAddresses và Stake
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, trace, warn}; // <-- THÊM trace
use network::{CancelHandler, ReliableSender};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr; // <-- THÊM SocketAddr
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
// use std::time::Duration; // <-- BỎ Duration (không dùng trực tiếp)
use store::{Store, ROUND_INDEX_CF};
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::{interval, sleep, Duration, Instant}; // <-- THÊM Duration, interval ở đây

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

// Constants for synchronization logic
const SYNC_CHUNK_SIZE: Round = 1000; // How many rounds to request in one sync attempt
const SYNC_RETRY_DELAY: u64 = 10_000; // Delay between sync retries (milliseconds)
const SYNC_MAX_RETRIES: u32 = 10; // Maximum retries for a sync chunk before giving up
const LAG_THRESHOLD: Round = 50; // Round difference triggering sync mode

// Interval for triggering committee reconfiguration checks
pub const RECONFIGURE_INTERVAL: Round = 1000;
// Local CF for mapping (epoch, round, author) -> header_id (Digest)
const AUTHOR_ROUND_CF: &str = "author_round_index";

// *** THAY ĐỔI BẮT ĐẦU: Giảm grace period và thêm quiet period ***
// Số round đệm (rỗng) để chạy trước khi kích hoạt reconfigure.
// Đặt là 1 để R100 là round cuối cùng, trigger ở R101.
pub const GRACE_PERIOD_ROUNDS: Round = 1;
// Số round "yên tĩnh" trước khi reconfigure (R95-R100).
pub const QUIET_PERIOD_ROUNDS: Round = 5;
// *** THAY ĐỔI KẾT THÚC ***

// Sync mode: StorageSync for deep catch-up (from storage, no gc_depth limit), CatchupSync for near-head sync (with gc_depth)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum SyncMode {
    StorageSync, // Deep sync from storage: only store/index certificates, no voting, no gc_depth limit
    CatchupSync, // Near-head sync: full verification + voting, respects gc_depth
}

// Persistible sync state (without Instant, which cannot be serialized)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyncStatePersisted {
    mode: SyncMode,
    final_target_round: Round,
    current_chunk_target: Round,
    retry_count: u32,
    epoch: Epoch, // Store epoch to ensure sync state matches current epoch
}

// Internal state structure for synchronization process
struct SyncState {
    mode: SyncMode,              // Sync mode: StorageSync or CatchupSync
    final_target_round: Round,   // The ultimate round we aim to sync up to
    current_chunk_target: Round, // The target round for the current sync request chunk
    retry_count: u32,            // Number of retries for the current chunk
    last_request_time: Instant,  // Track when the last request was sent
}

impl SyncState {
    fn to_persisted(&self, epoch: Epoch) -> SyncStatePersisted {
        SyncStatePersisted {
            mode: self.mode,
            final_target_round: self.final_target_round,
            current_chunk_target: self.current_chunk_target,
            retry_count: self.retry_count,
            epoch,
        }
    }
    
    fn from_persisted(persisted: SyncStatePersisted) -> Self {
        Self {
            mode: persisted.mode,
            final_target_round: persisted.final_target_round,
            current_chunk_target: persisted.current_chunk_target,
            retry_count: persisted.retry_count,
            last_request_time: Instant::now(), // Reset timer on restore
        }
    }
}

// Core component managing the DAG, certificate creation, and interaction with other components.
pub struct Core {
    // Identity
    name: PublicKey,
    committee: Arc<RwLock<Committee>>, // Shared committee information
    epoch: Epoch,                      // Internal epoch tracker

    // Storage
    store: Store, // Persistent storage

    // Consensus interaction
    consensus_round: Arc<AtomicU64>, // Last round decided by consensus (shared with GC)
    tx_consensus: Sender<Certificate>, // Send finalized certificates to consensus

    // Proposer interaction
    tx_proposer: Sender<(Epoch, Vec<Digest>, Round)>, // Send epoch, parent cert digests and round to Proposer

    // Reconfiguration signaling
    tx_reconfigure: broadcast::Sender<ReconfigureNotification>, // Broadcast channel for reconfiguration
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>, // Receive reconfiguration signals

    // Input channels
    rx_primaries: Receiver<PrimaryMessage>, // Receive messages from other primaries
    rx_header_waiter: Receiver<Header>,     // Receive headers after dependencies met
    rx_certificate_waiter: Receiver<Certificate>, // Receive certificates after dependencies met
    rx_proposer: Receiver<Header>,          // Receive own headers from Proposer

    // Internal components & state
    synchronizer: Synchronizer, // Handles fetching missing dependencies
    signature_service: SignatureService, // Signs votes and headers
    gc_depth: Round,            // Garbage collection depth
    gc_round: Round,            // The round below which certificates are GC'd
    dag_round: Round,           // Highest round seen or processed in the DAG
    current_header: Header,     // The latest header proposed by this node
    votes_aggregator: VotesAggregator, // Aggregates votes for the current_header
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>, // Aggregates certs per round for parents
    last_voted: HashMap<Round, HashSet<PublicKey>>, // Tracks authorities voted for per round
    processing: HashMap<Round, HashSet<Digest>>, // Tracks digests currently being processed (avoid re-processing)
    network: ReliableSender,                     // Network sender for reliable P2P messages
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>, // Handles for cancelling network requests
    sync_state: Option<SyncState>, // State indicating if the node is currently syncing
    // Queue votes that arrive for future rounds/current epoch when we are not ready
    pending_votes: HashMap<Round, Vec<Vote>>,
    // --- Quorum watchdog state ---
    authors_seen_per_round: HashMap<Round, HashSet<PublicKey>>, // Authors whose C_r we have seen
    headers_seen_per_round: HashMap<Round, HashSet<PublicKey>>, // Authors whose H_r we have seen (even without C_r)
    round_stuck_since: HashMap<Round, Instant>, // Track when a round started being stuck (missing quorum)
    last_proactive_request: HashMap<Round, Instant>, // Track when last proactive request was sent for a round (rate limiting)
    last_commit_observed_round: Round,               // Last consensus round observed locally
    last_commit_observed_at: Instant, // Timestamp when last commit round was observed
    highest_round_sent_to_consensus: Round, // Highest round of certificate sent to consensus (for watchdog when consensus_round=0)
    quorum_timeout_ms: u64,                 // Timeout to consider quorum stuck for a round
    commit_stall_timeout_ms: u64,           // Timeout to consider consensus stalled
    proactive_request_cooldown_ms: u64, // Cooldown between proactive requests for the same round
    // --- Epoch start suspicious-dag guard ---
    last_suspicious_epoch_sync: Option<Instant>, // last time we triggered the small epoch-start sync
    suspicious_epoch_sync_cooldown_ms: u64,      // cooldown to avoid spamming small syncs
    // --- Epoch upgrade guard: when seeing higher-epoch traffic, suspend voting until reconfigure ---
    epoch_upgrade_pending: Option<Epoch>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Arc<RwLock<Committee>>,
        store: Store,
        synchronizer: Synchronizer,
        signature_service: SignatureService,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_primaries: Receiver<PrimaryMessage>,
        rx_header_waiter: Receiver<Header>,
        rx_certificate_waiter: Receiver<Certificate>,
        rx_proposer: Receiver<Header>,
        tx_consensus: Sender<Certificate>,
        tx_proposer: Sender<(Epoch, Vec<Digest>, Round)>,
        tx_reconfigure: broadcast::Sender<ReconfigureNotification>,
        rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
        _epoch_transitioning: Arc<AtomicBool>, // Bỏ qua, không dùng cờ này trong Core
    ) {
        tokio::spawn(async move {
            // Read initial epoch from the committee
            let initial_epoch = committee.read().await.epoch;
            let mut core = Self {
                name,
                committee,
                store,
                synchronizer,
                signature_service,
                consensus_round,
                gc_depth,
                rx_primaries,
                rx_header_waiter,
                rx_certificate_waiter,
                rx_proposer,
                tx_consensus,
                tx_proposer,
                tx_reconfigure,
                rx_reconfigure,
                epoch: initial_epoch, // Initialize internal epoch tracker
                gc_round: 0,
                dag_round: 0, // Start DAG round at 0
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(), // Will be replaced by H1
                votes_aggregator: VotesAggregator::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
                sync_state: None, // Will be restored from storage if exists
                pending_votes: HashMap::new(),
                authors_seen_per_round: HashMap::new(),
                headers_seen_per_round: HashMap::new(),
                round_stuck_since: HashMap::new(),
                last_proactive_request: HashMap::new(),
                last_commit_observed_round: 0,
                last_commit_observed_at: Instant::now(),
                highest_round_sent_to_consensus: 0,
                quorum_timeout_ms: 5_000, // Tăng từ 3s lên 5s để giảm spam
                commit_stall_timeout_ms: 10_000,
                proactive_request_cooldown_ms: 10_000, // Cooldown 10s giữa các proactive requests
                last_suspicious_epoch_sync: None,
                suspicious_epoch_sync_cooldown_ms: 10_000,
                epoch_upgrade_pending: None,
            };
            
            // Restore dag_round from storage
            core.restore_dag_round().await;
            
            // Restore sync_state from storage if exists
            if let Some(restored_state) = core.restore_sync_state().await {
                info!(
                    "[Core][E{}] Resuming sync from previous session: target={}, dag_round={}",
                    core.epoch, restored_state.final_target_round, core.dag_round
                );
                core.sync_state = Some(restored_state);
            }
            
            core.run().await;
        });
    }

    // Calculates the start round of the last reconfiguration interval.
    pub fn calculate_last_reconfiguration_round(current_round: Round) -> Round {
        (current_round / RECONFIGURE_INTERVAL) * RECONFIGURE_INTERVAL
    }

    // Persist sync state to storage so it can be restored after restart
    async fn persist_sync_state(&mut self) {
        if let Some(sync_state) = &self.sync_state {
            let persisted = sync_state.to_persisted(self.epoch);
            let key = b"core_sync_state";
            if let Ok(bytes) = bincode::serialize(&persisted) {
                self.store.write(key.to_vec(), bytes).await;
                debug!("[Core][E{}] Persisted sync state: mode={:?}, target={}", 
                    self.epoch, persisted.mode, persisted.final_target_round);
            } else {
                warn!("[Core][E{}] Failed to serialize sync state", self.epoch);
            }
        }
        // Note: We don't explicitly "remove" sync state when not syncing,
        // as it will be ignored if epoch doesn't match during restore
        
        // Also persist dag_round to track sync progress
        let dag_round_key = b"core_dag_round";
        let dag_round_data = (self.epoch, self.dag_round);
        if let Ok(bytes) = bincode::serialize(&dag_round_data) {
            self.store.write(dag_round_key.to_vec(), bytes).await;
        }
    }

    // Restore sync state from storage on startup
    async fn restore_sync_state(&mut self) -> Option<SyncState> {
        let key = b"core_sync_state";
        match self.store.read(key.to_vec()).await {
            Ok(Some(bytes)) => {
                match bincode::deserialize::<SyncStatePersisted>(&bytes) {
                    Ok(persisted) => {
                        // Only restore if epoch matches
                        if persisted.epoch == self.epoch {
                            info!(
                                "[Core][E{}] Restored sync state: mode={:?}, target={}, retry_count={}",
                                self.epoch, persisted.mode, persisted.final_target_round, persisted.retry_count
                            );
                            Some(SyncState::from_persisted(persisted))
                        } else {
                            warn!(
                                "[Core][E{}] Ignoring sync state from epoch {} (current: {})",
                                self.epoch, persisted.epoch, self.epoch
                            );
                            // Note: Stale sync state will be ignored, no need to remove
                            None
                        }
                    }
                    Err(e) => {
                        warn!("[Core][E{}] Failed to deserialize sync state: {:?}", self.epoch, e);
                        None
                    }
                }
            }
            Ok(None) => {
                debug!("[Core][E{}] No persisted sync state found", self.epoch);
                None
            }
            Err(e) => {
                warn!("[Core][E{}] Failed to read sync state: {:?}", self.epoch, e);
                None
            }
        }
    }

    // Restore dag_round from storage
    async fn restore_dag_round(&mut self) {
        let dag_round_key = b"core_dag_round";
        match self.store.read(dag_round_key.to_vec()).await {
            Ok(Some(bytes)) => {
                match bincode::deserialize::<(Epoch, Round)>(&bytes) {
                    Ok((persisted_epoch, persisted_round)) => {
                        if persisted_epoch == self.epoch {
                            if persisted_round > self.dag_round {
                                info!(
                                    "[Core][E{}] Restored dag_round: {} (was {})",
                                    self.epoch, persisted_round, self.dag_round
                                );
                                self.dag_round = persisted_round;
                            }
                        } else {
                            debug!(
                                "[Core][E{}] Ignoring dag_round from epoch {} (current: {})",
                                self.epoch, persisted_epoch, self.epoch
                            );
                        }
                    }
                    Err(e) => {
                        warn!("[Core][E{}] Failed to deserialize dag_round: {:?}", self.epoch, e);
                    }
                }
            }
            Ok(None) | Err(_) => {
                debug!("[Core][E{}] No persisted dag_round found, starting from 0", self.epoch);
            }
        }
    }

    // Set sync_state and persist it to storage
    async fn set_sync_state(&mut self, state: Option<SyncState>) {
        self.sync_state = state;
        self.persist_sync_state().await;
    }

    // Requests a chunk of certificates from peers for synchronization.
    async fn request_sync_chunk(&mut self, start: Round, end: Round, from_storage: bool) {
        let current_epoch = self.epoch; // Use internal epoch
        let sync_mode_str = if from_storage { "StorageSync" } else { "CatchupSync" };
        info!(
            "[Core][E{}] Requesting certificate sync chunk for rounds {} to {} (mode: {})",
            current_epoch, start, end, sync_mode_str
        );
        let message = PrimaryMessage::CertificateRangeRequest {
            start_round: start,
            end_round: end,
            requestor: self.name.clone(), // Include own name
            from_storage,
        };
        // Get peer addresses from the current committee state
        let addresses: Vec<SocketAddr> = {
            // <-- THÊM kiểu dữ liệu
            // Acquire read lock briefly
            let committee_guard = self.committee.read().await;
            // Ensure we only request from peers of the *current* epoch
            if committee_guard.epoch != current_epoch {
                warn!("[Core][E{}] Committee epoch changed ({}) during sync request preparation. Aborting request.",
                      current_epoch, committee_guard.epoch);
                return; // Abort if committee changed mid-operation
            }
            committee_guard
                .others_primaries(&self.name)
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect::<Vec<_>>()
            // Lock released here
        };

        // Serialize and broadcast the request
        if !addresses.is_empty() {
            let bytes =
                bincode::serialize(&message).expect("Failed to serialize cert range request");
            self.network.broadcast(addresses, Bytes::from(bytes)).await;
            debug!("[Core][E{}] Sync request broadcasted.", current_epoch);
        } else {
            warn!(
                "[Core][E{}] No peers found to request sync chunk from.",
                current_epoch
            );
        }
    }

    // Advances the synchronization process based on the current state.
    async fn advance_sync(&mut self) {
        // Check epoch mismatch BEFORE continuing sync - critical for nodes lagging behind
        {
            let committee_guard = self.committee.read().await;
            let committee_epoch = committee_guard.epoch;
            drop(committee_guard);
            
            if committee_epoch > self.epoch {
                warn!(
                    "[Core][E{}] Detected epoch mismatch during sync: committee at epoch {} but Core still at epoch {}. Aborting sync and resetting state.",
                    self.epoch, committee_epoch, self.epoch
                );
                // Clear sync state and reset epoch
                self.set_sync_state(None).await;
                self.reset_state_for_new_epoch(committee_epoch);
                info!("[Core][E{}] State reset complete after epoch mismatch detection during sync.", self.epoch);
                return;
            }
        }
        
        // Extract sync parameters while borrowing sync_state
        let (start, end, from_storage, retry_count, final_target, mode_str) = if let Some(state) = &mut self.sync_state {
            // Check if synchronization is complete
            if self.dag_round >= state.final_target_round {
                info!(
                    "[Core][E{}] Synchronization complete. Reached round {}. Switching back to Running state.",
                    self.epoch, self.dag_round
                );
                // Clear potentially stale state from sync process
                self.last_voted.clear();
                self.processing.clear();
                self.certificates_aggregators.clear(); // Clear aggregators too
                // Borrow ends here when we exit the if let Some scope
                self.set_sync_state(None).await; // Exit sync mode and persist
                return;
            }

            // Check if we should transition from StorageSync to CatchupSync
            // Transition when we're within gc_depth/2 of the target (near head)
            let gap_to_target = state.final_target_round.saturating_sub(self.dag_round);
            let needs_mode_transition = state.mode == SyncMode::StorageSync && gap_to_target <= self.gc_depth / 2;
            if needs_mode_transition {
                info!(
                    "[Core][E{}] Transitioning from StorageSync to CatchupSync: gap={}, gc_depth={}",
                    self.epoch, gap_to_target, self.gc_depth
                );
                state.mode = SyncMode::CatchupSync;
            }

            // Check if maximum retries reached
            if state.retry_count >= SYNC_MAX_RETRIES {
                warn!(
                    "[Core][E{}] Sync failed after {} retries for target round {}. Exiting Syncing state at round {}.",
                    self.epoch, SYNC_MAX_RETRIES, state.current_chunk_target, self.dag_round
                );
                self.last_voted.clear();
                self.processing.clear();
                self.certificates_aggregators.clear();
                // Release borrow by ending scope, then set sync_state to None
                // (state borrow ends here)
                self.set_sync_state(None).await; // Exit sync mode and persist
                return;
            }

            // Calculate the next chunk range to request
            // Start from the round *after* the highest round we've currently processed.
            let start = self.dag_round + 1;
            // End at either start + chunk_size or the final target, whichever is smaller.
            // In CatchupSync, respect gc_depth limit (don't request beyond gc_depth from current round)
            let end = if state.mode == SyncMode::CatchupSync {
                let gc_limit = self.dag_round + self.gc_depth;
                (start + SYNC_CHUNK_SIZE - 1).min(state.final_target_round).min(gc_limit)
            } else {
                (start + SYNC_CHUNK_SIZE - 1).min(state.final_target_round)
            };

            // Update sync state for the new chunk request
            state.current_chunk_target = end;
            state.retry_count += 1; // Increment retry count for this chunk attempt
            state.last_request_time = Instant::now(); // Record time of request

            let mode_str = match state.mode {
                SyncMode::StorageSync => "StorageSync",
                SyncMode::CatchupSync => "CatchupSync",
            };
            let from_storage = state.mode == SyncMode::StorageSync;
            let retry_count = state.retry_count;
            let final_target = state.final_target_round;
            
            // Borrow ends here when we exit the if let Some scope
            (start, end, from_storage, retry_count, final_target, mode_str)
        } else {
            warn!(
                "[Core][E{}] advance_sync called but not in Syncing state.",
                self.epoch
            );
            return;
        };
        
        // Persist state after updates (mode transition or chunk update)
        // Borrow was released when exiting the if let Some scope above
        self.persist_sync_state().await;
        
        info!(
            "[Core][E{}] Sync attempt #{} ({}): requesting rounds {} to {} (final target {})",
            self.epoch, retry_count, mode_str, start, end, final_target
        );
        // Send the actual sync request to peers with from_storage flag
        self.request_sync_chunk(start, end, from_storage).await;
    }

    // Processes a header proposed by this node itself.
    async fn process_own_header(&mut self, header: Header) -> DagResult<()> {
        // Basic sanity checks - should always pass for own headers if Proposer is correct.
        // ensure!(header.author == self.name, DagError::...); // <-- Removed InvalidPublicKey check
        ensure!(header.epoch == self.epoch, DagError::InvalidEpoch); // Check epoch match

        // Update internal tracking of the highest round seen/processed.
        // This is important even before full processing, especially for sync logic.
        if header.round > self.dag_round {
            debug!(
                "[Core][E{}] Advancing internal dag_round from {} to {} upon receiving own H{}",
                self.epoch, self.dag_round, header.round, header.round
            );
            self.dag_round = header.round;
        } else if header.round < self.dag_round {
            warn!("[Core][E{}] Received own header H{} which is older than current dag_round {}. Potential issue?",
                  self.epoch, header.round, self.dag_round);
            // This might indicate a problem if it happens often.
        }

        // Store own header immediately (we trust it).
        let bytes = bincode::serialize(&header).expect("Failed to serialize own header");
        self.store.write(header.id.to_vec(), bytes).await;
        // Write index: (epoch, round, author) -> header_id
        let idx_key = bincode::serialize(&(self.epoch, header.round, header.author.clone()))
            .expect("serialize author_round index key");
        let idx_val = bincode::serialize(&header.id).expect("serialize header id for index");
        let _ = self
            .store
            .write_cf(AUTHOR_ROUND_CF.to_string(), idx_key, idx_val)
            .await;

        // Reset votes aggregator for this new header.
        self.current_header = header.clone();
        self.votes_aggregator = VotesAggregator::new();
        // Drain any pending votes queued for this round (same epoch)
        // These are votes that arrived BEFORE the own header was created
        if let Some(mut votes) = self.pending_votes.remove(&header.round) {
            info!(
                "[Core][E{}] Draining {} queued vote(s) for round {} when initializing own header H{}({}). Votes were stored earlier and will now be processed.",
                self.epoch, votes.len(), header.round, header.id, header.author
            );
            let mut processed_count = 0;
            let mut discarded_count = 0;
            let mut verified_count = 0;
            let mut failed_verification_count = 0;
            
            for v in votes.drain(..) {
                // Only append if still same epoch and matches current header
                if v.epoch == self.epoch
                    && v.round == self.current_header.round
                    && v.origin == self.current_header.author
                    && v.id == self.current_header.id
                {
                    let committee_guard = self.committee.read().await;
                    // Verify vote signature before appending
                    match v.verify(&committee_guard) {
                        Ok(_) => {
                            verified_count += 1;
                            if let Some(cert) = self.votes_aggregator.append(
                                v,
                                &committee_guard,
                                &self.current_header,
                                self.current_header.parents.len(),
                            )? {
                                drop(committee_guard);
                                // If certificate formed immediately, process it
                                info!(
                                    "[Core][E{}] Assembled Certificate C{}({}) from queued votes that arrived early",
                                    self.epoch,
                                    cert.round(),
                                    cert.origin()
                                );
                                self.process_certificate(cert, false).await?;
                            }
                            processed_count += 1;
                        }
                        Err(e) => {
                            warn!(
                                "[Core][E{}] Queued vote V{}({}) failed verification: {}",
                                self.epoch, v.round, v.author, e
                            );
                            failed_verification_count += 1;
                        }
                    }
                } else {
                    warn!(
                        "[Core][E{}] Discarding queued vote V{}({}) for H{}({}) - does not match current_header H{}({}). Vote round={}, vote.origin={}, vote.id={:?}",
                        self.epoch, v.round, v.author, v.round, v.origin,
                        self.current_header.round, self.current_header.author,
                        v.round, v.origin, v.id
                    );
                    discarded_count += 1;
                }
            }
            info!(
                "[Core][E{}] Queue drain result for R{}: {} processed ({} verified, {} failed verification), {} discarded",
                self.epoch, header.round, processed_count, verified_count, failed_verification_count, discarded_count
            );
        } else {
            debug!(
                "[Core][E{}] No queued votes found for round {} when initializing own header H{}",
                self.epoch, header.round, header.id
            );
        }
        debug!(
            "[Core][E{}] Initialized votes aggregator for own H{}",
            self.epoch, header.round
        );

        // Add header digest to the processing set for its round.
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());

        // Broadcast the header to other primaries.
        let addresses: Vec<SocketAddr> = {
            // <-- THÊM kiểu dữ liệu
            let committee_guard = self.committee.read().await;
            // Check epoch before getting addresses
            if committee_guard.epoch != self.epoch {
                warn!("[Core][E{}] Committee epoch changed during own header processing. Aborting broadcast for H{}.", self.epoch, header.round);
                return Ok(()); // Avoid broadcasting if epoch changed
            }
            let addrs = committee_guard
                .others_primaries(&self.name)
                .iter()
                .map(|(name, x)| {
                    let addr = x.primary_to_primary;
                    if addr.ip().to_string() == "0.0.0.0" || addr.port() == 0 {
                        warn!(
                            "[Core] ⚠️ INVALID PRIMARY ADDRESS for {} when broadcasting H{}: {}",
                            name, header.round, addr
                        );
                    }
                    addr
                })
                .collect();
            addrs
        };

        // DEBUG: Log địa chỉ trước khi broadcast
        info!(
            "[Core] Broadcasting header H{} to {} addresses: {:?}",
            header.round,
            addresses.len(),
            addresses
        );

        if !addresses.is_empty() {
            let bytes_msg = bincode::serialize(&PrimaryMessage::Header(header.clone()))
                .expect("Failed to serialize header message");
            let handlers = self
                .network
                .broadcast(addresses, Bytes::from(bytes_msg))
                .await;
            // Store cancel handlers for potential future cancellation (e.g., on GC).
            self.cancel_handlers
                .entry(header.round)
                .or_insert_with(Vec::new)
                .extend(handlers);
        } else {
            warn!(
                "[Core][E{}] No peers found to broadcast own H{} to.",
                self.epoch, header.round
            );
        }

        // Process own header locally (check parents, payload, vote for it).
        // Pass `syncing = false` as this is normal operation.
        self.process_header(&header, false).await
    }

    // Processes a header received from self or peers (after basic validation).
    #[async_recursion]
    async fn process_header(&mut self, header: &Header, syncing: bool) -> DagResult<()> {
        debug!(
            "[Core][E{}] Processing Header H{}({}) sync={}",
            self.epoch, header.round, header.author, syncing
        );

        // Ensure header epoch matches current core epoch. Sanitize should catch this, but double-check.
        ensure!(header.epoch == self.epoch, DagError::InvalidEpoch);

        // Avoid re-processing headers already in the pipeline for this round.
        if self
            .processing
            .get(&header.round)
            .map_or(false, |s| s.contains(&header.id))
            && header.author != self.name
        {
            trace!(
                "[Core][E{}] Header H{}({}) already processing. Skipping.",
                self.epoch,
                header.round,
                header.author
            );
            return Ok(());
        }

        // Add to processing set (or update if own header).
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());

        // --- Dependency Checks ---

        // 1. Check Parents: Ensure all parent certificates are available locally.
        let parents = self.synchronizer.get_parents(header).await?;
        if parents.is_empty() && header.round > 0 {
            // Genesis (R0) has no parents.
            // If parents are missing, synchronizer will request them. Suspend processing.
            warn!(
                "[Core][E{}] Processing of H{}({}) suspended: missing parent(s). Synchronizer notified.",
                 self.epoch, header.round, header.author
            );
            // Remove from processing? No, keep it so we don't request sync repeatedly if header arrives again.
            // HeaderWaiter will re-feed it once parents arrive.
            return Ok(());
        }

        // 2. Verify Parent Rounds and Quorum Stake.
        let (stake, _valid_rounds) = {
            // <-- Corrected variable name
            // Use a block to manage committee lock scope
            let committee_guard = self.committee.read().await;
            // Ensure committee epoch didn't change while waiting for parents
            if committee_guard.epoch != self.epoch {
                warn!("[Core][E{}] Committee epoch changed after fetching parents for H{}({}). Aborting processing.", self.epoch, header.round, header.author);
                // Clean up processing state? Maybe remove from `processing` set.
                self.processing.entry(header.round).and_modify(|s| {
                    s.remove(&header.id);
                });
                return Ok(());
            }

            let mut current_stake = 0;
            for p in &parents {
                // Ensure parent epoch matches.
                ensure!(p.epoch() == self.epoch, DagError::InvalidEpoch);
                // Ensure parent round is exactly one less than header round.
                if p.round() + 1 != header.round {
                    error!("[Core][E{}] Header H{}({}) has parent C{}({}) with incorrect round! Discarding header.",
                           self.epoch, header.round, header.author, p.round(), p.origin());
                    // Remove invalid header from processing? Yes.
                    self.processing.entry(header.round).and_modify(|s| {
                        s.remove(&header.id);
                    });
                    // Return specific error?
                    bail!(DagError::MalformedHeader(header.id.clone()));
                }
                current_stake += committee_guard.stake(&p.origin());
            }
            (current_stake, true)
            // Lock released here
        };

        // If parent rounds were invalid, we already bailed.
        // Check if the combined stake of parents meets the quorum threshold.
        // *** THAY ĐỔI: Sử dụng quorum động dựa trên số certificate đã có trong round trước đó ***
        let prev_round = header.round.saturating_sub(1);
        let (active_cert_count, quorum_threshold, static_quorum) = {
            let committee_guard = self.committee.read().await;
            let static_q = committee_guard.quorum_threshold();
            // prev_round == 0: dùng kích thước committee làm số active certs (genesis)
            let acc = if prev_round == 0 {
                committee_guard.authorities.len()
            } else {
                self
                    .authors_seen_per_round
                    .get(&prev_round)
                    .map(|s| s.len())
                    .unwrap_or(0)
            };
            let dynamic_q = if acc > 0 {
                committee_guard.quorum_threshold_dynamic(acc)
            } else {
                static_q
            };
            (acc, dynamic_q, static_q)
        };
        
        info!(
            "[Core][E{}] Quorum for R{}: threshold={} (basis: {} certs in R{}), static_quorum={}",
            self.epoch, header.round, quorum_threshold, active_cert_count, prev_round, static_quorum
        );

        debug!(
            "[Core][E{}] Checking parents stake for H{}({}): stake={}, dynamic_quorum={} (based on {} certs in R{}), static_quorum={}",
            self.epoch, header.round, header.author, stake, quorum_threshold, active_cert_count, prev_round, static_quorum
        );
        
        ensure!(
            stake >= quorum_threshold,
            DagError::HeaderRequiresQuorum(header.id.clone())  // Not enough parent stake.
        );

        // 3. Check Payload: Ensure all referenced batch digests are available locally.
        if self.synchronizer.missing_payload(header).await? {
            // If payload is missing, synchronizer will request it. Suspend processing.
            warn!(
                "[Core][E{}] Processing of H{}({}) suspended: missing payload. Synchronizer notified.",
                 self.epoch, header.round, header.author
            );
            // Keep in processing set. HeaderWaiter will re-feed later.
            return Ok(());
        }

        // --- Header is Valid - Store and Vote (if applicable) ---
        info!(
            "[Core][E{}] H{}({}) passed dependency checks. Proceeding to store/vote...",
            self.epoch, header.round, header.author
        ); // <-- THÊM LOG

        // Store the header if it wasn't already (e.g., own header stored earlier).
        // Use read-modify-write on store? For now, simple write is likely okay.
        let header_bytes = bincode::serialize(header).expect("Failed to serialize header");
        self.store.write(header.id.to_vec(), header_bytes).await;
        // Write index for peer header
        let idx_key = bincode::serialize(&(self.epoch, header.round, header.author.clone()))
            .expect("serialize author_round index key");
        let idx_val = bincode::serialize(&header.id).expect("serialize header id for index");
        let _ = self
            .store
            .write_cf(AUTHOR_ROUND_CF.to_string(), idx_key, idx_val)
            .await;
        // Track header author for quorum watchdog (even if certificate not formed yet)
        self.headers_seen_per_round
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.author.clone());
        
        // Lag detection: if we receive a header from a round much higher than dag_round, trigger sync
        if self.sync_state.is_none() 
            && header.round > self.dag_round.saturating_add(LAG_THRESHOLD)
        {
            warn!(
                "[Core][E{}] Lag detection in process_header: Received H{} from round {} >> dag_round {} (diff: {}). Entering syncing to catch up.",
                self.epoch, header.author, header.round, self.dag_round, header.round.saturating_sub(self.dag_round)
            );
            let target = header.round.saturating_add(5);
            let gap = target.saturating_sub(self.dag_round);
            let sync_mode = if gap > self.gc_depth {
                SyncMode::StorageSync
            } else {
                SyncMode::CatchupSync
            };
            info!(
                "[Core][E{}] Initializing sync from process_header: target={}, dag_round={}, gap={}, mode={:?}",
                self.epoch, target, self.dag_round, gap, sync_mode
            );
            self.set_sync_state(Some(SyncState {
                mode: sync_mode,
                final_target_round: target,
                current_chunk_target: 0,
                retry_count: 0,
                last_request_time: Instant::now(),
            })).await;
            // Note: We continue processing this header even if we enter sync mode
            // The header will be stored and processed normally
        }
        
        debug!(
            "[Core][E{}] Stored valid H{}({})",
            self.epoch, header.round, header.author
        );

        // Only vote if we are not in sync mode OR if we are syncing but header is for current epoch.
        // IMPORTANT: Vote even when syncing IF epoch matches - this ensures progress during catch-up.
        if !syncing || (syncing && header.epoch == self.epoch) {
            // Check if we already voted for *any* header by this author at this round.
            // Prevents voting multiple times if equivocating headers arrive.
            let not_already_voted = self
                .last_voted // <-- SỬA tên biến cho rõ nghĩa
                .entry(header.round)
                .or_insert_with(HashSet::new)
                .insert(header.author); // `insert` returns true if value was NOT present

            if not_already_voted {
                // If true, this is the first time voting for this author at this round
                info!(
                    "[Core][E{}] Creating vote for H{}({}) (syncing={})",
                    self.epoch, header.round, header.author, syncing
                ); // <-- ADD LOG
                let vote = Vote::new(header, &self.name, &mut self.signature_service).await;
                debug!(
                    "[Core][E{}] Created Vote V{}({}) for H{}({})",
                    self.epoch, vote.round, vote.author, header.round, header.author
                );

                // Acquire committee lock to get target address.
                let target_primary_address_result: Result<PrimaryAddresses, DagError> = {
                    // <-- Corrected type and name
                    let committee_guard = self.committee.read().await;
                    // Check epoch again before sending vote
                    if committee_guard.epoch != self.epoch {
                        warn!("[Core][E{}] Committee epoch changed before sending vote for H{}({}). Aborting vote.", self.epoch, header.round, header.author);
                        return Ok(());
                    }
                    match committee_guard.primary(&header.author) {
                        Ok(addrs) => Ok(addrs),
                        Err(e) => {
                            log::info!(
                                "[Core] UnknownAuthority when querying primary addr: author={}, round={}, epoch(core)={}, epoch(committee)={}, err={}",
                                header.author,
                                header.round,
                                self.epoch,
                                committee_guard.epoch,
                                e
                            );
                            Err(DagError::UnknownAuthority(header.author))
                        }
                    }
                    // Lock released here
                };

                // Handle sending the vote or processing it locally.
                if vote.origin == self.name {
                    // If we are voting for our own header, process the vote directly.
                    info!(
                        "[Core][E{}] Processing own vote for H{}({})",
                        self.epoch, header.round, header.author
                    );
                    self.process_vote(vote)
                        .await
                        .expect("Failed to process own vote");
                } else {
                    // If voting for a peer's header, send the vote reliably.
                    match target_primary_address_result {
                        Ok(addresses) => {
                            // DEBUG: Log địa chỉ trước khi gửi vote để trace
                            if addresses.primary_to_primary.ip().to_string() == "0.0.0.0"
                                || addresses.primary_to_primary.port() == 0
                            {
                                warn!("[Core][E{}] ⚠️ SENDING VOTE TO INVALID ADDRESS {} for H{}({}), header author: {}, vote origin: {}, epoch: {}", 
                                      self.epoch, addresses.primary_to_primary, header.round, header.author, header.author, vote.origin, self.epoch);
                            } else {
                                info!(
                                    "[Core][E{}] Sending vote V{}({}) for H{}({}) to {}",
                                    self.epoch, vote.round, vote.author, header.round, header.author, addresses.primary_to_primary
                                );
                            }

                            // Clone vote fields needed for logging before moving vote
                            let vote_round_for_log = vote.round;
                            let vote_author_for_log = vote.author.clone();
                            
                            let vote_bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
                                .expect("Failed to serialize vote message");
                            let handler = self
                                .network
                                .send(addresses.primary_to_primary, Bytes::from(vote_bytes))
                                .await;
                            // Store cancel handler.
                            self.cancel_handlers
                                .entry(header.round)
                                .or_insert_with(Vec::new)
                                .push(handler);
                            debug!(
                                "[Core][E{}] Vote V{}({}) for H{}({}) sent successfully to {}",
                                self.epoch, vote_round_for_log, vote_author_for_log, header.round, header.author, addresses.primary_to_primary
                            );
                        },
                        Err(e) => {
                            // Should not happen if header verification passed, but handle defensively.
                            // Clone vote fields needed for logging before error (vote may be dropped by caller)
                            let vote_round_for_log = vote.round;
                            let vote_author_for_log = vote.author.clone();
                            error!("[Core][E{}] Author {} of valid H{}({}) not found in committee! Cannot send vote V{}({}). Error: {:?}", 
                                   self.epoch, header.author, header.round, header.author, vote_round_for_log, vote_author_for_log, e);
                        }
                    }
                }
            } else {
                debug!("[Core][E{}] Already voted for author {} at round {}. Ignoring H{}({}) for voting.", self.epoch, header.author, header.round, header.round, header.author);
            }
        } else {
            warn!(
                "[Core][E{}] Skipping vote for H{}({}): syncing={} and epoch mismatch (header epoch {}, current epoch {})",
                self.epoch, header.round, header.author, syncing, header.epoch, self.epoch
            );
        }
        Ok(())
    }

    // Processes a vote received from a peer or self.
    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        debug!(
            "[Core][E{}] Processing Vote V{}({}) for H{}({})",
            self.epoch, vote.round, vote.author, vote.round, vote.origin
        );

        // Ensure vote epoch matches current core epoch. Sanitize should catch this.
        ensure!(vote.epoch == self.epoch, DagError::InvalidEpoch);

        // If not for current header/round, decide whether to queue or discard
        if vote.round != self.current_header.round
            || vote.origin != self.current_header.author
            || vote.id != self.current_header.id
        {
            if vote.epoch == self.epoch {
                // Only discard votes if they are below gc_round (already garbage collected)
                // Votes >= gc_round should be kept even if current_header has advanced, 
                // as they may still be useful for forming certificates (e.g., during quiet period)
                if vote.round >= self.gc_round {
                    // Queue vote if round >= current_header.round (future rounds or current round but different header)
                    // OR if round < current_header.round but >= gc_round (still within GC window, may be useful for certificate formation)
                    if vote.round >= self.current_header.round {
                        // Queue vote to be processed when own header for this round is created
                        // Votes are stored by round, and will be drained when process_own_header is called for that round
                        let queue_entry = self.pending_votes.entry(vote.round).or_default();
                        queue_entry.push(vote.clone());
                        let queue_size = queue_entry.len();
                        
                        info!(
                            "[Core][E{}] Queued vote V{}({}) for H{}({}) - current_header is H{}({}). Total queued votes for R{}: {}. Vote will be processed when own header for R{} arrives.",
                            self.epoch, vote.round, vote.author, vote.round, vote.origin, 
                            self.current_header.round, self.current_header.author, vote.round, queue_size, vote.round
                        );
                        return Ok(());
                    } else {
                        // Vote round < current_header.round but >= gc_round
                        // This vote may still be useful if certificate hasn't been formed yet
                        // Check if we already have a certificate for this round before discarding
                        let round_key = bincode::serialize(&(self.epoch, vote.round))
                            .expect("Failed to serialize round index key");
                        let has_cert = self.store.read_cf(ROUND_INDEX_CF.to_string(), round_key).await
                            .ok()
                            .and_then(|opt| opt)
                            .is_some();
                        
                        if !has_cert {
                            // No certificate yet - queue the vote as it may still be useful
                            let queue_entry = self.pending_votes.entry(vote.round).or_default();
                            queue_entry.push(vote.clone());
                            debug!(
                                "[Core][E{}] Queued vote V{}({}) for H{}({}) - round {} < current_header.round {} but >= gc_round {} and no certificate yet. May still form certificate.",
                                self.epoch, vote.round, vote.author, vote.round, vote.origin,
                                vote.round, self.current_header.round, self.gc_round
                            );
                            return Ok(());
                        } else {
                            // Certificate already exists for this round, vote is truly stale
                            debug!(
                                "[Core][E{}] Dropping stale vote V{}({}) for H{}({}) - vote round {} < current_header.round {} but certificate for R{} already exists.",
                                self.epoch, vote.round, vote.author, vote.round, vote.origin,
                                vote.round, self.current_header.round, vote.round
                            );
                            return Ok(());
                        }
                    }
                } else {
                    // Vote is below gc_round - truly stale and should be discarded
                    debug!(
                        "[Core][E{}] Dropping vote V{}({}) for H{}({}) - vote round {} < gc_round {} (already garbage collected).",
                        self.epoch, vote.round, vote.author, vote.round, vote.origin,
                        vote.round, self.gc_round
                    );
                    return Ok(());
                }
            } else {
                debug!(
                    "[Core][E{}] Dropping vote from different epoch (vote epoch {}, current epoch {}).",
                    self.epoch, vote.epoch, self.epoch
                );
                return Ok(());
            }
        }

        // Try appending the vote to the aggregator.
        let vote_round = vote.round;
        let vote_author = vote.author.clone();
        let current_header_round = self.current_header.round;
        let current_header_author = self.current_header.author.clone();
        let certificate_option = {
            // Acquire committee lock briefly to check quorum.
            let committee_guard = self.committee.read().await;
            // Check epoch consistency before aggregating.
            if committee_guard.epoch != self.epoch {
                warn!("[Core][E{}] Committee epoch changed while processing V{}({}). Aborting aggregation.", self.epoch, vote_round, vote_author);
                return Ok(());
            }
            info!(
                "[Core][E{}] Appending vote V{}({}) for H{}({}) to aggregator. Current weight: checking quorum...",
                self.epoch, vote_round, vote_author, current_header_round, current_header_author
            );
            let active_cert_count = self.current_header.parents.len();
            
            let result = self.votes_aggregator
                .append(vote, &committee_guard, &self.current_header, active_cert_count)?; // Pass committee ref
            // Lock released here
            if result.is_none() {
                info!(
                    "[Core][E{}] Vote V{}({}) appended for H{}({}). Quorum not reached yet.",
                    self.epoch, vote_round, vote_author, current_header_round, current_header_author
                );
            }
            result
        };

        // If aggregation resulted in a new certificate...
        if let Some(certificate) = certificate_option {
            info!(
                "[Core][E{}] Assembled Certificate C{}({}) digest {}",
                self.epoch,
                certificate.round(),
                certificate.origin(),
                certificate.digest()
            );
            info!(
                "[Core][E{}] Calling process_certificate for own C{}",
                self.epoch,
                certificate.round()
            ); // <-- ADD LOG

            // Store the new certificate locally.
            // Note: process_certificate also stores, maybe redundant? Check process_certificate logic.
            // Let's keep it here for now, ensures cert is stored before broadcast.
            let cert_bytes_store = bincode::serialize(&certificate)?;
            self.store
                .write(certificate.digest().to_vec(), cert_bytes_store)
                .await;

            // Broadcast the new certificate to peers.
            let addresses: Vec<SocketAddr> = {
                // <-- THÊM kiểu dữ liệu
                let committee_guard = self.committee.read().await;
                if committee_guard.epoch != self.epoch {
                    warn!("[Core][E{}] Committee epoch changed before broadcasting C{}({}). Aborting broadcast.", self.epoch, certificate.round(), certificate.origin());
                    return Ok(());
                }
                let addrs = committee_guard
                    .others_primaries(&self.name)
                    .iter()
                    .map(|(name, x)| {
                        let addr = x.primary_to_primary;
                        if addr.ip().to_string() == "0.0.0.0" || addr.port() == 0 {
                            warn!("[Core] ⚠️ INVALID PRIMARY ADDRESS for {}: {}", name, addr);
                        }
                        addr
                    })
                    .collect();
                addrs
            };

            // DEBUG: Log địa chỉ trước khi broadcast
            info!(
                "[Core] Broadcasting certificate C{}({}) to {} addresses: {:?}",
                certificate.round(),
                certificate.origin(),
                addresses.len(),
                addresses
            );

            if !addresses.is_empty() {
                let cert_bytes_msg =
                    bincode::serialize(&PrimaryMessage::Certificate(certificate.clone()))
                        .expect("Failed to serialize certificate message");
                let handlers = self
                    .network
                    .broadcast(addresses, Bytes::from(cert_bytes_msg))
                    .await;
                // Store cancel handlers.
                self.cancel_handlers
                    .entry(certificate.round())
                    .or_insert_with(Vec::new)
                    .extend(handlers);
            }

            // Process the certificate locally (adds to DAG, checks parents for next round, sends to consensus).
            // Pass `syncing = false`.
            // Use expect here? If we created it, it should be valid. Maybe handle error?
            self.process_certificate(certificate, false)
                .await
                .expect("Failed to process own certificate"); // Panic if processing own cert fails
        }
        Ok(())
    }

    // Processes a certificate received from peers, self, or waiters.
    #[async_recursion]
    async fn process_certificate(
        &mut self,
        certificate: Certificate,
        syncing: bool,
    ) -> DagResult<()> {
        debug!(
            "[Core][E{}] Processing Certificate C{}({}) sync={}",
            self.epoch,
            certificate.round(),
            certificate.origin(),
            syncing
        );

        // Ensure certificate epoch matches current core epoch. Sanitize should catch this.
        ensure!(certificate.epoch() == self.epoch, DagError::InvalidEpoch);

        // Update internal tracking of the highest round seen/processed.
        // Crucial for sync logic and GC round calculation.
        // IMPORTANT: Only update if certificate epoch matches current epoch
        // This prevents dag_round from being set by certificates from previous epoch
        if certificate.epoch() == self.epoch && certificate.round() > self.dag_round {
            debug!(
                "[Core][E{}] Advancing internal dag_round from {} to {} upon processing C{}",
                self.epoch,
                self.dag_round,
                certificate.round(),
                certificate.round()
            );
            self.dag_round = certificate.round();
        } else if certificate.epoch() != self.epoch {
            warn!(
                "[Core][E{}] Ignoring certificate C{}({}) from epoch {} when updating dag_round. Current epoch: {}",
                self.epoch, certificate.round(), certificate.origin(), certificate.epoch(), self.epoch
            );
        }

        // Check if the header embedded in the certificate needs processing
        // (i.e., if its dependencies like parents/payload were met).
        // Avoids redundant header processing if it arrived and was processed before the cert.
        // Check the `processing` map for the *header's* digest.
        let header_digest = certificate.header.id.clone();
        if !self
            .processing
            .get(&certificate.header.round)
            .map_or(false, |x| x.contains(&header_digest))
        {
            debug!(
                "[Core][E{}] Header H{}({}) for C{}({}) needs processing first.",
                self.epoch,
                certificate.header.round,
                certificate.header.author,
                certificate.round(),
                certificate.origin()
            );
            // Process the header (checks parents/payload). Pass `syncing` flag along.
            self.process_header(&certificate.header, syncing).await?;
            // If process_header suspended due to missing deps, we should exit here too.
            // Check processing map again? If it's still not marked as processed, dependencies are still missing.
            if !self
                .processing
                .get(&certificate.header.round)
                .map_or(false, |x| x.contains(&header_digest))
            {
                debug!("[Core][E{}] Processing of C{}({}) suspended: Header dependencies still missing after calling process_header.",
                       self.epoch, certificate.round(), certificate.origin());
                return Ok(()); // Dependencies still missing, wait for waiter.
            }
            debug!(
                "[Core][E{}] Header H{}({}) processed successfully.",
                self.epoch, certificate.header.round, certificate.header.author
            );
        } else {
            trace!(
                "[Core][E{}] Header H{}({}) for C{}({}) already processed.",
                self.epoch,
                certificate.header.round,
                certificate.header.author,
                certificate.round(),
                certificate.origin()
            );
        }

        // --- Certificate Dependencies Met - Proceed ---

        // Check if all *ancestor certificates* (parents of the header) are locally available.
        // This ensures the causal history is complete before sending to consensus or using for parent aggregation.
        if !self.synchronizer.deliver_certificate(&certificate).await? {
            // If ancestors are missing, synchronizer will request them. Suspend processing.
            debug!(
                "[Core][E{}] Processing of C{}({}) suspended: missing ancestors. Synchronizer notified.",
                 self.epoch, certificate.round(), certificate.origin()
            );
            // CertificateWaiter will re-feed this certificate once ancestors arrive.
            return Ok(());
        }
        debug!(
            "[Core][E{}] All ancestors for C{}({}) are present.",
            self.epoch,
            certificate.round(),
            certificate.origin()
        );

        // Store the certificate if it's not already present.
        let digest = certificate.digest();
        if self.store.read(digest.to_vec()).await?.is_none() {
            let value = bincode::serialize(&certificate)?;
            self.store.write(digest.to_vec(), value).await;
            debug!(
                "[Core][E{}] Stored C{}({})",
                self.epoch,
                certificate.round(),
                certificate.origin()
            );

            // Update the round index (mapping round number to certificate digests).
            // Key includes epoch to prevent cross-epoch collisions.
            let key = bincode::serialize(&(self.epoch, certificate.round()))
                .expect("Failed to serialize round index key");
            let cf_name = ROUND_INDEX_CF.to_string(); // Column family name

            // Read existing digests for this round/epoch, or default to empty list.
            let mut digests_for_round: Vec<Digest> = self
                .store
                .read_cf(cf_name.clone(), key.clone())
                .await? // Read from column family
                .and_then(|v| bincode::deserialize(&v).ok()) // Deserialize if present
                .unwrap_or_default();

            // Add the new digest if not already present (shouldn't be, as we checked store above)
            if !digests_for_round.contains(&digest) {
                digests_for_round.push(digest.clone()); // Use cloned digest
                let digests_value = bincode::serialize(&digests_for_round)?;
                self.store.write_cf(cf_name, key, digests_value).await; // Write back to column family
                trace!(
                    "[Core][E{}] Added digest {} to round index for R{}",
                    self.epoch,
                    digest,
                    certificate.round()
                );
            }
            // Track certificate author immediately when stored (for quorum watchdog)
            // This allows watchdog to accurately track which certificates we have, even if quorum not formed yet
            self.authors_seen_per_round
                .entry(certificate.round())
                .or_insert_with(HashSet::new)
                .insert(certificate.origin());
            debug!(
                "[Core][E{}] Tracked certificate C{}({}) author for quorum watchdog. Round {} now has {} certificate(s).",
                self.epoch, certificate.round(), certificate.origin(), certificate.round(),
                self.authors_seen_per_round.get(&certificate.round()).map(|s| s.len()).unwrap_or(0)
            );
        } else {
            trace!(
                "[Core][E{}] Certificate C{}({}) already in store.",
                self.epoch,
                certificate.round(),
                certificate.origin()
            );
            // Still track author even if certificate was already stored
            self.authors_seen_per_round
                .entry(certificate.round())
                .or_insert_with(HashSet::new)
                .insert(certificate.origin());
        }

        // --- Parent Aggregation for Next Round ---

        // Try appending the certificate to the aggregator for its round.
        let parents_option = {
            let committee_guard = self.committee.read().await;
            // Check epoch consistency before aggregating.
            if committee_guard.epoch != self.epoch {
                warn!("[Core][E{}] Committee epoch changed while aggregating parents for C{}({}). Aborting aggregation.", self.epoch, certificate.round(), certificate.origin());
                return Ok(());
            }
            self.certificates_aggregators
                .entry(certificate.round()) // Get or create aggregator for this round
                .or_insert_with(|| Box::new(CertificatesAggregator::new()))
                .append(
                    certificate.clone(), 
                    &committee_guard,
                    {
                        let pr = certificate.round().saturating_sub(1);
                        if pr == 0 { committee_guard.authorities.len() } else { self.authors_seen_per_round.get(&pr).map(|s| s.len()).unwrap_or(0) }
                    },
                )? // Append cert, check for quorum
                                                                // Lock released here
        };

        // If quorum of certificates for this round is reached...
        if let Some(parents) = parents_option {
            // Parents are the digests of the quorum of certificates forming round N.
            // These will be used by the Proposer to create Header for round N+1.
            let formed_round = certificate.round();
            // Note: authors_seen_per_round already updated above when certificate was stored
            // --- Auto-catchup: if this node is lagging behind too much, enter Syncing state ---
            if self.sync_state.is_none()
                && formed_round > self.dag_round.saturating_add(LAG_THRESHOLD)
            {
                info!(
                    "[Core][E{}] Detected lag via parent aggregation: formed_round {} >> dag_round {}. Entering Syncing to catch up.",
                    self.epoch,
                    formed_round,
                    self.dag_round
                );
                // Target at least the next proposing round; add small buffer of 5 rounds
                let target = formed_round.saturating_add(5);
                let gap = target.saturating_sub(self.dag_round);
                let sync_mode = if gap > self.gc_depth {
                    SyncMode::StorageSync
                } else {
                    SyncMode::CatchupSync
                };
                info!(
                    "[Core][E{}] Initializing sync from parent aggregation: target={}, dag_round={}, gap={}, mode={:?}",
                    self.epoch, target, self.dag_round, gap, sync_mode
                );
                self.set_sync_state(Some(SyncState {
                    mode: sync_mode,
                    final_target_round: target,
                    current_chunk_target: 0,
                    retry_count: 0,
                    last_request_time: Instant::now(),
                })).await;
                // Immediately kick sync progress
                self.advance_sync().await;
            }
            info!(
                "[Core][E{}] Quorum reached for round {}. Sending {} parents to Proposer (for R{}).", // <-- SỬA LOG
                self.epoch,
                formed_round,
                parents.len(),
                formed_round + 1 // <-- THÊM
            );

            // *** THAY ĐỔI BẮT ĐẦU: Logic Reconfigure đã được sửa đổi ***
            // --- Reconfiguration Check ---
            // Check if the *next* round (N+1) is a reconfiguration boundary.
            let proposing_round = formed_round + 1;

            // TÍNH TOÁN ROUND KÍCH HOẠT MỚI (100 + 1 = 101)
            let reconfigure_trigger_round = RECONFIGURE_INTERVAL + GRACE_PERIOD_ROUNDS;

            if proposing_round > 0 && proposing_round == reconfigure_trigger_round {
                info!(
                    "[Core][E{}] Round {} IS the reconfiguration trigger round {}. Signaling committee reconfiguration.", // Sửa log
                     self.epoch, proposing_round, reconfigure_trigger_round
                );
                
                // IMPORTANT: Delay reconfiguration trigger để đảm bảo round (proposing_round - 1) được commit trước
                // Round 500 cần được commit trước khi trigger reconfiguration ở round 501
                // Để round 500 commit, cần có round 501 certificates support nó
                // Vì vậy cần đợi:
                // 1. Round 501 certificates được gửi đến consensus (để có thể support round 500)
                // 2. Round 500 được commit
                let leader_round_to_commit = proposing_round.saturating_sub(1); // Round cần được commit trước
                let current_committed = self.consensus_round.load(Ordering::Relaxed) as Round;
                
                // Đợi tối đa 10 giây để round 500 được commit
                let mut waited = 0;
                let max_wait_ms = 10000;
                let check_interval_ms = 100;
                
                // First, đợi round 501 certificates được gửi đến consensus
                // (Để có thể support round 500 commit)
                let support_round = proposing_round;
                info!(
                    "[Core][E{}] Round {} is reconfiguration trigger. Waiting for round {} certificates to be sent to consensus (to support round {} commit)...",
                    self.epoch, proposing_round, support_round, leader_round_to_commit
                );
                
                let mut check_sent = self.highest_round_sent_to_consensus;
                while check_sent < support_round && waited < max_wait_ms / 2 {
                    tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                    waited += check_interval_ms;
                    check_sent = self.highest_round_sent_to_consensus;
                    if check_sent >= support_round {
                        info!(
                            "[Core][E{}] Round {} certificates now sent to consensus (highest sent: {}). Waiting for round {} commit...",
                            self.epoch, support_round, check_sent, leader_round_to_commit
                        );
                        break;
                    }
                }
                
                // Then, đợi round 500 được commit
                // NOTE: Nếu leader của round 500 fail và không tạo certificate, 
                // consensus sẽ không thể commit round 500 (cần leader certificate trong DAG).
                // Trong trường hợp này, vẫn đợi nhưng có warning nếu không commit được.
                if current_committed < leader_round_to_commit {
                    // Kiểm tra xem có leader certificate của round 500 không
                    let leader_pk = {
                        let committee_guard = self.committee.read().await;
                        let mut keys: Vec<_> = committee_guard.authorities.keys().cloned().collect();
                        keys.sort();
                        let leader_index = leader_round_to_commit as usize % committee_guard.size();
                        keys[leader_index].clone()
                    };
                    
                    let has_leader_cert = self.authors_seen_per_round
                        .get(&leader_round_to_commit)
                        .map(|authors| authors.contains(&leader_pk))
                        .unwrap_or(false);
                    
                    if !has_leader_cert {
                        warn!(
                            "[Core][E{}] Leader {} of round {} may have failed (no certificate seen). Round {} may not be able to commit. Waiting longer ({})...",
                            self.epoch, leader_pk, leader_round_to_commit, leader_round_to_commit, max_wait_ms * 2
                        );
                        // Đợi lâu hơn nếu leader fail (tối đa 20 giây)
                        let mut check_committed = current_committed;
                        let max_wait_for_failed_leader_ms = max_wait_ms * 2;
                        while check_committed < leader_round_to_commit && waited < max_wait_for_failed_leader_ms {
                            tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                            waited += check_interval_ms;
                            check_committed = self.consensus_round.load(Ordering::Relaxed) as Round;
                            if check_committed >= leader_round_to_commit {
                                info!(
                                    "[Core][E{}] Leader round {} now committed (current: {}) despite leader failure. Proceeding with reconfiguration trigger.",
                                    self.epoch, leader_round_to_commit, check_committed
                                );
                                break;
                            }
                        }
                    } else {
                        info!(
                            "[Core][E{}] Round {} certificates sent, but leader round {} not yet committed (current: {}). Waiting for commit before triggering reconfiguration...",
                            self.epoch, support_round, leader_round_to_commit, current_committed
                        );
                    }
                    
                    let mut check_committed = current_committed;
                    while check_committed < leader_round_to_commit && waited < max_wait_ms {
                        tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                        waited += check_interval_ms;
                        check_committed = self.consensus_round.load(Ordering::Relaxed) as Round;
                        if check_committed >= leader_round_to_commit {
                            info!(
                                "[Core][E{}] Leader round {} now committed (current: {}). Proceeding with reconfiguration trigger.",
                                self.epoch, leader_round_to_commit, check_committed
                            );
                            break;
                        }
                    }
                    
                    let final_committed = self.consensus_round.load(Ordering::Relaxed) as Round;
                    if final_committed < leader_round_to_commit {
                        let leader_pk_check = {
                            let committee_guard = self.committee.read().await;
                            let mut keys: Vec<_> = committee_guard.authorities.keys().cloned().collect();
                            keys.sort();
                            let leader_index = leader_round_to_commit as usize % committee_guard.size();
                            keys[leader_index].clone()
                        };
                        let final_has_leader_cert = self.authors_seen_per_round
                            .get(&leader_round_to_commit)
                            .map(|authors| authors.contains(&leader_pk_check))
                            .unwrap_or(false);
                        
                        if !final_has_leader_cert {
                            warn!(
                                "[Core][E{}] Leader {} of round {} failed (no certificate). Round {} cannot be committed (consensus requires leader certificate). Triggering reconfiguration anyway (block {} will be missing).",
                                self.epoch, leader_pk_check, leader_round_to_commit, leader_round_to_commit, leader_round_to_commit / 2
                            );
                        } else {
                            warn!(
                                "[Core][E{}] Leader round {} still not committed after {}ms wait (current: {}, highest sent: {}). Triggering reconfiguration anyway (may cause block {} to be missing).",
                                self.epoch, leader_round_to_commit, waited, final_committed, self.highest_round_sent_to_consensus, leader_round_to_commit / 2
                            );
                        }
                    }
                } else {
                    info!(
                        "[Core][E{}] Leader round {} already committed (current: {}). Proceeding with reconfiguration trigger.",
                        self.epoch, leader_round_to_commit, current_committed
                    );
                }
                
                // Send notification *before* sending parents to proposer for this round.
                // The notification includes the *current* committee (epoch N).
                let notification = ReconfigureNotification {
                    round: proposing_round, // The round triggering the change
                    committee: self.committee.read().await.clone(), // Clone current committee
                };
                // Send on broadcast channel. Ignore error if no receivers (e.g., if not main node).
                if self.tx_reconfigure.send(notification).is_err() {
                    warn!(
                        "[Core][E{}] No receivers for reconfigure signal (this is okay).",
                        self.epoch
                    );
                }

                // Note: vẫn tiếp tục gửi parents như bình thường.
                // Proposer có logic tự bỏ qua trong lúc epoch_transitioning, tránh kẹt leader-support.
            }
            // *** THAY ĐỔI KẾT THÚC ***

            // Send parents to Proposer
            // (Không cần cờ 'epoch_transitioning' ở đây nữa, Proposer sẽ tự kiểm tra round)
            if let Err(e) = self
                .tx_proposer
                .send((self.epoch, parents, formed_round)) // Send (epoch, parent_digests, round_N)
                .await
            {
                error!(
                    "[Core][E{}] Failed to send parents for round {} to Proposer: {}",
                    self.epoch, formed_round, e
                );
            }
        }

        // --- Send to Consensus ---
        // Send the certificate to the consensus layer for ordering, unless syncing.
        // Certificates processed during sync mode shouldn't be sent to consensus
        // until sync is complete and normal operation resumes.
        if !syncing {
            let cert_digest = certificate.digest(); // Get digest before moving certificate
            let cert_round = certificate.round(); // Get round before moving certificate
            info!(
                "[Core][E{}] Attempting to send C{} ({}) to consensus.",
                self.epoch, cert_round, cert_digest
            ); // <-- ADD LOG
               // Update commit-watchdog observation window baseline if consensus progressed
            let current_committed = self.consensus_round.load(Ordering::Relaxed) as Round;
            if current_committed > self.last_commit_observed_round {
                self.last_commit_observed_round = current_committed;
                self.last_commit_observed_at = Instant::now();
            }
            if self.tx_consensus.is_closed() {
                // <-- THÊM kiểm tra kênh đóng
                warn!(
                    "[Core][E{}] Consensus channel is closed. Cannot deliver certificate {} (R{})",
                    self.epoch, cert_digest, cert_round
                );
            } else if let Err(e) = self.tx_consensus.send(certificate).await {
                // If the consensus channel is closed, it's a critical issue.
                warn!(
                    "[Core][E{}] Failed to deliver certificate {} (R{}) to consensus: {}",
                    self.epoch,
                    cert_digest,
                    self.dag_round, // Ghi log dag_round tại thời điểm lỗi
                    e
                );
                // Depending on design, might need to panic or attempt recovery.
                // For now, just log the warning.
            } else {
                // Update highest round sent to consensus for watchdog
                if cert_round > self.highest_round_sent_to_consensus {
                    self.highest_round_sent_to_consensus = cert_round;
                }
                trace!(
                    "[Core][E{}] Certificate {} (R{}) sent to consensus. Highest round sent: {}",
                    self.epoch,
                    cert_digest,
                    cert_round,
                    self.highest_round_sent_to_consensus
                );
            }
        } else {
            debug!(
                "[Core][E{}] In sync mode. Skipping sending C{}({}) to consensus.",
                self.epoch,
                certificate.round(),
                certificate.origin()
            );
        }

        Ok(())
    }

    // --- Message Sanitization ---
    // Basic validation checks performed *before* full processing.

    // Validates a received header.
    async fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        // Acquire read lock for committee checks.
        let committee_guard = self.committee.read().await;
        // Check 1: Epoch must match the core's current epoch.
        ensure!(
            header.epoch == self.epoch,
            DagError::InvalidEpoch // Use specific error
        );
        // Check 2: Header round must not be older than the garbage collection round.
        ensure!(
            self.gc_round <= header.round,
            DagError::TooOld(header.id.clone(), header.round)
        );
        // Check 3: Verify header integrity and signature using the committee.
        header.verify(&committee_guard)?; // Propagate verification errors.
                                          // Lock released here
        Ok(())
    }

    // Validates a received vote.
    // NOTE: This function only checks epoch, GC round, and signature.
    // Votes that don't match current_header are NOT rejected here - they will be queued in process_vote.
    async fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        // Acquire read lock for committee checks.
        let committee_guard = self.committee.read().await;
        // Check 1: Epoch must match.
        if vote.epoch != self.epoch {
            warn!(
                "[Core][E{}] Rejecting vote V{}({}) for H{}({}): epoch mismatch (vote epoch {}, current epoch {})",
                self.epoch, vote.round, vote.author, vote.round, vote.origin, vote.epoch, self.epoch
            );
            ensure!(vote.epoch == self.epoch, DagError::InvalidEpoch);
        }
        // Check 2: Vote round must not be older than GC round (but can be older than current_header.round).
        // This allows votes for future rounds to be queued.
        ensure!(
            self.gc_round <= vote.round,
            DagError::TooOld(vote.digest(), vote.round)
        );
        // Check 3: Verify vote signature.
        vote.verify(&committee_guard)?; // Propagate verification errors.
                                        // Lock released here
        
        // Don't check if vote matches current_header here - let process_vote handle queueing.
        debug!(
            "[Core][E{}] Vote V{}({}) for H{}({}) passed sanitization (epoch, GC round, signature). Will check current_header match in process_vote.",
            self.epoch, vote.round, vote.author, vote.round, vote.origin
        );
        Ok(())
    }

    // Validates a received certificate.
    async fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        // Acquire read lock for committee checks.
        let committee_guard = self.committee.read().await;
        // Check 1: Epoch must match.
        ensure!(certificate.epoch() == self.epoch, DagError::InvalidEpoch);
        // Check 2: Certificate round must not be older than the GC round.
        ensure!(
            self.gc_round <= certificate.round(),
            DagError::TooOld(certificate.digest(), certificate.round())
        );
        // Check 3: Verify certificate integrity, header, and quorum signatures.
        // *** THAY ĐỔI: Sử dụng quorum động khi verify certificate ***
        // Ưu tiên verify dựa trên parents trong header; log quorum để quan sát
        let parents_count = certificate.header.parents.len();
        let threshold_preview = if parents_count > 0 {
            committee_guard.quorum_threshold_dynamic(parents_count)
        } else {
            committee_guard.quorum_threshold()
        };
        info!(
            "[Core][E{}] Quorum verify for C{}({}) in R{}: threshold={} (basis: {} parents)",
            self.epoch,
            certificate.round(),
            certificate.origin(),
            certificate.round(),
            threshold_preview,
            parents_count
        );
        certificate.verify_with_active_cert_count(&committee_guard, None)?; // Propagate verification errors.
                                               // Lock released here
        Ok(())
    }

    // Main message handling function, routes messages based on type and current state (Running vs Syncing).
    async fn handle_message(&mut self, message: PrimaryMessage) -> DagResult<()> {
        // --- Handling logic when in Syncing State ---
        if self.sync_state.is_some() {
            // Check epoch mismatch BEFORE processing sync messages - critical for nodes lagging behind
            {
                let committee_guard = self.committee.read().await;
                let committee_epoch = committee_guard.epoch;
                drop(committee_guard);
                
                if committee_epoch > self.epoch {
                    warn!(
                        "[Core][E{}] Detected epoch mismatch during sync message handling: committee at epoch {} but Core still at epoch {}. Aborting sync and resetting state.",
                        self.epoch, committee_epoch, self.epoch
                    );
                    // Clear sync state and reset epoch
                    self.set_sync_state(None).await;
                    self.reset_state_for_new_epoch(committee_epoch);
                    info!("[Core][E{}] State reset complete after epoch mismatch detection during sync.", self.epoch);
                    // Message will be processed in next iteration with new epoch
                    return Ok(());
                }
            }
            
            match message {
                // Primary Message specific to Syncing: Certificate Bundle
                PrimaryMessage::CertificateBundle(certificates) => {
                    // Temporarily take ownership of sync_state to avoid borrow checker issues.
                    let mut state = self.sync_state.take().unwrap(); // Should always exist if we are here.

                    if certificates.is_empty() {
                        warn!("[Core][E{}] Received empty certificate bundle during sync. Will retry.", self.epoch);
                        // No progress made, keep retry count, re-insert state and advance (which will retry).
                    } else {
                        info!(
                            "[Core][E{}] Processing sync bundle of {} certificates (Current dag_round: {}, Target: {}).",
                             self.epoch, certificates.len(), self.dag_round, state.final_target_round
                        );

                        // *** FIX: Filter Certificates by Epoch Only; full verification happens in sanitize_certificate() ***
                        let current_epoch_local = self.epoch; // Capture epoch for closure
                        let verified_certificates: Vec<Certificate> = certificates
                            .into_iter()
                            .filter(|cert| {
                                if cert.epoch() == current_epoch_local {
                                    true
                                } else {
                                    warn!(
                                        "[Core][Sync][E{}] Discarding certificate C{}({}) from wrong epoch {} in sync bundle.",
                                        current_epoch_local, cert.round(), cert.origin(), cert.epoch()
                                    );
                                    false
                                }
                            })
                            .collect();

                        // Process certificates based on sync mode
                        let mut latest_round_in_bundle = self.dag_round; // Track highest round processed from this bundle.
                        if !verified_certificates.is_empty() {
                            let mode_str = match state.mode {
                                SyncMode::StorageSync => "StorageSync",
                                SyncMode::CatchupSync => "CatchupSync",
                            };
                            info!(
                                "[Core][E{}] Processing {} certificates from sync bundle (mode: {}).",
                                self.epoch,
                                verified_certificates.len(),
                                mode_str
                            );
                            for certificate in verified_certificates {
                                match state.mode {
                                    SyncMode::StorageSync => {
                                        // StorageSync: only store/index, verify epoch/round/structure, no quorum check
                                        if certificate.epoch() == self.epoch {
                                            // Verify basic structure and signatures (but not quorum)
                                            // Store certificate
                                            let cert_bytes = bincode::serialize(&certificate)
                                                .expect("Failed to serialize certificate");
                                            self.store.write(certificate.digest().to_vec(), cert_bytes).await;
                                            
                                            // Update round index
                                            let round_key = bincode::serialize(&(self.epoch, certificate.round()))
                                                .expect("Failed to serialize round key");
                                            let mut existing_digests: Vec<Digest> = self
                                                .store
                                                .read_cf(ROUND_INDEX_CF.to_string(), round_key.clone())
                                                .await
                                                .ok()
                                                .flatten()
                                                .and_then(|b| bincode::deserialize(&b).ok())
                                                .unwrap_or_default();
                                            if !existing_digests.contains(&certificate.digest()) {
                                                existing_digests.push(certificate.digest());
                                                let digests_bytes = bincode::serialize(&existing_digests)
                                                    .expect("Failed to serialize digest list");
                                                let _ = self.store.write_cf(ROUND_INDEX_CF.to_string(), round_key, digests_bytes).await;
                                            }
                                            
                                            // Update latest round
                                            latest_round_in_bundle = latest_round_in_bundle.max(certificate.round());
                                            debug!(
                                                "[Core][StorageSync][E{}] Stored certificate C{}({}) without quorum verification",
                                                self.epoch, certificate.round(), certificate.origin()
                                            );
                                        }
                                    }
                                    SyncMode::CatchupSync => {
                                        // CatchupSync: full verification + normal processing
                                        if self.sanitize_certificate(&certificate).await.is_ok() {
                                            if self.process_certificate(certificate.clone(), true).await.is_ok() {
                                                latest_round_in_bundle = latest_round_in_bundle.max(certificate.round());
                                            } else {
                                                warn!("[Core][CatchupSync][E{}] Failed to process certificate C{}({})", 
                                                    self.epoch, certificate.round(), certificate.origin());
                                            }
                                        } else {
                                            warn!("[Core][CatchupSync][E{}] Certificate C{}({}) failed sanitization", 
                                                self.epoch, certificate.round(), certificate.origin());
                                        }
                                    }
                                }
                            }
                            // Update the core's main `dag_round` only if the bundle contained newer rounds.
                            if latest_round_in_bundle > self.dag_round {
                                debug!("[Core][E{}] Sync bundle processed. Advanced dag_round from {} to {}.", self.epoch, self.dag_round, latest_round_in_bundle);
                                self.dag_round = latest_round_in_bundle;
                            } else {
                                debug!(
                                    "[Core][E{}] Sync bundle processed. dag_round remains at {}.",
                                    self.epoch, self.dag_round
                                );
                            }
                            // Reset retry count as progress was made.
                            state.retry_count = 0;
                        } else {
                            warn!("[Core][E{}] Sync bundle contained no valid certificates for the current epoch.", self.epoch);
                            // Do not reset retry count, as no progress was made.
                        }
                    }
                    // Put sync_state back and potentially request the next chunk.
                    self.set_sync_state(Some(state)).await;
                    self.advance_sync().await; // Check completion or request next chunk/retry.
                }

                // Handling individual Certificates received during Syncing
                PrimaryMessage::Certificate(certificate) => {
                    // Check if this certificate could potentially advance our sync target.
                    let target_round = self.sync_state.as_ref().map_or(0, |s| s.final_target_round);
                    let cert_round = certificate.round();
                    let cert_epoch = certificate.epoch();

                    // If cert is from the current epoch and significantly newer than target...
                    if cert_epoch == self.epoch && cert_round > target_round + LAG_THRESHOLD {
                        // Validate the certificate briefly before potentially updating target.
                        // Avoid full processing, just basic checks.
                        if self.sanitize_certificate(&certificate).await.is_ok() {
                            info!(
                                "[Core][Sync][E{}] Received certificate C{} significantly newer than sync target {}. Updating target.",
                                 self.epoch, cert_round, target_round
                            );
                            // Update the final target round in the sync state.
                            if let Some(state) = self.sync_state.as_mut() {
                                state.final_target_round = cert_round;
                                // Reset retry count? Maybe not, let current chunk finish first.
                            }
                        } else {
                            warn!("[Core][Sync][E{}] Received potential future certificate C{}, but it failed sanitization.", self.epoch, cert_round);
                        }
                    } else if cert_epoch == self.epoch && cert_round > self.dag_round {
                        // If cert is for current epoch and newer than dag_round, but not drastically so,
                        // process it in sync mode. This helps fill gaps while waiting for bundles.
                        if self.sanitize_certificate(&certificate).await.is_ok() {
                            debug!("[Core][Sync][E{}] Processing individual certificate C{}({}) received during sync.", self.epoch, cert_round, certificate.origin());
                            let _ = self.process_certificate(certificate, true).await;
                            // Ignore result? Log error?
                            // Do *not* advance sync here, let bundle processing drive progress.
                        }
                    } else {
                        trace!(
                            "[Core][Sync][E{}] Ignoring certificate C{}({}) (epoch {}, round {}) during sync.",
                            self.epoch, certificate.round(), certificate.origin(), cert_epoch, cert_round
                        );
                    }
                }

                // Ignore other message types while syncing.
                _ => {
                    trace!(
                        "[Core][Sync][E{}] Ignoring message {:?} while in Syncing state.",
                        self.epoch,
                        message
                    );
                }
            }
            return Ok(()); // Return after handling message in sync state.
        }

        // --- Handling logic when in Running State ---
        // (Không cần cờ epoch_transitioning ở đây nữa)

        match message {
            // Received Header from peer or loopback (after dependencies met)
            PrimaryMessage::Header(header) => {
                info!(
                    "[Core][E{}] Received header message H{}({}) from network (header epoch: {})",
                    self.epoch, header.round, header.author, header.epoch
                );
                // Sanitize first (checks epoch, GC round, signature).
                match self.sanitize_header(&header).await {
                    Ok(_) => {
                        // Full processing (checks parents, payload, votes).
                        // IMPORTANT: Only suspend voting if epoch_upgrade_pending (different epoch).
                        // If just syncing in same epoch, still vote to ensure progress.
                        let suspend = self.epoch_upgrade_pending.is_some();
                        if suspend {
                            warn!(
                                "[Core][E{}] Processing header H{}({}) but suspending vote due to epoch_upgrade_pending",
                                self.epoch, header.round, header.author
                            );
                        } else if self.sync_state.is_some() {
                            info!(
                                "[Core][E{}] Processing header H{}({}) while syncing - will still vote if dependencies met",
                                self.epoch, header.round, header.author
                            );
                        }
                        self.process_header(&header, suspend).await
                    }
                    Err(e) => {
                        warn!(
                            "[Core][E{}] Header H{}({}) failed sanitization: {}",
                            self.epoch, header.round, header.author, e
                        );
                        // Nếu header có epoch mới hơn, chủ động yêu cầu committee để bắt kịp epoch
                        if header.epoch > self.epoch {
                            // Mark that an epoch upgrade is pending; suspend voting until reconfigure
                            self.epoch_upgrade_pending = Some(header.epoch);
                            let committee_guard = self.committee.read().await;
                            if let Ok(addresses) = committee_guard.primary(&header.author) {
                                let req = PrimaryMessage::CommitteeRequest { requestor: self.name.clone() };
                                if let Ok(bytes) = bincode::serialize(&req) {
                                    let _ = self.network.send(addresses.primary_to_primary, Bytes::from(bytes)).await;
                                    info!(
                                        "[Core][E{}] Detected higher-epoch header ({}> {}). Requested Committee from {} at {}",
                                        self.epoch, header.epoch, self.epoch, header.author, addresses.primary_to_primary
                                    );
                                }
                            }
                        }
                        Err(e)
                    }
                }
            }
            // Received Vote from peer
            PrimaryMessage::Vote(vote) => {
                info!(
                    "[Core][E{}] Received vote message V{}({}) for H{}({}) from network",
                    self.epoch, vote.round, vote.author, vote.round, vote.origin
                );
                // Sanitize first (checks epoch, GC round, signature - does NOT check current_header match).
                match self.sanitize_vote(&vote).await {
                    Ok(_) => {
                        // Full processing (aggregates votes, potentially creates/processes certificate).
                        self.process_vote(vote).await
                    }
                    Err(e) => {
                        warn!(
                            "[Core][E{}] Vote V{}({}) for H{}({}) failed sanitization: {}",
                            self.epoch, vote.round, vote.author, vote.round, vote.origin, e
                        );
                        Err(e)
                    }
                }
            }
            // Received Certificate from peer or loopback (after dependencies met)
            PrimaryMessage::Certificate(certificate) => {
                // *** FIX: Lagging Check only considers certificates from the CURRENT epoch ***
                if certificate.epoch() == self.epoch
                    && certificate.round() > self.dag_round.saturating_add(LAG_THRESHOLD)
                {
                    // Certificate is significantly ahead, likely lagging. Enter Syncing state.
                    info!(
                        "[Core][E{}] Detected lag: Received C{} while at dag_round {}. Switching to Syncing state to catch up.",
                         self.epoch, certificate.round(), self.dag_round
                    );
                    // Sanitize the certificate *before* using its round as the target.
                    self.sanitize_certificate(&certificate).await?; // Ensure it's valid first
                                                                    // Initialize sync state.
                    let target_round = certificate.round();
                    let gap = target_round.saturating_sub(self.dag_round);
                    let sync_mode = if gap > self.gc_depth {
                        SyncMode::StorageSync
                    } else {
                        SyncMode::CatchupSync
                    };
                    info!(
                        "[Core][E{}] Initializing sync: target_round={}, dag_round={}, gap={}, mode={:?}",
                        self.epoch, target_round, self.dag_round, gap, sync_mode
                    );
                    self.set_sync_state(Some(SyncState {
                        mode: sync_mode,
                        final_target_round: target_round, // Target the round of the future certificate.
                        current_chunk_target: 0,                 // Will be set by advance_sync.
                        retry_count: 0,
                        last_request_time: Instant::now(), // Initialize timer
                    })).await;
                    // Immediately request the first chunk.
                    self.advance_sync().await;
                    // We are now in Syncing state, return.
                    return Ok(());
                } else {
                    // Certificate is not significantly ahead, or from wrong epoch. Sanitize and process normally.
                    self.sanitize_certificate(&certificate).await?;
                    // Update dag_round immediately upon receiving a valid certificate for the current epoch.
                    if certificate.epoch() == self.epoch {
                        self.dag_round = self.dag_round.max(certificate.round());
                    }
                    // Full processing (stores cert, updates round index, aggregates for parents, sends to consensus).
                    self.process_certificate(certificate, false).await?;
                }
                Ok(())
            }
            // Handle HeaderRequest: peer asks for specific (epoch, round, author) header
            PrimaryMessage::HeaderRequest { round, epoch, author, requestor } => {
                info!(
                    "[Core][E{}] Received HeaderRequest for H{}({}) epoch {} from {}",
                    self.epoch, round, author, epoch, requestor
                );
                if epoch != self.epoch {
                    debug!(
                        "[Core][E{}] Ignoring HeaderRequest for epoch {} (current epoch {})",
                        self.epoch, epoch, self.epoch
                    );
                    return Ok(());
                }
                // Lookup header id by (epoch, round, author)
                let key = bincode::serialize(&(epoch, round, author.clone()))
                    .expect("serialize author_round index key");
                if let Ok(Some(id_bytes)) = self.store.read_cf(AUTHOR_ROUND_CF.to_string(), key).await {
                    if let Ok(header_id) = bincode::deserialize::<Digest>(&id_bytes) {
                        if let Ok(Some(header_bytes)) = self.store.read(header_id.to_vec()).await {
                            if let Ok(header) = bincode::deserialize::<Header>(&header_bytes) {
                                // Send back the header to requester
                                let committee_guard = self.committee.read().await;
                                if let Ok(addresses) = committee_guard.primary(&requestor) {
                                    let msg = PrimaryMessage::Header(header);
                                    let bytes = bincode::serialize(&msg)
                                        .expect("serialize header to respond HeaderRequest");
                                    let _ = self.network.send(addresses.primary_to_primary, Bytes::from(bytes)).await;
                                    info!(
                                        "[Core][E{}] Responded HeaderRequest with H{}({}) to {}",
                                        self.epoch, round, author, addresses.primary_to_primary
                                    );
                                }
                            }
                        }
                    }
                } else {
                    debug!(
                        "[Core][E{}] HeaderRequest: no index for H{}({}) epoch {}",
                        self.epoch, round, author, epoch
                    );
                }
                Ok(())
            }
            // Reply with current committee to help lagging peers catch up epoch
            PrimaryMessage::CommitteeRequest { requestor } => {
                let committee_guard = self.committee.read().await;
                if let Ok(addresses) = committee_guard.primary(&requestor) {
                    let msg = PrimaryMessage::Reconfigure(committee_guard.clone());
                    if let Ok(bytes) = bincode::serialize(&msg) {
                        let _ = self.network.send(addresses.primary_to_primary, Bytes::from(bytes)).await;
                        info!(
                            "[Core][E{}] Responded CommitteeRequest: sent Reconfigure(committee) to {} at {}",
                            self.epoch, requestor, addresses.primary_to_primary
                        );
                    }
                }
                Ok(())
            }
            // Received VoteNudge from peer: try to vote again for the specified header if conditions match
            PrimaryMessage::VoteNudge { epoch, round, header_id, origin } => {
                // Chỉ xử lý nếu cùng epoch và không bị lag quá xa
                if epoch != self.epoch {
                    debug!("[Core][E{}] Ignoring VoteNudge for different epoch {}.", self.epoch, epoch);
                    return Ok(());
                }
                if round + self.gc_depth < self.dag_round.saturating_sub(self.gc_depth) {
                    debug!("[Core][E{}] Ignoring VoteNudge R{} too old for dag_round {}.", self.epoch, round, self.dag_round);
                    return Ok(());
                }

                // Tìm header trong store theo header_id; nếu có thì process_header để tạo vote nếu phù hợp
                if let Ok(Some(bytes)) = self.store.read(header_id.to_vec()).await {
                    if let Ok(header) = bincode::deserialize::<Header>(&bytes) {
                        if header.epoch == self.epoch && header.round == round && header.author == origin {
                            info!(
                                "[Core][E{}] Received VoteNudge for H{}({}). Attempting to process and vote.",
                                self.epoch, round, origin
                            );
                            // process_header sẽ tự thực hiện dependency checks và gửi vote nếu không ở syncing
                            let _ = self.process_header(&header, false).await;
                        } else {
                            debug!(
                                "[Core][E{}] VoteNudge header meta mismatch. Ignoring.",
                                self.epoch
                            );
                        }
                    }
                } else {
                    debug!(
                        "[Core][E{}] VoteNudge header {} not found locally. Ignoring.",
                        self.epoch, header_id
                    );
                }
                Ok(())
            }

            // Ignore Reconfigure messages received directly from peers. Reconfiguration is driven internally.
            PrimaryMessage::Reconfigure(_) => {
                info!(
                    "[Core][E{}] Ignoring external Reconfigure message.",
                    self.epoch
                );
                Ok(())
            }
            // Accept Certificate Bundles in Running state if they are narrowly scoped to current epoch/rounds
            PrimaryMessage::CertificateBundle(certificates) => {
                if certificates.is_empty() {
                    warn!(
                        "[Core][E{}] Received empty CertificateBundle in Running state. Ignoring.",
                        self.epoch
                    );
                    return Ok(());
                }

                // Define a narrow acceptance window around our current dag_round to help liveness
                let current_epoch_local = self.epoch;
                let min_round = self.dag_round.saturating_sub(self.gc_depth);
                let max_round = self.dag_round.saturating_add(5);

                // Verify and filter bundle (epoch matches, round in window) without blocking the runtime
                let (accepted, rejected): (Vec<_>, Vec<_>) = {
                    let committee_guard = self.committee.read().await;
                    certificates
                        .into_iter()
                        .partition(|cert| {
                            let ok_epoch = cert.epoch() == current_epoch_local;
                            let r = cert.round();
                            let ok_round = r >= min_round && r <= max_round;
                            ok_epoch && ok_round && cert.verify(&committee_guard).is_ok()
                        })
                };

                if !rejected.is_empty() {
                    debug!(
                        "[Core][E{}] Rejected {} certificate(s) from bundle (epoch/round window/verify mismatch).",
                        self.epoch,
                        rejected.len()
                    );
                }

                if accepted.is_empty() {
                    debug!(
                        "[Core][E{}] No acceptable certificates in bundle for window [{}, {}] at dag_round {}.",
                        self.epoch,
                        min_round,
                        max_round,
                        self.dag_round
                    );
                    return Ok(());
                }

                info!(
                    "[Core][E{}] Processing {} certificate(s) from bundle in Running state (window [{}, {}], dag_round {}).",
                    self.epoch,
                    accepted.len(),
                    min_round,
                    max_round,
                    self.dag_round
                );

                // Process accepted certificates using syncing=true to avoid extra side-effects
                for certificate in accepted {
                    let _ = self.process_certificate(certificate, true).await;
                }

                Ok(())
            }
            // Certificate Request messages are handled by Helper, not Core.
            PrimaryMessage::CertificatesRequest(_, _)
            | PrimaryMessage::CertificateRangeRequest { .. } => {
                warn!("[Core][E{}] Received Certificate Request message unexpectedly. Should be handled by Helper. Ignoring.", self.epoch);
                Ok(())
            }
        }
    }

    // Resets the internal state of Core for a new epoch.
    fn reset_state_for_new_epoch(&mut self, new_epoch: Epoch) {
        info!(
            "[Core] Detected epoch change from {} to {}. Resetting internal state.",
            self.epoch, new_epoch
        );

        // Update internal epoch tracker.
        self.epoch = new_epoch;
        // Reset round counters.
        self.gc_round = 0;
        self.dag_round = 0; // Crucial: Reset DAG round tracker.

        // Clear state maps.
        self.last_voted.clear();
        self.processing.clear();
        self.certificates_aggregators.clear();
        self.cancel_handlers.clear(); // Cancel pending network requests? Maybe let them timeout. Clearing handlers is safer.
        self.pending_votes.clear(); // Clear pending votes from old epoch
        self.authors_seen_per_round.clear(); // Clear quorum tracking
        self.headers_seen_per_round.clear(); // Clear header tracking
        
        // *** LƯU Ý: Khi bắt đầu epoch mới, authors_seen_per_round sẽ rỗng ***
        // Khi tính quorum động cho round 1 (prev_round = 0), active_cert_count = 0
        // → Sẽ fallback về quorum cố định từ committee (đúng!)
        // Genesis certificates sẽ được track khi chúng được process qua process_certificate()
        
        self.round_stuck_since.clear(); // Clear stuck round tracking
        self.last_proactive_request.clear(); // Clear proactive request tracking
        self.last_commit_observed_round = 0; // Reset consensus commit tracking
        self.last_commit_observed_at = Instant::now(); // Reset timestamp
        self.highest_round_sent_to_consensus = 0; // Reset highest round sent to consensus
        self.last_suspicious_epoch_sync = None; // Reset epoch-start sync guard
        self.epoch_upgrade_pending = None; // Clear epoch upgrade pending

        // Reset current header and aggregators.
        self.current_header = Header::default();
        self.votes_aggregator = VotesAggregator::new();

        // Ensure we exit sync state if we were syncing during transition.
        self.sync_state = None;

        // Note: `consensus_round` (Arc<AtomicU64>) is reset by GarbageCollector.
        // Note: `committee` (Arc<RwLock<Committee>>) is updated by the main node loop.

        info!(
            "[Core] State reset complete for epoch {}. Ready.",
            new_epoch
        );
    }

    // Main execution loop for the Core task.
    pub async fn run(&mut self) {
        // Timer for sync retries.
        let mut sync_check_interval = interval(Duration::from_millis(SYNC_RETRY_DELAY / 2)); // Check more frequently
        sync_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Quorum & consensus liveness watchdog interval (only when not syncing)
        let mut quorum_check_interval = interval(Duration::from_millis(1_000));
        quorum_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!("[Core][E{}] Starting main loop.", self.epoch);
        loop {
            // Select the next event. Prioritize reconfiguration.
            let result = tokio::select! {
                biased; // Prioritize reconfigure channel

                // Handle Reconfiguration Signal
                result = self.rx_reconfigure.recv() => {
                    match result {
                        Ok(notification) => {
                             // --- Anti-Race Condition Logic for Core ---
                             // Accept reconfigure if notification epoch >= current epoch
                             // This allows nodes that missed previous epochs to catch up
                            if notification.committee.epoch < self.epoch {
                                warn!(
                                    "[Core][E{}] Ignoring stale reconfigure signal for epoch {} (current is {})",
                                    self.epoch, notification.committee.epoch, self.epoch
                                );
                                Ok(())
                            } else {
                                // Wait for the shared Committee Arc to be updated.
                                let target_epoch = notification.committee.epoch;
                                info!("[Core][E{}] Received reconfigure signal for epoch {}. Waiting for committee update...", self.epoch, target_epoch);
                                let mut updated_committee_epoch = self.epoch;
                                let mut wait_count = 0;
                                const MAX_WAIT_COUNT: u32 = 100; // Wait up to 10 seconds
                                while updated_committee_epoch < target_epoch && wait_count < MAX_WAIT_COUNT {
                                    sleep(Duration::from_millis(100)).await;
                                    let committee_guard = self.committee.read().await;
                                    updated_committee_epoch = committee_guard.epoch;
                                    drop(committee_guard);
                                    wait_count += 1;
                                }
                                
                                if updated_committee_epoch < target_epoch {
                                    warn!(
                                        "[Core][E{}] Committee Arc not updated to epoch {} after waiting (current: {}). May need manual intervention.",
                                        self.epoch, target_epoch, updated_committee_epoch
                                    );
                                } else {
                                    info!("[Core][E{}] Committee Arc updated to epoch {}. Resetting internal state.", self.epoch, updated_committee_epoch);
                                    // Perform the state reset for the new epoch.
                                    self.reset_state_for_new_epoch(updated_committee_epoch);
                                    info!("[Core][E{}] State reset for new epoch complete.", self.epoch);
                                }
                                Ok(()) // Continue loop after handling signal
                            }
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("[Core][E{}] Reconfigure receiver lagged by {} messages. Missed epoch transitions! Checking current committee epoch...", self.epoch, n);
                            // Check if committee has been updated to a newer epoch
                            let committee_guard = self.committee.read().await;
                            let current_committee_epoch = committee_guard.epoch;
                            drop(committee_guard);
                            
                            if current_committee_epoch > self.epoch {
                                info!("[Core][E{}] Detected committee at epoch {} after lag. Resetting state.", self.epoch, current_committee_epoch);
                                self.reset_state_for_new_epoch(current_committee_epoch);
                                info!("[Core][E{}] State reset complete after lag recovery.", self.epoch);
                            } else {
                                warn!("[Core][E{}] Committee still at epoch {} after lag. May need manual sync.", self.epoch, current_committee_epoch);
                            }
                            Ok(())
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            error!("[Core] Reconfigure channel closed unexpectedly. Shutting down.");
                            break; // Exit main loop
                        }
                    }
                },

                // *** THAY ĐỔI BẮT ĐẦU: Sửa lỗi borrow ***
                // Handle messages from other primaries (Headers, Votes, Certificates)
                // Receive message first, then handle based on sync_state.
                Some(message) = self.rx_primaries.recv() => {
                    self.handle_message(message).await // handle_message already checks sync_state internally
                },
                // *** THAY ĐỔI KẾT THÚC ***


                // Handle headers returned by HeaderWaiter (dependencies met)
                 // Chỉ xử lý nếu không syncing.
                Some(header) = self.rx_header_waiter.recv(), if self.sync_state.is_none() => {
                    // Sanitize checks epoch/GC round. Process checks parents/payload/votes.
                    match self.sanitize_header(&header).await {
                        Ok(_) => self.process_header(&header, false).await,
                        Err(e) => {
                            warn!("[Core] Header from waiter failed sanitization: {}", e);
                            Ok(()) // Ignore invalid header
                        }
                    }
                },

                // --- Quorum watchdog & Consensus liveness watchdog (only when not syncing) ---
                _ = quorum_check_interval.tick(), if self.sync_state.is_none() => {
                    // 0) Epoch mismatch check: detect if committee has been updated but Core hasn't reset
                    let committee_guard = self.committee.read().await;
                    let committee_epoch = committee_guard.epoch;
                    drop(committee_guard);
                    
                    if committee_epoch > self.epoch {
                        warn!(
                            "[Core][E{}] Detected epoch mismatch: committee at epoch {} but Core still at epoch {}. Auto-resetting state.",
                            self.epoch, committee_epoch, self.epoch
                        );
                        self.reset_state_for_new_epoch(committee_epoch);
                        info!("[Core][E{}] State reset complete after epoch mismatch detection.", self.epoch);
                        Ok(()) // Skip rest of watchdog this iteration
                    } else {
                        // 1) Quorum watchdog: detect rounds lagging quorum
                        let committee_guard = self.committee.read().await;
                        let expected_authors: HashSet<PublicKey> = committee_guard
                            .others_primaries(&self.name)
                            .iter()
                            .map(|(name, _)| name.clone())
                            .chain(std::iter::once(self.name.clone()))
                            .collect();
                        drop(committee_guard);

                    let r = self.dag_round;
                    // Skip quorum check if dag_round is from previous epoch (should be reset but wasn't)
                    // This happens when epoch transition occurs but dag_round wasn't properly reset
                    // Typical symptom: dag_round is very high (e.g., 100) but we're at a low round in new epoch
            if r > 0 {
                // *** THAY ĐỔI BẮT ĐẦU: Backup leader mechanism cho round RECONFIGURE_INTERVAL ***
                // Kiểm tra xem có phải round RECONFIGURE_INTERVAL không và leader có fail không
                // NOTE: Consensus chỉ commit leader certificate cụ thể, nên backup leader không thể tạo header thay thế
                // Thay vào đó, chúng ta sẽ khuyến khích leader tạo header bằng cách gửi VoteNudge
                if r == RECONFIGURE_INTERVAL {
                    let committee_guard = self.committee.read().await;
                    let mut keys: Vec<_> = committee_guard.authorities.keys().cloned().collect();
                    keys.sort();
                    let leader_index = r as usize % committee_guard.size();
                    let leader_pk = keys[leader_index].clone();
                    
                    // Kiểm tra xem có leader certificate không
                    let has_leader_cert = self.authors_seen_per_round
                        .get(&r)
                        .map(|authors| authors.contains(&leader_pk))
                        .unwrap_or(false);
                    
                    // Nếu leader fail và chúng ta không phải leader, gửi VoteNudge đến leader để khuyến khích tạo header
                    if !has_leader_cert && leader_pk != self.name && self.sync_state.is_none() {
                        if let Ok(addrs) = committee_guard.primary(&leader_pk) {
                            warn!(
                                "[Core][E{}] Leader {} of round {} (RECONFIGURE_INTERVAL) may have failed (no certificate seen). Sending VoteNudge to encourage header creation.",
                                self.epoch, leader_pk, r
                            );
                            // Gửi VoteNudge với round và epoch để signal cho leader rằng cần tạo header
                            // Sử dụng một header_id giả để signal "need header"
                            let nudge_msg = PrimaryMessage::VoteNudge {
                                header_id: Digest::default(), // Empty digest to signal "need header"
                                round: r,
                                epoch: self.epoch,
                                origin: leader_pk.clone(),
                            };
                            let nudge_bytes = bincode::serialize(&nudge_msg)
                                .expect("Failed to serialize VoteNudge");
                            let _ = self.network.send(addrs.primary_to_primary, Bytes::from(nudge_bytes)).await;
                        }
                    }
                    drop(committee_guard);
                }
                // *** THAY ĐỔI KẾT THÚC ***
                
                // Check if dag_round seems suspicious (very high round suggests old epoch)
                // Normal rounds in new epoch should be small initially (1, 2, 3, ...)
                // If dag_round is > 50 and we just started new epoch, it's likely stale
                let committee_guard = self.committee.read().await;
                let is_suspicious_dag_round = r > 50 && self.epoch > 0; // Simple heuristic
                drop(committee_guard);
                        
                        if is_suspicious_dag_round {
                            // Chỉ coi là đáng ngờ trong giai đoạn rất sớm sau epoch switch
                            let just_after_epoch_switch = self.last_commit_observed_round == 0
                                && self.last_commit_observed_at.elapsed() <= Duration::from_secs(10);

                            if just_after_epoch_switch {
                                // Tôn trọng cooldown để tránh spam
                                let can_sync_now = self
                                    .last_suspicious_epoch_sync
                                    .map(|t| t.elapsed() >= Duration::from_millis(self.suspicious_epoch_sync_cooldown_ms))
                                    .unwrap_or(true);
                                if can_sync_now {
                                    info!(
                                        "[Core][E{}] Quorum watchdog: dag_round={} looks high at epoch start. Triggering one small sync [1..=5] (cooldown {}ms) and skipping this check.",
                                        self.epoch, r, self.suspicious_epoch_sync_cooldown_ms
                                    );
                                    self.request_sync_chunk(1, 5, false).await; // Small sync, use CatchupSync
                                    self.last_suspicious_epoch_sync = Some(Instant::now());
                                } else {
                                    debug!(
                                        "[Core][E{}] Epoch-start small sync on cooldown. Skipping duplicate request.",
                                        self.epoch
                                    );
                                }
                                continue; // Skip this quorum check iteration
                            }
                            // Nếu không còn là giai đoạn đầu epoch, không coi là bất thường nữa.
                        }
                        
                        // Check if we're lagging behind significantly based on headers/certificates seen from other nodes
                        // If we see headers/certificates from rounds much higher than dag_round, we should sync
                        let max_round_seen = self.headers_seen_per_round
                            .keys()
                            .chain(self.authors_seen_per_round.keys())
                            .max()
                            .copied()
                            .unwrap_or(r);
                        
                        if max_round_seen > r.saturating_add(LAG_THRESHOLD) {
                            warn!(
                                "[Core][E{}] Lag detection: max_round_seen={} >> dag_round={} (diff: {}). Entering syncing to catch up.",
                                self.epoch, max_round_seen, r, max_round_seen.saturating_sub(r)
                            );
                            let target = max_round_seen.saturating_add(5);
                            let gap = target.saturating_sub(r);
                            let sync_mode = if gap > self.gc_depth {
                                SyncMode::StorageSync
                            } else {
                                SyncMode::CatchupSync
                            };
                            info!(
                                "[Core][E{}] Initializing sync from watchdog: target={}, dag_round={}, gap={}, mode={:?}",
                                self.epoch, target, r, gap, sync_mode
                            );
                            self.set_sync_state(Some(SyncState {
                                mode: sync_mode,
                                final_target_round: target,
                                current_chunk_target: 0,
                                retry_count: 0,
                                last_request_time: Instant::now(),
                            })).await;
                            self.advance_sync().await;
                            continue; // Skip quorum check for this iteration
                        }
                        
                        // Calculate dynamic quorum based on previous round's active certificates
                        let prev_round = r.saturating_sub(1);
                        
                        // Read cert_seen and headers_seen first
                        let cert_seen = self
                            .authors_seen_per_round
                            .get(&r)
                            .cloned()
                            .unwrap_or_default();
                        let headers_seen = self
                            .headers_seen_per_round
                            .get(&r)
                            .cloned()
                            .unwrap_or_default();
                        
                        // Calculate dynamic quorum and current certificate stake
                        let (active_cert_count, dynamic_quorum_required, current_cert_stake) = {
                            let committee_guard = self.committee.read().await;
                            let acc = if prev_round == 0 {
                                committee_guard.authorities.len()
                            } else {
                                self.authors_seen_per_round
                                    .get(&prev_round)
                                    .map(|s| s.len())
                                    .unwrap_or(0)
                            };
                            let dq = if acc > 0 {
                                committee_guard.quorum_threshold_dynamic(acc)
                            } else {
                                committee_guard.quorum_threshold()
                            };
                            // Calculate current certificate stake
                            let current_stake: Stake = cert_seen
                                .iter()
                                .map(|author| committee_guard.stake(author))
                                .sum();
                            drop(committee_guard);
                            (acc, dq, current_stake)
                        };
                        
                        // Check if we have enough certificates for dynamic quorum
                        // Instead of checking if cert_seen.len() < expected_authors.len()
                        // We check if current_cert_stake < dynamic_quorum_required
                        if current_cert_stake < dynamic_quorum_required {
                            // Round is missing certificates to reach dynamic quorum - track stuck time
                            let missing_certs: Vec<PublicKey> = expected_authors
                                .difference(&cert_seen)
                                .cloned()
                                .collect();
                            
                            // Check which missing authors have headers but no certificates yet
                            let missing_with_headers: Vec<PublicKey> = missing_certs
                                .iter()
                                .filter(|author| headers_seen.contains(*author))
                                .cloned()
                                .collect();
                            let missing_without_headers: Vec<PublicKey> = missing_certs
                                .iter()
                                .filter(|author| !headers_seen.contains(*author))
                                .cloned()
                                .collect();

                            // Record when this round first became stuck (if not already recorded)
                            let now = Instant::now();
                            let is_newly_stuck = !self.round_stuck_since.contains_key(&r);
                            if is_newly_stuck {
                                info!(
                                    "[Core][E{}] Quorum watchdog: R{} missing certificates to reach dynamic quorum (have {} certs with stake {}, need stake {} based on {} active certs in R{}). {} author(s) have headers but no cert yet: {:?}. {} author(s) missing headers: {:?}",
                                    self.epoch, r, cert_seen.len(), current_cert_stake, dynamic_quorum_required, active_cert_count, prev_round,
                                    missing_with_headers.len(), missing_with_headers,
                                    missing_without_headers.len(), missing_without_headers
                                );
                                self.round_stuck_since.insert(r, now);
                            }

                            // Only trigger proactive actions after timeout and cooldown
                            if let Some(stuck_since) = self.round_stuck_since.get(&r) {
                                let elapsed = stuck_since.elapsed();
                                let last_request = self.last_proactive_request.get(&r);

                                // Check both timeout and cooldown
                                let timeout_expired = elapsed >= Duration::from_millis(self.quorum_timeout_ms);
                                let cooldown_expired = last_request
                                    .map(|t| t.elapsed() >= Duration::from_millis(self.proactive_request_cooldown_ms))
                                    .unwrap_or(true); // If no previous request, allow it

                                if timeout_expired && cooldown_expired {
                                    warn!(
                                        "[Core][E{}] Quorum watchdog: R{} stuck for {:?} (>{}ms). Missing certificates to reach dynamic quorum (have {} certs with stake {}, need stake {} based on {} active certs in R{}). {} with headers but no cert: {:?}. {} missing headers: {:?}. Triggering proactive assistance.",
                                        self.epoch, r, elapsed, self.quorum_timeout_ms, 
                                        cert_seen.len(), current_cert_stake, dynamic_quorum_required, active_cert_count, prev_round,
                                        missing_with_headers.len(), missing_with_headers,
                                        missing_without_headers.len(), missing_without_headers
                                    );

                                    // 1) Request a small range sync for this round (to all peers)
                                    self.request_sync_chunk(r, r, false).await; // Small sync, use CatchupSync

                                    // 1.1) Request missing headers explicitly from authors lacking headers
                                    if !missing_without_headers.is_empty() {
                                        let committee_guard = self.committee.read().await;
                                        let targets: Vec<(PublicKey, SocketAddr)> = missing_without_headers
                                            .iter()
                                            .filter_map(|a| committee_guard.primary(a).ok().map(|addr| (a.clone(), addr.primary_to_primary)))
                                            .collect();
                                        drop(committee_guard);
                                        if !targets.is_empty() {
                                            info!(
                                                "[Core][E{}] Quorum watchdog: requesting missing headers for R{} from {} author(s) missing headers: {:?}",
                                                self.epoch, r, targets.len(), targets.iter().map(|(pk, _)| pk).collect::<Vec<&PublicKey>>()
                                            );
                                            let epoch_local = self.epoch;
                                            let requestor_local = self.name.clone();
                                            for (auth, addr) in targets {
                                                let req = PrimaryMessage::HeaderRequest {
                                                    round: r,
                                                    epoch: epoch_local,
                                                    author: auth.clone(),
                                                    requestor: requestor_local.clone(),
                                                };
                                                if let Ok(bytes) = bincode::serialize(&req) {
                                                    info!(
                                                        "[Core][E{}] Quorum watchdog: sending HeaderRequest for H{}({}) R{} to {} at {}",
                                                        self.epoch, r, auth, r, auth, addr
                                                    );
                                                    let _ = self.network.send(addr, Bytes::from(bytes)).await;
                                                } else {
                                                    warn!(
                                                        "[Core][E{}] Quorum watchdog: failed to serialize HeaderRequest for H{}({}) R{}",
                                                        self.epoch, r, auth, r
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    // 2) Proactively request certificates from missing authors specifically
                                    let committee_guard = self.committee.read().await;
                                    let missing_authors_with_addresses: Vec<(PublicKey, SocketAddr)> = missing_certs
                                        .iter()
                                        .filter_map(|missing_author| {
                                            committee_guard
                                                .primary(missing_author)
                                                .ok()
                                                .map(|addrs| (missing_author.clone(), addrs.primary_to_primary))
                                        })
                                        .collect();
                                    drop(committee_guard);

                                    if !missing_authors_with_addresses.is_empty() {
                                        debug!(
                                            "[Core][E{}] Quorum watchdog: requesting certificates for R{} from {} missing author(s)",
                                            self.epoch, r, missing_authors_with_addresses.len()
                                        );
                                        // Send CertificateRangeRequest directly to missing authors
                                        let range_request = PrimaryMessage::CertificateRangeRequest {
                                            start_round: r,
                                            end_round: r,
                                            requestor: self.name.clone(),
                                            from_storage: false, // Watchdog sync, use CatchupSync
                                        };
                                        let bytes = bincode::serialize(&range_request)
                                            .expect("Failed to serialize cert range request");
                                        for (_missing_author, address) in missing_authors_with_addresses {
                                            let _ = self.network.send(address, Bytes::from(bytes.clone())).await;
                                        }
                                    }

                                    // 3) Re-broadcast own header cho đúng peer còn thiếu và chỉ khi không bị lag quá nhiều
                                    // Điều kiện: current_header là H_r của chính node, không ở Syncing, và r nằm gần dag_round
                                    if self.sync_state.is_none()
                                        && self.current_header.round == r
                                        && self.current_header.author == self.name
                                        && r >= self.dag_round.saturating_sub(self.gc_depth)
                                        && r <= self.dag_round.saturating_add(5)
                                    {
                                        let missing_authors_with_addresses: Vec<(PublicKey, SocketAddr)> = {
                                            let committee_guard = self.committee.read().await;
                                            missing_certs
                                                .iter()
                                                .filter_map(|author| {
                                                    committee_guard
                                                        .primary(author)
                                                        .ok()
                                                        .map(|addrs| (author.clone(), addrs.primary_to_primary))
                                                })
                                                .collect()
                                        };
                                        if !missing_authors_with_addresses.is_empty() {
                                            info!(
                                                "[Core][E{}] Quorum watchdog: re-broadcasting own header H{} có mục tiêu tới {} peer(s) còn thiếu để thúc đẩy vote.",
                                                self.epoch, r, missing_authors_with_addresses.len()
                                            );
                                            let header_bytes_msg = bincode::serialize(&PrimaryMessage::Header(self.current_header.clone()))
                                                .expect("serialize header message");
                                            for (_auth, addr) in &missing_authors_with_addresses {
                                                let _ = self
                                                    .network
                                                    .send(*addr, Bytes::from(header_bytes_msg.clone()))
                                                    .await;
                                            }

                                            // Gửi VoteNudge tới các peer còn thiếu (cùng epoch và cửa sổ round)
                                            let nudge_msg = PrimaryMessage::VoteNudge {
                                                epoch: self.epoch,
                                                round: r,
                                                header_id: self.current_header.id.clone(),
                                                origin: self.current_header.author.clone(),
                                            };
                                            let nudge_bytes = bincode::serialize(&nudge_msg)
                                                .expect("serialize vote nudge");
                                            for (_auth, addr) in missing_authors_with_addresses {
                                                let _ = self.network.send(addr, Bytes::from(nudge_bytes.clone())).await;
                                            }
                                        }
                                    }

                                    // 4) Proactively re-broadcast known certificates for this round to all peers (vote nudge)
                                    let key = bincode::serialize(&(self.epoch, r)).expect("serialize round index key");
                                    if let Ok(Some(digests_bytes)) = self.store.read_cf(ROUND_INDEX_CF.to_string(), key).await {
                                        if let Ok(digests) = bincode::deserialize::<Vec<Digest>>(&digests_bytes) {
                                            let mut certs = Vec::new();
                                            for d in digests {
                                                if let Ok(Some(cert_bytes)) = self.store.read(d.to_vec()).await {
                                                    if let Ok(cert) = bincode::deserialize::<Certificate>(&cert_bytes) {
                                                        // Chỉ re-broadcast cert đúng epoch hiện tại
                                                        if cert.epoch() == self.epoch {
                                                            certs.push(cert);
                                                        }
                                                    }
                                                }
                                            }

                                            if !certs.is_empty() {
                                                let addresses: Vec<SocketAddr> = {
                                                    let committee_guard = self.committee.read().await;
                                                    committee_guard
                                                        .others_primaries(&self.name)
                                                        .iter()
                                                        .map(|(_, x)| x.primary_to_primary)
                                                        .collect()
                                                };
                                                debug!(
                                                    "[Core][E{}] Quorum watchdog: re-broadcasting {} certificate(s) for R{} to {} peers to nudge votes.",
                                                    self.epoch, certs.len(), r, addresses.len()
                                                );
                                                for cert in certs {
                                                    let cert_bytes_msg = bincode::serialize(&PrimaryMessage::Certificate(cert.clone()))
                                                        .expect("serialize certificate message");
                                                    let _ = self.network.broadcast(addresses.clone(), Bytes::from(cert_bytes_msg)).await;
                                                }
                                            }
                                        }
                                    }

                                    // Record that we sent a proactive request for rate limiting
                                    self.last_proactive_request.insert(r, Instant::now());
                                    // Don't reset stuck timer - keep tracking until quorum is achieved
                                }
                            }
                        } else {
                            // Round has quorum now - remove from stuck tracking
                            if self.round_stuck_since.remove(&r).is_some() {
                                debug!(
                                    "[Core][E{}] Quorum watchdog: R{} now has quorum ({} certificates from {} authors). Removed from stuck tracking.",
                                    self.epoch, r, cert_seen.len(), cert_seen.len()
                                );
                            }
                        }
                    }

                    // 2) Consensus liveness watchdog: if commit round hasn't advanced for too long
                    let current_committed = self.consensus_round.load(Ordering::Relaxed) as Round;

                    // If consensus_round is 0, it might be that GC hasn't updated it yet (no certificates committed since epoch transition)
                    // In this case, check if we've sent certificates to consensus but haven't seen commits
                    if current_committed == 0 && self.highest_round_sent_to_consensus > 0 {
                        // We've sent certificates but haven't seen any commits. Check if it's been too long.
                        // Use a longer timeout for the first commit after epoch transition
                        let first_commit_timeout = self.commit_stall_timeout_ms * 2; // Give more time for first commit
                        if self.last_commit_observed_at.elapsed() >= Duration::from_millis(first_commit_timeout) {
                            warn!(
                                "[Core][E{}] Consensus-liveness watchdog: consensus_round=0 but we've sent certificates up to R{} for {:?} (>{}ms). DAG at R{}. Consensus may be stuck waiting for certificates. Triggering assistance.",
                                self.epoch, self.highest_round_sent_to_consensus, self.last_commit_observed_at.elapsed(), first_commit_timeout, self.dag_round
                            );
                            // Request sync for rounds we've sent but not committed yet
                            let start = 1; // Start from round 1
                            let end = self.highest_round_sent_to_consensus.min(self.dag_round);
                            if end >= start {
                                self.request_sync_chunk(start, end, false).await; // Watchdog sync, use CatchupSync
                            }
                            self.last_commit_observed_at = Instant::now();
                        }
                    } else if current_committed > 0 {
                        // Normal case: consensus_round is > 0
                        if current_committed > self.last_commit_observed_round {
                            self.last_commit_observed_round = current_committed;
                            self.last_commit_observed_at = Instant::now();
                        } else if self.last_commit_observed_at.elapsed() >= Duration::from_millis(self.commit_stall_timeout_ms) {
                            warn!(
                                "[Core][E{}] Consensus-liveness watchdog: no commit progress for {:?} (>{}ms). last_committed_round={}, dag_round={}. Triggering assistance (range request).",
                                self.epoch, self.last_commit_observed_at.elapsed(), self.commit_stall_timeout_ms, current_committed, self.dag_round
                            );
                            let start = current_committed.saturating_add(1);
                            let end = self.dag_round;
                            if end >= start {
                                self.request_sync_chunk(start, end, false).await; // Watchdog sync, use CatchupSync
                            }
                            self.last_commit_observed_at = Instant::now();
                        }
                    }
                    // If current_committed == 0 and highest_round_sent_to_consensus == 0,
                    // we haven't sent anything yet, so no need to check for stall
                    Ok(())
                    } // Close else block
                },

                // Handle certificates returned by CertificateWaiter (dependencies met)
                 // Chỉ xử lý nếu không syncing.
                Some(certificate) = self.rx_certificate_waiter.recv(), if self.sync_state.is_none() => {
                     // Sanitize checks epoch/GC round. Process handles storage, aggregation, consensus send.
                     match self.sanitize_certificate(&certificate).await {
                        Ok(_) => {
                            // Update dag_round immediately if cert is newer.
                             if certificate.epoch() == self.epoch {
                                self.dag_round = self.dag_round.max(certificate.round());
                            }
                            self.process_certificate(certificate, false).await
                        },
                        Err(e) => {
                            warn!("[Core] Certificate from waiter failed sanitization: {}", e);
                            Ok(()) // Ignore invalid certificate
                        }
                    }
                },

                // Handle own headers created by Proposer
                 // Chỉ xử lý nếu không syncing.
                Some(header) = self.rx_proposer.recv(), if self.sync_state.is_none() => {
                    // Assumes own header is sanitized correctly by Proposer.
                    self.process_own_header(header).await
                },

                // Handle Sync Timeout Check (only active if sync_state is Some)
                _ = sync_check_interval.tick(), if self.sync_state.is_some() => {
                     // Check epoch mismatch FIRST before checking sync timeout
                     let committee_guard = self.committee.read().await;
                     let committee_epoch = committee_guard.epoch;
                     drop(committee_guard);
                     
                     if committee_epoch > self.epoch {
                         warn!(
                             "[Core][E{}] Detected epoch mismatch during sync timeout check: committee at epoch {} but Core still at epoch {}. Aborting sync and resetting state.",
                             self.epoch, committee_epoch, self.epoch
                         );
                         self.set_sync_state(None).await;
                         self.reset_state_for_new_epoch(committee_epoch);
                         info!("[Core][E{}] State reset complete after epoch mismatch detection during sync timeout.", self.epoch);
                         Ok(()) // Continue loop after epoch reset
                     } else {
                         // Determine if sync timeout check is needed based on last request time.
                         let sync_timeout_check = self.sync_state.as_ref().map_or(false, |s| {
                             s.last_request_time.elapsed() >= Duration::from_millis(SYNC_RETRY_DELAY)
                         });

                         // If sync timeout occurred...
                         if sync_timeout_check {
                             warn!("[Core][E{}] Sync request timed out (Round {}). Retrying...", self.epoch, self.sync_state.as_ref().map_or(0, |s| s.current_chunk_target));
                             self.advance_sync().await; // Retry or advance sync state.
                         }
                         Ok(()) // Continue loop after check
                     }
                }


                // Break loop if all input channels are closed (unlikely in normal operation)
                 else => {
                    info!("[Core][E{}] All input channels closed. Shutting down.", self.epoch);
                    break
                }
            }; // End tokio::select!

            // Handle results from the select! block.
            match result {
                Ok(()) => (), // Continue loop normally
                Err(DagError::StoreError(e)) => {
                    error!(
                        "[Core][E{}] Storage failure: {}. Shutting down node.",
                        self.epoch, e
                    );
                    panic!("Storage failure: killing node."); // Critical error
                }
                // Log recoverable errors but continue operation.
                Err(e @ DagError::TooOld(..)) | Err(e @ DagError::InvalidEpoch) => {
                    debug!("[Core][E{}] {}", self.epoch, e)
                } // Expected during transitions or late arrivals
                Err(e @ DagError::AuthorityReuse(..)) => {
                    warn!("[Core][E{}] Authority reuse detected: {}", self.epoch, e)
                } // Potential misbehavior
                Err(e) => warn!("[Core][E{}] General DAG error: {}", self.epoch, e), // Other errors
            }

            // --- Garbage Collection Logic (Run only when not syncing) ---
            if self.sync_state.is_none() {
                // Get the last round committed by consensus.
                let consensus_gc_round = self.consensus_round.load(Ordering::Relaxed);
                // Calculate the GC threshold round.
                if consensus_gc_round > self.gc_depth {
                    let new_gc_round = consensus_gc_round - self.gc_depth;
                    // Only perform GC if the threshold has actually advanced.
                    if new_gc_round > self.gc_round {
                        debug!(
                            "[Core][E{}] Advancing GC round from {} to {}",
                            self.epoch, self.gc_round, new_gc_round
                        );
                        // Remove old entries from internal state maps.
                        self.last_voted.retain(|k, _| k >= &new_gc_round);
                        self.processing.retain(|k, _| k >= &new_gc_round);
                        self.certificates_aggregators
                            .retain(|k, _| k >= &new_gc_round);
                        self.cancel_handlers.retain(|k, _| k >= &new_gc_round); // Also cancel old network requests?
                        self.gc_round = new_gc_round; // Update the GC round tracker.
                    }
                }
            }
        } // End main loop
        info!("[Core][E{}] Main loop terminated.", self.epoch);
    } // End fn run
} // End impl Core
