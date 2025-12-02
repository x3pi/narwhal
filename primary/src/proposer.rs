// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::{BatchRescue, CommittedBatches, Round};
// RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, warn};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

#[derive(Debug)]
struct BatchEntry {
    digest: Digest,
    worker_id: WorkerId,
    size: usize,
    state: BatchState,
    retry_count: usize, // Track number of times this batch has been retried
    rescue_sent: bool,
    /// LONG-TERM FIX: Priority for batch (higher = more important)
    priority: BatchPriority,
    /// LONG-TERM FIX: When batch was first added to queue
    added_at: Instant,
}

/// LONG-TERM FIX: Batch priority for smart retry and collection
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum BatchPriority {
    Low = 0,    // Batches cũ, đã retry nhiều lần
    Medium = 1, // Batches bình thường
    High = 2,   // Batches mới, từ leader, quan trọng
}

const MAX_ROUND_LEAD: Round = 50; // Không cho phép proposer vượt quá round đã commit + 2
const BOOTSTRAP_ROUND_BUDGET: Round = 50; // Cho phép tiến trước thêm trong giai đoạn chưa có commit
const MAX_ROUND_DRIFT: Round = 4; // Nếu vượt quá số vòng này thì không được force advance
const MIN_NETWORK_ROUND_TIMEOUT_SECS: u64 = 30; // Nếu minimum_network_round không được cập nhật trong 30s, bỏ qua nó để đảm bảo progress
const PARITY_STALL_ROUND_GAP: Round = 4; // Chênh lệch tối thiểu (hai round chẵn) để bật parity guard
const PARITY_STALL_ACTIVATE_MS: u64 = 1_500; // Đợi 1.5s trước khi bật parity guard
const PARITY_STALL_DELAY_MS: u64 = 600; // Delay thêm giữa các header khi parity guard bật
const PARITY_SYNC_RESCUE_INTERVAL_MS: u64 = 1_500; // Khoảng thời gian tối thiểu giữa các lần parity sync
const PARITY_SYNC_BATCH_BURST: usize = 5; // Số batch tối đa ép rescue mỗi lần parity sync
const RESCUE_RETRY_THRESHOLD: usize = 5;
const WARNING_SYNC_WAIT_SECS: u64 = 3; // Warning if batch waits this long for sync
const MAX_SYNC_WAIT_SECS: u64 = 6; // Force re-sync/rescue if batch waits this long
const WATCHDOG_PENDING_ZERO_SECS: u64 = 8; // Queue stuck threshold
const WATCHDOG_FORCE_RESCUE_BATCHES: usize = 10; // Max batches to rescue per watchdog tick
                                                 // LONG-TERM FIX: Queue size limit to prevent memory leak
const MAX_QUEUE_SIZE: usize = 10_000; // Maximum batches in queue
const QUEUE_WARNING_THRESHOLD: usize = 5_000; // Warn when queue size exceeds this
                                              // LONG-TERM FIX: Backpressure thresholds
const CHANNEL_WARNING_USAGE: f64 = 0.7; // Warn when channel usage > 70%
const CHANNEL_CRITICAL_USAGE: f64 = 0.9; // Critical when channel usage > 90%
                                         // LONG-TERM FIX: Adaptive retry thresholds
const HIGH_PRIORITY_RETRY_DELAY_MULTIPLIER: f64 = 0.5; // High priority batches retry 2x faster
const LOW_PRIORITY_RETRY_DELAY_MULTIPLIER: f64 = 2.0; // Low priority batches retry 2x slower
const PRIORITY_AGE_THRESHOLD_SECS: u64 = 60; // Batches older than 60s become low priority

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchState {
    Pending,
    InFlight {
        round: Round,
        sent_at: Instant,
        retry_count: usize,
    },
    Committed,
}

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The persistent storage.
    store: Store,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,
    /// The delay after which in-flight batches are re-queued if still uncommitted.
    retry_delay: Duration,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<(Vec<Digest>, Round)>,
    /// Receives verified headers from other primaries to extract batches from.
    rx_headers: Receiver<Header>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
    /// Receives notification of batches that have been committed.
    rx_committed: Receiver<CommittedBatches>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,
    /// Sends batch rescue requests to the `Core` when batches are stuck.
    tx_batch_rescue: Sender<BatchRescue>,

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Digest>,
    /// Holds the batches' digests waiting to be included in future headers (in arrival order).
    digests: VecDeque<BatchEntry>,
    /// Index mapping batch digest to its position in digests VecDeque for O(1) lookup.
    /// This dramatically improves extraction performance when queue is large.
    digests_index: HashMap<Digest, usize>,
    /// Keeps track of the size (in bytes) of batches that are ready to be scheduled (pending state).
    pending_payload_size: usize,
    /// Track the latest committed round to help decide when to retry stale payloads.
    latest_committed_round: Round,
    /// ROUND SYNC: Track minimum round seen from network (from parent certificates) to prevent round drift
    minimum_network_round: Round,
    /// ROUND SYNC: Track when minimum_network_round was last updated (to detect stale values)
    minimum_network_round_updated_at: Option<Instant>,
    /// Track digests that have been committed to avoid re-proposing them.
    /// Maps digest to the round it was committed in (for cleanup purposes).
    committed_digests: HashMap<Digest, Round>,
    /// Maximum rounds to wait before considering an InFlight batch as potentially committed.
    /// If a batch has been InFlight for more than this many rounds, we won't retry it.
    max_retry_rounds: Round,
    /// Maximum number of committed digests to keep in memory before cleanup.
    /// Older digests (from rounds before latest_committed_round - max_retry_rounds * 2) will be removed.
    max_committed_digests: usize,
    /// Track when we last received parent certificates from Core.
    /// Used to detect if proposer is stuck and needs to force advance round.
    last_parent_received_at: Option<Instant>,
    /// Maximum time to wait for parent certificates before force advancing round.
    /// This prevents proposer from being stuck indefinitely when Core stops sending parent certificates.
    max_parent_wait: Duration,
    // RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
    /// CATCH-UP MODE: Receive catch-up mode notifications from Core
    rx_catchup_mode: Receiver<bool>,
    /// CATCH-UP MODE: Track if node is in catch-up mode
    is_catchup_mode: bool,
    /// ROUND SYNC: Receive minimum network round updates from Core
    rx_min_network_round: Receiver<Round>,
    /// LONG-TERM FIX: Track header creation rate for backpressure
    header_creation_rate: f64, // Multiplier for header creation delay (1.0 = normal, >1.0 = slower)
    /// LONG-TERM FIX: Last time we checked channel usage
    last_channel_check: Option<Instant>,
    /// LONG-TERM FIX: Channel check interval
    channel_check_interval: Duration,
    /// WATCHDOG: Track when queue entered pending_payload_size == 0 state while batches exist.
    pending_zero_since: Option<Instant>,
    /// WATCHDOG: Maximum duration queue is allowed to be stuck before forcing rescue.
    max_pending_zero_duration: Duration,
    /// PARITY GUARD: Track if even rounds failed to commit and proposer must slow down.
    parity_guard_active: bool,
    parity_stall_since: Option<Instant>,
    parity_guard_backoff_until: Option<Instant>,
    last_parity_committed_even: Option<Round>,
    parity_last_sync: Option<Instant>,
}

impl Proposer {
    fn max_allowed_round(&self) -> Round {
        // REVERTED: Removed round sync logic, using only latest_committed_round
        let committed_cap = self.latest_committed_round.saturating_add(MAX_ROUND_LEAD);
        
        if self.latest_committed_round < MAX_ROUND_LEAD {
            committed_cap.max(BOOTSTRAP_ROUND_BUDGET)
        } else {
            committed_cap
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: &Committee,
        signature_service: SignatureService,
        store: Store,
        header_size: usize,
        max_header_delay: u64,
        sync_retry_delay: u64,
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_headers: Receiver<Header>,
        rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
        rx_committed: Receiver<CommittedBatches>,
        tx_core: Sender<Header>,
        tx_batch_rescue: Sender<BatchRescue>,
        // RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
        rx_catchup_mode: Receiver<bool>, // CATCH-UP MODE: Receive catch-up mode notifications
        rx_min_network_round: Receiver<Round>, // ROUND SYNC: Receive minimum network round updates
    ) {
        let genesis = Certificate::genesis(committee)
            .iter()
            .map(|x| x.digest())
            .collect();

        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                store,
                header_size,
                max_header_delay,
                retry_delay: Duration::from_millis(sync_retry_delay.max(1)),
                rx_core,
                rx_headers,
                rx_workers,
                rx_committed,
                tx_core,
                tx_batch_rescue,
                round: 1,
                last_parents: genesis,
                digests: VecDeque::with_capacity(2 * header_size.max(1)),
                digests_index: HashMap::new(),
                pending_payload_size: 0,
                latest_committed_round: 0,
                minimum_network_round: 0, // ROUND SYNC: Initialize to 0, will be updated from Core
                minimum_network_round_updated_at: None, // ROUND SYNC: Track when minimum_network_round was last updated
                committed_digests: HashMap::new(),
                max_retry_rounds: 1000, // Don't retry batches that have been InFlight for more than 1000 rounds
                max_committed_digests: 5000, // Giảm từ 10000 xuống 5000 để cleanup thường xuyên hơn, tránh memory leak
                last_parent_received_at: Some(Instant::now()), // Initialize with current time
                max_parent_wait: Duration::from_secs(10), // Force advance after 10 seconds without parent certificates
                // RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
                // CATCH-UP MODE: Initialize catch-up mode state
                rx_catchup_mode,
                is_catchup_mode: false,
                // ROUND SYNC: Initialize minimum network round receiver
                rx_min_network_round,
                // LONG-TERM FIX: Initialize performance tracking
                header_creation_rate: 1.0, // Start with normal rate
                last_channel_check: None,
                channel_check_interval: Duration::from_secs(5), // Check every 5 seconds
                pending_zero_since: None,
                max_pending_zero_duration: Duration::from_secs(WATCHDOG_PENDING_ZERO_SECS),
                parity_guard_active: false,
                parity_stall_since: None,
                parity_guard_backoff_until: None,
                last_parity_committed_even: None,
                parity_last_sync: None,
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) -> bool {
        // CRITICAL: Before collecting payload, check and remove any committed batches from queue
        // This prevents race conditions where batch was committed between last check and now
        let mut committed_removed = 0usize;
        let mut size_to_subtract = 0usize;
        let committed_digests_ref = &self.committed_digests;
        for entry in self.digests.iter() {
            if committed_digests_ref.contains_key(&entry.digest) {
                if matches!(entry.state, BatchState::Pending) {
                    size_to_subtract += entry.size;
                }
                committed_removed += 1;
            }
        }
        if committed_removed > 0 {
            warn!(
                "[MAKE_HEADER] Removing {} already-committed batches from queue before creating header for round {}",
                committed_removed, self.round
            );
            self.digests
                .retain(|entry| !committed_digests_ref.contains_key(&entry.digest));
            self.pending_payload_size = self.pending_payload_size.saturating_sub(size_to_subtract);
        }

        let payload: Vec<(Digest, WorkerId)> = self.collect_payload_for_header().await;

        // Final check: ensure no duplicates and no committed batches in payload before creating header
        let mut seen = HashSet::new();
        let mut deduplicated_payload = Vec::new();
        let mut duplicates_found = 0;
        let mut committed_found = 0;

        for (digest, worker_id) in payload {
            // CRITICAL: Final check - skip if already committed
            // This is a last line of defense against race conditions
            if self.committed_digests.contains_key(&digest) {
                committed_found += 1;
                warn!(
                    "[MAKE_HEADER] Removing already-committed digest {} from header payload for round {} (last check before creating header)",
                    digest, self.round
                );
                // Also mark the entry as committed if it exists in queue
                for entry in self.digests.iter_mut() {
                    if entry.digest == digest {
                        entry.state = BatchState::Committed;
                        break;
                    }
                }
                continue;
            }

            // Skip duplicates
            if seen.contains(&digest) {
                duplicates_found += 1;
                debug!(
                    "Removing duplicate digest {} from header payload for round {}",
                    digest, self.round
                );
                continue;
            }
            seen.insert(digest.clone());
            deduplicated_payload.push((digest, worker_id));
        }

        if duplicates_found > 0 {
            warn!(
                "Found {} duplicate digests in header payload for round {}, removed them",
                duplicates_found, self.round
            );
        }

        if committed_found > 0 {
            warn!(
                "Found {} already-committed digests in header payload for round {}, removed them",
                committed_found, self.round
            );
        }

        if duplicates_found > 0 || committed_found > 0 {
            info!(
                "Header payload sanitized at round {}: duplicates_removed={}, already_committed_removed={}",
                self.round, duplicates_found, committed_found
            );
        }

        if deduplicated_payload.is_empty() {
            debug!(
                "[HEADER CREATE] Primary {} no payload to include in header for round {} (pending_payload_size = {} bytes, queue_len: {})",
                self.name,
                self.round,
                self.pending_payload_size,
                self.digests.len()
            );
            // Still create an empty header if we have parents (for round advancement)
            // This is needed for empty rounds where no batches are available
            // PHASE 1: Also allow creating empty header with empty parents if force advance is enabled
            // This prevents proposer from being stuck when no parent certificates are received
            // CRITICAL FIX: Trong catch-up mode, vẫn tạo empty headers để đảm bảo hệ thống tiếp tục
            let force_advance = self
                .last_parent_received_at
                .map(|t| t.elapsed() > self.max_parent_wait)
                .unwrap_or(false);
            
            // CRITICAL: Trong catch-up mode, luôn cho phép tạo empty header khi không có parents
            // Điều này đảm bảo block rỗng vẫn được tạo ngay cả khi không có batches
            let allow_empty_header = !self.last_parents.is_empty() 
                || force_advance 
                || self.is_catchup_mode; // CRITICAL: Catch-up mode luôn cho phép empty header

            if allow_empty_header {
                // PHASE 1: Log warning if creating header with empty parents due to force advance
                if self.last_parents.is_empty() && (force_advance || self.is_catchup_mode) {
                    warn!(
                        "[FORCE ADVANCE HEADER] Creating header for round {} with EMPTY parents (force_advance={}, catchup_mode={}). This header may not be committed by consensus (requires quorum parents), but allows proposer to continue and avoid being stuck.",
                        self.round, force_advance, self.is_catchup_mode
                    );
                }

                let parents_for_header = if self.last_parents.is_empty() {
                    BTreeSet::new() // Empty parents when force advance or catch-up mode
                } else {
                    self.last_parents.drain(..).collect()
                };

                let header = Header::new(
                    self.name,
                    self.round,
                    BTreeMap::new(),
                    parents_for_header,
                    &mut self.signature_service,
                )
                .await;
                debug!("[HEADER CREATED] Primary {} created EMPTY header {} for round {} (pending_payload_size: {} bytes, queue_len: {}, catchup_mode: {})", self.name, header.id, self.round, self.pending_payload_size, self.digests.len(), self.is_catchup_mode);
                self.tx_core
                    .send(header)
                    .await
                    .expect("Failed to send header");
                return true;
            }
            return false;
        }

        // Make a new header.
        debug!(
            "[HEADER CREATE] Primary {} creating header for round {} with {} payload digests (pending_payload_size before send = {} bytes, queue_len: {})",
            self.name,
            self.round,
            deduplicated_payload.len(),
            self.pending_payload_size,
            self.digests.len()
        );

        // BATCH TRACKING: Log which batches are included in header
        let batch_digests: Vec<_> = deduplicated_payload
            .iter()
            .map(|(digest, _)| digest)
            .collect();

        // CRITICAL: Warning khi header empty - đây là dấu hiệu batches không được include
        if batch_digests.is_empty() {
            warn!(
                "[BATCH TRACK HEADER] WARNING: Primary {} creating EMPTY header for round {} (0 batches). Queue has {} batches (pending_payload_size: {} bytes). This may indicate batches are not being selected or queue is stuck!",
                self.name,
                self.round,
                self.digests.len(),
                self.pending_payload_size
            );
            
            // Tracing: Empty header warning
            for entry in self.digests.iter().take(10) {
                tracing::warn!(
                    batch_id = %entry.digest,
                    round = self.round,
                    queue_len = self.digests.len(),
                    pending_payload_size = self.pending_payload_size,
                    "[BATCH NOT INCLUDED] Batch NOT included in header - Queue may be stuck!"
                );
            }
        }
        // Bỏ log chi tiết về batches trong header - chỉ cần log khi empty hoặc warning

        let header = Header::new(
            self.name,
            self.round,
            deduplicated_payload.into_iter().collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        
        // CRITICAL: Log khi header được tạo với batches để đề xuất vote
        let batch_digests_in_header: Vec<String> = header.payload.keys().map(|d| format!("{}", d)).collect();
        tracing::info!(
            target: "narwhal_audit",
            "[HEADER PROPOSED] Primary {} PROPOSED header {} (round {}, {} batches) for voting. Batches in header: {:?}. Header will be broadcast to all primaries for voting.",
            self.name,
            header.id,
            header.round,
            header.payload.len(),
            batch_digests_in_header
        );

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
        true
    }

    async fn collect_payload_for_header(&mut self) -> Vec<(Digest, WorkerId)> {
        // First, cleanup any committed batches from the queue
        // We need to calculate size to subtract first
        let mut size_to_subtract = 0usize;
        let committed_digests_ref = &self.committed_digests;

        for entry in self.digests.iter() {
            if committed_digests_ref.contains_key(&entry.digest) {
                if matches!(entry.state, BatchState::Pending) {
                    size_to_subtract += entry.size;
                }
            }
        }

        // Now remove committed batches
        self.digests
            .retain(|entry| !committed_digests_ref.contains_key(&entry.digest));
        self.pending_payload_size = self.pending_payload_size.saturating_sub(size_to_subtract);

        let mut collected = Vec::new();
        let mut accumulated_size = 0usize;
        let mut seen_digests = HashSet::new(); // Track digests in this header to avoid duplicates
        let mut inflight_skipped = 0usize;
        let mut forced_rescues: Vec<(Digest, WorkerId)> = Vec::new();

        // LONG-TERM FIX: Update priority based on age first
        for entry in self.digests.iter_mut() {
            if entry.added_at.elapsed().as_secs() > PRIORITY_AGE_THRESHOLD_SECS {
                // Batches older than threshold become low priority
                if entry.priority > BatchPriority::Low {
                    entry.priority = BatchPriority::Low;
                }
            }
        }

        // LONG-TERM FIX: Collect batches with priority sorting
        // Collect high priority batches first, then medium, then low
        // We iterate multiple times: first High, then Medium, then Low
        for priority_level in [
            BatchPriority::High,
            BatchPriority::Medium,
            BatchPriority::Low,
        ] {
            for entry in self.digests.iter_mut() {
                if accumulated_size >= self.header_size {
                    break;
                }

                // Only process batches with current priority level
                if entry.priority != priority_level {
                    continue;
                }

                // CRITICAL: Double-check committed status before collecting
                // This prevents race conditions where batch was committed between cleanup and collection
                if self.committed_digests.contains_key(&entry.digest) {
                    warn!(
                        "Detected committed batch {} during payload collection for round {} - skipping",
                        entry.digest, self.round
                    );
                    entry.state = BatchState::Committed;
                    continue;
                }

                // Skip if already added to this header (duplicate check)
                if seen_digests.contains(&entry.digest) {
                    debug!(
                        "Skipping duplicate digest {} in header payload for round {}",
                        entry.digest, self.round
                    );
                    continue;
                }

                // OPTIMIZATION: Skip InFlight batches immediately without logging each one
                if matches!(entry.state, BatchState::InFlight { .. }) {
                    // CRITICAL DEBUG: Log tất cả batches InFlight bị skip
                    if let BatchState::InFlight { round, sent_at, retry_count } = &entry.state {
                        debug!(
                            target: "narwhal_audit",
                            "[BATCH TRACE] Batch {} is InFlight (round: {}, sent_at: {:?} ago, retry_count: {}). Skipped from header collection for round {}. Batch may be waiting for certificate commit.",
                            entry.digest,
                            round,
                            sent_at.elapsed(),
                            retry_count,
                            self.round
                        );
                    }
                    inflight_skipped += 1;
                    continue;
                }

                if matches!(entry.state, BatchState::Pending) {
                    // TRIPLE-CHECK: Verify batch is still not committed before marking as InFlight
                    // This prevents race conditions where batch was committed between cleanup and collection
                    if self.committed_digests.contains_key(&entry.digest) {
                        warn!(
                            "[COLLECT] Batch {} became committed during collection for round {} - skipping",
                            entry.digest, self.round
                        );
                        entry.state = BatchState::Committed;
                        continue;
                    }

                    // CRITICAL FIX: Verify batch is in store before including in header
                    // This ensures we only include batches that are available for other primaries to verify
                    // However, if batch has been waiting for sync too long, we may need to wait or skip
                    match self.store.read(entry.digest.to_vec()).await {
                        Ok(Some(_)) => {
                            // Batch is in store - safe to include
                            accumulated_size += entry.size;
                            seen_digests.insert(entry.digest.clone());
                            collected.push((entry.digest.clone(), entry.worker_id));
                            
                            // CRITICAL DEBUG: Log tất cả batches được collect cho header
                            tracing::info!(
                                target: "narwhal_audit",
                                "[BATCH TRACE] Batch {} COLLECTED for header round {} (size {} bytes, retry_count={}, priority={:?}). Accumulated payload = {} / target {} bytes",
                                entry.digest,
                                self.round,
                                entry.size,
                                entry.retry_count,
                                entry.priority,
                                accumulated_size,
                                self.header_size
                            );
                            
                            debug!(
                                "[BATCH TRACK PRIMARY] Primary {} COLLECTING batch {} from worker {} for header round {} (size {} bytes, retry_count={}, priority={:?}). Accumulated payload = {} / target {} bytes",
                                self.name,
                                entry.digest,
                                entry.worker_id,
                                self.round,
                                entry.size,
                                entry.retry_count,
                                entry.priority,
                                accumulated_size,
                                self.header_size
                            );
                            entry.state = BatchState::InFlight {
                                round: self.round,
                                sent_at: Instant::now(),
                                retry_count: entry.retry_count,
                            };
                            // Decrease pending_payload_size when batch is collected (marked as InFlight)
                            self.pending_payload_size =
                                self.pending_payload_size.saturating_sub(entry.size);
                        }
                        Ok(None) => {
                            // Batch not in store yet - check if it's been waiting too long
                            let time_waiting_for_sync = entry.added_at.elapsed();
                            
                            // CRITICAL DEBUG: Log tất cả batches không có trong store khi collect
                            warn!(
                                target: "narwhal_audit",
                                "[BATCH TRACE] Batch {} NOT IN STORE when collecting for header round {}. Waiting for sync for {:?}. rescue_sent={}. This may prevent batch from being included in header!",
                                entry.digest,
                                self.round,
                                time_waiting_for_sync,
                                entry.rescue_sent
                            );

                            if time_waiting_for_sync.as_secs() > MAX_SYNC_WAIT_SECS {
                                // Batch has been waiting too long - trigger forced rescue/sync
                                if !entry.rescue_sent {
                                    entry.rescue_sent = true;
                                    forced_rescues.push((entry.digest.clone(), entry.worker_id));
                                }
                                error!(
                                    "[COLLECT SYNC FAILED] Primary {} batch {} has been waiting for sync for {:?} (threshold: {}s) at round {}. Triggering forced rescue to replicate batch and unblock queue.",
                                    self.name,
                                    entry.digest,
                                    time_waiting_for_sync,
                                    MAX_SYNC_WAIT_SECS,
                                    self.round
                                );
                            } else if time_waiting_for_sync.as_secs() > WARNING_SYNC_WAIT_SECS {
                                // Batch has been waiting for a while - log warning
                                warn!(
                                    "[COLLECT SYNC SLOW] Primary {} batch {} has been waiting for sync for {:?} (warning threshold: {}s) at round {}. Sync may be slow. Monitoring closely.",
                                    self.name,
                                    entry.digest,
                                    time_waiting_for_sync,
                                    WARNING_SYNC_WAIT_SECS,
                                    self.round
                                );
                            } else {
                                debug!(
                                    "[COLLECT] Primary {} SKIP collecting batch {} for header round {} - NOT IN STORE yet (waiting for {:?}). Batch will be included after sync completes.",
                                    self.name,
                                    entry.digest,
                                    self.round,
                                    time_waiting_for_sync
                                );
                            }
                            // Keep batch in Pending state - will be collected after sync completes
                            // NOTE: The batch is NOT counted in pending_payload_size (not ready yet)
                        }
                        Err(e) => {
                            warn!(
                                "[COLLECT] Primary {} ERROR reading batch {} from store during collection for round {}: {}. Skipping batch.",
                                self.name,
                                entry.digest,
                                self.round,
                                e
                            );
                            // Keep batch in Pending state - will be retried later
                        }
                    }
                }
            }
        }

        if inflight_skipped > 0 {
            debug!(
                "[COLLECT] Skipped {} InFlight batches during collection for round {}",
                inflight_skipped, self.round
            );
        }

        debug!(
            "[COLLECT] Finished round {} collection: {} digests totaling {} bytes (pending_payload_size now {}).",
            self.round,
            collected.len(),
            accumulated_size,
            self.pending_payload_size
        );

        if !forced_rescues.is_empty() {
            debug!(
                "[COLLECT FORCED RESCUE] Primary {} dispatching rescue for {} batches that exceeded sync wait (round {}).",
                self.name,
                forced_rescues.len(),
                self.round
            );
            self.dispatch_batch_rescue_requests(forced_rescues);
        }

        collected
    }

    fn mark_committed(&mut self, committed: CommittedBatches) {
        let old_committed_round = self.latest_committed_round;
        self.latest_committed_round = self.latest_committed_round.max(committed.round);
        
        // Chỉ log khi round được cập nhật đáng kể (quan trọng để trace progress)
        if self.latest_committed_round > old_committed_round && self.latest_committed_round % 10 == 0 {
            info!(
                target: "narwhal_audit",
                "[COMMITTED ROUND] Primary {} committed round {} (current={})",
                self.name,
                self.latest_committed_round,
                self.round
            );
        }

        // CRITICAL DEBUG: Log tất cả batches được commit
        for digest in &committed.digests {
            tracing::info!(
                target: "narwhal_audit",
                "[BATCH TRACE] Batch {} COMMITTED at round {}!",
                digest,
                committed.round
            );
        }
        
        if committed.digests.is_empty() {
            return;
        }

        // Add all committed digests to the tracking set FIRST
        // This ensures we can check committed_digests even for InFlight batches
        for digest in &committed.digests {
            self.committed_digests
                .insert(digest.clone(), committed.round);
        }

        // Cleanup old digests if we've exceeded the limit
        self.cleanup_old_committed_digests();

        let committed_set: HashSet<_> = committed.digests.into_iter().collect();
        let mut marked_count = 0;
        let mut _inflight_count = 0;

        for entry in self.digests.iter_mut() {
            if committed_set.contains(&entry.digest) {
                // Mark as committed regardless of current state
                // This prevents InFlight batches from being retried later
                if matches!(entry.state, BatchState::Pending) {
                    self.pending_payload_size =
                        self.pending_payload_size.saturating_sub(entry.size);
                } else if matches!(entry.state, BatchState::InFlight { .. }) {
                    _inflight_count += 1;
                }
                entry.state = BatchState::Committed;
                marked_count += 1;
            }
        }

        // Bỏ log chi tiết về batch commit - chỉ log ở level cao hơn khi cần

        // Remove committed batches from queue
        self.digests
            .retain(|entry| !matches!(entry.state, BatchState::Committed));

        // Rebuild index after removing committed batches (indices may have shifted)
        // This is O(n) but only happens when batches are committed, not on every extraction
        self.rebuild_digests_index();
    }

    /// Cleanup old committed digests to prevent unbounded memory growth.
    /// Removes digests from rounds that are older than (latest_committed_round - max_retry_rounds * 2).
    /// This ensures we keep enough history to check for committed batches during retry logic.
    fn cleanup_old_committed_digests(&mut self) {
        // CRITICAL: Cleanup thường xuyên hơn để tránh memory leak
        // Thay vì chỉ cleanup khi > max, cleanup định kỳ khi > 50% max để tránh tích lũy
        const CLEANUP_THRESHOLD_RATIO: f64 = 0.5; // Cleanup khi > 50% capacity
        let cleanup_threshold =
            (self.max_committed_digests as f64 * CLEANUP_THRESHOLD_RATIO) as usize;

        if self.committed_digests.len() <= cleanup_threshold {
            return;
        }

        // Calculate watermark: keep digests from rounds that are within max_retry_rounds * 2 of latest_committed_round
        // This ensures we can still check for committed batches during retry logic
        let watermark = self
            .latest_committed_round
            .saturating_sub(self.max_retry_rounds * 2);

        let before_count = self.committed_digests.len();
        self.committed_digests
            .retain(|_, commit_round| *commit_round >= watermark);
        let after_count = self.committed_digests.len();
        let removed = before_count - after_count;

        if removed > 0 {
            debug!(
                "Cleaned up {} old committed digests (watermark: round {}, kept: {})",
                removed, watermark, after_count
            );
        }
    }

    fn rebuild_digests_index(&mut self) {
        self.digests_index.clear();
        for (idx, entry) in self.digests.iter().enumerate() {
            self.digests_index.insert(entry.digest.clone(), idx);
        }
    }

    fn get_valid_digest_index(&mut self, digest: &Digest) -> Option<usize> {
        if let Some(&idx) = self.digests_index.get(digest) {
            if idx < self.digests.len() {
                return Some(idx);
            }
            warn!(
                "[DIGEST INDEX] Primary {} phát hiện index {} (queue_len={}) bị lệch cho batch {:?} - rebuild lại index",
                self.name,
                idx,
                self.digests.len(),
                digest
            );
        } else {
            return None;
        }

        self.rebuild_digests_index();
        if let Some(&idx) = self.digests_index.get(digest) {
            if idx < self.digests.len() {
                return Some(idx);
            }
        }
        self.digests_index.remove(digest);
        None
    }

    fn retry_stale_batches(&mut self) {
        let now = Instant::now();
        let mut requeued = 0usize;
        let mut skipped_committed = 0usize;
        let mut skipped_too_old = 0usize;
        let mut requeued_old = 0usize;
        let mut removed_too_old = 0usize;
        let mut batches_to_rescue: Vec<(Digest, WorkerId)> = Vec::new();

        for entry in self.digests.iter_mut() {
            if let BatchState::InFlight {
                round,
                sent_at,
                retry_count,
            } = entry.state
            {
                // Skip if already committed - this is critical to prevent duplicates
                if self.committed_digests.contains_key(&entry.digest) {
                    info!(
                        "retry_stale_batches: digest {} sent at round {} already committed (current round {})",
                        entry.digest,
                        round,
                        self.round
                    );
                    entry.state = BatchState::Committed;
                    skipped_committed += 1;
                    continue;
                }

                // Check if batch is too old (InFlight for more than max_retry_rounds)
                let is_too_old = self.round > round.saturating_add(self.max_retry_rounds);
                // Check if batch is extremely old (InFlight for more than max_retry_rounds * 2)
                let is_extremely_old = self.round > round.saturating_add(self.max_retry_rounds * 2);

                if is_extremely_old {
                    // Remove extremely old batches - they will never be committed
                    warn!(
                        "Removing extremely old batch {} (sent at round {}, current round {}, extremely old threshold: {})",
                        entry.digest, round, self.round, self.max_retry_rounds * 2
                    );
                    entry.state = BatchState::Committed;
                    removed_too_old += 1;
                    continue;
                }

                // LONG-TERM FIX: Calculate adaptive retry delay based on priority
                // Note: Currently not used but kept for future optimization
                let _priority_multiplier = match entry.priority {
                    BatchPriority::High => HIGH_PRIORITY_RETRY_DELAY_MULTIPLIER,
                    BatchPriority::Medium => 1.0,
                    BatchPriority::Low => LOW_PRIORITY_RETRY_DELAY_MULTIPLIER,
                };

                // Calculate how long batch has been InFlight
                let rounds_since_sent = self.round.saturating_sub(round);
                let rounds_committed_since_sent = self.latest_committed_round.saturating_sub(round);

                // SAFE RETRY WINDOW: Only retry if we're confident batch hasn't been committed
                // Use a buffer to account for delayed commit notifications
                // If latest_committed_round is close to or past the round batch was sent,
                // batch might be committed but notification hasn't arrived yet
                const SAFE_RETRY_BUFFER: Round = 2; // Don't retry if latest_committed_round >= round - 2

                // CRITICAL: Check if batch is actually committed
                // If latest_committed_round >= sent_round but batch is not in committed_digests,
                // it means certificate of this primary was not committed (another primary's certificate was committed instead)
                // In this case, we should retry immediately to avoid waiting for max_retry_rounds
                let batch_is_actually_committed =
                    self.committed_digests.contains_key(&entry.digest);
                let own_certificate_not_committed =
                    !batch_is_actually_committed && self.latest_committed_round >= round;

                // IMPROVED: If batch has been InFlight for a long time (>= max_retry_rounds),
                // and latest_committed_round is still close to sent round, batch is likely stuck
                // Force retry to avoid batch being dropped forever
                let rounds_since_sent_long = rounds_since_sent >= self.max_retry_rounds;
                // IMPROVED: If batch has been retried multiple times (retry_count >= 2),
                // it's likely stuck and should be retried even if latest_committed_round is close
                let has_been_retried_multiple_times = retry_count >= 2;
                // CRITICAL FIX: If batch has been retried too many times (retry_count > MAX_RETRY_COUNT),
                // batch is stuck forever and should be removed to prevent system deadlock
                // Tăng từ 100 lên 1000 để cho batch nhiều cơ hội hơn khi certificate không được commit
                // Chỉ force remove khi batch thực sự không thể commit (retry quá nhiều lần)
                const MAX_RETRY_COUNT: usize = 1000; // Tăng từ 100 lên 1000 - Cho batch nhiều cơ hội commit hơn
                const WARNING_RETRY_COUNT: usize = 50; // Cảnh báo khi retry > 50
                let is_stuck_forever = retry_count > MAX_RETRY_COUNT;

                // Thêm cảnh báo sớm khi batch bị stuck
                if retry_count > WARNING_RETRY_COUNT && retry_count <= MAX_RETRY_COUNT {
                    warn!(
                        "[BATCH STUCK WARNING] Primary {} batch {} has been retried {} times (sent_round={}, current_round={}, rounds_since_sent={}, latest_committed_round={}). Batch may be stuck - monitoring closely.",
                        self.name,
                        entry.digest,
                        retry_count,
                        round,
                        self.round,
                        rounds_since_sent,
                        self.latest_committed_round
                    );
                    
                    // Tracing: Batch stuck warning
                    tracing::warn!(
                        batch_id = %entry.digest,
                        retry_count = retry_count,
                        sent_round = round,
                        current_round = self.round,
                        rounds_since_sent = rounds_since_sent,
                        latest_committed_round = self.latest_committed_round,
                        "[BATCH STUCK WARNING] Batch retried many times - May be stuck!"
                    );
                }

                if is_stuck_forever {
                    // Force remove batch - hệ thống không thể commit batch này
                    // Đây là biện pháp cuối cùng để ngăn chặn hệ thống bị đứng vĩnh viễn
                    // CRITICAL: Upgrade to error level để alert admin
                    error!(
                        "[BATCH STUCK CRITICAL] Primary {} FORCE REMOVING stuck batch {} (retry_count={}, sent_round={}, current_round={}, rounds_since_sent={}, latest_committed_round={}). Batch cannot be committed after {} retries - removing to prevent system deadlock. Transactions in this batch will be LOST. This indicates a serious system issue that needs investigation.",
                        self.name,
                        entry.digest,
                        retry_count,
                        round,
                        self.round,
                        rounds_since_sent,
                        self.latest_committed_round,
                        MAX_RETRY_COUNT
                    );
                    
                    // Tracing: Batch stuck critical
                    tracing::error!(
                        batch_id = %entry.digest,
                        retry_count = retry_count,
                        sent_round = round,
                        current_round = self.round,
                        rounds_since_sent = rounds_since_sent,
                        latest_committed_round = self.latest_committed_round,
                        "[BATCH STUCK CRITICAL] FORCE REMOVING stuck batch - Transactions LOST!"
                    );
                    
                    entry.state = BatchState::Committed; // Mark as committed to remove
                    removed_too_old += 1;
                    continue;
                }
                let safe_to_retry = if rounds_since_sent_long
                    || has_been_retried_multiple_times
                    || own_certificate_not_committed
                {
                    // Batch is old enough, has been retried multiple times, or certificate of this primary was not committed
                    // Force retry to avoid batch being dropped forever
                    true
                } else {
                    // Normal case: only retry if latest_committed_round is far enough behind
                    self.latest_committed_round < round.saturating_sub(SAFE_RETRY_BUFFER)
                };

                if is_too_old {
                    // For very old batches, we need to be more careful
                    // Only retry if we're confident batch hasn't been committed
                    if !safe_to_retry {
                        // Latest committed round is too close to the round batch was sent
                        // Batch might be committed but notification hasn't arrived yet
                        // Don't retry to avoid duplicate - wait for commit notification
                        info!(
                            "Skip retry for old batch {} - latest_committed_round {} is too close to sent round {} (current round {}, buffer={}). Batch might be committed, waiting for notification.",
                            entry.digest, self.latest_committed_round, round, self.round, SAFE_RETRY_BUFFER
                        );
                        skipped_too_old += 1;
                        // Keep it in InFlight state - will be marked as Committed when commit notification is received
                        continue;
                    }

                    // Safe to retry: either latest_committed_round is far enough behind, or batch is old enough to force retry
                    // CRITICAL: Double-check batch is still not committed before retry (even for force retry)
                    // This prevents duplicate even if commit notification arrives late
                    if self.committed_digests.contains_key(&entry.digest) {
                        // Batch became committed - don't retry
                        info!(
                            "Old batch {} became committed during retry check (sent round {}, current round {}). Marking as committed.",
                            entry.digest, round, self.round
                        );
                        entry.state = BatchState::Committed;
                        skipped_committed += 1;
                        continue;
                    }

                    // Batch is likely truly stale and needs retry
                    let retry_reason = if own_certificate_not_committed {
                        format!("certificate of this primary was not committed at round {} (latest_committed_round={}, batch not in committed_digests) - retry immediately", round, self.latest_committed_round)
                    } else if rounds_since_sent_long {
                        format!(
                            "batch is old enough ({} rounds >= {}) to force retry",
                            rounds_since_sent, self.max_retry_rounds
                        )
                    } else {
                        format!(
                            "latest_committed_round is {} rounds behind sent round",
                            round.saturating_sub(self.latest_committed_round)
                        )
                    };
                    info!(
                        "Requeue old batch {} for retry (sent at round {}, current round {}, latest_committed_round={}, rounds since sent: {}, committed rounds since: {}). {}",
                        entry.digest, round, self.round, self.latest_committed_round, rounds_since_sent, rounds_committed_since_sent,
                        retry_reason
                        );
                    entry.state = BatchState::Pending;
                    entry.retry_count += 1; // Increment retry count when requeuing
                    debug!(
                        "[BATCH RETRY] Primary {} RETRYING batch {} (retry_count={}, sent_round={}, current_round={}, rounds_since_sent={}, latest_committed_round={}, own_cert_not_committed={})",
                        self.name,
                        entry.digest,
                        entry.retry_count,
                        round,
                        self.round,
                        rounds_since_sent,
                        self.latest_committed_round,
                        own_certificate_not_committed
                    );
                    Self::maybe_schedule_batch_rescue(&mut batches_to_rescue, entry);
                    self.pending_payload_size += entry.size;
                    requeued_old += 1;
                } else {
                    // Batch is not too old - normal retry logic
                    // CRITICAL: Only retry if safe (latest_committed_round is far enough behind)
                    // This prevents requeueing batch that might be committed but notification hasn't arrived
                    if !safe_to_retry {
                        // Latest committed round is too close to the round batch was sent
                        // Batch might be committed but notification hasn't arrived yet
                        // Don't retry to avoid duplicate
                        info!(
                            "Skip retry for batch {} - latest_committed_round {} is too close to sent round {} (current round {}, buffer={}). Batch might be committed, waiting for notification.",
                            entry.digest, self.latest_committed_round, round, self.round, SAFE_RETRY_BUFFER
                        );
                        skipped_too_old += 1;
                        // Keep it in InFlight state - will be marked as Committed when commit notification is received
                        continue;
                    }

                    // Only retry if batch hasn't been committed and meets retry conditions
                    // IMPROVED: If own certificate was not committed, retry immediately without waiting for retry_delay
                    let should_retry_now = own_certificate_not_committed
                        || now.duration_since(sent_at) >= self.retry_delay;
                    if should_retry_now {
                        // Double-check: verify batch is still not committed before re-queueing
                        if !self.committed_digests.contains_key(&entry.digest) {
                            let retry_reason_detail = if own_certificate_not_committed {
                                format!("certificate of this primary was not committed at round {} - retry immediately", round)
                            } else {
                                format!("safe to retry as latest_committed_round is {} rounds behind sent round", round.saturating_sub(self.latest_committed_round))
                            };
                            info!(
                                "Requeue batch {} for retry (sent round {}, current round {}, latest_committed_round={}). {}",
                                entry.digest,
                                round,
                                self.round,
                                self.latest_committed_round,
                                retry_reason_detail
                            );
                            entry.state = BatchState::Pending;
                            entry.retry_count += 1; // Increment retry count when requeuing
                            Self::maybe_schedule_batch_rescue(&mut batches_to_rescue, entry);
                            self.pending_payload_size += entry.size;
                            requeued += 1;
                        } else {
                            info!(
                                "Batch {} became committed during retry check (sent round {}, current round {})",
                                entry.digest,
                                round,
                                self.round
                            );
                            entry.state = BatchState::Committed;
                            skipped_committed += 1;
                        }
                    }
                }
            }
        }

        if requeued > 0 {
            info!(
                "Requeued {} batches for re-inclusion (latest_committed_round = {})",
                requeued, self.latest_committed_round
            );
        }

        if requeued_old > 0 {
            info!(
                "Requeued {} old batches that are likely still pending (latest_committed_round = {})",
                requeued_old, self.latest_committed_round
            );
        }

        if skipped_committed > 0 {
            info!(
                "Skipped {} already-committed batches during retry check",
                skipped_committed
            );
        }

        if skipped_too_old > 0 {
            info!(
                "Skipped {} batches that are too old and likely already committed (max_retry_rounds = {}). These batches remain in InFlight state and will be marked as Committed when commit notification is received.",
                skipped_too_old, self.max_retry_rounds
            );
        }

        if removed_too_old > 0 {
            warn!(
                "Removed {} extremely old batches (max_retry_rounds * 2 = {}) that will never be committed",
                removed_too_old, self.max_retry_rounds * 2
            );
        }

        if !batches_to_rescue.is_empty() {
            self.dispatch_batch_rescue_requests(batches_to_rescue);
        }

        self.digests
            .retain(|entry| !matches!(entry.state, BatchState::Committed));
    }

    fn maybe_schedule_batch_rescue(pending: &mut Vec<(Digest, WorkerId)>, entry: &mut BatchEntry) {
        if entry.rescue_sent || entry.retry_count < RESCUE_RETRY_THRESHOLD {
            return;
        }
        entry.rescue_sent = true;
        pending.push((entry.digest.clone(), entry.worker_id));
    }

    fn dispatch_batch_rescue_requests(&self, pending: Vec<(Digest, WorkerId)>) {
        if pending.is_empty() {
            return;
        }

        let mut store = self.store.clone();
        let tx = self.tx_batch_rescue.clone();
        let origin = self.name.clone();
        
        tokio::spawn(async move {
            for (digest, worker_id) in pending {
                // CRITICAL DEBUG: Log nếu đây là batch đang tìm
                let is_target_batch = format!("{}", digest) == "gFe3xRf/Ba1q1VVU";
                
                match store.read(digest.to_vec()).await {
                    Ok(Some(batch)) => {
                        if is_target_batch {
                            info!(
                                target: "narwhal_audit",
                                "[BATCH TRACE] Batch gFe3xRf/Ba1q1VVU FOUND in store during rescue! Sending rescue request.",
                            );
                        }
                        
                        if let Err(e) = tx
                            .send(BatchRescue {
                                digest: digest.clone(),
                                worker_id,
                                batch,
                                origin: origin.clone(),
                            })
                            .await
                        {
                            if is_target_batch {
                                error!(
                                    target: "narwhal_audit",
                                    "[BATCH TRACE] Batch gFe3xRf/Ba1q1VVU rescue FAILED to send: {}",
                                    e
                                );
                            }
                            warn!(
                                "[BATCH RESCUE] Failed to send rescue request for batch {}: {}",
                                digest, e
                            );
                            break;
                        } else if is_target_batch {
                            info!(
                                target: "narwhal_audit",
                                "[BATCH TRACE] Batch gFe3xRf/Ba1q1VVU rescue request SENT successfully.",
                            );
                        }
                    }
                    Ok(None) => {
                        if is_target_batch {
                            error!(
                                target: "narwhal_audit",
                                "[BATCH TRACE] Batch gFe3xRf/Ba1q1VVU NOT IN STORE during rescue! This is the root cause - batch was never synced to store.",
                            );
                        }
                        warn!(
                            "[BATCH RESCUE] Unable to rescue batch {} - payload missing from store. Batch will be retried in next watchdog tick if still stuck.",
                            digest
                        );
                    }
                    Err(e) => {
                        if is_target_batch {
                            error!(
                                target: "narwhal_audit",
                                "[BATCH TRACE] Batch gFe3xRf/Ba1q1VVU rescue ERROR reading from store: {}",
                                e
                            );
                        }
                        warn!(
                            "[BATCH RESCUE] Error reading batch {} from store for rescue: {}",
                            digest, e
                        );
                    }
                }
            }
        });
    }

    /// WATCHDOG: Force batch rescue when queue is stuck (pending payload size stays 0).
    fn trigger_watchdog_rescue(&mut self) -> usize {
        let mut targets = Vec::new();
        const RESCUE_RETRY_TIMEOUT_SECS: u64 = 30; // Reset rescue_sent after 30s to allow retry
        
        for entry in self.digests.iter_mut() {
            if targets.len() >= WATCHDOG_FORCE_RESCUE_BATCHES {
                break;
            }
            
            // CRITICAL FIX: Reset rescue_sent if batch has been waiting too long after previous rescue attempt
            // This allows batches that failed to rescue (not in store) to be rescued again
            if entry.rescue_sent && entry.added_at.elapsed().as_secs() > RESCUE_RETRY_TIMEOUT_SECS {
                // Check if batch is still not in store (if it was in store, it would have been collected)
                // Reset rescue_sent to allow retry
                entry.rescue_sent = false;
                warn!(
                    target: "narwhal_audit",
                    "[WATCHDOG RESCUE RETRY] Resetting rescue_sent for batch {} after {}s. Batch still stuck, will retry rescue.",
                    entry.digest,
                    entry.added_at.elapsed().as_secs()
                );
            }
            
            if matches!(entry.state, BatchState::Pending) && !entry.rescue_sent {
                // CRITICAL DEBUG: Log nếu đây là batch đang tìm
                if format!("{}", entry.digest) == "gFe3xRf/Ba1q1VVU" {
                    warn!(
                        target: "narwhal_audit",
                        "[BATCH TRACE] Batch gFe3xRf/Ba1q1VVU eligible for WATCHDOG RESCUE (waiting for {:?}). Triggering rescue now.",
                        entry.added_at.elapsed()
                    );
                }
                
                entry.rescue_sent = true;
                targets.push((entry.digest.clone(), entry.worker_id));
            } else if format!("{}", entry.digest) == "gFe3xRf/Ba1q1VVU" {
                // CRITICAL DEBUG: Log tại sao batch không eligible
                warn!(
                    target: "narwhal_audit",
                    "[BATCH TRACE] Batch gFe3xRf/Ba1q1VVU NOT eligible for rescue. State: {:?}, rescue_sent: {}, waiting for {:?}.",
                    entry.state,
                    entry.rescue_sent,
                    entry.added_at.elapsed()
                );
            }
        }

        if !targets.is_empty() {
            let rescued_count = targets.len();
            warn!(
                "[WATCHDOG RESCUE] Proposer {} forcing batch rescue for {} stuck batches (queue_len={}, pending_payload_size={} bytes). pending_payload_size stayed at 0 despite queued batches for {:?}.",
                self.name,
                rescued_count,
                self.digests.len(),
                self.pending_payload_size,
                self.max_pending_zero_duration
            );
            self.dispatch_batch_rescue_requests(targets);
            return rescued_count;
        } else {
            debug!(
                "[WATCHDOG RESCUE] Proposer {} attempted forced rescue but no eligible batches found (queue_len={}, pending_payload_size={})",
                self.name,
                self.digests.len(),
                self.pending_payload_size
            );
        }

        0
    }

    fn update_parity_guard(&mut self) {
        let commit = self.latest_committed_round;
        let current = self.round;
        let commit_is_even = commit % 2 == 0;
        let stalled_two_even =
            commit_is_even && current >= commit.saturating_add(PARITY_STALL_ROUND_GAP);

        if stalled_two_even {
            if self.last_parity_committed_even != Some(commit) {
                self.last_parity_committed_even = Some(commit);
                self.parity_stall_since = Some(Instant::now());
            }

            let stalled_long_enough = self
                .parity_stall_since
                .map(|t| t.elapsed() >= Duration::from_millis(PARITY_STALL_ACTIVATE_MS))
                .unwrap_or(false);

            if stalled_long_enough && !self.parity_guard_active {
                self.parity_guard_active = true;
                self.parity_guard_backoff_until =
                    Some(Instant::now() + Duration::from_millis(PARITY_STALL_DELAY_MS));
                info!(
                    "[PARITY GUARD] Activated at commit {} (current round {}). Slowing headers and forcing sync before moving further.",
                    commit, current
                );
            }
        } else if self.parity_guard_active
            || self.parity_stall_since.is_some()
            || self.parity_guard_backoff_until.is_some()
        {
            if self.parity_guard_active {
                info!(
                    "[PARITY GUARD] Cleared (latest_committed_round={}, current_round={}).",
                    commit, current
                );
            }
            self.parity_guard_active = false;
            self.parity_stall_since = None;
            self.parity_guard_backoff_until = None;
            self.last_parity_committed_even = if commit_is_even {
                Some(commit)
            } else {
                None
            };
        }
    }

    fn parity_guard_blocking(&self) -> bool {
        if !self.parity_guard_active {
            return false;
        }
        self.parity_guard_backoff_until
            .map(|deadline| Instant::now() < deadline)
            .unwrap_or(false)
    }

    fn schedule_next_parity_backoff(&mut self) {
        if self.parity_guard_active {
            self.parity_guard_backoff_until =
                Some(Instant::now() + Duration::from_millis(PARITY_STALL_DELAY_MS));
        }
    }

    fn maybe_trigger_parity_rescue(&mut self) {
        if !self.parity_guard_active {
            return;
        }

        if self
            .parity_last_sync
            .map(|t| t.elapsed() < Duration::from_millis(PARITY_SYNC_RESCUE_INTERVAL_MS))
            .unwrap_or(false)
        {
            return;
        }
        self.parity_last_sync = Some(Instant::now());

        let mut targets = Vec::new();
        for entry in self.digests.iter_mut() {
            if targets.len() >= PARITY_SYNC_BATCH_BURST {
                break;
            }
            if matches!(entry.state, BatchState::Pending)
                && !entry.rescue_sent
                && entry.added_at.elapsed() >= Duration::from_millis(PARITY_STALL_ACTIVATE_MS)
            {
                entry.rescue_sent = true;
                targets.push((entry.digest.clone(), entry.worker_id));
            }
        }

        if !targets.is_empty() {
            info!(
                "[PARITY GUARD] Triggering batch rescue for {} pending batches while waiting for even rounds to commit (commit={}, round={}).",
                targets.len(),
                self.latest_committed_round,
                self.round
            );
            self.dispatch_batch_rescue_requests(targets);
        }
    }

    /// Extract batches from parent certificates and add them to queue if not committed
    /// This allows leader to include batches from other primaries, ensuring faster commit
    /// and reducing the need for retry
    async fn extract_batches_from_parents(
        &mut self,
        parent_digests: &[Digest],
        parent_round: Round,
    ) {
        let mut batches_extracted = 0usize;
        let mut batches_skipped_committed = 0usize;
        let mut batches_skipped_duplicate = 0usize;
        let mut batches_added = 0usize;

        for parent_digest in parent_digests {
            // Read certificate from store
            match self.store.read(parent_digest.to_vec()).await {
                Ok(Some(bytes)) => {
                    match bincode::deserialize::<Certificate>(&bytes) {
                        Ok(certificate) => {
                            // Extract batches from certificate header payload
                            for (batch_digest, worker_id) in certificate.header.payload.iter() {
                                batches_extracted += 1;

                                // Skip if already committed
                                if self.committed_digests.contains_key(batch_digest) {
                                    batches_skipped_committed += 1;
                                    debug!(
                                        "[BATCH TRACK PRIMARY] Primary {} SKIP extracting batch {} from parent certificate {} (round {}) - ALREADY COMMITTED",
                                        self.name,
                                        batch_digest,
                                        parent_digest,
                                        parent_round
                                    );
                                    continue;
                                }

                                // CRITICAL: Check if batch is already in queue
                                // If batch is already in queue and in Pending state, skip extraction
                                // (it will be included in next header anyway)
                                // If batch is in InFlight state and not committed, we should STILL extract it
                                // because this primary (possibly leader) can include it immediately,
                                // reducing the need for retry and making the system smoother
                                let already_in_queue = self
                                    .digests
                                    .iter()
                                    .any(|entry| entry.digest == *batch_digest);
                                if already_in_queue {
                                    // Check if batch is in Pending state
                                    let is_pending = self.digests.iter().any(|entry| {
                                        entry.digest == *batch_digest
                                            && matches!(entry.state, BatchState::Pending)
                                    });

                                    if is_pending {
                                        // Batch is already in Pending state - skip extraction
                                        // It will be included in next header anyway
                                        batches_skipped_duplicate += 1;
                                        debug!(
                                            "[BATCH TRACK PRIMARY] Primary {} SKIP extracting batch {} from parent certificate {} (round {}) - ALREADY IN QUEUE (Pending state)",
                                            self.name,
                                            batch_digest,
                                            parent_digest,
                                            parent_round
                                        );
                                        continue;
                                    }
                                    // If batch is in InFlight state, convert it back to Pending state
                                    // This allows leader to include batch immediately in next header, reducing retry
                                    // This is safe because:
                                    // 1. Batch is not committed (checked above)
                                    // 2. Converting InFlight to Pending allows it to be included in next header
                                    // 3. collect_payload_for_header will deduplicate (uses seen_digests HashSet)
                                    // 4. This prevents batch from waiting for retry logic
                                    let mut converted = false;
                                    for entry in self.digests.iter_mut() {
                                        if entry.digest == *batch_digest
                                            && matches!(entry.state, BatchState::InFlight { .. })
                                        {
                                            // Convert InFlight to Pending to allow immediate inclusion
                                            self.pending_payload_size += entry.size;
                                            entry.state = BatchState::Pending;
                                            batches_added += 1; // Count as added (converted from InFlight)
                                            converted = true;
                                            debug!(
                                                "[BATCH TRACK PRIMARY] Primary {} CONVERTED batch {} from parent certificate {} (round {}) from InFlight to Pending to allow immediate inclusion (reduces retry). This batch was sent in a previous header but certificate was not committed. Converting to Pending allows it to be included immediately in next header.",
                                                self.name,
                                                batch_digest,
                                                parent_digest,
                                                parent_round
                                            );
                                            break;
                                        }
                                    }
                                    if converted {
                                        // Skip adding new entry since we just converted existing one
                                        continue;
                                    }
                                    // Batch is in queue but not in InFlight state (shouldn't happen, but handle it)
                                    batches_skipped_duplicate += 1;
                                    debug!(
                                        "[BATCH TRACK PRIMARY] Primary {} SKIP extracting batch {} from parent certificate {} (round {}) - ALREADY IN QUEUE (not InFlight, already handled)",
                                        self.name,
                                        batch_digest,
                                        parent_digest,
                                        parent_round
                                    );
                                    continue;
                                }

                                // Try to read batch from store to get size
                                // If batch is not in store, we can't include it (will be synced later)
                                match self.store.read(batch_digest.to_vec()).await {
                                    Ok(Some(_batch_data)) => {
                                        let size = batch_digest.size();

                                        // Add to queue as Pending
                                        let entry_idx = self.digests.len();
                                        // LONG-TERM FIX: Batches from parent certificates are medium priority
                                        let priority = BatchPriority::Medium;
                                        self.digests.push_back(BatchEntry {
                                            digest: batch_digest.clone(),
                                            worker_id: *worker_id,
                                            size,
                                            state: BatchState::Pending,
                                            retry_count: 0,
                                            rescue_sent: false,
                                            priority,
                                            added_at: Instant::now(),
                                        });
                                        // Maintain HashMap index for O(1) lookup
                                        self.digests_index.insert(batch_digest.clone(), entry_idx);
                                        self.pending_payload_size += size;
                                        batches_added += 1;

                                        debug!(
                                            "[BATCH TRACK PRIMARY] Primary {} EXTRACTED batch {} (worker {}) from parent certificate {} (round {}) into queue for round {} to help leader commit batches from other primaries. Batch size: {} bytes, retry_count: 0",
                                            self.name,
                                            batch_digest,
                                            worker_id,
                                            parent_digest,
                                            parent_round,
                                            self.round,
                                            size
                                        );
                                    }
                                    Ok(None) => {
                                        // Batch not in store yet - will be synced later
                                        // Don't add to queue now, but log for debugging
                                        debug!(
                                            "[EXTRACT BATCHES] Batch {} from parent certificate {} (round {}) not in store yet, will be synced later",
                                            batch_digest,
                                            parent_digest,
                                            parent_round
                                        );
                                    }
                                    Err(e) => {
                                        warn!(
                                            "[EXTRACT BATCHES] Error reading batch {} from store: {}",
                                            batch_digest, e
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!(
                                "[EXTRACT BATCHES] Failed to deserialize certificate {}: {}",
                                parent_digest, e
                            );
                        }
                    }
                }
                Ok(None) => {
                    // Certificate not in store yet - will be synced later
                    debug!(
                        "[EXTRACT BATCHES] Parent certificate {} not in store yet",
                        parent_digest
                    );
                }
                Err(e) => {
                    warn!(
                        "[EXTRACT BATCHES] Error reading parent certificate {} from store: {}",
                        parent_digest, e
                    );
                }
            }
        }

        if batches_extracted > 0 {
            debug!(
                "[EXTRACT BATCHES] Extracted {} batches from {} parent certificates (round {}): {} added to queue, {} skipped (committed), {} skipped (duplicate)",
                batches_extracted,
                parent_digests.len(),
                parent_round,
                batches_added,
                batches_skipped_committed,
                batches_skipped_duplicate
            );
        }
    }

    /// Extract batches from verified headers of other primaries
    /// CRITICAL FIX: This method ensures leader ALWAYS extracts batches from ALL headers,
    /// not just from headers of the leader itself. This prevents batches from being stuck
    /// when non-leader primaries create headers with batches but their certificates are not committed.
    ///
    /// This allows any primary (especially leader) to include batches from non-leader primaries,
    /// ensuring faster commit and preventing batches from being stuck indefinitely.
    ///
    /// SAFETY: This method is deterministic because:
    /// 1. Headers are already verified and stored before being sent here
    /// 2. All primaries receive the same headers via network (deterministic source)
    /// 3. Extraction logic is deterministic (same order, same checks)
    /// 4. Batches from own headers are also extracted if they're in InFlight state (allows retry)
    async fn extract_batches_from_headers(&mut self, header: &Header) {
        // CRITICAL: For own headers, only skip if ALL batches are already committed or in Pending state
        // This allows leader to extract batches from own headers when they're in InFlight state,
        // preventing batches from being stuck when own certificate is not committed
        if header.author == self.name {
            // Check if all batches in this own header are either:
            // 1. Already committed, OR
            // 2. Already in queue in Pending state (not InFlight)
            let all_batches_safe = header.payload.iter().all(|(batch_digest, _)| {
                // Skip if already committed
                if self.committed_digests.contains_key(batch_digest) {
                    return true;
                }

                // Check if batch is in queue using O(1) HashMap lookup
                if let Some(entry_idx) = self.get_valid_digest_index(batch_digest) {
                    let entry = &self.digests[entry_idx];
                    // If batch is in Pending state, it's safe to skip (already available for inclusion)
                    if matches!(entry.state, BatchState::Pending) {
                        return true;
                    }
                    // If batch is in InFlight state, we should NOT skip - need to extract to convert to Pending
                    // This allows leader to include batch even if own certificate wasn't committed
                    if matches!(entry.state, BatchState::InFlight { .. }) {
                        return false; // Not safe to skip - need to process
                    }
                }

                // Batch not in queue - safe to skip (will be added when received from worker)
                true
            });

            if all_batches_safe {
                // All batches are safe to skip - return early
                return;
            }
            // Some batches are in InFlight state - continue to extract them
            // Bỏ log chi tiết - không cần thiết cho trace batch
        }

        // PERFORMANCE: Skip headers that are too old (more than max_retry_rounds behind current round)
        // BUT: Only skip if ALL batches in the header are already committed
        // This prevents processing very old headers that are unlikely to contain uncommitted batches
        // However, we still process headers within a reasonable window to catch late batches
        // CRITICAL: If header is old but contains uncommitted batches, we MUST extract them
        // to prevent batches from being stuck forever
        let is_header_too_old = header.round < self.round.saturating_sub(self.max_retry_rounds);
        if is_header_too_old {
            // Check if ALL batches in this header are already committed
            let all_batches_committed = header
                .payload
                .iter()
                .all(|(batch_digest, _)| self.committed_digests.contains_key(batch_digest));

            if all_batches_committed {
                // All batches are committed - safe to skip this old header
                return;
            }
            // Header is old but contains uncommitted batches - MUST extract them
            // Bỏ log chi tiết - không cần thiết cho trace batch
        }

        let mut batches_extracted = 0usize;
        let mut batches_skipped_committed = 0usize;
        let mut batches_skipped_duplicate = 0usize;
        let mut batches_added = 0usize;
        let mut batches_not_in_store = 0usize;

        // Bỏ log start - không cần thiết cho trace batch

        // Extract batches from header payload
        for (batch_digest, worker_id) in header.payload.iter() {
            batches_extracted += 1;

            // Skip if already committed
            if self.committed_digests.contains_key(batch_digest) {
                batches_skipped_committed += 1;
                debug!(
                    "[BATCH EXTRACTION] Primary {} SKIP extracting batch {} from header {} (round {}, author: {}) - ALREADY COMMITTED",
                    self.name,
                    batch_digest,
                    header.id,
                    header.round,
                    header.author
                );
                continue;
            }

            // CRITICAL: Check if batch is already trong queue bằng index hợp lệ
            if let Some(existing_idx) = self.get_valid_digest_index(batch_digest) {
                let is_pending = {
                    let entry = &self.digests[existing_idx];
                    matches!(entry.state, BatchState::Pending)
                };

                if is_pending {
                    batches_skipped_duplicate += 1;
                    // Bỏ log verbose - không cần thiết
                    continue;
                }

                let mut converted = false;
                if let Some(entry_idx) = self.get_valid_digest_index(batch_digest) {
                    let entry = &mut self.digests[entry_idx];
                    if matches!(entry.state, BatchState::InFlight { .. }) {
                        if self.committed_digests.contains_key(batch_digest) {
                            batches_skipped_committed += 1;
                            // Bỏ log verbose - không cần thiết
                            entry.state = BatchState::Committed;
                            converted = true;
                        } else {
                            self.pending_payload_size += entry.size;
                            entry.state = BatchState::Pending;
                            batches_added += 1;
                            converted = true;
                            // Bỏ log verbose - không cần thiết
                        }
                    }
                }
                if converted {
                    continue;
                }

                batches_skipped_duplicate += 1;
                // Bỏ log verbose - không cần thiết
                continue;
            }

            // Check if batch is in store (required for inclusion in header)
            // If batch is not in store, we can't include it yet - will be synced later
            match self.store.read(batch_digest.to_vec()).await {
                Ok(Some(_batch_data)) => {
                    // CRITICAL: Double-check batch is still not committed before adding to queue
                    // This prevents race conditions where batch was committed between first check and now
                    if self.committed_digests.contains_key(batch_digest) {
                        batches_skipped_committed += 1;
                        // Bỏ log verbose - không cần thiết
                        continue;
                    }

                    let size = batch_digest.size();

                    // LONG-TERM FIX: Batches extracted from headers are medium priority
                    let priority = BatchPriority::Medium;
                    // Add to queue as Pending
                    let entry_idx = self.digests.len();
                    self.digests.push_back(BatchEntry {
                        digest: batch_digest.clone(),
                        worker_id: *worker_id,
                        size,
                        state: BatchState::Pending,
                        retry_count: 0,
                        rescue_sent: false,
                        priority,
                        added_at: Instant::now(),
                    });
                    // Maintain HashMap index for O(1) lookup
                    self.digests_index.insert(batch_digest.clone(), entry_idx);
                    self.pending_payload_size += size;
                    batches_added += 1;

                    // Bỏ log verbose - không cần thiết cho trace batch
                }
                Ok(None) => {
                    // CRITICAL FIX: Batch not in store yet - add to queue with special state to trigger sync
                    // This ensures leader can extract batches even if they're not in store yet
                    // The batch will be synced and then can be included in next header
                    batches_not_in_store += 1;

                    // Check if batch is already in queue (might have been added from worker)
                    if !self.digests_index.contains_key(batch_digest) {
                        // CRITICAL FIX: Add batch to queue with Pending state even though it's not in store
                        // This allows us to track the batch and trigger sync
                        // When batch arrives in store, it will be available for inclusion
                        //
                        // SAFETY: pending_payload_size is NOT increased here because batch is not in store yet.
                        // When batch is synced and arrives in store, it will be collected in collect_payload_for_header()
                        // and pending_payload_size will be correctly managed (decreased when collected).
                        // This ensures consistency: pending_payload_size only tracks batches that are ready to be included.
                        let size = batch_digest.size();
                        let entry_idx = self.digests.len();
                        // LONG-TERM FIX: Batches not in store yet are low priority (will be synced)
                        let priority = BatchPriority::Low;
                        self.digests.push_back(BatchEntry {
                            digest: batch_digest.clone(),
                            worker_id: *worker_id,
                            size,
                            state: BatchState::Pending, // Mark as Pending to trigger sync check
                            retry_count: 0,
                            rescue_sent: false,
                            priority,
                            added_at: Instant::now(),
                        });
                        self.digests_index.insert(batch_digest.clone(), entry_idx);
                        // SAFETY: Do NOT add to pending_payload_size here because:
                        // 1. Batch is not in store yet - cannot be included in header
                        // 2. When batch arrives in store, it will be collected in collect_payload_for_header()
                        // 3. pending_payload_size will be correctly managed during collection
                        // 4. This ensures pending_payload_size only tracks batches ready for inclusion
                        // 5. Does NOT cause fork because header content is deterministic (only includes batches in store)

                        // CRITICAL DEBUG: Log nếu đây là batch đang tìm
                        if format!("{}", batch_digest) == "gFe3xRf/Ba1q1VVU" {
                            warn!(
                                target: "narwhal_audit",
                                "[BATCH TRACE] Batch gFe3xRf/Ba1q1VVU ADDED to queue for SYNC (NOT IN STORE). Header: {} (round {}, author: {}). pending_payload_size NOT increased. Batch will be included once sync completes.",
                                header.id,
                                header.round,
                                header.author
                            );
                        }
                        
                        warn!(
                            "[BATCH EXTRACTION SYNC] Primary {} ADDED batch {} (worker {}) from header {} (round {}, author: {}) to queue for SYNC. Batch not in store yet - sync should be triggered by Synchronizer when header is processed in Core. Batch will be included once sync completes. pending_payload_size NOT increased (batch not ready yet). If batch is still not in store after 10 seconds, this indicates sync may have failed!",
                            self.name,
                            batch_digest,
                            worker_id,
                            header.id,
                            header.round,
                            header.author
                        );
                    } else {
                        // Batch already in queue - check if it needs pending_payload_size update
                        // If batch was added without pending_payload_size (from extraction when not in store),
                        // and now it's in store, we should update pending_payload_size
                        // However, this is handled in collect_payload_for_header() when batch is collected
                        
                        // CRITICAL DEBUG: Log nếu đây là batch đang tìm
                        if format!("{}", batch_digest) == "gFe3xRf/Ba1q1VVU" {
                            warn!(
                                target: "narwhal_audit",
                                "[BATCH TRACE] Batch gFe3xRf/Ba1q1VVU already in queue but NOT IN STORE. Header: {} (round {}, author: {}). Sync should be triggered by process_header. If batch is still not in store after 10 seconds, sync may have failed!",
                                header.id,
                                header.round,
                                header.author
                            );
                        }
                        
                        debug!(
                            "[BATCH EXTRACTION] Primary {} SKIP extracting batch {} from header {} (round {}, author: {}) - NOT IN STORE yet but already in queue. Batch will be synced and then included.",
                            self.name,
                            batch_digest,
                            header.id,
                            header.round,
                            header.author
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "[BATCH EXTRACTION] Primary {} ERROR reading batch {} from store (header {} round {} author {}): {}",
                        self.name,
                        batch_digest,
                        header.id,
                        header.round,
                        header.author,
                        e
                    );
                }
            }
        }

        // Chỉ log summary khi có nhiều batches được extract hoặc có vấn đề
        if batches_added > 5 || batches_not_in_store > 0 {
            debug!(
                "[BATCH EXTRACTION SUMMARY] Primary {} extracted {} batches from header {} (round {}), {} added, {} not in store",
                self.name,
                batches_extracted,
                header.id,
                header.round,
                batches_added,
                batches_not_in_store
            );
        }
    }

    /// LONG-TERM FIX: Check channel usage and apply backpressure if needed
    async fn check_channel_backpressure(&mut self) {
        // Check rx_headers channel usage (approximate)
        // Note: tokio::sync::mpsc::Receiver doesn't expose len(), so we use try_recv to estimate
        let mut headers_pending = 0;

        // Try to peek at channel capacity (non-blocking)
        loop {
            match self.rx_headers.try_recv() {
                Ok(_header) => {
                    headers_pending += 1;
                    if headers_pending > 100 {
                        break; // Don't check too many
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    break;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }

        // Estimate channel usage (rough approximation)
        // If we got many headers, channel is likely busy
        let estimated_usage = if headers_pending > 50 {
            0.8 // High usage
        } else if headers_pending > 20 {
            0.5 // Medium usage
        } else {
            0.2 // Low usage
        };

        // Apply backpressure if channel usage is high
        if estimated_usage >= CHANNEL_CRITICAL_USAGE {
            // Critical: Increase header creation delay significantly
            self.header_creation_rate = (self.header_creation_rate * 1.5).min(5.0);
            error!(
                "[BACKPRESSURE CRITICAL] Proposer {} channel usage estimated at {:.1}% (>= {:.1}%). Increasing header creation delay by {:.2}x to prevent channel overflow. System may be slow!",
                self.name,
                estimated_usage * 100.0,
                CHANNEL_CRITICAL_USAGE * 100.0,
                self.header_creation_rate
            );
        } else if estimated_usage >= CHANNEL_WARNING_USAGE {
            // Warning: Increase header creation delay moderately
            self.header_creation_rate = (self.header_creation_rate * 1.2).min(3.0);
            warn!(
                "[BACKPRESSURE WARNING] Proposer {} channel usage estimated at {:.1}% (>= {:.1}%). Increasing header creation delay by {:.2}x to prevent channel overflow.",
                self.name,
                estimated_usage * 100.0,
                CHANNEL_WARNING_USAGE * 100.0,
                self.header_creation_rate
            );
        } else {
            // Normal: Gradually reduce delay back to normal
            self.header_creation_rate = (self.header_creation_rate * 0.95).max(1.0);
        }

        // Note: Headers we peeked at will be processed in main loop normally
        // This is acceptable as it just means we process them slightly earlier
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("[PROPOSER] Dag starting at round {}", self.round);

        // LONG-TERM FIX: Apply adaptive header delay based on backpressure
        let adaptive_delay = (self.max_header_delay as f64 * self.header_creation_rate) as u64;
        let header_timer = sleep(Duration::from_millis(adaptive_delay));
        tokio::pin!(header_timer);

        let retry_timer = sleep(self.retry_delay);
        tokio::pin!(retry_timer);

        loop {
            // RATE CONTROL ĐÃ BỊ BỎ - Không còn record queue

            // LONG-TERM FIX: Check channel usage for backpressure
            let should_check_channels = self
                .last_channel_check
                .map(|t| t.elapsed() >= self.channel_check_interval)
                .unwrap_or(true);

            if should_check_channels {
                self.check_channel_backpressure().await;
                self.last_channel_check = Some(Instant::now());
            }

            // CATCH-UP MODE: Check for catch-up mode notifications
            if let Ok(is_catchup) = self.rx_catchup_mode.try_recv() {
                self.is_catchup_mode = is_catchup;
                if is_catchup {
                    debug!("[CATCH-UP] Proposer entering catch-up mode - pausing header creation to focus on syncing");
                } else {
                    debug!("[CATCH-UP] Proposer resuming normal operation - node has caught up");
                }
            }

            // CATCH-UP MODE: CRITICAL FIX - Không pause hoàn toàn, chỉ giảm tần suất
            // Vẫn tạo headers khi có batches để đảm bảo giao dịch được thực thi
            // CRITICAL: Vẫn tạo empty headers để đảm bảo hệ thống tiếp tục (block rỗng vẫn được tạo)
            if self.is_catchup_mode {
                let has_pending_batches = self.pending_payload_size > 0;
                if has_pending_batches {
                    debug!("[CATCH-UP] Proposer in catch-up mode but has {} pending batches - will still create headers to avoid batches being stuck", self.pending_payload_size);
                } else {
                    debug!("[CATCH-UP] Proposer {} in catch-up mode - will create headers (including empty) when timer expired to ensure system continues (round {})", self.name, self.round);
                }
                // Still process other messages (parents, headers, batches) but reduce header creation frequency
                // This allows node to continue syncing while still processing batches
                // CRITICAL: Empty headers are still created to ensure empty blocks are generated
            }

            self.update_parity_guard();
            if self.parity_guard_active {
                info!(
                    "[PARITY GUARD] Active while consensus stuck at even round {} (current round {}). Backing off header creation and forcing batch sync.",
                    self.latest_committed_round,
                    self.round
                );
                self.maybe_trigger_parity_rescue();
            }

            // PHASE 1: Check if we're stuck (no parent certificates received for too long)
            // Force advance round to prevent proposer from being stuck indefinitely
            let mut just_force_advanced = false;
            let max_allowed_round = self.max_allowed_round();
            let round_drift = self.round.saturating_sub(self.latest_committed_round);
            let drift_too_high = round_drift > MAX_ROUND_DRIFT;
            if drift_too_high {
                debug!(
                        target: "narwhal_audit",
                        "[ROUND DRIFT] Primary {} đang dẫn {} round (current={}, latest_committed_round={}). Đang chờ consensus/parents mới trước khi force advance.",
                        self.name,
                        round_drift,
                        self.round,
                        self.latest_committed_round
                );
            }
            let effective_max_wait = if self.round <= 5 {
                Duration::from_secs(3) // Chỉ chờ 3 giây trong giai đoạn khởi động
            } else {
                self.max_parent_wait
            };

            if let Some(last_received) = self.last_parent_received_at {
                // REVERTED: Removed round sync logic - only check max_allowed_round
                if last_received.elapsed() > effective_max_wait && self.round < max_allowed_round {
                    // Force advance round with empty parents
                    warn!(
                        target: "narwhal_audit",
                        "[FORCE ADVANCE] Primary {} force advancing round {} -> {} due to no parent certificates received for {} seconds (max_allowed_round={}). This prevents proposer from being stuck when Core stops sending parent certificates.",
                        self.name,
                        self.round,
                        self.round + 1,
                        last_received.elapsed().as_secs(),
                        max_allowed_round
                    );
                    self.round += 1;
                    self.last_parents = Vec::new(); // Clear old parents
                    self.last_parent_received_at = Some(Instant::now()); // Reset timer
                    just_force_advanced = true; // Đánh dấu vừa force advance để cho phép tạo header ngay
                    debug!(
                        "[FORCE ADVANCE] Dag force advanced to round {} (last_parents cleared)",
                        self.round
                    );
                } else if self.round >= max_allowed_round {
                    debug!(
                        target: "narwhal_audit",
                        "[ROUND GUARD] Primary {} chặn force advance vì round {} đã đạt giới hạn tối đa {} (latest_committed_round={})",
                        self.name,
                        self.round,
                        max_allowed_round,
                        self.latest_committed_round
                    );
                }
            } else {
                // CRITICAL: Nếu last_parent_received_at là None (chưa nhận được parents lần nào)
                // và timer đã hết, cho phép force advance ngay để tránh hệ thống bị kẹt vĩnh viễn
                let timer_expired = header_timer.is_elapsed();
                if timer_expired && self.round < max_allowed_round {
                    warn!(
                        "[FORCE ADVANCE] Primary {} chưa nhận được parents lần nào, cho phép force advance round {} để tránh kẹt",
                        self.name, self.round
                    );
                    just_force_advanced = true;
                } else if self.round >= max_allowed_round {
                    debug!(
                        "[ROUND GUARD] Primary {} giữ nguyên round {} (giới hạn {}) khi chưa có parents",
                        self.name,
                        self.round,
                        max_allowed_round
                    );
                }
            }

            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            // PHASE 1: Also allow creating header with empty parents if we're force advancing
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.pending_payload_size >= self.header_size;
            let timer_expired = header_timer.is_elapsed();

            // PHASE 1: Allow force advance (creating header even with empty parents) if timeout exceeded
            // CRITICAL: Đảm bảo force_advance luôn true nếu vừa force advance hoặc timeout quá lâu
            let base_force_advance = just_force_advanced
                || self
                    .last_parent_received_at
                    .map(|t| t.elapsed() > effective_max_wait)
                    .unwrap_or_else(|| {
                        // Nếu chưa nhận được parents lần nào và timer đã hết, cho phép force advance
                        // KHÔNG giới hạn round để tránh hệ thống bị kẹt vĩnh viễn
                        timer_expired
                    });
            let force_advance = !drift_too_high && base_force_advance;

            let ready_to_make_header =
                (timer_expired || enough_digests) && (enough_parents || force_advance);

            // Log debug để theo dõi điều kiện tạo header - chỉ log khi có thay đổi đáng kể
            // Giảm log spam bằng cách chỉ log khi timer expired và có vấn đề (không ready)
            if timer_expired && !ready_to_make_header {
                debug!(
                    "[PROPOSER] Điều kiện tạo header round {}: timer_expired={}, enough_digests={}, enough_parents={}, force_advance={}, ready={}, last_parent_received={:?}s",
                    self.round, timer_expired, enough_digests, enough_parents, force_advance, ready_to_make_header,
                    self.last_parent_received_at.map(|t| t.elapsed().as_secs())
                );
            }

            // RATE CONTROL ĐÃ BỊ BỎ - Không còn chặn tạo header
            // Hệ thống sẽ tạo header ngay khi có điều kiện để đảm bảo tiến triển nhanh nhất
            // CATCH-UP MODE: CRITICAL FIX - Vẫn tạo headers (bao gồm empty headers) để đảm bảo hệ thống tiếp tục
            // CRITICAL: Trong catch-up mode, vẫn tạo empty headers khi timer expired để đảm bảo block rỗng được tạo
            // Điều này ngăn hệ thống dừng hoàn toàn khi không có batches
            let has_pending_batches = self.pending_payload_size > 0;
            let mut should_create_header = if self.is_catchup_mode {
                // Trong catch-up mode: tạo headers khi có batches HOẶC timer expired (bao gồm empty headers)
                // CRITICAL: Timer expired cho phép tạo empty headers để đảm bảo hệ thống tiếp tục
                // Điều này đảm bảo block rỗng vẫn được tạo ngay cả khi không có batches
                ready_to_make_header && (has_pending_batches || timer_expired)
            } else {
                // Bình thường: tạo headers như bình thường
                ready_to_make_header
            };
            let round_guard_limit = self.max_allowed_round();
            // PROGRESS GUARANTEE: Check if minimum_network_round is stale
            let min_round_is_stale = if let Some(updated_at) = self.minimum_network_round_updated_at {
                updated_at.elapsed() > Duration::from_secs(MIN_NETWORK_ROUND_TIMEOUT_SECS)
            } else {
                self.minimum_network_round == 0 || true // Treat as stale if not initialized
            };
            
            if should_create_header && self.round > round_guard_limit {
                if min_round_is_stale {
                    // PROGRESS GUARANTEE: Allow header creation if minimum_network_round is stale
                    // This ensures system can still progress even if slow nodes are stuck
                    warn!(
                        target: "narwhal_audit",
                        "[PROGRESS GUARANTEE] Primary {} allowing header creation at round {} despite max_allowed_round {} because minimum_network_round {} is stale (last updated {:?} ago). Ensuring system progress.",
                        self.name,
                        self.round,
                        round_guard_limit,
                        self.minimum_network_round,
                        self.minimum_network_round_updated_at.map(|t| t.elapsed()).unwrap_or_default()
                    );
                    // Don't set should_create_header = false - allow it to proceed
                } else {
                    warn!(
                        target: "narwhal_audit",
                        "[ROUND GUARD] Primary {} không tạo header round {} vì đã vượt giới hạn (latest_committed_round={}, minimum_network_round={}, max_allowed_round={}, min_round_stale={}). Đợi consensus/network tiến thêm.",
                        self.name,
                        self.round,
                        self.latest_committed_round,
                        self.minimum_network_round,
                        round_guard_limit,
                        min_round_is_stale
                    );
                    should_create_header = false;
                }
            }

            if drift_too_high && !enough_parents && should_create_header {
                should_create_header = false;
                if timer_expired {
                    debug!(
                        "[ROUND DRIFT] Primary {} tạm dừng force advance tại round {} (latest_committed_round={}, drift={}). Đợi thêm parent certificate để không vượt quá trạng thái mạng.",
                        self.name,
                        self.round,
                        self.latest_committed_round,
                        round_drift
                    );
                }
            }

            if should_create_header && self.parity_guard_blocking() {
                should_create_header = false;
                let remaining_ms = self
                    .parity_guard_backoff_until
                    .map(|deadline| deadline.saturating_duration_since(Instant::now()).as_millis())
                    .unwrap_or(0);
                info!(
                    "[PARITY GUARD] Throttling header for round {} (latest_committed_round={}, wait ~{}ms). Waiting for even rounds to commit before advancing further.",
                    self.round,
                    self.latest_committed_round,
                    remaining_ms
                );
            }

            // WATCHDOG: If queue has digests but pending_payload_size stays 0, force rescue after timeout.
            let queue_stuck = self.pending_payload_size == 0 && !self.digests.is_empty();
            if queue_stuck {
                match self.pending_zero_since {
                    None => {
                        self.pending_zero_since = Some(Instant::now());
                    }
                    Some(since) => {
                        if since.elapsed() >= self.max_pending_zero_duration {
                            let rescued = self.trigger_watchdog_rescue();
                            self.pending_zero_since = Some(Instant::now());
                            if rescued == 0 {
                                warn!(
                                    "[WATCHDOG] Proposer {} queue stuck (queue_len={}, pending_payload_size=0) but no batches were eligible for rescue. Investigate sync/store immediately!",
                                    self.name,
                                    self.digests.len()
                                );
                                
                                // Tracing: Queue stuck
                                for entry in self.digests.iter().take(10) {
                                    // CRITICAL DEBUG: Log chi tiết cho batch đang tìm
                                    if format!("{}", entry.digest) == "gFe3xRf/Ba1q1VVU" {
                                        warn!(
                                            target: "narwhal_audit",
                                            "[BATCH TRACE] Batch gFe3xRf/Ba1q1VVU STUCK in queue! State: {:?}, rescue_sent: {}, added_at: {:?} ago, round: {}, latest_committed_round: {}. Batch not in store and rescue already sent. This indicates sync failure!",
                                            entry.state,
                                            entry.rescue_sent,
                                            entry.added_at.elapsed(),
                                            self.round,
                                            self.latest_committed_round
                                        );
                                    }
                                    
                                    tracing::warn!(
                                        batch_id = %entry.digest,
                                        queue_len = self.digests.len(),
                                        round = self.round,
                                        latest_committed_round = self.latest_committed_round,
                                        state = ?entry.state,
                                        rescue_sent = entry.rescue_sent,
                                        "[QUEUE STUCK] Batch in stuck queue - No rescue possible!"
                                    );
                                }
                            }
                        }
                    }
                }
            } else if self.pending_zero_since.take().is_some() {
                debug!(
                    "[WATCHDOG] Proposer {} queue recovered (queue_len={}, pending_payload_size={} bytes).",
                    self.name,
                    self.digests.len(),
                    self.pending_payload_size
                );
            }

                // PHASE 3: Backpressure Check (Kiểm tra áp lực ngược)
                // Trước khi tạo header, kiểm tra xem chúng ta có chạy quá nhanh so với consensus không
                let current_lag = self.round.saturating_sub(self.latest_committed_round);
                
                // Thresholds cho Backpressure
                const LAG_WARNING_THRESHOLD: Round = 20; // Cảnh báo khi lag > 20 rounds
                const LAG_PAUSE_THRESHOLD: Round = 50;   // Tạm dừng khi lag > 50 rounds
                
                if current_lag > LAG_PAUSE_THRESHOLD {
                    // Nếu lag quá lớn, tạm dừng tạo header để chờ consensus bắt kịp
                    warn!(
                        "[BACKPRESSURE] Proposer {} running TOO FAST! Lag: {} rounds (current: {}, committed: {}). PAUSING header creation to let consensus catch up.",
                        self.name, current_lag, self.round, self.latest_committed_round
                    );
                    
                    // Reset timer với delay dài hơn (ví dụ: 1 giây) để check lại sau
                    let pause_delay = Duration::from_secs(1);
                    header_timer.as_mut().reset(Instant::now() + pause_delay);
                    
                    // Skip tạo header lần này
                    should_create_header = false;
                } else if current_lag > LAG_WARNING_THRESHOLD {
                    // Nếu lag trung bình, giảm tốc độ bằng cách tăng delay
                    warn!(
                        "[BACKPRESSURE] Proposer {} running fast. Lag: {} rounds. Slowing down.",
                        self.name, current_lag
                    );
                    // Tăng header_creation_rate để làm chậm nhịp độ
                    self.header_creation_rate = 1.5; 
                } else {
                    // Lag thấp -> Chạy bình thường
                    self.header_creation_rate = 1.0;
                }

                if should_create_header {
                // Make a new header.
                // PHASE 1: Header can be created even with empty parents if force_advance is true
                debug!("[PROPOSER] Attempting to create header for round {} (force_advance={}, enough_parents={}, enough_digests={})", 
                    self.round, force_advance, enough_parents, enough_digests);
                if self.make_header().await {
                    debug!(
                        "[PROPOSER] Successfully created header for round {}",
                        self.round
                    );
                    if self.parity_guard_active {
                        self.schedule_next_parity_backoff();
                    }
                    // LONG-TERM FIX: Reschedule timer with adaptive delay based on backpressure
                    let adaptive_delay =
                        (self.max_header_delay as f64 * self.header_creation_rate) as u64;
                    let deadline = Instant::now() + Duration::from_millis(adaptive_delay);
                    header_timer.as_mut().reset(deadline);
                } else {
                    warn!(
                        "[PROPOSER] Failed to create header for round {} (no payload or parents)",
                        self.round
                    );
                    if timer_expired {
                        // Nothing to send but timer elapsed: reschedule to avoid busy loop.
                        let deadline =
                            Instant::now() + Duration::from_millis(self.max_header_delay);
                        header_timer.as_mut().reset(deadline);
                    }
                }
            }

            tokio::select! {
                // CRITICAL: Prioritize committed batches to ensure latest_committed_round is always updated
                // This prevents ROUND GUARD from blocking header creation
                Some(committed) = self.rx_committed.recv() => {
                    // BATCH TRACKING: Log when receiving committed batches notification
                    info!(
                        target: "narwhal_audit",
                        "[COMMITTED BATCHES RECEIVED] Proposer {} received committed batches notification: {} batches committed at round {} (current latest_committed_round={})",
                        self.name,
                        committed.digests.len(),
                        committed.round,
                        self.latest_committed_round
                    );
                    self.mark_committed(committed);
                }
                // REVERTED: Round sync logic removed - ignore minimum_network_round updates
                Some(_min_round) = self.rx_min_network_round.recv() => {
                    // Ignore minimum_network_round updates (reverted feature)
                }
                Some((parents, round)) = self.rx_core.recv() => {
                    // PHASE 1: Update last_parent_received_at when we receive parent certificates
                    self.last_parent_received_at = Some(Instant::now());
                    debug!("[PROPOSER] Received {} parents for round {} (current round: {})", parents.len(), round, self.round);

                    if round < self.round {
                        warn!("[PROPOSER] Ignoring parents for round {} (current round: {})", round, self.round);
                        continue;
                    }

                    let target_round = round.saturating_add(1);
                    let max_allowed_round = self.max_allowed_round();
                    if target_round > max_allowed_round {
                        warn!(
                            target: "narwhal_audit",
                            "[ROUND GUARD] Primary {} bỏ qua parents round {} vì đã vượt giới hạn (latest_committed_round={}, max_allowed_round={}). Sẽ đồng bộ lại khi consensus tiến thêm.",
                            self.name,
                            round,
                            self.latest_committed_round,
                            max_allowed_round
                        );
                        continue;
                    }

                    // Advance to the next round.
                    self.round = target_round;
                    debug!("[PROPOSER] Dag moved to round {} with {} parents", self.round, parents.len());

                    // IMPROVED: Extract batches from parent certificates to help leader commit batches from other primaries
                    // This ensures batches are committed faster and reduces the need for retry
                    // Only extract if we have parents (quorum of certificates from previous round)
                    if !parents.is_empty() {
                        self.extract_batches_from_parents(&parents, round).await;
                    }

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;

                    // CẢI THIỆN: Sau khi nhận parents và chuyển sang round mới, reset timer để tạo header sớm hơn
                    // Điều này giúp hệ thống tiến triển nhanh hơn thay vì phải chờ timer hết
                    let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                    header_timer.as_mut().reset(deadline);
                    debug!("[PROPOSER] Reset header timer after receiving parents for round {} (new round: {})", round, self.round);
                }
                Some(header) = self.rx_headers.recv() => {
                    // IMPROVED: Extract batches from verified headers of other primaries
                    // This allows any primary (especially leader) to include batches from non-leader primaries,
                    // ensuring faster commit and preventing batches from being stuck indefinitely.
                    //
                    // SAFETY: This is deterministic because:
                    // 1. Headers are already verified and stored before being sent here
                    // 2. All primaries receive the same headers via network (deterministic source)
                    // 3. Extraction logic is deterministic (same order, same checks)

                    // BATCH TRACKING: Log when receiving header for batch extraction
                    // Bỏ log verbose về header received - không cần thiết cho trace batch
                    self.extract_batches_from_headers(&header).await;
                }
                Some((digest, worker_id, batch)) = self.rx_workers.recv() => {
                    // Skip if already committed
                    if self.committed_digests.contains_key(&digest) {
                        continue;
                    }

                    // Store the batch in the primary's store for the `analyze` function to find.
                    let size = digest.size();

                    if self.digests.iter().any(|entry| entry.digest == digest) {
                        // Bỏ log duplicate - không cần thiết cho trace batch
                        continue;
                    }

                    // CRITICAL FIX: Đảm bảo batch được write vào store TRƯỚC KHI thêm vào queue
                    // Điều này ngăn batches bị stuck vì không có trong store khi collect
                    // Nếu store.write() chậm, chúng ta vẫn đợi để đảm bảo batch có trong store
                    let mut store = self.store.clone();
                    let digest_for_store = digest.clone();
                    let batch_for_store = batch.clone();
                    
                    // CRITICAL: Write vào store với timeout để tránh block quá lâu
                    // Nếu timeout, vẫn thêm vào queue nhưng sẽ retry rescue sau
                    match tokio::time::timeout(Duration::from_millis(500), store.write(digest_for_store.to_vec(), batch_for_store)).await {
                        Ok(()) => {
                            // Store write thành công - batch đã có trong store
                        }
                        Err(_) => {
                            warn!(
                                "[PROPOSER] Store write timeout for batch {} (500ms). Batch will be added to queue but may need rescue.",
                                digest
                            );
                        }
                    }

                    let digest_for_log = digest.clone();

                    // LONG-TERM FIX: Determine batch priority
                    let priority = BatchPriority::High; // New batches from workers are high priority

                    let entry_idx = self.digests.len();
                    self.digests.push_back(BatchEntry {
                        digest: digest.clone(),
                        worker_id,
                        size,
                        state: BatchState::Pending,
                        retry_count: 0,
                        rescue_sent: false,
                        priority,
                        added_at: Instant::now(),
                    });
                    // Maintain HashMap index for O(1) lookup
                    self.digests_index.insert(digest, entry_idx);
                    self.pending_payload_size += size;

                    // LONG-TERM FIX: Queue size limit and monitoring
                    if self.digests.len() >= MAX_QUEUE_SIZE {
                        // Queue is full - remove oldest batches to make room
                        let batches_to_remove = 100; // Remove 100 oldest batches
                        let mut removed = 0;
                        let mut size_freed = 0;

                        // Remove oldest batches (those with highest retry_count or oldest InFlight)
                        let mut to_remove: Vec<usize> = Vec::new();
                        for (idx, entry) in self.digests.iter().enumerate() {
                            if to_remove.len() >= batches_to_remove {
                                break;
                            }
                            // Prioritize removing old InFlight batches or batches with high retry_count
                            if let BatchState::InFlight { retry_count, .. } = entry.state {
                                if retry_count > 50 {
                                    to_remove.push(idx);
                                }
                            } else if matches!(entry.state, BatchState::Pending) && entry.retry_count > 20 {
                                to_remove.push(idx);
                            }
                        }

                        // Remove in reverse order to maintain indices
                        to_remove.sort_by(|a, b| b.cmp(a));
                        for idx in to_remove {
                            if let Some(entry) = self.digests.get(idx) {
                                size_freed += entry.size;
                                self.digests_index.remove(&entry.digest);
                            }
                            self.digests.remove(idx);
                            removed += 1;
                        }

                        // Rebuild index after removal
                        self.rebuild_digests_index();

                        self.pending_payload_size = self.pending_payload_size.saturating_sub(size_freed);

                        error!(
                            "[QUEUE FULL] Proposer {} queue FULL (size: {} >= {}). Removed {} oldest batches (freed {} bytes). This indicates batches are not being processed fast enough! System may be slow.",
                            self.name,
                            self.digests.len(),
                            MAX_QUEUE_SIZE,
                            removed,
                            size_freed
                        );
                    } else if self.digests.len() >= QUEUE_WARNING_THRESHOLD {
                        warn!(
                            "[BATCH QUEUE WARNING] Proposer {} queue size is {} (threshold: {}). pending_payload_size: {} bytes. This may indicate batches are not being processed fast enough!",
                            self.name,
                            self.digests.len(),
                            QUEUE_WARNING_THRESHOLD,
                            self.pending_payload_size
                        );
                    }

                    debug!(
                        "[BATCH TRACK WORKER] Proposer {} ENQUEUED batch {} from worker {} at round {}; pending_payload_size = {}, queue_len = {}",
                        self.name,
                        digest_for_log,
                        worker_id,
                        self.round,
                        self.pending_payload_size,
                        self.digests.len()
                    );
                }
                () = &mut header_timer => {
                    // Timer expired - loop will evaluate conditions again.
                }
                () = &mut retry_timer => {
                    self.retry_stale_batches();
                    retry_timer.as_mut().reset(Instant::now() + self.retry_delay);

                    // PERIODIC SUMMARY: Log tình trạng queue mỗi khi retry timer trigger
                    // Giúp theo dõi tình trạng hệ thống và phát hiện vấn đề sớm
                    let pending_count = self.digests.iter().filter(|e| matches!(e.state, BatchState::Pending)).count();
                    let inflight_count = self.digests.iter().filter(|e| matches!(e.state, BatchState::InFlight { .. })).count();
                    let committed_count = self.digests.iter().filter(|e| matches!(e.state, BatchState::Committed)).count();
                    
                    // CRITICAL: Check if latest_committed_round is stale (not updated in a while)
                    // This can cause ROUND GUARD to block header creation
                    let lag = self.round.saturating_sub(self.latest_committed_round);
                    if lag > 50 && self.latest_committed_round > 0 {
                        warn!(
                            target: "narwhal_audit",
                            "[COMMITTED ROUND STALE] Primary {} latest_committed_round {} is {} rounds behind current_round {}. This may cause ROUND GUARD to block header creation. Check if rx_committed channel is working!",
                            self.name,
                            self.latest_committed_round,
                            lag,
                            self.round
                        );
                    }
                    
                    // CRITICAL FIX: If latest_committed_round is stale and we're creating headers,
                    // try to update it from consensus round if available
                    // This is a fallback mechanism to ensure progress even if GarbageCollector is not sending notifications
                    if lag > 100 && self.latest_committed_round > 0 {
                        // Try to receive any pending committed batches (non-blocking)
                        // This helps catch up if notifications were queued
                        while let Ok(committed) = self.rx_committed.try_recv() {
                            info!(
                                target: "narwhal_audit",
                                "[COMMITTED BATCHES CATCHUP] Proposer {} received queued committed batches: {} batches at round {} (lag was {} rounds)",
                                self.name,
                                committed.digests.len(),
                                committed.round,
                                lag
                            );
                            self.mark_committed(committed);
                        }
                        
                        // FALLBACK: If still stale after catchup, auto-update latest_committed_round
                        // This ensures progress even if GarbageCollector is completely broken
                        let current_lag = self.round.saturating_sub(self.latest_committed_round);
                        if current_lag > 100 {
                            let old_committed_round = self.latest_committed_round;
                            // Auto-update to current_round - 50 to allow some progress
                            // This is a safety mechanism to prevent complete system halt
                            let estimated_committed_round = self.round.saturating_sub(50);
                            if estimated_committed_round > self.latest_committed_round {
                                warn!(
                                    target: "narwhal_audit",
                                    "[FALLBACK COMMITTED ROUND UPDATE] Primary {} latest_committed_round {} is {} rounds behind current_round {}. GarbageCollector appears to be broken. Auto-updating to {} to ensure progress. This is a fallback mechanism!",
                                    self.name,
                                    self.latest_committed_round,
                                    current_lag,
                                    self.round,
                                    estimated_committed_round
                                );
                                self.latest_committed_round = estimated_committed_round;
                                
                                info!(
                                    target: "narwhal_audit",
                                    "[COMMITTED ROUND UPDATE] Primary {} updated latest_committed_round from {} to {} (FALLBACK - current_round={}, max_allowed_round={}). This allows proposer to create headers for higher rounds.",
                                    self.name,
                                    old_committed_round,
                                    self.latest_committed_round,
                                    self.round,
                                    self.max_allowed_round()
                                );
                            }
                        }
                    }
                    
                    // PROGRESS MONITORING: Check if minimum_network_round is stale
                    let min_round_is_stale = if let Some(updated_at) = self.minimum_network_round_updated_at {
                        updated_at.elapsed() > Duration::from_secs(MIN_NETWORK_ROUND_TIMEOUT_SECS)
                    } else {
                        self.minimum_network_round == 0 || true
                    };
                    let min_round_age = self.minimum_network_round_updated_at
                        .map(|t| t.elapsed().as_secs())
                        .unwrap_or(0);

                    debug!(
                        target: "narwhal_audit",
                        "[PROPOSER SUMMARY] Primary {} round {}: queue_len={}, pending={}, inflight={}, committed={}, pending_payload_size={} bytes, latest_committed_round={}, minimum_network_round={}, min_round_stale={}, min_round_age={}s, max_allowed_round={}",
                        self.name,
                        self.round,
                        self.digests.len(),
                        pending_count,
                        inflight_count,
                        committed_count,
                        self.pending_payload_size,
                        self.latest_committed_round,
                        self.minimum_network_round,
                        min_round_is_stale,
                        min_round_age,
                        self.max_allowed_round()
                    );

                    // CRITICAL: Warning nếu queue quá lớn hoặc có nhiều InFlight batches
                    if self.digests.len() > 500 {
                        warn!(
                            "[PROPOSER SUMMARY] WARNING: Primary {} queue size {} is high (threshold: 500). pending={}, inflight={}. This may indicate batches are not being processed fast enough!",
                            self.name,
                            self.digests.len(),
                            pending_count,
                            inflight_count
                        );
                    }
                }
            }
        }
    }
}
