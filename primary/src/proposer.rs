// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::{BatchRescue, CommittedBatches, Round};
// RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, info, warn};
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
}

const RESCUE_RETRY_THRESHOLD: usize = 25;

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
}

impl Proposer {
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
                committed_digests: HashMap::new(),
                max_retry_rounds: 1000, // Don't retry batches that have been InFlight for more than 1000 rounds
                max_committed_digests: 10000, // Cleanup old digests when this limit is reached
                last_parent_received_at: Some(Instant::now()), // Initialize with current time
                max_parent_wait: Duration::from_secs(10), // Force advance after 10 seconds without parent certificates
                // RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
                // CATCH-UP MODE: Initialize catch-up mode state
                rx_catchup_mode,
                is_catchup_mode: false,
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

        let payload: Vec<(Digest, WorkerId)> = self.collect_payload_for_header();

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
                "No payload to include in header for round {} (pending_payload_size = {}, queue_len = {})",
                self.round,
                self.pending_payload_size,
                self.digests.len()
            );
            // Still create an empty header if we have parents (for round advancement)
            // This is needed for empty rounds where no batches are available
            // PHASE 1: Also allow creating empty header with empty parents if force advance is enabled
            // This prevents proposer from being stuck when no parent certificates are received
            let force_advance = self
                .last_parent_received_at
                .map(|t| t.elapsed() > self.max_parent_wait)
                .unwrap_or(false);

            if !self.last_parents.is_empty() || force_advance {
                // PHASE 1: Log warning if creating header with empty parents due to force advance
                if self.last_parents.is_empty() && force_advance {
                    warn!(
                        "[FORCE ADVANCE HEADER] Creating header for round {} with EMPTY parents due to force advance. This header may not be committed by consensus (requires quorum parents), but allows proposer to continue and avoid being stuck.",
                        self.round
                    );
                }

                let parents_for_header = if self.last_parents.is_empty() {
                    BTreeSet::new() // Empty parents when force advance
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
                debug!("Created empty header {:?}", header);
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
            "Creating header for round {} with {} payload digests (pending_payload_size before send = {}, queue_len = {})",
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
        info!(
            "[BATCH TRACK HEADER] Primary {} creating header for round {} with {} batches: {:?}",
            self.name,
            self.round,
            batch_digests.len(),
            batch_digests
        );

        let header = Header::new(
            self.name,
            self.round,
            deduplicated_payload.into_iter().collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

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

    fn collect_payload_for_header(&mut self) -> Vec<(Digest, WorkerId)> {
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

        for entry in self.digests.iter_mut() {
            if accumulated_size >= self.header_size {
                break;
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

                accumulated_size += entry.size;
                seen_digests.insert(entry.digest.clone());
                collected.push((entry.digest.clone(), entry.worker_id));
                info!(
                    "[BATCH TRACK PRIMARY] Primary {} COLLECTING batch {} from worker {} for header round {} (size {} bytes, retry_count={}). Accumulated payload = {} / target {} bytes",
                    self.name,
                    entry.digest,
                    entry.worker_id,
                    self.round,
                    entry.size,
                    entry.retry_count,
                    accumulated_size,
                    self.header_size
                );
                entry.state = BatchState::InFlight {
                    round: self.round,
                    sent_at: Instant::now(),
                    retry_count: entry.retry_count,
                };
                self.pending_payload_size = self.pending_payload_size.saturating_sub(entry.size);
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

        collected
    }

    fn mark_committed(&mut self, committed: CommittedBatches) {
        // BATCH TRACKING: Log when batches are marked as committed
        if !committed.digests.is_empty() {
            info!(
                "[BATCH COMMIT] Primary {} marking {} batches as committed at round {}: {:?}",
                self.name,
                committed.digests.len(),
                committed.round,
                committed.digests.iter().take(10).collect::<Vec<_>>()
            );
        }
        
        self.latest_committed_round = self.latest_committed_round.max(committed.round);

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
        let mut inflight_count = 0;

        for entry in self.digests.iter_mut() {
            if committed_set.contains(&entry.digest) {
                // Mark as committed regardless of current state
                // This prevents InFlight batches from being retried later
                if matches!(entry.state, BatchState::Pending) {
                    self.pending_payload_size =
                        self.pending_payload_size.saturating_sub(entry.size);
                } else if matches!(entry.state, BatchState::InFlight { .. }) {
                    inflight_count += 1;
                }
                entry.state = BatchState::Committed;
                marked_count += 1;
            }
        }

        if marked_count > 0 {
            info!(
                "Mark committed notification for round {}: {} batches transitioned ({} were InFlight)",
                committed.round, marked_count, inflight_count
            );
        }

        // Remove committed batches from queue
        self.digests
            .retain(|entry| !matches!(entry.state, BatchState::Committed));
        
        // Rebuild index after removing committed batches (indices may have shifted)
        // This is O(n) but only happens when batches are committed, not on every extraction
        self.digests_index.clear();
        for (idx, entry) in self.digests.iter().enumerate() {
            self.digests_index.insert(entry.digest.clone(), idx);
        }
    }

    /// Cleanup old committed digests to prevent unbounded memory growth.
    /// Removes digests from rounds that are older than (latest_committed_round - max_retry_rounds * 2).
    /// This ensures we keep enough history to check for committed batches during retry logic.
    fn cleanup_old_committed_digests(&mut self) {
        // Only cleanup if we've exceeded the limit
        if self.committed_digests.len() <= self.max_committed_digests {
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
                const MAX_RETRY_COUNT: usize = 1000; // Nếu retry > 1000, batch bị stuck forever
                let is_stuck_forever = retry_count > MAX_RETRY_COUNT;

                if is_stuck_forever {
                    // Force remove batch - hệ thống không thể commit batch này
                    // Đây là biện pháp cuối cùng để ngăn chặn hệ thống bị đứng vĩnh viễn
                    warn!(
                        "[BATCH STUCK] Primary {} FORCE REMOVING stuck batch {} (retry_count={}, sent_round={}, current_round={}, rounds_since_sent={}, latest_committed_round={}). Batch cannot be committed after {} retries - removing to prevent system deadlock. Transactions in this batch will be LOST.",
                        self.name,
                        entry.digest,
                        retry_count,
                        round,
                        self.round,
                        rounds_since_sent,
                        self.latest_committed_round,
                        MAX_RETRY_COUNT
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
                    info!(
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
                match store.read(digest.to_vec()).await {
                    Ok(Some(batch)) => {
                        if let Err(e) = tx
                            .send(BatchRescue {
                                digest: digest.clone(),
                                worker_id,
                                batch,
                                origin: origin.clone(),
                            })
                            .await
                        {
                            warn!(
                                "[BATCH RESCUE] Failed to send rescue request for batch {}: {}",
                                digest, e
                            );
                            break;
                        }
                    }
                    Ok(None) => {
                        warn!(
                            "[BATCH RESCUE] Unable to rescue batch {} - payload missing from store",
                            digest
                        );
                    }
                    Err(e) => {
                        warn!(
                            "[BATCH RESCUE] Error reading batch {} from store for rescue: {}",
                            digest, e
                        );
                    }
                }
            }
        });
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
                                            info!(
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
                                        self.digests.push_back(BatchEntry {
                                            digest: batch_digest.clone(),
                                            worker_id: *worker_id,
                                            size,
                                            state: BatchState::Pending,
                                            retry_count: 0,
                                            rescue_sent: false,
                                        });
                                        // Maintain HashMap index for O(1) lookup
                                        self.digests_index.insert(batch_digest.clone(), entry_idx);
                                        self.pending_payload_size += size;
                                        batches_added += 1;

                                        info!(
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
            info!(
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
    /// This allows any primary (especially leader) to include batches from non-leader primaries,
    /// ensuring faster commit and preventing batches from being stuck indefinitely.
    ///
    /// SAFETY: This method is deterministic because:
    /// 1. Headers are already verified and stored before being sent here
    /// 2. All primaries receive the same headers via network (deterministic source)
    /// 3. Extraction logic is deterministic (same order, same checks)
    /// 4. Only batches from other primaries are extracted (skip own headers)
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
                if let Some(entry_idx) = self.digests_index.get(batch_digest) {
                    let entry = &self.digests[*entry_idx];
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
                debug!(
                    "[BATCH EXTRACTION] Primary {} SKIP extracting from own header {} (round {}) - all batches are committed or in Pending state",
                    self.name,
                    header.id,
                    header.round
                );
                return;
            } else {
                // Some batches are in InFlight state - continue to extract them
                // This allows leader to convert InFlight batches to Pending for immediate inclusion
                info!(
                    "[BATCH EXTRACTION] Primary {} PROCESSING own header {} (round {}) - contains InFlight batches that need to be converted to Pending. This allows leader to include batches even if own certificate wasn't committed.",
                    self.name,
                    header.id,
                    header.round
                );
            }
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
                debug!(
                    "[BATCH EXTRACTION] Primary {} SKIP extracting from header {} (round {}, author: {}) - TOO OLD and all batches committed (current round: {}). Header is more than {} rounds behind.",
                    self.name,
                    header.id,
                    header.round,
                    header.author,
                    self.round,
                    self.max_retry_rounds
                );
                return;
            } else {
                // Header is old but contains uncommitted batches - MUST extract them
                // This prevents batches from being stuck forever
                info!(
                    "[BATCH EXTRACTION] Primary {} PROCESSING old header {} (round {}, author: {}) - contains uncommitted batches (current round: {}). Header is more than {} rounds behind but batches are not committed yet.",
                    self.name,
                    header.id,
                    header.round,
                    header.author,
                    self.round,
                    self.max_retry_rounds
                );
                // Continue to extract batches from this old header
            }
        }

        let mut batches_extracted = 0usize;
        let mut batches_skipped_committed = 0usize;
        let mut batches_skipped_duplicate = 0usize;
        let mut batches_added = 0usize;
        let mut batches_not_in_store = 0usize;

        // BATCH TRACKING: Log start of extraction
        info!(
            "[BATCH EXTRACTION START] Primary {} starting extraction from header {} (round {}, author: {}). Header contains {} batches. Current round: {}, max_retry_rounds: {}",
            self.name,
            header.id,
            header.round,
            header.author,
            header.payload.len(),
            self.round,
            self.max_retry_rounds
        );
        
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

            // CRITICAL: Check if batch is already in queue using O(1) HashMap lookup
            // OPTIMIZATION: Use HashMap index instead of linear search for 1000x speedup
            let already_in_queue = self.digests_index.contains_key(batch_digest);
            if already_in_queue {
                // Get index from HashMap for O(1) access
                let entry_idx = *self.digests_index.get(batch_digest).unwrap();
                let entry = &self.digests[entry_idx];
                // Check if batch is in Pending state
                let is_pending = matches!(entry.state, BatchState::Pending);

                if is_pending {
                    // Batch is already in Pending state - skip extraction
                    batches_skipped_duplicate += 1;
                    debug!(
                        "[BATCH EXTRACTION] Primary {} SKIP extracting batch {} from header {} (round {}, author: {}) - ALREADY IN QUEUE (Pending state)",
                        self.name,
                        batch_digest,
                        header.id,
                        header.round,
                        header.author
                    );
                    continue;
                }

                // If batch is in InFlight state, convert it back to Pending state
                // This allows this primary (possibly leader) to include batch immediately,
                // reducing the need for retry and making the system smoother
                // OPTIMIZATION: Use HashMap index for O(1) access instead of iterating
                let mut converted = false;
                if let Some(entry_idx) = self.digests_index.get(batch_digest) {
                    let entry = &mut self.digests[*entry_idx];
                    if matches!(entry.state, BatchState::InFlight { .. }) {
                        // CRITICAL: Double-check batch is still not committed before converting
                        // This prevents race conditions where batch was committed between first check and now
                        if self.committed_digests.contains_key(batch_digest) {
                            batches_skipped_committed += 1;
                            debug!(
                                "[BATCH EXTRACTION] Primary {} SKIP converting batch {} from header {} (round {}, author: {}) - BECAME COMMITTED during conversion check (race condition prevented)",
                                self.name,
                                batch_digest,
                                header.id,
                                header.round,
                                header.author
                            );
                            // Mark as committed and skip this batch
                            entry.state = BatchState::Committed;
                            converted = true; // Set to true to skip adding this batch
                        } else {
                            // Convert InFlight to Pending to allow immediate inclusion
                            // SAFETY: pending_payload_size was decreased when Pending -> InFlight,
                            // so increasing it back is correct
                            self.pending_payload_size += entry.size;
                            entry.state = BatchState::Pending;
                            batches_added += 1; // Count as added (converted from InFlight)
                            converted = true;
                            info!(
                                "[BATCH EXTRACTION] Primary {} CONVERTED batch {} from header {} (round {}, author: {}) from InFlight to Pending to allow immediate inclusion. Batch from non-leader primary can now be committed by this primary.",
                                self.name,
                                batch_digest,
                                header.id,
                                header.round,
                                header.author
                            );
                        }
                    }
                }
                if converted {
                    // If converted (either InFlight->Pending or marked as Committed), skip to next batch
                    continue;
                }

                // Batch is in queue but not in InFlight state - skip
                batches_skipped_duplicate += 1;
                debug!(
                    "[BATCH EXTRACTION] Primary {} SKIP extracting batch {} from header {} (round {}, author: {}) - ALREADY IN QUEUE",
                    self.name,
                    batch_digest,
                    header.id,
                    header.round,
                    header.author
                );
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
                        debug!(
                            "[BATCH EXTRACTION] Primary {} SKIP extracting batch {} from header {} (round {}, author: {}) - BECAME COMMITTED during store read (race condition prevented)",
                            self.name,
                            batch_digest,
                            header.id,
                            header.round,
                            header.author
                        );
                        continue;
                    }

                    let size = batch_digest.size();

                    // Add to queue as Pending
                    let entry_idx = self.digests.len();
                    self.digests.push_back(BatchEntry {
                        digest: batch_digest.clone(),
                        worker_id: *worker_id,
                        size,
                        state: BatchState::Pending,
                        retry_count: 0,
                        rescue_sent: false,
                    });
                    // Maintain HashMap index for O(1) lookup
                    self.digests_index.insert(batch_digest.clone(), entry_idx);
                    self.pending_payload_size += size;
                    batches_added += 1;

                    info!(
                        "[BATCH EXTRACTION SUCCESS] Primary {} EXTRACTED batch {} (worker {}) from header {} (round {}, author: {}) into queue. Batch from non-leader primary can now be committed by this primary. Batch size: {} bytes, queue_len: {}, pending_payload_size: {}",
                        self.name,
                        batch_digest,
                        worker_id,
                        header.id,
                        header.round,
                        header.author,
                        size,
                        self.digests.len(),
                        self.pending_payload_size
                    );
                }
                Ok(None) => {
                    // Batch not in store yet - will be synced later
                    batches_not_in_store += 1;
                    debug!(
                        "[BATCH EXTRACTION] Primary {} SKIP extracting batch {} from header {} (round {}, author: {}) - NOT IN STORE yet. Batch will be extracted again after sync.",
                        self.name,
                        batch_digest,
                        header.id,
                        header.round,
                        header.author
                    );
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

        // BATCH TRACKING: Always log extraction summary (even if 0 batches extracted)
        info!(
            "[BATCH EXTRACTION SUMMARY] Primary {} completed extraction from header {} (round {}, author: {}): {} total batches in header, {} extracted, {} added to queue, {} skipped (committed), {} skipped (duplicate), {} not in store. Current round: {}, queue_len: {}, pending_payload_size: {}",
            self.name,
            header.id,
            header.round,
            header.author,
            header.payload.len(),
            batches_extracted,
            batches_added,
            batches_skipped_committed,
            batches_skipped_duplicate,
            batches_not_in_store,
            self.round,
            self.digests.len(),
            self.pending_payload_size
        );
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        info!("[PROPOSER] Dag starting at round {}", self.round);

        let header_timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(header_timer);

        let retry_timer = sleep(self.retry_delay);
        tokio::pin!(retry_timer);

        loop {
            // RATE CONTROL ĐÃ BỊ BỎ - Không còn record queue
            
            // CATCH-UP MODE: Check for catch-up mode notifications
            if let Ok(is_catchup) = self.rx_catchup_mode.try_recv() {
                self.is_catchup_mode = is_catchup;
                if is_catchup {
                    info!("[CATCH-UP] Proposer entering catch-up mode - pausing header creation to focus on syncing");
                } else {
                    info!("[CATCH-UP] Proposer resuming normal operation - node has caught up");
                }
            }

            // CATCH-UP MODE: Skip creating headers if in catch-up mode
            if self.is_catchup_mode {
                debug!("[CATCH-UP] Proposer paused - node is catching up, skipping header creation for round {}", self.round);
                // Still process other messages (parents, headers, batches) but don't create new headers
                // This allows node to continue syncing while paused
            }

            // PHASE 1: Check if we're stuck (no parent certificates received for too long)
            // Force advance round to prevent proposer from being stuck indefinitely
            let mut just_force_advanced = false;
            let effective_max_wait = if self.round <= 5 {
                Duration::from_secs(3) // Chỉ chờ 3 giây trong giai đoạn khởi động
            } else {
                self.max_parent_wait
            };
            
            if let Some(last_received) = self.last_parent_received_at {
                if last_received.elapsed() > effective_max_wait {
                    // Force advance round with empty parents
                    warn!(
                        "[FORCE ADVANCE] Primary {} force advancing round {} -> {} due to no parent certificates received for {} seconds. This prevents proposer from being stuck when Core stops sending parent certificates.",
                        self.name,
                        self.round,
                        self.round + 1,
                        last_received.elapsed().as_secs()
                    );
                    self.round += 1;
                    self.last_parents = Vec::new(); // Clear old parents
                    self.last_parent_received_at = Some(Instant::now()); // Reset timer
                    just_force_advanced = true; // Đánh dấu vừa force advance để cho phép tạo header ngay
                    debug!(
                        "[FORCE ADVANCE] Dag force advanced to round {} (last_parents cleared)",
                        self.round
                    );
                }
            } else {
                // CRITICAL: Nếu last_parent_received_at là None (chưa nhận được parents lần nào)
                // và timer đã hết, cho phép force advance ngay để tránh hệ thống bị kẹt vĩnh viễn
                let timer_expired = header_timer.is_elapsed();
                if timer_expired {
                    warn!(
                        "[FORCE ADVANCE] Primary {} chưa nhận được parents lần nào, cho phép force advance round {} để tránh kẹt",
                        self.name, self.round
                    );
                    just_force_advanced = true;
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
            let force_advance = just_force_advanced || self
                .last_parent_received_at
                .map(|t| t.elapsed() > effective_max_wait)
                .unwrap_or_else(|| {
                    // Nếu chưa nhận được parents lần nào và timer đã hết, cho phép force advance
                    // KHÔNG giới hạn round để tránh hệ thống bị kẹt vĩnh viễn
                    timer_expired
                });

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
            // CATCH-UP MODE: Don't create headers when in catch-up mode
            let should_create_header = ready_to_make_header && !self.is_catchup_mode;

            if should_create_header {
                // Make a new header.
                // PHASE 1: Header can be created even with empty parents if force_advance is true
                info!("[PROPOSER] Attempting to create header for round {} (force_advance={}, enough_parents={}, enough_digests={})", 
                    self.round, force_advance, enough_parents, enough_digests);
                if self.make_header().await {
                    info!("[PROPOSER] Successfully created header for round {}", self.round);
                    // Reschedule the timer.
                    let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                    header_timer.as_mut().reset(deadline);
                } else {
                    warn!("[PROPOSER] Failed to create header for round {} (no payload or parents)", self.round);
                    if timer_expired {
                        // Nothing to send but timer elapsed: reschedule to avoid busy loop.
                        let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                        header_timer.as_mut().reset(deadline);
                    }
                }
            }

            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    // PHASE 1: Update last_parent_received_at when we receive parent certificates
                    self.last_parent_received_at = Some(Instant::now());
                    info!("[PROPOSER] Received {} parents for round {} (current round: {})", parents.len(), round, self.round);

                    if round < self.round {
                        warn!("[PROPOSER] Ignoring parents for round {} (current round: {})", round, self.round);
                        continue;
                    }

                    // Advance to the next round.
                    self.round = round + 1;
                    info!("[PROPOSER] Dag moved to round {} with {} parents", self.round, parents.len());

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
                    info!("[PROPOSER] Reset header timer after receiving parents for round {} (new round: {})", round, self.round);
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
                    info!(
                        "[BATCH TRACK PROPOSER] Proposer {} received header {} (round {}, author: {}) from Core for batch extraction. Header contains {} batches: {:?}",
                        self.name,
                        header.id,
                        header.round,
                        header.author,
                        header.payload.len(),
                        header.payload.keys().take(5).collect::<Vec<_>>()
                    );
                    self.extract_batches_from_headers(&header).await;
                }
                Some((digest, worker_id, batch)) = self.rx_workers.recv() => {
                    // BATCH TRACKING: Log when receiving batch from worker
                    info!(
                        "[BATCH TRACK WORKER] Proposer {} received batch {} from worker {} at round {} (batch size: {} bytes, raw payload: {} bytes)",
                        self.name,
                        digest,
                        worker_id,
                        self.round,
                        digest.size(),
                        batch.len()
                    );
                    
                    // Skip if already committed
                    if self.committed_digests.contains_key(&digest) {
                        warn!(
                            "[BATCH TRACK WORKER] Proposer {} SKIP enqueue batch {} from worker {} at round {} - ALREADY COMMITTED at round {}",
                            self.name,
                            digest,
                            worker_id,
                            self.round,
                            self.committed_digests.get(&digest).copied().unwrap_or(0)
                        );
                        continue;
                    }

                    // Store the batch in the primary's store for the `analyze` function to find.
                    let size = digest.size();

                    if self.digests.iter().any(|entry| entry.digest == digest) {
                        warn!(
                            "[BATCH TRACK WORKER] Proposer {} IGNORING duplicate batch {} from worker {} at round {} (already queued; pending_payload_size = {}, queue_len = {})",
                            self.name,
                            digest,
                            worker_id,
                            self.round,
                            self.pending_payload_size,
                            self.digests.len()
                        );
                        continue;
                    }

                    self.store.write(digest.clone().to_vec(), batch).await;

                    let digest_for_log = digest.clone();

                    let entry_idx = self.digests.len();
                    self.digests.push_back(BatchEntry {
                        digest: digest.clone(),
                        worker_id,
                        size,
                        state: BatchState::Pending,
                        retry_count: 0,
                        rescue_sent: false,
                    });
                    // Maintain HashMap index for O(1) lookup
                    self.digests_index.insert(digest, entry_idx);
                    self.pending_payload_size += size;
                    info!(
                        "[BATCH TRACK WORKER] Proposer {} ENQUEUED batch {} from worker {} at round {}; pending_payload_size = {}, queue_len = {}",
                        self.name,
                        digest_for_log,
                        worker_id,
                        self.round,
                        self.pending_payload_size,
                        self.digests.len()
                    );
                }
                Some(committed) = self.rx_committed.recv() => {
                    // BATCH TRACKING: Log when receiving committed batches notification
                    info!(
                        "[BATCH COMMIT NOTIFICATION] Proposer {} received committed batches notification: {} batches committed at round {}",
                        self.name,
                        committed.digests.len(),
                        committed.round
                    );
                    self.mark_committed(committed);
                }
                () = &mut header_timer => {
                    // Timer expired - loop will evaluate conditions again.
                }
                () = &mut retry_timer => {
                    self.retry_stale_batches();
                    retry_timer.as_mut().reset(Instant::now() + self.retry_delay);
                }
            }
        }
    }
}
