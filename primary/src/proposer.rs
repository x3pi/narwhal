// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::{CommittedBatches, Round};
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, info, warn};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchState {
    Pending,
    InFlight { round: Round, sent_at: Instant, retry_count: usize },
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

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Digest>,
    /// Holds the batches' digests waiting to be included in future headers (in arrival order).
    digests: VecDeque<BatchEntry>,
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
                round: 1,
                last_parents: genesis,
                digests: VecDeque::with_capacity(2 * header_size.max(1)),
                pending_payload_size: 0,
                latest_committed_round: 0,
                committed_digests: HashMap::new(),
                max_retry_rounds: 1000, // Don't retry batches that have been InFlight for more than 1000 rounds
                max_committed_digests: 10000, // Cleanup old digests when this limit is reached
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
            self.digests.retain(|entry| !committed_digests_ref.contains_key(&entry.digest));
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
            if !self.last_parents.is_empty() {
                let header = Header::new(
                    self.name,
                    self.round,
                    BTreeMap::new(),
                    self.last_parents.drain(..).collect(),
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

        info!(
            "Creating header for round {} with digests {:?}",
            self.round,
            deduplicated_payload
                .iter()
                .map(|(digest, _)| digest)
                .collect::<Vec<_>>()
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

        self.digests
            .retain(|entry| !matches!(entry.state, BatchState::Committed));
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

        for entry in self.digests.iter_mut() {
            if let BatchState::InFlight { round, sent_at, retry_count } = entry.state {
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
                let batch_is_actually_committed = self.committed_digests.contains_key(&entry.digest);
                let own_certificate_not_committed = !batch_is_actually_committed 
                    && self.latest_committed_round >= round;
                
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
                        "FORCE REMOVING stuck batch {} (retry_count={}, sent_round={}, current_round={}, rounds_since_sent={}). Batch cannot be committed after {} retries - removing to prevent system deadlock. Transactions in this batch will be LOST.",
                        entry.digest, retry_count, round, self.round, rounds_since_sent, MAX_RETRY_COUNT
                    );
                    entry.state = BatchState::Committed; // Mark as committed to remove
                    removed_too_old += 1;
                    continue;
                }
                let safe_to_retry = if rounds_since_sent_long || has_been_retried_multiple_times || own_certificate_not_committed {
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
                        format!("batch is old enough ({} rounds >= {}) to force retry", rounds_since_sent, self.max_retry_rounds)
                    } else {
                        format!("latest_committed_round is {} rounds behind sent round", round.saturating_sub(self.latest_committed_round))
                    };
                    info!(
                        "Requeue old batch {} for retry (sent at round {}, current round {}, latest_committed_round={}, rounds since sent: {}, committed rounds since: {}). {}",
                        entry.digest, round, self.round, self.latest_committed_round, rounds_since_sent, rounds_committed_since_sent,
                        retry_reason
                        );
                        entry.state = BatchState::Pending;
                    entry.retry_count += 1; // Increment retry count when requeuing
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
                    let should_retry_now = own_certificate_not_committed || now.duration_since(sent_at) >= self.retry_delay;
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

        self.digests
            .retain(|entry| !matches!(entry.state, BatchState::Committed));
    }

    /// Extract batches from parent certificates and add them to queue if not committed
    /// This allows leader to include batches from other primaries, ensuring faster commit
    /// and reducing the need for retry
    async fn extract_batches_from_parents(&mut self, parent_digests: &[Digest], parent_round: Round) {
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
                                let already_in_queue = self.digests.iter().any(|entry| entry.digest == *batch_digest);
                                if already_in_queue {
                                    // Check if batch is in Pending state
                                    let is_pending = self.digests.iter()
                                        .any(|entry| entry.digest == *batch_digest 
                                            && matches!(entry.state, BatchState::Pending));
                                    
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
                                            && matches!(entry.state, BatchState::InFlight { .. }) {
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
                                        self.digests.push_back(BatchEntry {
                                            digest: batch_digest.clone(),
                                            worker_id: *worker_id,
                                            size,
                                            state: BatchState::Pending,
                                            retry_count: 0,
                                        });
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
        // CRITICAL: Skip our own headers - we already know about these batches
        if header.author == self.name {
            return;
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
            let all_batches_committed = header.payload.iter()
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

            // CRITICAL: Check if batch is already in queue
            let already_in_queue = self.digests.iter().any(|entry| entry.digest == *batch_digest);
            if already_in_queue {
                // Check if batch is in Pending state
                let is_pending = self.digests.iter()
                    .any(|entry| entry.digest == *batch_digest 
                        && matches!(entry.state, BatchState::Pending));
                
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
                let mut converted = false;
                for entry in self.digests.iter_mut() {
                    if entry.digest == *batch_digest 
                        && matches!(entry.state, BatchState::InFlight { .. }) {
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
                            // Mark as committed and skip this batch (break out of entry loop, continue to next batch)
                            entry.state = BatchState::Committed;
                            converted = true; // Set to true to skip adding this batch
                            break; // Break out of entry iteration loop
                        }
                        
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
                        break;
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
                    self.digests.push_back(BatchEntry {
                        digest: batch_digest.clone(),
                        worker_id: *worker_id,
                        size,
                        state: BatchState::Pending,
                        retry_count: 0,
                    });
                    self.pending_payload_size += size;
                    batches_added += 1;

                    info!(
                        "[BATCH EXTRACTION] Primary {} EXTRACTED batch {} (worker {}) from header {} (round {}, author: {}) into queue. Batch from non-leader primary can now be committed by this primary. Batch size: {} bytes",
                        self.name,
                        batch_digest,
                        worker_id,
                        header.id,
                        header.round,
                        header.author,
                        size
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

        if batches_extracted > 0 {
            info!(
                "[BATCH EXTRACTION] Primary {} extracted batches from header {} (round {}, author: {}): {} total, {} added to queue, {} skipped (committed), {} skipped (duplicate), {} not in store yet",
                self.name,
                header.id,
                header.round,
                header.author,
                batches_extracted,
                batches_added,
                batches_skipped_committed,
                batches_skipped_duplicate,
                batches_not_in_store
            );
        }
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);

        let header_timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(header_timer);

        let retry_timer = sleep(self.retry_delay);
        tokio::pin!(retry_timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.pending_payload_size >= self.header_size;
            let timer_expired = header_timer.is_elapsed();
            if (timer_expired || enough_digests) && enough_parents {
                // Make a new header.
                if self.make_header().await {
                    // Reschedule the timer.
                    let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                    header_timer.as_mut().reset(deadline);
                } else if timer_expired {
                    // Nothing to send but timer elapsed: reschedule to avoid busy loop.
                    let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                    header_timer.as_mut().reset(deadline);
                }
            }

            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    if round < self.round {
                        continue;
                    }

                    // Advance to the next round.
                    self.round = round + 1;
                    debug!("Dag moved to round {}", self.round);

                    // IMPROVED: Extract batches from parent certificates to help leader commit batches from other primaries
                    // This ensures batches are committed faster and reduces the need for retry
                    // Only extract if we have parents (quorum of certificates from previous round)
                    if !parents.is_empty() {
                        self.extract_batches_from_parents(&parents, round).await;
                    }

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;
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
                    self.extract_batches_from_headers(&header).await;
                }
                Some((digest, worker_id, batch)) = self.rx_workers.recv() => {
                    // Skip if already committed
                    if self.committed_digests.contains_key(&digest) {
                        info!(
                            "Skip enqueue batch {} from worker {} at round {} because it is already committed",
                            digest,
                            worker_id,
                            self.round
                        );
                        continue;
                    }

                    // Store the batch in the primary's store for the `analyze` function to find.
                    let size = digest.size();
                    let raw_len = batch.len();

                    if self.digests.iter().any(|entry| entry.digest == digest) {
                        info!(
                            "Ignoring duplicate batch {} from worker {} at round {} (already queued; pending_payload_size = {}).",
                            digest,
                            worker_id,
                            self.round,
                            self.pending_payload_size
                        );
                        continue;
                    }

                    debug!(
                        "Received batch {} from worker {} (digest size {} bytes, raw payload {} bytes) at round {}.",
                        digest,
                        worker_id,
                        size,
                        raw_len,
                        self.round
                    );

                    self.store.write(digest.clone().to_vec(), batch).await;

                    let digest_for_log = digest.clone();

                    self.digests.push_back(BatchEntry {
                        digest,
                        worker_id,
                        size,
                        state: BatchState::Pending,
                        retry_count: 0,
                    });
                    self.pending_payload_size += size;
                    info!(
                        "Batch {} enqueued from worker {} at round {}; pending_payload_size = {}, queue_len = {}",
                        digest_for_log,
                        worker_id,
                        self.round,
                        self.pending_payload_size,
                        self.digests.len()
                    );
                }
                Some(committed) = self.rx_committed.recv() => {
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
