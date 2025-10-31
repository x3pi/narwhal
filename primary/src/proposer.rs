// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::{CommittedBatches, Round};
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use log::warn;
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchState {
    Pending,
    InFlight { round: Round, sent_at: Instant },
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
                rx_workers,
                rx_committed,
                tx_core,
                round: 1,
                last_parents: genesis,
                digests: VecDeque::with_capacity(2 * header_size.max(1)),
                pending_payload_size: 0,
                latest_committed_round: 0,
                committed_digests: HashMap::new(),
                max_retry_rounds: 20, // Don't retry batches that have been InFlight for more than 20 rounds
                max_committed_digests: 10000, // Cleanup old digests when this limit is reached
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) -> bool {
        let payload: Vec<(Digest, WorkerId)> = self.collect_payload_for_header();

        // Final check: ensure no duplicates and no committed batches in payload before creating header
        let mut seen = HashSet::new();
        let mut deduplicated_payload = Vec::new();
        let mut duplicates_found = 0;
        let mut committed_found = 0;

        for (digest, worker_id) in payload {
            // CRITICAL: Final check - skip if already committed
            if self.committed_digests.contains_key(&digest) {
                committed_found += 1;
                warn!(
                    "Removing already-committed digest {} from header payload for round {}",
                    digest, self.round
                );
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

        if deduplicated_payload.is_empty() {
            debug!("No payload to include in header for round {}", self.round);
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

            if matches!(entry.state, BatchState::Pending) {
                accumulated_size += entry.size;
                seen_digests.insert(entry.digest.clone());
                collected.push((entry.digest.clone(), entry.worker_id));
                entry.state = BatchState::InFlight {
                    round: self.round,
                    sent_at: Instant::now(),
                };
                self.pending_payload_size = self.pending_payload_size.saturating_sub(entry.size);
            } else if let BatchState::InFlight {
                round: old_round, ..
            } = entry.state
            {
                // CRITICAL: Don't re-propose batches that are already InFlight
                // This prevents the same batch from being included in multiple headers
                warn!(
                    "Batch {} is already InFlight from round {}, skipping in round {}",
                    entry.digest, old_round, self.round
                );
                continue;
            }
        }

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
            debug!(
                "Marked {} batches as committed (round {}), {} were InFlight",
                marked_count, committed.round, inflight_count
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

        for entry in self.digests.iter_mut() {
            if let BatchState::InFlight { round, sent_at } = entry.state {
                // Skip if already committed - this is critical to prevent duplicates
                if self.committed_digests.contains_key(&entry.digest) {
                    entry.state = BatchState::Committed;
                    skipped_committed += 1;
                    continue;
                }

                // Check if batch is too old (InFlight for more than max_retry_rounds)
                let is_too_old = self.round > round.saturating_add(self.max_retry_rounds);

                if is_too_old {
                    // For very old batches, only retry if latest_committed_round hasn't advanced much
                    // This means the batch likely hasn't been committed yet
                    // If latest_committed_round has advanced significantly, the batch might be committed
                    // but we haven't received notification yet, so we skip retry
                    let rounds_since_sent = self.round.saturating_sub(round);
                    let rounds_committed_since_sent =
                        self.latest_committed_round.saturating_sub(round);

                    // If we've committed many rounds since batch was sent, likely it's already committed
                    // If we've committed few rounds, batch might still be pending
                    if rounds_committed_since_sent < rounds_since_sent / 2 {
                        // Not many rounds committed since batch was sent - likely still pending
                        debug!(
                            "Retrying old batch {} (sent at round {}, current round {}, committed rounds since: {})",
                            entry.digest, round, self.round, rounds_committed_since_sent
                        );
                        entry.state = BatchState::Pending;
                        self.pending_payload_size += entry.size;
                        requeued_old += 1;
                    } else {
                        // Many rounds committed since batch was sent - likely already committed
                        debug!(
                            "Skipping retry for batch {} - too old and likely committed (sent at round {}, current round {}, committed rounds since: {})",
                            entry.digest, round, self.round, rounds_committed_since_sent
                        );
                        skipped_too_old += 1;
                        // Keep it in InFlight state - will be marked as Committed when commit notification is received
                        continue;
                    }
                } else {
                    // Batch is not too old - normal retry logic
                    // Only retry if batch hasn't been committed and meets retry conditions
                    if now.duration_since(sent_at) >= self.retry_delay
                        || self.latest_committed_round > round
                    {
                        // Double-check: verify batch is still not committed before re-queueing
                        if !self.committed_digests.contains_key(&entry.digest) {
                            entry.state = BatchState::Pending;
                            self.pending_payload_size += entry.size;
                            requeued += 1;
                        } else {
                            entry.state = BatchState::Committed;
                            skipped_committed += 1;
                        }
                    }
                }
            }
        }

        if requeued > 0 {
            debug!(
                "Requeued {} batches for re-inclusion (latest_committed_round = {})",
                requeued, self.latest_committed_round
            );
        }

        if requeued_old > 0 {
            debug!(
                "Requeued {} old batches that are likely still pending (latest_committed_round = {})",
                requeued_old, self.latest_committed_round
            );
        }

        if skipped_committed > 0 {
            debug!(
                "Skipped {} already-committed batches during retry check",
                skipped_committed
            );
        }

        if skipped_too_old > 0 {
            debug!(
                "Skipped {} batches that are too old and likely already committed (max_retry_rounds = {}). These batches remain in InFlight state and will be marked as Committed when commit notification is received.",
                skipped_too_old, self.max_retry_rounds
            );
        }

        self.digests
            .retain(|entry| !matches!(entry.state, BatchState::Committed));
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

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;
                }
                Some((digest, worker_id, batch)) = self.rx_workers.recv() => {
                    // Skip if already committed
                    if self.committed_digests.contains_key(&digest) {
                        debug!("Skipping batch {} - already committed", digest);
                        continue;
                    }

                    // Store the batch in the primary's store for the `analyze` function to find.
                    let size = digest.size();

                    if self.digests.iter().any(|entry| entry.digest == digest) {
                        continue;
                    }

                    self.store.write(digest.clone().to_vec(), batch).await;

                    self.digests.push_back(BatchEntry {
                        digest,
                        worker_id,
                        size,
                        state: BatchState::Pending,
                    });
                    self.pending_payload_size += size;
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
