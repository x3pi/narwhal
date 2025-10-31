// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::{CommittedBatches, Round};
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use std::collections::{HashSet, VecDeque};
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
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) -> bool {
        let payload: Vec<(Digest, WorkerId)> = self.collect_payload_for_header();
        // Make a new header.
        let header = Header::new(
            self.name,
            self.round,
            payload.into_iter().collect(),
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
        let mut collected = Vec::new();
        let mut accumulated_size = 0usize;

        for entry in self.digests.iter_mut() {
            if accumulated_size >= self.header_size {
                break;
            }

            if matches!(entry.state, BatchState::Pending) {
                accumulated_size += entry.size;
                collected.push((entry.digest.clone(), entry.worker_id));
                entry.state = BatchState::InFlight {
                    round: self.round,
                    sent_at: Instant::now(),
                };
                self.pending_payload_size = self.pending_payload_size.saturating_sub(entry.size);
            }
        }

        collected
    }

    fn mark_committed(&mut self, committed: CommittedBatches) {
        self.latest_committed_round = self.latest_committed_round.max(committed.round);

        if committed.digests.is_empty() {
            return;
        }

        let committed_set: HashSet<_> = committed.digests.into_iter().collect();
        for entry in self.digests.iter_mut() {
            if committed_set.contains(&entry.digest) {
                if matches!(entry.state, BatchState::Pending) {
                    self.pending_payload_size =
                        self.pending_payload_size.saturating_sub(entry.size);
                }
                entry.state = BatchState::Committed;
            }
        }

        self.digests
            .retain(|entry| !matches!(entry.state, BatchState::Committed));
    }

    fn retry_stale_batches(&mut self) {
        let now = Instant::now();
        let mut requeued = 0usize;

        for entry in self.digests.iter_mut() {
            if let BatchState::InFlight { round, sent_at } = entry.state {
                if now.duration_since(sent_at) >= self.retry_delay
                    || self.latest_committed_round > round
                {
                    entry.state = BatchState::Pending;
                    self.pending_payload_size += entry.size;
                    requeued += 1;
                }
            }
        }

        if requeued > 0 {
            debug!(
                "Requeued {} batches for re-inclusion (latest_committed_round = {})",
                requeued, self.latest_committed_round
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
