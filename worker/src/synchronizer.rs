// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use crate::worker::{Round, WorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error, info, warn};
use network::SimpleSender;
use primary::PrimaryWorkerMessage;
use tracing;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{Store, StoreError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

/// Resolution of the timer managing retrials of sync requests (in ms).
const TIMER_RESOLUTION: u64 = 1_000;
/// When the number of pending batch digests waiting for sync goes above this threshold,
/// emit a warning to hint potential worker-to-worker congestion.
const PENDING_WARN_THRESHOLD: usize = 200;
/// Warn when the same batch digest has been retried this many times.
const RETRY_WARN_THRESHOLD: u32 = 3;

// The `Synchronizer` is responsible to keep the worker in sync with the others.
pub struct Synchronizer {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    // The persistent storage.
    store: Store,
    /// The depth of the garbage collection.
    gc_depth: Round,
    /// The delay to wait before re-trying to send sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-requests. These nodes
    /// are picked at random from the committee.
    sync_retry_nodes: usize,
    /// Input channel to receive the commands from the primary.
    rx_message: Receiver<PrimaryWorkerMessage>,
    /// A network sender to send requests to the other workers.
    network: SimpleSender,
    /// Channel to send synced batches to primary (via Processor).
    tx_processor: Sender<SerializedBatchMessage>,
    /// Loosely keep track of the primary's round number (only used for cleanup).
    round: Round,
    /// Keeps the digests (of batches) that are waiting to be processed by the primary. Their
    /// processing will resume when we get the missing batches in the store or we no longer need them.
    /// It also keeps the round number, timestamp (`u128`), initial target, and retry counter of each request we sent.
    pending: HashMap<Digest, (Round, Sender<()>, u128, PublicKey, u32)>,
}

impl Synchronizer {
    fn log_sync_trigger(name: &PublicKey, id: WorkerId, target: PublicKey, count: usize) {
        info!(
            "Worker {:?}-{}: received sync request for {} digest(s) targeting {:?}",
            name, id, count, target
        );
    }

    fn log_local_store_check(
        name: &PublicKey,
        id: WorkerId,
        target: &PublicKey,
        requested: usize,
        missing: usize,
    ) {
        info!(
            "Worker {:?}-{}: local store check for {:?}: requested={}, missing={}",
            name, id, target, requested, missing
        );
        if missing == 0 {
            info!(
                "Worker {:?}-{}: skipping sync as all digests already in store for target {:?}",
                name, id, target
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        store: Store,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_message: Receiver<PrimaryWorkerMessage>,
        tx_processor: Sender<SerializedBatchMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                id,
                committee,
                store,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_message,
                network: SimpleSender::new(),
                tx_processor,
                round: Round::default(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a batch to become available in the storage
    /// and then delivers its digest.
    async fn waiter(
        missing: Digest,
        mut store: Store,
        deliver: Digest,
        mut handler: Receiver<()>,
    ) -> Result<Option<Digest>, StoreError> {
        tokio::select! {
            result = store.notify_read(missing.to_vec()) => {
                result.map(|_| Some(deliver))
            }
            _ = handler.recv() => Ok(None),
        }
    }

    /// Main loop listening to the primary's messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Handle primary's messages.
                Some(message) = self.rx_message.recv() => match message {
                    PrimaryWorkerMessage::Synchronize(digests, target) => {
                        // CRITICAL: Log khi worker nhận sync request từ primary
                        tracing::info!(
                            target: "narwhal_audit",
                            "[SYNC REQUEST RECEIVED] Worker {}-{} received sync request from primary for {} batches targeting {:?}. Worker will check local store and request missing batches.",
                            self.name, self.id, digests.len(), target
                        );
                        
                        Self::log_sync_trigger(&self.name, self.id, target.clone(), digests.len());
                        let now = Self::now_millis();

                        let mut missing = Vec::new();
                        let mut found = 0;
                        for digest in &digests {
                            // Ensure we do not send twice the same sync request.
                            if self.pending.contains_key(&digest) {
                                continue;
                            }

                            // Check if we received the batch in the meantime.
                            match self.store.read(digest.to_vec()).await {
                                Ok(None) => {
                                    missing.push(digest.clone());
                                    debug!("Requesting sync for batch {}", digest);
                                },
                                Ok(Some(_)) => {
                                    // The batch arrived in the meantime: no need to request it.
                                    found += 1;
                                },
                                Err(e) => {
                                    error!("{}", e);
                                    continue;
                                }
                            }

                            // Add the digest to the waiter.
                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            self.pending.insert(digest.clone(), (self.round, tx_cancel, now, target.clone(), 0));
                        }

                        if !missing.is_empty() {
                            // CRITICAL: Log khi worker gửi batch request đến target worker
                            tracing::info!(
                                target: "narwhal_audit",
                                "[SYNC BATCH REQUEST] Worker {}-{} requesting {} missing batch(es) from target worker {:?} (found locally: {}, total requested: {}, pending_before: {}). Batch request will be sent to target worker.",
                                self.name, self.id, missing.len(), target, found, digests.len(), self.pending.len()
                            );
                            
                            info!(
                                "Worker {:?}-{}: requesting {} missing batch(es) from {:?} (pending_before={})",
                                self.name,
                                self.id,
                                missing.len(),
                                target,
                                self.pending.len()
                            );
                        } else {
                            // CRITICAL: Log khi tất cả batches đã có sẵn
                            tracing::info!(
                                target: "narwhal_audit",
                                "[SYNC ALL BATCHES AVAILABLE] Worker {}-{} received sync request for {} batches from primary targeting {:?}. All batches are already available locally. No sync request needed.",
                                self.name, self.id, digests.len(), target
                            );
                        }
                        self.warn_if_backlogged();

                        // Send sync request to a single node. If this fails, we will send it
                        // to other nodes when a timer times out.
                        let address = match self.committee.worker(&target, &self.id) {
                            Ok(address) => address.worker_to_worker,
                            Err(e) => {
                                tracing::error!(
                                    target: "narwhal_audit",
                                    "[SYNC ERROR] Worker {}-{} cannot sync with unknown target {:?}: {}. Sync request will be skipped.",
                                    self.name, self.id, target, e
                                );
                                error!("The primary asked us to sync with an unknown node: {}", e);
                                continue;
                            }
                        };
                        Self::log_local_store_check(&self.name, self.id, &target, digests.len(), missing.len());
                        if missing.is_empty() {
                            continue;
                        }
                        let message = WorkerMessage::BatchRequest(missing.clone(), self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        
                        // CRITICAL: Log trước khi gửi batch request
                        tracing::info!(
                            target: "narwhal_audit",
                            "[SYNC BATCH REQUEST SENT] Worker {}-{} sending batch request for {} batches to target worker {:?} at {}. Waiting for response.",
                            self.name, self.id, missing.len(), target, address
                        );
                        
                        self.network.send(address, Bytes::from(serialized)).await;
                    },
                    PrimaryWorkerMessage::Cleanup(round) => {
                        // Keep track of the primary's round number.
                        self.round = round;

                        // Cleanup internal state.
                        if self.round < self.gc_depth {
                            continue;
                        }

                        let mut gc_round = self.round - self.gc_depth;
                        for (r, handler, _, _, _) in self.pending.values() {
                            if r <= &gc_round {
                                let _ = handler.send(()).await;
                            }
                        }
                        self.pending.retain(|_, (r, _, _, _, _)| r > &mut gc_round);
                    }
                },

                // Stream out the futures of the `FuturesUnordered` that completed.
                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        // We got the batch, remove it from the pending list.
                        let now = Self::now_millis();
                        if let Some((_, _, inserted_at, initial_target, _)) = self.pending.remove(&digest) {
                            let latency = now.saturating_sub(inserted_at);
                            
                            // CRITICAL: Log khi worker nhận batch từ sync
                            tracing::info!(
                                target: "narwhal_audit",
                                "[SYNC BATCH RECEIVED] Worker {}-{} received batch {} from sync after {}ms (requested from {:?}, pending={}). Batch will be stored and sent to primary.",
                                self.name, self.id, digest, latency, initial_target, self.pending.len()
                            );
                            
                            info!(
                                "Worker {:?}-{}: batch {} arrived after {} ms (first requested from {:?}, pending={})",
                                self.name,
                                self.id,
                                digest,
                                latency,
                                initial_target,
                                self.pending.len()
                            );
                            
                            // CRITICAL FIX: Đọc batch từ store và gửi lên primary
                            // Batch đã được lưu vào store qua notify_read, nhưng worker chưa gửi lên primary
                            // Điều này gây ra race condition: primary check payload trước khi worker gửi batch
                            // Batch trong store là Vec<u8> (serialized WorkerMessage::Batch), có thể gửi trực tiếp đến Processor
                            match self.store.read(digest.to_vec()).await {
                                Ok(Some(batch)) => {
                                    // Batch trong store là serialized WorkerMessage::Batch (Vec<u8>)
                                    // Gửi trực tiếp đến Processor, Processor sẽ deserialize và gửi lên primary
                                    if let Err(e) = self.tx_processor.send(batch).await {
                                        tracing::error!(
                                            target: "narwhal_audit",
                                            "[SYNC BATCH SEND ERROR] Worker {}-{} failed to send synced batch {} to processor: {}. Primary may not receive this batch.",
                                            self.name, self.id, digest, e
                                        );
                                    } else {
                                        tracing::info!(
                                            target: "narwhal_audit",
                                            "[SYNC BATCH SENT TO PRIMARY] Worker {}-{} successfully sent synced batch {} to processor. Batch will be forwarded to primary.",
                                            self.name, self.id, digest
                                        );
                                    }
                                },
                                Ok(None) => {
                                    tracing::warn!(
                                        target: "narwhal_audit",
                                        "[SYNC BATCH NOT FOUND] Worker {}-{} received notification for batch {} but batch not found in store. This should not happen.",
                                        self.name, self.id, digest
                                    );
                                },
                                Err(e) => {
                                    tracing::error!(
                                        target: "narwhal_audit",
                                        "[SYNC BATCH READ ERROR] Worker {}-{} failed to read synced batch {} from store: {}. Primary will not receive this batch.",
                                        self.name, self.id, digest, e
                                    );
                                }
                            }
                        }
                    },
                    Ok(None) => {
                        debug!(
                            "Worker {:?}-{}: sync waiter cancelled before batch arrival",
                            self.name,
                            self.id
                        );
                    },
                    Err(e) => error!("{}", e)
                },

                // Triggers on timer's expiration.
                () = &mut timer => {
                    // We optimistically sent sync requests to a single node. If this timer triggers,
                    // it means we were wrong to trust it. We are done waiting for a reply and we now
                    // broadcast the request to a bunch of other nodes (selected at random).
                    let now = Self::now_millis();

                    let mut retry = Vec::new();
                    for (digest, (_, _, timestamp, _, retry_count)) in self.pending.iter_mut() {
                        if *timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            *timestamp = now;
                            *retry_count += 1;
                            if *retry_count >= RETRY_WARN_THRESHOLD {
                                warn!(
                                    "Worker {:?}-{}: batch {} retried {} times; worker-to-worker path might be unhealthy",
                                    self.name,
                                    self.id,
                                    digest,
                                    retry_count
                                );
                            }
                            retry.push(digest.clone());
                        }
                    }
                    if !retry.is_empty() {
                        info!(
                            "Worker {:?}-{}: retrying {} batch digests after {} ms (pending={})",
                            self.name,
                            self.id,
                            retry.len(),
                            self.sync_retry_delay,
                            self.pending.len()
                        );
                        let addresses = self
                            .committee
                            .others_workers(&self.name, &self.id)
                            .iter()
                            .map(|(_, address)| address.worker_to_worker)
                            .collect();
                        let message = WorkerMessage::BatchRequest(retry, self.name);
                        let serialized =
                            bincode::serialize(&message).expect("Failed to serialize our own message");
                        self.network
                            .lucky_broadcast(addresses, Bytes::from(serialized), self.sync_retry_nodes)
                            .await;
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }

    fn now_millis() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis()
    }

    fn warn_if_backlogged(&self) {
        let pending = self.pending.len();
        if pending >= PENDING_WARN_THRESHOLD {
            warn!(
                "Worker {:?}-{}: pending batch sync backlog = {} (threshold={}), check worker-to-worker connectivity",
                self.name,
                self.id,
                pending,
                PENDING_WARN_THRESHOLD
            );
        }
    }
}
