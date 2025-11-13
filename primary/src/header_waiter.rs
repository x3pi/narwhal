// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::Header;
use crate::primary::{PrimaryMessage, PrimaryWorkerMessage, Round};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error, info};
use network::SimpleSender;
use std::cmp::min;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

/// The resolution of the timer that checks whether we received replies to our sync requests, and triggers
/// new sync requests if we didn't.
const TIMER_RESOLUTION: u64 = 1_000;
const BROADCAST_RETRY_THRESHOLD: u32 = 2;

/// The commands that can be sent to the `Waiter`.
#[derive(Debug)]
pub enum WaiterMessage {
    SyncBatches(HashMap<Digest, WorkerId>, Header),
    SyncParents(Vec<Digest>, Header),
}

/// Waits for missing parent certificates and batches' digests.
pub struct HeaderWaiter {
    /// The name of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,
    /// The delay to wait before re-trying sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-request.
    sync_retry_nodes: usize,

    /// Receives sync commands from the `Synchronizer`.
    rx_synchronizer: Receiver<WaiterMessage>,
    /// Loops back to the core headers for which we got all parents and batches.
    tx_core: Sender<Header>,

    /// Network driver allowing to send messages.
    network: SimpleSender,
    /// Keeps the digests of the all certificates for which we sent a sync request,
    /// along with a timestamp (`u128`) indicating when we sent the request.
    parent_requests: HashMap<Digest, (Round, u128, u32)>,
    /// Keeps the digests of the all tx batches for which we sent a sync request,
    /// similarly to `header_requests`.
    batch_requests: HashMap<Digest, Round>,
    /// List of digests (either certificates, headers or tx batch) that are waiting
    /// to be processed. Their processing will resume when we get all their dependencies.
    pending: HashMap<Digest, (Round, Sender<()>)>,
}

impl HeaderWaiter {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_synchronizer: Receiver<WaiterMessage>,
        tx_core: Sender<Header>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                consensus_round,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_synchronizer,
                tx_core,
                network: SimpleSender::new(),
                parent_requests: HashMap::new(),
                batch_requests: HashMap::new(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for particular data to become available in the storage
    /// and then delivers the specified header.
    async fn waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: Header,
        mut handler: Receiver<()>,
    ) -> DagResult<Option<Header>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    /// Main loop listening to the `Synchronizer` messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.rx_synchronizer.recv() => {
                    match message {
                        WaiterMessage::SyncBatches(missing, header) => {
                            debug!("Synching the payload of {}", header);
                            let header_id = header.id.clone();
                            let round = header.round;
                            let author = header.author;

                            // Ensure we sync only once per header.
                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            // Add the header to the waiter pool. The waiter will return it to when all
                            // its parents are in the store.
                            let wait_for = missing
                                .keys() // Chỉ cần lấy digest từ HashMap
                                .map(|digest| (digest.to_vec(), self.store.clone()))
                                .collect();

                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (round, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);

                            // Ensure we didn't already send a sync request for these parents.
                            let mut requires_sync = HashMap::new();
                            for (digest, worker_id) in missing.into_iter() {
                                self.batch_requests.entry(digest.clone()).or_insert_with(|| {
                                    requires_sync.entry(worker_id).or_insert_with(Vec::new).push(digest);
                                    round
                                });
                            }
                            for (worker_id, digests) in requires_sync {
                                let address = self.committee
                                    .worker(&author, &worker_id)
                                    .expect("Author of valid header is not in the committee")
                                    .primary_to_worker;
                                let message = PrimaryWorkerMessage::Synchronize(digests, author);
                                let bytes = bincode::serialize(&message)
                                    .expect("Failed to serialize batch sync request");
                                self.network.send(address, Bytes::from(bytes)).await;
                            }
                        }

                        WaiterMessage::SyncParents(missing, header) => {
                            debug!("Synching the parents of {}", header);
                            let header_id = header.id.clone();
                            let round = header.round;
                            let author = header.author;

                            // Ensure we sync only once per header.
                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            // Add the header to the waiter pool. The waiter will return it to us
                            // when all its parents are in the store.
                            let wait_for = missing
                                .iter()
                                .cloned()
                                .map(|x| (x.to_vec(), self.store.clone()))
                                .collect();
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (round, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);

                            // Ensure we didn't already sent a sync request for these parents.
                            // Optimistically send the sync request to the node that created the certificate.
                            // If this fails (after a timeout), we broadcast the sync request.
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            let mut requires_sync = Vec::new();
                            for missing in missing {
                                self.parent_requests
                                    .entry(missing.clone())
                                    .or_insert_with(|| {
                                        requires_sync.push(missing.clone());
                                        (round, now, 0)
                                    });
                            }
                            if !requires_sync.is_empty() {
                                for digest in &requires_sync {
                                    if let Some((_, ts, attempts)) =
                                        self.parent_requests.get_mut(digest)
                                    {
                                        *ts = now;
                                        *attempts = attempts.saturating_add(1);
                                    }
                                }
                                let address = self.committee
                                    .primary(&author)
                                    .expect("Author of valid header not in the committee")
                                    .primary_to_primary;
                                let message = PrimaryMessage::CertificatesRequest(
                                    requires_sync.clone(),
                                    self.name,
                                );
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                                self.network.send(address, Bytes::from(bytes)).await;
                            }
                        }
                    }
                },

                Some(result) = waiting.next() => match result {
                    Ok(Some(header)) => {
                        let _ = self.pending.remove(&header.id);
                        for x in header.payload.keys() {
                            let _ = self.batch_requests.remove(x);
                        }
                        for x in &header.parents {
                            let _ = self.parent_requests.remove(x);
                        }
                        self.tx_core.send(header).await.expect("Failed to send header");
                    },
                    Ok(None) => {
                        // This request has been canceled.
                    },
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                () = &mut timer => {
                    // We optimistically sent sync requests to a single node. If this timer triggers,
                    // it means we were wrong to trust it. We are done waiting for a reply and we now
                    // broadcast the request to all nodes.
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry_targeted = Vec::new();
                    let mut retry_broadcast = Vec::new();
                    for (digest, (_, timestamp, attempts)) in self.parent_requests.iter_mut() {
                        if *timestamp + (self.sync_retry_delay as u128) <= now {
                            debug!(
                                "Requesting sync for certificate {} (retry #{})",
                                digest,
                                attempts.saturating_add(1)
                            );
                            *timestamp = now;
                            *attempts = attempts.saturating_add(1);
                            if *attempts >= BROADCAST_RETRY_THRESHOLD {
                                retry_broadcast.push(digest.clone());
                            } else {
                                retry_targeted.push(digest.clone());
                            }
                        }
                    }

                    if !retry_targeted.is_empty() {
                        let addresses: Vec<_> = self
                            .committee
                            .others_primaries(&self.name)
                            .iter()
                            .map(|(_, x)| x.primary_to_primary)
                            .collect();
                        let message = PrimaryMessage::CertificatesRequest(
                            retry_targeted.clone(),
                            self.name,
                        );
                        let bytes =
                            Bytes::from(bincode::serialize(&message).expect("Failed to serialize cert request"));
                        let node_count = if self.sync_retry_nodes == 0 {
                            addresses.len()
                        } else {
                            min(self.sync_retry_nodes, addresses.len())
                        };
                        if node_count >= addresses.len() {
                            self.network.broadcast(addresses, bytes).await;
                        } else {
                            self.network
                                .lucky_broadcast(addresses, bytes, node_count)
                                .await;
                        }
                    }

                    if !retry_broadcast.is_empty() {
                        let addresses: Vec<_> = self
                            .committee
                            .others_primaries(&self.name)
                            .iter()
                            .map(|(_, x)| x.primary_to_primary)
                            .collect();
                        let message =
                            PrimaryMessage::CertificatesRequest(retry_broadcast.clone(), self.name);
                        let bytes =
                            Bytes::from(bincode::serialize(&message).expect("Failed to serialize cert request"));
                        self.network.broadcast(addresses, bytes).await;
                    }

                    if !(retry_targeted.is_empty() && retry_broadcast.is_empty()) {
                        info!(
                            "HeaderWaiter {:?}: retrying {} parent certificates after {} ms (pending headers={}, targeted={}, broadcast={})",
                            self.name,
                            retry_targeted.len() + retry_broadcast.len(),
                            self.sync_retry_delay,
                            self.pending.len(),
                            retry_targeted.len(),
                            retry_broadcast.len()
                        );
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let mut gc_round = round - self.gc_depth;

                for (r, handler) in self.pending.values() {
                    if r <= &gc_round {
                        let _ = handler.send(()).await;
                    }
                }
                self.pending.retain(|_, (r, _)| r > &mut gc_round);
                self.batch_requests.retain(|_, r| r > &mut gc_round);
                self.parent_requests
                    .retain(|_, (r, _, _)| r > &mut gc_round);
            }
        }
    }
}
