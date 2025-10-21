// In worker/src/synchronizer.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{Round, WorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use network::SimpleSender;
use primary::PrimaryWorkerMessage;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{Store, StoreError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

const TIMER_RESOLUTION: u64 = 1_000;

pub struct Synchronizer {
    name: PublicKey,
    id: WorkerId,
    committee: Arc<RwLock<Committee>>,
    store: Store,
    gc_depth: Round,
    sync_retry_delay: u64,
    sync_retry_nodes: usize,
    rx_message: Receiver<PrimaryWorkerMessage>,
    network: SimpleSender,
    round: Round,
    pending: HashMap<Digest, (Round, Sender<()>, u128)>,
}

impl Synchronizer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Arc<RwLock<Committee>>,
        store: Store,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_message: Receiver<PrimaryWorkerMessage>,
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
                round: Round::default(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

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

    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();
        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.rx_message.recv() => {
                    let committee = self.committee.read().await;
                    match message {
                        PrimaryWorkerMessage::Synchronize(digests, target) => {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();

                            let mut missing = Vec::new();
                            for digest in digests {
                                if self.pending.contains_key(&digest) {
                                    continue;
                                }
                                match self.store.read(digest.to_vec()).await {
                                    Ok(None) => {
                                        missing.push(digest.clone());
                                        debug!("Requesting sync for batch {}", digest);
                                    },
                                    Ok(Some(_)) => {},
                                    Err(e) => {
                                        error!("{}", e);
                                        continue;
                                    }
                                }
                                let deliver = digest.clone();
                                let (tx_cancel, rx_cancel) = channel(1);
                                let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                                waiting.push(fut);
                                self.pending.insert(digest, (self.round, tx_cancel, now));
                            }
                            if !missing.is_empty() {
                                let address = match committee.worker(&target, &self.id) {
                                    Ok(address) => address.worker_to_worker,
                                    Err(e) => {
                                        error!("Primary asked us to sync with an unknown node: {}", e);
                                        continue;
                                    }
                                };
                                let message = WorkerMessage::BatchRequest(missing, self.name);
                                let serialized = bincode::serialize(&message).expect("Failed to serialize message");
                                self.network.send(address, Bytes::from(serialized)).await;
                            }
                        },
                        PrimaryWorkerMessage::Cleanup(round) => {
                            self.round = round;
                            if self.round < self.gc_depth {
                                continue;
                            }
                            let mut gc_round = self.round - self.gc_depth;
                            for (r, handler, _) in self.pending.values() {
                                if r <= &gc_round {
                                    let _ = handler.send(()).await;
                                }
                            }
                            self.pending.retain(|_, (r, _, _)| r > &mut gc_round);
                        },
                        PrimaryWorkerMessage::Reconfigure(_) => {
                            // The actual committee update is handled in worker.rs
                        }
                    }
                },
                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        self.pending.remove(&digest);
                    },
                    Ok(None) => {},
                    Err(e) => error!("{}", e)
                },
                () = &mut timer => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry = Vec::new();
                    for (digest, (_, _, timestamp)) in &self.pending {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }
                    if !retry.is_empty() {
                        let committee = self.committee.read().await;
                        let addresses = committee
                            .others_workers(&self.name, &self.id)
                            .iter().map(|(_, address)| address.worker_to_worker)
                            .collect();
                        let message = WorkerMessage::BatchRequest(retry, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize message");
                        self.network
                            .lucky_broadcast(addresses, Bytes::from(serialized), self.sync_retry_nodes)
                            .await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }
}
