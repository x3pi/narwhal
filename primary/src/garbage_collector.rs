// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::{PendingBatches, PrimaryWorkerMessage, Round};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use log::warn;
use network::SimpleSender;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{interval, Duration};

const REPROPOSE_SCAN_INTERVAL_MS: u64 = 5_000;
const ORPHAN_BATCH_THRESHOLD_ROUNDS: Round = 10;

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    store: Store,
    consensus_round: Arc<AtomicU64>,
    /// Cache dùng chung cho các batch đang chờ.
    pending_batches: PendingBatches,
    rx_consensus: Receiver<Certificate>,
    tx_repropose: Sender<(Digest, WorkerId, Vec<u8>)>,
    addresses: Vec<SocketAddr>,
    network: SimpleSender,
}

impl GarbageCollector {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: &PublicKey,
        committee: &Committee,
        store: Store,
        consensus_round: Arc<AtomicU64>,
        pending_batches: PendingBatches,
        rx_consensus: Receiver<Certificate>,
        tx_repropose: Sender<(Digest, WorkerId, Vec<u8>)>,
    ) {
        let addresses = committee
            .our_workers(name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();

        tokio::spawn(async move {
            Self {
                store,
                consensus_round,
                pending_batches,
                rx_consensus,
                tx_repropose,
                addresses,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn re_propose_orphaned_batches(&mut self) {
        let current_round = self.consensus_round.load(Ordering::Relaxed);
        if current_round < ORPHAN_BATCH_THRESHOLD_ROUNDS {
            return;
        }
        let orphan_round_threshold = current_round - ORPHAN_BATCH_THRESHOLD_ROUNDS;

        // Quét cache đồng thời một cách an toàn.
        let mut orphaned_digests = Vec::new();
        for item in self.pending_batches.iter() {
            let (digest, (_worker_id, round)) = item.pair();
            if *round < orphan_round_threshold {
                orphaned_digests.push(digest.clone());
            }
        }

        for digest in orphaned_digests {
            // Xóa khỏi cache và lấy giá trị.
            if let Some((_, (worker_id, round))) = self.pending_batches.remove(&digest) {
                if let Ok(Some(batch_data)) = self.store.read(digest.to_vec()).await {
                    warn!(
                        "Found orphaned batch {} from round {}, re-proposing.",
                        digest, round
                    );
                    // Gửi lại cho Proposer để được đề xuất lại.
                    let _ = self
                        .tx_repropose
                        .send((digest, worker_id, batch_data))
                        .await;
                }
            }
        }
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;
        let mut repropose_timer = interval(Duration::from_millis(REPROPOSE_SCAN_INTERVAL_MS));

        loop {
            tokio::select! {
                Some(certificate) = self.rx_consensus.recv() => {
                    // Xóa các batch đã được commit khỏi cache.
                    for (digest, _) in certificate.header.payload.iter() {
                        self.pending_batches.remove(digest);
                    }

                    let round = certificate.round();
                    if round > last_committed_round {
                        last_committed_round = round;
                        self.consensus_round.store(round, Ordering::Relaxed);

                        // Gửi tín hiệu dọn dẹp cho các worker.
                        let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                            .expect("Failed to serialize our own message");
                        self.network
                            .broadcast(self.addresses.clone(), Bytes::from(bytes))
                            .await;
                    }
                },
                _ = repropose_timer.tick() => {
                    // Theo định kỳ, quét cache để tìm và đề xuất lại các batch mồ côi.
                    self.re_propose_orphaned_batches().await;
                }
            }
        }
    }
}
