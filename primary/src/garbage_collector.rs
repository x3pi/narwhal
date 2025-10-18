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

/// Dọn dẹp DAG và giải cứu các batch bị bỏ rơi.
pub struct GarbageCollector {
    // Các trường cần thiết cho luồng chính (xử lý certificate đã commit).
    consensus_round: Arc<AtomicU64>,
    pending_batches: PendingBatches,
    rx_consensus: Receiver<Certificate>,
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

        // --- TỐI ƯU HÓA: Chạy tác vụ quét nền độc lập ---
        let store_clone = store.clone();
        let pending_batches_clone = pending_batches.clone();
        let consensus_round_clone = consensus_round.clone();
        tokio::spawn(async move {
            let mut repropose_timer = interval(Duration::from_millis(REPROPOSE_SCAN_INTERVAL_MS));
            loop {
                repropose_timer.tick().await;
                Self::re_propose_orphaned_batches(
                    store_clone.clone(),
                    pending_batches_clone.clone(),
                    tx_repropose.clone(),
                    consensus_round_clone.clone(),
                )
                .await;
            }
        });
        // --- KẾT THÚC TỐI ƯU HÓA ---

        // Khởi chạy luồng chính của Garbage Collector.
        tokio::spawn(async move {
            Self {
                consensus_round,
                pending_batches,
                rx_consensus,
                addresses,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Quét cache để tìm và đề xuất lại các batch mồ côi.
    /// Chạy trong một tác vụ độc lập để không chặn luồng chính.
    async fn re_propose_orphaned_batches(
        mut store: Store,
        pending_batches: PendingBatches,
        tx_repropose: Sender<(Digest, WorkerId, Vec<u8>)>,
        consensus_round: Arc<AtomicU64>,
    ) {
        let current_round = consensus_round.load(Ordering::Relaxed);
        if current_round < ORPHAN_BATCH_THRESHOLD_ROUNDS {
            return;
        }
        let orphan_round_threshold = current_round - ORPHAN_BATCH_THRESHOLD_ROUNDS;

        let mut orphaned_digests = Vec::new();
        // Quét cache trong RAM, không khóa toàn bộ.
        for item in pending_batches.iter() {
            let (digest, (_worker_id, round)) = item.pair();
            if *round < orphan_round_threshold {
                orphaned_digests.push(digest.clone());
            }
        }

        for digest in orphaned_digests {
            // Xóa khỏi cache và lấy giá trị.
            if let Some((_, (worker_id, round))) = pending_batches.remove(&digest) {
                // Đọc lại từ store (chỉ xảy ra khi giải cứu, là cold path).
                if let Ok(Some(batch_data)) = store.read(digest.to_vec()).await {
                    warn!(
                        "Found orphaned batch {} from round {}, re-proposing.",
                        digest, round
                    );
                    let _ = tx_repropose.send((digest, worker_id, batch_data)).await;
                }
            }
        }
    }

    /// Vòng lặp chính, chỉ xử lý các tác vụ nhanh trên luồng nóng (hot path).
    async fn run(&mut self) {
        let mut last_committed_round = 0;

        while let Some(certificate) = self.rx_consensus.recv().await {
            // Xóa các batch đã được commit khỏi cache theo dõi (thao tác trong RAM, rất nhanh).
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
        }
    }
}
