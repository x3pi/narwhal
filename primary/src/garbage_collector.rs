// In primary/src/garbage_collector.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::{
    CommittedBatches, PendingBatches, PrimaryWorkerMessage, ReconfigureNotification,
}; // <-- THÊM ReconfigureNotification
use crate::{Epoch, Round}; // <-- THÊM Epoch
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use log::{debug, info, warn}; // <-- THÊM info
use network::SimpleSender;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::broadcast; // <-- THÊM
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::{interval, sleep, Duration}; // <-- THÊM sleep

const GC_TIMER_MS: u64 = 5_000;
const ORPHAN_BATCH_THRESHOLD_ROUNDS: Round = 10;

pub struct GarbageCollector {
    name: PublicKey,                   // Thêm trường name
    committee: Arc<RwLock<Committee>>, // SỬA ĐỔI
    consensus_round: Arc<AtomicU64>,
    gc_depth: Round,
    pending_batches: PendingBatches,
    committed_batches: CommittedBatches,
    rx_consensus: Receiver<Certificate>,
    tx_repropose: Sender<(Digest, WorkerId, Vec<u8>)>,
    // *** THAY ĐỔI: Thêm Receiver ***
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    network: SimpleSender,
    store: Store,
    epoch: Epoch, // <-- THÊM TRẠNG THÁI EPOCH
}

impl GarbageCollector {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Arc<RwLock<Committee>>, // SỬA ĐỔI
        store: Store,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        pending_batches: PendingBatches,
        committed_batches: CommittedBatches,
        rx_consensus: Receiver<Certificate>,
        tx_repropose: Sender<(Digest, WorkerId, Vec<u8>)>,
        // *** THAY ĐỔI: Thêm tham số mới ***
        rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    ) {
        // *** BỌC VIỆC KHỞI TẠO TRONG MỘT ASYNC BLOCK ***
        tokio::spawn(async move {
            // Lấy epoch ban đầu từ committee Arc
            let initial_epoch = committee.read().await.epoch;

            Self {
                name,      // Thêm khởi tạo
                committee, // SỬA ĐỔI
                consensus_round,
                gc_depth,
                pending_batches,
                committed_batches,
                rx_consensus,
                tx_repropose,
                // *** THAY ĐỔI: Khởi tạo trường mới ***
                rx_reconfigure,
                network: SimpleSender::new(),
                store,
                epoch: initial_epoch, // <-- KHỞI TẠO EPOCH
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut gc_timer = interval(Duration::from_millis(GC_TIMER_MS));

        loop {
            tokio::select! {
                Some(certificate) = self.rx_consensus.recv() => {
                    let round = certificate.round();
                    let _ = self.consensus_round.fetch_max(round, Ordering::Relaxed);

                    for (digest, _) in certificate.header.payload.iter() {
                        self.pending_batches.remove(digest);
                        self.committed_batches.insert(digest.clone(), round);
                    }

                    // Lấy worker addresses từ committee được chia sẻ
                    let committee = self.committee.read().await;

                    // *** THAY ĐỔI: Chỉ gửi Cleanup nếu epoch khớp ***
                    // *** VÀ SỬ DỤNG self.epoch NỘI BỘ ***
                    if certificate.epoch() == self.epoch && certificate.epoch() == committee.epoch {
                        let worker_addresses: Vec<SocketAddr> = committee
                            .our_workers(&self.name)
                            .expect("Our public key or worker id is not in the committee")
                            .iter()
                            .map(|x| x.primary_to_worker)
                            .collect();

                        let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                            .expect("Failed to serialize Cleanup message");
                        self.network.broadcast(worker_addresses, Bytes::from(bytes)).await;
                    }
                    drop(committee); // Giải phóng lock
                },

                _ = gc_timer.tick() => {
                    let current_round = self.consensus_round.load(Ordering::Relaxed);

                    if current_round > self.gc_depth {
                        let cleanup_round = current_round - self.gc_depth;
                        self.committed_batches.retain(|_, r| *r > cleanup_round);
                         debug!("[GC] Pruned committed batches cache, {} items remaining", self.committed_batches.len());
                    }

                    let orphan_round_threshold = current_round.saturating_sub(ORPHAN_BATCH_THRESHOLD_ROUNDS);

                    let mut orphaned_digests = Vec::new();
                    for item in self.pending_batches.iter() {
                        let (digest, (_worker_id, round)) = item.pair();
                        if *round < orphan_round_threshold {
                           orphaned_digests.push(digest.clone());
                        }
                    }

                    for digest in orphaned_digests {
                        if self.committed_batches.contains_key(&digest) {
                            self.pending_batches.remove(&digest);
                            continue;
                        }

                        if let Some(entry) = self.pending_batches.get(&digest) {
                            let (worker_id, round) = *entry.value();
                             if let Ok(Some(batch_data)) = self.store.read(digest.to_vec()).await {
                                warn!("[GC] Found orphaned batch {} from round {}, re-proposing.", digest, round);
                                let _ = self.tx_repropose.send((digest.clone(), worker_id, batch_data)).await;
                            }
                        }
                    }
                },

                // *** SỬA ĐỔI BẮT ĐẦU: Sửa nhánh reconfigure ***
                result = self.rx_reconfigure.recv() => {
                    match result {
                        Ok(notification) => { // <-- LẤY `notification`
                            // KIỂM TRA 1: Tín hiệu này có phải cho epoch HIỆN TẠI của chúng ta không?
                            if notification.committee.epoch != self.epoch {
                                warn!(
                                    "GarbageCollector: Ignoring stale reconfigure signal for epoch {} (current epoch is {})",
                                    notification.committee.epoch, self.epoch
                                );
                                continue;
                            }

                            // KIỂM TRA 2: Chờ cho committee Arc được cập nhật
                            info!("GarbageCollector: Received reconfigure signal for epoch {}. Waiting for committee update...", self.epoch);
                            let mut new_epoch = self.epoch;
                            while new_epoch == self.epoch {
                                sleep(Duration::from_millis(100)).await; // Chờ và thử lại
                                let committee = self.committee.read().await;
                                new_epoch = committee.epoch;
                                drop(committee); // Giải phóng read-lock
                            }

                            info!("GarbageCollector: Committee updated to epoch {}. Clearing caches AND resetting consensus round.", new_epoch);

                            // CẬP NHẬT EPOCH NỘI BỘ
                            self.epoch = new_epoch;

                            self.pending_batches.clear();
                            self.committed_batches.clear();

                            // !!!!! SỬA LỖI QUAN TRỌNG !!!!!
                            // Đặt lại round đồng thuận về 0 cho kỷ nguyên mới.
                            self.consensus_round.store(0, Ordering::Relaxed);
                        },
                        Err(e) => {
                            warn!("Reconfigure channel error in GarbageCollector: {}", e);
                             if e == broadcast::error::RecvError::Closed {
                                break; // Thoát vòng lặp nếu kênh bị đóng
                            }
                        }
                    }
                }
                // *** SỬA ĐỔI KẾT THÚC ***
            }
        }
    }
}
