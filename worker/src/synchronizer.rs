// In worker/src/synchronizer.rs

// Copyright(C) Facebook, Inc. and its affiliates.
// *** THAY ĐỔI: Thêm ResetEpoch vào import ***
use crate::worker::{PrimaryWorkerMessage, Round, WorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error, info, warn}; // <-- THÊM info, warn
use network::SimpleSender;
// primary::PrimaryWorkerMessage đã được import ở trên
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

// *** THAY ĐỔI: Thêm định nghĩa ResetEpoch vào PrimaryWorkerMessage (trong worker.rs) ***
// Giả sử đã định nghĩa trong worker.rs:
// pub enum PrimaryWorkerMessage {
//     Synchronize(Vec<Digest>, PublicKey),
//     Cleanup(Round),
//     Reconfigure(Committee),
//     ResetEpoch(Epoch), // <-- Thông điệp mới
// }

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
    // *** THAY ĐỔI: Lưu trữ thêm Epoch trong pending ***
    pending: HashMap<Digest, (Round, Sender<()>, u128, /* epoch */ u64)>,
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
                    // *** THAY ĐỔI: Lấy committee và epoch hiện tại ***
                    let committee_guard = self.committee.read().await;
                    let current_epoch = committee_guard.epoch;

                    match message {
                        PrimaryWorkerMessage::Synchronize(digests, target) => {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();

                            let mut missing = Vec::new();
                            for digest in digests {
                                // Bỏ qua nếu đã pending từ epoch hiện tại
                                if self.pending.get(&digest).map_or(false, |(_, _, _, epoch)| *epoch == current_epoch) {
                                    continue;
                                }
                                match self.store.read(digest.to_vec()).await {
                                    Ok(None) => {
                                        missing.push(digest.clone());
                                        debug!("Requesting sync for batch {}", digest);

                                        // Thêm vào pending chỉ khi thực sự yêu cầu sync
                                        let deliver = digest.clone();
                                        let (tx_cancel, rx_cancel) = channel(1);
                                        let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                                        waiting.push(fut);
                                        // Lưu epoch hiện tại vào pending
                                        self.pending.insert(digest, (self.round, tx_cancel, now, current_epoch));
                                    },
                                    Ok(Some(_)) => {}, // Đã có trong store, không cần làm gì
                                    Err(e) => {
                                        error!("Store error reading digest {}: {}", digest, e);
                                        // Không thêm vào pending nếu có lỗi store
                                        continue;
                                    }
                                }
                            }
                            if !missing.is_empty() {
                                // Lấy địa chỉ từ committee đã lock
                                let address = match committee_guard.worker(&target, &self.id) {
                                    Ok(address) => address.worker_to_worker,
                                    Err(e) => {
                                        error!("Primary asked us to sync with an unknown node: {}", e);
                                        // Drop guard trước khi continue
                                        drop(committee_guard);
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
                                // Drop guard trước khi continue
                                drop(committee_guard);
                                continue;
                            }
                            let gc_round = self.round.saturating_sub(self.gc_depth);

                            // Hủy các pending requests quá cũ (bất kể epoch)
                            let mut cancelled_digests = Vec::new();
                            for (digest, (r, handler, _, _)) in &self.pending {
                                if r <= &gc_round {
                                     debug!("GC cancelling old pending sync request for {}", digest);
                                    let _ = handler.try_send(()); // try_send để không block nếu channel đầy/đã đóng
                                    cancelled_digests.push(digest.clone());
                                }
                            }
                            // Xóa các entry đã hủy khỏi pending
                            for digest in cancelled_digests {
                                self.pending.remove(&digest);
                            }
                        },
                        PrimaryWorkerMessage::Reconfigure(_) => {
                            // Logic reconfigure chính nằm ở worker.rs
                            // Synchronizer sẽ nhận ResetEpoch ngay sau đó
                            info!("Synchronizer acknowledged Reconfigure signal.");
                        },
                        // *** THAY ĐỔI: Xử lý ResetEpoch ***
                        PrimaryWorkerMessage::ResetEpoch(new_epoch) => {
                             info!("Synchronizer received ResetEpoch signal for epoch {}. Clearing pending requests from previous epochs.", new_epoch);
                             let mut cancelled_digests = Vec::new();
                             // Hủy tất cả các pending requests không thuộc epoch mới
                            for (digest, (_, handler, _, pending_epoch)) in &self.pending {
                                if *pending_epoch < new_epoch {
                                    debug!("ResetEpoch cancelling pending sync request for {} from epoch {}", digest, pending_epoch);
                                    let _ = handler.try_send(());
                                    cancelled_digests.push(digest.clone());
                                }
                            }
                            // Xóa các entry đã hủy khỏi pending
                            for digest in cancelled_digests {
                                self.pending.remove(&digest);
                            }
                            // Reset round nội bộ (tùy chọn, có thể không cần thiết nếu Cleanup vẫn hoạt động)
                            // self.round = 0;
                        }
                    }
                    // Drop guard ở cuối scope của match
                    drop(committee_guard);
                },
                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        // Batch đã được đọc thành công từ store
                        self.pending.remove(&digest);
                    },
                    Ok(None) => {
                        // Future bị hủy (do GC hoặc ResetEpoch)
                        // Entry tương ứng đã bị xóa khỏi self.pending
                    },
                    Err(e) => error!("Synchronizer waiter error: {}", e)
                },
                () = &mut timer => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let committee = self.committee.read().await; // Lock committee để lấy địa chỉ retry
                    let current_epoch = committee.epoch;

                    let mut retry_digests = Vec::new();
                    let mut updated_timestamps = Vec::new(); // Lưu digest cần cập nhật timestamp

                    for (digest, (_, _, timestamp, epoch)) in &self.pending {
                        // Chỉ retry nếu request thuộc epoch hiện tại và đã quá hạn
                        if *epoch == current_epoch && timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Retrying sync for batch {}", digest);
                            retry_digests.push(digest.clone());
                            updated_timestamps.push(digest.clone()); // Đánh dấu để cập nhật timestamp
                        }
                    }

                    if !retry_digests.is_empty() {
                        let addresses: Vec<_> = committee // Sử dụng committee đã lock
                            .others_workers(&self.name, &self.id)
                            .iter().map(|(_, address)| address.worker_to_worker)
                            .collect();

                        if !addresses.is_empty() { // Chỉ broadcast nếu có người nhận
                            let message = WorkerMessage::BatchRequest(retry_digests, self.name);
                            let serialized = bincode::serialize(&message).expect("Failed to serialize message");
                            self.network
                                .lucky_broadcast(addresses, Bytes::from(serialized), self.sync_retry_nodes)
                                .await;

                            // Cập nhật timestamp cho các digest đã retry
                            for digest in updated_timestamps {
                                if let Some(entry) = self.pending.get_mut(&digest) {
                                     entry.2 = now; // Cập nhật timestamp thành thời điểm retry
                                }
                            }
                        } else {
                            warn!("No other workers found to retry batch requests.");
                        }
                    }
                    drop(committee); // Drop lock
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }
}
