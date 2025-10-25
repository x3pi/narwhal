// In primary/src/header_waiter.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::Header;
use crate::primary::{PrimaryMessage, PrimaryWorkerMessage, ReconfigureNotification}; // <-- THÊM ReconfigureNotification
use crate::Round; // SỬA LỖI: Thay đổi đường dẫn import
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error, info, warn}; // <-- THÊM info
use network::SimpleSender;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::broadcast; // <-- THÊM broadcast
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration, Instant};

const TIMER_RESOLUTION: u64 = 1_000;

#[derive(Debug)]
pub enum WaiterMessage {
    SyncBatches(HashMap<Digest, WorkerId>, Header),
    SyncParents(Vec<Digest>, Header),
}

pub struct HeaderWaiter {
    name: PublicKey,
    committee: Arc<RwLock<Committee>>, // SỬA ĐỔI
    store: Store,
    consensus_round: Arc<AtomicU64>,
    gc_depth: Round,
    sync_retry_delay: u64,
    sync_retry_nodes: usize,

    rx_synchronizer: Receiver<WaiterMessage>,
    tx_core: Sender<Header>,
    // *** THAY ĐỔI: Thêm Receiver ***
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,

    network: SimpleSender,
    parent_requests: HashMap<Digest, (Round, u128)>,
    batch_requests: HashMap<Digest, Round>,
    pending: HashMap<Digest, (Round, Sender<()>)>,
}

impl HeaderWaiter {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Arc<RwLock<Committee>>, // SỬA ĐỔI
        store: Store,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_synchronizer: Receiver<WaiterMessage>,
        tx_core: Sender<Header>,
        // *** THAY ĐỔI: Thêm tham số mới ***
        rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
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
                // *** THAY ĐỔI: Khởi tạo trường mới ***
                rx_reconfigure,
                network: SimpleSender::new(),
                parent_requests: HashMap::new(),
                batch_requests: HashMap::new(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

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

    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.rx_synchronizer.recv() => {
                    // *** THAY ĐỔI: Check xem committee đã bị lock chưa trước khi lock ***
                    // (Lưu ý: Đoạn code gốc đã lock ở đây)
                    let committee_guard = self.committee.read().await; // Lock committee
                    let current_epoch = committee_guard.epoch;

                    match message {
                        WaiterMessage::SyncBatches(missing, header) => {
                            // Chỉ xử lý nếu header thuộc epoch hiện tại
                            if header.epoch != current_epoch {
                                warn!("HeaderWaiter discarding sync batch request for header {} from old epoch {}", header.id, header.epoch);
                                continue;
                            }
                            debug!("Synching the payload of {}", header);
                            let header_id = header.id.clone();
                            let round = header.round;
                            let author = header.author;

                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            let wait_for = missing
                                .keys()
                                .map(|digest| (digest.to_vec(), self.store.clone()))
                                .collect();

                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (round, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);

                            let mut requires_sync = HashMap::new();
                            for (digest, worker_id) in missing.into_iter() {
                                self.batch_requests.entry(digest.clone()).or_insert_with(|| {
                                    requires_sync.entry(worker_id).or_insert_with(Vec::new).push(digest);
                                    round
                                });
                            }
                            for (worker_id, digests) in requires_sync {
                                let address = committee_guard // *** SỬA: Dùng committee_guard
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
                             // Chỉ xử lý nếu header thuộc epoch hiện tại
                            if header.epoch != current_epoch {
                                warn!("HeaderWaiter discarding sync parents request for header {} from old epoch {}", header.id, header.epoch);
                                continue;
                            }
                            debug!("Synching the parents of {}", header);
                            let header_id = header.id.clone();
                            let round = header.round;
                            let author = header.author;

                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            let wait_for = missing
                                .iter()
                                .cloned()
                                .map(|x| (x.to_vec(), self.store.clone()))
                                .collect();
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (round, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);

                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            let mut requires_sync = Vec::new();
                            for m in missing {
                                self.parent_requests.entry(m.clone()).or_insert_with(|| {
                                    requires_sync.push(m);
                                    (round, now)
                                });
                            }
                            if !requires_sync.is_empty() {
                                let address = committee_guard // *** SỬA: Dùng committee_guard
                                    .primary(&author)
                                    .expect("Author of valid header not in the committee")
                                    .primary_to_primary;
                                let message = PrimaryMessage::CertificatesRequest(requires_sync, self.name);
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                                self.network.send(address, Bytes::from(bytes)).await;
                            }
                        }
                    }
                    // drop(committee_guard) // Tự động drop khi hết scope
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
                    Ok(None) => {},
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                () = &mut timer => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry = Vec::new();
                    for (digest, (_, timestamp)) in &self.parent_requests {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for certificate {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }

                    // *** SỬA: Lock committee ở đây ***
                    let committee = self.committee.read().await;
                    let addresses = committee
                        .others_primaries(&self.name)
                        .iter()
                        .map(|(_, x)| x.primary_to_primary)
                        .collect();
                    drop(committee); // Release lock

                    let message = PrimaryMessage::CertificatesRequest(retry, self.name);
                    let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                    self.network.lucky_broadcast(addresses, Bytes::from(bytes), self.sync_retry_nodes).await;

                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },

                // *** SỬA ĐỔI BẮT ĐẦU: Sửa nhánh reconfigure ***
                result = self.rx_reconfigure.recv() => {
                    match result {
                        Ok(_notification) => {
                            // Đọc ủy ban MỚI NHẤT từ Arc để log
                            let new_committee = self.committee.read().await;
                            let new_epoch = new_committee.epoch;
                            drop(new_committee);

                            info!("HeaderWaiter received reconfigure for epoch {}. Clearing all pending requests.", new_epoch);

                            // 1. Gửi tín hiệu hủy đến tất cả các future 'waiter' đang chạy
                            for (_, (_, handler)) in self.pending.iter() {
                                let _ = handler.send(()).await; // Gửi tín hiệu hủy
                            }

                            // 2. Xóa tất cả state nội bộ
                            self.parent_requests.clear();
                            self.batch_requests.clear();
                            self.pending.clear();

                            // 3. Xóa các future khỏi FuturesUnorderedSet
                            // (Chúng sẽ tự kết thúc với Ok(None) khi nhận tín hiệu hủy)
                            waiting.clear();
                        },
                        Err(e) => {
                            warn!("Reconfigure channel error in HeaderWaiter: {}", e);
                            if e == broadcast::error::RecvError::Closed {
                                break; // Thoát vòng lặp nếu kênh bị đóng
                            }
                        }
                    }
                },
                // *** SỬA ĐỔI KẾT THÚC ***
            }

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
                self.parent_requests.retain(|_, (r, _)| r > &mut gc_round);
            }
        }
    }
}
