// In primary/src/proposer.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::{CommittedBatches, PendingBatches, ReconfigureNotification};
// SỬA LỖI: Thêm Epoch
use crate::{Epoch, Round};
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::info;
use log::{debug, warn};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use store::Store;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

pub struct Proposer {
    name: PublicKey,
    committee: Arc<RwLock<Committee>>,
    signature_service: SignatureService,
    store: Store,
    header_size: usize,
    max_header_delay: u64,

    pending_batches: PendingBatches,
    committed_batches: CommittedBatches,
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,

    rx_core: Receiver<(Vec<Digest>, Round)>,
    rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
    rx_repropose: Receiver<(Digest, WorkerId, Vec<u8>)>,
    tx_core: Sender<Header>,

    epoch: Epoch, // <-- Trường epoch nội bộ
    round: Round,
    last_parents: Vec<Digest>,
    digests: Vec<(Digest, WorkerId)>,
    payload_size: usize,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Arc<RwLock<Committee>>,
        signature_service: SignatureService,
        store: Store,
        header_size: usize,
        max_header_delay: u64,
        pending_batches: PendingBatches,
        committed_batches: CommittedBatches,
        _epoch_transitioning: Arc<AtomicBool>,
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
        rx_repropose: Receiver<(Digest, WorkerId, Vec<u8>)>,
        tx_core: Sender<Header>,
        rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    ) {
        tokio::spawn(async move {
            let initial_committee = committee.read().await;
            let genesis = Certificate::genesis(&*initial_committee)
                .iter()
                .map(|x| x.digest()) // Sửa lỗi cú pháp ở đây
                .collect();
            let initial_epoch = initial_committee.epoch;
            drop(initial_committee);

            Self {
                name,
                committee,
                signature_service,
                store,
                header_size,
                max_header_delay,
                pending_batches,
                committed_batches,
                rx_reconfigure,
                rx_core,
                rx_workers,
                rx_repropose,
                tx_core,
                epoch: initial_epoch, // Khởi tạo epoch nội bộ
                round: 1,
                last_parents: genesis,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        let digests_for_header: Vec<_> = self.digests.drain(..).collect();

        if self.last_parents.is_empty() {
            warn!("Proposer::make_header called with no parents. Skipping.");
            return;
        }

        let current_epoch = self.epoch; // Dùng epoch nội bộ

        let header = Header::new(
            self.name,
            self.round,
            current_epoch,
            digests_for_header.iter().cloned().collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        for (digest, worker_id) in digests_for_header {
            self.pending_batches.insert(digest, (worker_id, self.round));
        }

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            info!("Created {} -> {:?}", header, digest);
        }

        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);
        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            let timer_expired = timer.is_elapsed();

            if !self.last_parents.is_empty()
                && (timer_expired || self.payload_size >= self.header_size)
            {
                if !self.digests.is_empty() || timer_expired {
                    self.make_header().await;
                    self.payload_size = 0;
                    let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                    timer.as_mut().reset(deadline);
                }
            }

            tokio::select! {
                // *** LOGIC RECONFIGURE ĐÃ SỬA ***
                result = self.rx_reconfigure.recv() => {
                    match result {
                        Ok(_notification) => { // Trigger only, ignore old committee inside

                            // 1. Đọc ủy ban MỚI NHẤT từ Arc
                            let new_committee = self.committee.read().await;
                            let new_epoch = new_committee.epoch;

                            // 2. So sánh epoch MỚI TỪ ARC với epoch NỘI BỘ
                            if new_epoch > self.epoch {
                                info!("Proposer detected epoch change from {} to {}. Resetting and proposing empty header.", self.epoch, new_epoch);

                                // 3. Cập nhật epoch nội bộ
                                self.epoch = new_epoch;

                                // 4. Reset trạng thái DÙNG ỦY BAN MỚI (từ Arc)
                                self.round = 1;
                                self.digests.clear();
                                self.payload_size = 0;
                                self.last_parents = Certificate::genesis(&*new_committee) // <-- DÙNG new_committee TỪ ARC
                                    .iter()
                                    .map(|x| x.digest())
                                    .collect();

                                // 5. Giải phóng lock sớm
                                drop(new_committee);

                                // 6. Chủ động đề xuất header MỚI (header rỗng)
                                if !self.last_parents.is_empty() {
                                    self.make_header().await; // Sẽ dùng self.epoch=2, self.round=1
                                } else {
                                    warn!("Proposer reset for epoch {} but found no genesis parents!", self.epoch);
                                }

                                // 7. Reset timer
                                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                                timer.as_mut().reset(deadline);
                            } else {
                                // Epoch không mới hơn, chỉ cần giải phóng lock
                                drop(new_committee);
                                // Log cũ không còn đúng, nên xóa hoặc sửa
                                // warn!("Proposer received reconfigure signal but epoch {} is not newer than {}.", new_epoch, self.epoch);
                                debug!("Proposer received reconfigure signal for epoch {} but current epoch is already {}. Ignoring reset.", new_epoch, self.epoch);
                            }
                        },
                        Err(e) => warn!("Reconfigure channel error in Proposer: {}", e),
                    }
                },
                // *** KẾT THÚC SỬA LOGIC RECONFIGURE ***

                Some((parents, round)) = self.rx_core.recv() => {
                    // Chỉ cập nhật round và parents nếu nó thuộc epoch hiện tại hoặc tương lai gần
                    // (Có thể cần kiểm tra epoch của certificate tạo ra parents này nếu có)
                    if self.epoch > 0 && round >= self.round { // Điều kiện đơn giản hóa
                        self.round = round + 1;
                        debug!("Dag moved to round {}", self.round);
                        self.last_parents = parents;
                         // Reset timer nếu có parents mới để đảm bảo đề xuất kịp thời
                        let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                        timer.as_mut().reset(deadline);
                    } else if round < self.round {
                         debug!("Received outdated parents for round {}, current round is {}. Ignoring.", round, self.round);
                    }
                     // Trường hợp epoch không khớp có thể cần xử lý thêm nếu có
                }
                Some((digest, worker_id, batch)) = self.rx_workers.recv() => {
                    // Chỉ xử lý batch nếu node không đang trong quá trình chuyển đổi (hoặc đã chuyển xong)
                     // Logic epoch_transitioning có thể hữu ích ở đây nếu cần tạm dừng nhận batch mới
                    if !self.committed_batches.contains_key(&digest) && !self.pending_batches.contains_key(&digest) {
                        // Nên kiểm tra xem batch này có thuộc epoch hiện tại không nếu có thông tin
                        self.store.write(digest.to_vec(), batch).await;
                        self.payload_size += digest.size();
                        self.digests.push((digest, worker_id));
                    }
                }
                Some((digest, worker_id, _batch)) = self.rx_repropose.recv() => {
                     // Tương tự như trên
                    if !self.committed_batches.contains_key(&digest) && !self.pending_batches.contains_key(&digest) {
                        warn!("Re-proposing orphaned batch {}", digest);
                        self.payload_size += digest.size();
                        self.digests.push((digest, worker_id));
                    }
                }
                () = &mut timer => {
                     // Timer hết hạn, nếu có parents thì đề xuất header (có thể rỗng)
                     if !self.last_parents.is_empty() {
                         debug!("Proposer timer expired, making header for round {}", self.round);
                         self.make_header().await;
                         self.payload_size = 0; // Reset payload size sau khi đề xuất
                     } else {
                         debug!("Proposer timer expired but no parents available for round {}. Waiting.", self.round);
                     }
                      // Reset lại timer bất kể có đề xuất được hay không
                     let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                     timer.as_mut().reset(deadline);
                }

            }
        }
    }
}
