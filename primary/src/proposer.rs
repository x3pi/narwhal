// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::{PendingBatches, Round};
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
#[cfg(feature = "benchmark")]
use log::info;
use log::{debug, warn};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

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

    /// Cache dùng chung để theo dõi các batch đang chờ, được chia sẻ với GarbageCollector.
    pending_batches: PendingBatches,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<(Vec<Digest>, Round)>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
    /// Receives orphaned batches from the `GarbageCollector`.
    rx_repropose: Receiver<(Digest, WorkerId, Vec<u8>)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Digest>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
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
        pending_batches: PendingBatches,
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
        rx_repropose: Receiver<(Digest, WorkerId, Vec<u8>)>,
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
                pending_batches,
                rx_core,
                rx_workers,
                rx_repropose,
                tx_core,
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
        // Gom các digest để đưa vào header.
        let digests_for_header: Vec<(Digest, WorkerId)> = self.digests.drain(..).collect();

        // Tạo header mới.
        let header = Header::new(
            self.name,
            self.round,
            digests_for_header.iter().cloned().collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        // Ghi vào cache dùng chung (thao tác nhanh, không khóa).
        // Đây là cách Proposer "thông báo" cho GarbageCollector về các batch đã được đề xuất.
        for (digest, worker_id) in digests_for_header {
            self.pending_batches.insert(digest, (worker_id, self.round));
        }

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            info!("Created {} -> {:?}", header, digest);
        }

        // Gửi header mới đến Core để xử lý tiếp.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    /// Vòng lặp chính lắng nghe các tin nhắn đến.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);
        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // Kiểm tra các điều kiện để tạo header mới.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();

            if (timer_expired || enough_digests) && enough_parents {
                // Tạo header mới và reset trạng thái.
                self.make_header().await;
                self.payload_size = 0;
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                // Nhận thông tin về các parent certificate từ Core.
                Some((parents, round)) = self.rx_core.recv() => {
                    if round >= self.round {
                        self.round = round + 1;
                        debug!("Dag moved to round {}", self.round);
                        self.last_parents = parents;
                    }
                }
                // Nhận batch mới từ các worker.
                Some((digest, worker_id, batch)) = self.rx_workers.recv() => {
                    self.store.write(digest.to_vec(), batch).await;
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                // Nhận lại các batch mồ côi từ GarbageCollector.
                Some((digest, worker_id, _batch)) = self.rx_repropose.recv() => {
                    warn!("Re-proposing orphaned batch {}", digest);
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                // Xử lý khi timer hết hạn.
                () = &mut timer => {}
            }
        }
    }
}
