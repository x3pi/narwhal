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

pub struct Proposer {
    name: PublicKey,
    signature_service: SignatureService,
    store: Store,
    header_size: usize,
    max_header_delay: u64,

    pending_batches: PendingBatches,

    rx_core: Receiver<(Vec<Digest>, Round)>,
    /// Nhận tất cả batch (bao gồm cả nội dung) từ các worker.
    rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
    rx_repropose: Receiver<(Digest, WorkerId, Vec<u8>)>,
    tx_core: Sender<Header>,

    round: Round,
    last_parents: Vec<Digest>,
    digests: Vec<(Digest, WorkerId)>,
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
        let digests_for_header: Vec<(Digest, WorkerId)> = self.digests.drain(..).collect();
        let header = Header::new(
            self.name,
            self.round,
            digests_for_header.iter().cloned().collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        // Ghi vào cache để GC theo dõi.
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
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();

            if (timer_expired || enough_digests) && enough_parents {
                self.make_header().await;
                self.payload_size = 0;
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    if round >= self.round {
                        self.round = round + 1;
                        debug!("Dag moved to round {}", self.round);
                        self.last_parents = parents;
                    }
                }
                // --- Proposer nhận lại trách nhiệm ghi vào store ---
                Some((digest, worker_id, batch)) = self.rx_workers.recv() => {
                    self.store.write(digest.to_vec(), batch).await;
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                Some((digest, worker_id, _batch)) = self.rx_repropose.recv() => {
                    // Batch đã có trong store, chỉ cần thêm lại vào danh sách đề xuất.
                    warn!("Re-proposing orphaned batch {}", digest);
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                () = &mut timer => {}
            }
        }
    }
}
