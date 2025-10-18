// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::{PrimaryWorkerMessage, Round};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use log::{debug, warn};
use network::SimpleSender;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::{Store, PENDING_BATCHES_CF};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{interval, Duration};

const REPROPOSE_SCAN_INTERVAL_MS: u64 = 5_000; // Quét 5 giây một lần
const ORPHAN_BATCH_THRESHOLD_ROUNDS: Round = 10; // Batch cũ hơn 10 round sẽ bị coi là mồ côi

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    store: Store,
    consensus_round: Arc<AtomicU64>,
    rx_consensus: Receiver<Certificate>,
    tx_repropose: Sender<(Digest, WorkerId, Vec<u8>)>,
    addresses: Vec<SocketAddr>,
    network: SimpleSender,
}

impl GarbageCollector {
    pub fn spawn(
        name: &PublicKey,
        committee: &Committee,
        store: Store,
        consensus_round: Arc<AtomicU64>,
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

        let pending_batches_cf = PENDING_BATCHES_CF.to_string();

        // Sử dụng phương thức iter_cf mới để lấy tất cả các batch đang chờ.
        match self.store.iter_cf(pending_batches_cf.clone()).await {
            Ok(pending_items) => {
                for (key, value_bytes) in pending_items {
                    // Key có cấu trúc [round_bytes (8), digest_bytes (32)]
                    if key.len() < 8 {
                        continue;
                    }
                    let round_bytes: [u8; 8] = key[0..8].try_into().unwrap();
                    let round = Round::from_le_bytes(round_bytes);

                    if round < orphan_round_threshold {
                        // Đây là một batch mồ côi.
                        let digest_bytes: [u8; 32] = key[8..].try_into().unwrap();
                        let digest = Digest(digest_bytes);

                        if let Ok(worker_id) = bincode::deserialize::<WorkerId>(&value_bytes) {
                            // Đọc lại dữ liệu batch từ store chính.
                            if let Ok(Some(batch_data)) = self.store.read(digest.to_vec()).await {
                                warn!(
                                    "Found orphaned batch {} from round {}, re-proposing.",
                                    digest, round
                                );
                                // Gửi lại cho Proposer.
                                if self
                                    .tx_repropose
                                    .send((digest, worker_id, batch_data))
                                    .await
                                    .is_ok()
                                {
                                    // Sau khi gửi thành công, xóa khỏi danh sách chờ.
                                    self.store.delete_cf(pending_batches_cf.clone(), key).await;
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to iterate over pending batches: {}", e);
            }
        }
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;
        let mut repropose_timer = interval(Duration::from_millis(REPROPOSE_SCAN_INTERVAL_MS));

        loop {
            tokio::select! {
                Some(certificate) = self.rx_consensus.recv() => {
                    // Khi một certificate được commit, xóa các batch của nó khỏi danh sách chờ.
                    for (digest, _) in certificate.header.payload.iter() {
                        let key = [certificate.round().to_le_bytes().to_vec(), digest.to_vec()].concat();
                        self.store
                            .delete_cf(PENDING_BATCHES_CF.to_string(), key)
                            .await;
                    }

                    let round = certificate.round();
                    if round > last_committed_round {
                        last_committed_round = round;

                        self.consensus_round.store(round, Ordering::Relaxed);

                        let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                            .expect("Failed to serialize our own message");
                        self.network
                            .broadcast(self.addresses.clone(), Bytes::from(bytes))
                            .await;
                    }
                },
                _ = repropose_timer.tick() => {
                    // Theo định kỳ, quét và đề xuất lại các batch mồ côi.
                    self.re_propose_orphaned_batches().await;
                }
            }
        }
    }
}
