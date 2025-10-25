// In worker/src/quorum_waiter.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use config::{Committee, Stake};
use crypto::PublicKey;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, warn}; // <-- THÊM debug
use network::CancelHandler;
use std::sync::Arc; // <-- THÊM Arc
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock; // <-- THÊM RwLock

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

#[derive(Debug)]
pub struct QuorumWaiterMessage {
    /// A serialized `WorkerMessage::Batch` message.
    pub batch: SerializedBatchMessage,
    /// The cancel handlers to receive the acknowledgements of our broadcast.
    pub handlers: Vec<(PublicKey, CancelHandler)>,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// The committee information.
    // *** THAY ĐỔI: Sử dụng Arc<RwLock<Committee>> ***
    committee: Arc<RwLock<Committee>>,
    /// The stake of this authority (worker).
    // *** THAY ĐỔI: Thêm name để lấy stake động ***
    name: PublicKey,
    /// Input Channel to receive commands.
    rx_message: Receiver<QuorumWaiterMessage>,
    /// Channel to deliver batches for which we have enough acknowledgements.
    tx_batch: Sender<SerializedBatchMessage>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    // *** THAY ĐỔI: Cập nhật chữ ký hàm ***
    pub fn spawn(
        name: PublicKey, // <-- Thêm name
        committee: Arc<RwLock<Committee>>,
        // stake: Stake, // Loại bỏ stake cố định
        rx_message: Receiver<QuorumWaiterMessage>,
        tx_batch: Sender<Vec<u8>>,
    ) {
        tokio::spawn(async move {
            Self {
                name, // <-- Khởi tạo name
                committee,
                // stake, // Loại bỏ
                rx_message,
                tx_batch,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(QuorumWaiterMessage { batch, handlers }) = self.rx_message.recv().await {
            // *** THAY ĐỔI: Lấy committee và stake động trong mỗi lần lặp ***
            let committee = self.committee.read().await;
            let my_stake = committee.stake(&self.name);
            let quorum_threshold = committee.quorum_threshold();

            debug!(
                "Received batch for quorum wait. My stake: {}, Quorum threshold: {}",
                my_stake, quorum_threshold
            );

            let mut wait_for_quorum: FuturesUnordered<_> = handlers
                .into_iter()
                .map(|(name, handler)| {
                    // Lấy stake từ committee hiện tại
                    let stake = committee.stake(&name);
                    Self::waiter(handler, stake)
                })
                .collect();

            // Drop committee lock sớm
            drop(committee);

            // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
            // delivered and we send its digest to the primary (that will include it into
            // the dag). This should reduce the amount of synching.
            let mut total_stake = my_stake; // Bắt đầu với stake của chính mình
            while let Some(stake) = wait_for_quorum.next().await {
                total_stake += stake;
                debug!(
                    "Received ack, total stake now: {}/{}",
                    total_stake, quorum_threshold
                );
                if total_stake >= quorum_threshold {
                    debug!("Quorum reached! Sending batch to processor.");
                    if self.tx_batch.send(batch).await.is_err() {
                        // Lỗi xảy ra nếu kênh tx_batch bị đóng (ví dụ: Processor đã dừng)
                        warn!("Failed to send batch to processor channel closed.");
                    }
                    // Thoát khỏi vòng lặp while let Some(stake) sau khi đạt quorum
                    break;
                }
            }
            // Nếu vòng lặp kết thúc mà không đạt quorum (ví dụ: tất cả handler bị hủy)
            if total_stake < quorum_threshold {
                warn!(
                    "Batch failed to reach quorum ({}/{})",
                    total_stake, quorum_threshold
                );
            }
        }
    }
}
