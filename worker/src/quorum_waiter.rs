// In worker/src/quorum_waiter.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use config::{Committee, Stake};
use crypto::PublicKey;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info, warn}; // <-- THÊM debug
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
            // ✅ LẤY COMMITTEE VÀ STAKE ĐỘNG TRONG MỖI LẦN LẶP
            let committee = self.committee.read().await;
            let my_stake = committee.stake(&self.name);
            let quorum_threshold = committee.quorum_threshold();
            let committee_size = committee.authorities.len();

            info!("QuorumWaiter: Processing batch. My stake: {}, Quorum: {}, Committee size: {}, Handlers: {}", 
                  my_stake, quorum_threshold, committee_size, handlers.len());

            // ✅ KIỂM TRA HANDLERS HỢP LỆ
            if handlers.is_empty() && my_stake < quorum_threshold {
                warn!(
                    "QuorumWaiter: No handlers and stake insufficient. Batch may not reach quorum!"
                );
            }

            let mut wait_for_quorum: FuturesUnordered<_> = handlers
                .into_iter()
                .map(|(name, handler)| {
                    let stake = committee.stake(&name);
                    debug!(
                        "QuorumWaiter: Waiting for ACK from {} (stake: {})",
                        name, stake
                    );
                    Self::waiter(handler, stake)
                })
                .collect();

            drop(committee);

            // Wait for quorum
            let mut total_stake = my_stake;
            info!(
                "QuorumWaiter: Starting quorum wait with initial stake {}/{}",
                total_stake, quorum_threshold
            );

            while let Some(stake) = wait_for_quorum.next().await {
                total_stake += stake;
                debug!(
                    "QuorumWaiter: Received ACK (stake: {}), total: {}/{}",
                    stake, total_stake, quorum_threshold
                );

                if total_stake >= quorum_threshold {
                    info!("QuorumWaiter: Quorum reached! Sending batch to Processor.");
                    if self.tx_batch.send(batch).await.is_err() {
                        warn!("QuorumWaiter: Failed to send batch - channel closed");
                    }
                    break;
                }
            }

            if total_stake < quorum_threshold {
                warn!(
                    "QuorumWaiter: Failed to reach quorum ({}/{}) for batch",
                    total_stake, quorum_threshold
                );
            }
        }
    }
}
