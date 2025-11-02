// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use log::info;
use network::{CancelHandler, SimpleSender}; // THÊM CancelHandler
#[cfg(feature = "benchmark")]
use sha3::{Digest as Sha3Digest, Sha3_512 as Sha512};
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    simple_network: SimpleSender,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                simple_network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    // Log when transaction is added to batch
                    #[cfg(feature = "benchmark")]
                    {
                        // Tạo hash SHA-512 của toàn bộ nội dung transaction làm định danh
                        let hash = Sha512::digest(&transaction);
                        let mut digest_bytes = [0u8; 32];
                        digest_bytes.copy_from_slice(&hash[..32]);
                        let tx_digest = Digest(digest_bytes);

                        // Also extract original tx ID if it's a sample transaction
                        let original_tx_id = if transaction.len() > 8 && transaction[0] == 0u8 {
                            let tx_id_bytes: [u8; 8] = transaction[1..9].try_into().unwrap_or([0; 8]);
                            Some(u64::from_be_bytes(tx_id_bytes))
                        } else {
                            None
                        };

                        if let Some(orig_id) = original_tx_id {
                            log::info!("[TX_ADDED_TO_BATCH] Transaction {} (tx_hash: {}) added to current batch (batch_size: {} bytes, tx_count: {})",
                                orig_id,
                                tx_digest,
                                self.current_batch_size + transaction.len(),
                                self.current_batch.len() + 1);
                        } else {
                            log::info!("[TX_ADDED_TO_BATCH] Transaction (tx_hash: {}) added to current batch (batch_size: {} bytes, tx_count: {})",
                                tx_digest,
                                self.current_batch_size + transaction.len(),
                                self.current_batch.len() + 1);
                        }
                    }

                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        #[cfg(feature = "benchmark")]
        let batch_len = self.current_batch.len();

        // Extract both original tx IDs and transaction hashes (SHA-512, 32 bytes) for all transactions
        #[cfg(feature = "benchmark")]
        let (tx_ids, tx_hashes): (Vec<_>, Vec<_>) = self
            .current_batch
            .iter()
            .map(|tx| {
                // Extract original tx ID if it's a sample transaction
                let orig_id = if tx[0] == 0u8 && tx.len() > 8 {
                    tx[1..9]
                        .try_into()
                        .ok()
                        .map(|bytes: [u8; 8]| u64::from_be_bytes(bytes))
                } else {
                    None
                };

                // Tạo hash SHA-512 của toàn bộ nội dung transaction làm định danh
                let hash = Sha512::digest(tx);
                let mut digest_bytes = [0u8; 32];
                digest_bytes.copy_from_slice(&hash[..32]);
                let tx_digest = Digest(digest_bytes);

                (orig_id, tx_digest)
            })
            .unzip();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let hash = Sha512::digest(&serialized);
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&hash[..32]);
            let digest = Digest(bytes);

            // Log sample transactions with original IDs
            for orig_id_opt in &tx_ids {
                if let Some(orig_id) = orig_id_opt {
                    // NOTE: This log entry is used to compute performance.
                    info!("Batch {:?} contains sample tx {}", digest, orig_id);
                }
            }

            // Log all transactions in batch with both original IDs (if available) and transaction hashes
            let tx_info: Vec<String> = tx_ids
                .iter()
                .zip(tx_hashes.iter())
                .map(|(orig_id_opt, tx_hash)| {
                    if let Some(orig_id) = orig_id_opt {
                        format!("{}[hash:{}]", orig_id, tx_hash)
                    } else {
                        format!("[hash:{}]", tx_hash)
                    }
                })
                .collect();

            info!(
                "[BATCH_CREATED] Batch {:?} created with {} transactions (tx_info: {:?}, total_size: {} B)",
                digest,
                batch_len,
                tx_info,
                size
            );

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        self.simple_network.broadcast(addresses, bytes).await;

        // SỬA LỖI: Cung cấp kiểu dữ liệu tường minh cho channel và collection.
        // Tạo các dummy handler vì QuorumWaiter vẫn cần chúng.
        let handlers: Vec<CancelHandler> = names
            .iter()
            .map(|_| tokio::sync::oneshot::channel::<Bytes>().1)
            .collect();

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }
}
