// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
use log::{info, warn};
use network::{CancelHandler, SimpleSender};
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

        info!(
            "BatchMaker started: batch_size={} bytes, max_delay={} ms, workers={}",
            self.batch_size,
            self.max_batch_delay,
            self.workers_addresses.len()
        );

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    let tx_size = transaction.len();
                    self.current_batch_size += tx_size;
                    self.current_batch.push(transaction);

                    // Log khi nhận transaction
                    if self.current_batch.len() == 1 {
                        info!("BatchMaker: Started new batch (first tx size: {} bytes)", tx_size);
                    }

                    if self.current_batch_size >= self.batch_size {
                        info!(
                            "BatchMaker: Batch size threshold reached ({}/{} bytes, {} txs). Sealing batch.",
                            self.current_batch_size, self.batch_size, self.current_batch.len()
                        );
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        info!(
                            "BatchMaker: Timer expired. Sealing batch with {}/{} bytes ({} txs).",
                            self.current_batch_size, self.batch_size, self.current_batch.len()
                        );
                        self.seal().await;
                    } else {
                        // Log chỉ ở debug level để tránh spam
                        // debug!("BatchMaker: Timer expired but batch is empty, no action taken.");
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        // ✅ KIỂM TRA BATCH KHÔNG RỖNG
        if self.current_batch.is_empty() {
            warn!("BatchMaker: Attempted to seal empty batch, skipping");
            return;
        }

        let batch_tx_count = self.current_batch.len();
        let batch_size_bytes = self.current_batch_size;

        info!(
            "BatchMaker: Sealing batch with {} transactions ({} bytes)",
            batch_tx_count, batch_size_bytes
        );

        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        info!(
            "BatchMaker: Batch serialized to {} bytes (original size: {} bytes)",
            serialized.len(),
            batch_size_bytes
        );

        #[cfg(feature = "benchmark")]
        {
            let digest = Digest(Sha512::digest(&serialized)[..32].try_into().unwrap());
            for id in tx_ids {
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }
            info!("Batch {:?} contains {} B", digest, size);
        }

        // ✅ FIX: CHỈ ĐỊNH KIỂU RÕ RÀNG CHO UNZIP
        let (names, addresses): (Vec<PublicKey>, Vec<SocketAddr>) =
            self.workers_addresses.iter().cloned().unzip();

        let bytes = Bytes::from(serialized.clone());

        info!(
            "BatchMaker: Broadcasting batch to {} workers at addresses: {:?}",
            addresses.len(),
            addresses
        );

        self.simple_network
            .broadcast(addresses.clone(), bytes)
            .await;

        info!(
            "BatchMaker: Broadcast completed to {} workers. Preparing QuorumWaiter message.",
            addresses.len()
        );

        let handlers: Vec<CancelHandler> = names
            .iter()
            .map(|_| {
                let (_, rx) = tokio::sync::oneshot::channel::<Bytes>();
                rx
            })
            .collect();

        info!(
            "BatchMaker: Sending batch to QuorumWaiter with {} handlers",
            handlers.len()
        );

        let quorum_message = QuorumWaiterMessage {
            batch: serialized.clone(),
            handlers: names.into_iter().zip(handlers.into_iter()).collect(),
        };

        match self.tx_message.send(quorum_message).await {
            Ok(_) => {
                info!(
                    "BatchMaker: Batch successfully sent to QuorumWaiter ({} bytes, {} txs)",
                    serialized.len(),
                    batch_tx_count
                );
            }
            Err(e) => {
                warn!(
                    "BatchMaker: Failed to send batch to QuorumWaiter: {}. Channel may be closed.",
                    e
                );
            }
        }
    }
}
