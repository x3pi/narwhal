// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::WorkerMessage;
use crate::{metrics::WorkerMetrics, quorum_waiter::QuorumWaiterMessage};
use bytes::Bytes;
use config::UpdatableParameters;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The validator's parameters.
    parameters: Arc<RwLock<UpdatableParameters>>,
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
    network: ReliableSender,
    /// Prometheus metrics.
    metrics: Option<Arc<WorkerMetrics>>,
}

impl BatchMaker {
    /// Create a BatchMaker maker instance.
    pub fn new(
        parameters: Arc<RwLock<UpdatableParameters>>,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) -> Self {
        let batch_size = parameters.read().unwrap().batch_size;
        Self {
            parameters,
            rx_transaction,
            tx_message,
            workers_addresses,
            current_batch: Batch::with_capacity(batch_size * 2),
            current_batch_size: 0,
            network: ReliableSender::new(),
            metrics: None,
        }
    }

    /// Configure prometheus metrics.
    /// Configure prometheus metrics.
    pub fn set_metrics(mut self, metrics: Arc<WorkerMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Spawn a BatchMaker in a new task.
    pub fn spawn(mut self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let max_batch_delay = {
            let parameters = self.parameters.read().unwrap();
            parameters.max_batch_delay
        };
        let timer = sleep(Duration::from_millis(max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    let batch_size = self.parameters.read().unwrap().batch_size;
                    if self.current_batch_size >= batch_size {
                        self.seal("full").await;
                        let max_batch_delay = self.parameters.read().unwrap().max_batch_delay;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal("timeout").await;
                    }
                    let max_batch_delay = self.parameters.read().unwrap().max_batch_delay;
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self, reason: &str) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        if let Some(metrics) = self.metrics.as_ref() {
            metrics
                .batch_sealed_total
                .with_label_values(&[reason])
                .inc();
            metrics
                .batch_size_bytes_total
                .inc_by(self.current_batch_size as u64);
        }

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .map(|tx| {
                let id = tx[1..9].try_into().unwrap();
                let time = tx[9..17].try_into().unwrap();
                (u64::from_be_bytes(id), u64::from_be_bytes(time))
            })
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch {
            batch,
            #[cfg(feature = "benchmark")]
            batch_benchmark_info: config::BatchBenchmarkInfo {
                sample_txs: tx_ids.clone(),
                size,
            },
        };
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for (id, time) in &tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!("Batch {digest:?} contains sample tx {id} ({time})");
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

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
