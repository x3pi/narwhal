// Copyright(C) Facebook, Inc. and its affiliates.
use crate::{metrics::WorkerMetrics, worker::SerializedBatchDigestMessage};
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use prometheus::Registry;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor {
    id: WorkerId,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive batches.
    rx_batch: Receiver<SerializedBatchMessage>,
    /// Output channel to send out batches' digests.
    tx_digest: Sender<SerializedBatchDigestMessage>,
    /// Whether we are processing our own batches or the batches of other nodes.
    own_digest: bool,
    /// Prometheus metrics.
    metrics: Option<WorkerMetrics>,
}

impl Processor {
    /// Create a new Processor.
    pub fn new(
        id: WorkerId,
        store: Store,
        rx_batch: Receiver<SerializedBatchMessage>,
        tx_digest: Sender<SerializedBatchDigestMessage>,
        own_digest: bool,
    ) -> Self {
        Self {
            id,
            store,
            rx_batch,
            tx_digest,
            own_digest,
            metrics: None,
        }
    }

    /// Configure prometheus metrics.
    pub fn set_metrics(mut self, registry: &Registry) -> Self {
        self.metrics = Some(WorkerMetrics::new(registry));
        self
    }

    /// Spawn a QuorumWaiter in a new task.
    pub fn spawn(mut self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(batch) = self.rx_batch.recv().await {
            // Hash the batch.
            let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());

            // Store the batch.
            self.store.write(digest.to_vec(), batch).await;

            // Deliver the batch's digest.
            let (message, origin) = match self.own_digest {
                true => (WorkerPrimaryMessage::OurBatch(digest, self.id), "own"),
                false => (WorkerPrimaryMessage::OthersBatch(digest, self.id), "others"),
            };

            if let Some(metrics) = self.metrics.as_ref() {
                metrics
                    .batch_persisted_total
                    .with_label_values(&[origin])
                    .inc();
            }

            let message = bincode::serialize(&message)
                .expect("Failed to serialize our own worker-primary message");
            self.tx_digest
                .send(message)
                .await
                .expect("Failed to send digest");
        }
    }
}
