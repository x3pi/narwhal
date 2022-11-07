use std::time::Duration;

use crate::{batch_loader::SerializedBatchMessage, transaction::Transaction};
use crypto::Digest;
use log::{info, warn};
use primary::Certificate;
use tokio::{sync::mpsc::Receiver, time::timeout};
use worker::WorkerMessage;

#[derive(Debug)]
pub struct CoreMessage {
    /// The serialized batch message.
    pub batch: SerializedBatchMessage,
    /// The digest of the batch.
    pub digest: Digest,
    /// The certificate referencing this batch.
    pub certificate: Certificate,
}

/// Executes batches of transactions.
pub struct Core {
    /// Input channel to receive (serialized) batches to execute.
    rx_batch_loader: Receiver<CoreMessage>,
}

impl Core {
    pub fn spawn(rx_batch_loader: Receiver<CoreMessage>) {
        tokio::spawn(async move {
            Self { rx_batch_loader }.run().await;
        });
    }

    /// Simple function simulating a CPU-intensive execution.
    async fn burn_cpu() {
        fn x() {
            let mut _x = 0;
            loop {
                _x += 1;
                _x -= 1;
            }
        }
        std::hint::black_box(x())
    }

    /// Main loop receiving batches to execute.
    async fn run(&mut self) {
        while let Some(core_message) = self.rx_batch_loader.recv().await {
            let CoreMessage {
                batch,
                digest,
                certificate,
            } = core_message;

            match bincode::deserialize(&batch) {
                Ok(WorkerMessage::Batch(batch)) => {
                    // Deserialize each transaction.
                    for serialized_tx in batch {
                        let transaction: Transaction = match bincode::deserialize(&serialized_tx) {
                            Ok(x) => x,
                            Err(e) => {
                                #[cfg(feature = "benchmark")]
                                panic!("Failed to deserialize transaction: {e}");

                                warn!("Failed to deserialize transaction: {e}");
                                continue;
                            }
                        };

                        // Execute the transaction.
                        let duration = Duration::from_millis(transaction.contract);
                        let _ = timeout(duration, Self::burn_cpu()).await;

                        #[cfg(not(feature = "benchmark"))]
                        {
                            info!("Executed {}", certificate.header);
                            let _digest = digest.clone();
                        }

                        #[cfg(feature = "benchmark")]
                        for digest in certificate.header.payload.keys() {
                            // NOTE: This log entry is used to compute performance.
                            info!("Executed {} -> {:?}", certificate.header, digest);
                        }
                    }
                }
                Ok(_) => panic!("Unexpected protocol message"),
                Err(e) => panic!("Failed to deserialize batch: {}", e),
            }
        }
    }
}
