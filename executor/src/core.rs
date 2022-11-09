use std::time::Duration;

use crate::{batch_loader::SerializedBatchMessage, transaction::Transaction};
use crypto::Digest;
use log::{debug, info};
use primary::Certificate;
use tokio::{sync::mpsc::Receiver, time::Instant};
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
    async fn burn_cpu(duration: Duration) {
        let now = Instant::now();
        let mut _x = 0;
        loop {
            while _x < 1_000_000 {
                _x += 1;
            }
            _x = 0;
            if now.elapsed() >= duration {
                return;
            }
        }
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
                    debug!("Executing batch {digest} ({} tx)", batch.len());

                    // Deserialize each transaction.
                    for serialized_tx in batch {
                        #[cfg(feature = "benchmark")]
                        // The first 9 bytes of the serialized transaction are used as identification
                        // tags and are not part of the actual transaction.
                        let bytes = &serialized_tx[9..];
                        #[cfg(not(feature = "benchmark"))]
                        let bytes = &serialized_tx;

                        let transaction: Transaction = match bincode::deserialize(bytes) {
                            Ok(x) => x,
                            Err(e) => {
                                #[cfg(feature = "benchmark")]
                                panic!("Failed to deserialize transaction: {e}");

                                #[cfg(not(feature = "benchmark"))]
                                {
                                    log::warn!("Failed to deserialize transaction: {e}");
                                    continue;
                                }
                            }
                        };

                        // Execute the transaction.
                        if transaction.execution_time != 0 {
                            let duration = Duration::from_millis(transaction.execution_time);
                            Self::burn_cpu(duration).await;
                        }

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
