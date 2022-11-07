use std::time::Duration;

use crate::{batch_loader::SerializedBatchMessage, transaction::Transaction};
use log::warn;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::timeout,
};
use worker::WorkerMessage;

/// Executes batches of transactions.
pub struct Core {
    /// Input channel to receive (serialized) batches to execute.
    rx_batch_loader: Receiver<SerializedBatchMessage>,
    /// Output channel to send transactions to execute to the runner.
    tx_core_runner: Sender<Transaction>,
}

impl Core {
    pub fn spawn(rx_batch_loader: Receiver<SerializedBatchMessage>) {
        let (tx_core_runner, rx_core_runner) = channel(100);
        CoreRunner::spawn(rx_core_runner);
        tokio::spawn(async move {
            Self {
                rx_batch_loader,
                tx_core_runner,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving batches to execute.
    async fn run(&mut self) {
        while let Some(serialized) = self.rx_batch_loader.recv().await {
            match bincode::deserialize(&serialized) {
                Ok(WorkerMessage::Batch(batch)) => {
                    // Deserialize each transaction.
                    for serialized_tx in batch {
                        let transaction = match bincode::deserialize(&serialized_tx) {
                            Ok(x) => x,
                            Err(e) => {
                                #[cfg(feature = "benchmark")]
                                panic!("Failed to deserialize transaction: {e}");

                                warn!("Failed to deserialize transaction: {e}");
                                continue;
                            }
                        };

                        // Send it to the runner for execution.
                        self.tx_core_runner
                            .send(transaction)
                            .await
                            .expect("Failed to send transaction to core executor");
                    }
                }
                Ok(_) => panic!("Unexpected protocol message"),
                Err(e) => panic!("Failed to deserialize batch: {}", e),
            }
        }
    }
}

/// The runner executing the transaction.
struct CoreRunner {
    /// Input channel to transactions to execute.
    rx_core: Receiver<Transaction>,
}

impl CoreRunner {
    pub fn spawn(rx_core: Receiver<Transaction>) {
        tokio::spawn(async move {
            Self { rx_core }.run().await;
        });
    }

    /// Simple function simulating a CPU-intensive execution.
    async fn burn_cpu() {
        let mut _x = 0;
        loop {
            _x += 1;
            _x -= 1;
        }
    }

    /// Mail loop receiving transactions to execute.
    async fn run(&mut self) {
        while let Some(transaction) = self.rx_core.recv().await {
            // Execute the transaction.
            let duration = Duration::from_millis(transaction.contract);
            let _ = timeout(duration, Self::burn_cpu()).await;
        }
    }
}
