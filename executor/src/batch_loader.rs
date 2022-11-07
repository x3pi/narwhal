use std::collections::HashMap;

use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use ed25519_dalek::{Digest as _, Sha512};
use futures::{stream::FuturesOrdered, StreamExt};
use log::debug;
use network::SimpleSender;
use primary::Certificate;
use store::{Store, StoreError};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::{sleep, Duration, Instant},
};
use worker::WorkerMessage;

use crate::core::CoreMessage;

/// The resolution of the timer that checks whether we received replies to our batch requests,
/// and triggers new batch requests if we didn't.
const TIMER_RESOLUTION: u64 = 500; // ms

/// Represents a serialized batch.
pub type SerializedBatchMessage = Vec<u8>;

/// Receives certificates from consensus and downloads batches from workers.
pub struct BatchLoader {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent store to temporarily keep downloaded batches.
    store: Store,
    /// Input channel to receive certificates from consensus.
    rx_consensus: Receiver<Certificate>,
    /// Input channel to receive batches from workers.
    rx_worker: Receiver<SerializedBatchMessage>,
    /// Output channel to notify the core that a certificate is ready for execution.
    tx_core: Sender<CoreMessage>,
    /// The delay to wait before re-trying sync requests.
    sync_retry_delay: u64,
    /// A simply network sender to request batches from workers.
    network: SimpleSender,
    /// Keeps all batch digests for which we are waiting the corresponding batch. This
    /// map is used to re-try batch requests upon a timer's expiration.
    pending: HashMap<Digest, (WorkerId, Instant)>,
}

impl BatchLoader {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        rx_consensus: Receiver<Certificate>,
        rx_worker: Receiver<SerializedBatchMessage>,
        tx_core: Sender<CoreMessage>,
        sync_retry_delay: u64,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                rx_consensus,
                rx_worker,
                tx_core,
                sync_retry_delay,
                network: SimpleSender::new(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for particular data to become available in the storage
    /// and then delivers the specified certificate.
    async fn waiter(
        digest: Digest,
        mut store: Store,
        deliver: Certificate,
    ) -> Result<CoreMessage, StoreError> {
        store
            .notify_read(digest.to_vec())
            .await
            .map(|batch| CoreMessage {
                batch,
                digest,
                certificate: deliver,
            })
    }

    async fn send_batch_requests(&mut self, requests: HashMap<WorkerId, Vec<Digest>>) {
        for (worker, digests) in requests {
            for digest in &digests {
                debug!("Requesting batch {digest} from worker {worker}");
            }

            let address = self
                .committee
                .worker(&self.name, &worker)
                .expect(&format!("Our worker {} is not in the committee", worker))
                .worker_to_worker;
            let message = WorkerMessage::ExecutorRequest(digests, self.name);
            let serialized =
                bincode::serialize(&message).expect("(Failed to serialize executor message");
            self.network.send(address, Bytes::from(serialized)).await;
        }
    }

    /// Main loop receiving new sequenced certificates and ensuring their batches are downloaded.
    async fn run(&mut self) -> ! {
        let mut waiting = FuturesOrdered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Receive sequenced certificates from consensus.
                Some(certificate) = self.rx_consensus.recv() => {
                    let now = Instant::now();

                    let mut requests = HashMap::new();
                    for (digest, worker) in certificate.header.payload.clone() {
                        // Register a future to be notified when we receive all batches.
                        let fut = Self::waiter(
                            digest.clone(),
                            self.store.clone(),
                            certificate.clone()
                        );
                        waiting.push_back(fut);

                        // Add the digest to the list of pending batches.
                        self.pending.insert(digest.clone(), (worker, now));

                        // Construct the network request for our workers.
                        requests.entry(worker).or_insert_with(Vec::new).push(digest);
                    }

                    // Request the batches referenced by this certificate.
                    self.send_batch_requests(requests).await;
                },

                // Notification that a batch has been downloaded from the workers.
                Some(result) = waiting.next() => {
                    let core_message = result
                        .expect("Failed to read digests from store");
                    self
                        .tx_core
                        .send(core_message)
                        .await
                        .expect("Failed to send batches to executor core");
                }

                // Receive batches from workers.
                Some(batch) = self.rx_worker.recv() => {
                    // Hash and store the batch.
                    let digest = Digest(Sha512::digest(&batch)
                        .as_slice()[..32]
                        .try_into()
                        .unwrap());
                    self.store.write(digest.to_vec(), batch).await;

                    debug!("Received batch {digest}");

                    // Clear the pending map.
                    let _ = self.pending.remove(&digest);
                }

                // Timer ensuring we re-try to ask for batches until we get them.
                () = &mut timer => {
                    // Gather all headers that have not been received for a while.
                    let mut requests = HashMap::new();
                    for (digest, (worker, timestamp)) in &self.pending {
                        if timestamp.elapsed().as_millis() as u64 > self.sync_retry_delay {
                            debug!("Timer triggered for batch {digest} (worker {worker})");
                            requests.entry(*worker).or_insert_with(Vec::new).push(digest.clone());
                        }
                    }

                    // Resend the batch requests.
                    self.send_batch_requests(requests).await;

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }
        }
    }
}
