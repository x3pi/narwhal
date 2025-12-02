// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use log::{error, info, warn};
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::Receiver;
use tracing;

#[cfg(test)]
#[path = "tests/helper_tests.rs"]
pub mod helper_tests;

/// A task dedicated to help other authorities by replying to their batch requests.
pub struct Helper {
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive batch requests.
    rx_request: Receiver<(Vec<Digest>, PublicKey)>,
    /// A network sender to send the batches to the other workers.
    network: SimpleSender,
}

impl Helper {
    pub fn spawn(
        id: WorkerId,
        committee: Committee,
        store: Store,
        rx_request: Receiver<(Vec<Digest>, PublicKey)>,
    ) {
        tokio::spawn(async move {
            Self {
                id,
                committee,
                store,
                rx_request,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digests, origin)) = self.rx_request.recv().await {
            // TODO [issue #7]: Do some accounting to prevent bad nodes from monopolizing our resources.

            // CRITICAL: Log khi worker helper nhận batch request
            tracing::info!(
                target: "narwhal_audit",
                "[WORKER HELPER REQUEST RECEIVED] Worker {} helper received batch request for {} batches from requester {:?}. Worker will check store and reply.",
                self.id, digests.len(), origin
            );

            // get the requestors address.
            let address = match self.committee.worker(&origin, &self.id) {
                Ok(x) => x.worker_to_worker,
                Err(e) => {
                    tracing::warn!(
                        target: "narwhal_audit",
                        "[WORKER HELPER ERROR] Worker {} helper cannot resolve address for requester {:?}: {}. Request will be skipped.",
                        self.id, origin, e
                    );
                    warn!("Unexpected batch request: {}", e);
                    continue;
                }
            };

            // Reply to the request (the best we can).
            info!(
                "[WORKER HELPER] Worker {} replying to {} batch digests requested by {}",
                self.id,
                digests.len(),
                origin
            );
            
            // CRITICAL: Log danh sách batches được request để debug
            let batch_list: Vec<String> = digests.iter().take(10).map(|d| format!("{}", d)).collect();
            tracing::info!(
                target: "narwhal_audit",
                "[WORKER HELPER PROCESSING] Worker {} helper processing batch request for {} batches from requester {:?} at {}. Batches: {:?}",
                self.id, digests.len(), origin, address, batch_list
            );
            
            let mut found_count = 0;
            let mut missing_count = 0;
            let total_count = digests.len();
            let processing_start = std::time::Instant::now();
            
            for digest in &digests {
                let read_start = std::time::Instant::now();
                match self.store.read(digest.to_vec()).await {
                    Ok(Some(data)) => {
                        let read_duration = read_start.elapsed();
                        // CRITICAL: Log khi worker helper gửi batch về requester với timing
                        tracing::info!(
                            target: "narwhal_audit",
                            "[WORKER HELPER SENDING BATCH] Worker {} helper sending batch {} ({} bytes) to requester {:?} at {} (read from store took {}ms). Batch will be delivered to requester worker.",
                            self.id, digest, data.len(), origin, address, read_duration.as_millis()
                        );
                        
                        info!(
                            "[WORKER HELPER] Worker {} sending batch {} ({} bytes) to requester {}",
                            self.id,
                            digest,
                            data.len(),
                            origin
                        );
                        found_count += 1;
                        let send_start = std::time::Instant::now();
                        self.network.send(address, Bytes::from(data)).await;
                        let send_duration = send_start.elapsed();
                        
                        // CRITICAL: Log sau khi gửi batch thành công với timing
                        if send_duration.as_millis() > 10 {
                            tracing::warn!(
                                target: "narwhal_audit",
                                "[WORKER HELPER SEND SLOW] Worker {} helper sent batch {} to requester {:?} at {} in {}ms - network may be slow!",
                                self.id, digest, origin, address, send_duration.as_millis()
                            );
                        } else {
                            tracing::debug!(
                                target: "narwhal_audit",
                                "[WORKER HELPER SEND SUCCESS] Worker {} helper successfully sent batch {} to requester {:?} at {} in {}ms.",
                                self.id, digest, origin, address, send_duration.as_millis()
                            );
                        }
                    }
                    Ok(None) => {
                        let read_duration = read_start.elapsed();
                        // CRITICAL: Log khi worker helper không có batch với timing
                        tracing::warn!(
                            target: "narwhal_audit",
                            "[WORKER HELPER MISSING BATCH] Worker {} helper missing batch {} requested by {:?} (read from store took {}ms). Requester worker will need to request from another worker.",
                            self.id, digest, origin, read_duration.as_millis()
                        );
                        
                        missing_count += 1;
                        info!(
                            "[WORKER HELPER] Worker {} missing batch {} requested by {}",
                            self.id, digest, origin
                        )
                    },
                    Err(e) => {
                        let read_duration = read_start.elapsed();
                        tracing::error!(
                            target: "narwhal_audit",
                            "[WORKER HELPER ERROR] Worker {} helper error reading batch {} from store for requester {:?} (read took {}ms): {}. Requester worker will not receive this batch.",
                            self.id, digest, origin, read_duration.as_millis(), e
                        );
                        error!("{}", e)
                    },
                }
            }
            
            // CRITICAL: Log tổng kết khi worker helper hoàn thành request với timing
            let processing_duration = processing_start.elapsed();
            tracing::info!(
                target: "narwhal_audit",
                "[WORKER HELPER REQUEST COMPLETE] Worker {} helper completed batch request from requester {:?} in {}ms. Found: {}, Missing: {}, Total: {}. Batches were sent to {}.",
                self.id, origin, processing_duration.as_millis(), found_count, missing_count, total_count, address
            );
        }
    }
}
