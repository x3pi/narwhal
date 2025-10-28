// In worker/src/helper.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use log::{error, warn};
use network::SimpleSender;
use std::sync::Arc; // <-- THÊM Arc
use store::Store;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock; // <-- THÊM RwLock

#[cfg(test)]
#[path = "tests/helper_tests.rs"]
pub mod helper_tests;

/// A task dedicated to help other authorities by replying to their batch requests.
pub struct Helper {
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    // *** THAY ĐỔI: Sử dụng Arc<RwLock<Committee>> ***
    committee: Arc<RwLock<Committee>>,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive batch requests.
    rx_request: Receiver<(Vec<Digest>, PublicKey)>,
    /// A network sender to send the batches to the other workers.
    network: SimpleSender,
}

impl Helper {
    // *** THAY ĐỔI: Cập nhật chữ ký hàm ***
    pub fn spawn(
        id: WorkerId,
        committee: Arc<RwLock<Committee>>,
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

            // get the requestors address.
            // *** THAY ĐỔI: Lock committee để đọc ***
            let committee = self.committee.read().await;
            let address = match committee.worker(&origin, &self.id) {
                Ok(x) => x.worker_to_worker,
                Err(e) => {
                    warn!("Unexpected batch request: {}", e);
                    // Drop lock trước khi continue
                    drop(committee);
                    continue;
                }
            };
            // Drop lock sớm
            drop(committee);

            // Reply to the request (the best we can).
            for digest in digests {
                match self.store.read(digest.to_vec()).await {
                    Ok(Some(data)) => {
                        // *** SỬA LỖI: Chỉ gửi nếu dữ liệu không rỗng ***
                        if !data.is_empty() {
                            // DEBUG: Log địa chỉ trước khi gửi batch data
                            if address.ip().to_string() == "0.0.0.0" || address.port() == 0 {
                                warn!("[Worker::Helper] ⚠️ SENDING BATCH DATA TO INVALID ADDRESS: {} (digest: {})", address, digest);
                            }

                            // *** SỬA LỖI: Clone `data` để sử dụng lại ***
                            // Gửi data gốc đã đọc từ store
                            self.network.send(address, Bytes::from(data.clone())).await;
                        // <-- Clone data here
                        } else {
                            warn!("Skipping sending empty batch data for digest {}", digest);
                        }
                    }
                    Ok(None) => (), // Batch not found, do nothing.
                    Err(e) => error!("Failed to read batch {} from store: {}", digest, e),
                }
            }
        }
    }
}
// *** THAY ĐỔI: Import WorkerMessage để sử dụng trong run (nếu cần serialize) ***
// (Hiện tại không cần vì gửi Bytes thô, nhưng để lại phòng trường hợp thay đổi logic)
// use crate::worker::WorkerMessage;
