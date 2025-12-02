// In worker/src/processor.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use config::WorkerId;
use crypto::Digest;
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use sha3::Digest as Sha3Digest;
use sha3::Sha3_512 as Sha512;
use std::net::SocketAddr;
use store::Store;
use tokio::sync::mpsc::Receiver;

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // SỬA ĐỔI: Thay thế channel bằng network sender và địa chỉ primary.
        mut network: SimpleSender,
        primary_address: SocketAddr,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                // Hash the batch.
                let hash = Sha512::digest(&batch);
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(&hash[..32]);
                let digest = Digest(bytes);

                // Store the batch.
                store.write(digest.to_vec(), batch.clone()).await;

                let digest_for_log = digest.clone();
                let batch_len = batch.len();
                
                // Create the message for the primary.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id, batch),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id, batch),
                };
                let serialized_message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                
                // Log batch được gửi đến primary (sử dụng hệ thống log mới)
                log_batch_gui_primary!(&format!("{}", digest_for_log), id, batch_len);
                
                // CRITICAL DEBUG: Log tất cả batches được gửi đến primary để trace
                tracing::info!(
                    target: "narwhal_audit",
                    "[BATCH TRACE] Worker {} sending batch {} to primary (size: {} bytes, own_digest: {}). Batch will be processed by primary and included in header.",
                    id,
                    digest_for_log,
                    batch_len,
                    own_digest
                );
                
                // SỬA ĐỔI: Gửi trực tiếp đến primary qua mạng.
                // CRITICAL: Log trước khi gửi batch đến primary (có thể là từ sync hoặc từ batch maker)
                tracing::info!(
                    target: "narwhal_audit",
                    "[BATCH SENT TO PRIMARY] Worker {} sending batch {} to primary at {} (size: {} bytes, own_digest: {}). Primary will include batch in header proposal. This batch may have been synced from another worker.",
                    id,
                    digest_for_log,
                    primary_address,
                    batch_len,
                    own_digest
                );
                
                // CRITICAL: Track thời gian gửi batch đến primary
                let send_start = std::time::Instant::now();
                network
                    .send(primary_address, Bytes::from(serialized_message))
                    .await;
                let send_duration = send_start.elapsed();
                
                // CRITICAL: Log sau khi gửi batch thành công với timing
                tracing::info!(
                    target: "narwhal_audit",
                    "[BATCH SENT TO PRIMARY SUCCESS] Worker {} successfully sent batch {} to primary at {} in {}ms. Primary should receive and process this batch. Batch is now available for consensus.",
                    id,
                    digest_for_log,
                    primary_address,
                    send_duration.as_millis()
                );
            }
        });
    }
}
