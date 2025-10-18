// In worker/src/processor.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use config::WorkerId;
use crypto::Digest;
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use serde::{Deserialize, Serialize};
use sha3::{Digest as Sha3Digest, Sha3_512 as Sha512};
use std::convert::TryInto;
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
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // A network sender to send the batches' digests to the primary.
        mut network: SimpleSender,
        primary_address: SocketAddr,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                // --- TỐI ƯU HÓA: Offload tác vụ hashing ---
                // Hashing một batch lớn có thể tốn nhiều CPU và chặn event loop.
                // Chúng ta chuyển nó sang một luồng chặn chuyên dụng của Tokio.
                let batch_clone_for_hashing = batch.clone();
                let digest = tokio::task::spawn_blocking(move || {
                    Digest(
                        Sha512::digest(&batch_clone_for_hashing).as_slice()[..32]
                            .try_into()
                            .unwrap(),
                    )
                })
                .await
                .expect("Hashing task panicked");
                // --- KẾT THÚC TỐI ƯU HÓA ---

                // Store the batch.
                store.write(digest.to_vec(), batch.clone()).await;

                // Create the message for the primary.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id, batch),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id, batch),
                };
                let serialized_message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");

                // Send the digest to the primary.
                network
                    .send(primary_address, Bytes::from(serialized_message))
                    .await;
            }
        });
    }
}
