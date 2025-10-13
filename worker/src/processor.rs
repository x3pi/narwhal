// In worker/src/processor.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use config::WorkerId;
use crypto::Digest;
use sha2::{Digest as Sha2DigestTrait, Sha512};
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
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
                let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());

                // Store the batch.
                store.write(digest.to_vec(), batch.clone()).await;

                // Create the message for the primary.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id, batch),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id, batch),
                };
                let serialized_message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                log::info!("Processor: Sending batch to primary at {:?}", primary_address);
                // SỬA ĐỔI: Gửi trực tiếp đến primary qua mạng.
                network
                    .send(primary_address, Bytes::from(serialized_message))
                    .await;
            }
        });
    }
}