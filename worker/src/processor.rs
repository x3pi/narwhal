// In worker/src/processor.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use config::WorkerId;
use crypto::Digest;
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use sha3::{Digest as Sha3Digest, Sha3_512 as Sha512};
use std::convert::TryInto;
use std::net::SocketAddr;
use store::Store;
use tokio::sync::mpsc::Receiver;

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

pub type SerializedBatchMessage = Vec<u8>;

pub struct Processor;

impl Processor {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        id: WorkerId,
        mut store: Store,
        mut rx_batch: Receiver<SerializedBatchMessage>,
        mut network: SimpleSender,
        primary_address: SocketAddr,
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                let batch_clone_for_hashing = batch.clone();
                let digest = tokio::task::spawn_blocking(move || {
                    // SỬA LỖI: Sử dụng cách truy cập slice hiện đại hơn
                    Digest(
                        (&Sha512::digest(&batch_clone_for_hashing)[..32])
                            .try_into()
                            .unwrap(),
                    )
                })
                .await
                .expect("Hashing task panicked");

                store.write(digest.to_vec(), batch.clone()).await;

                // DEBUG: Log địa chỉ trước khi gửi batch đến primary
                if primary_address.ip().to_string() == "0.0.0.0" || primary_address.port() == 0 {
                    ::log::warn!("[Processor] ⚠️ SENDING BATCH TO INVALID PRIMARY ADDRESS: {}", primary_address);
                }

                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id, batch),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id, batch),
                };
                let serialized_message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");

                network
                    .send(primary_address, Bytes::from(serialized_message))
                    .await;
            }
        });
    }
}
