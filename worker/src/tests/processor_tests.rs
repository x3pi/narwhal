// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch, serialized_batch}; // Sửa đổi: sử dụng serialized_batch từ common
use crate::worker::WorkerMessage;
use primary::WorkerPrimaryMessage; // Thêm import
use std::convert::TryInto;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn hash_and_store() {
    let (tx_batch, rx_batch) = channel(1);
    let (tx_digest, mut rx_digest) = channel(1);

    // Create a new test store.
    let path = ".db_test_hash_and_store";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Spawn a new `Processor` instance.
    let id = 0;
    Processor::spawn(
        id,
        store.clone(),
        rx_batch,
        tx_digest,
        /* own_batch */ true,
    );

    // Send a batch to the `Processor`.
    // Lưu ý: Processor bây giờ nhận vào batch đã được serialize, không phải message.
    let batch_data = serialized_batch();
    tx_batch.send(batch_data.clone()).await.unwrap();

    // Ensure the `Processor` outputs the batch's digest along with the batch data.
    let output = rx_digest.recv().await.unwrap();

    // Tính toán digest và tạo message kỳ vọng
    let digest = Digest(
        Sha512::digest(&batch_data).as_slice()[..32]
            .try_into()
            .unwrap(),
    );
    let expected_message = WorkerPrimaryMessage::OurBatch(digest.clone(), id, batch_data.clone());
    let expected_serialized =
        bincode::serialize(&expected_message).expect("Failed to serialize our own worker-primary message");

    assert_eq!(output, expected_serialized);

    // Ensure the `Processor` correctly stored the batch.
    let stored_batch = store.read(digest.to_vec()).await.unwrap();
    assert!(stored_batch.is_some(), "The batch is not in the store");
    assert_eq!(stored_batch.unwrap(), batch_data);
}