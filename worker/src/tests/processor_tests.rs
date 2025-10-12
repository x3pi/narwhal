// In worker/src/tests/processor_tests.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{listener, serialized_batch};
use bytes::Bytes;
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn hash_and_store() {
    let (tx_batch, rx_batch) = channel(1);

    // Tạo một test store mới.
    let path = ".db_test_hash_and_store";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Dữ liệu batch để gửi.
    let batch_data = serialized_batch();
    let id = 0;

    // Tính toán digest và tạo message kỳ vọng.
    let digest = Digest(
        Sha512::digest(&batch_data).as_slice()[..32]
            .try_into()
            .unwrap(),
    );
    let expected_message = WorkerPrimaryMessage::OurBatch(digest.clone(), id, batch_data.clone());
    let expected_serialized = bincode::serialize(&expected_message)
        .expect("Failed to serialize our own worker-primary message");

    // Spawn một listener giả lập Primary để nhận tin nhắn từ Processor.
    let primary_address = "127.0.0.1:12345".parse().unwrap();
    let handle = listener(primary_address, Some(Bytes::from(expected_serialized)));

    // Spawn một Processor mới, trỏ đến địa chỉ của listener.
    Processor::spawn(
        id,
        store.clone(),
        rx_batch,
        SimpleSender::new(),
        primary_address,
        /* own_batch */ true,
    );

    // Gửi một batch đến Processor.
    tx_batch.send(batch_data.clone()).await.unwrap();

    // Đảm bảo listener (Primary) nhận được đúng tin nhắn.
    assert!(handle.await.is_ok());

    // Đảm bảo Processor đã lưu trữ batch một cách chính xác.
    let stored_batch = store.read(digest.to_vec()).await.unwrap();
    assert!(stored_batch.is_some(), "The batch is not in the store");
    assert_eq!(stored_batch.unwrap(), batch_data);
}