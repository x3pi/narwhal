// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{committee, keys};
use std::fs;
use store::Store;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn propose_empty() {
    // Lấy đúng khóa đồng thuận (consensus_secret) để khởi tạo SignatureService.
    let (name, _, _, consensus_secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(consensus_secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (_tx_our_digests, rx_our_digests) = channel::<(Digest, WorkerId, Vec<u8>)>(1);
    let (_tx_committed, rx_committed) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Create a new test store.
    let path = ".db_test_propose_empty";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        store, // Thêm store
        /* header_size */ 1_000,
        /* max_header_delay */ 20,
        /* sync_retry_delay */ 10,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* rx_committed */ rx_committed,
        /* tx_core */ tx_headers,
    );

    // Ensure the proposer makes a correct empty header.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert!(header.payload.is_empty());
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
async fn propose_payload() {
    // Lấy đúng khóa đồng thuận (consensus_secret) để khởi tạo SignatureService.
    let (name, _, _, consensus_secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(consensus_secret);

    let (_tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel::<(Digest, WorkerId, Vec<u8>)>(1);
    let (_tx_committed, rx_committed) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    // Create a new test store.
    let path = ".db_test_propose_payload";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        store, // Thêm store
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* sync_retry_delay */ 10,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* rx_committed */ rx_committed,
        /* tx_core */ tx_headers,
    );

    // Send enough digests for the header payload.
    // Sửa lỗi: Tạo một digest hợp lệ thay vì sử dụng sai kiểu dữ liệu.
    let digest = Digest::default();
    let worker_id = 0;
    let batch = Vec::new(); // Proposer giờ cần cả batch
    tx_our_digests
        .send((digest.clone(), worker_id, batch)) // Gửi cả batch
        .await
        .unwrap();

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
}
