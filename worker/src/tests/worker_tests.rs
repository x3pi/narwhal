// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{
    batch, batch_digest, committee_with_base_port, keys, listener, serialized_batch, transaction,
}; // Thêm import
use bytes::Bytes; // Thêm import
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use std::fs;

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Create a new test store.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Worker` instance.
    Worker::spawn(name, id, committee.clone(), parameters, store).await; // THÊM .await

    // Spawn a network listener to receive our batch's digest.
    let primary_address = committee.primary(&name).unwrap().worker_to_primary;

    // Listener bây giờ sẽ mong đợi nhận được message có cả nội dung batch.
    let batch_content = serialized_batch();
    let expected_message =
        WorkerPrimaryMessage::OurBatch(batch_digest(), id, batch_content.clone());
    let expected_serialized = bincode::serialize(&expected_message).unwrap();
    let handle = listener(primary_address, Some(Bytes::from(expected_serialized)));

    // Spawn enough workers' listeners to acknowledge our batches.
    for (_, addresses) in committee.others_workers(&name, &id) {
        let address = addresses.worker_to_worker;
        let _ = listener(address, /* expected */ None);
    }

    // Send enough transactions to create a batch.
    let mut network = SimpleSender::new();
    let address = committee.worker(&name, &id).unwrap().transactions;
    network.send(address, Bytes::from(transaction())).await;
    network.send(address, Bytes::from(transaction())).await;

    // Ensure the primary received the batch's digest (ie. it did not panic).
    assert!(handle.await.is_ok());
}