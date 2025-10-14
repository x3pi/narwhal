// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{
    batch_digest, committee_with_base_port, keys, listener, serialized_batch, transaction,
};
use bytes::Bytes;
use log::info;
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use std::fs;

#[tokio::test]

async fn handle_clients_transactions() {
    println!("Starting test: handle_clients_transactions");

    // Khởi tạo logger để xem output.
    let _ = env_logger::builder().is_test(true).try_init();

    // Sửa lỗi: Destructure tuple trả về từ `keys()` cho đúng.
    let (name, _, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Tạo một test store mới.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    println!("Store created at '{}'", path);

    // Spawn một `Worker` instance.
    println!("Spawning worker instance for node '{}', worker id {}", name, id);
    Worker::spawn(name, id, committee.clone(), parameters, store).await;
    println!("Worker instance spawned");

    // Spawn một network listener để giả lập Primary, nhận message từ worker.
    let primary_address = committee.primary(&name).unwrap().worker_to_primary;
    println!("Setting up primary listener at {}", primary_address);

    let batch_content = serialized_batch();
    let expected_message = WorkerPrimaryMessage::OurBatch(batch_digest(), id, batch_content.clone());
    let expected_serialized = bincode::serialize(&expected_message).unwrap();
    let primary_handle = listener(primary_address, Some(Bytes::from(expected_serialized)));
    println!("Primary listener is ready");

    // Spawn các listener cho các worker khác để nhận batch được broadcast.
    println!("Spawning other worker listeners...");
    let mut worker_handles = Vec::new();
    let expected_batch_broadcast = Bytes::from(batch_content);
    for (i, (worker_name, addresses)) in committee.others_workers(&name, &id).iter().enumerate() {
        let address = addresses.worker_to_worker;
        println!(
            "Spawning listener for other worker {} ('{}') at {}",
            i, worker_name, address
        );
        let handle = listener(address, Some(expected_batch_broadcast.clone()));
        worker_handles.push(handle);
    }
    println!("All other worker listeners are ready");

    // Gửi đủ transaction để tạo một batch.
    let mut network = SimpleSender::new();
    let address = committee.worker(&name, &id).unwrap().transactions;
    println!("Sending transactions to worker at {}", address);
    network.send(address, Bytes::from(transaction())).await;
    network.send(address, Bytes::from(transaction())).await;
    println!("All transactions sent");

    // Đảm bảo Primary nhận được message.
    println!("Waiting for primary to receive the batch message...");
    assert!(primary_handle.await.is_ok(), "Primary listener failed");
    println!("Primary received message successfully.");

    // Đảm bảo các worker khác cũng nhận được batch.
    println!("Waiting for other workers to receive the broadcasted batch...");
    for (i, handle) in worker_handles.into_iter().enumerate() {
        assert!(
            handle.await.is_ok(),
            "Listener for other worker {} failed",
            i
        );
    }
    println!("Other workers received batch successfully.");

    println!("Test handle_clients_transactions finished successfully");
}
