// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, BatchMaker, Transaction};
use crate::helper::Helper;
// use crate::primary_connector::PrimaryConnector; // <--- XÓA DÒNG NÀY
use crate::processor::{Processor, SerializedBatchMessage};
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use log::{error, info, warn};
use network::{
    quic::QuicTransport,
    transport::Transport,
    MessageHandler,
    Receiver,
    SimpleSender, // Thêm SimpleSender vào import
    Writer,
};
use primary::PrimaryWorkerMessage;
use serde::{Deserialize, Serialize};
use sha3::{Digest as Sha3Digest, Sha3_512 as Sha512};
use std::convert::TryInto;
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Sender}; // <--- THÊM DÒNG NÀY

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

pub const CHANNEL_CAPACITY: usize = 1_000;
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

pub struct Worker {
    name: PublicKey,
    id: WorkerId,
    committee: Committee,
    parameters: Parameters,
    store: Store,
}

impl Worker {
    pub async fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        parameters: Parameters,
        store: Store,
    ) {
        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
        };

        let transport = QuicTransport::new();

        // SỬA ĐỔI: Không cần channel đi đến PrimaryConnector nữa.
        // let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);

        worker.handle_primary_messages(&transport).await;
        // worker.handle_clients_transactions(&transport, tx_primary.clone()).await;
        // worker.handle_workers_messages(&transport, tx_primary).await;
        worker.handle_clients_transactions(&transport).await;
        worker.handle_workers_messages(&transport).await;

        // SỬA ĐỔI: Xóa bỏ PrimaryConnector.
        // PrimaryConnector::spawn(
        //     worker
        //         .committee
        //         .primary(&worker.name)
        //         .expect("Our public key is not in the committee")
        //         .worker_to_primary,
        //     rx_primary,
        // );

        info!(
            "Worker {} successfully booted on {}",
            id,
            worker
                .committee
                .worker(&worker.name, &worker.id)
                .expect("Our public key or worker id is not in the committee")
                .transactions
                .ip()
        );
    }

    async fn handle_primary_messages(&self, transport: &QuicTransport) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());

        let listener = transport
            .listen(address)
            .await
            .expect("Failed to create primary message listener");

        Receiver::spawn(listener, PrimaryReceiverHandler { tx_synchronizer });

        Synchronizer::spawn(
            self.name,
            self.id,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            rx_synchronizer,
        );

        info!(
            "Worker {} listening to primary messages on {}",
            self.id, address
        );
    }

    async fn handle_clients_transactions(
        &self,
        transport: &QuicTransport,
        // tx_primary: Sender<SerializedBatchDigestMessage>, // <--- XÓA PARAMETER NÀY
    ) {
        log::info!("Worker: Processor will send to primary_address: {:?}", 222);

        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());

        let listener = transport
            .listen(address)
            .await
            .expect("Failed to create transaction listener");
        Receiver::spawn(listener, TxReceiverHandler { tx_batch_maker });

        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            rx_batch_maker,
            tx_quorum_waiter,
            self.committee
                .others_workers(&self.name, &self.id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
        );

        QuorumWaiter::spawn(
            self.committee.clone(),
            self.committee.stake(&self.name),
            rx_quorum_waiter,
            tx_processor,
        );
        // SỬA ĐỔI: Khởi tạo Processor với SimpleSender.
        let primary_address = self
            .committee
            .primary(&self.name)
            .expect("Our public key is not in the committee")
            .worker_to_primary;
        log::info!(
            "Worker: Processor will send to primary_address: {:?}",
            primary_address
        );
        Processor::spawn(
            self.id,
            self.store.clone(),
            rx_processor,
            SimpleSender::new(),
            primary_address,
            true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    async fn handle_workers_messages(
        &self,
        transport: &QuicTransport,
        // tx_primary: Sender<SerializedBatchDigestMessage>, // <--- XÓA PARAMETER NÀY
    ) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());

        let listener = transport
            .listen(address)
            .await
            .expect("Failed to create worker message listener");
        Receiver::spawn(
            listener,
            WorkerReceiverHandler {
                tx_helper,
                tx_processor,
            },
        );

        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            rx_helper,
        );

        // SỬA ĐỔI: Khởi tạo Processor với SimpleSender.
        let primary_address = self
            .committee
            .primary(&self.name)
            .expect("Our public key is not in the committee")
            .worker_to_primary;
        Processor::spawn(
            self.id,
            self.store.clone(),
            rx_processor,
            SimpleSender::new(),
            primary_address,
            false,
        );

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
    }
}

// --- Các struct Handler (không thay đổi) ---

#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Kiểm tra xem message có đủ 8 byte để cắt không.
        if message.len() < 8 {
            warn!(
                "Giao dịch nhận được quá ngắn ({_len} bytes), không chứa đủ 8 byte độ dài. Bỏ qua.",
                _len = message.len()
            );
            return Ok(());
        }

        // Cắt lấy phần payload: bắt đầu từ byte thứ 8 cho đến hết.
        let payload = &message[8..];
        info!(
            target: "payload_logger",
            "{}",
            hex::encode(payload)
        );
        // Gửi phần payload đã được cắt vào quá trình đồng thuận.
        self.tx_batch_maker
            .send(payload.to_vec())
            .await
            .expect("Failed to send transaction payload");

        Ok(())
    }
}

#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;
        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(batch)) => {
                // -- BỔ SUNG LOG TẠI ĐÂY --
                let message = WorkerMessage::Batch(batch);
                let serialized =
                    bincode::serialize(&message).expect("Failed to serialize our own batch");

                let digest = Digest(Sha512::digest(&serialized)[..32].try_into().unwrap());
                // -- KẾT THÚC BỔ SUNG --
                info!("WorkerMessage Batch {:?} ", digest);

                self.tx_processor
                    .send(serialized.to_vec())
                    .await
                    .expect("Failed to send batch")
            }
            Ok(WorkerMessage::BatchRequest(missing, requestor)) => self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send batch request"),
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize(&serialized) {
            Err(e) => error!("Failed to deserialize primary message: {}", e),
            Ok(message) => {
                if let PrimaryWorkerMessage::Reconfigure = message {
                    info!("Received reconfigure signal from primary.");
                    // The synchronizer and other worker components should handle this message
                    // to reload the committee and update their configurations.
                }
                self.tx_synchronizer
                    .send(message)
                    .await
                    .expect("Failed to send message to synchronizer")
            }
        }
        Ok(())
    }
}
