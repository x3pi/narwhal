// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, BatchMaker, Transaction};
use crate::helper::Helper;
use crate::primary_connector::PrimaryConnector;
use crate::processor::{Processor, SerializedBatchMessage};
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use log::{error, info, warn};
// SỬA ĐỔI: Import QuicTransport thay vì TcpTransport
use network::{
    quic::QuicTransport, // <--- THAY ĐỔI
    transport::Transport,
    MessageHandler, Receiver, Writer,
};
use primary::PrimaryWorkerMessage;
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

pub const CHANNEL_CAPACITY: usize = 1_000;
pub type Round = u64;
pub type SerializedBatchDigestMessage = Vec<u8>;

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

        // SỬA ĐỔI: Tạo một đối tượng transport QUIC.
        let transport = QuicTransport::new(); // <--- THAY ĐỔI

        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);

        // Khởi chạy các handler với transport mới.
        worker.handle_primary_messages(&transport).await;
        worker.handle_clients_transactions(&transport, tx_primary.clone()).await;
        worker.handle_workers_messages(&transport, tx_primary).await;

        PrimaryConnector::spawn(
            worker
                .committee
                .primary(&worker.name)
                .expect("Our public key is not in the committee")
                .worker_to_primary,
            rx_primary,
        );

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

    // SỬA ĐỔI: Hàm nhận vào QuicTransport.
    async fn handle_primary_messages(&self, transport: &QuicTransport) { // <--- THAY ĐỔI
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

        Receiver::spawn(
            listener,
            PrimaryReceiverHandler { tx_synchronizer },
        );

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

    // SỬA ĐỔI: Hàm nhận vào QuicTransport.
    async fn handle_clients_transactions(
        &self,
        transport: &QuicTransport, // <--- THAY ĐỔI
        tx_primary: Sender<SerializedBatchDigestMessage>,
    ) {
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
        Receiver::spawn(
            listener,
            TxReceiverHandler { tx_batch_maker },
        );

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

        Processor::spawn(
            self.id,
            self.store.clone(),
            rx_processor,
            tx_primary,
            true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    // SỬA ĐỔI: Hàm nhận vào QuicTransport.
    async fn handle_workers_messages(
        &self,
        transport: &QuicTransport, // <--- THAY ĐỔI
        tx_primary: Sender<SerializedBatchDigestMessage>,
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

        Processor::spawn(
            self.id,
            self.store.clone(),
            rx_processor,
            tx_primary,
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
        self.tx_batch_maker
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");
        tokio::task::yield_now().await;
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
            Ok(WorkerMessage::Batch(..)) => self
                .tx_processor
                .send(serialized.to_vec())
                .await
                .expect("Failed to send batch"),
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
            Ok(message) => self
                .tx_synchronizer
                .send(message)
                .await
                .expect("Failed to send transaction"),
        }
        Ok(())
    }
}