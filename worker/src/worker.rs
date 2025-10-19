// In worker/src/worker.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, BatchMaker, Transaction};
use crate::helper::Helper;
use crate::processor::{Processor, SerializedBatchMessage};
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use log::{error, info, warn};
use network::{
    quic::QuicTransport, transport::Transport, MessageHandler, Receiver, SimpleSender, Writer,
};
use primary::PrimaryWorkerMessage;
use serde::{Deserialize, Serialize};
use sha3::{Digest as Sha3Digest, Sha3_512 as Sha512};
use std::convert::TryInto;
use std::error::Error;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::RwLock;

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
    committee: Arc<RwLock<Committee>>,
    parameters: Parameters,
    store: Store,
}

impl Worker {
    pub async fn spawn(
        name: PublicKey,
        id: WorkerId,
        initial_committee: Committee,
        parameters: Parameters,
        store: Store,
    ) {
        let committee = Arc::new(RwLock::new(initial_committee));
        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
        };

        let transport = QuicTransport::new();

        worker.handle_primary_messages(&transport).await;
        worker.handle_clients_transactions(&transport).await;
        worker.handle_workers_messages(&transport).await;

        let committee_reader = worker.committee.read().await;
        let worker_info = committee_reader
            .worker(&worker.name, &worker.id)
            .expect("Our public key or worker id is not in the committee");

        info!(
            "Worker {} successfully booted on {}",
            id,
            worker_info.transactions.ip()
        );
    }

    async fn handle_primary_messages(&self, transport: &QuicTransport) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);

        let mut address = self
            .committee
            .read()
            .await
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
            PrimaryReceiverHandler {
                tx_synchronizer,
                committee: self.committee.clone(),
            },
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

    async fn handle_clients_transactions(&self, transport: &QuicTransport) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        let committee = self.committee.read().await;
        let mut address = committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());

        let listener = transport
            .listen(address)
            .await
            .expect("Failed to create transaction listener");
        Receiver::spawn(listener, TxReceiverHandler { tx_batch_maker });

        let primary_address = committee
            .primary(&self.name)
            .expect("Our public key is not in the committee")
            .worker_to_primary;

        let others_workers = committee
            .others_workers(&self.name, &self.id)
            .iter()
            .map(|(name, addresses)| (*name, addresses.worker_to_worker))
            .collect();

        // SỬA LỖI: Lấy stake của chính worker này
        let stake = committee.stake(&self.name);

        // SỬA LỖI: Clone committee để truyền vào QuorumWaiter
        let committee_clone_for_quorum = committee.clone();
        drop(committee); // Release lock

        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            rx_batch_maker,
            tx_quorum_waiter,
            others_workers,
        );

        // SỬA LỖI: Truyền đủ 4 tham số
        QuorumWaiter::spawn(
            committee_clone_for_quorum,
            stake,
            rx_quorum_waiter,
            tx_processor,
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

    async fn handle_workers_messages(&self, transport: &QuicTransport) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        let committee = self.committee.read().await;
        let mut address = committee
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

        // SỬA LỖI: Clone committee để truyền vào Helper
        let committee_clone_for_helper = committee.clone();

        let primary_address = committee
            .primary(&self.name)
            .expect("Our public key is not in the committee")
            .worker_to_primary;
        drop(committee); // Release lock

        // SỬA LỖI: Truyền committee đã clone
        Helper::spawn(
            self.id,
            committee_clone_for_helper,
            self.store.clone(),
            rx_helper,
        );

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

#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        let payload = &message;
        info!(target: "payload_logger", "{}", hex::encode(payload));
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
                let message = WorkerMessage::Batch(batch);
                let serialized =
                    bincode::serialize(&message).expect("Failed to serialize our own batch");
                let digest = Digest(Sha512::digest(&serialized)[..32].try_into().unwrap());
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
    committee: Arc<RwLock<Committee>>,
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
                if let PrimaryWorkerMessage::Reconfigure(new_committee) = &message {
                    info!("Worker received reconfigure signal from primary.");
                    let mut committee_lock = self.committee.write().await;
                    *committee_lock = new_committee.clone();
                    info!("Worker committee updated.");
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
