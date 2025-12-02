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
use prost::Message;
use tracing;
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
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};

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

        // Tính node_id từ vị trí trong committee (tương tự primary)
        let mut primary_keys: Vec<_> = worker.committee.authorities.keys().cloned().collect();
        primary_keys.sort();
        let _node_id = primary_keys
            .iter()
            .position(|pk| pk == &worker.name)
            .unwrap_or(0) as u32;

        // CRITICAL FIX: Tạo tx_processor ở level cao để pass vào cả handle_primary_messages và handle_workers_messages
        // Điều này cho phép Synchronizer gửi batch lên primary sau khi nhận từ sync
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);
        
        // Spawn Processor trước để nó sẵn sàng nhận batches
        let primary_address = worker
            .committee
            .primary(&worker.name)
            .expect("Our public key is not in the committee")
            .worker_to_primary;
        Processor::spawn(
            worker.id,
            worker.store.clone(),
            rx_processor,
            SimpleSender::new(),
            primary_address,
            false,
        );

        worker.handle_primary_messages(&transport, tx_processor.clone()).await;
        worker.handle_clients_transactions(&transport).await;
        worker.handle_workers_messages(&transport, tx_processor).await;

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

    async fn handle_primary_messages(&self, transport: &QuicTransport, tx_processor: Sender<SerializedBatchMessage>) {
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
            tx_processor,
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
        Receiver::spawn(
            listener,
            TxReceiverHandler {
                tx_batch_maker,
                worker_id: self.id,
            },
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
        tx_processor: Sender<SerializedBatchMessage>,
    ) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);

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

        // NOTE: Processor đã được spawn trong handle_primary_messages để nhận batches từ sync

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
    worker_id: WorkerId,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Thử parse như Transactions (nhiều giao dịch)
        use crate::transaction_logger::parse_and_log_transactions_simple_with_logger;

        // Parse và log transactions nếu có thể
        // Cập nhật để không dùng structured logger nữa (chỉ log ra stdout/json nếu cần hoặc bỏ qua)
        parse_and_log_transactions_simple_with_logger(&message, self.worker_id);

        // Tính hash của transaction để log (nếu có thể parse)
        // Chỉ log khi cần thiết để trace giao dịch, không log hex đầy đủ
        if let Ok(txs) = crate::transaction_logger::transaction::Transactions::decode(&message[..]) {
            if let Some(tx) = txs.transactions.first() {
                let tx_hash = crate::transaction_logger::calculate_transaction_hash(tx);
                let tx_hash_hex = hex::encode(&tx_hash);
                log_tx_nhan_tu_client!(&tx_hash_hex, self.worker_id, message.len());
            }
        }

        self.tx_batch_maker
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");
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
            Ok(WorkerMessage::Batch(_batch_data)) => {
                // CRITICAL: Calculate digest để log
                use sha3::Digest as Sha3Digest;
                use sha3::Sha3_512 as Sha512;
                let hash = Sha512::digest(&serialized);
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(&hash[..32]);
                let batch_digest = crypto::Digest(bytes);
                
                // CRITICAL: Log khi worker nhận batch từ worker khác (có thể từ sync)
                tracing::info!(
                    target: "narwhal_audit",
                    "[WORKER BATCH RECEIVED] Worker received batch {} ({} bytes) from another worker (possibly from sync). Batch will be processed and sent to primary.",
                    batch_digest, serialized.len()
                );
                
                self
                .tx_processor
                .send(serialized.to_vec())
                .await
                .expect("Failed to send batch")
            },
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
