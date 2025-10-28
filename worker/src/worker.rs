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
// *** SỬA LỖI: Thêm import này ***
use network::{
    quic::QuicTransport, transport::Transport, MessageHandler, Receiver as NetworkReceiver,
    SimpleSender, Writer,
};
use primary::{Epoch, PrimaryWorkerMessage as PrimaryToWorkerMessage}; // <-- Đổi tên để tránh xung đột
use serde::{Deserialize, Serialize};
// *** SỬA LỖI: Xóa import không dùng ***
// use sha3::{Digest as Sha3Digest, Sha3_512 as Sha512};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::RwLock;

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

pub const CHANNEL_CAPACITY: usize = 1_000;
pub type Round = u64;

// *** THAY ĐỔI: Sử dụng tên đã đổi ở trên và thêm ResetEpoch ***
// Đổi tên enum để tránh xung đột với primary::PrimaryWorkerMessage
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PrimaryWorkerMessage {
    Synchronize(Vec<Digest>, PublicKey),
    Cleanup(Round),
    Reconfigure(Committee),
    ResetEpoch(Epoch), // <-- Thêm thông điệp mới
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
    // BatchResponse(Digest, Batch), // Cân nhắc thêm loại này cho Helper
}

pub struct Worker {
    name: PublicKey,
    id: WorkerId,
    committee: Arc<RwLock<Committee>>,
    parameters: Parameters,
    store: Store,
    // *** THÊM: Lưu địa chỉ primary nội bộ để không đổi theo epoch ***
    primary_address: SocketAddr,
}

impl Worker {
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Arc<RwLock<Committee>>,
        parameters: Parameters,
        store: Store,
    ) {
        tokio::spawn(async move {
            // *** Lấy địa chỉ primary_address một lần duy nhất lúc khởi động ***
            let primary_address = {
                let committee_reader = committee.read().await;
                committee_reader
                    .primary(&name)
                    .expect("Our public key is not in the committee")
                    .worker_to_primary
            };

            // *** Log địa chỉ primary được lưu ***
            info!(
                "Worker {} initialized with primary address: {} (will not change across epochs)",
                id, primary_address
            );

            Self {
                name,
                id,
                committee,
                parameters,
                store,
                primary_address, // *** Lưu địa chỉ ***
            }
            .run()
            .await;
        });
    }

    async fn run(&self) {
        let transport = QuicTransport::new();
        self.handle_clients_transactions(&transport).await;
        self.handle_workers_messages(&transport).await;
        self.handle_primary_messages(&transport).await;
    }

    async fn handle_primary_messages(&self, transport: &QuicTransport) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);

        // Đọc committee từ Arc để lấy địa chỉ
        let committee_reader = self.committee.read().await;
        let mut address = committee_reader
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        drop(committee_reader); // Giải phóng lock sớm
        address.set_ip("0.0.0.0".parse().unwrap());

        let listener = transport
            .listen(address)
            .await
            .expect("Failed to create primary message listener");

        NetworkReceiver::spawn(
            // <--- Sử dụng tên đã import
            listener,
            PrimaryReceiverHandler {
                tx_synchronizer,
                // *** THAY ĐỔI: Clone Arc thay vì committee ***
                committee: self.committee.clone(),
            },
        );

        Synchronizer::spawn(
            self.name,
            self.id,
            // *** THAY ĐỔI: Clone Arc ***
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            rx_synchronizer, // rx_message của Synchronizer nhận PrimaryWorkerMessage
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

        // Đọc committee từ Arc
        let committee_reader = self.committee.read().await;
        let mut address = committee_reader
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());

        let listener = transport
            .listen(address)
            .await
            .expect("Failed to create transaction listener");
        NetworkReceiver::spawn(listener, TxReceiverHandler { tx_batch_maker }); // <--- Sử dụng tên đã import

        // *** THAY ĐỔI: Không lấy primary_address từ committee nữa, dùng self.primary_address ***
        // let primary_address = committee_reader
        //     .primary(&self.name)
        //     .expect("Our public key is not in the committee")
        //     .worker_to_primary;

        let others_workers: Vec<_> = committee_reader // <-- Đảm bảo kiểu dữ liệu rõ ràng
            .others_workers(&self.name, &self.id)
            .iter()
            .map(|(name, addresses)| (*name, addresses.worker_to_worker))
            .collect();

        // Lấy stake của chính worker này từ committee đang lock
        // let my_stake = committee_reader.stake(&self.name);

        // *** THAY ĐỔI: Clone Arc để truyền vào QuorumWaiter ***
        let committee_clone_for_quorum = self.committee.clone();
        drop(committee_reader); // Release lock

        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            rx_batch_maker,
            tx_quorum_waiter,
            others_workers,
        );

        // *** THAY ĐỔI: Truyền Arc committee và name vào QuorumWaiter::spawn ***
        QuorumWaiter::spawn(
            self.name, // <-- Thêm name
            committee_clone_for_quorum,
            // my_stake, // Loại bỏ stake cố định
            rx_quorum_waiter,
            tx_processor,
        );

        Processor::spawn(
            self.id,
            self.store.clone(),
            rx_processor,
            SimpleSender::new(),
            self.primary_address, // *** SỬ DỤNG địa chỉ đã lưu ***
            /* own_digest */ true, // Tạo digest cho batch của chính mình
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    async fn handle_workers_messages(&self, transport: &QuicTransport) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // Đọc committee từ Arc
        let committee_reader = self.committee.read().await;
        let mut address = committee_reader
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());

        let listener = transport
            .listen(address)
            .await
            .expect("Failed to create worker message listener");
        NetworkReceiver::spawn(
            // <--- Sử dụng tên đã import
            listener,
            WorkerReceiverHandler {
                tx_helper,
                tx_processor,
            },
        );

        // *** THAY ĐỔI: Clone Arc committee để truyền vào Helper ***
        let committee_clone_for_helper = self.committee.clone();

        // *** THAY ĐỔI: Không lấy primary_address từ committee nữa ***
        // let primary_address = committee_reader
        //     .primary(&self.name)
        //     .expect("Our public key is not in the committee")
        //     .worker_to_primary;
        drop(committee_reader); // Release lock

        // *** THAY ĐỔI: Truyền Arc committee đã clone vào Helper::spawn ***
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
            self.primary_address, // *** SỬ DỤNG địa chỉ đã lưu ***
            /* own_digest */
            false, // Không tạo digest cho batch của người khác
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
        // The message contains a string identifying the transaction (its digest).
        // It is safe to assume that this is correct since it comes from our own batch maker.
        let tx = message.to_vec(); // message is Vec<u8> (Transaction)
        if tx.is_empty() {
            warn!("Received empty transaction!");
            return Ok(());
        }
        info!(target: "payload_logger", "{}", hex::encode(&tx));
        self.tx_batch_maker
            .send(tx)
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
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize the message.
        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(batch)) => {
                // Đây là batch nhận được từ worker khác.
                // Chúng ta cần serialize lại *toàn bộ* WorkerMessage::Batch(batch)
                // để Processor tính digest và lưu trữ đúng cách.
                let message_to_process = WorkerMessage::Batch(batch);
                // Serialize lại để gửi cho Processor
                let serialized_for_processor = bincode::serialize(&message_to_process)
                    .expect("Failed to serialize received batch message");

                self.tx_processor
                    .send(serialized_for_processor)
                    .await
                    .expect("Failed to send batch")
            }
            Ok(WorkerMessage::BatchRequest(missing, requestor)) => self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send batch request"),
            // Ok(WorkerMessage::BatchResponse(..)) => { /* Xử lý nếu cần */ }
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>, // <-- Kiểu dữ liệu đúng
    // *** THAY ĐỔI: Sử dụng Arc<RwLock<Committee>> ***
    committee: Arc<RwLock<Committee>>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize the message using the *correct* enum type from primary
        let message: PrimaryToWorkerMessage = match bincode::deserialize(&serialized) {
            Ok(msg) => msg,
            Err(e) => {
                error!("Failed to deserialize primary message: {}", e);
                // Không gửi message lỗi đi đâu cả, chỉ log lỗi
                return Ok(()); // Trả về Ok để không đóng kết nối
            }
        };

        // Chuyển đổi sang kiểu enum cục bộ của worker nếu cần (hoặc dùng trực tiếp nếu trùng)
        // Hiện tại cấu trúc gần giống nhau, chỉ cần khớp tên variant
        let worker_internal_message = match message {
            PrimaryToWorkerMessage::Synchronize(d, p) => PrimaryWorkerMessage::Synchronize(d, p),
            PrimaryToWorkerMessage::Cleanup(r) => PrimaryWorkerMessage::Cleanup(r),
            PrimaryToWorkerMessage::Reconfigure(new_committee) => {
                info!(
                    "Worker received reconfigure signal from primary for epoch {}.",
                    new_committee.epoch
                );
                let new_epoch = new_committee.epoch; // Lấy epoch mới trước khi move committee

                // *** THAY ĐỔI: Lock và cập nhật committee trong Arc ***
                {
                    // Tạo scope riêng cho write lock
                    let mut committee_lock = self.committee.write().await;
                    *committee_lock = new_committee.clone(); // Cập nhật committee dùng chung
                    info!(
                        "Worker committee updated to epoch {}.",
                        committee_lock.epoch
                    );
                } // Lock được giải phóng ở đây

                // Gửi message Reconfigure gốc đến Synchronizer
                self.tx_synchronizer
                    .send(PrimaryWorkerMessage::Reconfigure(new_committee)) // Gửi Reconfigure trước
                    .await
                    .expect("Failed to send Reconfigure message to synchronizer");

                // *** THAY ĐỔI: Gửi thêm message ResetEpoch ***
                info!(
                    "Sending ResetEpoch signal for epoch {} to synchronizer.",
                    new_epoch
                );
                self.tx_synchronizer
                    .send(PrimaryWorkerMessage::ResetEpoch(new_epoch)) // Gửi ResetEpoch sau
                    .await
                    .expect("Failed to send ResetEpoch message to synchronizer");

                return Ok(()); // Kết thúc sớm vì đã xử lý và gửi message cần thiết
            }
        };

        // Gửi các message khác (Synchronize, Cleanup) đến Synchronizer
        self.tx_synchronizer
            .send(worker_internal_message)
            .await
            .expect("Failed to send message to synchronizer");

        Ok(())
    }
}
