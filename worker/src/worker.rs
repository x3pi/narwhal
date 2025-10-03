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
use network::{MessageHandler, Receiver, Writer};
use primary::PrimaryWorkerMessage;
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The primary round number.
// TODO: Move to the primary.
pub type Round = u64;

/// Indicates a serialized `WorkerPrimaryMessage` message.
pub type SerializedBatchDigestMessage = Vec<u8>;

/// The message exchanged between workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch), // Một batch hoàn chỉnh.
    BatchRequest(Vec<Digest>, /* origin */ PublicKey), // Yêu cầu xin các batch bị thiếu.
}

pub struct Worker {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
}

impl Worker {
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        parameters: Parameters,
        store: Store,
    ) {
        // Define a worker instance.
        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
        };

        // Spawn all worker tasks.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        worker.handle_primary_messages(); // Dây chuyền nhận lệnh từ Primary.
        worker.handle_clients_transactions(tx_primary.clone()); // Dây chuyền sản xuất chính, từ giao dịch của client.
        worker.handle_workers_messages(tx_primary); // Dây chuyền xử lý tin nhắn từ các Worker khác.


         // Khởi chạy 'người giao liên' chuyên gửi tin nhắn LÊN cho Primary.
        // Nó nhận tin nhắn từ `rx_primary` và gửi chúng qua mạng.
        // The `PrimaryConnector` allows the worker to send messages to its primary.
        PrimaryConnector::spawn(
            worker
                .committee
                .primary(&worker.name)
                .expect("Our public key is not in the committee")
                .worker_to_primary,
            rx_primary, // rx để nhận tin nhắn từ các bộ phận khác của worker - sau đó gửi cho primary.
        );

        // NOTE: This log entry is used to compute performance.
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

    /// Thiết lập và khởi chạy dây chuyền xử lý các mệnh lệnh từ Primary.
    /// Spawn all tasks responsible to handle messages from our primary.
    fn handle_primary_messages(&self) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);

         // Mở cổng mạng để lắng nghe các chỉ thị từ Primary của chính node này.
        // Receive incoming messages from our primary.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        // worker nhận tin nhắn từ primary
        Receiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler { tx_synchronizer },
        );

        // Khởi chạy 'bộ phận đồng bộ hóa'.
        // Chịu trách nhiệm thực hiện các lệnh từ Primary, chủ yếu là đi tìm các batch bị thiếu
        // hoặc dọn dẹp dữ liệu cũ.
        Synchronizer::spawn(
            self.name,
            self.id,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            /* rx_message */ rx_synchronizer,
        );

        info!(
            "Worker {} listening to primary messages on {}",
            self.id, address
        );
    }

    /// Thiết lập và khởi chạy dây chuyền sản xuất chính: xử lý giao dịch từ client.
    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        // nhan tu client
        Receiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_batch_maker },
        );


        // 2. Khởi chạy 'máy đóng gói'.
        // Nó nhận giao dịch, gom chúng thành batch, sau đó phát sóng batch này đến các worker khác.
        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other workers that share the same `id` as us. Finally, it
        // gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            /* tx_message */ tx_quorum_waiter,
            // Danh sách địa chỉ của các worker khác để phát sóng.
            self.committee
                .others_workers(&self.name, &self.id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
        );

         // 3. Khởi chạy 'bộ phận chờ xác nhận'.
        // Nó chờ cho đến khi đủ số lượng worker khác xác nhận đã nhận được batch.
        // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        QuorumWaiter::spawn(
            self.committee.clone(),
            /* stake */ self.committee.stake(&self.name),
            /* rx_message */ rx_quorum_waiter,
            /* tx_batch */ tx_processor,
        );

         // 4. Khởi chạy 'bộ phận lưu kho và dán nhãn'.
        // Nó băm batch, lưu vào kho, và gửi báo cáo (digest) lên cho Primary.
        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the `PrimaryConnector`
        // that will send it to our primary machine.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle messages from other workers.
    fn handle_workers_messages(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

          // 1. Mở 'cổng giao tiếp' để nhận tin nhắn từ các worker ngang hàng.
        // Receive incoming messages from other workers.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        // worker nhận tin nhắn từ các worker khác
        Receiver::spawn(
            address,
            // Tùy vào loại tin nhắn, handler sẽ chuyển đến bộ phận phù hợp.
            /* handler */
            WorkerReceiverHandler {
                tx_helper,    // Nếu là yêu cầu xin batch -> Helper.
                tx_processor, // Nếu là batch hoàn chỉnh -> Processor.
            },
        );
                // 2. Khởi chạy 'bộ phận hỗ trợ'.
        // Chuyên trả lời các yêu cầu xin batch từ các worker khác.
        // The `Helper` is dedicated to reply to batch requests from other workers.
        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,
        );

         // 3. Khởi chạy 'bộ phận nhập kho'.
        // Xử lý các batch nhận được từ worker khác: băm, lưu, và báo cáo lên Primary.
        // This `Processor` hashes and stores the batches we receive from the other workers. It then forwards the
        // batch's digest to the `PrimaryConnector` that will send it to our primary.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ false,
        );

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
    }
}

/// Bộ xử lý logic cho cổng mạng nhận giao dịch từ client.
/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the transaction to the batch maker.
        self.tx_batch_maker
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");

        // Nhường quyền thực thi cho các task khác, tránh block luồng.
        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
          // Gửi lại một tin "Ack" để xác nhận đã nhận.
        let _ = writer.send(Bytes::from("Ack")).await;
        // Giải mã tin nhắn và phân loại.
        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            // Nếu là một batch hoàn chỉnh, gửi đến Processor để xử lý.
            Ok(WorkerMessage::Batch(..)) => self
                .tx_processor
                .send(serialized.to_vec())
                .await
                .expect("Failed to send batch"),
                // Nếu là yêu cầu xin batch, gửi đến Helper để trả lời.
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

/// Defines how the network receiver handles incoming primary messages.
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
        // Deserialize the message and send it to the synchronizer.
         // Giải mã lệnh và gửi nó đến Synchronizer.
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
