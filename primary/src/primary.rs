// Copyright(C) Facebook, Inc. and its affiliates.
use crate::certificate_waiter::CertificateWaiter;
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::header_waiter::HeaderWaiter;
use crate::helper::Helper;
use crate::messages::{Certificate, Header, Vote};
use crate::payload_receiver::PayloadReceiver;
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use futures::sink::SinkExt as _;
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The round number.
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    // Đề xuất header mới
    Header(Header),
    // Vote cho header
    Vote(Vote),
    // Certificate được tạo từ đủ votes
    Certificate(Certificate),
    // Yêu cầu đồng bộ
    CertificatesRequest(Vec<Digest>, /* requestor */ PublicKey),
}

/// The messages sent by the primary to its workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
     /// Primary yêu cầu Worker đồng bộ hóa các batch bị thiếu.
    Synchronize(Vec<Digest>, /* node mục tiêu */ PublicKey),
    /// Primary thông báo một round đã được xử lý xong để Worker dọn dẹp.
    Cleanup(Round),
}

/// The messages sent by the workers to their primary.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    /// Worker thông báo nó đã tạo xong một batch mới.
    OurBatch(Digest, WorkerId, Vec<u8>),
    /// Worker thông báo nó đã nhận được digest của một batch từ một authority khác.
    OthersBatch(Digest, WorkerId, Vec<u8>),
}

pub struct Primary;

impl Primary {
    pub fn spawn(
        keypair: KeyPair,                    // Khóa công khai và bí mật của node này.
        committee: Committee,                // Thông tin về tất cả các node trong mạng.
        parameters: Parameters,              // Các tham số cấu hình hệ thống (vd: kích thước header).
        store: Store,                        // Kết nối đến database để lưu trữ dữ liệu.
        tx_consensus: Sender<Certificate>,   // Kênh để GỬI certificate đã được chốt cho lớp Consensus.
        rx_consensus: Receiver<Certificate>, // Kênh để NHẬN feedback từ lớp Consensus (dùng cho garbage collection).
    ) {
        // Nhóm 1: Giao tiếp với Worker (Xử lý Batch Giao dịch)
        // Kênh cho các batch của node khác (Worker -> PayloadReceiver).
        let (tx_others_digests, rx_others_digests) =
            channel::<(Digest, WorkerId, Vec<u8>)>(CHANNEL_CAPACITY);
        // Kênh cho các batch của chính mình (Worker -> Proposer).
        let (tx_our_digests, rx_our_digests) =
            channel::<(Digest, WorkerId, Vec<u8>)>(CHANNEL_CAPACITY);
        // Nhóm 2: Vòng lặp Tạo Header Nội bộ (Core <-> Proposer)
        // Kênh gửi tín hiệu "đủ cha mẹ rồi, tạo header mới đi" (Core -> Proposer).
        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        // Kênh gửi header mới tạo (Proposer -> Core).
        let (tx_headers, rx_headers) = channel(CHANNEL_CAPACITY);
        // Nhóm 3: Xử lý Đồng bộ hóa (Khi bị thiếu dữ liệu)
        // Kênh gửi yêu cầu "đi tìm header/certificate bị thiếu" (Synchronizer -> Waiters).
        let (tx_sync_headers, rx_sync_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_certificates, rx_sync_certificates) = channel(CHANNEL_CAPACITY);
        // Kênh "hàng đã về, xử lý lại đi!" (Waiters -> Core).
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        // Nhóm 4: Giao tiếp với các Primary khác
        // Kênh chính nhận message từ các primary khác (NetworkReceiver -> Core).
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        let (tx_cert_requests, rx_cert_requests) = channel(CHANNEL_CAPACITY);

        // Write the parameters to the logs.
        parameters.log();

        // Parse the public and secret key of this authority.
        let name = keypair.name;
        let secret = keypair.secret;

        // Atomic variable use to synchronizer all tasks with the latest consensus round. This is only
        // used for cleanup. The only tasks that write into this variable is `GarbageCollector`.
        let consensus_round = Arc::new(AtomicU64::new(0));

        // Spawn the network receiver listening to messages from the other primaries.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_primary; // ← Địa chỉ TCP để lắng nghe
        address.set_ip("0.0.0.0".parse().unwrap()); // Lắng nghe trên tất cả các interface mạng.
        //"Hãy mở một cổng và bắt đầu lắng nghe tại địa chỉ <address>. Bất kỳ kết nối nào đến đây đều là từ một Primary khác."
        NetworkReceiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler {
                tx_primary_messages, // Tin nhắn thường sẽ được đẩy vào kênh này cho Core.
                tx_cert_requests,    // Yêu cầu xin certificate sẽ được đẩy vào kênh này cho Helper.
            },
        );
        info!(
            "Primary {} listening to primary messages on {}",
            name, address
        );
         // Khởi chạy 'tổng đài' mạng thứ hai, dành riêng để lắng nghe tin nhắn từ các Worker của chính node này.
        // Spawn the network receiver listening to messages from our workers.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_our_digests,
                tx_others_digests,
            },
        );
        info!(
            "Primary {} listening to workers messages on {}",
            name, address
        );

        // Tạo ra một 'bộ điều phối' phụ trách kiểm tra xem node có đủ dữ liệu (parents, payload)
        // để xử lý một Header hay không. Đây là một đối tượng phụ trợ được Core sử dụng.
        // The `Synchronizer` provides auxiliary methods helping to `Core` to sync.
        let synchronizer = Synchronizer::new(
            name,
            &committee,
            store.clone(),
            /* tx_header_waiter */ tx_sync_headers,
            /* tx_certificate_waiter */ tx_sync_certificates,
        );

        // Dịch vụ ký, sử dụng khóa bí mật để tạo chữ ký cho các Header và Vote.
        // The `SignatureService` is used to require signatures on specific digests.
        let signature_service = SignatureService::new(secret);

        // Khởi chạy 'bộ não trung tâm' của Primary.
        // Chịu trách nhiệm xử lý logic cốt lõi của DAG: xác thực Header, Vote, Certificate;
        // tạo Vote; tập hợp Certificate; và quyết định khi nào nên đề xuất Header mới.
        // The `Core` receives and handles headers, votes, and certificates from the other primaries.
        Core::spawn(
            name,
            committee.clone(),
            store.clone(),
            synchronizer,
            signature_service.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            /* Đầu vào: Nhận tin từ các Primary khác  rx_primaries */ rx_primary_messages,
            /* Đầu vào: Nhận header để xử lý lại rx_header_waiter*/ rx_headers_loopback,
            /* Đầu vào: Nhận certificate để xử lý lại rx_certificate_waiter */ rx_certificates_loopback,
            /* Đầu vào: Nhận header mới do mình tạo  rx_proposer*/ rx_headers,
            /* Đầu ra: Gửi certificate đã chốt cho Consensus */ tx_consensus,
            /* Đầu ra: Gửi tín hiệu tạo header mới  tx_proposer */ tx_parents,
        );
        // Khởi chạy 'bộ phận dọn dẹp'.
        // Lắng nghe round đã được đồng thuận cuối cùng từ Consensus (qua rx_consensus) và
        // ra lệnh cho các thành phần khác xóa dữ liệu cũ bằng cách cập nhật biến `consensus_round`.
        GarbageCollector::spawn(&name, &committee, consensus_round.clone(), rx_consensus);

        // Khởi chạy 'bộ phận kho tạm' cho payload từ node khác.
        // Chỉ đơn giản là nhận batch digest (qua rx_others_digests) và lưu vào Store để xác thực sau này.
        // Receives batch digests from other workers. They are only used to validate headers.
        PayloadReceiver::spawn(store.clone(), /* rx_workers */ rx_others_digests);


         // Khởi chạy 'bộ phận chờ hàng' cho Header.
        // Khi Core bị thiếu dữ liệu (parent hoặc batch), nó sẽ nhận yêu cầu từ Synchronizer,
        // đi đòi hàng từ node khác, và chờ cho đến khi hàng về. Khi đủ, nó sẽ gửi Header
        // trở lại Core qua kênh loopback để xử lý lại.
        // Whenever the `Synchronizer` does not manage to validate a header due to missing parent certificates of
        // batch digests, it commands the `HeaderWaiter` to synchronizer with other nodes, wait for their reply, and
        // re-schedule execution of the header once we have all missing data.
        HeaderWaiter::spawn(
            name,
            committee.clone(),
            store.clone(),
            consensus_round,
            parameters.gc_depth,
            parameters.sync_retry_delay,
            parameters.sync_retry_nodes,
            /*Đầu vào: Nhận yêu cầu sync rx_synchronizer */ rx_sync_headers,
            /*Đầu ra: Gửi lại cho Core  tx_core */ tx_headers_loopback,
        );

        // Khởi chạy 'bộ phận chờ hàng' cho Certificate.
        // Tương tự HeaderWaiter, nhưng xử lý việc thiếu các certificate tổ tiên (ancestors).
        // The `CertificateWaiter` waits to receive all the ancestors of a certificate before looping it back to the
        // `Core` for further processing.
        CertificateWaiter::spawn(
            store.clone(),
            /* rx_synchronizer */ rx_sync_certificates,
            /* tx_core */ tx_certificates_loopback,
        );

        // Khởi chạy 'bộ phận đề xuất'.
        // Khi nhận được lệnh từ Core (qua rx_parents), nó sẽ gom các batch digest mới nhất
        // (từ rx_our_digests), kết hợp với các parent certificate, và tạo ra một Header mới.
        // Header này sau đó được gửi lại cho Core qua tx_headers.

        // When the `Core` collects enough parent certificates, the `Proposer` generates a new header with new batch
        // digests from our workers and it back to the `Core`.
        Proposer::spawn(
            name,
            &committee,
            signature_service,
            store.clone(),
            parameters.header_size,
            parameters.max_header_delay,
            /* rx_core */ rx_parents,
            /* rx_workers */ rx_our_digests,
            /* tx_core */ tx_headers,
        );

        // Khởi chạy 'bộ phận hỗ trợ/xuất kho'.
        // Đáp ứng các yêu cầu xin Certificate (nhận qua rx_cert_requests) từ các node khác.
        // Việc này giúp giảm tải cho Core.
        // The `Helper` is dedicated to reply to certificates requests from other primaries.
        Helper::spawn(committee.clone(), store, rx_cert_requests);

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {} successfully booted on {}",
            name,
            committee
                .primary(&name)
                .expect("Our public key or worker id is not in the committee")
                .primary_to_primary
                .ip()
        );
    }
}

// Định nghĩa logic xử lý cho 'tổng đài' nhận tin từ các Primary khác.
/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryMessage>,
    tx_cert_requests: Sender<(Vec<Digest>, PublicKey)>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
     // Hàm này được gọi cho mỗi gói tin nhận được từ một Primary khác.
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        // Gửi lại một tin nhắn "Ack" (acknowledgement) để xác nhận đã nhận được.
        let _ = writer.send(Bytes::from("Ack")).await;
        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            // 2a. Nếu là CertificateRequest
            // Nếu là yêu cầu xin Certificate, gửi nó đến kênh của Helper.
            PrimaryMessage::CertificatesRequest(missing, requestor) => self
                .tx_cert_requests
                .send((missing, requestor))
                .await
                .expect("Failed to send primary message"),
             // 2b.
             // Nếu là các loại tin nhắn khác (Header, Vote, Certificate), gửi đến kênh chính của Core.    
            request => self
                .tx_primary_messages
                .send(request)
                .await
                .expect("Failed to send certificate"),
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_our_digests: Sender<(Digest, WorkerId, Vec<u8>)>,
    tx_others_digests: Sender<(Digest, WorkerId, Vec<u8>)>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Hàm này được gọi cho mỗi gói tin nhận được từ một Worker.
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            // Nếu là batch của mình, gửi đến kênh của Proposer.
            WorkerPrimaryMessage::OurBatch(digest, worker_id, batch) => self
                .tx_our_digests
                .send((digest, worker_id, batch))
                .await
                .expect("Failed to send workers' digests"),
               // Nếu là batch của node khác, gửi đến kênh của PayloadReceiver.   
            WorkerPrimaryMessage::OthersBatch(digest, worker_id, batch) => self
                .tx_others_digests
                .send((digest, worker_id, batch))
                .await
                .expect("Failed to send workers' digests"),
        }
        Ok(())
    }
}
