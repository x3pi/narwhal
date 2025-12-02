// Copyright(C) Facebook, Inc. and its affiliates.
use crate::certificate_waiter::CertificateWaiter;
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::header_waiter::HeaderWaiter;
use crate::certificate_cache::new_certificate_cache;
use crate::helper::{Helper, HelperRequest};
use crate::messages::{Certificate, Header, Vote};
use crate::payload_receiver::PayloadReceiver;
use crate::proposer::Proposer;
// RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use dashmap::DashMap;
use log::{error, info};
use network::{
    quic::QuicTransport, // <--- THAY ĐỔI
    transport::Transport,
    MessageHandler,
    Receiver as NetworkReceiver,
    Writer,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
pub type PayloadCache = Arc<DashMap<Digest, Vec<u8>>>;

// CHANNEL_CAPACITY: Buffer size của tokio mpsc channel
// - Channel tự động dọn dẹp khi receiver nhận messages
// - Nếu receiver (proposer) xử lý chậm hơn sender (core), channel sẽ tích lũy
// - Giảm capacity xuống 5_000 để phát hiện lỗi sớm hơn (thay vì chờ 2 tiếng)
// - Nếu channel đầy, lỗi sẽ xuất hiện nhanh hơn → dễ debug hơn
pub const CHANNEL_CAPACITY: usize = 5_000;
pub const CERTIFICATE_CACHE_LIMIT: usize = 512;
pub type Round = u64;

#[derive(Debug, Clone)]
pub struct CommittedBatches {
    pub round: Round,
    pub digests: Vec<Digest>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Header(Header),
    Vote(Vote),
    Certificate(Certificate),
    CertificatesRequest(Vec<Digest>, PublicKey),
    StateSyncRequest {
        requester: PublicKey,
        since_round: Round,
        max_rounds: Round,
    },
    BatchReplica {
        digest: Digest,
        worker_id: WorkerId,
        batch: Vec<u8>,
        origin: PublicKey,
    },
    BatchSyncRecovery {
        digest: Digest,
        worker_id: WorkerId,
        author: PublicKey,
        requester: PublicKey,
        round: Round,
        attempts: u32,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    Synchronize(Vec<Digest>, PublicKey),
    Cleanup(Round),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    OurBatch(Digest, WorkerId, Vec<u8>),
    OthersBatch(Digest, WorkerId, Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct BatchRescue {
    pub digest: Digest,
    pub worker_id: WorkerId,
    pub batch: Vec<u8>,
    pub origin: PublicKey,
}

pub struct Primary;

impl Primary {
    pub async fn spawn(
        keypair: KeyPair,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        tx_consensus: Sender<Certificate>,
        rx_consensus: Receiver<Certificate>,
    ) {
        let (tx_others_digests, rx_others_digests) =
            channel::<(Digest, WorkerId, Vec<u8>)>(CHANNEL_CAPACITY);
        let (tx_our_digests, rx_our_digests) =
            channel::<(Digest, WorkerId, Vec<u8>)>(CHANNEL_CAPACITY);
        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        let (tx_headers, rx_proposer) = channel(CHANNEL_CAPACITY);
        let (tx_headers_to_proposer, rx_headers_from_core) = channel(CHANNEL_CAPACITY);
        let (tx_sync_headers, rx_sync_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_certificates, rx_sync_certificates) = channel(CHANNEL_CAPACITY);
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        let (tx_helper_requests, rx_helper_requests) = channel(CHANNEL_CAPACITY);
        let (tx_committed_batches, rx_committed_batches) = channel(CHANNEL_CAPACITY);
        let (tx_batch_rescue, rx_batch_rescue) = channel(CHANNEL_CAPACITY);
        let payload_cache = Arc::new(DashMap::new());
        let certificate_cache = new_certificate_cache(CERTIFICATE_CACHE_LIMIT);
        // RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng

        parameters.log();
        let name = keypair.name;
        let consensus_secret = keypair.consensus_secret;

        // Tính node ID từ vị trí trong committee
        let mut primary_keys: Vec<_> = committee.authorities.keys().cloned().collect();
        primary_keys.sort();
        let _node_id = primary_keys
            .iter()
            .position(|pk| pk == &name)
            .unwrap_or(0) as u32;

        let consensus_round = Arc::new(AtomicU64::new(0));

        // SỬA ĐỔI: Sử dụng QuicTransport.
        let transport = QuicTransport::new(); // <--- THAY ĐỔI

        let mut primary_address = committee
            .primary(&name)
            .expect("Our public key is not in the committee")
            .primary_to_primary;
        primary_address.set_ip("0.0.0.0".parse().unwrap());
        let primary_listener = transport
            .listen(primary_address)
            .await
            .expect("Failed to create primary listener");

        NetworkReceiver::spawn(
            primary_listener,
            PrimaryReceiverHandler {
                tx_primary_messages,
                tx_helper_requests,
            },
        );
        info!(
            "Primary {} listening to primary messages on {}",
            name, primary_address
        );

        let mut worker_address = committee
            .primary(&name)
            .expect("Our public key is not in the committee")
            .worker_to_primary;
        worker_address.set_ip("0.0.0.0".parse().unwrap());
        let worker_listener = transport
            .listen(worker_address)
            .await
            .expect("Failed to create worker listener");

        NetworkReceiver::spawn(
            worker_listener,
            WorkerReceiverHandler {
                name: name.clone(),
                tx_our_digests,
                tx_others_digests,
            },
        );
        info!(
            "Primary {} listening to workers messages on {}",
            name, worker_address
        );

        let synchronizer = Synchronizer::new(
            name,
            &committee,
            store.clone(),
            payload_cache.clone(),
            tx_sync_headers,
            tx_sync_certificates,
            consensus_round.clone(),
        );

        let signature_service = SignatureService::new(consensus_secret);

        // CATCH-UP MODE: Create channel to notify proposer about catch-up mode
        let (tx_proposer_catchup, rx_proposer_catchup) = channel(10);
        // ROUND SYNC: Create channel to send minimum network round to proposer
        let (tx_proposer_min_round, rx_proposer_min_round) = channel(10);

        Core::spawn(
            name,
            committee.clone(),
            store.clone(),
            synchronizer,
            signature_service.clone(),
            consensus_round.clone(),
            parameters.gc_depth,
            rx_primary_messages,
            rx_headers_loopback,
            rx_certificates_loopback,
            rx_proposer,
            tx_consensus,
            tx_parents,
            tx_headers_to_proposer.clone(),
            rx_batch_rescue,
            payload_cache.clone(),
            certificate_cache.clone(),
            // RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
            tx_proposer_catchup, // CATCH-UP MODE: Pass sender to Core
            tx_proposer_min_round.clone(), // ROUND SYNC: Pass sender to Core
        );

        GarbageCollector::spawn(
            &name,
            &committee,
            consensus_round.clone(),
            rx_consensus,
            tx_committed_batches.clone(),
        );

        PayloadReceiver::spawn(store.clone(), payload_cache.clone(), rx_others_digests, tx_batch_rescue.clone(), name.clone());

        HeaderWaiter::spawn(
            name,
            committee.clone(),
            store.clone(),
            consensus_round,
            parameters.gc_depth,
            parameters.sync_retry_delay,
            parameters.sync_retry_nodes,
            rx_sync_headers,
            tx_headers_loopback,
        );

        CertificateWaiter::spawn(
            store.clone(),
            rx_sync_certificates,
            tx_certificates_loopback,
        );

        Proposer::spawn(
            name,
            &committee,
            signature_service,
            store.clone(),
            parameters.header_size,
            parameters.max_header_delay,
            parameters.sync_retry_delay,
            rx_parents,
            rx_headers_from_core,
            rx_our_digests,
            rx_committed_batches,
            tx_headers,
            tx_batch_rescue,
            // RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
            rx_proposer_catchup, // CATCH-UP MODE: Pass receiver to Proposer
            rx_proposer_min_round, // ROUND SYNC: Pass receiver to Proposer
        );

        Helper::spawn(
            committee.clone(),
            store,
            rx_helper_requests,
            certificate_cache.clone(),
        );

        info!(
            "Primary {} successfully booted on {}",
            name,
            committee
                .primary(&name)
                .expect("Our public key is not in the committee")
                .primary_to_primary
                .ip()
        );
    }
}

// --- Các struct Handler (không thay đổi) ---

#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryMessage>,
    tx_helper_requests: Sender<HelperRequest>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            PrimaryMessage::CertificatesRequest(missing, requestor) => self
                .tx_helper_requests
                .send(HelperRequest::Certificates {
                    digests: missing,
                    requester: requestor,
                })
                .await
                .expect("Failed to send helper request"),
            PrimaryMessage::StateSyncRequest {
                requester,
                since_round,
                max_rounds,
            } => self
                .tx_helper_requests
                .send(HelperRequest::StateSync {
                    requester,
                    since_round,
                    max_rounds,
                })
                .await
                .expect("Failed to send state sync request"),
            request => self
                .tx_primary_messages
                .send(request)
                .await
                .expect("Failed to send certificate"),
        }
        Ok(())
    }
}

#[derive(Clone)]
struct WorkerReceiverHandler {
    name: PublicKey,
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
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            WorkerPrimaryMessage::OurBatch(digest, worker_id, batch) => {
                match self
                    .tx_our_digests
                    .send((digest.clone(), worker_id, batch))
                    .await
                {
                    Ok(()) => {
                        // Chỉ log khi có lỗi - bỏ log success để giảm noise
                    }
                    Err(e) => {
                        // CRITICAL ERROR: Channel đầy hoặc đóng - batches không được gửi tới proposer
                        error!(
                            "[PRIMARY RX WORKER] CRITICAL: Primary {} FAILED to send batch {} from worker {} to proposer channel: {}. Channel may be full!",
                            self.name, digest, worker_id, e
                        );
                    }
                }
            }
            WorkerPrimaryMessage::OthersBatch(digest, worker_id, batch) => {
                match self
                    .tx_others_digests
                    .send((digest.clone(), worker_id, batch))
                    .await
                {
                    Ok(()) => {
                        // Chỉ log khi có lỗi - bỏ log success để giảm noise
                    }
                    Err(e) => {
                        // CRITICAL ERROR: Channel đầy hoặc đóng
                        error!(
                            "[PRIMARY RX WORKER] CRITICAL: Primary {} FAILED to send batch {} from worker {} to payload receiver channel: {}. Channel may be full!",
                            self.name, digest, worker_id, e
                        );
                    }
                }
            }
        }
        Ok(())
    }
}
