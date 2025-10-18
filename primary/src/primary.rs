// Copyright(C) Facebook, Inc. and its affiliates.
use crate::certificate_waiter::CertificateWaiter;
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::header_waiter::HeaderWaiter;
use crate::helper::Helper;
use crate::messages::{Certificate, Header, Vote};
// PayloadReceiver đã bị loại bỏ.
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use dashmap::DashMap;
use log::info;
use network::{
    quic::QuicTransport, transport::Transport, MessageHandler, Receiver as NetworkReceiver, Writer,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

// --- Định nghĩa các kiểu Cache dùng chung, hiệu năng cao ---
pub type PayloadCache = Arc<DashMap<Digest, Vec<u8>>>;
pub type PendingBatches = Arc<DashMap<Digest, (WorkerId, Round)>>;
// ---------------------------------------------------------

pub const CHANNEL_CAPACITY: usize = 1_000;
pub type Round = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimaryState {
    Running,
    Syncing,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Header(Header),
    Vote(Vote),
    Certificate(Certificate),
    CertificatesRequest(Vec<Digest>, PublicKey),
    CertificateRangeRequest {
        start_round: Round,
        end_round: Round,
        requestor: PublicKey,
    },
    CertificateBundle(Vec<Certificate>),
    Reconfigure,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    Synchronize(Vec<Digest>, PublicKey),
    Cleanup(Round),
    Reconfigure,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    OurBatch(Digest, WorkerId, Vec<u8>),
    OthersBatch(Digest, WorkerId, Vec<u8>),
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
        let payload_cache: PayloadCache = Arc::new(DashMap::new());
        let pending_batches: PendingBatches = Arc::new(DashMap::new());

        // Kênh để Proposer nhận batch (bao gồm cả nội dung).
        let (tx_workers, rx_workers) = channel::<(Digest, WorkerId, Vec<u8>)>(CHANNEL_CAPACITY);

        // Kênh để GC gửi lại batch mồ côi.
        let (tx_repropose, rx_repropose) = channel::<(Digest, WorkerId, Vec<u8>)>(CHANNEL_CAPACITY);

        let (tx_parents, rx_parents) = channel(CHANNEL_CAPACITY);
        let (tx_headers, rx_proposer) = channel(CHANNEL_CAPACITY);
        let (tx_sync_headers, rx_sync_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_certificates, rx_sync_certificates) = channel(CHANNEL_CAPACITY);
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        let (tx_helper_requests, rx_helper_requests) = channel(CHANNEL_CAPACITY);

        parameters.log();
        let name = keypair.name;
        let consensus_secret = keypair.consensus_secret;
        let consensus_round = Arc::new(AtomicU64::new(0));
        let transport = QuicTransport::new();

        let mut primary_address = committee.primary(&name).unwrap().primary_to_primary;
        primary_address.set_ip("0.0.0.0".parse().unwrap());
        let primary_listener = transport.listen(primary_address).await.unwrap();
        NetworkReceiver::spawn(
            primary_listener,
            PrimaryReceiverHandler {
                tx_primary_messages: tx_primary_messages.clone(),
                tx_helper: tx_helper_requests,
            },
        );
        info!(
            "Primary {} listening to primary messages on {}",
            name, primary_address
        );

        let mut worker_address = committee.primary(&name).unwrap().worker_to_primary;
        worker_address.set_ip("0.0.0.0".parse().unwrap());
        let worker_listener = transport.listen(worker_address).await.unwrap();
        NetworkReceiver::spawn(
            worker_listener,
            WorkerReceiverHandler {
                tx_workers: tx_workers,
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
        );

        let signature_service = SignatureService::new(consensus_secret);

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
            tx_primary_messages.clone(),
            tx_consensus,
            tx_parents,
        );

        GarbageCollector::spawn(
            &name,
            &committee,
            store.clone(),
            consensus_round.clone(),
            pending_batches.clone(),
            rx_consensus,
            tx_repropose,
        );

        Proposer::spawn(
            name,
            &committee,
            signature_service,
            store.clone(),
            parameters.header_size,
            parameters.max_header_delay,
            pending_batches.clone(),
            rx_parents,
            rx_workers,
            rx_repropose,
            tx_headers,
        );

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
        Helper::spawn(committee.clone(), store, rx_helper_requests);

        info!(
            "Primary {} successfully booted on {}",
            name,
            committee.primary(&name).unwrap().primary_to_primary.ip()
        );
    }
}

/// Xử lý tin nhắn đến từ các Primary khác.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_primary_messages: Sender<PrimaryMessage>,
    tx_helper: Sender<PrimaryMessage>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;
        let message: PrimaryMessage =
            bincode::deserialize(&serialized).map_err(DagError::SerializationError)?;
        match message {
            msg @ PrimaryMessage::CertificatesRequest(..)
            | msg @ PrimaryMessage::CertificateRangeRequest { .. } => {
                self.tx_helper
                    .send(msg)
                    .await
                    .expect("Failed to send request to Helper");
            }
            msg => {
                self.tx_primary_messages
                    .send(msg)
                    .await
                    .expect("Failed to send message to Core");
            }
        }
        Ok(())
    }
}

/// Xử lý tin nhắn đến từ các Worker, gửi thẳng đến Proposer.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_workers: Sender<(Digest, WorkerId, Vec<u8>)>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            WorkerPrimaryMessage::OurBatch(digest, worker_id, batch)
            | WorkerPrimaryMessage::OthersBatch(digest, worker_id, batch) => self
                .tx_workers
                .send((digest, worker_id, batch))
                .await
                .expect("Failed to send batch to proposer"),
        }
        Ok(())
    }
}
