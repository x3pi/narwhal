// In primary/src/primary.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::certificate_waiter::CertificateWaiter;
use crate::core::Core;
use crate::error::DagError;
use crate::garbage_collector::GarbageCollector;
use crate::header_waiter::HeaderWaiter;
use crate::helper::Helper;
use crate::messages::{Certificate, Header, Vote};
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
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;

pub type PayloadCache = Arc<DashMap<Digest, Vec<u8>>>;
pub type PendingBatches = Arc<DashMap<Digest, (WorkerId, Round)>>;

pub const CHANNEL_CAPACITY: usize = 1_000;
pub type Round = u64;

#[derive(Serialize, Deserialize)]
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
    Reconfigure(Committee),
}

impl fmt::Debug for PrimaryMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Header(h) => f.debug_tuple("Header").field(h).finish(),
            Self::Vote(v) => f.debug_tuple("Vote").field(v).finish(),
            Self::Certificate(c) => f.debug_tuple("Certificate").field(c).finish(),
            Self::CertificatesRequest(d, p) => f
                .debug_tuple("CertificatesRequest")
                .field(d)
                .field(p)
                .finish(),
            Self::CertificateRangeRequest {
                start_round,
                end_round,
                requestor,
            } => f
                .debug_struct("CertificateRangeRequest")
                .field("start_round", start_round)
                .field("end_round", end_round)
                .field("requestor", requestor)
                .finish(),
            Self::CertificateBundle(c) => f.debug_tuple("CertificateBundle").field(c).finish(),
            Self::Reconfigure(_) => f.debug_tuple("Reconfigure").field(&"[Committee]").finish(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    Synchronize(Vec<Digest>, PublicKey),
    Cleanup(Round),
    Reconfigure(Committee),
}

impl fmt::Debug for PrimaryWorkerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Synchronize(d, p) => f.debug_tuple("Synchronize").field(d).field(p).finish(),
            Self::Cleanup(r) => f.debug_tuple("Cleanup").field(r).finish(),
            Self::Reconfigure(_) => f.debug_tuple("Reconfigure").field(&"[Committee]").finish(),
        }
    }
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
        initial_committee: Committee,
        parameters: Parameters,
        store: Store,
        tx_consensus: Sender<Certificate>,
        rx_consensus: Receiver<Certificate>,
    ) {
        let committee = Arc::new(RwLock::new(initial_committee));

        let payload_cache: PayloadCache = Arc::new(DashMap::new());
        let pending_batches: PendingBatches = Arc::new(DashMap::new());

        let (tx_workers, rx_workers) = channel::<(Digest, WorkerId, Vec<u8>)>(CHANNEL_CAPACITY);
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

        // SỬA LỖI: Lock committee một lần và truyền tham chiếu/clone vào các hàm.
        let committee_guard = committee.read().await;

        let primary_address = committee_guard.primary(&name).unwrap().primary_to_primary;
        let mut listen_address = primary_address;
        listen_address.set_ip("0.0.0.0".parse().unwrap());
        let primary_listener = transport.listen(listen_address).await.unwrap();
        NetworkReceiver::spawn(
            primary_listener,
            PrimaryReceiverHandler {
                tx_primary_messages: tx_primary_messages.clone(),
                tx_helper: tx_helper_requests,
            },
        );
        info!(
            "Primary {} listening to primary messages on {}",
            name, listen_address
        );

        let worker_address = committee_guard.primary(&name).unwrap().worker_to_primary;
        let mut listen_address = worker_address;
        listen_address.set_ip("0.0.0.0".parse().unwrap());
        let worker_listener = transport.listen(listen_address).await.unwrap();
        NetworkReceiver::spawn(
            worker_listener,
            WorkerReceiverHandler {
                tx_workers: tx_workers,
            },
        );
        info!(
            "Primary {} listening to workers messages on {}",
            name, listen_address
        );

        let synchronizer = Synchronizer::new(
            name,
            &*committee_guard, // Truyền tham chiếu &Committee
            store.clone(),
            payload_cache.clone(),
            tx_sync_headers,
            tx_sync_certificates,
        );

        let signature_service = SignatureService::new(consensus_secret);

        Core::spawn(
            name,
            committee.clone(), // Core cần Arc<RwLock<Committee>> để có thể thay đổi
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
        );

        GarbageCollector::spawn(
            &name,
            &*committee_guard, // Truyền tham chiếu &Committee
            store.clone(),
            consensus_round.clone(),
            pending_batches.clone(),
            rx_consensus,
            tx_repropose,
        );

        Proposer::spawn(
            name,
            &*committee_guard, // Truyền tham chiếu &Committee
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
            committee_guard.clone(), // Truyền Committee bằng value
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

        Helper::spawn(committee_guard.clone(), store, rx_helper_requests); // Truyền Committee bằng value

        info!(
            "Primary {} successfully booted on {}",
            name,
            primary_address.ip()
        );
    }
}

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
