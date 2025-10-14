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
use dashmap::DashMap;
use log::info;
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

pub const CHANNEL_CAPACITY: usize = 1_000;
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Header(Header),
    Vote(Vote),
    Certificate(Certificate),
    CertificatesRequest(Vec<Digest>, PublicKey),
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
        let (tx_sync_headers, rx_sync_headers) = channel(CHANNEL_CAPACITY);
        let (tx_sync_certificates, rx_sync_certificates) = channel(CHANNEL_CAPACITY);
        let (tx_headers_loopback, rx_headers_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_certificates_loopback, rx_certificates_loopback) = channel(CHANNEL_CAPACITY);
        let (tx_primary_messages, rx_primary_messages) = channel(CHANNEL_CAPACITY);
        let (tx_cert_requests, rx_cert_requests) = channel(CHANNEL_CAPACITY);
        let payload_cache = Arc::new(DashMap::new());

        parameters.log();
        let name = keypair.name;
        let consensus_secret = keypair.consensus_secret;

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
                tx_cert_requests,
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
            tx_consensus,
            tx_parents,
        );

        GarbageCollector::spawn(&name, &committee, consensus_round.clone(), rx_consensus);

        PayloadReceiver::spawn(store.clone(), payload_cache.clone(), rx_others_digests);

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
            rx_parents,
            rx_our_digests,
            tx_headers,
        );

        Helper::spawn(committee.clone(), store, rx_cert_requests);

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
    tx_cert_requests: Sender<(Vec<Digest>, PublicKey)>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            PrimaryMessage::CertificatesRequest(missing, requestor) => self
                .tx_cert_requests
                .send((missing, requestor))
                .await
                .expect("Failed to send primary message"),
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
            WorkerPrimaryMessage::OurBatch(digest, worker_id, batch) => self
                .tx_our_digests
                .send((digest, worker_id, batch))
                .await
                .expect("Failed to send workers' digests"),
            WorkerPrimaryMessage::OthersBatch(digest, worker_id, batch) => self
                .tx_others_digests
                .send((digest, worker_id, batch))
                .await
                .expect("Failed to send workers' digests"),
        }
        Ok(())
    }
}
