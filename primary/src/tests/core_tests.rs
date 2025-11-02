// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{
    certificate, committee, committee_with_base_port, header, keys, listener, votes,
};
use crate::messages::Header;
use config::RoundWithEpoch;
use crypto::Signature;
use dashmap::DashMap; // Thêm import cho DashMap
use futures::future::try_join_all;
use std::fs;
use std::sync::Arc; // Thêm import cho Arc
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn process_header() {
    let mut keys = keys();
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let (name, _, _, consensus_secret) = keys.pop().unwrap();
    let mut signature_service = SignatureService::new(consensus_secret);

    let committee = committee_with_base_port(13_000);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make the vote we expect to receive.
    let expected = Vote::new(&header(), &name, &mut signature_service).await;

    // Spawn a listener to receive the vote.
    let address = committee
        .primary(&header().author)
        .unwrap()
        .primary_to_primary;
    let handle = listener(address);

    // Make a synchronizer for the core.
    let payload_cache = Arc::new(DashMap::new());
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        payload_cache, // Truyền cache vào
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee,
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
        /* shutdown_handle */ config::ShutdownHandle::new(),
    );

    // Send a header to the core.
    tx_primary_messages
        .send(PrimaryMessage::Header(header()))
        .await
        .unwrap();

    // Ensure the listener correctly received the vote.
    let received = handle.await.unwrap();
    match bincode::deserialize(&received).unwrap() {
        PrimaryMessage::Vote(x) => assert_eq!(x, expected),
        x => panic!("Unexpected message: {:?}", x),
    }

    // Ensure the header is correctly stored.
    let stored = store
        .read(header().id.to_vec())
        .await
        .unwrap()
        .map(|x| bincode::deserialize(&x).unwrap());
    assert_eq!(stored, Some(header()));
}

#[tokio::test]
async fn process_header_missing_parent() {
    let (name, _, __, consensus_secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(consensus_secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_parent";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let payload_cache = Arc::new(DashMap::new());
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        payload_cache, // Truyền cache vào
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
        /* shutdown_handle */ config::ShutdownHandle::new(),
    );

    // Send a header to the core.
    let header = Header {
        parents: [Digest::default()].iter().cloned().collect(),
        ..header()
    };
    let id = header.id.clone();
    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn process_header_missing_payload() {
    let (name, _, __, consensus_secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(consensus_secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_payload";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let payload_cache = Arc::new(DashMap::new());
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        payload_cache, // Truyền cache vào
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
        /* shutdown_handle */ config::ShutdownHandle::new(),
    );

    // Send a header to the core.
    let header = Header {
        payload: [(Digest::default(), 0)].iter().cloned().collect(),
        ..header()
    };
    let id = header.id.clone();
    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
async fn process_votes() {
    let (name, _, __, consensus_secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(consensus_secret);

    let committee = committee_with_base_port(13_100);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_vote";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let payload_cache = Arc::new(DashMap::new());
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        payload_cache, // Truyền cache vào
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core.
    Core::spawn(
        name,
        committee.clone(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
        /* shutdown_handle */ config::ShutdownHandle::new(),
    );

    // Make the certificate we expect to receive.
    let expected = certificate(&Header::default());

    // Spawn all listeners to receive our newly formed certificate.
    let handles: Vec<_> = committee
        .others_primaries(&name)
        .iter()
        .map(|(_, address)| listener(address.primary_to_primary))
        .collect();

    // Send a votes to the core.
    for vote in votes(&Header::default()) {
        tx_primary_messages
            .send(PrimaryMessage::Vote(vote))
            .await
            .unwrap();
    }

    // Ensure all listeners got the certificate.
    for received in try_join_all(handles).await.unwrap() {
        match bincode::deserialize(&received).unwrap() {
            PrimaryMessage::Certificate(x) => assert_eq!(x, expected),
            x => panic!("Unexpected message: {:?}", x),
        }
    }
}

#[tokio::test]
async fn process_certificates() {
    let (name, _, __, consensus_secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(consensus_secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(3);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_consensus, mut rx_consensus) = channel(3);
    let (tx_parents, mut rx_parents) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_certificates";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Store genesis certificates so that deliver_certificate can find parents
    let comm = committee();
    let genesis_certificates = Certificate::genesis(&comm);
    for genesis_cert in &genesis_certificates {
        let bytes = bincode::serialize(genesis_cert).unwrap();
        store.write(genesis_cert.digest().to_vec(), bytes).await;
    }

    // Make a synchronizer for the core.
    let payload_cache = Arc::new(DashMap::new());
    let synchronizer = Synchronizer::new(
        name,
        &comm,
        store.clone(),
        payload_cache, // Truyền cache vào
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    // Spawn the core with the same committee instance
    Core::spawn(
        name,
        comm.clone(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
        /* shutdown_handle */ config::ShutdownHandle::new(),
    );

    // Send enough certificates to the core.
    // Use headers from the same committee - must use consensus keys for signing
    let cert_headers: Vec<_> = {
        let comm_for_headers = comm.clone();
        keys()
            .into_iter()
            .take(3)
            .map(|(author, _, consensus_pk, consensus_secret)| {
                // Verify author matches consensus key in committee
                assert_eq!(
                    comm_for_headers.consensus_key(&author).unwrap(),
                    consensus_pk
                );

                let header = Header {
                    author,
                    round_with_epoch: RoundWithEpoch::from_round_and_committee(
                        1,
                        &comm_for_headers,
                    ),
                    parents: Certificate::genesis(&comm_for_headers)
                        .iter()
                        .map(|x| x.digest())
                        .collect(),
                    ..Header::default()
                };
                let id = header.digest();
                Header {
                    id: id.clone(),
                    signature: Signature::new(&id, &consensus_secret), // Use consensus secret key
                    ..header
                }
            })
            .collect()
    };
    let certificates: Vec<_> = cert_headers
        .iter()
        .map(|header| certificate(header))
        .collect();

    // Verify all headers first
    for header in &cert_headers {
        match header.verify(&comm) {
            Ok(()) => {}
            Err(e) => panic!(
                "Header verification failed: {:?}. Header: {:?}",
                e,
                header.digest()
            ),
        }
    }

    // Verify all certificates before sending
    for cert in &certificates {
        match cert.verify(&comm) {
            Ok(()) => {}
            Err(e) => panic!(
                "Certificate verification failed: {:?}. Certificate: {:?}, Header verify: {:?}",
                e,
                cert.digest(),
                cert.header.verify(&comm)
            ),
        }
    }

    for x in certificates.clone() {
        tx_primary_messages
            .send(PrimaryMessage::Certificate(x))
            .await
            .unwrap();
    }

    // Give the core some time to process certificates
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Ensure the core sends the parents of the certificates to the proposer.
    // Note: certificates aggregator needs quorum (3 out of 4 authorities with stake 1 each)
    // With 3 certificates from 3 different authorities, we should have stake 3 >= quorum threshold 3
    let received = tokio::time::timeout(
        tokio::time::Duration::from_secs(10),
        rx_parents.recv()
    ).await.unwrap_or_else(|_| {
        panic!("Timeout waiting for parents from proposer. Certificates may not have reached quorum.");
    }).unwrap();
    let parents = certificates.iter().map(|x| x.digest()).collect();
    assert_eq!(received, (parents, 1));

    // Ensure the core sends the certificates to the consensus.
    for x in certificates.clone() {
        let received =
            tokio::time::timeout(tokio::time::Duration::from_secs(10), rx_consensus.recv())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "Timeout waiting for certificate {:?} from consensus",
                        x.digest()
                    );
                })
                .unwrap();
        assert_eq!(received, x);
    }

    // Ensure the certificates are stored.
    for x in &certificates {
        let stored = store.read(x.digest().to_vec()).await.unwrap();
        let serialized = bincode::serialize(x).unwrap();
        assert_eq!(stored, Some(serialized));
    }
}
