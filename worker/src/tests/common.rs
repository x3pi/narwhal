// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, Transaction};
use crate::worker::WorkerMessage;
use bytes::Bytes;
use config::{Authority, Committee, PrimaryAddresses, WorkerAddresses};
use crypto::{
    generate_consensus_keypair, generate_keypair, ConsensusPublicKey, Digest, PublicKey, SecretKey,
};
use log::warn;
use network::quic::QuicTransport;
use network::transport::Transport;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use sha3::{Digest as Sha3DigestTrait, Sha3_512};
use std::net::SocketAddr;
use tokio::task::JoinHandle;

// Fixture to generate keypairs.
pub fn keys() -> Vec<(PublicKey, SecretKey, ConsensusPublicKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4)
        .map(|_| {
            let (pk, sk) = generate_keypair(&mut rng);
            let (cpk, _) = generate_consensus_keypair(&mut rng);
            (pk, sk, cpk)
        })
        .collect()
}

// Fixture to create a committee.
pub fn committee() -> Committee {
    Committee {
        authorities: keys()
            .into_iter()
            .enumerate()
            .map(|(i, (id, _, consensus_key))| {
                let primary = PrimaryAddresses {
                    primary_to_primary: format!("127.0.0.1:{}", 100 + i).parse().unwrap(),
                    worker_to_primary: format!("127.0.0.1:{}", 200 + i).parse().unwrap(),
                };
                let workers = vec![(
                    0,
                    WorkerAddresses {
                        primary_to_worker: format!("127.0.0.1:{}", 300 + i).parse().unwrap(),
                        transactions: format!("127.0.0.1:{}", 400 + i).parse().unwrap(),
                        worker_to_worker: format!("127.0.0.1:{}", 500 + i).parse().unwrap(),
                    },
                )]
                .into_iter()
                .collect();
                (
                    id,
                    Authority {
                        stake: 1,
                        consensus_key,
                        primary,
                        workers,
                    },
                )
            })
            .collect(),
    }
}

// Fixture to create a committee with a specific base port.
pub fn committee_with_base_port(base_port: u16) -> Committee {
    let mut committee = committee();
    for authority in committee.authorities.values_mut() {
        let primary = &mut authority.primary;

        let port = primary.primary_to_primary.port();
        primary.primary_to_primary.set_port(base_port + port);

        let port = primary.worker_to_primary.port();
        primary.worker_to_primary.set_port(base_port + port);

        for worker in authority.workers.values_mut() {
            let port = worker.primary_to_worker.port();
            worker.primary_to_worker.set_port(base_port + port);

            let port = worker.transactions.port();
            worker.transactions.set_port(base_port + port);

            let port = worker.worker_to_worker.port();
            worker.worker_to_worker.set_port(base_port + port);
        }
    }
    committee
}

// Fixture for a single transaction.
pub fn transaction() -> Transaction {
    vec![0; 100]
}

// Fixture for a batch of transactions.
pub fn batch() -> Batch {
    vec![transaction(), transaction()]
}

// Fixture for a serialized batch message.
pub fn serialized_batch() -> Vec<u8> {
    let message = WorkerMessage::Batch(batch());
    bincode::serialize(&message).unwrap()
}

// Fixture for the digest of a serialized batch.
pub fn batch_digest() -> Digest {
    let hash = Sha3_512::digest(&serialized_batch());
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&hash[..32]);
    Digest(bytes)
}

// A fixture for a test network listener.
pub fn listener(address: SocketAddr, expected: Option<Bytes>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let transport = QuicTransport::new();
        let mut listener = transport.listen(address).await.unwrap();

        // Loop to accept multiple connections (useful for retries).
        loop {
            let (mut connection, peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("Listener failed to accept connection: {}", e);
                    continue;
                }
            };

            // Loop to process messages on the current connection.
            loop {
                match connection.recv().await {
                    Ok(Some(received)) => {
                        connection.send(Bytes::from("Ack")).await.unwrap();

                        if let Some(ref expected_bytes) = expected {
                            if received == *expected_bytes {
                                return; // End the task when the expected message is received.
                            }
                        }
                    }
                    Ok(None) => break, // Connection closed by peer.
                    Err(e) => {
                        warn!(
                            "Listener ({}) connection error: {}. Ready for new connection.",
                            peer, e
                        );
                        break;
                    }
                }
            }
        }
    })
}
