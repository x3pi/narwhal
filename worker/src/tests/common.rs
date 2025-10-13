// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, Transaction};
use crate::worker::WorkerMessage;
use bytes::Bytes;
use config::{Authority, Committee, PrimaryAddresses, WorkerAddresses};
use crypto::{generate_keypair, Digest, PublicKey, SecretKey};
use sha2::{Digest as Sha2DigestTrait, Sha512};
use log::warn;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

// SỬA ĐỔI: Import các thành phần QUIC.
use network::quic::QuicTransport;
use network::transport::Transport;

// Fixture
pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture
pub fn committee() -> Committee {
    Committee {
        authorities: keys()
            .iter()
            .enumerate()
            .map(|(i, (id, _))| {
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
                .iter()
                .cloned()
                .collect();
                (
                    *id,
                    Authority {
                        stake: 1,
                        primary,
                        workers,
                    },
                )
            })
            .collect(),
    }
}

// Fixture.
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

// Fixture
pub fn transaction() -> Transaction {
    vec![0; 100]
}

// Fixture
pub fn batch() -> Batch {
    vec![transaction(), transaction()]
}

// Fixture
pub fn serialized_batch() -> Vec<u8> {
    let message = WorkerMessage::Batch(batch());
    bincode::serialize(&message).unwrap()
}

// Fixture
pub fn batch_digest() -> Digest {
    Digest(
        Sha512::digest(&serialized_batch()).as_slice()[..32]
            .try_into()
            .unwrap(),
    )
}

// Fixture
// SỬA LỖI: Listener này bây giờ có thể xử lý nhiều kết nối (ví dụ: khi sender kết nối lại).
pub fn listener(address: SocketAddr, expected: Option<Bytes>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let transport = QuicTransport::new();
        let mut listener = transport.listen(address).await.unwrap();

        // Vòng lặp bên ngoài để chấp nhận nhiều kết nối.
        loop {
            let (mut connection, peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("Listener failed to accept connection: {}", e);
                    continue; // Chờ kết nối tiếp theo.
                }
            };

            // Vòng lặp bên trong để xử lý tin nhắn trên kết nối hiện tại.
            loop {
                match connection.recv().await {
                    Ok(Some(received)) => {
                        connection.send(Bytes::from("Ack")).await.unwrap();
                        
                        // Nếu có tin nhắn mong đợi và khớp, task listener hoàn thành.
                        if let Some(ref expected_bytes) = expected {
                            if received == *expected_bytes {
                                return;
                            }
                        }
                    }
                    // Client đóng kết nối, thoát vòng lặp trong để chờ kết nối mới.
                    Ok(None) => break,
                    Err(e) => {
                        warn!("Listener ({}) connection error: {}. Ready for new connection.", peer, e);
                        break;
                    }
                }
            }
        }
    })
}