// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header, Vote};
use bytes::Bytes;
use config::{Authority, Committee, PrimaryAddresses, RoundWithEpoch, WorkerAddresses};
use crypto::Hash as _;
use crypto::{
    generate_consensus_keypair, generate_keypair, ConsensusPublicKey, ConsensusSecretKey,
    PublicKey, SecretKey, Signature,
};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

// SỬA ĐỔI: Import các thành phần QUIC.
use network::quic::QuicTransport;
use network::transport::Transport;

impl PartialEq for Header {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl PartialEq for Vote {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

// Fixture: Cập nhật để tạo cả khóa mạng và khóa đồng thuận.
pub fn keys() -> Vec<(PublicKey, SecretKey, ConsensusPublicKey, ConsensusSecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4)
        .map(|_| {
            let (pk, sk) = generate_keypair(&mut rng);
            let (cpk, csk) = generate_consensus_keypair(&mut rng);
            (pk, sk, cpk, csk)
        })
        .collect()
}

// Fixture: Cập nhật để thêm consensus_key vào Authority.
pub fn committee() -> Committee {
    Committee {
        authorities: keys()
            .into_iter()
            .enumerate()
            .map(|(i, (id, _, consensus_key, _))| {
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
                        p2p_address: format!("127.0.0.1:{}", 600 + i),
                    },
                )
            })
            .collect(),
        epoch: 0,
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

// Fixture: Cập nhật để ký bằng ConsensusSecretKey.
pub fn header() -> Header {
    let (author, _, _, secret) = keys().pop().unwrap();
    let comm = committee();
    let header = Header {
        author,
        round_with_epoch: RoundWithEpoch::from_round_and_committee(1, &comm),
        parents: Certificate::genesis(&comm)
            .iter()
            .map(|x| x.digest())
            .collect(),
        ..Header::default()
    };
    let id = header.digest();
    Header {
        id: id.clone(),
        signature: Signature::new(&id, &secret),
        ..header
    }
}

// Fixture: Cập nhật để ký bằng ConsensusSecretKey.
#[allow(dead_code)]
pub fn headers() -> Vec<Header> {
    let comm = committee();
    keys()
        .into_iter()
        .map(|(author, _, _, secret)| {
            let header = Header {
                author,
                round_with_epoch: RoundWithEpoch::from_round_and_committee(1, &comm),
                parents: Certificate::genesis(&comm)
                    .iter()
                    .map(|x| x.digest())
                    .collect(),
                ..Header::default()
            };
            let id = header.digest();
            Header {
                id: id.clone(),
                signature: Signature::new(&id, &secret),
                ..header
            }
        })
        .collect()
}

// Fixture: Cập nhật để ký bằng ConsensusSecretKey.
pub fn votes(header: &Header) -> Vec<Vote> {
    keys()
        .into_iter()
        .map(|(author, _, _, consensus_secret)| {
            let vote = Vote {
                id: header.id.clone(),
                round_with_epoch: header.round_with_epoch,
                origin: header.author,
                author,
                signature: Signature::default(),
            };
            let digest = vote.digest();
            Vote {
                signature: Signature::new(&digest, &consensus_secret),
                ..vote
            }
        })
        .collect()
}

// Fixture
pub fn certificate(header: &Header) -> Certificate {
    // Create certificate first to get its digest
    let cert = Certificate {
        header: header.clone(),
        votes: Vec::new(),
    };
    let cert_digest = cert.digest();

    // Create votes with signatures on certificate digest, not vote digest
    let votes_with_sigs: Vec<_> = keys()
        .into_iter()
        .map(|(author, _, _, consensus_secret)| {
            let signature = Signature::new(&cert_digest, &consensus_secret);
            (author, signature)
        })
        .collect();

    Certificate {
        header: header.clone(),
        votes: votes_with_sigs,
    }
}

// Fixture
// SỬA ĐỔI: Listener này bây giờ sử dụng QUIC và trả về Bytes.
pub fn listener(address: SocketAddr) -> JoinHandle<Bytes> {
    tokio::spawn(async move {
        let transport = QuicTransport::new();
        let mut listener = transport.listen(address).await.unwrap();
        let (mut connection, _) = listener.accept().await.unwrap();

        match connection.recv().await {
            Ok(Some(received)) => {
                connection.send(Bytes::from("Ack")).await.unwrap();
                received
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}
