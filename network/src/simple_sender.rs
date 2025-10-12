// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use crate::quic::QuicTransport;
use crate::transport::{Connection as _, Transport};
use bytes::Bytes;
use log::{info, warn};
use rand::prelude::SliceRandom as _;
use rand::rngs::SmallRng;
use rand::SeedableRng as _;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};

#[cfg(test)]
#[path = "tests/simple_sender_tests.rs"]
pub mod simple_sender_tests;

pub struct SimpleSender {
    connections: HashMap<SocketAddr, Sender<Bytes>>,
    transport: QuicTransport,
    rng: SmallRng,
}

impl std::default::Default for SimpleSender {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleSender {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            transport: QuicTransport::new(),
            rng: SmallRng::from_entropy(),
        }
    }

    pub async fn send(&mut self, address: SocketAddr, data: Bytes) {
        let spawn_new_manager = |transport: &QuicTransport, addr: SocketAddr| {
            let (tx, rx) = channel(1_000);
            ConnectionManager::spawn(transport.clone(), addr, rx);
            tx
        };

        let transport_clone = self.transport.clone();
        let tx = self
            .connections
            .entry(address)
            .or_insert_with(|| spawn_new_manager(&transport_clone, address));

        if tx.send(data.clone()).await.is_err() {
            let new_tx = spawn_new_manager(&self.transport, address);
            if new_tx.send(data).await.is_ok() {
                self.connections.insert(address, new_tx);
            }
        }
    }

    pub async fn broadcast(&mut self, addresses: Vec<SocketAddr>, data: Bytes) {
        for address in addresses {
            self.send(address, data.clone()).await;
        }
    }

    pub async fn lucky_broadcast(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
    ) {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast(addresses, data).await
    }
}

struct ConnectionManager {
    transport: QuicTransport,
    address: SocketAddr,
    receiver: Receiver<Bytes>,
}

impl ConnectionManager {
    fn spawn(transport: QuicTransport, address: SocketAddr, receiver: Receiver<Bytes>) {
        tokio::spawn(async move {
            Self {
                transport,
                address,
                receiver,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut retry_delay = Duration::from_millis(200);

        loop {
            match self.transport.connect(self.address).await {
                Ok(mut connection) => {
                    info!("Outgoing connection established with {}", self.address);
                    retry_delay = Duration::from_millis(200);

                    while let Some(data) = self.receiver.recv().await {
                        if let Err(e) = connection.send(data).await {
                            warn!(
                                "{}",
                                NetworkError::FailedToSendMessage(self.address, e.to_string())
                            );
                            break;
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to {}: {}. Retrying in {:?}...",
                        self.address, e, retry_delay
                    );
                    sleep(retry_delay).await;
                    retry_delay = Duration::from_millis(
                        (retry_delay.as_millis() * 2).clamp(200, 10_000) as u64,
                    );
                }
            }
        }
    }
}