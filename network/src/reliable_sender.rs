// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use crate::transport::Transport;
use bytes::Bytes;
use log::{info, warn};
use rand::prelude::SliceRandom as _;
use rand::rngs::SmallRng;
use rand::SeedableRng as _;
use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::net::SocketAddr;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};

use crate::quic::QuicTransport;

#[cfg(test)]
#[path = "tests/reliable_sender_tests.rs"]
pub mod reliable_sender_tests;

pub type CancelHandler = oneshot::Receiver<Bytes>;

pub struct ReliableSender {
    connections: HashMap<SocketAddr, Sender<InnerMessage>>,
    transport: QuicTransport,
    rng: SmallRng,
}

impl std::default::Default for ReliableSender {
    fn default() -> Self {
        Self::new()
    }
}

impl ReliableSender {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            transport: QuicTransport::new(),
            rng: SmallRng::from_entropy(),
        }
    }

    fn spawn_connection(address: SocketAddr, transport: QuicTransport) -> Sender<InnerMessage> {
        let (tx, rx) = channel(1_000);
        Connection::spawn(address, transport, rx);
        tx
    }

    pub async fn send(&mut self, address: SocketAddr, data: Bytes) -> CancelHandler {
        let (sender, receiver) = oneshot::channel();
        let transport = self.transport.clone();
        self.connections
            .entry(address)
            .or_insert_with(|| Self::spawn_connection(address, transport))
            .send(InnerMessage {
                data,
                cancel_handler: sender,
            })
            .await
            .expect("Failed to send internal message");
        receiver
    }

    pub async fn broadcast(
        &mut self,
        addresses: Vec<SocketAddr>,
        data: Bytes,
    ) -> Vec<CancelHandler> {
        let mut handlers = Vec::new();
        for address in addresses {
            let handler = self.send(address, data.clone()).await;
            handlers.push(handler);
        }
        handlers
    }

    pub async fn lucky_broadcast(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
    ) -> Vec<CancelHandler> {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast(addresses, data).await
    }
}

#[derive(Debug)]
struct InnerMessage {
    data: Bytes,
    cancel_handler: oneshot::Sender<Bytes>,
}

struct Connection {
    address: SocketAddr,
    transport: QuicTransport,
    receiver: Receiver<InnerMessage>,
    retry_delay: u64,
    buffer: VecDeque<(Bytes, oneshot::Sender<Bytes>)>,
}

impl Connection {
    fn spawn(address: SocketAddr, transport: QuicTransport, receiver: Receiver<InnerMessage>) {
        tokio::spawn(async move {
            Self {
                address,
                transport,
                receiver,
                retry_delay: 200,
                buffer: VecDeque::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut delay = self.retry_delay;
        let mut retry = 0;
        loop {
            match self.transport.connect(self.address).await {
                Ok(connection) => {
                    info!("Outgoing connection established with {}", self.address);
                    delay = self.retry_delay;
                    retry = 0;

                    let error = self.keep_alive(connection).await;
                    warn!("{}", error);
                }
                Err(e) => {
                    warn!(
                        "{}",
                        NetworkError::FailedToConnect(self.address, retry, e.to_string())
                    );
                    let timer = sleep(Duration::from_millis(delay));
                    tokio::pin!(timer);

                    'waiter: loop {
                        tokio::select! {
                            () = &mut timer => {
                                delay = min(2 * delay, 60_000);
                                retry += 1;
                                break 'waiter;
                            },
                            Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                                self.buffer.push_back((data, cancel_handler));
                                self.buffer.retain(|(_, handler)| !handler.is_closed());
                            }
                        }
                    }
                }
            }
        }
    }

    async fn keep_alive(
        &mut self,
        mut connection: Box<dyn crate::transport::Connection>,
    ) -> NetworkError {
        let mut pending_replies = VecDeque::new();

        let error = 'connection: loop {
            while let Some((data, handler)) = self.buffer.pop_front() {
                if handler.is_closed() {
                    continue;
                }
                match connection.send(data.clone()).await {
                    Ok(()) => {
                        pending_replies.push_back((data, handler));
                    }
                    Err(e) => {
                        self.buffer.push_front((data, handler));
                        break 'connection NetworkError::FailedToSendMessage(
                            self.address,
                            e.to_string(),
                        );
                    }
                }
            }

            tokio::select! {
                Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                    self.buffer.push_back((data, cancel_handler));
                },
                response = connection.recv() => {
                    let (data, handler) = match pending_replies.pop_front() {
                        Some(message) => message,
                        None => break 'connection NetworkError::UnexpectedAck(self.address)
                    };
                    match response {
                        Ok(Some(bytes)) => {
                            let _ = handler.send(bytes);
                        },
                        _ => {
                            pending_replies.push_front((data, handler));
                            break 'connection NetworkError::FailedToReceiveAck(self.address);
                        }
                    }
                },
            }
        };

        while let Some(message) = pending_replies.pop_back() {
            self.buffer.push_front(message);
        }
        error
    }
}
