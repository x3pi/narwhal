// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use executor::transaction::{Object, Transaction};
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
use std::{net::SocketAddr, usize};
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--objects=[INT] 'The number of objects in the transaction'")
        .args_from_usage("--execution_time=[INT] 'The execution time (ms) of each transaction'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let objects = match matches.value_of("objects") {
        Some(x) => Some(
            x.parse::<usize>()
                .context("The number of objects must be a non-negative integer")?,
        ),
        None => None,
    };
    let execution_time = match matches.value_of("execution_time") {
        Some(x) => Some(
            x.parse::<u64>()
                .context("The execution time must be a non-negative integer")?,
        ),
        None => None,
    };
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    let mut client = Client::new(target, size, rate, nodes);

    if let Some(objects) = objects {
        // NOTE: This log entry is used to compute performance.
        info!("Number of objects per transaction: {objects}");

        client.set_number_of_objects(objects);
    }
    if let Some(execution_time) = execution_time {
        // NOTE: This log entry is used to compute performance.
        info!("Transaction's execution time: {execution_time} ms");

        client.set_execution_time(execution_time);
    }

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    if client.bytes_transactions() {
        info!("Sending bytes transactions");
        client
            .send_bytes_transactions()
            .await
            .context("Failed to submit transactions")
    } else {
        info!("Sending executable transactions");
        client
            .send_executor_transactions()
            .await
            .context("Failed to submit transactions")
    }
}

struct Client {
    target: SocketAddr,
    size: usize,
    rate: u64,
    nodes: Vec<SocketAddr>,
    objects: usize,
    execution_time: u64,
}

impl Client {
    pub fn new(target: SocketAddr, size: usize, rate: u64, nodes: Vec<SocketAddr>) -> Self {
        Self {
            target,
            size,
            rate,
            nodes,
            objects: 0,
            execution_time: 0,
        }
    }

    pub fn set_number_of_objects(&mut self, objects: usize) {
        self.objects = objects;
    }

    pub fn set_execution_time(&mut self, execution_time: u64) {
        self.execution_time = execution_time;
    }

    pub fn bytes_transactions(&self) -> bool {
        self.objects == 0 || self.execution_time == 0
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }

    pub async fn send_executor_transactions(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // The transaction size must be at least 9 bytes to ensure all txs are different.
        if self.size < 9 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Pre-compute and allocate buffers to quickly create transactions.
        let mut rng = rand::thread_rng();
        let mut counter = 0;

        let object_size = self.size / self.objects;
        let object_content = vec![0; object_size];
        let mut buf = BytesMut::default();

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                // Make the transaction.
                let objects = (0..self.objects)
                    .map(|_| Object::random(&mut rng, object_content.clone()))
                    .collect();
                let tx = Transaction::new(objects, self.execution_time);
                let serialized = bincode::serialize(&tx).unwrap();

                if x == counter % burst {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {}", counter);

                    buf.put_u8(0u8); // Sample txs start with 0.
                } else {
                    buf.put_u8(1u8); // Standard txs start with 1.
                };

                buf.put_u64(counter); // This counter identifies the sample tx.
                buf.extend_from_slice(&serialized);

                let bytes = buf.split().freeze();
                if let Err(e) = transport.send(bytes).await {
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                };
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
        Ok(())
    }

    pub async fn send_bytes_transactions(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 16 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 16 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                if x == counter % burst {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {}", counter);

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u64(counter); // This counter identifies the tx.
                } else {
                    r += 1;
                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u64(r); // Ensures all clients send different txs.
                };

                tx.resize(self.size, 0u8);
                let bytes = tx.split().freeze();
                if let Err(e) = transport.send(bytes).await {
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
        Ok(())
    }
}
