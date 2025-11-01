// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, Parameters, WorkerId};
use consensus::Consensus;
use consensus::{Bullshark, ConsensusProtocol};
use crypto::Digest;
use crypto::Hash as _;
use env_logger::Env;
use primary::{Certificate, Primary};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use worker::{Worker, WorkerMessage};

// use std::io::Write;

// Thêm các use statements cần thiết
use bytes::{BufMut, BytesMut};
use prost::Message;
use std::collections::{HashMap, HashSet};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

// Thêm module để import các struct được tạo bởi prost
pub mod comm {
    include!(concat!(env!("OUT_DIR"), "/comm.rs"));
}

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 10_000;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Narwhal and Tusk.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .subcommand(SubCommand::with_name("primary").about("Run a single primary"))
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => KeyPair::new()
            .export(sub_matches.value_of("filename").unwrap())
            .context("Failed to generate key pair")?,
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

// Runs either a worker or a primary.
async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    let store = Store::new(store_path).context("Failed to create a store")?;
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    match matches.subcommand() {
        ("primary", _) => {
            let mut primary_keys: Vec<_> = committee.authorities.keys().cloned().collect();
            primary_keys.sort();

            let node_id = primary_keys
                .iter()
                .position(|pk| pk == &keypair.name)
                .context("Public key không tìm thấy trong committee file")?;

            log::info!("Node {} khởi chạy với ID: {}", keypair.name, node_id);
            log::info!("Node's eth secret: {}", keypair.secret.encode_base64());
            log::info!("Node's eth address: {:?}", keypair.name.to_eth_address());

            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);

            tokio::spawn(Primary::spawn(
                keypair,
                committee.clone(),
                parameters.clone(),
                store.clone(),
                tx_new_certificates,
                rx_feedback,
            ));

            let committee_clone: Committee = committee.clone();

            Consensus::spawn(
                committee,
                parameters.gc_depth,
                store.clone(),
                rx_new_certificates,
                tx_feedback,
                tx_output,
                ConsensusProtocol::Bullshark(Bullshark {
                    committee: committee_clone,
                    gc_depth: parameters.gc_depth,
                }),
            );

            analyze(rx_output, node_id, store, 1).await; // Epoch mặc định là 1
        }
        ("worker", Some(sub_matches)) => {
            let id_str = sub_matches.value_of("id").unwrap();
            let id = id_str.parse::<WorkerId>().with_context(|| {
                format!(
                    "Giá trị '{}' không phải là một số nguyên hợp lệ cho tham số --id",
                    id_str
                )
            })?;

            tokio::spawn(Worker::spawn(
                keypair.name,
                id,
                committee,
                parameters,
                store,
            ));
        }
        _ => unreachable!(),
    }

    // Giữ cho tiến trình chính sống mãi mãi.
    // SỬA LỖI: Đổi tên biến không sử dụng thành `_tx`.
    let (_tx, mut rx) = channel::<()>(1);
    rx.recv().await;

    unreachable!();
}

/// Receives an ordered list of certificates and apply any application-specific logic.
/// Sửa logic: Gửi block theo commit (bất kỳ round nào), tạo fake block rỗng cho round chẵn không commit.
async fn analyze(
    mut rx_output: Receiver<Certificate>,
    node_id: usize,
    mut store: Store,
    initial_epoch: u64,
) {
    fn put_uvarint_to_bytes_mut(buf: &mut BytesMut, mut value: u64) {
        loop {
            if value < 0x80 {
                buf.put_u8(value as u8);
                break;
            }
            buf.put_u8(((value & 0x7F) | 0x80) as u8);
            value >>= 7;
        }
    }

    let socket_path = format!("/tmp/executor{}.sock", node_id);
    log::info!(
        "[ANALYZE] Node ID {} attempting to connect to {}",
        node_id,
        socket_path
    );

    let mut stream = loop {
        match UnixStream::connect(&socket_path).await {
            Ok(stream) => {
                log::info!(
                    "[ANALYZE] Node ID {} connected successfully to {}",
                    node_id,
                    socket_path
                );
                break stream;
            }
            Err(e) => {
                log::warn!(
                    "[ANALYZE] Node ID {}: Connection to {} failed: {}. Retrying...",
                    node_id,
                    socket_path,
                    e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }
    };
    log::info!(
        "[ANALYZE] Node ID {} entering loop to wait for committed blocks.",
        node_id
    );

    // Track height lớn nhất đã gửi cho mỗi epoch (height = ceil(round / 2))
    let mut last_committed_height_per_epoch: HashMap<u64, u64> = HashMap::new();

    #[derive(Debug)]
    struct BlockBuilder {
        epoch: u64,
        height: u64,
        certificate_count: usize,
        transactions: Vec<comm::Transaction>,
        batch_hashes: HashSet<Digest>,
    }

    impl BlockBuilder {
        fn new(epoch: u64, height: u64) -> Self {
            Self {
                epoch,
                height,
                certificate_count: 0,
                transactions: Vec::new(),
                batch_hashes: HashSet::new(),
            }
        }
    }

    async fn send_blocks(
        stream: &mut UnixStream,
        blocks: Vec<comm::CommittedBlock>,
        node_id: usize,
    ) -> Result<(), String> {
        if blocks.is_empty() {
            return Ok(());
        }

        let epoch_data = comm::CommittedEpochData { blocks };

        let mut proto_buf = BytesMut::new();
        epoch_data
            .encode(&mut proto_buf)
            .map_err(|e| format!("Failed to encode Protobuf: {}", e))?;

        let mut len_buf = BytesMut::new();
        put_uvarint_to_bytes_mut(&mut len_buf, proto_buf.len() as u64);

        log::info!(
            "[ANALYZE] Node ID {} WRITING {} bytes (len) and {} bytes (data) to socket for {} blocks.",
            node_id,
            len_buf.len(),
            proto_buf.len(),
            epoch_data.blocks.len()
        );

        stream
            .write_all(&len_buf)
            .await
            .map_err(|e| format!("Failed to write length to socket: {}", e))?;

        stream
            .write_all(&proto_buf)
            .await
            .map_err(|e| format!("Failed to write payload to socket: {}", e))?;

        Ok(())
    }

    async fn emit_blocks(
        stream: &mut UnixStream,
        node_id: usize,
        blocks: Vec<comm::CommittedBlock>,
        last_committed: &mut HashMap<u64, u64>,
    ) -> Result<(), String> {
        if blocks.is_empty() {
            return Ok(());
        }

        let metadata: Vec<(u64, u64)> = blocks
            .iter()
            .map(|block| (block.epoch, block.height))
            .collect();

        send_blocks(stream, blocks, node_id).await?;

        for (epoch, height) in metadata {
            last_committed.insert(epoch, height);
        }

        Ok(())
    }

    let mut current_block: Option<BlockBuilder> = None;

    while let Some(certificate) = rx_output.recv().await {
        let commit_round = certificate.header.round;
        let epoch = initial_epoch;

        log::info!(
            "[ANALYZE] Node ID {} RECEIVED certificate for round {} (epoch {}) from consensus.",
            node_id,
            commit_round,
            epoch
        );

        let height = (commit_round + 1) / 2;

        // Nếu đang xây dựng block và gặp round cao hơn thì flush block cũ
        if let Some(current_height) = current_block.as_ref().map(|b| b.height) {
            if height > current_height {
                if let Some(finished) = current_block.take() {
                    let leader_round = finished.height * 2;
                    log::info!(
                        "[ANALYZE] Node ID {} Finalizing block for height {} (leader round {}) containing {} unique transactions ({} certificates).",
                        node_id,
                        finished.height,
                        leader_round,
                        finished.transactions.len(),
                        finished.certificate_count
                    );

                    let block_count = 1;
                    if let Err(e) = emit_blocks(
                        &mut stream,
                        node_id,
                        vec![comm::CommittedBlock {
                            epoch: finished.epoch,
                            height: finished.height,
                            transactions: finished.transactions,
                        }],
                        &mut last_committed_height_per_epoch,
                    )
                    .await
                    {
                        log::error!(
                            "[ANALYZE] FATAL: Node ID {} Failed to send blocks: {}",
                            node_id,
                            e
                        );
                        break;
                    } else {
                        log::info!(
                            "[ANALYZE] SUCCESS: Node ID {} sent {} block(s) successfully.",
                            node_id,
                            block_count
                        );
                    }
                }
            }
        }

        let last_height = last_committed_height_per_epoch
            .get(&epoch)
            .copied()
            .unwrap_or(0);

        if height <= last_height {
            log::warn!(
                "[ANALYZE] Node ID {} received certificate round {} (height {}) but last committed height is {}. Skipping to avoid duplicates.",
                node_id,
                commit_round,
                height,
                last_height
            );
            continue;
        }

        if current_block.is_none() {
            if height > last_height + 1 {
                let mut missing_blocks = Vec::new();
                for missing_height in (last_height + 1)..height {
                    log::info!(
                        "[ANALYZE] Node ID {} Creating fake empty block for missing height {} (last committed {}).",
                        node_id,
                        missing_height,
                        last_height
                    );
                    missing_blocks.push(comm::CommittedBlock {
                        epoch,
                        height: missing_height,
                        transactions: Vec::new(),
                    });
                }

                if let Err(e) = emit_blocks(
                    &mut stream,
                    node_id,
                    missing_blocks,
                    &mut last_committed_height_per_epoch,
                )
                .await
                {
                    log::error!(
                        "[ANALYZE] FATAL: Node ID {} Failed to send missing blocks: {}",
                        node_id,
                        e
                    );
                    break;
                } else {
                    let block_count = height.saturating_sub(last_height + 1);
                    if block_count > 0 {
                        log::info!(
                            "[ANALYZE] SUCCESS: Node ID {} sent {} synthetic block(s) for gaps.",
                            node_id,
                            block_count
                        );
                    }
                }
            }

            current_block = Some(BlockBuilder::new(epoch, height));
        }

        if current_block.is_none() {
            current_block = Some(BlockBuilder::new(epoch, height));
        }

        if let Some(builder_ref) = current_block.as_ref() {
            if builder_ref.height > height {
                log::warn!(
                    "[ANALYZE] Node ID {} encountered out-of-order certificate (round {}, height {}) already building height {}. Skipping.",
                    node_id,
                    commit_round,
                    height,
                    builder_ref.height
                );
                continue;
            } else if builder_ref.height < height {
                if let Some(finished) = current_block.take() {
                    let leader_round = finished.height * 2;
                    log::warn!(
                        "[ANALYZE] Node ID {} forcing flush of unfinished block height {} (leader round {}) due to new height {}.",
                        node_id,
                        finished.height,
                        leader_round,
                        height
                    );

                    let block_count = 1;
                    if let Err(e) = emit_blocks(
                        &mut stream,
                        node_id,
                        vec![comm::CommittedBlock {
                            epoch: finished.epoch,
                            height: finished.height,
                            transactions: finished.transactions,
                        }],
                        &mut last_committed_height_per_epoch,
                    )
                    .await
                    {
                        log::error!(
                            "[ANALYZE] FATAL: Node ID {} Failed to send blocks: {}",
                            node_id,
                            e
                        );
                        break;
                    } else {
                        log::info!(
                            "[ANALYZE] SUCCESS: Node ID {} sent {} block(s) successfully.",
                            node_id,
                            block_count
                        );
                    }
                }

                current_block = Some(BlockBuilder::new(epoch, height));
            }
        }

        let builder = current_block.as_mut().expect("Block builder must exist");

        builder.certificate_count += 1;

        let cert_digest = certificate.digest();
        let payload_len = certificate.header.payload.len();
        log::debug!(
            "[ANALYZE] Node ID {} adding certificate {} (round {}) with {} batch digests to block height {}",
            node_id,
            cert_digest,
            commit_round,
            payload_len,
            builder.height
        );

        for (batch_digest, worker_id) in certificate.header.payload.iter() {
            if !builder.batch_hashes.insert(batch_digest.clone()) {
                log::debug!(
                    "[ANALYZE] Skipping duplicate batch {} within block height {}",
                    batch_digest,
                    builder.height
                );
                continue;
            }

            match store.read(batch_digest.to_vec()).await {
                Ok(Some(serialized_batch_message)) => {
                    log::debug!(
                        "[ANALYZE] Found batch {} from worker {} in store ({} bytes, height {}).",
                        batch_digest,
                        worker_id,
                        serialized_batch_message.len(),
                        builder.height
                    );
                    match bincode::deserialize::<WorkerMessage>(&serialized_batch_message) {
                        Ok(WorkerMessage::Batch(batch)) => {
                            if batch.is_empty() {
                                log::warn!(
                                    "[ANALYZE] Batch {} from worker {} decoded with 0 transactions (height {}).",
                                    batch_digest,
                                    worker_id,
                                    builder.height
                                );
                            }
                            for tx_data in batch {
                                builder.transactions.push(comm::Transaction {
                                    digest: tx_data,
                                    worker_id: *worker_id as u32,
                                });
                            }
                        }
                        Ok(_) => {
                            log::warn!(
                                "[ANALYZE] Digest {} did not correspond to a Batch message (height {}).",
                                batch_digest,
                                builder.height
                            );
                        }
                        Err(e) => {
                            log::error!(
                                "[ANALYZE] Failed to deserialize batch {}: {}",
                                batch_digest,
                                e
                            );
                        }
                    }
                }
                Ok(None) => {
                    log::warn!(
                        "[ANALYZE] Batch {} referenced in committed DAG not found in store (height {}).",
                        batch_digest,
                        builder.height
                    );
                }
                Err(e) => {
                    log::error!(
                        "[ANALYZE] Error reading batch {} from store: {}",
                        batch_digest,
                        e
                    );
                }
            }
        }
    }

    if let Some(current) = current_block.take() {
        let leader_round = current.height * 2;
        log::info!(
            "[ANALYZE] Node ID {} Finalizing block for height {} (leader round {}) containing {} unique transactions ({} certificates) before shutdown.",
            node_id,
            current.height,
            leader_round,
            current.transactions.len(),
            current.certificate_count
        );

        let block_count = 1;
        if let Err(e) = emit_blocks(
            &mut stream,
            node_id,
            vec![comm::CommittedBlock {
                epoch: current.epoch,
                height: current.height,
                transactions: current.transactions,
            }],
            &mut last_committed_height_per_epoch,
        )
        .await
        {
            log::error!(
                "[ANALYZE] FATAL: Node ID {} Failed to send final block: {}",
                node_id,
                e
            );
        } else {
            log::info!(
                "[ANALYZE] SUCCESS: Node ID {} sent {} block(s) successfully.",
                node_id,
                block_count
            );
        }
    }

    log::warn!(
        "[ANALYZE] Node ID {} exited the receive loop. No more blocks will be processed.",
        node_id
    );
}
