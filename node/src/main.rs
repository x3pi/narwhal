// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{anyhow, Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, NodeConfig, Parameters, ValidatorInfo, WorkerId};
use consensus::Consensus;
use consensus::{Bullshark, ConsensusProtocol, ConsensusState};
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::sleep;
use tokio::time::Duration;

// Thêm module để import các struct được tạo bởi prost
pub mod comm {
    include!(concat!(env!("OUT_DIR"), "/comm.rs"));
}

pub mod validator {
    include!(concat!(env!("OUT_DIR"), "/validator.rs"));
}

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 10_000;
const CONSENSUS_STATE_KEY: &[u8] = b"consensus_state";

async fn fetch_validators_via_uds(socket_path: &str, block_number: u64) -> Result<ValidatorInfo> {
    log::info!(
        "[NODE] Fetching validator list for block {} via UDS {}",
        block_number,
        socket_path
    );

    const UDS_CONNECT_TIMEOUT_MS: u64 = 5_000;
    const UDS_RW_TIMEOUT_MS: u64 = 10_000;

    let mut stream = tokio::time::timeout(
        Duration::from_millis(UDS_CONNECT_TIMEOUT_MS),
        UnixStream::connect(socket_path),
    )
    .await
    .context(format!(
        "UDS connection timeout ({}ms)",
        UDS_CONNECT_TIMEOUT_MS
    ))?
    .map_err(|e| anyhow::anyhow!("Failed to connect to UDS path '{}': {}", socket_path, e))?;

    let request = validator::Request {
        payload: Some(validator::request::Payload::BlockRequest(
            validator::BlockRequest { block_number },
        )),
    };
    let request_bytes = request.encode_to_vec();
    let len_bytes = (request_bytes.len() as u32).to_be_bytes();

    tokio::time::timeout(
        Duration::from_millis(UDS_RW_TIMEOUT_MS),
        stream.write_all(&len_bytes),
    )
    .await
    .context(format!("UDS write timeout ({}ms)", UDS_RW_TIMEOUT_MS))?
    .map_err(|e| anyhow::anyhow!("Failed to write request length to UDS: {}", e))?;

    tokio::time::timeout(
        Duration::from_millis(UDS_RW_TIMEOUT_MS),
        stream.write_all(&request_bytes),
    )
    .await
    .context(format!("UDS write timeout ({}ms)", UDS_RW_TIMEOUT_MS))?
    .map_err(|e| anyhow::anyhow!("Failed to write request payload to UDS: {}", e))?;

    let mut len_buf = [0u8; 4];
    tokio::time::timeout(
        Duration::from_millis(UDS_RW_TIMEOUT_MS),
        stream.read_exact(&mut len_buf),
    )
    .await
    .context(format!(
        "UDS read timeout ({}ms) - server may not be responding",
        UDS_RW_TIMEOUT_MS
    ))?
    .map_err(|e| anyhow::anyhow!("Failed to read response length from UDS: {}", e))?;

    let response_len = u32::from_be_bytes(len_buf) as usize;
    let mut response_buf = vec![0u8; response_len];
    tokio::time::timeout(
        Duration::from_millis(UDS_RW_TIMEOUT_MS),
        stream.read_exact(&mut response_buf),
    )
    .await
    .context(format!(
        "UDS read timeout ({}ms) - server may not be responding",
        UDS_RW_TIMEOUT_MS
    ))?
    .map_err(|e| anyhow::anyhow!("Failed to read response payload from UDS: {}", e))?;

    let wrapped_response = validator::Response::decode(&response_buf[..])
        .context("Failed to decode validator Response protobuf")?;

    let list = match wrapped_response.payload {
        Some(validator::response::Payload::ValidatorList(list)) => list,
        Some(other) => {
            return Err(anyhow::anyhow!(
                "Received unexpected response payload: {:?}",
                other
            ))
        }
        None => {
            return Err(anyhow::anyhow!(
                "Received empty response payload from validator service"
            ))
        }
    };

    let mut wrapper_list = ValidatorInfo::default();
    for proto_val in list.validators {
        wrapper_list.validators.push(config::Validator {
            address: proto_val.address,
            primary_address: proto_val.primary_address,
            worker_address: proto_val.worker_address,
            p2p_address: proto_val.p2p_address,
            total_staked_amount: proto_val.total_staked_amount,
            pubkey_bls: proto_val.pubkey_bls,
            pubkey_secp: proto_val.pubkey_secp,
        });
    }

    Ok(wrapper_list)
}

async fn read_last_committed_round(store: &mut Store) -> Option<u64> {
    match store.read(CONSENSUS_STATE_KEY.to_vec()).await {
        Ok(Some(bytes)) => match bincode::deserialize::<ConsensusState>(&bytes) {
            Ok(state) => Some(state.last_committed_round),
            Err(e) => {
                log::error!(
                    "Failed to deserialize consensus state: {}. Falling back to round 0.",
                    e
                );
                None
            }
        },
        Ok(None) => None,
        Err(e) => {
            log::error!(
                "Failed to read consensus state from store: {}. Falling back to round 0.",
                e
            );
            None
        }
    }
}

async fn fetch_committee_from_uds(
    socket_path: &str,
    block_number: u64,
    node_config: &NodeConfig,
    epoch: u64,
) -> Result<Committee> {
    const RETRY_DELAY_MS: u64 = 5_000;

    loop {
        match fetch_validators_via_uds(socket_path, block_number).await {
            Ok(validator_info) => {
                let self_address = node_config.name.to_eth_address();
                match Committee::from_validator_info(validator_info, &self_address, epoch) {
                    Ok(committee) => {
                        log::info!(
                            "[NODE] Loaded committee for epoch {} from UDS (block {}).",
                            epoch,
                            block_number
                        );
                        return Ok(committee);
                    }
                    Err(e) => {
                        log::error!(
                            "[NODE] Failed to parse committee data from UDS: {}. Retrying in {} ms...",
                            e,
                            RETRY_DELAY_MS
                        );
                    }
                }
            }
            Err(e) => {
                log::error!(
                    "[NODE] Failed to fetch validators for block {}: {}. Retrying in {} ms...",
                    block_number,
                    e,
                    RETRY_DELAY_MS
                );
            }
        }

        sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
    }
}

async fn load_initial_committee(
    committee_file: Option<&str>,
    store: &mut Store,
    node_config: &NodeConfig,
) -> Result<Committee> {
    let always_false = false; // Tạm thời để logic UDS chạy
    if always_false && committee_file.is_some() {
        let filename = committee_file.unwrap();
        log::info!("[New Branch] Loading committee from file: {}", filename);
        let mut committee =
            Committee::import(filename).context("Failed to load committee from file")?;
        if committee.epoch == 0 {
            committee.epoch = 1;
            log::info!("Committee file has epoch 0, setting to 1.");
        }
        Ok(committee)
    } else {
        let socket_path = node_config.uds_get_validators_path.trim();
        if socket_path.is_empty() {
            return Err(anyhow!(
                "NodeConfig.uds_get_validators_path is empty; cannot fetch committee"
            ));
        }

        let last_committed_round = read_last_committed_round(store).await.unwrap_or(0);
        let block_number = last_committed_round / 2;
        let epoch_to_load = block_number.saturating_add(1);

        log::info!(
            "[NODE] No committee file provided. Fetching via UDS for epoch {} (block {}).",
            epoch_to_load,
            block_number
        );

        fetch_committee_from_uds(socket_path, block_number, node_config, epoch_to_load).await
    }
}

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
                .args_from_usage(
                    "--committee=[FILE] 'Optional path to the committee definition file'",
                )
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
        ("generate_keys", Some(sub_matches)) => NodeConfig::new()
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
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    let node_config =
        NodeConfig::import(key_file).context("Failed to load the node's configuration")?;

    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    let store = Store::new(store_path).context("Failed to create a store")?;
    let mut store_for_committee = store.clone();
    let committee_file = matches.value_of("committee");
    let committee = load_initial_committee(committee_file, &mut store_for_committee, &node_config)
        .await
        .context("Failed to initialize committee")?;

    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);
    let initial_epoch = committee.epoch;
    let block_socket = if node_config.uds_block_path.trim().is_empty() {
        log::info!(
            "Node {} cấu hình uds_block_path trống: bỏ qua gửi committed block qua UDS",
            node_config.name
        );
        None
    } else {
        log::info!(
            "Node {} sẽ gửi committed block tới uds_block_path: {}",
            node_config.name,
            node_config.uds_block_path
        );
        Some(node_config.uds_block_path.clone())
    };

    match matches.subcommand() {
        ("primary", _) => {
            let mut primary_keys: Vec<_> = committee.authorities.keys().cloned().collect();
            primary_keys.sort();

            let node_id = primary_keys
                .iter()
                .position(|pk| pk == &node_config.name)
                .context("Public key không tìm thấy trong committee")?;

            log::info!("Node {} khởi chạy với ID: {}", node_config.name, node_id);
            log::info!("Node's eth secret: {}", node_config.secret.encode_base64());
            log::info!("Node's eth address: {}", node_config.name.to_eth_address());

            let keypair = KeyPair {
                name: node_config.name.clone(),
                secret: node_config.secret.clone(),
                consensus_key: node_config.consensus_key.clone(),
                consensus_secret: node_config.consensus_secret.clone(),
            };

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

            analyze(rx_output, node_id, store, initial_epoch, block_socket).await;
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
                node_config.name,
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
    block_socket: Option<String>,
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

    let mut stream_opt: Option<UnixStream> = if let Some(socket_path) = block_socket {
        log::info!(
            "[ANALYZE] Node ID {} attempting to connect to {}",
            node_id,
            socket_path
        );

        let stream = loop {
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

        Some(stream)
    } else {
        log::info!(
            "[ANALYZE] Node ID {} has no block socket configured; committed blocks will not be sent via UDS.",
            node_id
        );
        None
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
        stream: Option<&mut UnixStream>,
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

        match stream {
            Some(stream) => {
                send_blocks(stream, blocks, node_id).await?;
            }
            None => {
                drop(blocks);
            }
        }

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
                        stream_opt.as_mut(),
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
                    } else if stream_opt.is_some() {
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
                    stream_opt.as_mut(),
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
                } else if stream_opt.is_some() {
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
                        stream_opt.as_mut(),
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
                    } else if stream_opt.is_some() {
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
            stream_opt.as_mut(),
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
        } else if stream_opt.is_some() {
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
