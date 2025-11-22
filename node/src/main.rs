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

pub mod transaction {
    include!(concat!(env!("OUT_DIR"), "/transaction.rs"));
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

    log::info!(
        "Node config address : {:?}",
        node_config.name.to_eth_address()
    );
    log::info!(
        "Node config secp public key (hex) : {}",
        hex::encode(node_config.name.as_ref())
    );
    log::info!(
        "Node config consensus public key (hex) : {}",
        hex::encode(node_config.consensus_key.as_bytes())
    );

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

    log::info!("Committee: {:?}", committee);

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

/// Helper functions để parse và log transaction
mod tx_logger {
    use super::transaction::{AccessTuple, Transaction, TransactionHashData};
    use prost::Message;
    use sha3::{Digest as Sha3Digest, Keccak256};

    /// Tính hash của transaction (sử dụng Keccak256 như Ethereum)
    pub fn calculate_transaction_hash(tx: &Transaction) -> Vec<u8> {
        let hash_data = TransactionHashData {
            from_address: tx.from_address.clone(),
            to_address: tx.to_address.clone(),
            amount: tx.amount.clone(),
            max_gas: tx.max_gas,
            max_gas_price: tx.max_gas_price,
            max_time_use: tx.max_time_use,
            data: tx.data.clone(),
            r#type: tx.r#type,
            last_device_key: tx.last_device_key.clone(),
            new_device_key: tx.new_device_key.clone(),
            nonce: tx.nonce.clone(),
            chain_id: tx.chain_id,
            r: tx.r.clone(),
            s: tx.s.clone(),
            v: tx.v.clone(),
            gas_tip_cap: tx.gas_tip_cap.clone(),
            gas_fee_cap: tx.gas_fee_cap.clone(),
            access_list: tx
                .access_list
                .iter()
                .map(|at| AccessTuple {
                    address: at.address.clone(),
                    storage_keys: at.storage_keys.clone(),
                })
                .collect(),
        };

        let mut buf = Vec::new();
        if let Err(e) = hash_data.encode(&mut buf) {
            log::warn!("Failed to encode TransactionHashData: {}", e);
            return Vec::new();
        }

        let hash = Keccak256::digest(&buf);
        hash.to_vec()
    }

    /// Parse và log transaction từ payload
    pub fn parse_and_log_transaction(
        payload: &[u8],
        batch_digest: &crypto::Digest,
        tx_idx: usize,
        _worker_id: u32,
        height: u64,
    ) {
        // Thử parse như Transaction
        match Transaction::decode(payload) {
            Ok(tx) => {
                let transaction_hash = calculate_transaction_hash(&tx);
                let hash_hex = hex::encode(&transaction_hash);
                let from_hex = hex::encode(&tx.from_address);
                let to_hex = hex::encode(&tx.to_address);
                let amount_hex = hex::encode(&tx.amount);

                log::info!(
                    "[CONSENSUS TX LOG] Batch {} tx[{}] (height {}): Hash={}, From={}, To={}, Amount={}, Gas={}, GasPrice={}, ChainID={}, Type={}, Size={} bytes",
                    batch_digest,
                    tx_idx,
                    height,
                    hash_hex,
                    from_hex,
                    to_hex,
                    amount_hex,
                    tx.max_gas,
                    tx.max_gas_price,
                    tx.chain_id,
                    tx.r#type,
                    payload.len()
                );
            }
            Err(e) => {
                // Nếu parse failed, log hex để debug
                let payload_hex = if payload.len() <= 64 {
                    hex::encode(payload)
                } else {
                    format!("{}...", hex::encode(&payload[..64]))
                };
                log::warn!(
                    "[CONSENSUS TX LOG] Batch {} tx[{}] (height {}): Failed to parse Transaction: {}. Payload hex: {}",
                    batch_digest,
                    tx_idx,
                    height,
                    e,
                    payload_hex
                );
            }
        }
    }
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

        // Thử kết nối một số lần giới hạn, không block vô hạn
        const MAX_CONNECT_ATTEMPTS: u32 = 3;
        const CONNECT_RETRY_DELAY_MS: u64 = 500;

        let mut stream = None;
        for attempt in 1..=MAX_CONNECT_ATTEMPTS {
            match UnixStream::connect(&socket_path).await {
                Ok(s) => {
                    log::info!(
                        "[ANALYZE] Node ID {} connected successfully to {} on attempt {}",
                        node_id,
                        socket_path,
                        attempt
                    );
                    stream = Some(s);
                    break;
                }
                Err(e) => {
                    log::warn!(
                        "[ANALYZE] Node ID {}: Connection attempt {}/{} to {} failed: {}",
                        node_id,
                        attempt,
                        MAX_CONNECT_ATTEMPTS,
                        socket_path,
                        e
                    );
                    if attempt < MAX_CONNECT_ATTEMPTS {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            CONNECT_RETRY_DELAY_MS,
                        ))
                        .await;
                    }
                }
            }
        }

        if stream.is_none() {
            log::warn!(
                "[ANALYZE] Node ID {} failed to connect to {} after {} attempts. Will continue processing certificates without sending to UDS.",
                node_id,
                socket_path,
                MAX_CONNECT_ATTEMPTS
            );
        }

        stream
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

    // Track các batch đã được xử lý và gửi tới unix domain socket
    // Để tránh xử lý lại batch đã được gửi (có thể xuất hiện trong nhiều certificate/round)
    let mut processed_batches: HashSet<Digest> = HashSet::new();

    // Track các transaction đã được gửi tới unix domain socket
    // Để tránh thực thi lại transaction đã được gửi (có thể xuất hiện trong nhiều block)
    // NOTE: Đây là execution-level tracking, không ảnh hưởng đến consensus
    let mut processed_transactions: HashSet<Vec<u8>> = HashSet::new();

    #[derive(Debug)]
    struct BlockBuilder {
        epoch: u64,
        height: u64,
        certificate_count: usize,
        transactions: Vec<comm::Transaction>,
        batch_hashes: HashSet<Digest>,
        // Track các batch digest đã được thêm vào block này (để mark as processed sau khi gửi)
        batch_digests: Vec<Digest>,
        // Track các transaction hash đã được thêm vào block này (để mark as processed sau khi gửi)
        transaction_hashes_in_block: Vec<Vec<u8>>,
        // Track late batches được thêm vào block này
        // Map từ original height -> true nếu đã có late batch từ height đó
        late_batches_from_height: HashSet<u64>,
        // Track transaction hashes để tránh duplicate transaction trong cùng block
        // Sử dụng hash của transaction payload để identify
        transaction_hashes: HashSet<Vec<u8>>,
    }

    impl BlockBuilder {
        fn new(epoch: u64, height: u64) -> Self {
            Self {
                epoch,
                height,
                certificate_count: 0,
                transactions: Vec::new(),
                batch_hashes: HashSet::new(),
                batch_digests: Vec::new(),
                transaction_hashes_in_block: Vec::new(),
                late_batches_from_height: HashSet::new(),
                transaction_hashes: HashSet::new(),
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

        // Log chi tiết từng block trước khi gửi
        for block in &blocks {
            log::info!(
                "[UDS SEND] Node ID {} preparing block: epoch={}, height={}, tx_count={}",
                node_id,
                block.epoch,
                block.height,
                block.transactions.len()
            );

            // Log một số transactions mẫu
            if !block.transactions.is_empty() {
                for (idx, tx) in block.transactions.iter().enumerate().take(3) {
                    let tx_hex = hex::encode(&tx.digest);
                    log::info!(
                        "[UDS SEND] Node ID {} block height {} tx[{}]: worker_id={}, size={} bytes, hex={}",
                        node_id,
                        block.height,
                        idx,
                        tx.worker_id,
                        tx.digest.len(),
                        if tx.digest.len() <= 64 {
                            tx_hex
                        } else {
                            format!("{}...", &tx_hex[..128])
                        }
                    );
                }
                if block.transactions.len() > 3 {
                    log::info!(
                        "[UDS SEND] Node ID {} block height {} has {} more transactions (not shown)",
                        node_id,
                        block.height,
                        block.transactions.len() - 3
                    );
                }
            } else {
                log::info!(
                    "[UDS SEND] Node ID {} block height {} is EMPTY (no transactions)",
                    node_id,
                    block.height
                );
            }
        }

        let epoch_data = comm::CommittedEpochData { blocks };

        let mut proto_buf = BytesMut::new();
        epoch_data
            .encode(&mut proto_buf)
            .map_err(|e| format!("Failed to encode Protobuf: {}", e))?;

        let mut len_buf = BytesMut::new();
        put_uvarint_to_bytes_mut(&mut len_buf, proto_buf.len() as u64);

        log::info!(
            "[UDS SEND] Node ID {} WRITING {} bytes (len) and {} bytes (data) to socket for {} blocks.",
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

        log::info!(
            "[UDS SEND] Node ID {} successfully sent {} blocks to UDS",
            node_id,
            epoch_data.blocks.len()
        );

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

        let payload_len = certificate.header.payload.len();
        let cert_digest = certificate.digest();

        // Log chi tiết về payload ngay khi nhận certificate
        if payload_len > 0 {
            let batch_list: Vec<String> = certificate
                .header
                .payload
                .iter()
                .map(|(digest, worker_id)| format!("{} (worker {})", digest, worker_id))
                .collect();
            log::info!(
                "[ANALYZE] Node ID {} RECEIVED certificate {} for round {} (epoch {}) from consensus with {} batches in payload: {:?}",
            node_id,
                cert_digest,
                commit_round,
                epoch,
                payload_len,
                batch_list
            );
        } else {
            log::info!(
                "[ANALYZE] Node ID {} RECEIVED certificate {} for round {} (epoch {}) from consensus with EMPTY payload (0 batches).",
                node_id,
                cert_digest,
            commit_round,
            epoch
        );
        }

        let height = (commit_round + 1) / 2;

        // EARLY DETECTION: Phát hiện sớm batch đến muộn ngay khi nhận certificate
        // Kiểm tra trước khi xử lý để có thể cảnh báo sớm
        let last_height_check = last_committed_height_per_epoch
            .get(&epoch)
            .copied()
            .unwrap_or(0);

        if height <= last_height_check && payload_len > 0 {
            // Có thể là batch đến muộn - kiểm tra chi tiết
            let is_late = if let Some(current_builder) = current_block.as_ref() {
                // Đang xây dựng block cho height cao hơn -> block cũ đã finalize
                current_builder.height > height
            } else {
                // Không có block đang xây dựng -> block đã finalize
                true
            };

            if is_late {
                let batch_list: Vec<String> = certificate
                    .header
                    .payload
                    .iter()
                    .map(|(digest, worker_id)| format!("{} (worker {})", digest, worker_id))
                    .collect();

                log::error!(
                    "[LATE BATCH DETECTION] ⚠️ Node ID {} EARLY DETECTION: Certificate {} round {} (height {}) arrived LATE! Last committed: {}, Currently building: {}. {} batches will be SKIPPED: {:?}",
                    node_id,
                    cert_digest,
                    commit_round,
                    height,
                    last_height_check,
                    current_block.as_ref().map(|b| b.height).unwrap_or(0),
                    payload_len,
                    batch_list
                );

                // Log chi tiết từng batch để dễ trace
                for (batch_digest, worker_id) in certificate.header.payload.iter() {
                    log::error!(
                        "[LATE BATCH DETAIL] Batch {} from worker {} (height {}) will be SKIPPED due to late arrival",
                        batch_digest,
                        worker_id,
                        height
                    );
                }
            }
        }

        // Nếu đang xây dựng block và gặp round cao hơn thì flush block cũ
        if let Some(current_height) = current_block.as_ref().map(|b| b.height) {
            if height > current_height {
                if let Some(finished) = current_block.take() {
                    let leader_round = finished.height * 2;
                    let tx_count = finished.transactions.len();
                    if tx_count == 0 {
                        log::warn!(
                            "[ANALYZE] Node ID {} Finalizing EMPTY block for height {} (leader round {}) containing {} transactions ({} certificates). This may indicate batches were not processed correctly!",
                            node_id,
                            finished.height,
                            leader_round,
                            tx_count,
                            finished.certificate_count
                        );
                    } else {
                        log::info!(
                        "[ANALYZE] Node ID {} Finalizing block for height {} (leader round {}) containing {} unique transactions ({} certificates).",
                        node_id,
                        finished.height,
                        leader_round,
                            tx_count,
                        finished.certificate_count
                    );
                    }

                    // Track các batch và transaction đã được gửi để tránh duplicate
                    let batch_digests_to_mark = finished.batch_digests.clone();
                    let transaction_hashes_to_mark = finished.transaction_hashes_in_block.clone();
                    let tx_count = finished.transactions.len();
                    let batch_count = batch_digests_to_mark.len();
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
                            "[BATCH TRACK] Node ID {} SUCCESSFULLY sent block height {} (leader round {}) to UDS containing {} transactions from {} batches. Batches: {:?}",
                            node_id,
                            finished.height,
                            leader_round,
                            tx_count,
                            batch_count,
                            batch_digests_to_mark
                        );
                        // Mark các batch đã được gửi tới UDS
                        for batch_digest in &batch_digests_to_mark {
                            processed_batches.insert(batch_digest.clone());
                            log::debug!(
                                "[BATCH TRACK] Node ID {} MARKED batch {} as PROCESSED (sent to UDS in block height {})",
                                node_id,
                                batch_digest,
                                finished.height
                            );
                        }
                        // Mark các transaction đã được gửi tới UDS
                        for tx_hash in transaction_hashes_to_mark {
                            processed_transactions.insert(tx_hash);
                        }
                    }
                }
            }
        }

        let last_height = last_committed_height_per_epoch
            .get(&epoch)
            .copied()
            .unwrap_or(0);

        // IMPROVED: Xử lý late batch khi height < last_height nếu an toàn
        // Điều kiện an toàn:
        // 1. Certificate đã được commit (tất cả node đều thấy) - deterministic
        // 2. Đang xây dựng block cho height > height (block tiếp theo hoặc xa hơn)
        // 3. Chưa có late batch từ height này trong block hiện tại (tránh duplicate)
        if height < last_height {
            let payload_len = certificate.header.payload.len();
            if payload_len > 0 {
                // Kiểm tra xem có thể xử lý late batch trong block hiện tại không
                if let Some(current_builder) = current_block.as_mut() {
                    if current_builder.height > height
                        && !current_builder.late_batches_from_height.contains(&height)
                    {
                        // AN TOÀN: Thêm batch vào block hiện tại
                        // Certificate đã được commit, tất cả node sẽ xử lý giống nhau (deterministic)
                        let height_diff = current_builder.height - height;
                        log::warn!(
                            "[LATE BATCH HANDLING] Node ID {} received late certificate {} round {} (height {}) but last committed height is {}. Will process {} batches in CURRENT block {} (height {}, {} blocks ahead) to avoid batch being dropped. Certificate was committed, so all nodes will handle this the same way (deterministic).",
                node_id,
                            cert_digest,
                            commit_round,
                            height,
                            last_height,
                            payload_len,
                            current_builder.height,
                            current_builder.height,
                            height_diff
                        );

                        // Đánh dấu đã có late batch từ height này
                        current_builder.late_batches_from_height.insert(height);

                        // Tiếp tục xử lý batch bên dưới
                        // Batch sẽ được thêm vào builder.height
                    } else {
                        // Không thể xử lý an toàn - skip
                        let batch_list: Vec<String> = certificate
                            .header
                            .payload
                            .iter()
                            .map(|(digest, worker_id)| format!("{} (worker {})", digest, worker_id))
                            .collect();

                        if current_builder.height <= height {
                            log::warn!(
                                "[ANALYZE] Node ID {} received certificate {} round {} (height {}) but last committed height is {}, and currently building block for height {} (not > height). Cannot safely add late batches. SKIPPING to avoid fork. WARNING: {} batches will NOT be processed: {:?}",
                                node_id,
                                cert_digest,
                                commit_round,
                                height,
                                last_height,
                                current_builder.height,
                                payload_len,
                                batch_list
                            );
                        } else {
                            log::warn!(
                                "[ANALYZE] Node ID {} received certificate {} round {} (height {}) but last committed height is {}, and already processed late batches from height {} in block {}. SKIPPING duplicate to avoid duplicate execution. WARNING: {} batches will NOT be processed: {:?}",
                                node_id,
                                cert_digest,
                                commit_round,
                                height,
                                last_height,
                                height,
                                current_builder.height,
                                payload_len,
                                batch_list
                            );
                        }
                        continue;
                    }
                } else {
                    // Không có block đang xây dựng - tạo block mới cho height + 1
                    let next_height = height + 1;
                    log::warn!(
                        "[LATE BATCH HANDLING] Node ID {} received late certificate {} round {} (height {}) but last committed height is {}. Will create new block {} (height {}) to process {} batches. Certificate was committed, so all nodes will handle this the same way.",
                        node_id,
                        cert_digest,
                        commit_round,
                        height,
                        last_height,
                        next_height,
                        next_height,
                        payload_len
                    );

                    // Tạo block mới cho height + 1
                    let mut new_builder = BlockBuilder::new(epoch, next_height);
                    new_builder.late_batches_from_height.insert(height);
                    current_block = Some(new_builder);

                    // Tiếp tục xử lý batch bên dưới với height = next_height
                }
            } else {
                // Empty payload - skip
                log::warn!(
                    "[ANALYZE] Node ID {} received certificate {} round {} (height {}) but last committed height is {}. Skipping to avoid duplicates (empty payload).",
                    node_id,
                    cert_digest,
                commit_round,
                height,
                last_height
            );
                continue;
            }
        }

        // Nếu height == last_height, kiểm tra xem có đang xây dựng block cho height đó không
        // CHỈ xử lý batch nếu đang xây dựng block cho đúng height đó
        // KHÔNG tạo block mới cho height đã được finalize (tránh fork)
        // KHÔNG thêm batch vào block có height khác (tránh fork)
        if height == last_height {
            // Kiểm tra xem có đang xây dựng block cho height này không
            if let Some(current_builder) = current_block.as_mut() {
                if current_builder.height == height {
                    // Đang xây dựng block cho height này, vẫn xử lý batch
                    // (certificate đến muộn nhưng block chưa finalize)
                    log::info!(
                        "[ANALYZE] Node ID {} received certificate {} round {} (height {}) same as last committed height {}, but currently building block for this height. Will process batches.",
                        node_id,
                        cert_digest,
                        commit_round,
                        height,
                        last_height
                    );
                    // Tiếp tục xử lý bên dưới, không skip
                } else {
                    // Đang xây dựng block cho height khác
                    // Block cho height này đã được finalize, certificate đến muộn
                    let payload_len = certificate.header.payload.len();
                    if payload_len > 0 {
                        // IMPROVED: Xử lý late batch trong block hiện tại nếu an toàn
                        // Điều kiện an toàn:
                        // 1. Certificate đã được commit (tất cả node đều thấy) - deterministic
                        // 2. Đang xây dựng block cho height > height (block tiếp theo hoặc xa hơn)
                        // 3. Chưa có late batch từ height này trong block hiện tại (tránh duplicate)
                        //
                        // Lý do an toàn: Certificate đã commit, tất cả node sẽ xử lý giống nhau
                        // Nếu đang xây dựng block height + 2, +3..., vẫn có thể xử lý batch
                        // từ height cũ trong block đó, vì tất cả node sẽ làm giống nhau (certificate đã commit)
                        if current_builder.height > height
                            && !current_builder.late_batches_from_height.contains(&height)
                        {
                            // AN TOÀN: Thêm batch vào block hiện tại (có thể là height + 1, +2, +3...)
                            // Certificate đã được commit, tất cả node sẽ xử lý giống nhau (deterministic)
                            let height_diff = current_builder.height - height;
                            log::warn!(
                                "[LATE BATCH HANDLING] Node ID {} received late certificate {} round {} (height {}) after block {} was finalized. Will process {} batches in CURRENT block {} (height {}, {} blocks ahead) to avoid batch being dropped. Certificate was committed, so all nodes will handle this the same way (deterministic).",
                                node_id,
                                cert_digest,
                                commit_round,
                                height,
                                height,
                                payload_len,
                                current_builder.height,
                                current_builder.height,
                                height_diff
                            );

                            // Đánh dấu đã có late batch từ height này
                            current_builder.late_batches_from_height.insert(height);

                            // Tiếp tục xử lý batch bên dưới
                            // Batch sẽ được thêm vào builder.height (có thể là height + 1, +2, +3...)
                        } else {
                            // Không thể xử lý an toàn - skip để tránh duplicate hoặc fork
                            let batch_list: Vec<String> = certificate
                                .header
                                .payload
                                .iter()
                                .map(|(digest, worker_id)| {
                                    format!("{} (worker {})", digest, worker_id)
                                })
                                .collect();

                            if current_builder.height <= height {
                                log::warn!(
                                    "[ANALYZE] Node ID {} received certificate {} round {} (height {}) same as last committed height {}, but currently building block for height {} (not > height). Cannot safely add late batches. SKIPPING to avoid fork. WARNING: {} batches will NOT be processed: {:?}",
                                    node_id,
                                    cert_digest,
                                    commit_round,
                                    height,
                                    last_height,
                                    current_builder.height,
                                    payload_len,
                                    batch_list
                                );
                            } else {
                                log::warn!(
                                    "[ANALYZE] Node ID {} received certificate {} round {} (height {}) same as last committed height {}, but already processed late batches from height {} in block {}. SKIPPING duplicate to avoid duplicate execution. WARNING: {} batches will NOT be processed: {:?}",
                                    node_id,
                                    cert_digest,
                                    commit_round,
                                    height,
                                    last_height,
                                    height,
                                    current_builder.height,
                                    payload_len,
                                    batch_list
                                );
                            }
                            continue;
                        }
                    } else {
                        log::warn!(
                            "[ANALYZE] Node ID {} received certificate {} round {} (height {}) same as last committed height {}, but currently building block for height {}. Skipping (empty payload).",
                            node_id,
                            cert_digest,
                            commit_round,
                            height,
                            last_height,
                            current_builder.height
                        );
                        continue;
                    }
                }
            } else {
                // Không có block đang xây dựng, block cho height này đã được finalize
                // Tạo block mới cho height + 1 để xử lý batch đến muộn
                let payload_len = certificate.header.payload.len();
                if payload_len > 0 {
                    // GIẢI PHÁP: Tạo block mới cho height + 1 để xử lý batch đến muộn
                    // Điều kiện an toàn: Certificate đã được commit (tất cả node đều thấy)
                    let next_height = height + 1;
                    log::warn!(
                        "[LATE BATCH HANDLING] Node ID {} received late certificate {} round {} (height {}) after block {} was finalized. Will create new block {} (height {}) to process {} batches. Certificate was committed, so all nodes will handle this the same way.",
                        node_id,
                        cert_digest,
                        commit_round,
                        height,
                        height,
                        next_height,
                        next_height,
                        payload_len
                    );

                    // Tạo block mới cho height + 1
                    let mut new_builder = BlockBuilder::new(epoch, next_height);
                    new_builder.late_batches_from_height.insert(height);
                    current_block = Some(new_builder);

                    // Tiếp tục xử lý batch bên dưới với height = next_height
                    // (sẽ được xử lý như batch của block mới)
                } else {
                    log::warn!(
                        "[ANALYZE] Node ID {} received certificate {} round {} (height {}) same as last committed height {}, but no block being built. Skipping (empty payload).",
                        node_id,
                        cert_digest,
                        commit_round,
                        height,
                        last_height
                    );
                    continue;
                }
            }
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
                // IMPROVED: Xử lý late batch trong block hiện tại nếu đã được quyết định xử lý
                // Kiểm tra xem có phải là late batch đã được quyết định xử lý không
                // Nếu đã có late batch từ height này trong block hiện tại, tiếp tục xử lý
                // (có thể là height + 1, +2, +3...)
                let is_late_batch_being_handled = builder_ref.height > height
                    && builder_ref.late_batches_from_height.contains(&height);

                if !is_late_batch_being_handled {
                    // Không phải late batch đang được xử lý - skip như bình thường
                    let payload_len = certificate.header.payload.len();
                    if payload_len > 0 {
                        let batch_list: Vec<String> = certificate
                            .header
                            .payload
                            .iter()
                            .map(|(digest, worker_id)| format!("{} (worker {})", digest, worker_id))
                            .collect();
                        log::warn!(
                            "[ANALYZE] Node ID {} encountered out-of-order certificate (round {}, height {}) already building height {}. SKIPPING certificate with {} batches: {:?}",
                            node_id,
                            commit_round,
                            height,
                            builder_ref.height,
                            payload_len,
                            batch_list
                        );
                    } else {
                        log::warn!(
                    "[ANALYZE] Node ID {} encountered out-of-order certificate (round {}, height {}) already building height {}. Skipping.",
                    node_id,
                    commit_round,
                    height,
                    builder_ref.height
                );
                    }
                    continue;
                }
                // Nếu là late batch đang được xử lý, tiếp tục xử lý bên dưới
            } else if builder_ref.height < height {
                if let Some(finished) = current_block.take() {
                    let leader_round = finished.height * 2;
                    let tx_count = finished.transactions.len();
                    if tx_count == 0 {
                        log::warn!(
                            "[ANALYZE] Node ID {} forcing flush of EMPTY unfinished block height {} (leader round {}) with {} transactions ({} certificates) due to new height {}. This may indicate batches were not processed correctly!",
                        node_id,
                        finished.height,
                        leader_round,
                            tx_count,
                            finished.certificate_count,
                        height
                    );
                    } else {
                        log::warn!(
                            "[ANALYZE] Node ID {} forcing flush of unfinished block height {} (leader round {}) containing {} transactions ({} certificates) due to new height {}.",
                            node_id,
                            finished.height,
                            leader_round,
                            tx_count,
                            finished.certificate_count,
                            height
                        );
                    }

                    // Track các batch và transaction đã được gửi để tránh duplicate
                    let batch_digests_to_mark = finished.batch_digests.clone();
                    let transaction_hashes_to_mark = finished.transaction_hashes_in_block.clone();
                    let tx_count = finished.transactions.len();
                    let batch_count = batch_digests_to_mark.len();
                    let leader_round_for_log = finished.height * 2;
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
                            "[BATCH TRACK] Node ID {} SUCCESSFULLY sent force-flush block height {} (leader round {}) to UDS containing {} transactions from {} batches. Batches: {:?}",
                            node_id,
                            finished.height,
                            leader_round_for_log,
                            tx_count,
                            batch_count,
                            batch_digests_to_mark
                        );
                        // Mark các batch đã được gửi tới UDS
                        for batch_digest in &batch_digests_to_mark {
                            processed_batches.insert(batch_digest.clone());
                            log::debug!(
                                "[BATCH TRACK] Node ID {} MARKED batch {} as PROCESSED (sent to UDS in force-flush block height {})",
                                node_id,
                                batch_digest,
                                finished.height
                            );
                        }
                        // Mark các transaction đã được gửi tới UDS
                        for tx_hash in transaction_hashes_to_mark {
                            processed_transactions.insert(tx_hash);
                        }
                    }
                }

                current_block = Some(BlockBuilder::new(epoch, height));
            }
        }

        let builder = current_block.as_mut().expect("Block builder must exist");

        builder.certificate_count += 1;

        let cert_digest = certificate.digest();
        let payload_len = certificate.header.payload.len();

        // Kiểm tra xem có phải là late batch không
        // Late batch là batch từ height cũ được thêm vào block có height cao hơn
        let is_late_batch =
            builder.late_batches_from_height.contains(&height) && builder.height > height;

        // Log chi tiết về payload của certificate
        if is_late_batch {
            let height_diff = builder.height - height;
            log::info!(
                "[ANALYZE] Node ID {} adding LATE certificate {} (round {}, original height {}) with {} batch digests to block height {} ({} blocks ahead). This is a late batch being processed in a later block.",
                node_id,
                cert_digest,
                commit_round,
                height,
                payload_len,
                builder.height,
                height_diff
            );
        } else {
            log::info!(
            "[ANALYZE] Node ID {} adding certificate {} (round {}) with {} batch digests to block height {}",
            node_id,
            cert_digest,
            commit_round,
            payload_len,
            builder.height
        );
        }

        // Log danh sách batch digests trong payload
        if payload_len > 0 {
            let batch_list: Vec<String> = certificate
                .header
                .payload
                .iter()
                .map(|(digest, worker_id)| format!("{} (worker {})", digest, worker_id))
                .collect();
            log::info!(
                "[ANALYZE] Node ID {} certificate {} round {} payload contains {} batches: {:?}",
                node_id,
                cert_digest,
                commit_round,
                payload_len,
                batch_list
            );
        } else {
            log::info!(
                "[ANALYZE] Node ID {} certificate {} round {} has EMPTY payload (no batches)",
                node_id,
                cert_digest,
                commit_round
            );
        }

        // Track batches trong certificate này để log summary sau
        let mut batches_in_cert = certificate.header.payload.len();
        let mut batches_processed = 0usize;
        let mut batches_skipped_duplicate_in_block = 0usize;
        let mut batches_skipped_already_processed = 0usize;
        let mut batches_not_found = 0usize;
        let mut batches_failed = 0usize;

        for (batch_digest, worker_id) in certificate.header.payload.iter() {
            // CRITICAL: Kiểm tra duplicate batch trong cùng block trước
            if !builder.batch_hashes.insert(batch_digest.clone()) {
                batches_skipped_duplicate_in_block += 1;
                log::warn!(
                    "[BATCH TRACK] Node ID {} SKIP batch {} in certificate {} (round {}, height {}) - DUPLICATE within block height {} (already in block builder). This batch appears multiple times in the same certificate or was already added to this block.",
                    node_id,
                    batch_digest,
                    cert_digest,
                    commit_round,
                    height,
                    builder.height
                );
                continue;
            }

            // CRITICAL: Kiểm tra batch đã được xử lý trong blocks trước đó
            // Đây là cần thiết vì batch có thể xuất hiện trong nhiều certificates khác nhau
            // (do logic extract batches từ parent certificates)
            // Tuy nhiên, chỉ skip nếu batch đã được xử lý trong một block đã được finalize
            // (đảm bảo deterministic: tất cả nodes đều đã xử lý batch đó)
            if processed_batches.contains(batch_digest) {
                batches_skipped_already_processed += 1;
                // Batch đã được xử lý trong một block trước đó
                // Skip để tránh duplicate execution
                log::warn!(
                    "[BATCH TRACK] Node ID {} SKIP batch {} in certificate {} (round {}, height {}) - ALREADY PROCESSED in a previous block (current block height: {}). This batch was already executed and sent to UDS in an earlier block. Skipping to avoid duplicate execution.",
                    node_id,
                    batch_digest,
                    cert_digest,
                    commit_round,
                    height,
                    builder.height
                );
                // Remove from batch_hashes vì đã skip
                builder.batch_hashes.remove(batch_digest);
                continue;
            }

            // Log bắt đầu xử lý batch
            log::info!(
                "[BATCH TRACK] Node ID {} PROCESSING batch {} from worker {} in certificate {} (round {}, height {}), current block height: {}",
                node_id,
                batch_digest,
                worker_id,
                cert_digest,
                commit_round,
                height,
                builder.height
            );

            match store.read(batch_digest.to_vec()).await {
                Ok(Some(serialized_batch_message)) => {
                    log::info!(
                        "[BATCH TRACK] Node ID {} FOUND batch {} from worker {} in store ({} bytes, certificate: {}, round {}, height {}, block height: {}).",
                        node_id,
                        batch_digest,
                        worker_id,
                        serialized_batch_message.len(),
                        cert_digest,
                        commit_round,
                        height,
                        builder.height
                    );
                    match bincode::deserialize::<WorkerMessage>(&serialized_batch_message) {
                        Ok(WorkerMessage::Batch(batch)) => {
                            let batch_tx_count = batch.len();
                            if batch.is_empty() {
                                log::warn!(
                                    "[BATCH PROCESSING] Batch {} from worker {} decoded with 0 transactions (height {}).",
                                    batch_digest,
                                    worker_id,
                                    builder.height
                                );
                            } else {
                                if is_late_batch {
                                    let height_diff = builder.height - height;
                                    log::info!(
                                        "[BATCH PROCESSING] LATE Batch {} (original height {}) contains {} transactions, adding to block height {} ({} blocks ahead)",
                                        batch_digest,
                                        height,
                                        batch_tx_count,
                                        builder.height,
                                        height_diff
                                );
                                } else {
                                    log::info!(
                                    "[BATCH PROCESSING] Batch {} contains {} transactions, adding to block height {}",
                                    batch_digest,
                                    batch_tx_count,
                                    builder.height
                                );
                                }
                            }

                            for (tx_idx, tx_data) in batch.into_iter().enumerate() {
                                // Cắt bỏ 8 byte đầu tiên (độ dài message)
                                const LENGTH_PREFIX_SIZE: usize = 8;
                                let tx_payload = if tx_data.len() > LENGTH_PREFIX_SIZE {
                                    let payload = tx_data[LENGTH_PREFIX_SIZE..].to_vec();

                                    // Log mẫu cho 2 transactions đầu
                                    if tx_idx < 2 {
                                        let tx_hex = hex::encode(&payload);
                                        log::info!(
                                            "[BATCH PROCESSING] Batch {} tx[{}]: original {} bytes -> after strip {} bytes, hex={}",
                                            batch_digest,
                                            tx_idx,
                                            tx_data.len(),
                                            payload.len(),
                                            if payload.len() <= 64 {
                                                tx_hex
                                            } else {
                                                format!("{}...", &tx_hex[..128])
                                            }
                                        );
                                    }

                                    payload
                                } else {
                                    log::warn!(
                                        "[BATCH PROCESSING] Transaction in batch {} has only {} bytes, cannot strip 8-byte length prefix. Keeping as-is.",
                                        batch_digest,
                                        tx_data.len()
                                    );
                                    tx_data
                                };

                                // Tính hash của transaction để kiểm tra duplicate
                                // Sử dụng hash của transaction payload (sau khi strip length prefix)
                                use sha3::{Digest as Sha3Digest, Keccak256};
                                let tx_hash = Keccak256::digest(&tx_payload).to_vec();

                                // Kiểm tra duplicate transaction trong cùng block
                                if !builder.transaction_hashes.insert(tx_hash.clone()) {
                                    // Transaction đã tồn tại trong block này - skip để tránh duplicate
                                    let tx_hash_hex = hex::encode(&tx_hash);
                                    log::warn!(
                                        "[DUPLICATE TX DETECTION] Node ID {} detected DUPLICATE transaction {} in batch {} tx[{}] (height {}). Skipping to avoid duplicate execution. Transaction may appear in multiple batches.",
                                        node_id,
                                        tx_hash_hex,
                                        batch_digest,
                                        tx_idx,
                                        builder.height
                                    );
                                    continue; // Skip transaction này
                                }

                                // NOTE: Không sử dụng processed_transactions để skip transaction vì nó là local state
                                // có thể khác nhau giữa các node, gây fork. Thay vào đó, chỉ dựa vào
                                // transaction_hashes trong BlockBuilder để đảm bảo deterministic.
                                // processed_transactions chỉ dùng để track sau khi gửi, không dùng để quyết định skip.
                                //
                                // Nếu transaction xuất hiện trong nhiều block khác nhau (do batch được commit ở nhiều round),
                                // tất cả node sẽ xử lý transaction trong cùng block (vì certificate đã commit).
                                // Việc duplicate transaction giữa các block sẽ được xử lý ở execution layer (application layer).

                                // Log từng transaction với hash và các thông tin chi tiết
                                tx_logger::parse_and_log_transaction(
                                    &tx_payload,
                                    batch_digest,
                                    tx_idx,
                                    *worker_id as u32,
                                    builder.height,
                                );

                                builder.transactions.push(comm::Transaction {
                                    digest: tx_payload,
                                    worker_id: *worker_id as u32,
                                });

                                // Track transaction hash để mark as processed sau khi gửi block
                                builder.transaction_hashes_in_block.push(tx_hash);
                            }

                            // Track batch digest để mark as processed sau khi gửi block
                            builder.batch_digests.push(batch_digest.clone());
                            batches_processed += 1;

                            log::info!(
                                "[BATCH TRACK] Node ID {} COMPLETED processing batch {} from worker {} (certificate: {}, round {}, height {}, block height: {}). Batch contains {} transactions. Total transactions in block so far: {}",
                                node_id,
                                batch_digest,
                                worker_id,
                                cert_digest,
                                commit_round,
                                height,
                                builder.height,
                                batch_tx_count,
                                builder.transactions.len()
                            );
                        }
                        Ok(_) => {
                            batches_failed += 1;
                            log::warn!(
                                "[BATCH TRACK] Node ID {} FAILED to deserialize batch {} from worker {} in certificate {} (round {}, height {}, block height: {}). Digest did not correspond to a Batch message.",
                                node_id,
                                batch_digest,
                                worker_id,
                                cert_digest,
                                commit_round,
                                height,
                                builder.height
                            );
                        }
                        Err(e) => {
                            batches_failed += 1;
                            log::error!(
                                "[BATCH TRACK] Node ID {} ERROR deserializing batch {} from worker {} in certificate {} (round {}, height {}, block height: {}): {}",
                                node_id,
                                batch_digest,
                                worker_id,
                                cert_digest,
                                commit_round,
                                height,
                                builder.height,
                                e
                            );
                        }
                    }
                }
                Ok(None) => {
                    batches_not_found += 1;
                    log::warn!(
                        "[BATCH TRACK] Node ID {} NOT FOUND batch {} from worker {} in certificate {} (round {}, height {}, block height: {}). Batch not found in store - may have been garbage collected or not received. WARNING: Transactions in this batch will be MISSING from the block!",
                        node_id,
                        batch_digest,
                        worker_id,
                        cert_digest,
                        commit_round,
                        height,
                        builder.height
                    );
                }
                Err(e) => {
                    batches_not_found += 1;
                    log::error!(
                        "[BATCH TRACK] Node ID {} ERROR reading batch {} from worker {} in certificate {} (round {}, height {}, block height: {}) from store: {}",
                        node_id,
                        batch_digest,
                        worker_id,
                        cert_digest,
                        commit_round,
                        height,
                        builder.height,
                        e
                    );
                }
            }
        }

        // Log summary về batches trong certificate này
        if batches_in_cert > 0 {
            log::info!(
                "[BATCH TRACK] Node ID {} CERTIFICATE SUMMARY: certificate {} (round {}, height {}, block height: {}) contains {} batches: {} processed, {} skipped (duplicate in block), {} skipped (already processed), {} not found, {} failed",
                node_id,
                cert_digest,
                commit_round,
                height,
                builder.height,
                batches_in_cert,
                batches_processed,
                batches_skipped_duplicate_in_block,
                batches_skipped_already_processed,
                batches_not_found,
                batches_failed
            );
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
