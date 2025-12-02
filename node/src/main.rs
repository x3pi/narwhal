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
// use env_logger::Env;
use tracing_subscriber::{fmt, EnvFilter};
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

#[macro_use]
mod logger;
// Sử dụng logger module trực tiếp

/// The default channel capacity.
// Giảm capacity xuống một nửa để phát hiện lỗi sớm hơn (thay vì chờ 2 tiếng)
// Nếu channel đầy, lỗi sẽ xuất hiện nhanh hơn → dễ debug hơn
pub const CHANNEL_CAPACITY: usize = 5_000;
const CONSENSUS_STATE_KEY: &[u8] = b"consensus_state";

async fn fetch_validators_via_uds(socket_path: &str, block_number: u64) -> Result<ValidatorInfo> {
    log::info!(
        "[NODE] Đang lấy danh sách validator cho block {} qua UDS {}",
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
    .map_err(|e| anyhow::anyhow!("Không thể connect to UDS path '{}': {}", socket_path, e))?;

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
    .map_err(|e| anyhow::anyhow!("Không thể write request length to UDS: {}", e))?;

    tokio::time::timeout(
        Duration::from_millis(UDS_RW_TIMEOUT_MS),
        stream.write_all(&request_bytes),
    )
    .await
    .context(format!("UDS write timeout ({}ms)", UDS_RW_TIMEOUT_MS))?
    .map_err(|e| anyhow::anyhow!("Không thể write request payload to UDS: {}", e))?;

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
    .map_err(|e| anyhow::anyhow!("Không thể read response length from UDS: {}", e))?;

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
    .map_err(|e| anyhow::anyhow!("Không thể read response payload from UDS: {}", e))?;

    let wrapped_response = validator::Response::decode(&response_buf[..])
        .context("Không thể decode validator Response protobuf")?;

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
                    "Không thể deserialize consensus state: {}. Sử dụng round 0 làm mặc định.",
                    e
                );
                None
            }
        },
        Ok(None) => None,
        Err(e) => {
            log::error!(
                "Không thể đọc consensus state từ store: {}. Sử dụng round 0 làm mặc định.",
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
                            "[NODE] Đã tải committee cho epoch {} từ UDS (block {}).",
                            epoch,
                            block_number
                        );
                        return Ok(committee);
                    }
                    Err(e) => {
                        log::error!(
                            "[NODE] Không thể parse dữ liệu committee từ UDS: {}. Đang thử lại sau {} ms...",
                            e,
                            RETRY_DELAY_MS
                        );
                    }
                }
            }
            Err(e) => {
                log::error!(
                    "[NODE] Không thể lấy danh sách validator cho block {}: {}. Đang thử lại sau {} ms...",
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
        log::info!("[New Branch] Đang tải committee từ file: {}", filename);
        let mut committee =
            Committee::import(filename).context("Không thể load committee from file")?;
        if committee.epoch == 0 {
            committee.epoch = 1;
            log::info!("File committee có epoch 0, đặt thành 1.");
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
            "[NODE] Không có file committee. Đang lấy qua UDS cho epoch {} (block {}).",
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

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level));

    // Cấu hình logging dạng JSON để dễ dàng trace và query
    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(filter)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .json()
        .finish(); // Output JSON format

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => NodeConfig::new()
            .export(sub_matches.value_of("filename").unwrap())
            .context("Không thể generate key pair")?,
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
        NodeConfig::import(key_file).context("Không thể load the node's configuration")?;

    log::info!(
        "Địa chỉ node config: {:?}",
        node_config.name.to_eth_address()
    );
    log::info!(
        "Public key secp của node config (hex): {}",
        hex::encode(node_config.name.as_ref())
    );
    log::info!(
        "Public key consensus của node config (hex): {}",
        hex::encode(node_config.consensus_key.as_bytes())
    );

    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Không thể load the node's parameters")?
        }
        None => Parameters::default(),
    };

    let store = Store::new(store_path).context("Không thể create a store")?;
    let mut store_for_committee = store.clone();
    let committee_file = matches.value_of("committee");
    let committee = load_initial_committee(committee_file, &mut store_for_committee, &node_config)
        .await
        .context("Không thể initialize committee")?;

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
            log::warn!("Không thể encode TransactionHashData: {}", e);
            return Vec::new();
        }

        let hash = Keccak256::digest(&buf);
        hash.to_vec()
    }

    /// Parse và log transaction từ payload
    /// Tính hash từ TransactionHashData (protobuf encoded) để đảm bảo khớp với Go
    pub fn parse_and_log_transaction(
        payload: &[u8],
        batch_digest: &crypto::Digest,
        tx_idx: usize,
        worker_id: u32,
        height: u64,
    ) {
        // Thử parse như Transaction
        match Transaction::decode(payload) {
            Ok(tx) => {
                // Tính hash từ TransactionHashData (protobuf encoded) - thống nhất với Go
                let transaction_hash = calculate_transaction_hash(&tx);
                let hash_hex = hex::encode(&transaction_hash);

                let from_hex = hex::encode(&tx.from_address);
                let to_hex = hex::encode(&tx.to_address);
                let amount_hex = hex::encode(&tx.amount);
                let batch_id_str = format!("{}", batch_digest);

                // Sử dụng hệ thống log mới
                crate::logger::log_giao_dich_chi_tiet(
                    &hash_hex,
                    &from_hex,
                    &to_hex,
                    &amount_hex,
                    height,
                    tx_idx,
                    worker_id,
                    &batch_id_str,
                );
            }
            Err(e) => {
                // Nếu parse failed, không thể tính hash từ TransactionHashData
                // Fallback: tính hash từ raw payload (không khớp với Go)
                use sha3::{Digest as Sha3Digest, Keccak256};
                let transaction_hash = Keccak256::digest(payload).to_vec();
                let hash_hex = hex::encode(&transaction_hash);

                log_canh_bao!(
                    target: "tx_parse_error",
                    batch_id = %batch_digest,
                    tx_index = tx_idx,
                    height = height,
                    tx_hash = %hash_hex,
                    error = %e,
                    "⚠️ Không thể parse giao dịch, sử dụng hash từ raw payload"
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
            "[ANALYZE] Node ID {} đang thử kết nối đến {}",
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
                        "[ANALYZE] Node ID {} đã kết nối thành công đến {} ở lần thử {}",
                        node_id,
                        socket_path,
                        attempt
                    );
                    stream = Some(s);
                    break;
                }
                Err(e) => {
                    log::warn!(
                        "[ANALYZE] Node ID {}: Lần thử kết nối {}/{} đến {} thất bại: {}",
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
                "[ANALYZE] Node ID {} không thể kết nối đến {} sau {} lần thử. Sẽ tiếp tục xử lý certificates mà không gửi qua UDS.",
                node_id,
                socket_path,
                MAX_CONNECT_ATTEMPTS
            );
        }

        stream
    } else {
        log::info!(
            "[ANALYZE] Node ID {} không có block socket được cấu hình; các block đã commit sẽ không được gửi qua UDS.",
            node_id
        );
        None
    };

    log::info!(
        "[ANALYZE] Node ID {} đang vào vòng lặp để chờ các block đã commit.",
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
        // Track tổng số batches từ tất cả certificates trong block (để debug block rỗng)
        total_batches_in_certificates: usize,
        total_batches_processed: usize,
        total_batches_skipped_duplicate: usize,
        total_batches_skipped_already_processed: usize,
        total_batches_not_found: usize,
        total_batches_failed: usize,
        // Track chi tiết batches bị skip để log khi block rỗng
        skipped_batches_duplicate: Vec<(Digest, u32, String)>, // (batch_digest, worker_id, certificate_digest)
        skipped_batches_already_processed: Vec<(Digest, u32, String)>, // (batch_digest, worker_id, certificate_digest)
        not_found_batches: Vec<(Digest, u32, String)>, // (batch_digest, worker_id, certificate_digest)
        failed_batches: Vec<(Digest, u32, String, String)>, // (batch_digest, worker_id, certificate_digest, error)
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
                total_batches_in_certificates: 0,
                total_batches_processed: 0,
                total_batches_skipped_duplicate: 0,
                total_batches_skipped_already_processed: 0,
                total_batches_not_found: 0,
                total_batches_failed: 0,
                skipped_batches_duplicate: Vec::new(),
                skipped_batches_already_processed: Vec::new(),
                not_found_batches: Vec::new(),
                failed_batches: Vec::new(),
            }
        }
    }

    async fn send_blocks(
        stream: &mut UnixStream,
        blocks: Vec<comm::CommittedBlock>,
        _node_id: usize,
    ) -> Result<(), String> {
        if blocks.is_empty() {
            return Ok(());
        }

        // Bỏ log chi tiết từng transaction - chỉ log khi gửi block thành công ở level cao hơn

        let epoch_data = comm::CommittedEpochData { blocks };

        let mut proto_buf = BytesMut::new();
        epoch_data
            .encode(&mut proto_buf)
            .map_err(|e| format!("Không thể encode Protobuf: {}", e))?;

        let mut len_buf = BytesMut::new();
        put_uvarint_to_bytes_mut(&mut len_buf, proto_buf.len() as u64);

        // Gửi dữ liệu đến UDS (không log chi tiết để giảm spam)
        stream
            .write_all(&len_buf)
            .await
            .map_err(|e| format!("Không thể write length to socket: {}", e))?;

        stream
            .write_all(&proto_buf)
            .await
            .map_err(|e| format!("Không thể write payload to socket: {}", e))?;

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
                send_blocks(stream, blocks.clone(), node_id).await?;
                // Log khi gửi block thành công sang UDS - đây là log quan trọng để trace
                for block in &blocks {
                    // Log block được tạo và gửi sang UDS
                    log::info!(
                        target: "narwhal_audit",
                        "[BLOCK CREATED AND SENT TO UDS] Node ID {} đã tạo và gửi block height {} (epoch {}) đến UDS để thực thi: {} transactions",
                        node_id,
                        block.height,
                        block.epoch,
                        block.transactions.len()
                    );
                    
                    // Log từng transaction được gửi sang UDS (lúc vào executor)
                    for (tx_idx, tx) in block.transactions.iter().enumerate() {
                        use sha3::{Digest as Sha3Digest, Keccak256};
                        let tx_hash = Keccak256::digest(&tx.digest).to_vec();
                        let tx_hash_hex = hex::encode(&tx_hash);
                        log::info!(
                            target: "narwhal_audit",
                            "[TX SENT TO UDS] Node ID {} đã gửi transaction {} (index {}) trong block height {} đến UDS để thực thi",
                            node_id,
                            tx_hash_hex,
                            tx_idx,
                            block.height
                        );
                        log_tx_gui_executor!(&tx_hash_hex, block.height, tx_idx);
                    }
                }
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
        let cert_author = certificate.origin();

        // CRITICAL: Log khi nhận certificate từ consensus để gửi đến UDS
        log::info!(
            target: "narwhal_audit",
            "[NODE RECEIVED FROM CONSENSUS] Node ID {} received certificate {} (round {}, author: {}, {} batches) from consensus output. Certificate will be processed and sent to UDS.",
            node_id, cert_digest, commit_round, cert_author, payload_len
        );

        // Bỏ log chi tiết về certificate khi nhận - chỉ log khi có vấn đề hoặc khi gửi block thành công
        if payload_len == 0 {
            log::warn!(
                "[ANALYZE] Node ID {} đã nhận certificate {} cho round {} (epoch {}) từ consensus với payload RỖNG (0 batches).",
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
                    target: "narwhal_audit",
                    "[LATE BATCH DETECTION] ⚠️ Node ID {} PHÁT HIỆN SỚM: Certificate {} round {} (height {}) đến MUỘN! Last committed: {}, Đang xây dựng: {}. {} batches sẽ bị BỎ QUA: {:?}",
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
                        target: "narwhal_audit",
                        "[LATE BATCH DETAIL] Batch {} từ worker {} (height {}) sẽ bị BỎ QUA do đến muộn",
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
                    let tx_count = finished.transactions.len();
                    
                    // Log block commit với hệ thống mới
                    log_block_commit!(finished.height, tx_count, finished.epoch);
                    
                    // Log chi tiết khi block rỗng để debug
                    if tx_count == 0 {
                        if !finished.batch_digests.is_empty() {
                            // Có batches tracked nhưng không có transactions - vấn đề thực sự
                            log::error!(
                                "[EMPTY BLOCK ERROR] Node ID {} block height {} có {} certificates và {} batches tracked nhưng 0 transactions. Batches không được xử lý đúng! Batch digests: {:?}",
                                node_id,
                                finished.height,
                                finished.certificate_count,
                                finished.batch_digests.len(),
                                finished.batch_digests.iter().take(10).collect::<Vec<_>>()
                            );
                        } else {
                            // Không có batches tracked - có thể là fake block hoặc tất cả batches bị skip
                            // Log chi tiết về từng batch bị skip để debug
                            let mut detail_msg = format!(
                                "[EMPTY BLOCK DETAIL] Node ID {} block height {} có {} certificates nhưng 0 transactions và 0 batches tracked.\n",
                                node_id,
                                finished.height,
                                finished.certificate_count
                            );
                            detail_msg.push_str(&format!(
                                "Tổng batches trong certificates: {}, Processed: {}, Skipped (duplicate): {}, Skipped (đã xử lý): {}, Not found: {}, Failed: {}\n",
                                finished.total_batches_in_certificates,
                                finished.total_batches_processed,
                                finished.total_batches_skipped_duplicate,
                                finished.total_batches_skipped_already_processed,
                                finished.total_batches_not_found,
                                finished.total_batches_failed
                            ));
                            
                            if !finished.skipped_batches_duplicate.is_empty() {
                                detail_msg.push_str(&format!(
                                    "Batches bị skip (duplicate trong block) - {} batches: ",
                                    finished.skipped_batches_duplicate.len()
                                ));
                                for (digest, worker_id, cert) in finished.skipped_batches_duplicate.iter().take(10) {
                                    detail_msg.push_str(&format!("batch {} (worker {}, cert {}), ", digest, worker_id, cert));
                                }
                                detail_msg.push_str("\n");
                            }
                            
                            if !finished.skipped_batches_already_processed.is_empty() {
                                detail_msg.push_str(&format!(
                                    "Batches bị skip (đã xử lý trước đó) - {} batches: ",
                                    finished.skipped_batches_already_processed.len()
                                ));
                                for (digest, worker_id, cert) in finished.skipped_batches_already_processed.iter().take(10) {
                                    detail_msg.push_str(&format!("batch {} (worker {}, cert {}), ", digest, worker_id, cert));
                                }
                                detail_msg.push_str("\n");
                            }
                            
                            if !finished.not_found_batches.is_empty() {
                                detail_msg.push_str(&format!(
                                    "Batches không tìm thấy - {} batches: ",
                                    finished.not_found_batches.len()
                                ));
                                for (digest, worker_id, cert) in finished.not_found_batches.iter().take(10) {
                                    detail_msg.push_str(&format!("batch {} (worker {}, cert {}), ", digest, worker_id, cert));
                                }
                                detail_msg.push_str("\n");
                            }
                            
                            if !finished.failed_batches.is_empty() {
                                detail_msg.push_str(&format!(
                                    "Batches thất bại - {} batches: ",
                                    finished.failed_batches.len()
                                ));
                                for (digest, worker_id, cert, error) in finished.failed_batches.iter().take(10) {
                                    detail_msg.push_str(&format!("batch {} (worker {}, cert {}, error: {}), ", digest, worker_id, cert, error));
                                }
                                detail_msg.push_str("\n");
                            }
                            
                            detail_msg.push_str("Có thể là fake block (round chẵn không commit) hoặc tất cả batches bị skip.");
                            
                            log::warn!("{}", detail_msg);
                        }
                    }

                    // Track các batch và transaction đã được gửi để tránh duplicate
                    let batch_digests_to_mark = finished.batch_digests.clone();
                    let transaction_hashes_to_mark = finished.transaction_hashes_in_block.clone();
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
                            "[ANALYZE] FATAL: Node ID {} Không thể send blocks: {}",
                            node_id,
                            e
                        );
                        break;
                    } else if stream_opt.is_some() {
                        // Log batches được gửi sang UDS để thực thi
                        for batch_digest in &batch_digests_to_mark {
                            log_batch_gui_uds!(
                                &format!("{}", batch_digest),
                                finished.height,
                                node_id
                            );
                            
                            // CRITICAL: Log với target narwhal_audit khi batch được gửi đến UDS
                            // Đánh dấu batch đã được thực thi - có thể dùng để filter log sau này
                            log::info!(
                                target: "narwhal_audit",
                                "[BATCH SENT TO UDS] Node ID {} batch {} đã được gửi đến UDS để thực thi (block height {}). Batch đã hoàn thành lifecycle và sẽ được thực thi. Có thể filter log của batch này sau khi thực thi.",
                                node_id,
                                batch_digest,
                                finished.height
                            );
                            
                            processed_batches.insert(batch_digest.clone());
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
                            target: "narwhal_audit",
                            "[LATE BATCH HANDLING] Node ID {} đã nhận certificate muộn {} round {} (height {}) nhưng last committed height là {}. Sẽ xử lý {} batches trong block HIỆN TẠI {} (height {}, {} blocks phía trước) để tránh batch bị bỏ qua. Certificate đã được commit, nên tất cả nodes sẽ xử lý giống nhau (deterministic).",
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
                                "[ANALYZE] Node ID {} đã nhận certificate {} round {} (height {}) nhưng last committed height là {}, và đang xây dựng block cho height {} (không > height). Không thể an toàn thêm late batches. BỎ QUA để tránh fork. CẢNH BÁO: {} batches sẽ KHÔNG được xử lý: {:?}",
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
                                "[ANALYZE] Node ID {} đã nhận certificate {} round {} (height {}) nhưng last committed height là {}, và đã xử lý late batches từ height {} trong block {}. BỎ QUA duplicate để tránh duplicate execution. CẢNH BÁO: {} batches sẽ KHÔNG được xử lý: {:?}",
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
                        target: "narwhal_audit",
                        "[LATE BATCH HANDLING] Node ID {} đã nhận certificate muộn {} round {} (height {}) nhưng last committed height là {}. Sẽ tạo block mới {} (height {}) để xử lý {} batches. Certificate đã được commit, nên tất cả nodes sẽ xử lý giống nhau.",
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
                // payload rỗng - skip
                log::warn!(
                    "[ANALYZE] Node ID {} đã nhận certificate {} round {} (height {}) nhưng last committed height là {}. Bỏ qua để tránh duplicates (payload rỗng).",
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
                    // Bỏ log chi tiết - chỉ log khi có vấn đề
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
                                target: "narwhal_audit",
                                "[LATE BATCH HANDLING] Node ID {} đã nhận certificate muộn {} round {} (height {}) sau khi block đã được finalize. Sẽ xử lý {} batches trong block HIỆN TẠI (height {}, {} blocks phía trước) để tránh batch bị bỏ qua. Certificate đã được commit, nên tất cả nodes sẽ xử lý giống nhau (deterministic).",
                                node_id,
                                cert_digest,
                                commit_round,
                                height,
                                payload_len,
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
                                    "[ANALYZE] Node ID {} đã nhận certificate {} round {} (height {}) giống với last committed height {}, nhưng đang xây dựng block cho height {} (không > height). Không thể an toàn thêm late batches. BỎ QUA để tránh fork. CẢNH BÁO: {} batches sẽ KHÔNG được xử lý: {:?}",
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
                                    "[ANALYZE] Node ID {} đã nhận certificate {} round {} (height {}) giống với last committed height {}, nhưng đã xử lý late batches từ height {} trong block {}. BỎ QUA duplicate để tránh duplicate execution. CẢNH BÁO: {} batches sẽ KHÔNG được xử lý: {:?}",
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
                            "[ANALYZE] Node ID {} đã nhận certificate {} round {} (height {}) giống với last committed height {}, nhưng đang xây dựng block cho height {}. BỎ QUA (payload rỗng).",
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
                        target: "narwhal_audit",
                        "[LATE BATCH HANDLING] Node ID {} đã nhận certificate muộn {} round {} (height {}) sau khi block đã được finalize. Sẽ tạo block mới (height {}) để xử lý {} batches. Certificate đã được commit, nên tất cả nodes sẽ xử lý giống nhau.",
                        node_id,
                        cert_digest,
                        commit_round,
                        height,
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
                        "[ANALYZE] Node ID {} đã nhận certificate {} round {} (height {}) giống với last committed height {}, nhưng không có block đang được xây dựng. BỎ QUA (payload rỗng).",
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
                        "[ANALYZE] Node ID {} đang tạo block rỗng giả cho height thiếu {} (last committed {}).",
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
                        "[ANALYZE] LỖI NGHIÊM TRỌNG: Node ID {} không thể gửi các block thiếu: {}",
                        node_id,
                        e
                    );
                    break;
                }
                // Log khi gửi block thành công đã được thực hiện trong emit_blocks
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
                            "[ANALYZE] Node ID {} gặp certificate không đúng thứ tự (round {}, height {}) đang xây dựng height {}. BỎ QUA certificate với {} batches: {:?}",
                            node_id,
                            commit_round,
                            height,
                            builder_ref.height,
                            payload_len,
                            batch_list
                        );
                    } else {
                        log::warn!(
                    "[ANALYZE] Node ID {} gặp certificate không đúng thứ tự (round {}, height {}) đang xây dựng height {}. BỎ QUA.",
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
                            "[ANALYZE] Node ID {} buộc flush block RỖNG chưa hoàn thành height {} (leader round {}) với {} transactions ({} certificates) do height mới {}. Điều này có thể cho thấy batches không được xử lý đúng!",
                        node_id,
                        finished.height,
                        leader_round,
                            tx_count,
                            finished.certificate_count,
                        height
                    );
                    } else {
                        log::warn!(
                            "[ANALYZE] Node ID {} buộc flush block chưa hoàn thành height {} (leader round {}) chứa {} transactions ({} certificates) do height mới {}.",
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
                    // Bỏ các biến không dùng - log đã được thực hiện trong emit_blocks
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
                            "[ANALYZE] FATAL: Node ID {} Không thể send blocks: {}",
                            node_id,
                            e
                        );
                        break;
                    } else if stream_opt.is_some() {
                        // Log batches được gửi sang UDS để thực thi
                        for batch_digest in &batch_digests_to_mark {
                            log_batch_gui_uds!(
                                &format!("{}", batch_digest),
                                finished.height,
                                node_id
                            );
                            
                            // CRITICAL: Log với target narwhal_audit khi batch được gửi đến UDS
                            // Đánh dấu batch đã được thực thi - có thể dùng để filter log sau này
                            log::info!(
                                target: "narwhal_audit",
                                "[BATCH SENT TO UDS] Node ID {} batch {} đã được gửi đến UDS để thực thi (block height {}). Batch đã hoàn thành lifecycle và sẽ được thực thi. Có thể filter log của batch này sau khi thực thi.",
                                node_id,
                                batch_digest,
                                finished.height
                            );
                            
                            processed_batches.insert(batch_digest.clone());
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

        // Bỏ log chi tiết về certificate - chỉ log khi có vấn đề hoặc khi gửi block thành công

        // Log danh sách batch digests trong payload
        if payload_len > 0 {
            let batch_list: Vec<String> = certificate
                .header
                .payload
                .iter()
                .map(|(digest, worker_id)| format!("{} (worker {})", digest, worker_id))
                .collect();
            log::info!(
                "[ANALYZE] Node ID {} certificate {} round {} payload chứa {} batches: {:?}",
                node_id,
                cert_digest,
                commit_round,
                payload_len,
                batch_list
            );
        } else {
            log::info!(
                "[ANALYZE] Node ID {} certificate {} round {} has payload rỗng (no batches)",
                node_id,
                cert_digest,
                commit_round
            );
        }

        // Track batches để log chi tiết khi có vấn đề (đặc biệt khi block rỗng)
        let batches_in_cert = certificate.header.payload.len();
        builder.total_batches_in_certificates += batches_in_cert;
        let mut batches_processed = 0usize;
        let mut batches_skipped_duplicate_in_block = 0usize;
        let mut batches_skipped_already_processed = 0usize;
        let mut batches_not_found = 0usize;
        let mut batches_failed = 0usize;

        for (batch_digest, worker_id) in certificate.header.payload.iter() {
            // CRITICAL: Kiểm tra duplicate batch trong cùng block trước
            if !builder.batch_hashes.insert(batch_digest.clone()) {
                batches_skipped_duplicate_in_block += 1;
                builder.total_batches_skipped_duplicate += 1;
                builder.skipped_batches_duplicate.push((
                    batch_digest.clone(),
                    *worker_id as u32,
                    format!("{}", cert_digest)
                ));
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
                builder.total_batches_skipped_already_processed += 1;
                builder.skipped_batches_already_processed.push((
                    batch_digest.clone(),
                    *worker_id as u32,
                    format!("{}", cert_digest)
                ));
                // Batch đã được xử lý trong một block trước đó
                // Skip để tránh duplicate execution
                log::warn!(
                    "[BATCH TRACK] Node ID {} SKIP batch {} in certificate {} (round {}, height {}) - đã xử lý in a previous block (current block height: {}). This batch was already executed and sent to UDS in an earlier block. BỎ QUA để tránh duplicate execution.",
                    node_id,
                    batch_digest,
                    cert_digest,
                    commit_round,
                    height,
                    builder.height
                );
                
                // CRITICAL DEBUG: Log transactions trong batch bị skip để trace
                // Điều này giúp phát hiện tại sao transaction không được thực thi
                match store.read(batch_digest.to_vec()).await {
                    Ok(Some(batch_data)) => {
                        if let Ok(worker::WorkerMessage::Batch(batch)) = bincode::deserialize::<worker::WorkerMessage>(&batch_data) {
                            // Batch is Vec<Vec<u8>>, so iterate directly
                            for (tx_idx, tx_payload) in batch.iter().enumerate() {
                                use transaction::Transaction;
                                let tx_hash = match Transaction::decode(tx_payload.as_slice()) {
                                    Ok(tx) => tx_logger::calculate_transaction_hash(&tx),
                                    Err(_) => {
                                        use sha3::{Digest as Sha3Digest, Keccak256};
                                        Keccak256::digest(&tx_payload).to_vec()
                                    }
                                };
                                let tx_hash_hex = hex::encode(&tx_hash);
                                
                                // CRITICAL DEBUG: Log tất cả transactions trong batch bị skip để trace
                                log::warn!(
                                    target: "narwhal_audit",
                                    "[TX SKIP TRACE] Transaction {} was SKIPPED because batch {} was already processed in a previous block (current block height: {}). Transaction will NOT be executed!",
                                    tx_hash_hex,
                                    batch_digest,
                                    builder.height
                                );
                            }
                        }
                    }
                    _ => {}
                }
                
                // Remove from batch_hashes vì đã skip
                builder.batch_hashes.remove(batch_digest);
                continue;
            }

            // Bỏ log processing batch - chỉ log khi có lỗi hoặc khi gửi block thành công

            match store.read(batch_digest.to_vec()).await {
                Ok(None) => {
                    // Batch không tìm thấy trong store
                    batches_not_found += 1;
                    builder.total_batches_not_found += 1;
                    builder.not_found_batches.push((
                        batch_digest.clone(),
                        *worker_id as u32,
                        format!("{}", cert_digest)
                    ));
                    
                    // CRITICAL DEBUG: Log khi batch không tìm thấy - transactions trong batch sẽ không được thực thi
                    log::warn!(
                        target: "narwhal_audit",
                        "[BATCH NOT FOUND] Node ID {} cannot find batch {} in store (certificate {} round {} height {}). All transactions in this batch will NOT be executed!",
                        node_id,
                        batch_digest,
                        cert_digest,
                        commit_round,
                        height
                    );
                    
                    continue;
                }
                Ok(Some(serialized_batch_message)) => {
                    // Bỏ log FOUND batch - chỉ log khi có lỗi
                    match bincode::deserialize::<WorkerMessage>(&serialized_batch_message) {
                        Ok(WorkerMessage::Batch(batch)) => {
                            let batch_tx_count = batch.len();
                            if batch.is_empty() {
                                log_canh_bao!(
                                    target: "empty_batch",
                                    batch_id = %batch_digest,
                                    worker_id = worker_id,
                                    height = builder.height,
                                    "⚠️ Batch rỗng"
                                );
                            } else {
                                // Log batch xử lý
                                log_batch_xu_ly!(&format!("{}", batch_digest), batch_tx_count, builder.height);
                                
                                if is_late_batch {
                                    // Chỉ log batch đến muộn khi độ trễ > 10 blocks
                                    let height_diff = builder.height.saturating_sub(height);
                                    if height_diff > 10 {
                                        crate::logger::log_batch_muon(
                                            &format!("{}", batch_digest),
                                            height,
                                            builder.height,
                                            *worker_id as u32,
                                        );
                                    }
                                }
                                
                                // Log batch được commit thành công (sẽ được thêm vào block)
                                log_batch_commit!(
                                    &format!("{}", batch_digest),
                                    batch_tx_count,
                                    builder.height,
                                    *worker_id as u32
                                );
                            }

                            for (tx_idx, tx_data) in batch.into_iter().enumerate() {
                                // Cắt bỏ 8 byte đầu tiên (độ dài message)
                                const LENGTH_PREFIX_SIZE: usize = 8;
                                let tx_payload = if tx_data.len() > LENGTH_PREFIX_SIZE {
                                    let payload = tx_data[LENGTH_PREFIX_SIZE..].to_vec();

                                    // Không log chi tiết từng transaction để giảm spam

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
                                // Thống nhất với Go: Parse Transaction, tạo TransactionHashData, encode, rồi tính hash
                                use transaction::Transaction;
                                let tx_hash = match Transaction::decode(tx_payload.as_slice()) {
                                    Ok(tx) => {
                                        // Tính hash từ TransactionHashData (protobuf encoded) - thống nhất với Go
                                        tx_logger::calculate_transaction_hash(&tx)
                                    }
                                    Err(e) => {
                                        // Nếu parse failed, fallback: tính hash từ raw payload
                                        log::warn!(
                                            "[BATCH PROCESSING] Không thể parse Transaction in batch {} tx[{}] for hash calculation: {}. Using fallback hash from raw payload.",
                                            batch_digest,
                                            tx_idx,
                                            e
                                        );
                                        use sha3::{Digest as Sha3Digest, Keccak256};
                                        Keccak256::digest(&tx_payload).to_vec()
                                    }
                                };

                                // Kiểm tra duplicate transaction trong cùng block
                                let tx_hash_hex = hex::encode(&tx_hash);
                                if !builder.transaction_hashes.insert(tx_hash.clone()) {
                                    // Transaction đã tồn tại trong block này - skip để tránh duplicate
                                    crate::logger::log_tx_trung_lap(
                                        &tx_hash_hex,
                                        builder.height,
                                        &format!("{}", batch_digest),
                                    );
                                    
                                    // CRITICAL DEBUG: Log tất cả transactions bị skip do duplicate
                                    log::warn!(
                                        target: "narwhal_audit",
                                        "[TX SKIP TRACE] Transaction {} was SKIPPED as DUPLICATE in block height {}. Transaction already exists in this block!",
                                        tx_hash_hex,
                                        builder.height
                                    );
                                    
                                    continue; // Skip transaction này
                                }
                                
                                // CRITICAL DEBUG: Log tất cả transactions được thêm vào block để trace
                                log::info!(
                                    target: "narwhal_audit",
                                    "[TX TRACE] Transaction {} found in batch {} (worker {}, tx_idx {}) and added to block height {}",
                                    tx_hash_hex,
                                    batch_digest,
                                    worker_id,
                                    tx_idx,
                                    builder.height
                                );

                                // NOTE: Không sử dụng processed_transactions để skip transaction vì nó là local state
                                // có thể khác nhau giữa các node, gây fork. Thay vào đó, chỉ dựa vào
                                // transaction_hashes trong BlockBuilder để đảm bảo deterministic.
                                // processed_transactions chỉ dùng để track sau khi gửi, không dùng để quyết định skip.
                                //
                                // Nếu transaction xuất hiện trong nhiều block khác nhau (do batch được commit ở nhiều round),
                                // tất cả node sẽ xử lý transaction trong cùng block (vì certificate đã commit).
                                // Việc duplicate transaction giữa các block sẽ được xử lý ở execution layer (application layer).

                                // Log giao dịch được thêm vào block
                                let tx_hash_hex = hex::encode(&tx_hash);
                                tx_logger::parse_and_log_transaction(
                                    &tx_payload,
                                    batch_digest,
                                    tx_idx,
                                    *worker_id as u32,
                                    builder.height,
                                );

                                // Log giao dịch được thêm vào block với hệ thống mới
                                log_tx_them_vao_block!(&tx_hash_hex, builder.height, tx_idx);
                                
                                // Log transaction được commit thành công
                                log_tx_commit!(
                                    &tx_hash_hex,
                                    builder.height,
                                    tx_idx,
                                    &format!("{}", batch_digest)
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
                            builder.total_batches_processed += 1;
                            
                            // Log batch đã được commit và sẽ được gửi sang UDS
                            log::info!(
                                target: "narwhal_audit",
                                "[BATCH COMMITTED] Node ID {} batch {} (worker {}, {} transactions) đã được commit thành công vào block height {}. Sẽ được gửi sang UDS để thực thi.",
                                node_id,
                                batch_digest,
                                worker_id,
                                batch_tx_count,
                                builder.height
                            );

                            // Bỏ log chi tiết về batch processing - chỉ log khi có lỗi hoặc khi gửi block thành công
                        }
                        Ok(_) => {
                            batches_failed += 1;
                            builder.total_batches_failed += 1;
                            builder.failed_batches.push((
                                batch_digest.clone(),
                                *worker_id as u32,
                                format!("{}", cert_digest),
                                "Digest did not correspond to a Batch message".to_string()
                            ));
                            log::warn!(
                                "[BATCH TRACK] Node ID {} Không thể deserialize batch {} from worker {} in certificate {} (round {}, height {}, block height: {}). Digest did not correspond to a Batch message.",
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
                            builder.total_batches_failed += 1;
                            builder.failed_batches.push((
                                batch_digest.clone(),
                                *worker_id as u32,
                                format!("{}", cert_digest),
                                format!("{}", e)
                            ));
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
                    builder.total_batches_not_found += 1;
                    builder.not_found_batches.push((
                        batch_digest.clone(),
                        *worker_id as u32,
                        format!("{}", cert_digest)
                    ));
                    crate::logger::log_batch_khong_tim_thay(
                        &format!("{}", batch_digest),
                        *worker_id as u32,
                        builder.height,
                    );
                }
                        Err(e) => {
                            batches_not_found += 1;
                            builder.total_batches_not_found += 1;
                            builder.not_found_batches.push((
                                batch_digest.clone(),
                                *worker_id as u32,
                                format!("{}", cert_digest)
                            ));
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

        // Log chi tiết về batches trong certificate - đặc biệt quan trọng khi block rỗng
        if batches_in_cert > 0 {
            let total_skipped = batches_skipped_duplicate_in_block + batches_skipped_already_processed;
            let total_issues = batches_failed + batches_not_found;
            
            // Log chi tiết khi có vấn đề hoặc khi block có thể rỗng
            if total_issues > 0 || total_skipped > 0 || batches_processed == 0 {
                log::warn!(
                    "[BATCH DETAIL] Node ID {} certificate {} (round {}, height {}, block height: {}) - Tổng: {} batches, Xử lý: {}, Bỏ qua (duplicate trong block): {}, Bỏ qua (đã xử lý): {}, Không tìm thấy: {}, Thất bại: {}",
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
    }

    if let Some(current) = current_block.take() {
        let leader_round = current.height * 2;
        log::info!(
            "[ANALYZE] Node ID {} đang finalize block cho height {} (leader round {}) chứa {} unique transactions ({} certificates) trước khi shutdown.",
            node_id,
            current.height,
            leader_round,
            current.transactions.len(),
            current.certificate_count
        );

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
                "[ANALYZE] LỖI NGHIÊM TRỌNG: Node ID {} không thể gửi final block: {}",
                node_id,
                e
            );
        }
        // Bỏ log THÀNH CÔNG - log đã được thực hiện trong emit_blocks
    }

    log::warn!(
        "[ANALYZE] Node ID {} đã thoát khỏi vòng lặp nhận. Không còn block nào sẽ được xử lý.",
        node_id
    );
}

