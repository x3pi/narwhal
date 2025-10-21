// In node/src/main.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bincode;
use bytes::{BufMut, BytesMut};
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, NodeConfig, Parameters, WorkerId};
use config::{Validator, ValidatorInfo};
use consensus::Consensus;
use consensus::{Bullshark, ConsensusProtocol, ConsensusState, STATE_KEY};
use env_logger::Env;
use log::{error, info};
use primary::Core;
use primary::{Certificate, Primary};
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::RwLock;
use tokio::time::sleep;
use worker::{Worker, WorkerMessage};
// --- Thêm module validator ---
pub mod validator {
    include!(concat!(env!("OUT_DIR"), "/validator.rs"));
}
// SỬA ĐỔI: Thêm các import Protobuf cần thiết cho logic bọc Request/Response
// ----------------------------

pub mod comm {
    include!(concat!(env!("OUT_DIR"), "/comm.rs"));
}

pub const CHANNEL_CAPACITY: usize = 10_000;

/// Hàm mới để lấy và chuyển đổi danh sách validator qua Unix Domain Socket.
async fn fetch_validators_via_uds(socket_path: &str, block_number: u64) -> Result<ValidatorInfo> {
    log::info!(
        "Attempting to fetch validator list for block {} from UDS: {}",
        block_number,
        socket_path
    );

    // 1. Kết nối UDS
    let mut stream = tokio::net::UnixStream::connect(socket_path)
        .await
        .context(format!("Failed to connect to UDS path '{}'", socket_path))?;

    // 2. TẠO VÀ BỌC BlockRequest trong Request (SỬA ĐỔI)
    let block_req = validator::BlockRequest { block_number };
    let request = validator::Request {
        payload: Some(validator::request::Payload::BlockRequest(block_req)),
    };

    let request_bytes = request.encode_to_vec();
    let request_len = request_bytes.len() as u32;

    // 3. Gửi độ dài (4 bytes, big-endian)
    stream
        .write_all(&request_len.to_be_bytes())
        .await
        .context("Failed to write request length to UDS")?;

    // 4. Gửi data Protobuf của Request đã bọc (SỬA ĐỔI)
    stream
        .write_all(&request_bytes)
        .await
        .context("Failed to write request payload to UDS")?;

    // 5. Đọc độ dài của response (4 bytes, big-endian)
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("Failed to read response length from UDS. Connection likely closed.")?;

    let response_len = u32::from_be_bytes(len_buf) as usize;

    // 6. Đọc data Protobuf của response đã bọc
    let mut response_buf = vec![0u8; response_len];
    stream
        .read_exact(&mut response_buf)
        .await
        .context("Failed to read response payload from UDS")?;

    // 7. Giải mã Protobuf Response đã bọc (SỬA ĐỔI)
    let wrapped_response = validator::Response::decode(&response_buf[..])
        .context("Failed to decode wrapped Response Protobuf")?;

    // 8. Giải bọc (unwrap) ValidatorList (SỬA ĐỔI)
    let proto_list = match wrapped_response.payload {
        Some(validator::response::Payload::ValidatorList(list)) => {
            log::info!("Successfully received ValidatorList payload.");
            list
        }
        Some(p) => {
            return Err(anyhow::anyhow!(
                "Received unexpected response payload type: {:?}",
                p
            ))
        }
        None => {
            return Err(anyhow::anyhow!(
                "Received empty response payload from UDS server"
            ))
        }
    };

    // 9. Chuyển đổi Protobuf sang cấu trúc nội bộ (ValidatorInfo)
    let mut wrapper_list = ValidatorInfo::default();
    for proto_val in proto_list.validators {
        // SỬA ĐỔI: Thêm hai trường pubkey mới vào quá trình khởi tạo.
        // LƯU Ý: Điều này yêu cầu cấu trúc `config::Validator` cũng phải có
        // hai trường `pubkey_bls` và `pubkey_secp`.
        let wrapper_val = Validator {
            address: proto_val.address,
            primary_address: proto_val.primary_address,
            worker_address: proto_val.worker_address,
            p2p_address: proto_val.p2p_address,
            total_staked_amount: proto_val.total_staked_amount,
            pubkey_bls: proto_val.pubkey_bls,   // <-- TRƯỜNG MỚI
            pubkey_secp: proto_val.pubkey_secp, // <-- TRƯỜNG MỚI
        };
        wrapper_list.validators.push(wrapper_val);
    }

    Ok(wrapper_list)
}

/// Hàm mới để tải toàn bộ ConsensusState từ Store.
/// Hàm này mô phỏng logic `load_state` bạn cung cấp.
async fn load_consensus_state(store: &mut Store) -> ConsensusState {
    match store.read(STATE_KEY.to_vec()).await {
        // Lỗi E0596 đã được sửa
        Ok(Some(bytes)) => {
            match bincode::deserialize::<ConsensusState>(&bytes) {
                Ok(state) => {
                    info!(
                        "Loaded consensus state from store. Last committed round: {}",
                        state.last_committed_round
                    );
                    state
                }
                Err(e) => {
                    error!("Failed to deserialize consensus state: {}. Starting from genesis (Round 0).", e);
                    ConsensusState::default()
                }
            }
        }
        Ok(None) => {
            info!("No consensus state found in store. Starting from genesis (Round 0).");
            ConsensusState::default()
        }
        Err(e) => {
            error!(
                "Failed to read from store: {:?}. Starting from genesis (Round 0).",
                e
            );
            ConsensusState::default()
        }
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
                .args_from_usage("--committee=[FILE] 'The file containing committee information (Optional)'")
                .args_from_usage(
                    "--uds-socket=[PATH] 'Unix Domain Socket path to fetch committee (Required if --committee is absent)'",
                )
                // THAY ĐỔI: Loại bỏ --block-number
                // .args_from_usage(
                //     "--block-number=[INT] 'The block number to fetch (Required if --committee is absent)'",
                // )
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

async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee");
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    let node_config = NodeConfig::import(key_file).context("Failed to load the node's keypair")?;

    let address = node_config.name.to_eth_address();

    log::info!("address: {}", address);
    let name = crypto::base64_to_hex(&node_config.name.encode_base64());

    log::info!("name: {:?}", name);

    let secret: std::result::Result<String, anyhow::Error> =
        crypto::base64_to_hex(&node_config.secret.encode_base64());
    log::info!("secret: {:?}", secret);
    let consensus_key = crypto::base64_to_hex(&node_config.consensus_key.to_string());
    log::info!("consensus_key: {:?}", consensus_key);
    let consensus_secret = crypto::base64_to_hex(&node_config.consensus_secret.encode_base64());

    log::info!("consensus_secret: {:?}", consensus_secret);

    // KHỞI TẠO STORE SỚM VÀ CẦN PHẢI LÀ MUTABLE ĐỂ load_consensus_state VÀ analyze SỬ DỤNG
    let mut store = Store::new(store_path).context("Failed to create a store")?;

    let always_false = false;
    let committee = if always_false && committee_file.is_some() {
        let filename = committee_file.unwrap();
        // TẢI TỪ FILE: Nếu --committee được cung cấp
        log::info!("Loading committee from file: {}", filename);
        Committee::import(filename).context("Failed to load the committee information from file")?
    } else {
        // FETCH TỪ UDS: Nếu --committee KHÔNG được cung cấp
        log::info!("--committee not provided. Attempting to fetch validator list via Unix Socket.");

        let socket_path = matches
            .value_of("uds-socket")
            .context("Must provide --uds-socket if --committee file is absent")?;

        // LẤY BLOCK NUMBER TỪ CONSENSUS STATE
        // SỬA LỖI: Truyền tham chiếu thay đổi (mutable reference)
        let consensus_state = load_consensus_state(&mut store).await;
        let block_number =
            Core::calculate_last_reconfiguration_round(consensus_state.last_committed_round);
        log::info!(
            "Using last committed round {} :: {} as block number for fetching committee.",
            block_number,
            consensus_state.last_committed_round
        );

        // Logic retry cho UDS
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY_MS: u64 = 1000; // Tăng thời gian chờ lên 1s cho kết nối

        let mut validator_info = None;
        for attempt in 1..=MAX_RETRIES {
            match fetch_validators_via_uds(socket_path, block_number).await {
                Ok(info) => {
                    validator_info = Some(info);
                    log::info!("Successfully fetched validator info from UDS.");
                    break;
                }
                Err(e) => {
                    log::warn!(
                        "Attempt {}/{} to fetch validators failed via UDS: {}. Retrying in {}ms...",
                        attempt,
                        MAX_RETRIES,
                        e,
                        RETRY_DELAY_MS
                    );
                    sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                }
            }
        }

        let validator_info = validator_info.context(format!(
            "Failed to fetch validator info after {} attempts",
            MAX_RETRIES
        ))?;

        log::info!("validator_info {:?}", validator_info.validators);

        Committee::from_validator_info(validator_info, &address)
            .context("Failed to create committee from validator info")?
    };

    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // Store đã được khởi tạo ở trên
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    log::info!("committee {:?}", committee);

    match matches.subcommand() {
        ("primary", _) => {
            let _committee_arc = Arc::new(RwLock::new(committee.clone())); // Sửa cảnh báo unused variable

            let mut primary_keys: Vec<_> = committee.authorities.keys().cloned().collect();
            primary_keys.sort();

            let node_id = primary_keys
                .iter()
                .position(|pk| pk == &node_config.name)
                .context("Public key không tìm thấy trong committee")?;

            log::info!("Node {} khởi chạy với ID: {}", node_config.name, node_id);

            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);

            tokio::spawn(Primary::spawn(
                node_config.clone(),
                committee.clone(),
                parameters.clone(),
                store.clone(),
                tx_new_certificates,
                rx_feedback,
            ));

            Consensus::spawn(
                committee.clone(),
                parameters.gc_depth,
                store.clone(),
                rx_new_certificates,
                tx_feedback,
                tx_output,
                ConsensusProtocol::Bullshark(Bullshark::new(committee, parameters.gc_depth)),
            );

            // SỬA LỖI: Truyền store là mutable reference
            analyze(rx_output, node_id, store, node_config).await;
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
                store, // store được chuyển ownership cho Worker
            ));
        }
        _ => unreachable!(),
    }

    let (_tx, mut rx) = channel::<()>(1); // Sửa cảnh báo unused variable
    rx.recv().await;

    unreachable!();
}

async fn analyze(
    mut rx_output: Receiver<Certificate>,
    node_id: usize,
    mut store: Store,
    node_config: NodeConfig,
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
    // node_config.uds_block_path // Dòng này có thể bị xóa

    // Thêm kiểm tra điều kiện cho uds_block_path
    if !node_config.uds_block_path.is_empty() {
        let socket_path = node_config.uds_block_path; // Sử dụng đường dẫn từ node_config
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

        while let Some(certificate) = rx_output.recv().await {
            // ... existing code ...
            log::info!(
                "[ANALYZE] Node ID {} RECEIVED certificate for round {} from consensus.",
                node_id,
                certificate.header.round
            );
            let mut all_transactions = Vec::new();

            for (digest, worker_id) in certificate.header.payload {
                match store.read(digest.to_vec()).await {
                    Ok(Some(serialized_batch_message)) => {
                        match bincode::deserialize(&serialized_batch_message) {
                            Ok(WorkerMessage::Batch(batch)) => {
                                log::debug!(
                                    "[ANALYZE] Unpacked batch {} with {} transactions for worker {}.",
                                    digest,
                                    batch.len(),
                                    worker_id
                                );
                                for tx_data in batch {
                                    all_transactions.push(comm::Transaction {
                                        digest: tx_data,
                                        worker_id: worker_id as u32,
                                    });
                                }
                            }
                            Ok(_) => {
                                log::warn!(
                                    "[ANALYZE] Digest {} did not correspond to a Batch message.",
                                    digest
                                );
                            }
                            Err(e) => {
                                log::error!(
                                    "[ANALYZE] Failed to deserialize message for digest {}: {}",
                                    digest,
                                    e
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        log::warn!("[ANALYZE] Batch for digest {} not found in store.", digest);
                    }
                    Err(e) => {
                        log::error!(
                            "[ANALYZE] Failed to read batch for digest {}: {}",
                            digest,
                            e
                        );
                    }
                }
            }

            let committed_block = comm::CommittedBlock {
                epoch: certificate.header.round,
                height: certificate.header.round,
                transactions: all_transactions,
            };

            let epoch_data = comm::CommittedEpochData {
                blocks: vec![committed_block],
            };

            log::debug!(
                "[ANALYZE] Node ID {} serializing data for round {}",
                node_id,
                certificate.header.round
            );
            let mut proto_buf = BytesMut::new();
            epoch_data
                .encode(&mut proto_buf)
                .expect("FATAL: Protobuf serialization failed!");

            let mut len_buf = BytesMut::new();
            put_uvarint_to_bytes_mut(&mut len_buf, proto_buf.len() as u64);

            if epoch_data.blocks.iter().all(|b| b.transactions.is_empty()) {
                log::info!(
                    "[ANALYZE] Node ID {} SENDING EMPTY BLOCK for round {}.",
                    node_id,
                    certificate.header.round
                );
            }

            log::info!("[ANALYZE] Node ID {} WRITING {} bytes (len) and {} bytes (data) to socket for round {}.", node_id, len_buf.len(), proto_buf.len(), certificate.header.round);

            if let Err(e) = stream.write_all(&len_buf).await {
                log::error!(
                    "[ANALYZE] FATAL: Node ID {}: Failed to write length to socket: {}",
                    node_id,
                    e
                );
                break;
            }

            if let Err(e) = stream.write_all(&proto_buf).await {
                log::error!(
                    "[ANALYZE] FATAL: Node ID {}: Failed to write payload to socket: {}",
                    node_id,
                    e
                );
                break;
            }

            log::info!(
                "[ANALYZE] SUCCESS: Node ID {} sent block for round {} successfully.",
                node_id,
                certificate.header.round
            );
        }

        log::warn!(
            "[ANALYZE] Node ID {} exited the receive loop. No more blocks will be processed.",
            node_id
        );
    } else {
        log::info!(
            "[ANALYZE] Node ID {}: uds_block_path is empty. Skipping socket_path_executor initialization.",
            node_id
        );
        // Nếu không có uds_block_path, có thể cần xử lý rx_output một cách khác
        // hoặc đơn giản là bỏ qua nếu không có logic nào khác phụ thuộc vào nó.
        // Ở đây, tôi chỉ bỏ qua và để rx_output tự động đóng khi hàm kết thúc.
    }
}
