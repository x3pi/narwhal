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
use consensus::CommittedSubDag;
use consensus::{Consensus, ConsensusState, STATE_KEY};
use crypto::Hash as _;
use env_logger::Env;
// THAY ĐỔI: Xóa `trace` không dùng
use log::{debug, error, info, warn};
// THAY ĐỔI: Import thêm GRACE_PERIOD_ROUNDS và BLOCKS_PER_EPOCH
use primary::{core::RECONFIGURE_INTERVAL, Core, Primary, ReconfigureNotification, Round};
use prost::Message;
use std::collections::{HashMap, HashSet};
// THAY ĐỔI: Xóa Ordering
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio::time::sleep;
use worker::{Worker, WorkerMessage};

mod state_syncer;
use state_syncer::StateSyncer;

pub mod validator {
    include!(concat!(env!("OUT_DIR"), "/validator.rs"));
}

pub mod comm {
    include!(concat!(env!("OUT_DIR"), "/comm.rs"));
}

pub const CHANNEL_CAPACITY: usize = 10_000;
// *** THAY ĐỔI 1: Định nghĩa BLOCKS_PER_EPOCH ***
// Giả định RECONFIGURE_INTERVAL = 100 và height = round / 2
pub const BLOCKS_PER_EPOCH: u64 = RECONFIGURE_INTERVAL / 2;

#[derive(Clone, Debug)]
enum Shutdown {
    Now,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum NodeRole {
    Validator,
    Follower,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RunMode {
    Primary,
    Worker(WorkerId),
}

#[derive(Clone)]
struct SharedNodeState {
    committee: Arc<RwLock<Committee>>,
    store: Store,
    node_config: NodeConfig,
    parameters: Parameters,
}

// --- Các hàm fetch_validators_via_uds, load_consensus_state giữ nguyên ---
async fn fetch_validators_via_uds(socket_path: &str, block_number: u64) -> Result<ValidatorInfo> {
    log::info!(
        "Attempting to fetch validator list for block {} from UDS: {}",
        block_number,
        socket_path
    );

    const UDS_CONNECT_TIMEOUT_MS: u64 = 5000;
    const UDS_READ_TIMEOUT_MS: u64 = 10000;

    // Connect to UDS with timeout
    let mut stream = tokio::time::timeout(
        Duration::from_millis(UDS_CONNECT_TIMEOUT_MS),
        tokio::net::UnixStream::connect(socket_path),
    )
    .await
    .context(format!(
        "UDS connection timeout ({}ms)",
        UDS_CONNECT_TIMEOUT_MS
    ))?
    .context(format!("Failed to connect to UDS path '{}'", socket_path))?;

    let block_req = validator::BlockRequest { block_number };
    let request = validator::Request {
        payload: Some(validator::request::Payload::BlockRequest(block_req)),
    };
    let request_bytes = request.encode_to_vec();
    let request_len = request_bytes.len() as u32;

    // Write request with timeout
    tokio::time::timeout(
        Duration::from_millis(UDS_READ_TIMEOUT_MS),
        stream.write_all(&request_len.to_be_bytes()),
    )
    .await
    .context(format!("UDS write timeout ({}ms)", UDS_READ_TIMEOUT_MS))?
    .context("Failed to write request length to UDS")?;

    tokio::time::timeout(
        Duration::from_millis(UDS_READ_TIMEOUT_MS),
        stream.write_all(&request_bytes),
    )
    .await
    .context(format!("UDS write timeout ({}ms)", UDS_READ_TIMEOUT_MS))?
    .context("Failed to write request payload to UDS")?;

    // Read response length with timeout
    let mut len_buf = [0u8; 4];
    tokio::time::timeout(
        Duration::from_millis(UDS_READ_TIMEOUT_MS),
        stream.read_exact(&mut len_buf),
    )
    .await
    .context(format!(
        "UDS read timeout ({}ms) - server may not be responding",
        UDS_READ_TIMEOUT_MS
    ))?
    .context("Failed to read response length from UDS. Connection likely closed.")?;

    let response_len = u32::from_be_bytes(len_buf) as usize;
    let mut response_buf = vec![0u8; response_len];

    // Read response payload with timeout
    tokio::time::timeout(
        Duration::from_millis(UDS_READ_TIMEOUT_MS),
        stream.read_exact(&mut response_buf),
    )
    .await
    .context(format!(
        "UDS read timeout ({}ms) - server may not be responding",
        UDS_READ_TIMEOUT_MS
    ))?
    .context("Failed to read response payload from UDS")?;

    let wrapped_response = validator::Response::decode(&response_buf[..])
        .context("Failed to decode wrapped Response Protobuf")?;
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
    let mut wrapper_list = ValidatorInfo::default();
    for proto_val in proto_list.validators {
        // DEBUG: Log primary_address nhận được từ chain để trace
        if proto_val
            .primary_address
            .parse::<std::net::SocketAddr>()
            .is_ok()
        {
            let parsed_addr = proto_val
                .primary_address
                .parse::<std::net::SocketAddr>()
                .unwrap();
            if parsed_addr.ip().to_string() == "0.0.0.0" || parsed_addr.port() == 0 {
                log::warn!("[Node] ⚠️ RECEIVED INVALID PRIMARY ADDRESS from chain for validator {} (address: {}): {}", 
                      proto_val.address, proto_val.address, proto_val.primary_address);
            } else {
                log::info!(
                    "[Node] Received validator {} from chain with primary_address: {}",
                    proto_val.address,
                    proto_val.primary_address
                );
            }
        } else {
            log::warn!(
                "[Node] ⚠️ FAILED TO PARSE primary_address from chain for validator {}: {}",
                proto_val.address,
                proto_val.primary_address
            );
        }

        let wrapper_val = Validator {
            address: proto_val.address,
            primary_address: proto_val.primary_address,
            worker_address: proto_val.worker_address,
            p2p_address: proto_val.p2p_address,
            total_staked_amount: proto_val.total_staked_amount,
            pubkey_bls: proto_val.pubkey_bls,
            pubkey_secp: proto_val.pubkey_secp,
        };
        wrapper_list.validators.push(wrapper_val);
    }
    Ok(wrapper_list)
}

async fn load_consensus_state(store: &mut Store) -> ConsensusState {
    match store.read(STATE_KEY.to_vec()).await {
        Ok(Some(bytes)) => match bincode::deserialize::<ConsensusState>(&bytes) {
            Ok(state) => state,
            Err(e) => {
                error!(
                    "Failed to deserialize consensus state: {}. Starting from genesis (Round 0).",
                    e
                );
                ConsensusState::default() // Sử dụng default() nếu state rỗng
            }
        },
        Ok(None) => ConsensusState::default(), // Sử dụng default() nếu state rỗng
        Err(e) => {
            error!(
                "Failed to read from store: {:?}. Starting from genesis (Round 0).",
                e
            );
            ConsensusState::default() // Sử dụng default() nếu state rỗng
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // --- Phần parse arguments giữ nguyên ---
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
                .args_from_usage("--uds-socket=[PATH] 'Unix Domain Socket path to fetch committee (Required if --committee is absent)'")
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
        ("run", Some(sub_matches)) => setup_and_run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

async fn setup_and_run(matches: &ArgMatches<'_>) -> Result<()> {
    // --- Phần khởi tạo config, store giữ nguyên ---
    let key_file = matches.value_of("keys").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    let node_config = NodeConfig::import(key_file).context("Failed to load the node's keypair")?;
    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };
    let store = Store::new(store_path).context("Failed to create a store")?;
    let epoch_transitioning = Arc::new(AtomicBool::new(false)); // Giữ lại cờ này cho Proposer

    // --- Tải committee ban đầu ---
    let initial_committee =
        load_initial_committee(matches, &mut store.clone(), &node_config).await?;
    let shared_state = SharedNodeState {
        committee: Arc::new(RwLock::new(initial_committee.clone())),
        store,
        node_config,
        parameters,
    };

    info!("initial_committee: {:?}", initial_committee);

    // Xác định RunMode
    let run_mode = match matches.subcommand() {
        ("primary", _) => RunMode::Primary,
        ("worker", Some(sub_matches)) => {
            let id_str = sub_matches.value_of("id").unwrap();
            let id = id_str
                .parse::<WorkerId>()
                .context(format!("'{}' is not a valid worker id", id_str))?;
            RunMode::Worker(id)
        }
        _ => unreachable!(),
    };

    run_hot_swap(shared_state, matches, run_mode, epoch_transitioning).await
}

// --- Hàm load_initial_committee và fetch_committee_from_uds giữ nguyên ---
async fn load_initial_committee(
    matches: &ArgMatches<'_>,
    store: &mut Store,
    node_config: &NodeConfig,
) -> Result<Committee> {
    let committee_file = matches.value_of("committee");

    let always_false = false; // Tạm thời để logic UDS chạy
    if always_false && committee_file.is_some() {
        let filename = committee_file.unwrap();
        info!("[New Branch] Loading committee from file: {}", filename);
        let mut committee =
            Committee::import(filename).context("Failed to load committee from file")?;
        if committee.epoch == 0 {
            committee.epoch = 1;
            info!("Committee file has epoch 0, setting to 1.");
        }
        Ok(committee)
    } else {
        info!("Fetching committee via UDS for initial load.");
        let socket_path = matches
            .value_of("uds-socket")
            .context("UDS socket path is required when committee file is not provided")?;

        let consensus_state = load_consensus_state(store).await;
        info!(
            "Loaded initial consensus state. Last committed round: {}",
            consensus_state.last_committed_round
        );

        let current_epoch_start_round =
            Core::calculate_last_reconfiguration_round(consensus_state.last_committed_round);
        // SỬA: Tính block number chính xác hơn, dựa vào cấu trúc block/round (giả sử 2 round/block)
        let block_number_to_fetch = current_epoch_start_round / 2; // Ví dụ: round 100 -> block 50
                                                                   // SỬA: Epoch sẽ là block number + 1 (vì epoch bắt đầu từ block 0)
        let epoch_to_load = block_number_to_fetch + 1;

        info!(
            "Fetching committee for epoch {} (based on block number {})",
            epoch_to_load, block_number_to_fetch
        );
        fetch_committee_from_uds(
            socket_path,
            block_number_to_fetch,
            node_config,
            epoch_to_load,
        )
        .await
    }
}

async fn fetch_committee_from_uds(
    socket_path: &str,
    block_number: u64,
    node_config: &NodeConfig,
    epoch: u64,
) -> Result<Committee> {
    const RETRY_DELAY_MS: u64 = 5000;

    loop {
        match fetch_validators_via_uds(socket_path, block_number).await {
            Ok(validator_info) => {
                info!(
                    "Successfully fetched validator info for block {}.",
                    block_number
                );
                match Committee::from_validator_info(
                    validator_info,
                    &node_config.name.to_eth_address(),
                    epoch,
                ) {
                    Ok(committee) => {
                        info!(
                            "Successfully parsed new committee for epoch {} from fetched data.",
                            epoch
                        );
                        return Ok(committee);
                    }
                    Err(e) => {
                        error!(
                            "Failed to parse committee data for epoch {} (block {}): {}. Retrying in {} ms...",
                           epoch, block_number, e, RETRY_DELAY_MS
                        );
                        sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to fetch validators for block {}: {}. Retrying in {} ms...",
                    block_number, e, RETRY_DELAY_MS
                );
                sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }
    }
}

// ########################################################################
// #                      HÀM `run_hot_swap` (ĐÃ SỬA ĐỔI)                  #
// ########################################################################

async fn run_hot_swap(
    shared_state: SharedNodeState,
    matches: &ArgMatches<'_>,
    run_mode: RunMode,
    epoch_transitioning: Arc<AtomicBool>, // Giữ lại cho Proposer
) -> Result<()> {
    // --- Tạo các kênh giao tiếp vĩnh viễn ---
    let (tx_reconfigure, mut rx_reconfigure_main) =
        broadcast::channel::<ReconfigureNotification>(1);
    let rx_reconfigure_analyze = tx_reconfigure.subscribe();
    let rx_reconfigure_for_consensus = tx_reconfigure.subscribe();
    // *** THAY ĐỔI: Thêm '_' cho các biến không dùng ***
    // Tạo các kênh reconfigure nhưng đánh dấu là không dùng trực tiếp ở đây
    let _rx_reconfigure_for_core = tx_reconfigure.subscribe();
    let _rx_reconfigure_for_gc = tx_reconfigure.subscribe();
    let _rx_reconfigure_for_proposer = tx_reconfigure.subscribe();
    let _rx_reconfigure_for_hw = tx_reconfigure.subscribe();
    let _rx_reconfigure_for_cw = tx_reconfigure.subscribe();
    let _rx_reconfigure_for_helper = tx_reconfigure.subscribe();

    let (tx_shutdown, rx_shutdown_for_analyze) = broadcast::channel::<Shutdown>(1);
    let mut rx_shutdown_main = tx_shutdown.subscribe();

    let (tx_output, _rx_dummy) =
        broadcast::channel::<(Vec<CommittedSubDag>, Committee, Option<Round>)>(CHANNEL_CAPACITY);
    let rx_output_for_analyze = tx_output.subscribe();

    // --- Khởi chạy các service MỘT LẦN ---
    info!("Spawning all services for long-running (hot-swap) mode...");
    let committee_arc = shared_state.committee.clone();
    let initial_committee = committee_arc.read().await.clone();

    // 1. Khởi chạy Analyze (nếu là Primary)
    if let RunMode::Primary = run_mode {
        let tx_shutdown_clone = tx_shutdown.clone(); // Clone tx_shutdown để move
        tokio::spawn(analyze_hot_swap(
            // <--- Dùng phiên bản CÓ TRẠNG THÁI
            rx_reconfigure_analyze,
            rx_output_for_analyze,
            shared_state.store.clone(),
            shared_state.node_config.clone(),
            rx_shutdown_for_analyze,
            tx_shutdown_clone, // Truyền tx_shutdown_clone vào đây
        ));
    }

    // 2. Xác định vai trò (Validator / Follower)
    let is_in_committee = initial_committee
        .authorities
        .contains_key(&shared_state.node_config.name);
    let role = if is_in_committee {
        NodeRole::Validator
    } else {
        NodeRole::Follower
    };

    // 3. Khởi chạy các service theo vai trò
    if role == NodeRole::Validator {
        info!(
            "Starting Validator tasks for epoch {}...",
            initial_committee.epoch
        );
        match run_mode {
            RunMode::Primary => {
                let (tx_new_certificates, rx_new_certificates) =
                    tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
                let (tx_feedback, rx_feedback) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);

                // *** THAY ĐỔI: Gọi Primary::spawn với 8 đối số ***
                Primary::spawn(
                    // Giữ lại 8 đối số gốc
                    shared_state.node_config.clone(),
                    committee_arc.clone(),
                    shared_state.parameters.clone(),
                    shared_state.store.clone(),
                    tx_new_certificates.clone(),
                    rx_feedback,
                    tx_reconfigure.clone(), // Sender chính
                    epoch_transitioning.clone(),
                );

                Consensus::spawn(
                    committee_arc.clone(),
                    shared_state.parameters.clone(),
                    shared_state.store.clone(),
                    rx_new_certificates,
                    tx_feedback,
                    tx_output.clone(),
                    rx_reconfigure_for_consensus,
                );
            }
            RunMode::Worker(id) => {
                // Worker::spawn sẽ thiết lập listener để nhận message Reconfigure từ Primary
                Worker::spawn(
                    shared_state.node_config.name,
                    id,
                    shared_state.committee.clone(), // Worker bắt đầu với committee ban đầu
                    shared_state.parameters.clone(),
                    shared_state.store.clone(),
                ); // Xóa .await vì Worker::spawn không phải async
            }
        }
    } else {
        info!(
            "Starting Follower (StateSyncer) tasks for epoch {}...",
            initial_committee.epoch
        );
        StateSyncer::spawn(
            shared_state.node_config.name,
            committee_arc.clone(),
            shared_state.store.clone(),
            tx_shutdown.subscribe(),
        );
    }
    info!("All services spawned and running.");

    loop {
        tokio::select! {
            // 1. Lắng nghe tín hiệu Reconfigure từ Core
            result = rx_reconfigure_main.recv() => {
                match result {
                    Ok(notification) => {
                        let old_epoch = notification.committee.epoch;
                        info!(
                            "Received reconfigure signal for end of epoch {} (at round {}).",
                             old_epoch, notification.round // Round này là round kích hoạt (trigger round)
                        );

                        // epoch_transitioning được set/unset bởi Core/Proposer
                        // Không cần quản lý ở đây

                        let new_epoch = old_epoch + 1;
                        // SỬA: Tính block number chính xác dựa trên round kích hoạt
                        let trigger_round = notification.round;
                        // Tính block number tương ứng (giả sử 2 round/block)
                        let block_number_to_fetch = trigger_round / 2;


                        let uds_socket_path = matches.value_of("uds-socket")
                            .context("UDS socket path is required for reconfiguration")?;

                        let new_committee = match fetch_committee_from_uds(
                            uds_socket_path,
                            block_number_to_fetch,
                            &shared_state.node_config,
                            new_epoch,
                        ).await {
                            Ok(c) => c,
                            Err(e) => {
                                error!("FATAL: Failed to fetch new committee: {}. Shutting down.", e);
                                let _ = tx_shutdown.send(Shutdown::Now);
                                break;
                            }
                        };

                        info!("Successfully fetched new committee for epoch {}", new_epoch);
                        info!("New_committee: {:?}", new_committee);


                        // Cập nhật committee dùng chung
                        {
                            let mut committee_guard = committee_arc.write().await;
                            *committee_guard = new_committee.clone();
                            info!("Updated shared committee Arc to epoch {}", new_epoch);
                        }
                        // Core, Consensus, Proposer,... sẽ tự nhận biết sự thay đổi epoch
                        // qua kênh broadcast `tx_reconfigure` và cập nhật committee nội bộ của chúng.

                        // KHÔNG cần broadcast reconfigure tới workers của chính mình qua network
                        // vì workers đã share Arc<RwLock<Committee>> với primary.
                        // Workers sẽ tự động thấy committee mới khi Arc được update.
                        // Địa chỉ primary nội bộ (worker_to_primary) không đổi theo epoch.
                        info!("Workers will automatically use new committee from shared Arc (no network broadcast needed).");

                        // Kiểm tra xem node còn trong committee mới không
                        if !new_committee.authorities.contains_key(&shared_state.node_config.name) {
                            info!("Node is no longer in committee. Initiating shutdown.");
                            // TODO: Cần cơ chế shutdown graceful cho các task validator cũ
                            // và khởi chạy task follower mới nếu cần.
                            // Hiện tại chỉ shutdown.
                            let _ = tx_shutdown.send(Shutdown::Now);
                            break;
                        }

                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                         warn!("Reconfigure channel lagged by {}. Missed epoch transitions!", n);
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                         error!("Reconfigure channel closed unexpectedly. Shutting down.");
                         let _ = tx_shutdown.send(Shutdown::Now);
                         break;
                    }
                }
            },

            // 2. Lắng nghe Ctrl+C (giữ nguyên)
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C received, initiating graceful shutdown...");
                let _ = tx_shutdown.send(Shutdown::Now);
            },

            // 3. Lắng nghe tín hiệu Shutdown (giữ nguyên)
            result = rx_shutdown_main.recv() => {
                 match result {
                    Ok(Shutdown::Now) => {
                        info!("Received Shutdown::Now. Exiting main loop.");
                    },
                    Err(_) => {
                        info!("Shutdown channel closed. Exiting main loop.");
                    },
                 }
                 break;
            }
        }
    }

    info!("Node main loop finished.");
    Ok(())
}

// ########################################################################
// #                      HÀM `analyze_hot_swap` (ĐÃ SỬA ĐỔI)             #
// ########################################################################
async fn analyze_hot_swap(
    mut rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    mut rx_output: broadcast::Receiver<(Vec<CommittedSubDag>, Committee, Option<Round>)>, // Giữ nguyên type signature
    mut store: Store,
    node_config: NodeConfig,
    mut rx_shutdown: broadcast::Receiver<Shutdown>,
    tx_shutdown: broadcast::Sender<Shutdown>,
) {
    let mut all_committed_txs_by_epoch: HashMap<u64, HashSet<Vec<u8>>> = HashMap::new();
    // Track round chẵn lớn nhất đã commit cho mỗi epoch để tạo fake block rỗng cho round chẵn bị skip
    let mut last_committed_even_round_per_epoch: HashMap<u64, Round> = HashMap::new();

    let socket_path = node_config.uds_block_path.clone();
    if socket_path.is_empty() {
        warn!("[ANALYZE] uds_block_path is not configured. Committed blocks will be discarded.");
        if let Err(e) = rx_shutdown.recv().await {
            warn!("[ANALYZE] Shutdown error: {}", e);
        }
        info!("[ANALYZE] Discard-mode task exiting.");
        return;
    }

    // Tạo background channel để gửi blocks không blocking event loop chính
    let (tx_blocks, mut rx_blocks) =
        tokio::sync::mpsc::unbounded_channel::<comm::CommittedEpochData>();

    // Spawn background task để gửi blocks sang UDS một cách không blocking
    // Background task sẽ xử lý việc gửi blocks một cách tuần tự, không làm gián đoạn event loop chính
    let socket_path_bg = socket_path.clone();
    tokio::spawn(async move {
        let bg_uds_sender = UdsSender::new(socket_path_bg);
        while let Some(epoch_data) = rx_blocks.recv().await {
            if let Err(e) = bg_uds_sender.send(epoch_data, "background").await {
                error!("[ANALYZE] Background UDS send failed: {}", e);
            }
        }
    });

    info!("[ANALYZE] Started (Hot-Swap Mode). Waiting for commits."); // Log cập nhật

    loop {
        tokio::select! {
            result = rx_output.recv() => {
                match result {
                    // *** === THAY ĐỔI LOGIC XỬ LÝ === ***
                    // Chỉ quan tâm đến `dags`, bỏ qua `skipped_round_option` (luôn là None)
                    Ok((dags, committee, _skipped_round_option)) => {
                         let message_epoch = committee.epoch;

                         // Chỉ xử lý khi dags không rỗng (chỉ nhận khi commit)
                         if !dags.is_empty() {
                            let representative_round = dags.iter()
                                .map(|dag| dag.leader.round())
                                .max()
                                .unwrap_or(0);
                            info!(
                                "[ANALYZE] Processing {} committed sub-dag(s) (Epoch {}, Max Leader Round: {}).", // Thêm epoch vào log
                                dags.len(), message_epoch, representative_round
                            );
                            // Gọi hàm xử lý dags với tracking cho fake block rỗng
                            finalize_and_send_epoch(
                                message_epoch,
                                dags,
                                &mut store,
                                &socket_path,
                                &mut all_committed_txs_by_epoch,
                                &mut last_committed_even_round_per_epoch,
                                &tx_blocks,
                            ).await;
                         } else if _skipped_round_option.is_some() {
                              // Log cảnh báo nếu nhận được skipped_round (không mong đợi)
                              warn!("[ANALYZE] Received unexpected skipped_round notification: {:?}. Ignoring.", _skipped_round_option);
                         }
                         // Nếu dags rỗng và skipped_round là None -> không làm gì
                    },
                     Err(broadcast::error::RecvError::Lagged(n)) => {
                         warn!("[ANALYZE] Receiver lagged by {} messages. Skipping blocks.", n);
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("[ANALYZE] Output channel closed. Exiting.");
                        break;
                    }
                }
            },
            result = rx_reconfigure.recv() => {
                 match result {
                    Ok(notification) => {
                        let epoch_to_finalize = notification.committee.epoch;
                        info!("[ANALYZE] Received Reconfigure signal for end of epoch {}. Clearing TX cache.", epoch_to_finalize);
                        all_committed_txs_by_epoch.remove(&epoch_to_finalize);
                        last_committed_even_round_per_epoch.remove(&epoch_to_finalize);
                    },
                     Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("[ANALYZE] Reconfigure receiver lagged by {}. Missed final blocks!", n);
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        error!("[ANALYZE] Reconfigure channel closed unexpectedly. Sending shutdown signal.");
                        let _ = tx_shutdown.send(Shutdown::Now);
                        break;
                    }
                 }
            },
            result = rx_shutdown.recv() => {
                 match result {
                    Ok(Shutdown::Now) => {
                        info!("[ANALYZE] Received Shutdown::Now signal. Exiting.");
                        break; // Thoát vòng lặp
                    },
                    Err(_) => { // Channel closed
                         info!("[ANALYZE] Shutdown channel closed. Exiting.");
                         break; // Thoát vòng lặp
                    }
                }
            },
        }
    }
    info!("[ANALYZE] Task finished.");
}

// *** THAY ĐỔI: Thêm tham số last_committed_even_round_per_epoch để track và tạo fake block rỗng ***
async fn finalize_and_send_epoch(
    epoch: u64,
    dags: Vec<CommittedSubDag>, // Luôn không rỗng khi được gọi
    store: &mut Store,
    _socket_path: &str, // Keep for backward compatibility but not used anymore
    all_committed_txs_by_epoch: &mut HashMap<u64, HashSet<Vec<u8>>>,
    last_committed_even_round_per_epoch: &mut HashMap<u64, Round>,
    tx_blocks: &tokio::sync::mpsc::UnboundedSender<comm::CommittedEpochData>,
) {
    let committed_tx_digests = all_committed_txs_by_epoch.entry(epoch).or_default();
    let mut processed_certificates_in_call = HashSet::new();
    let mut blocks_to_send = Vec::new();
    let mut committed_even_rounds: Vec<Round> = Vec::new(); // Track các round chẵn đã commit trong batch này

    let max_leader_round = dags.iter().map(|d| d.leader.round()).max().unwrap_or(0);
    // Log chỉ khi xử lý nhiều dags hoặc debug
    if dags.len() > 1 {
        info!(
            "[ANALYZE] Processing {} committed sub-dag(s) individually for epoch {} (Max Leader Round: {}).",
            dags.len(), epoch, max_leader_round,
        );
    }

    for sub_dag in &dags {
        let consensus_commit_round = sub_dag.leader.round();
        let height_relative = consensus_commit_round / 2;
        if height_relative == 0 {
            info!(
                "[ANALYZE] Skipping committed round {} as its relative height is 0.",
                consensus_commit_round
            );
            continue;
        }

        // Track round chẵn đã commit (trong Bullshark, chỉ round chẵn mới được commit làm leader)
        if consensus_commit_round % 2 == 0 {
            committed_even_rounds.push(consensus_commit_round);
        }

        let block_number_absolute = (epoch.saturating_sub(1)) * BLOCKS_PER_EPOCH + height_relative;

        // Giảm log verbosity để tăng hiệu suất
        debug!(
            "[ANALYZE] Creating block #{} for epoch {} (from consensus round {}).",
            block_number_absolute, epoch, consensus_commit_round
        );

        let mut block_transactions = Vec::new();

        // Xử lý certificates và đọc batches một cách hiệu quả
        // Collect tất cả batch digests trước để có thể batch read nếu store hỗ trợ
        let mut batch_digests: Vec<(crypto::Digest, u32)> = Vec::new();
        for certificate in &sub_dag.certificates {
            if certificate.epoch() != epoch {
                warn!(
                     "[ANALYZE] Skipping certificate {} from wrong epoch {} during processing of epoch {}",
                     certificate.digest(), certificate.epoch(), epoch
                 );
                continue;
            }
            if !processed_certificates_in_call.insert(certificate.digest()) {
                continue;
            }
            for (digest, worker_id) in &certificate.header.payload {
                batch_digests.push((digest.clone(), *worker_id));
            }
        }

        // Đọc batches tuần tự nhưng tối ưu bằng cách giảm overhead
        for (digest, worker_id) in batch_digests {
            match store.read(digest.to_vec()).await {
                Ok(Some(serialized_batch)) => {
                    match bincode::deserialize::<WorkerMessage>(&serialized_batch) {
                        Ok(WorkerMessage::Batch(batch)) => {
                            for tx_data in batch {
                                if committed_tx_digests.insert(tx_data.clone()) {
                                    block_transactions.push(comm::Transaction {
                                        digest: tx_data.clone(),
                                        worker_id: worker_id as u32,
                                    });
                                } else {
                                    debug!("[ANALYZE] Skipping duplicate transaction {} (already sent this epoch).", hex::encode(&tx_data));
                                }
                            }
                        }
                        Ok(_) => warn!(
                            "[ANALYZE] Deserialized unexpected WorkerMessage type for batch {}",
                            digest
                        ),
                        Err(e) => {
                            warn!("[ANALYZE] Failed to deserialize batch {}: {}", digest, e)
                        }
                    }
                }
                Ok(None) => {
                    warn!("[ANALYZE] Batch {} referenced in committed certificate not found in store!", digest);
                }
                Err(e) => {
                    error!("[ANALYZE] Error reading batch {} from store: {}", digest, e);
                }
            }
        }

        blocks_to_send.push(comm::CommittedBlock {
            epoch,
            height: block_number_absolute,
            transactions: block_transactions,
        });
    } // end dag loop

    // *** THAY ĐỔI: Tạo fake block rỗng cho các round chẵn bị skip ***
    // Trong Bullshark, mỗi round chẵn tương ứng với một block (height = round / 2)
    // Nếu một round chẵn không được commit, vẫn cần tạo fake block rỗng để đảm bảo tính liên tục
    if !committed_even_rounds.is_empty() {
        let last_committed_even_round = committed_even_rounds.iter().max().copied().unwrap_or(0);
        let previous_last_committed_even_round = last_committed_even_round_per_epoch
            .get(&epoch)
            .copied()
            .unwrap_or(0);

        // Tạo fake block rỗng cho các round chẵn bị skip giữa previous và current
        if last_committed_even_round > previous_last_committed_even_round + 2 {
            // Có ít nhất một round chẵn bị skip
            let mut fake_blocks = Vec::new();
            let mut skipped_round = previous_last_committed_even_round + 2;

            while skipped_round < last_committed_even_round {
                let skipped_height_relative = skipped_round / 2;
                let skipped_block_number_absolute =
                    (epoch.saturating_sub(1)) * BLOCKS_PER_EPOCH + skipped_height_relative;

                // Kiểm tra xem block này đã được tạo chưa (tránh duplicate)
                let already_exists = blocks_to_send
                    .iter()
                    .any(|b| b.height == skipped_block_number_absolute);
                if !already_exists {
                    info!(
                        "[ANALYZE] Creating fake empty block #{} for epoch {} (from skipped even round {}).",
                        skipped_block_number_absolute, epoch, skipped_round
                    );
                    fake_blocks.push(comm::CommittedBlock {
                        epoch,
                        height: skipped_block_number_absolute,
                        transactions: Vec::new(), // Fake block rỗng
                    });
                }
                skipped_round += 2; // Chỉ xử lý round chẵn
            }

            // Thêm fake blocks vào danh sách
            blocks_to_send.extend(fake_blocks);
        }

        // Cập nhật round chẵn lớn nhất đã commit cho epoch này
        last_committed_even_round_per_epoch.insert(epoch, last_committed_even_round);
    }

    if !blocks_to_send.is_empty() {
        let epoch_data = comm::CommittedEpochData {
            blocks: blocks_to_send,
        };
        let min_h = epoch_data
            .blocks
            .iter()
            .map(|b| b.height)
            .min()
            .unwrap_or(0);
        let max_h = epoch_data
            .blocks
            .iter()
            .map(|b| b.height)
            .max()
            .unwrap_or(0);
        let context = format!("committed block(s) (max leader round {})", max_leader_round);
        // Dùng INFO thay vì WARN cho log gửi thành công
        info!(
            "[SEND_UDS_ENTRY] Queuing blocks for Context='{}', Epoch={}, Height(s)={}-{}",
            context, epoch, min_h, max_h
        );

        // Gửi blocks qua channel (non-blocking) để background task xử lý
        if let Err(e) = tx_blocks.send(epoch_data) {
            error!("[ANALYZE] Failed to queue blocks for UDS: {}", e);
        }
    } else {
        warn!( // Trường hợp này giờ đây không mong muốn
            "[ANALYZE] No valid blocks created from committed dags for epoch {} (Max Leader Round: {}). Investigate.",
             epoch, max_leader_round
        );
    }
}

// --- UDS Sender với persistent connection để tối ưu hiệu suất ---
struct UdsSender {
    socket_path: String,
    connection: Arc<tokio::sync::Mutex<Option<UnixStream>>>,
}

impl UdsSender {
    fn new(socket_path: String) -> Self {
        Self {
            socket_path,
            connection: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    async fn ensure_connected(&self) -> Result<(), String> {
        let mut conn_guard = self.connection.lock().await;

        // Nếu đã có connection, giữ nguyên (không kiểm tra vì có thể tốn thời gian)
        if conn_guard.is_some() {
            return Ok(());
        }

        // Connect nếu chưa có connection
        match UnixStream::connect(&self.socket_path).await {
            Ok(stream) => {
                *conn_guard = Some(stream);
                debug!(
                    "[ANALYZE] UDS: Established persistent connection to {}",
                    self.socket_path
                );
                Ok(())
            }
            Err(e) => Err(format!(
                "Failed to connect to UDS {}: {}",
                self.socket_path, e
            )),
        }
    }

    async fn send(
        &self,
        epoch_data: comm::CommittedEpochData,
        context: &str,
    ) -> Result<(), String> {
        let epoch_num = epoch_data.blocks.first().map(|b| b.epoch).unwrap_or(0);
        let block_count = epoch_data.blocks.len();
        let tx_count: usize = epoch_data.blocks.iter().map(|b| b.transactions.len()).sum();
        let min_height = epoch_data
            .blocks
            .iter()
            .map(|b| b.height)
            .min()
            .unwrap_or(0);
        let max_height = epoch_data
            .blocks
            .iter()
            .map(|b| b.height)
            .max()
            .unwrap_or(0);

        // Ensure connection is established
        if let Err(e) = self.ensure_connected().await {
            return Err(e);
        }

        // Encode data
        let mut proto_buf = BytesMut::new();
        epoch_data
            .encode(&mut proto_buf)
            .map_err(|e| format!("Failed to encode Protobuf: {}", e))?;

        let len = proto_buf.len();
        let mut len_buf = BytesMut::new();
        len_buf.put_u32(len as u32);

        // Write data using persistent connection (retry once if connection broken)
        let mut retry = false;
        loop {
            let mut conn_guard = self.connection.lock().await;
            if let Some(ref mut stream) = *conn_guard {
                // Write length
                if let Err(e) = stream.write_all(&len_buf).await {
                    warn!(
                        "[ANALYZE] UDS ({}) Write failed, connection broken: {}. Will reconnect.",
                        context, e
                    );
                    *conn_guard = None;
                    drop(conn_guard);
                    if !retry {
                        retry = true;
                        if let Err(e) = self.ensure_connected().await {
                            return Err(format!("Failed to reconnect: {}", e));
                        }
                        continue;
                    } else {
                        return Err(format!("Failed to write length after retry: {}", e));
                    }
                }

                // Write payload
                if let Err(e) = stream.write_all(&proto_buf).await {
                    warn!("[ANALYZE] UDS ({}) Write payload failed, connection broken: {}. Will reconnect.", context, e);
                    *conn_guard = None;
                    drop(conn_guard);
                    if !retry {
                        retry = true;
                        if let Err(e) = self.ensure_connected().await {
                            return Err(format!("Failed to reconnect: {}", e));
                        }
                        continue;
                    } else {
                        return Err(format!("Failed to write payload after retry: {}", e));
                    }
                }

                info!(
                    "[ANALYZE] UDS ({}) Epoch {}: Successfully sent {} block(s) (Height {}-{}) with {} total transactions (payload size: {} bytes).",
                    context, epoch_num, block_count, min_height, max_height, tx_count, proto_buf.len()
                );
                return Ok(());
            } else {
                drop(conn_guard);
                if !retry {
                    retry = true;
                    if let Err(e) = self.ensure_connected().await {
                        return Err(format!("Failed to connect: {}", e));
                    }
                    continue;
                } else {
                    return Err("Connection not available after retry".to_string());
                }
            }
        }
    }
}

// --- Hàm send_to_uds với persistent connection (backward compatible) ---
// NOTE: Function này không còn được sử dụng trực tiếp, nhưng giữ lại để tương thích
#[allow(dead_code)]
async fn send_to_uds(
    epoch_data: comm::CommittedEpochData,
    _socket_path: &str, // Keep for backward compatibility but not used
    context: &str,
    uds_sender: &Arc<UdsSender>,
) {
    if let Err(e) = uds_sender.send(epoch_data, context).await {
        error!("[ANALYZE] UDS ({}) Failed to send: {}", context, e);
    }
}
