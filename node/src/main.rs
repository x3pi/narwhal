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
use network::SimpleSender;
use primary::PrimaryWorkerMessage;
use primary::{core::RECONFIGURE_INTERVAL, Core, Primary, ReconfigureNotification, Round};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
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
    let mut stream = tokio::net::UnixStream::connect(socket_path)
        .await
        .context(format!("Failed to connect to UDS path '{}'", socket_path))?;
    let block_req = validator::BlockRequest { block_number };
    let request = validator::Request {
        payload: Some(validator::request::Payload::BlockRequest(block_req)),
    };
    let request_bytes = request.encode_to_vec();
    let request_len = request_bytes.len() as u32;
    stream
        .write_all(&request_len.to_be_bytes())
        .await
        .context("Failed to write request length to UDS")?;
    stream
        .write_all(&request_bytes)
        .await
        .context("Failed to write request payload to UDS")?;
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("Failed to read response length from UDS. Connection likely closed.")?;
    let response_len = u32::from_be_bytes(len_buf) as usize;
    let mut response_buf = vec![0u8; response_len];
    stream
        .read_exact(&mut response_buf)
        .await
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
    let epoch_transitioning = Arc::new(AtomicBool::new(false));

    // --- Tải committee ban đầu ---
    let initial_committee =
        load_initial_committee(matches, &mut store.clone(), &node_config).await?;
    let shared_state = SharedNodeState {
        committee: Arc::new(RwLock::new(initial_committee)),
        store,
        node_config,
        parameters,
    };

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

    let always_false = false;
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
        let block_number_to_fetch = if current_epoch_start_round > 0 {
            current_epoch_start_round / 2
        } else {
            0
        };
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
// #                      HÀM `run_hot_swap` (Không đổi)                  #
// ########################################################################
async fn run_hot_swap(
    shared_state: SharedNodeState,
    matches: &ArgMatches<'_>,
    run_mode: RunMode,
    epoch_transitioning: Arc<AtomicBool>,
) -> Result<()> {
    // --- Tạo các kênh giao tiếp vĩnh viễn ---
    let (tx_reconfigure, mut rx_reconfigure_main) =
        broadcast::channel::<ReconfigureNotification>(1);
    let rx_reconfigure_analyze = tx_reconfigure.subscribe();
    let rx_reconfigure_for_consensus = tx_reconfigure.subscribe();

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
        let tx_shutdown_clone = tx_shutdown.clone();
        tokio::spawn(analyze_hot_swap(
            // <--- Dùng phiên bản CÓ TRẠNG THÁI
            rx_reconfigure_analyze,
            rx_output_for_analyze,
            shared_state.store.clone(),
            shared_state.node_config.clone(),
            rx_shutdown_for_analyze,
            tx_shutdown_clone,
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

                Primary::spawn(
                    shared_state.node_config.clone(),
                    committee_arc.clone(),
                    shared_state.parameters.clone(),
                    shared_state.store.clone(),
                    tx_new_certificates.clone(),
                    rx_feedback,
                    tx_reconfigure.clone(),
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
                Worker::spawn(
                    shared_state.node_config.name,
                    id,
                    initial_committee.clone(),
                    shared_state.parameters.clone(),
                    shared_state.store.clone(),
                )
                .await;
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

    // --- Vòng lặp `run` (giữ nguyên) ---
    let mut network_sender = SimpleSender::new();

    loop {
        tokio::select! {
            // 1. Lắng nghe tín hiệu Reconfigure từ Core (giữ nguyên)
            result = rx_reconfigure_main.recv() => {
                match result {
                    Ok(notification) => {
                        let old_epoch = notification.committee.epoch;
                        info!(
                            "Received reconfigure signal for end of epoch {} (at round {}).",
                             old_epoch, notification.round
                        );

                        epoch_transitioning.store(true, Ordering::SeqCst);

                        let new_epoch = old_epoch + 1;
                        let block_number_to_fetch = if notification.round > 0 {
                             (notification.round / RECONFIGURE_INTERVAL) * RECONFIGURE_INTERVAL / 2
                        } else { 0 };

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

                        {
                            let mut committee_guard = committee_arc.write().await;
                            *committee_guard = new_committee.clone();
                            info!("Updated shared committee Arc to epoch {}", new_epoch);
                        }

                        info!("Broadcasting PrimaryWorkerMessage::Reconfigure to all workers...");
                        let reconfigure_msg = PrimaryWorkerMessage::Reconfigure(new_committee.clone());
                        let bytes = bincode::serialize(&reconfigure_msg)
                            .expect("Failed to serialize reconfigure message");

                        match new_committee.our_workers(&shared_state.node_config.name) {
                            Ok(worker_addrs) => {
                                let addresses: Vec<_> = worker_addrs
                                    .iter()
                                    .map(|addr| addr.primary_to_worker)
                                    .collect();
                                network_sender.broadcast(addresses, bytes.into()).await;
                                info!("Reconfigure message sent to workers.");
                            },
                            Err(e) => {
                                warn!("Could not find own workers in new committee: {}. Node may be shutting down.", e);
                                if !new_committee.authorities.contains_key(&shared_state.node_config.name) {
                                     info!("Node is no longer in committee. Initiating shutdown.");
                                     let _ = tx_shutdown.send(Shutdown::Now);
                                     break;
                                }
                            }
                        }

                        epoch_transitioning.store(false, Ordering::SeqCst);
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
// #                      HÀM `analyze_hot_swap` (ĐÃ SỬA LỖI)             #
// ########################################################################
async fn analyze_hot_swap(
    mut rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    mut rx_output: broadcast::Receiver<(Vec<CommittedSubDag>, Committee, Option<Round>)>,
    mut store: Store,
    node_config: NodeConfig,
    mut rx_shutdown: broadcast::Receiver<Shutdown>,
    tx_shutdown: broadcast::Sender<Shutdown>,
) {
    let mut epoch_buffers: HashMap<u64, Vec<CommittedSubDag>> = HashMap::new();

    // [SỬA LỖI BẮT ĐẦU]
    // Di chuyển bộ lọc chống trùng lặp (trí nhớ) ra ngoài vòng lặp.
    // Key: Epoch, Value: Set chứa các digest của giao dịch (dạng Vec<u8>) đã được gửi đi.
    let mut all_committed_txs_by_epoch: HashMap<u64, HashSet<Vec<u8>>> = HashMap::new();
    // [SỬA LỖI KẾT THÚC]

    let socket_path = node_config.uds_block_path.clone();

    if socket_path.is_empty() {
        warn!("[ANALYZE] uds_block_path is not configured. Committed blocks will be discarded.");
        let _ = rx_shutdown.recv().await;
        info!("[ANALYZE] Discard-mode task exiting.");
        return;
    }

    info!("[ANALYZE] Started (Hot-Swap Mode). Waiting for commits or signals.");

    loop {
        tokio::select! {
            // 1. Nhận committed dags từ Consensus
            result = rx_output.recv() => {
                match result {
                    Ok((dags, committee, skipped_round_option)) => {
                         let current_epoch = committee.epoch;

                         if let Some(skipped_round) = skipped_round_option {
                            // (Xử lý skipped_round_option giữ nguyên)
                            info!(
                                "[ANALYZE] Sending empty block for skipped epoch {} at height {}.",
                                current_epoch, skipped_round
                            );
                            let empty_block = comm::CommittedBlock {
                                epoch: current_epoch,
                                height: skipped_round,
                                transactions: Vec::new(),
                            };
                            let epoch_data = comm::CommittedEpochData {
                                blocks: vec![empty_block],
                            };
                            send_to_uds(epoch_data, &socket_path, "skipped round").await;
                        } else if !dags.is_empty() {
                            let representative_round = dags.iter()
                                .map(|dag| dag.leader.round())
                                .max()
                                .unwrap_or(0);

                            // Logic "gửi lắt nhắt" (round < 100) hay "gộp" (round >= 100)
                            if representative_round < RECONFIGURE_INTERVAL {
                                info!(
                                    "[ANALYZE] Processing {} sub-dag(s) immediately (Leader Round: {} < {}).",
                                    dags.len(), representative_round, RECONFIGURE_INTERVAL
                                );
                                // [SỬA LỖI] Truyền `all_committed_txs_by_epoch` vào hàm
                                finalize_and_send_epoch(current_epoch, dags, &mut store, &socket_path, &mut all_committed_txs_by_epoch).await;
                            } else {
                                info!(
                                    "[ANALYZE] Buffering {} sub-dag(s) (Leader Round: {} >= {}) for final aggregation.",
                                    dags.len(), representative_round, RECONFIGURE_INTERVAL
                                );
                                epoch_buffers
                                    .entry(current_epoch)
                                    .or_default()
                                    .extend(dags);
                            }
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                         warn!("[ANALYZE] Receiver lagged by {} messages. Skipping blocks.", n);
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("[ANALYZE] Output channel closed. Finalizing buffers and exiting.");
                         for (epoch, dags) in epoch_buffers.drain() {
                             warn!("[ANALYZE] Finalizing epoch {} from buffer (channel close).", epoch);
                             // [SỬA LỖI] Truyền `all_committed_txs_by_epoch` vào hàm
                             finalize_and_send_epoch(epoch, dags, &mut store, &socket_path, &mut all_committed_txs_by_epoch).await;
                         }
                        break;
                    }
                }
            },

            // 2. Nhận tín hiệu Reconfigure từ Core (qua main)
            result = rx_reconfigure.recv() => {
                 match result {
                    Ok(notification) => {
                        let epoch_to_finalize = notification.committee.epoch;
                        info!("[ANALYZE] Received Reconfigure signal for end of epoch {}. Processing final block.", epoch_to_finalize);

                        if let Some(dags) = epoch_buffers.remove(&epoch_to_finalize) {
                            // [SỬA LỖI] Truyền `all_committed_txs_by_epoch` vào hàm
                            finalize_and_send_epoch(epoch_to_finalize, dags, &mut store, &socket_path, &mut all_committed_txs_by_epoch).await;
                        } else {
                             warn!("[ANALYZE] No committed dags found in buffer for epoch {} to finalize. Sending empty final block.", epoch_to_finalize);
                             // [SỬA LỖI] Truyền `all_committed_txs_by_epoch` vào hàm (kể cả khi rỗng)
                             finalize_and_send_epoch(epoch_to_finalize, Vec::new(), &mut store, &socket_path, &mut all_committed_txs_by_epoch).await;
                        }

                        // [SỬA LỖI BẮT ĐẦU]
                        // Dọn dẹp bộ lọc (HashSet) của epoch cũ để tiết kiệm bộ nhớ.
                        all_committed_txs_by_epoch.remove(&epoch_to_finalize);
                        info!("[ANALYZE] Cleared committed TX cache for epoch {}.", epoch_to_finalize);
                        // [SỬA LỖI KẾT THÚC]

                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("[ANALYZE] Reconfigure receiver lagged by {}. Missed final blocks!", n);
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        error!("[ANALYZE] Reconfigure channel closed unexpectedly. Shutting down.");
                        let _ = tx_shutdown.send(Shutdown::Now);
                        break;
                    }
                 }
            },


            // 3. Nhận tín hiệu Shutdown (Now)
            result = rx_shutdown.recv() => {
                match result {
                    Ok(Shutdown::Now) => {
                        info!("[ANALYZE] Received Shutdown::Now signal. Finalizing all remaining buffers and exiting.");
                        for (epoch, dags) in epoch_buffers.drain() {
                             warn!("[ANALYZE] Finalizing epoch {} from buffer (Shutdown::Now).", epoch);
                             // [SỬA LỖI] Truyền `all_committed_txs_by_epoch` vào hàm
                             finalize_and_send_epoch(epoch, dags, &mut store, &socket_path, &mut all_committed_txs_by_epoch).await;
                         }
                        break;
                    },
                    Err(_) => { // Channel closed
                         info!("[ANALYZE] Shutdown channel closed. Finalizing buffers and exiting.");
                         for (epoch, dags) in epoch_buffers.drain() {
                             warn!("[ANALYZE] Finalizing epoch {} from buffer (channel close).", epoch);
                             // [SỬA LỖI] Truyền `all_committed_txs_by_epoch` vào hàm
                             finalize_and_send_epoch(epoch, dags, &mut store, &socket_path, &mut all_committed_txs_by_epoch).await;
                         }
                         break;
                    }
                }
            },
        }
    }
    info!("[ANALYZE] Task finished.");
}

// --- Các hàm finalize_and_send_epoch (ĐÃ SỬA) và send_to_uds ---

// [SỬA LỖI] Thay đổi chữ ký hàm (thêm `all_committed_txs_by_epoch`)
async fn finalize_and_send_epoch(
    epoch: u64,
    dags: Vec<CommittedSubDag>,
    store: &mut Store,
    socket_path: &str,
    all_committed_txs_by_epoch: &mut HashMap<u64, HashSet<Vec<u8>>>,
) {
    if dags.is_empty() {
        // ... (Logic gửi block rỗng khi reconfigure giữ nguyên) ...
        info!("[ANALYZE] No dags to process for final block of epoch {}. Sending an empty final block.", epoch);
        let final_height = (epoch + 1) * RECONFIGURE_INTERVAL - 1;
        let final_block = comm::CommittedBlock {
            epoch,
            height: final_height,
            transactions: Vec::new(),
        };
        let epoch_data = comm::CommittedEpochData {
            blocks: vec![final_block],
        };
        send_to_uds(epoch_data, socket_path, "empty final").await;
        return;
    }

    info!(
        "[ANALYZE] Finalizing/Sending for epoch {}. Aggregating transactions from {} sub-dags...",
        epoch,
        dags.len()
    );

    let mut all_transactions = Vec::new();
    let mut processed_certificates = HashSet::new();

    // [SỬA LỖI BẮT ĐẦU]
    // Lấy HashSet của epoch này từ HashMap, hoặc tạo mới nếu chưa có.
    // Đây là "trí nhớ" dài hạn cho epoch này.
    let committed_tx_digests = all_committed_txs_by_epoch.entry(epoch).or_default();
    // [SỬA LỖI KẾT THÚC]

    let final_height = dags
        .iter()
        .flat_map(|dag| &dag.certificates)
        .map(|cert| cert.round())
        .max()
        .unwrap_or_else(|| (epoch + 1) * RECONFIGURE_INTERVAL - 1); // Giá trị dự phòng

    for sub_dag in dags {
        for certificate in sub_dag.certificates {
            if certificate.epoch() != epoch {
                warn!(
                     "[ANALYZE] Skipping certificate {} from wrong epoch {} during finalization of epoch {}",
                      certificate.digest(), certificate.epoch(), epoch
                 );
                continue;
            }
            // `processed_certificates` chỉ dùng để tránh xử lý trùng cert *trong cùng một block*
            if !processed_certificates.insert(certificate.digest()) {
                continue;
            }

            for (digest, worker_id) in &certificate.header.payload {
                match store.read(digest.to_vec()).await {
                    Ok(Some(serialized_batch)) => {
                        match bincode::deserialize::<WorkerMessage>(&serialized_batch) {
                            Ok(WorkerMessage::Batch(batch)) => {
                                for tx_data in batch {
                                    // [SỬA LỖI] Kiểm tra bằng `committed_tx_digests` (trí nhớ dài hạn)
                                    if committed_tx_digests.insert(tx_data.clone()) {
                                        // Chỉ thêm vào block và log nếu NÓ CHƯA TỒN TẠI trong epoch này
                                        all_transactions.push(comm::Transaction {
                                            digest: tx_data.clone(),
                                            worker_id: *worker_id as u32,
                                        });
                                        info!("tx hex encode: {}", hex::encode(tx_data));
                                    } else {
                                        // Giao dịch này đã được gửi đi trong một block (L2) trước đó của epoch này.
                                        // Bỏ qua để tránh trùng lặp.
                                        debug!("[ANALYZE] Skipping duplicate transaction (already sent this epoch).");
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
                        warn!("[ANALYZE] Batch {} referenced in committed certificate {} not found in store!", digest, certificate.digest());
                    }
                    Err(e) => {
                        error!("[ANALYZE] Error reading batch {} from store: {}", digest, e);
                    }
                }
            }
        }
    }

    // [SỬA LỖI] Chỉ gửi block nếu nó chứa giao dịch MỚI
    if all_transactions.is_empty() {
        // [SỬA LỖI BIÊN DỊCH] Sửa lại log message
        info!("[ANALYZE] No *new* unique transactions found in this batch (Epoch {}, Leader Round {}). Skipping UDS send.", epoch, final_height);
        return;
    }

    let final_block = comm::CommittedBlock {
        epoch,
        height: final_height,
        transactions: all_transactions,
    };

    info!(
        "[ANALYZE] Created final block for epoch {} at height {} with {} unique transactions.",
        epoch,
        final_height,
        final_block.transactions.len()
    );

    let epoch_data = comm::CommittedEpochData {
        blocks: vec![final_block],
    };

    send_to_uds(epoch_data, socket_path, "final block").await;
}

// --- Hàm send_to_uds (Không đổi) ---
async fn send_to_uds(epoch_data: comm::CommittedEpochData, socket_path: &str, context: &str) {
    let epoch_num = epoch_data.blocks.first().map(|b| b.epoch).unwrap_or(0);
    let block_count = epoch_data.blocks.len();
    let tx_count: usize = epoch_data.blocks.iter().map(|b| b.transactions.len()).sum();

    match UnixStream::connect(socket_path).await {
        Ok(mut stream) => {
            let mut proto_buf = BytesMut::new();
            if let Err(e) = epoch_data.encode(&mut proto_buf) {
                error!(
                    "[ANALYZE] UDS ({}) Epoch {}: Failed to encode Protobuf: {}",
                    context, epoch_num, e
                );
                return;
            }

            let len = proto_buf.len();
            let mut len_buf = BytesMut::new();
            len_buf.put_u32(len as u32); // Sửa: Dùng u32 (Big Endian)

            if let Err(e) = stream.write_all(&len_buf).await {
                error!(
                    "[ANALYZE] UDS ({}) Epoch {}: Failed to write length ({} bytes): {}",
                    context,
                    epoch_num,
                    len_buf.len(),
                    e
                );
                return;
            }
            if let Err(e) = stream.write_all(&proto_buf).await {
                error!(
                    "[ANALYZE] UDS ({}) Epoch {}: Failed to write payload ({} bytes): {}",
                    context,
                    epoch_num,
                    proto_buf.len(),
                    e
                );
                return;
            }

            info!(
                "[ANALYZE] UDS ({}) Epoch {}: Successfully sent {} block(s) with {} total transactions (payload size: {} bytes).",
                context, epoch_num, block_count, tx_count, proto_buf.len()
            );
        }
        Err(e) => {
            error!(
                "[ANALYZE] UDS ({}) Epoch {}: Connection to {} failed: {}",
                context, epoch_num, socket_path, e
            );
        }
    }
}
