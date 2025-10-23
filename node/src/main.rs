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
use consensus::{Bullshark, ConsensusProtocol, ConsensusState, STATE_KEY};
use consensus::{CommittedSubDag, Consensus};
use crypto::Digest;
use crypto::Hash as _;
use env_logger::Env;
use log::{error, info, warn};
use network::SimpleSender;
use primary::{Certificate, Core, Primary, PrimaryWorkerMessage, ReconfigureNotification};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc::{channel, Receiver};
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

struct SharedNodeState {
    committee: Arc<RwLock<Committee>>,
    store: Store,
    node_config: NodeConfig,
    parameters: Parameters,
}

// ... (các hàm fetch_validators_via_uds, load_consensus_state, main, run, load_initial_committee, fetch_committee_from_uds không đổi) ...
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
                ConsensusState::default()
            }
        },
        Ok(None) => ConsensusState::default(),
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
    // ... (hàm main không đổi) ...
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
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    // ... (hàm run không đổi) ...
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

    let (run_mode, shared_state) = match matches.subcommand() {
        ("primary", _) => {
            let initial_committee =
                load_initial_committee(matches, &mut store.clone(), &node_config).await?;
            let state = SharedNodeState {
                committee: Arc::new(RwLock::new(initial_committee)),
                store,
                node_config,
                parameters,
            };
            (RunMode::Primary, state)
        }
        ("worker", Some(sub_matches)) => {
            let id_str = sub_matches.value_of("id").unwrap();
            let id = id_str
                .parse::<WorkerId>()
                .context(format!("'{}' is not a valid worker id", id_str))?;
            let initial_committee =
                load_initial_committee(matches, &mut store.clone(), &node_config).await?;
            let state = SharedNodeState {
                committee: Arc::new(RwLock::new(initial_committee)),
                store,
                node_config,
                parameters,
            };
            (RunMode::Worker(id), state)
        }
        _ => unreachable!(),
    };

    run_loop(shared_state, matches, run_mode).await
}

async fn load_initial_committee(
    matches: &ArgMatches<'_>,
    store: &mut Store,
    node_config: &NodeConfig,
) -> Result<Committee> {
    // ... (hàm load_initial_committee không đổi) ...
    let committee_file = matches.value_of("committee");

    let always_false = false;
    if always_false && committee_file.is_some() {
        let filename = committee_file.unwrap();
        info!("[New Branch] Loading committee from file: {}", filename);
        Committee::import(filename).context("Failed to load committee from file")
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

        let block_number =
            Core::calculate_last_reconfiguration_round(consensus_state.last_committed_round);

        fetch_committee_from_uds(socket_path, block_number, node_config).await
    }
}

async fn fetch_committee_from_uds(
    socket_path: &str,
    block_number: u64,
    node_config: &NodeConfig,
) -> Result<Committee> {
    const MAX_RETRIES: u32 = 5;
    const RETRY_DELAY_MS: u64 = 1000;
    let mut validator_info = None;
    for attempt in 1..=MAX_RETRIES {
        match fetch_validators_via_uds(socket_path, block_number).await {
            Ok(info) => {
                validator_info = Some(info);
                break;
            }
            Err(e) => {
                warn!(
                    "Attempt {}/{} to fetch validators for block {} failed: {}. Retrying...",
                    attempt, MAX_RETRIES, block_number, e
                );
                sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }
    }
    let validator_info = validator_info.context(format!(
        "Failed to fetch validator info for block {} after {} attempts",
        block_number, MAX_RETRIES
    ))?;
    Committee::from_validator_info(validator_info, &node_config.name.to_eth_address())
        .context("Failed to create committee from validator info")
}

async fn run_loop(
    shared_state: SharedNodeState,
    matches: &ArgMatches<'_>,
    run_mode: RunMode,
) -> Result<()> {
    let (tx_reconfigure, mut rx_reconfigure) = channel::<ReconfigureNotification>(CHANNEL_CAPACITY);
    let mut current_role: Option<NodeRole> = None;
    let mut shutdown_trigger = tokio::sync::broadcast::channel(1).0;

    let (tx_output, rx_output) = channel::<(Vec<CommittedSubDag>, Committee)>(CHANNEL_CAPACITY);

    if let RunMode::Primary = run_mode {
        let temp_committee = shared_state.committee.read().await;
        let node_id = temp_committee
            .authorities
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .iter()
            .position(|pk| pk == &shared_state.node_config.name)
            .unwrap_or(0);
        drop(temp_committee);

        tokio::spawn(analyze(
            rx_output,
            node_id,
            shared_state.store.clone(),
            shared_state.node_config.clone(),
        ));
    }

    let committee = shared_state.committee.read().await;
    let my_pubkey = &shared_state.node_config.name;
    let is_in_committee = committee.authorities.contains_key(my_pubkey);
    let initial_role = if is_in_committee {
        NodeRole::Validator
    } else {
        NodeRole::Follower
    };
    current_role = Some(initial_role);

    match initial_role {
        NodeRole::Validator => {
            info!("Starting Validator tasks...");
            match run_mode {
                RunMode::Primary => {
                    let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
                    let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);

                    Primary::spawn(
                        shared_state.node_config.clone(),
                        shared_state.committee.clone(),
                        shared_state.parameters.clone(),
                        shared_state.store.clone(),
                        tx_new_certificates,
                        rx_feedback,
                        tx_reconfigure.clone(),
                    );

                    // SỬA ĐỔI: Lấy committee hiện tại để khởi tạo Bullshark
                    let initial_committee_for_bullshark = committee.clone();
                    Consensus::spawn(
                        shared_state.committee.clone(),
                        shared_state.parameters.gc_depth,
                        shared_state.store.clone(),
                        rx_new_certificates,
                        tx_feedback,
                        tx_output.clone(),
                        ConsensusProtocol::Bullshark(Bullshark::new(
                            initial_committee_for_bullshark, // Dùng bản sao
                            shared_state.parameters.gc_depth,
                        )),
                    );
                }
                RunMode::Worker(id) => {
                    Worker::spawn(
                        shared_state.node_config.name,
                        id,
                        committee.clone(),
                        shared_state.parameters.clone(),
                        shared_state.store.clone(),
                    )
                    .await;
                }
            }
        }
        NodeRole::Follower => {
            info!("Starting Follower (StateSyncer) tasks...");
            StateSyncer::spawn(
                shared_state.node_config.name,
                shared_state.committee.clone(),
                shared_state.store.clone(),
                shutdown_trigger.subscribe(),
            );
        }
    }
    drop(committee);

    loop {
        tokio::select! {
            Some(notification) = rx_reconfigure.recv(), if run_mode == RunMode::Primary => {
                info!(
                    "Received reconfigure notification for round {}",
                    notification.round
                );

                let target_commit_round = notification.round.saturating_sub(1);
                info!(
                    "Waiting for round {} to be committed before proceeding...",
                    target_commit_round
                );
                loop {
                    let consensus_state = load_consensus_state(&mut shared_state.store.clone()).await;
                    if consensus_state.last_committed_round >= target_commit_round {
                        info!(
                            "Round {} is committed (current last committed: {}). Proceeding.",
                            target_commit_round,
                            consensus_state.last_committed_round
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                info!("Hot-reloading committee...");

                let new_committee = if let Some(socket_path) = matches.value_of("uds-socket") {
                    info!("Fetching new committee for round {} from UDS.", notification.round);
                    match fetch_committee_from_uds(
                        socket_path,
                        notification.round,
                        &shared_state.node_config,
                    ).await {
                        Ok(c) => c,
                        Err(e) => {
                            error!("Failed to load new committee from UDS: {}", e);
                            continue;
                        }
                    }
                } else {
                    error!("No committee source specified for reconfiguration.");
                    continue;
                };

                {
                    let mut committee_guard = shared_state.committee.write().await;
                    *committee_guard = new_committee.clone();
                }
                info!("Shared committee state updated successfully.");

                let worker_reconfigure_message = PrimaryWorkerMessage::Reconfigure(new_committee);
                let bytes = bincode::serialize(&worker_reconfigure_message)
                    .expect("Failed to serialize worker reconfigure message");

                let committee_guard = shared_state.committee.read().await;
                match committee_guard.our_workers(&shared_state.node_config.name) {
                    Ok(worker_addresses) => {
                        let addresses: Vec<_> = worker_addresses.iter().map(|x| x.primary_to_worker).collect();
                        if !addresses.is_empty() {
                            info!("Broadcasting Reconfigure message to own workers at {:?}", addresses);
                            SimpleSender::new().broadcast(addresses, bytes.into()).await;
                        }
                    }
                    Err(e) => warn!("Could not get our worker addresses for reconfiguring: {}", e),
                }
            },
            else => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn analyze(
    // ... (hàm analyze không đổi) ...
    mut rx_output: Receiver<(Vec<CommittedSubDag>, Committee)>,
    node_id: usize,
    mut store: Store,
    node_config: NodeConfig,
) {
    let mut processed_certificates = HashSet::<Digest>::new();
    let mut committed_tx_digests = HashSet::<Vec<u8>>::new();

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

    if !node_config.uds_block_path.is_empty() {
        let socket_path = node_config.uds_block_path;
        let mut stream_option = None;

        'main_loop: loop {
            if stream_option.is_none() {
                match UnixStream::connect(&socket_path).await {
                    Ok(stream) => {
                        info!("[ANALYZE] Node ID {} connected to {}", node_id, socket_path);
                        stream_option = Some(stream);
                    }
                    Err(e) => {
                        warn!(
                            "[ANALYZE] Connection to {} failed: {}. Retrying...",
                            socket_path, e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                        continue;
                    }
                }
            }

            match rx_output.recv().await {
                Some((committed_sub_dags, _committee)) => {
                    if committed_sub_dags.is_empty() {
                        continue;
                    }

                    let mut blocks_to_send = Vec::new();

                    for sub_dag in committed_sub_dags {
                        let mut all_transactions = Vec::new();

                        let block_height = sub_dag.leader.round();
                        let epoch = block_height;

                        for certificate in sub_dag.certificates.iter() {
                            if !processed_certificates.insert(certificate.digest()) {
                                continue;
                            }

                            for (digest, worker_id) in &certificate.header.payload {
                                match store.read(digest.to_vec()).await {
                                    Ok(Some(serialized_batch)) => {
                                        if let Ok(WorkerMessage::Batch(batch)) =
                                            bincode::deserialize(&serialized_batch)
                                        {
                                            for tx_data in batch {
                                                if committed_tx_digests.insert(tx_data.clone()) {
                                                    all_transactions.push(comm::Transaction {
                                                        digest: tx_data,
                                                        worker_id: *worker_id as u32,
                                                    });
                                                }
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        warn!("[ANALYZE] Batch for digest {} not found.", digest)
                                    }
                                    Err(e) => {
                                        error!("[ANALYZE] Failed to read batch {}: {}", digest, e)
                                    }
                                }
                            }
                        }

                        if all_transactions.is_empty() {
                            info!("[ANALYZE] Skipping commit for leader at round {} with no new unique transactions.", block_height);
                            continue;
                        }

                        let committed_block = comm::CommittedBlock {
                            epoch,
                            height: block_height,
                            transactions: all_transactions,
                        };

                        info!(
                            "[ANALYZE] Prepared block #{} (Epoch {}) with {} unique transactions.",
                            block_height,
                            epoch,
                            committed_block.transactions.len()
                        );
                        blocks_to_send.push(committed_block);
                    }

                    if blocks_to_send.is_empty() {
                        continue;
                    }

                    let epoch_data = comm::CommittedEpochData {
                        blocks: blocks_to_send,
                    };

                    let mut proto_buf = BytesMut::new();
                    epoch_data
                        .encode(&mut proto_buf)
                        .expect("Protobuf serialization failed");
                    let mut len_buf = BytesMut::new();
                    put_uvarint_to_bytes_mut(&mut len_buf, proto_buf.len() as u64);

                    let stream = stream_option.as_mut().unwrap();
                    if let Err(e) = stream.write_all(&len_buf).await {
                        error!("[ANALYZE] Failed to write length: {}. Reconnecting...", e);
                        stream_option = None;
                        continue 'main_loop;
                    }
                    if let Err(e) = stream.write_all(&proto_buf).await {
                        error!("[ANALYZE] Failed to write payload: {}. Reconnecting...", e);
                        stream_option = None;
                        continue 'main_loop;
                    }

                    info!(
                        "[ANALYZE] Sent {} new blocks successfully.",
                        epoch_data.blocks.len()
                    );
                }
                None => {
                    info!("[ANALYZE] Main channel closed. Exiting.");
                    break;
                }
            }
        }
    } else {
        info!("[ANALYZE] uds_block_path is empty. Skipping UDS connection.");
    }
    info!("[ANALYZE] Task finished.");
}
