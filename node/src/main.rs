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
use crypto::Hash as _;
use env_logger::Env;
use log::{error, info, warn};
use primary::{Core, Primary, ReconfigureNotification, Round};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::broadcast;
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

#[derive(Clone, Debug)]
enum Shutdown {
    Now,
    Finalize(u64), // u64 là epoch cần được chốt sổ
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

    run_loop(shared_state, matches, run_mode, epoch_transitioning).await
}

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
        }
        Ok(committee)
    } else {
        // if let Some(filename) = committee_file {
        //     info!("[New Branch] Loading committee from file: {}", filename);
        //     let mut committee =
        //         Committee::import(filename).context("Failed to load committee from file")?;
        //     if committee.epoch == 0 {
        //         committee.epoch = 1;
        //     }
        //     return Ok(committee);
        // }

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
        let block_number = if current_epoch_start_round > 0 {
            (current_epoch_start_round / 2) - 1
        } else {
            0
        };

        let epoch = block_number + 1;
        fetch_committee_from_uds(socket_path, block_number, node_config, epoch).await
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
                            "Failed to parse committee data for block {}: {}. Retrying in {} ms...",
                            block_number, e, RETRY_DELAY_MS
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

async fn run_loop(
    mut shared_state: SharedNodeState,
    matches: &ArgMatches<'_>,
    run_mode: RunMode,
    epoch_transitioning: Arc<AtomicBool>,
) -> Result<()> {
    let mut current_committee = shared_state.committee.read().await.clone();

    'epoch_loop: loop {
        let (tx_reconfigure, mut rx_reconfigure) = channel::<ReconfigureNotification>(1);
        let (tx_shutdown, _) = broadcast::channel::<Shutdown>(1);
        let (tx_output, rx_output) =
            channel::<(Vec<CommittedSubDag>, Committee, Option<Round>)>(CHANNEL_CAPACITY);

        if let RunMode::Primary = run_mode {
            tokio::spawn(analyze(
                rx_output,
                shared_state.store.clone(),
                shared_state.node_config.clone(),
                tx_shutdown.subscribe(),
            ));
        }

        let is_in_committee = current_committee
            .authorities
            .contains_key(&shared_state.node_config.name);
        let role = if is_in_committee {
            NodeRole::Validator
        } else {
            NodeRole::Follower
        };

        if role == NodeRole::Validator {
            info!(
                "Starting Validator tasks for epoch {}...",
                current_committee.epoch
            );
            if let RunMode::Primary = run_mode {
                let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
                let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);

                shared_state.store.write(STATE_KEY.to_vec(), vec![]).await;

                Primary::spawn(
                    shared_state.node_config.clone(),
                    shared_state.committee.clone(),
                    shared_state.parameters.clone(),
                    shared_state.store.clone(),
                    tx_new_certificates,
                    rx_feedback,
                    tx_reconfigure.clone(),
                    epoch_transitioning.clone(),
                );

                Consensus::spawn(
                    shared_state.committee.clone(),
                    shared_state.parameters.gc_depth,
                    shared_state.store.clone(),
                    rx_new_certificates,
                    tx_feedback,
                    tx_output.clone(),
                    ConsensusProtocol::Bullshark(Bullshark::new(
                        current_committee.clone(),
                        shared_state.parameters.gc_depth,
                    )),
                );
            } else if let RunMode::Worker(id) = run_mode {
                Worker::spawn(
                    shared_state.node_config.name,
                    id,
                    current_committee.clone(),
                    shared_state.parameters.clone(),
                    shared_state.store.clone(),
                )
                .await;
            }
        } else {
            info!(
                "Starting Follower (StateSyncer) tasks for epoch {}...",
                current_committee.epoch
            );
            StateSyncer::spawn(
                shared_state.node_config.name,
                shared_state.committee.clone(),
                shared_state.store.clone(),
                tx_shutdown.subscribe(),
            );

            let mut rx_shutdown = tx_shutdown.subscribe();
            if let Ok(Shutdown::Now) = rx_shutdown.recv().await {}
            break 'epoch_loop;
        }

        if run_mode == RunMode::Primary {
            let notification = rx_reconfigure.recv().await.unwrap();
            let old_epoch = notification.committee.epoch;

            info!(
                "Received reconfigure notification for end of epoch {} (at round {}). Entering wind-down period.",
                old_epoch, notification.round
            );
            epoch_transitioning.store(true, Ordering::SeqCst);

            // SỬA ĐỔI: Logic chờ một round rỗng được commit sau round 100.
            info!(
                "Winding down epoch {}: Waiting for an empty round >= {} to be committed...",
                old_epoch, notification.round
            );
            loop {
                let consensus_state = load_consensus_state(&mut shared_state.store.clone()).await;
                if consensus_state.last_committed_round >= notification.round {
                    let committed_round = consensus_state.last_committed_round;
                    if let Some(certs_in_round) = consensus_state.dag.get(&committed_round) {
                        let is_payload_empty = certs_in_round
                            .values()
                            .all(|(_, cert)| cert.header.payload.is_empty());
                        if is_payload_empty {
                            info!(
                                 "Epoch {} successfully wound down. Detected empty commit at round {}.",
                                 old_epoch, committed_round
                             );
                            break;
                        }
                    }
                }
                sleep(Duration::from_millis(500)).await;
            }
            // KẾT THÚC SỬA ĐỔI

            info!("[ANALYZE] Sending Finalize signal for epoch {}.", old_epoch);
            if tx_shutdown.send(Shutdown::Finalize(old_epoch)).is_err() {
                warn!("[ANALYZE] Failed to send Finalize signal: No active receivers.");
            }

            drop(tx_output);

            sleep(Duration::from_millis(2000)).await;

            info!("Starting transition to new epoch...");
            let new_epoch = old_epoch + 1;
            let block_number_to_fetch = (notification.round / 2) - 1;

            let new_committee = fetch_committee_from_uds(
                matches.value_of("uds-socket").unwrap(),
                block_number_to_fetch,
                &shared_state.node_config,
                new_epoch,
            )
            .await?;

            if new_committee.authorities.is_empty()
                || !new_committee
                    .authorities
                    .contains_key(&shared_state.node_config.name)
            {
                warn!(
                    "Node is no longer in the committee for epoch {}. Shutting down.",
                    new_epoch
                );
                let _ = tx_shutdown.send(Shutdown::Now);
                break 'epoch_loop;
            }

            {
                let mut committee_guard = shared_state.committee.write().await;
                *committee_guard = new_committee.clone();
            }
            current_committee = new_committee;

            epoch_transitioning.store(false, Ordering::SeqCst);
            info!(
                "Transition complete. Restarting components for new epoch {}.",
                current_committee.epoch
            );
        } else {
            let mut rx_shutdown = tx_shutdown.subscribe();
            if let Ok(_) = rx_shutdown.recv().await {
                info!("Worker received shutdown signal. Restarting for new epoch.");
            }
        }
    }
    Ok(())
}

async fn analyze(
    mut rx_output: Receiver<(Vec<CommittedSubDag>, Committee, Option<Round>)>,
    mut store: Store,
    node_config: NodeConfig,
    mut rx_shutdown: broadcast::Receiver<Shutdown>,
) {
    let mut epoch_buffers: HashMap<u64, Vec<CommittedSubDag>> = HashMap::new();

    if node_config.uds_block_path.is_empty() {
        warn!("[ANALYZE] uds_block_path is not configured. Committed blocks will be discarded.");
        let _ = rx_shutdown.recv().await;
        return;
    }
    let socket_path = node_config.uds_block_path.clone();

    loop {
        tokio::select! {
            Some((dags, committee, skipped_round_option)) = rx_output.recv() => {
                 if let Some(skipped_round) = skipped_round_option {
                    info!(
                        "[ANALYZE] Sending empty block for skipped epoch {} at height {}.",
                        committee.epoch, skipped_round
                    );
                    let empty_block = comm::CommittedBlock {
                        epoch: committee.epoch,
                        height: skipped_round,
                        transactions: Vec::new(),
                    };
                    let epoch_data = comm::CommittedEpochData {
                        blocks: vec![empty_block],
                    };
                    send_to_uds(epoch_data, &socket_path).await;
                } else if !dags.is_empty() {
                    epoch_buffers
                        .entry(committee.epoch)
                        .or_default()
                        .extend(dags);
                }
            },
            Ok(shutdown_signal) = rx_shutdown.recv() => {
                match shutdown_signal {
                    Shutdown::Finalize(epoch_to_finalize) => {
                        info!("[ANALYZE] Received Finalize signal for epoch {}. Processing final block.", epoch_to_finalize);
                        if let Some(dags) = epoch_buffers.remove(&epoch_to_finalize) {
                            finalize_and_send_epoch(epoch_to_finalize, dags, &mut store, &socket_path).await;
                        } else {
                             warn!("[ANALYZE] No committed dags found in buffer for epoch {} to finalize.", epoch_to_finalize);
                             finalize_and_send_epoch(epoch_to_finalize, Vec::new(), &mut store, &socket_path).await;
                        }
                    },
                    Shutdown::Now => {
                        info!("[ANALYZE] Received Shutdown::Now signal. Exiting.");
                        break;
                    }
                }
            },
            else => {
                info!("[ANALYZE] Channel closed. Finalizing any remaining buffers and exiting.");
                for (epoch, dags) in epoch_buffers.drain() {
                    warn!("[ANALYZE] Finalizing epoch {} from buffer due to channel close.", epoch);
                    finalize_and_send_epoch(epoch, dags, &mut store, &socket_path).await;
                }
                break;
            }
        }
    }
}

async fn finalize_and_send_epoch(
    epoch: u64,
    dags: Vec<CommittedSubDag>,
    store: &mut Store,
    socket_path: &str,
) {
    if dags.is_empty() {
        info!("[ANALYZE] No dags to process for final block of epoch {}. Sending an empty final block.", epoch);
        let final_block = comm::CommittedBlock {
            epoch,
            height: 100,
            transactions: Vec::new(),
        };
        let epoch_data = comm::CommittedEpochData {
            blocks: vec![final_block],
        };
        send_to_uds(epoch_data, socket_path).await;
        return;
    }

    info!(
        "[ANALYZE] Finalizing epoch {}. Aggregating all transactions into a single final block.",
        epoch
    );

    let mut all_transactions = Vec::new();
    let mut processed_certificates = HashSet::new();
    let mut committed_tx_digests = HashSet::new();

    let final_height = dags
        .iter()
        .flat_map(|dag| &dag.certificates)
        .map(|cert| cert.round())
        .max()
        .unwrap_or(100);

    for sub_dag in dags {
        for certificate in sub_dag.certificates {
            if certificate.epoch() != epoch {
                continue;
            }

            if !processed_certificates.insert(certificate.digest()) {
                continue;
            }

            for (digest, worker_id) in &certificate.header.payload {
                if let Ok(Some(serialized_batch)) = store.read(digest.to_vec()).await {
                    if let Ok(WorkerMessage::Batch(batch)) = bincode::deserialize(&serialized_batch)
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
            }
        }
    }

    let final_block = comm::CommittedBlock {
        epoch,
        height: final_height,
        transactions: all_transactions,
    };

    info!(
        "[ANALYZE] Created final block for epoch {} at height {} with {} total transactions.",
        epoch,
        final_height,
        final_block.transactions.len()
    );

    let epoch_data = comm::CommittedEpochData {
        blocks: vec![final_block],
    };

    send_to_uds(epoch_data, socket_path).await;
}

async fn send_to_uds(epoch_data: comm::CommittedEpochData, socket_path: &str) {
    match UnixStream::connect(socket_path).await {
        Ok(mut stream) => {
            let mut proto_buf = BytesMut::new();
            epoch_data
                .encode(&mut proto_buf)
                .expect("Protobuf serialization failed");

            let mut len_buf = BytesMut::new();
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
            put_uvarint_to_bytes_mut(&mut len_buf, proto_buf.len() as u64);

            if let Err(e) = stream.write_all(&len_buf).await {
                error!("[ANALYZE] UDS: Failed to write length: {}", e);
                return;
            }
            if let Err(e) = stream.write_all(&proto_buf).await {
                error!("[ANALYZE] UDS: Failed to write payload: {}", e);
                return;
            }
            info!(
                "[ANALYZE] Successfully sent epoch data for epoch {} to UDS.",
                epoch_data.blocks.first().map(|b| b.epoch).unwrap_or(0)
            );
        }
        Err(e) => error!("[ANALYZE] UDS: Connection to {} failed: {}", socket_path, e),
    }
}
