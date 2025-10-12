// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, Parameters, WorkerId};
use consensus::Consensus;
use env_logger::Env;
use primary::{Certificate, Primary};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use worker::{Worker, WorkerMessage};
use consensus::{ConsensusProtocol, Tusk, Bullshark};

use std::io::Write;

// Thêm các use statements cần thiết
use bytes::{BufMut, BytesMut};
use prost::Message;
use tokio::io::AsyncWriteExt;
// SỬA LỖI: Loại bỏ `use` không cần thiết.
// use tokio::net::UnixStream;

// Thêm module để import các struct được tạo bởi prost
pub mod comm {
    include!(concat!(env!("OUT_DIR"), "/comm.rs"));
}

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

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

            analyze(rx_output, node_id, store).await;
        }
        ("worker", Some(sub_matches)) => {
            let id_str = sub_matches.value_of("id").unwrap();
            let id = id_str
                .parse::<WorkerId>()
                .with_context(|| format!("Giá trị '{}' không phải là một số nguyên hợp lệ cho tham số --id", id_str))?;
            
            tokio::spawn(Worker::spawn(keypair.name, id, committee, parameters, store));
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
async fn analyze(mut rx_output: Receiver<Certificate>, node_id: usize, mut store: Store) {
    // fn put_uvarint_to_bytes_mut(buf: &mut BytesMut, mut value: u64) {
    //     loop {
    //         if value < 0x80 {
    //             buf.put_u8(value as u8);
    //             break;
    //         }
    //         buf.put_u8(((value & 0x7F) | 0x80) as u8);
    //         value >>= 7;
    //     }
    // }
    
    // let socket_path = format!("/tmp/executor{}.sock", node_id);
    // log::info!(
    //     "[ANALYZE] Node ID {} attempting to connect to {}",
    //     node_id,
    //     socket_path
    // );
    
    // let mut stream = loop { 
    //     match UnixStream::connect(&socket_path).await {
    //         Ok(stream) => {
    //             log::info!(
    //                 "[ANALYZE] Node ID {} connected successfully to {}",
    //                 node_id,
    //                 socket_path
    //             );
    //             break stream;
    //         }
    //         Err(e) => {
    //             log::warn!(
    //                 "[ANALYZE] Node ID {}: Connection to {} failed: {}. Retrying...",
    //                 node_id,
    //                 socket_path,
    //                 e
    //             );
    //             tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    //         }
    //     }
    // };
    log::info!(
        "[ANALYZE] Node ID {} entering loop to wait for committed blocks.",
        node_id
    );

    while let Some(certificate) = rx_output.recv().await {
        log::info!(
            "[ANALYZE] Node ID {} RECEIVED certificate for round {} from consensus.",
            node_id,
            certificate.header.round
        );

        // let mut all_transactions = Vec::new();

        // for (digest, worker_id) in certificate.header.payload {
        //     match store.read(digest.to_vec()).await {
        //         Ok(Some(serialized_batch_message)) => {
        //             match bincode::deserialize(&serialized_batch_message) {
        //                 Ok(WorkerMessage::Batch(batch)) => {
        //                     log::debug!(
        //                         "[ANALYZE] Unpacked batch {} with {} transactions for worker {}.",
        //                         digest,
        //                         batch.len(),
        //                         worker_id
        //                     );
        //                     for tx_data in batch {
        //                         all_transactions.push(comm::Transaction {
        //                             digest: tx_data,
        //                             worker_id: worker_id as u32,
        //                         });
        //                     }
        //                 }
        //                 Ok(_) => {
        //                     log::warn!(
        //                         "[ANALYZE] Digest {} did not correspond to a Batch message.",
        //                         digest
        //                     );
        //                 }
        //                 Err(e) => {
        //                     log::error!(
        //                         "[ANALYZE] Failed to deserialize message for digest {}: {}",
        //                         digest,
        //                         e
        //                     );
        //                 }
        //             }
        //         }
        //         Ok(None) => {
        //             log::warn!("[ANALYZE] Batch for digest {} not found in store.", digest);
        //         }
        //         Err(e) => {
        //             log::error!(
        //                 "[ANALYZE] Failed to read batch for digest {}: {}",
        //                 digest,
        //                 e
        //             );
        //         }
        //     }
        // }

        // let committed_block = comm::CommittedBlock {
        //     epoch: certificate.header.round,
        //     height: certificate.header.round,
        //     transactions: all_transactions,
        // };

        // let epoch_data = comm::CommittedEpochData {
        //     blocks: vec![committed_block],
        // };

        // log::debug!(
        //     "[ANALYZE] Node ID {} serializing data for round {}",
        //     node_id,
        //     certificate.header.round
        // );
        // let mut proto_buf = BytesMut::new();
        // epoch_data
        //     .encode(&mut proto_buf)
        //     .expect("FATAL: Protobuf serialization failed!");

        // let mut len_buf = BytesMut::new();
        // put_uvarint_to_bytes_mut(&mut len_buf, proto_buf.len() as u64);

        // if epoch_data.blocks.iter().all(|b| b.transactions.is_empty()) {
        //      log::info!("[ANALYZE] Node ID {} SENDING EMPTY BLOCK for round {}.", node_id, certificate.header.round);
        // }

        // log::info!("[ANALYZE] Node ID {} WRITING {} bytes (len) and {} bytes (data) to socket for round {}.", node_id, len_buf.len(), proto_buf.len(), certificate.header.round);

        // if let Err(e) = stream.write_all(&len_buf).await {
        //     log::error!(
        //         "[ANALYZE] FATAL: Node ID {}: Failed to write length to socket: {}",
        //         node_id,
        //         e
        //     );
        //     break;
        // }

        // if let Err(e) = stream.write_all(&proto_buf).await {
        //     log::error!(
        //         "[ANALYZE] FATAL: Node ID {}: Failed to write payload to socket: {}",
        //         node_id,
        //         e
        //     );
        //     break;
        // }

        // log::info!(
        //     "[ANALYZE] SUCCESS: Node ID {} sent block for round {} successfully.",
        //     node_id,
        //     certificate.header.round
        // );
    }

    log::warn!(
        "[ANALYZE] Node ID {} exited the receive loop. No more blocks will be processed.",
        node_id
    );
}