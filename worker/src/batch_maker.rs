// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use crypto::Digest;
use crypto::PublicKey;
use network::{CancelHandler, SimpleSender}; // THÊM CancelHandler
use sha3::{Digest as Sha3Digest, Sha3_512 as Sha512};
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use hex;
use prost::Message;

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    simple_network: SimpleSender,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                simple_network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    // CRITICAL FIX: Parse và tính hash đúng theo proto
                    // transaction có thể là:
                    // 1. Transactions protobuf (wrapper chứa nhiều transactions)
                    // 2. Single Transaction protobuf
                    // 3. Raw bytes với 8-byte length prefix
                    
                    // CRITICAL FIX: Cắt bỏ 8-byte length prefix trước khi parse (giống như parse_and_log_transactions_simple_with_logger)
                    // Đảm bảo hash khớp với [TX RECEIVED]
                    const LENGTH_PREFIX_SIZE: usize = 8;
                    let payload = if transaction.len() > LENGTH_PREFIX_SIZE {
                        // Cắt bỏ 8-byte length prefix (giống như parse_and_log_transactions_simple_with_logger)
                        &transaction[LENGTH_PREFIX_SIZE..]
                    } else {
                        // Nếu không đủ 8 bytes, dùng toàn bộ (có thể không có prefix)
                        transaction.as_slice()
                    };
                    
                    // Thử parse như Transactions trước
                    let tx_hash_hex = if let Ok(txs) = crate::transaction_logger::transaction::Transactions::decode(payload) {
                        // Parse thành công như Transactions - lấy transaction đầu tiên để log
                        if let Some(tx) = txs.transactions.first() {
                            // CRITICAL: Sử dụng calculate_transaction_hash() - tính từ TransactionHashData (protobuf encoded)
                            // Đảm bảo hash khớp với Go và [TX RECEIVED]
                            let tx_hash = crate::transaction_logger::calculate_transaction_hash(tx);
                            hex::encode(&tx_hash) // Full 32 bytes hash (64 hex chars)
                        } else {
                            // Nếu không có transaction trong Transactions, fallback
                            use sha3::{Digest as Sha3Digest, Keccak256};
                            // Tính hash từ payload (đã cắt prefix) để khớp với [TX RECEIVED]
                            let tx_hash = Keccak256::digest(payload).to_vec();
                            hex::encode(&tx_hash) // Full 32 bytes hash
                        }
                    } else {
                        // Nếu không parse được như Transactions, thử parse như single Transaction
                        // payload đã được cắt prefix ở trên
                        if let Ok(tx) = crate::transaction_logger::transaction::Transaction::decode(payload) {
                            // CRITICAL: Sử dụng calculate_transaction_hash() - tính từ TransactionHashData (protobuf encoded)
                            // Đảm bảo hash khớp với Go và [TX RECEIVED]
                            let tx_hash = crate::transaction_logger::calculate_transaction_hash(&tx);
                            hex::encode(&tx_hash) // Full 32 bytes hash (64 hex chars)
                        } else {
                            // Fallback: tính hash từ raw bytes (đã cắt prefix) bằng Keccak256
                            use sha3::{Digest as Sha3Digest, Keccak256};
                            // Tính hash từ payload (đã cắt prefix) để khớp với [TX RECEIVED]
                            let tx_hash = Keccak256::digest(payload).to_vec();
                            hex::encode(&tx_hash) // Full 32 bytes hash
                        }
                    };
                    
                    // CRITICAL DEBUG: Log tất cả transactions được thêm vào current_batch để trace
                    log::info!(
                        target: "narwhal_audit",
                        "[TX ADDED TO CURRENT BATCH] Transaction {} added to current batch (size: {} bytes, batch_size: {} bytes). Transaction will be included in batch when sealed.",
                        tx_hash_hex,
                        transaction.len(),
                        self.current_batch_size
                    );
                    
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    
                    // Log sau khi push để đảm bảo transaction đã được thêm
                    log::info!(
                        "[TX IN CURRENT BATCH] Transaction {} now in current batch (total size: {} bytes, tx_count: {})",
                        tx_hash_hex,
                        self.current_batch_size,
                        self.current_batch.len()
                    );
                    
                    if self.current_batch_size >= self.batch_size {
                        tracing::info!(
                            target: "narwhal_audit",
                            "[BATCH SEAL TRIGGER] Batch size reached {} bytes ({} transactions) - sealing batch immediately. Batch will be created and sent to primary.",
                            self.current_batch_size,
                            self.current_batch.len()
                        );
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        tracing::info!(
                            target: "narwhal_audit",
                            "[BATCH SEAL TRIGGER] Timer expired - sealing batch with {} transactions (size: {} bytes). Batch will be created and sent to primary.",
                            self.current_batch.len(),
                            self.current_batch_size
                        );
                        self.seal().await;
                    } else {
                        tracing::debug!(
                            "[BATCH SEAL] Timer expired but current batch is empty - no batch to seal."
                        );
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        // Serialize the batch.
        let batch_size = self.current_batch_size;
        let batch_len = self.current_batch.len();
        
        // CRITICAL DEBUG: Log khi batch được seal
        tracing::info!(
            target: "narwhal_audit",
            "[BATCH SEAL START] Starting to seal batch with {} transactions (size: {} bytes). Batch will be serialized, hashed, and sent to primary.",
            batch_len,
            batch_size
        );
        
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        
        // Log từng transaction trong batch để trace (trước khi move batch)
        // Batch là Vec<Transaction> = Vec<Vec<u8>>, mỗi transaction là raw bytes
        // Tính batch digest tạm thời để log (sẽ tính lại sau khi serialize)
        let temp_serialized = bincode::serialize(&WorkerMessage::Batch(batch.clone()))
            .expect("Failed to serialize batch for digest");
        let temp_hash = Sha512::digest(&temp_serialized);
        let mut temp_bytes = [0u8; 32];
        temp_bytes.copy_from_slice(&temp_hash[..32]);
        let temp_digest = Digest(temp_bytes);
        
        for (tx_idx, tx_data) in batch.iter().enumerate() {
            // CRITICAL FIX: Parse và log tất cả transactions trong tx_data
            // tx_data có thể là:
            // 1. Transactions protobuf (wrapper chứa nhiều transactions)
            // 2. Single Transaction protobuf
            // 3. Raw bytes với 8-byte length prefix
            
            // CRITICAL FIX: Cắt bỏ 8-byte length prefix trước khi parse (giống như parse_and_log_transactions_simple_with_logger)
            // Đảm bảo hash khớp với [TX RECEIVED]
            const LENGTH_PREFIX_SIZE: usize = 8;
            let payload = if tx_data.len() > LENGTH_PREFIX_SIZE {
                // Cắt bỏ 8-byte length prefix (giống như parse_and_log_transactions_simple_with_logger)
                &tx_data[LENGTH_PREFIX_SIZE..]
            } else {
                // Nếu không đủ 8 bytes, dùng toàn bộ (có thể không có prefix)
                tx_data.as_slice()
            };
            
            // Thử parse như Transactions trước (wrapper chứa nhiều transactions)
            if let Ok(txs) = crate::transaction_logger::transaction::Transactions::decode(payload) {
                // Parse thành công như Transactions - log tất cả transactions
                for (sub_idx, tx) in txs.transactions.iter().enumerate() {
                    // CRITICAL: Sử dụng calculate_transaction_hash() - tính từ TransactionHashData (protobuf encoded)
                    // Đảm bảo hash khớp với Go và [TX RECEIVED]
                    let tx_hash = crate::transaction_logger::calculate_transaction_hash(tx);
                    let tx_hash_hex = hex::encode(&tx_hash); // Full 32 bytes hash (64 hex chars)
                    
                    crate::log_tx_them_vao_batch!(
                        &tx_hash_hex,
                        &format!("{}", temp_digest),
                        0, // worker_id sẽ được thêm sau
                        tx_idx // Vẫn dùng tx_idx chính cho batch index
                    );
                }
            } else {
                // Nếu không parse được như Transactions, thử parse như single Transaction
                // payload đã được cắt prefix ở trên
                if let Ok(tx) = crate::transaction_logger::transaction::Transaction::decode(payload) {
                    // Parse thành công như single Transaction
                    // CRITICAL: Sử dụng calculate_transaction_hash() - tính từ TransactionHashData (protobuf encoded)
                    // Đảm bảo hash khớp với Go và [TX RECEIVED]
                    let tx_hash = crate::transaction_logger::calculate_transaction_hash(&tx);
                    let tx_hash_hex = hex::encode(&tx_hash); // Full 32 bytes hash (64 hex chars)
                    
                    crate::log_tx_them_vao_batch!(
                        &tx_hash_hex,
                        &format!("{}", temp_digest),
                        0, // worker_id sẽ được thêm sau
                        tx_idx
                    );
                } else {
                    // Không parse được - fallback: tính hash từ raw bytes (đã cắt prefix)
                    // Log warning và dùng fallback hash
                    log::warn!(
                        "[TX TO BATCH] Cannot parse transaction at index {} as Transactions or Transaction (after stripping 8-byte prefix). Using fallback hash from raw bytes.",
                        tx_idx
                    );
                    let tx_hash = {
                        use sha3::{Digest as Sha3Digest, Keccak256};
                        // Tính hash từ payload (đã cắt prefix) để khớp với [TX RECEIVED]
                        Keccak256::digest(payload).to_vec()
                    };
                    let tx_hash_hex = hex::encode(&tx_hash);
                    crate::log_tx_them_vao_batch!(
                        &tx_hash_hex,
                        &format!("{}", temp_digest),
                        0,
                        tx_idx
                    );
                }
            }
        }
        
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        // Calculate batch digest (always, not just for benchmark)
        let hash = Sha512::digest(&serialized);
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&hash[..32]);
        let digest = Digest(bytes);

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        {
            use log::info;
            use std::convert::TryInto as _;
            if let WorkerMessage::Batch(batch_ref) = &message {
                let tx_ids: Vec<_> = batch_ref
                    .iter()
                    .filter(|tx| !tx.is_empty() && tx[0] == 0u8 && tx.len() > 8)
                    .filter_map(|tx| tx[1..9].try_into().ok())
                    .collect();
                
                for id in tx_ids {
                    // NOTE: This log entry is used to compute performance.
                    info!(
                        "Batch {:?} contains sample tx {}",
                        digest,
                        u64::from_be_bytes(id)
                    );
                }
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, batch_size);
        }

        // Log batch được tạo (sử dụng hệ thống log mới)
        crate::log_batch_tao!(&format!("{}", digest), batch_len, batch_size, 0); // worker_id sẽ được thêm sau
        
        // CRITICAL DEBUG: Log tất cả batches được tạo để trace
        tracing::info!(
            target: "narwhal_audit",
            "[BATCH TRACE] Batch {} CREATED ({} transactions, {} bytes). Batch will be broadcast to workers and sent to primary.",
            digest,
            batch_len,
            batch_size
        );

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<PublicKey>, Vec<SocketAddr>) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        
        // Clone addresses trước khi move vào broadcast
        let addresses_count = addresses.len();
        let addresses_for_log: Vec<String> = addresses.iter().map(|a| format!("{}", a)).collect();
        
        // CRITICAL: Log khi batch được broadcast đến các workers khác
        tracing::info!(
            target: "narwhal_audit",
            "[BATCH BROADCAST] Batch {} being broadcast to {} workers (addresses: {:?}). Batch will be replicated across all nodes for consensus.",
            digest,
            addresses_count,
            addresses_for_log
        );
        
        self.simple_network.broadcast(addresses, bytes).await;
        
        // CRITICAL: Log sau khi broadcast thành công
        tracing::info!(
            target: "narwhal_audit",
            "[BATCH BROADCAST SUCCESS] Batch {} successfully broadcast to {} workers. All workers should receive this batch for replication.",
            digest,
            addresses_count
        );

        // SỬA LỖI: Cung cấp kiểu dữ liệu tường minh cho channel và collection.
        // Tạo các dummy handler vì QuorumWaiter vẫn cần chúng.
        let handlers: Vec<CancelHandler> = names
            .iter()
            .map(|_| tokio::sync::oneshot::channel::<Bytes>().1)
            .collect();

        // Send the batch through the deliver channel for further processing.
        // CRITICAL DEBUG: Log trước khi gửi batch đến QuorumWaiter
        tracing::info!(
            target: "narwhal_audit",
            "[BATCH TRACE] Batch {} sending to QuorumWaiter ({} transactions, {} bytes). Batch will wait for quorum before being sent to primary.",
            digest,
            batch_len,
            batch_size
        );
        
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
        
        // CRITICAL DEBUG: Log sau khi gửi batch đến QuorumWaiter thành công
        tracing::info!(
            target: "narwhal_audit",
            "[BATCH TRACE] Batch {} successfully sent to QuorumWaiter. Batch will wait for quorum ACKs before being sent to primary.",
            digest
        );
    }
}
