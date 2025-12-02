// Module để parse và log transactions từ protobuf
use bytes::Bytes;
use log::{info, warn};
use prost::Message;
use sha3::{Digest, Keccak256};
use std::time::{SystemTime, UNIX_EPOCH};

// Include generated protobuf code
pub mod transaction {
    include!(concat!(env!("OUT_DIR"), "/transaction.rs"));
}

use transaction::{
    AccessTuple, Transaction, TransactionLogBatch, TransactionLogEntry, Transactions,
};

/// Tính hash của transaction từ Transaction object
/// Thống nhất với Go: Tạo TransactionHashData từ Transaction, encode thành protobuf, rồi tính Keccak256 hash
/// Đảm bảo hash khớp giữa Go và Rust vì cả hai đều tính từ TransactionHashData (protobuf encoded)
pub fn calculate_transaction_hash(tx: &Transaction) -> Vec<u8> {
    // Tạo TransactionHashData từ Transaction
    let hash_data = transaction::TransactionHashData {
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

    // Encode hash_data thành bytes
    let mut buf = Vec::new();
    if let Err(e) = hash_data.encode(&mut buf) {
        warn!("Failed to encode TransactionHashData: {}", e);
        return Vec::new();
    }

    // Tính Keccak256 hash
    let hash = Keccak256::digest(&buf);
    hash.to_vec()
}

/// Tạo TransactionLogEntry từ Transaction
pub fn create_transaction_log_entry(
    tx: &Transaction,
    worker_id: u32,
    index_in_batch: u32,
) -> TransactionLogEntry {
    let transaction_hash = calculate_transaction_hash(tx);
    let received_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Tính size của transaction (approximate)
    let mut size = 0u64;
    size += tx.data.len() as u64;
    size += 8; // max_gas
    size += 8; // max_gas_price
    size += tx.related_addresses.len() as u64 * 32; // approximate per address

    TransactionLogEntry {
        transaction_hash,
        from_address: tx.from_address.clone(),
        to_address: tx.to_address.clone(),
        amount: tx.amount.clone(),
        nonce: tx.nonce.clone(),
        max_gas: tx.max_gas,
        max_gas_price: tx.max_gas_price,
        gas_tip_cap: tx.gas_tip_cap.clone(),
        gas_fee_cap: tx.gas_fee_cap.clone(),
        chain_id: tx.chain_id,
        r#type: tx.r#type,
        r: tx.r.clone(),
        s: tx.s.clone(),
        v: tx.v.clone(),
        sign: tx.sign.clone(),
        last_device_key: tx.last_device_key.clone(),
        new_device_key: tx.new_device_key.clone(),
        data: tx.data.clone(),
        related_addresses: tx.related_addresses.clone(),
        access_list: tx
            .access_list
            .iter()
            .map(|at| AccessTuple {
                address: at.address.clone(),
                storage_keys: at.storage_keys.clone(),
            })
            .collect(),
        read_only: tx.read_only,
        max_time_use: tx.max_time_use,
        received_timestamp,
        worker_id,
        size,
        index_in_batch,
    }
}

/// Parse Transactions từ bytes và tạo TransactionLogBatch
/// Tính hash từ raw protobuf payload cho từng transaction để đảm bảo khớp với Node
pub fn parse_and_log_transactions(
    data: &[u8],
    worker_id: u32,
) -> Result<TransactionLogBatch, String> {
    // Parse Transactions từ bytes
    let transactions =
        Transactions::decode(data).map_err(|e| format!("Failed to decode Transactions: {}", e))?;

    let received_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let total_transactions = transactions.transactions.len() as u32;

    // Tạo log entry cho từng transaction
    // Tính hash từ TransactionHashData (protobuf encoded) - thống nhất với Go
    let mut transaction_logs = Vec::new();
    for (index, tx) in transactions.transactions.iter().enumerate() {
        // Sử dụng create_transaction_log_entry để tính hash từ TransactionHashData
        let log_entry = create_transaction_log_entry(tx, worker_id, index as u32);
        transaction_logs.push(log_entry);
    }

    // Tính tổng size
    let total_size = data.len() as u64;

    let log_batch = TransactionLogBatch {
        received_timestamp,
        worker_id,
        total_transactions,
        transaction_logs,
        total_size,
    };

    Ok(log_batch)
}

/// Log transaction log batch ra console
pub fn log_transaction_batch(log_batch: &TransactionLogBatch) {
    info!(
        "[TX LOG BATCH] Worker: {}, Total: {}, Size: {} bytes, Timestamp: {}",
        log_batch.worker_id,
        log_batch.total_transactions,
        log_batch.total_size,
        log_batch.received_timestamp
    );

    for (idx, entry) in log_batch.transaction_logs.iter().enumerate() {
        log_transaction_entry(idx, entry);
    }
}

/// Log một transaction entry
pub fn log_transaction_entry(index: usize, entry: &TransactionLogEntry) {
    log_transaction_entry_with_logger(index, entry);
}

/// Log một transaction entry với structured logger (đã được thay thế bằng tracing)
pub fn log_transaction_entry_with_logger(index: usize, entry: &TransactionLogEntry) {
    let hash_hex = hex::encode(&entry.transaction_hash);
    let from_hex = hex::encode(&entry.from_address);
    let to_hex = hex::encode(&entry.to_address);
    let amount_hex = hex::encode(&entry.amount);

    let log_msg = format!(
        "[TX LOG {}] Hash: {}, From: {}, To: {}, Amount: {}, Gas: {}, GasPrice: {}, ChainID: {}, Type: {}, Size: {} bytes",
        index,
        hash_hex,
        from_hex,
        to_hex,
        amount_hex,
        entry.max_gas,
        entry.max_gas_price,
        entry.chain_id,
        entry.r#type,
        entry.size
    );

    // Tracing: Received transaction
    tracing::info!(
        target: "narwhal_audit",
        tx_hash = %hash_hex,
        worker_id = entry.worker_id,
        from = %from_hex,
        to = %to_hex,
        size = entry.size,
        chain_id = entry.chain_id,
        "[TX RECEIVED] Worker received transaction"
    );

    info!("{}", log_msg);
}

/// Parse và log transactions từ bytes (wrapper function tiện lợi)
/// Cắt bỏ 8-byte length prefix trước khi parse
pub fn parse_and_log_transactions_simple(data: &Bytes, worker_id: u32) {
    parse_and_log_transactions_simple_with_logger(data, worker_id);
}

/// Parse và log transactions từ bytes với tracing
pub fn parse_and_log_transactions_simple_with_logger(data: &Bytes, worker_id: u32) {
    const LENGTH_PREFIX_SIZE: usize = 8;

    // Kiểm tra xem có đủ 8 bytes để cắt prefix không
    if data.len() <= LENGTH_PREFIX_SIZE {
        warn!(
            "[TX LOG] Data too short ({} bytes), cannot strip {}-byte length prefix",
            data.len(),
            LENGTH_PREFIX_SIZE
        );
        return;
    }

    // Cắt bỏ 8 bytes đầu (length prefix)
    let payload = &data[LENGTH_PREFIX_SIZE..];

    match parse_and_log_transactions(payload, worker_id) {
        Ok(log_batch) => {
            log_transaction_batch(&log_batch);
            // Tracing cho từng transaction trong batch
            for entry in &log_batch.transaction_logs {
                let hash_hex = hex::encode(&entry.transaction_hash);
                tracing::info!(
                    tx_hash = %hash_hex,
                    worker_id = entry.worker_id,
                    index = entry.index_in_batch,
                    "[TX IN BATCH] Transaction included in batch"
                );
            }
        }
        Err(e) => {
            // Nếu parse Transactions failed, thử parse như single Transaction
            match parse_single_transaction(payload, worker_id) {
                Ok(log_entry) => {
                    log_transaction_entry_with_logger(0, &log_entry);
                }
                Err(_) => {
                    warn!(
                        "[TX LOG] Failed to parse as Transactions or single Transaction: {}",
                        e
                    );
                }
            }
        }
    }
}

/// Parse một transaction đơn lẻ (không phải Transactions)
/// Tính hash từ TransactionHashData (protobuf encoded) để đảm bảo khớp với Go
fn parse_single_transaction(data: &[u8], worker_id: u32) -> Result<TransactionLogEntry, String> {
    // Parse Transaction từ payload
    let tx =
        Transaction::decode(data).map_err(|e| format!("Failed to decode Transaction: {}", e))?;

    // Tính hash từ TransactionHashData (protobuf encoded) - thống nhất với Go
    let transaction_hash = calculate_transaction_hash(&tx);

    let received_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Tính size của transaction (approximate)
    let mut size = 0u64;
    size += tx.data.len() as u64;
    size += 8; // max_gas
    size += 8; // max_gas_price
    size += tx.related_addresses.len() as u64 * 32; // approximate per address

    Ok(TransactionLogEntry {
        transaction_hash, // Sử dụng hash từ raw payload
        from_address: tx.from_address.clone(),
        to_address: tx.to_address.clone(),
        amount: tx.amount.clone(),
        nonce: tx.nonce.clone(),
        max_gas: tx.max_gas,
        max_gas_price: tx.max_gas_price,
        gas_tip_cap: tx.gas_tip_cap.clone(),
        gas_fee_cap: tx.gas_fee_cap.clone(),
        chain_id: tx.chain_id,
        r#type: tx.r#type,
        r: tx.r.clone(),
        s: tx.s.clone(),
        v: tx.v.clone(),
        sign: tx.sign.clone(),
        last_device_key: tx.last_device_key.clone(),
        new_device_key: tx.new_device_key.clone(),
        data: tx.data.clone(),
        related_addresses: tx.related_addresses.clone(),
        access_list: tx
            .access_list
            .iter()
            .map(|at| AccessTuple {
                address: at.address.clone(),
                storage_keys: at.storage_keys.clone(),
            })
            .collect(),
        read_only: tx.read_only,
        max_time_use: tx.max_time_use,
        received_timestamp,
        worker_id,
        size,
        index_in_batch: 0,
    })
}
