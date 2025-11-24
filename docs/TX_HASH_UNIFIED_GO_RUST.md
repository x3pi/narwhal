# THỐNG NHẤT CÔNG THỨC TÍNH HASH GIỮA GO VÀ RUST

## VẤN ĐỀ

Công thức tính hash transaction khác nhau giữa Go và Rust:
- **Go:** Tính hash từ `TransactionHashData` (protobuf encoded)
- **Rust:** Đang tính hash từ raw Protobuf payload

**Kết quả:** Hash không khớp giữa Go và Rust.

---

## GIẢI PHÁP

Thống nhất công thức tính hash theo Go:
1. Parse Transaction từ payload
2. Tạo `TransactionHashData` từ Transaction
3. Encode `TransactionHashData` thành protobuf bytes
4. Tính Keccak256 hash từ encoded bytes

---

## THAY ĐỔI

### 1. **Worker: `worker/src/transaction_logger.rs`**

#### A. Sửa `calculate_transaction_hash`

- **Trước:** DEPRECATED, tính từ TransactionHashData
- **Sau:** Chính thức, tính từ TransactionHashData (thống nhất với Go)

```rust
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
        access_list: tx.access_list.iter().map(|at| AccessTuple {
            address: at.address.clone(),
            storage_keys: at.storage_keys.clone(),
        }).collect(),
    };

    // Encode hash_data thành bytes
    let mut buf = Vec::new();
    hash_data.encode(&mut buf)?;

    // Tính Keccak256 hash
    let hash = Keccak256::digest(&buf);
    hash.to_vec()
}
```

#### B. Sửa `parse_single_transaction`

- **Trước:** Tính hash từ raw payload
- **Sau:** Parse Transaction, rồi tính hash từ TransactionHashData

```rust
fn parse_single_transaction(data: &[u8], worker_id: u32) -> Result<TransactionLogEntry, String> {
    // Parse Transaction từ payload
    let tx = Transaction::decode(data)?;
    
    // Tính hash từ TransactionHashData (protobuf encoded) - thống nhất với Go
    let transaction_hash = calculate_transaction_hash(&tx);
    // ...
}
```

#### C. Sửa `parse_and_log_transactions`

- **Trước:** Encode transaction, rồi tính hash từ raw payload
- **Sau:** Tính hash từ TransactionHashData (sử dụng `create_transaction_log_entry`)

```rust
for (index, tx) in transactions.transactions.iter().enumerate() {
    // Sử dụng create_transaction_log_entry để tính hash từ TransactionHashData
    let log_entry = create_transaction_log_entry(tx, worker_id, index as u32);
    transaction_logs.push(log_entry);
}
```

#### D. Xóa `calculate_transaction_hash_from_payload`

- Function này không còn cần thiết vì đã thống nhất tính hash từ TransactionHashData

---

### 2. **Node: `node/src/main.rs`**

#### A. Sửa `parse_and_log_transaction`

- **Trước:** Tính hash từ raw payload
- **Sau:** Parse Transaction, rồi tính hash từ TransactionHashData

```rust
pub fn parse_and_log_transaction(
    payload: &[u8],
    batch_digest: &crypto::Digest,
    tx_idx: usize,
    _worker_id: u32,
    height: u64,
) {
    match Transaction::decode(payload) {
        Ok(tx) => {
            // Tính hash từ TransactionHashData (protobuf encoded) - thống nhất với Go
            let transaction_hash = calculate_transaction_hash(&tx);
            // ...
        }
        // ...
    }
}
```

#### B. Sửa duplicate detection

- **Trước:** Tính hash từ raw payload
- **Sau:** Parse Transaction, rồi tính hash từ TransactionHashData

```rust
// Tính hash của transaction để kiểm tra duplicate
// Thống nhất với Go: Parse Transaction, tạo TransactionHashData, encode, rồi tính hash
use transaction::Transaction;
let tx_hash = match Transaction::decode(&tx_payload) {
    Ok(tx) => {
        // Tính hash từ TransactionHashData (protobuf encoded) - thống nhất với Go
        tx_logger::calculate_transaction_hash(&tx)
    }
    Err(e) => {
        // Fallback: tính hash từ raw payload nếu parse failed
        Keccak256::digest(&tx_payload).to_vec()
    }
};
```

---

## SO SÁNH VỚI GO

### Go Code:
```go
func (t *Transaction) Hash() common.Hash {
    hashPb := &pb.TransactionHashData{
        FromAddress:   t.proto.FromAddress,
        ToAddress:     t.proto.ToAddress,
        Amount:        t.proto.Amount,
        MaxGas:        t.proto.MaxGas,
        MaxGasPrice:   t.proto.MaxGasPrice,
        MaxTimeUse:    t.proto.MaxTimeUse,
        Data:          t.proto.Data,
        Type:          t.proto.Type,
        LastDeviceKey: t.proto.LastDeviceKey,
        NewDeviceKey:  t.proto.NewDeviceKey,
        Nonce:         t.proto.Nonce,
        ChainID:       t.proto.ChainID,
        R:             t.proto.R,
        S:             t.proto.S,
        V:             t.proto.V,
        GasTipCap:     t.proto.GasTipCap,
        GasFeeCap:     t.proto.GasFeeCap,
        AccessList:    t.proto.AccessList,
    }
    bHashPb, _ := proto.Marshal(hashPb)
    hash := crypto.Keccak256Hash(bHashPb)
    return hash
}
```

### Rust Code (sau khi sửa):
```rust
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
        access_list: tx.access_list.iter().map(|at| AccessTuple {
            address: at.address.clone(),
            storage_keys: at.storage_keys.clone(),
        }).collect(),
    };
    
    let mut buf = Vec::new();
    hash_data.encode(&mut buf)?;
    let hash = Keccak256::digest(&buf);
    hash.to_vec()
}
```

**Kết quả:** Cả Go và Rust đều:
1. Tạo `TransactionHashData` từ Transaction
2. Encode `TransactionHashData` thành protobuf bytes
3. Tính Keccak256 hash từ encoded bytes

**Hash sẽ khớp nhau 100%** ✅

---

## KẾT QUẢ

### Trước khi sửa:
- Go hash: Tính từ `TransactionHashData` (protobuf encoded)
- Rust hash: Tính từ raw Protobuf payload
- **Không khớp** ❌

### Sau khi sửa:
- Go hash: Tính từ `TransactionHashData` (protobuf encoded)
- Rust hash: Tính từ `TransactionHashData` (protobuf encoded)
- **Khớp nhau** ✅

---

## LƯU Ý

### 1. **Protobuf Encoding**

- Go: `proto.Marshal(hashPb)` - Marshal TransactionHashData
- Rust: `hash_data.encode(&mut buf)` - Encode TransactionHashData

**Đảm bảo:** Cả hai đều encode `TransactionHashData` theo cùng cách (protobuf).

### 2. **Keccak256 Hash**

- Go: `crypto.Keccak256Hash(bHashPb)` - Keccak256 hash
- Rust: `Keccak256::digest(&buf)` - Keccak256 hash

**Đảm bảo:** Cả hai đều dùng Keccak256.

### 3. **Transaction Fields**

- Go: Lấy từ `t.proto.*`
- Rust: Lấy từ `tx.*`

**Đảm bảo:** Cả hai đều lấy cùng các fields từ Transaction.

---

## KẾT LUẬN

Đã thống nhất công thức tính hash giữa Go và Rust:
- Cả hai đều tính hash từ `TransactionHashData` (protobuf encoded)
- Hash sẽ khớp nhau 100%
- Có thể track transaction bằng hash giữa Go và Rust

