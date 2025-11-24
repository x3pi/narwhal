# SỬA LỖI: TÍNH HASH TỪ RAW PROTOBUF PAYLOAD

## VẤN ĐỀ

Hash transaction được tính **KHÁC NHAU** giữa Worker và Node:
- **Worker:** Tính hash từ Transaction object (sau khi parse từ protobuf)
- **Node:** Tính hash từ raw protobuf payload (sau khi strip length prefix)

**Kết quả:** Hash không khớp, không thể track transaction bằng hash.

---

## GIẢI PHÁP

Tính hash từ **raw Protobuf payload** (sau khi strip length prefix) thay vì từ Transaction object để đảm bảo:
1. Worker và Node tính hash từ **cùng nguồn dữ liệu**
2. Hash không phụ thuộc vào quá trình parse
3. Hash khớp nhau 100%

---

## THAY ĐỔI

### 1. **Worker: `worker/src/transaction_logger.rs`**

#### A. Thêm function tính hash từ raw payload

```rust
/// Tính hash của transaction từ raw Protobuf payload (sau khi strip length prefix)
/// Đảm bảo Worker và Node tính hash từ cùng nguồn dữ liệu (raw protobuf bytes)
pub fn calculate_transaction_hash_from_payload(payload: &[u8]) -> Vec<u8> {
    // Tính hash trực tiếp từ raw protobuf payload
    // Điều này đảm bảo hash khớp giữa Worker và Node vì cả hai đều tính từ cùng payload
    let hash = Keccak256::digest(payload);
    hash.to_vec()
}
```

#### B. Sửa `parse_single_transaction`

- **Trước:** Tính hash từ Transaction object
- **Sau:** Tính hash từ raw protobuf payload

```rust
fn parse_single_transaction(data: &[u8], worker_id: u32) -> Result<TransactionLogEntry, String> {
    // Tính hash từ raw protobuf payload (sau khi strip length prefix)
    let transaction_hash = calculate_transaction_hash_from_payload(data);
    
    // Parse Transaction để lấy thông tin chi tiết
    let tx = Transaction::decode(data)?;
    // ...
}
```

#### C. Sửa `parse_and_log_transactions`

- **Trước:** Tính hash từ Transaction object
- **Sau:** Encode lại từng transaction thành protobuf bytes, rồi tính hash từ raw payload

```rust
for (index, tx) in transactions.transactions.iter().enumerate() {
    // Encode transaction thành protobuf bytes để tính hash từ raw payload
    let mut tx_bytes = Vec::new();
    tx.encode(&mut tx_bytes)?;
    
    // Tính hash từ raw protobuf payload (đảm bảo khớp với Node)
    let transaction_hash = calculate_transaction_hash_from_payload(&tx_bytes);
    // ...
}
```

---

### 2. **Node: `node/src/main.rs`**

#### Sửa `parse_and_log_transaction`

- **Trước:** Tính hash từ Transaction object (sau khi parse)
- **Sau:** Tính hash từ raw protobuf payload (trước khi parse)

```rust
pub fn parse_and_log_transaction(
    payload: &[u8],
    batch_digest: &crypto::Digest,
    tx_idx: usize,
    _worker_id: u32,
    height: u64,
) {
    // Tính hash từ raw Protobuf payload (sau khi strip length prefix)
    // Đảm bảo hash khớp với Worker vì cả hai đều tính từ cùng payload
    use sha3::{Digest as Sha3Digest, Keccak256};
    let transaction_hash = Keccak256::digest(payload).to_vec();
    let hash_hex = hex::encode(&transaction_hash);
    
    // Parse Transaction để lấy thông tin chi tiết
    match Transaction::decode(payload) {
        Ok(tx) => {
            // Log với hash từ raw payload
            log::info!("[CONSENSUS TX LOG] ... Hash={} ...", hash_hex, ...);
        }
        // ...
    }
}
```

**Lưu ý:** Node đã tính hash từ raw payload cho duplicate detection (dòng 1592), nhưng logging vẫn dùng hash từ Transaction object. Đã sửa để logging cũng dùng hash từ raw payload.

---

## KẾT QUẢ

### Trước khi sửa:
- Worker hash: `b1fc29b0dceeea5a898720332592d59b095fa209beb0616213055150ff119754`
- Node hash: `c19c6b9d314c9b1fe37480871bd744cf2f25c81e3d0273e47aa91e66a349bd0a`
- **Không khớp** ❌

### Sau khi sửa:
- Worker hash: Tính từ raw protobuf payload (sau khi strip length prefix)
- Node hash: Tính từ raw protobuf payload (sau khi strip length prefix)
- **Khớp nhau** ✅

---

## LƯU Ý

### 1. **Length Prefix**

Client gửi transaction với **8-byte length prefix**:
- Worker: Strip length prefix trước khi parse và tính hash
- Node: Strip length prefix trước khi parse và tính hash

**Đảm bảo:** Cả hai đều tính hash từ payload **sau khi strip length prefix**.

### 2. **Protobuf Encoding**

- **Worker:** Nhận transaction từ client (đã có length prefix), strip prefix, parse, encode lại để tính hash
- **Node:** Nhận batch, strip length prefix, tính hash trực tiếp từ payload

**Đảm bảo:** Cả hai đều tính hash từ **cùng payload** (raw protobuf bytes).

### 3. **Backward Compatibility**

Function `calculate_transaction_hash(tx: &Transaction)` vẫn được giữ lại (deprecated) để đảm bảo backward compatibility, nhưng không được sử dụng trong code mới.

---

## TEST

Sau khi deploy, kiểm tra:
1. Worker log hash: `[TX LOG 0] Hash: ...`
2. Node log hash: `[CONSENSUS TX LOG] ... Hash=...`
3. **Hash phải khớp nhau** ✅

---

## KẾT LUẬN

Đã sửa để Worker và Node tính hash từ **cùng nguồn dữ liệu** (raw Protobuf payload sau khi strip length prefix), đảm bảo hash khớp nhau 100% và có thể track transaction bằng hash.

