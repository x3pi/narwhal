# PHÂN TÍCH: TẠI SAO CÓ GIAO DỊCH TÍNH ĐÚNG HASH, CÓ GIAO DỊCH TÍNH SAI HASH

## VẤN ĐỀ

Một số giao dịch có hash **KHỚP** giữa Worker và Node, một số giao dịch có hash **KHÔNG KHỚP**.

**Ví dụ:**
- Transaction `b1fc29b0dceeea5a898720332592d59b095fa209beb0616213055150ff119754` (Worker hash)
- Batch: `1P2zsE4ynWak19lMuA95ReI3pV1Abp79T3xuxOwPU9Y=`
- Block height: 25697

---

## PHÂN TÍCH NGUYÊN NHÂN

### 1. **Worker Hash Calculation**

Worker tính hash từ **Transaction object** (protobuf) **TRƯỚC KHI** thêm vào batch:

```rust
// worker/src/transaction_logger.rs
pub fn calculate_transaction_hash(tx: &Transaction) -> Vec<u8> {
    let hash_data = TransactionHashData { ... };
    let mut buf = Vec::new();
    hash_data.encode(&mut buf); // Protobuf encode
    let hash = Keccak256::digest(&buf);
    hash.to_vec()
}
```

**Điểm quan trọng:** Hash được tính từ Transaction object **gốc**, không có length prefix.

### 2. **Node Hash Calculation**

Node tính hash từ **payload SAU KHI STRIP length prefix**:

```rust
// node/src/main.rs
const LENGTH_PREFIX_SIZE: usize = 8;
let tx_payload = if tx_data.len() > LENGTH_PREFIX_SIZE {
    let payload = tx_data[LENGTH_PREFIX_SIZE..].to_vec(); // STRIP LENGTH PREFIX
    // ...
}

// Parse Transaction từ payload
match Transaction::decode(payload) {
    Ok(tx) => {
        let transaction_hash = calculate_transaction_hash(&tx);
        // ...
    }
}
```

**Điểm quan trọng:** Hash được tính từ Transaction object **SAU KHI PARSE** từ payload đã strip length prefix.

### 3. **Tại Sao Có Giao Dịch Tính Đúng, Có Giao Dịch Tính Sai?**

**Nguyên nhân có thể:**

#### A. **Transaction Payload Khác Nhau**

- **Worker:** Transaction được encode thành protobuf và thêm **8-byte length prefix** trước khi thêm vào batch
- **Node:** Transaction được parse từ batch **SAU KHI STRIP length prefix**

**Nếu payload giống nhau:** Hash sẽ khớp ✅
**Nếu payload khác nhau:** Hash sẽ không khớp ❌

#### B. **Protobuf Encoding Khác Nhau**

- **Worker:** Encode Transaction thành protobuf **một lần** (khi tạo batch)
- **Node:** Decode từ batch rồi encode lại để tính hash

**Nếu encoding giống nhau:** Hash sẽ khớp ✅
**Nếu encoding khác nhau:** Hash sẽ không khớp ❌

#### C. **Transaction Object Khác Nhau**

- **Worker:** Transaction object **gốc** từ client
- **Node:** Transaction object **parsed** từ batch

**Nếu object giống nhau:** Hash sẽ khớp ✅
**Nếu object khác nhau:** Hash sẽ không khớp ❌

---

## GIẢI PHÁP

### 1. **Đảm Bảo Cùng Nguồn Dữ Liệu**

Cả Worker và Node phải tính hash từ **cùng nguồn dữ liệu**:

```rust
// Worker: Tính hash từ Transaction object gốc
let tx_hash = calculate_transaction_hash(&tx);

// Node: Parse Transaction từ batch, rồi tính hash
let tx = Transaction::decode(&payload)?;
let tx_hash = calculate_transaction_hash(&tx);
```

**Vấn đề:** Nếu Transaction object khác nhau (do encoding/decoding), hash sẽ khác nhau.

### 2. **Sửa Hash Calculation**

**Option 1: Tính hash từ raw payload (sau khi strip length prefix)**

```rust
// Worker: Tính hash từ payload (sau khi strip length prefix)
let tx_payload = &tx_data[8..]; // Strip 8-byte length prefix
let tx_hash = Keccak256::digest(tx_payload);

// Node: Tính hash từ payload (sau khi strip length prefix)
let tx_payload = &tx_data[8..]; // Strip 8-byte length prefix
let tx_hash = Keccak256::digest(tx_payload);
```

**Option 2: Tính hash từ Transaction object (sau khi parse)**

```rust
// Worker: Parse Transaction, rồi tính hash
let tx = Transaction::decode(&tx_data[8..])?;
let tx_hash = calculate_transaction_hash(&tx);

// Node: Parse Transaction, rồi tính hash
let tx = Transaction::decode(&tx_data[8..])?;
let tx_hash = calculate_transaction_hash(&tx);
```

**Option 3: Tính hash từ TransactionHashData (protobuf encoded)**

```rust
// Worker: Encode TransactionHashData, rồi tính hash
let hash_data = TransactionHashData { ... };
let mut buf = Vec::new();
hash_data.encode(&mut buf);
let tx_hash = Keccak256::digest(&buf);

// Node: Parse Transaction, tạo TransactionHashData, encode, rồi tính hash
let tx = Transaction::decode(&tx_data[8..])?;
let hash_data = TransactionHashData { ... };
let mut buf = Vec::new();
hash_data.encode(&mut buf);
let tx_hash = Keccak256::digest(&buf);
```

---

## KẾT LUẬN

**Nguyên nhân:** Hash được tính từ **nguồn dữ liệu khác nhau** giữa Worker và Node:
- Worker: Transaction object gốc (trước khi encode vào batch)
- Node: Transaction object parsed (sau khi decode từ batch)

**Giải pháp:** Đảm bảo cả Worker và Node tính hash từ **cùng nguồn dữ liệu**:
- Cùng parse Transaction từ payload (sau khi strip length prefix)
- Cùng encode TransactionHashData
- Cùng tính hash từ encoded data

**Kết quả:** Hash sẽ khớp cho **TẤT CẢ** giao dịch, không chỉ một số giao dịch.

