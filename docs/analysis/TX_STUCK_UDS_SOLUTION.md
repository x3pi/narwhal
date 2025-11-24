# GIẢI PHÁP: GIAO DỊCH KHÔNG ĐƯỢC GỬI TỚI UDS

## TÓM TẮT VẤN ĐỀ

Giao dịch `0x838c8b7c6684d173167ee51449eb15f41340ff7a5a7450eecabdd3feff92c476` không được đưa tới unix domain socket để thực thi sau khi hệ thống chạy một thời gian.

## NGUYÊN NHÂN CÓ THỂ

### 1. Channel rx_output Đầy (MOST LIKELY)

**Vấn đề:**
- Consensus gửi certificates tới `tx_output.send(certificate)`
- Nếu channel đầy, certificates không được gửi → batches không được xử lý
- Channel capacity hiện tại: 10_000

**Dấu hiệu:**
- Log "Failed to output certificate" từ consensus
- Certificates không được nhận ở node

### 2. Certificate Không Được Commit

**Vấn đề:**
- Certificate chứa batch không được commit bởi consensus
- Consensus có thể bị stuck hoặc không đạt quorum

**Dấu hiệu:**
- Không có log "Committed" từ consensus
- Certificate không được gửi tới rx_output

### 3. Batch Bị Skip Do Duplicate Detection

**Vấn đề:**
- Logic duplicate detection có thể skip batch sai
- Batch bị đánh dấu là "already processed" dù chưa được gửi tới UDS

**Dấu hiệu:**
- Log "ALREADY PROCESSED" hoặc "SKIP batch"
- Batch không được thêm vào block

### 4. Batch Không Có Trong Store

**Vấn đề:**
- Batch không được lưu trong store
- Khi node cố extract batch từ certificate, không tìm thấy trong store

**Dấu hiệu:**
- Log "NOT FOUND batch ... in store"
- Batch không được xử lý

## GIẢI PHÁP ĐÃ IMPLEMENT

### 1. Thêm Logging Chi Tiết (✅ COMPLETED)

#### a. Consensus → rx_output Channel
- **File:** `consensus/src/lib.rs`
- **Logging:**
  - ✅ Log khi gửi certificate thành công
  - ✅ Log ERROR khi channel đầy (critical)
  - ✅ Log certificate digest, round, và số batches

**Code:**
```rust
match self.tx_output.send(certificate).await {
    Ok(()) => {
        info!(
            "[CONSENSUS OUTPUT] Successfully sent certificate {} (round {}, {} batches) to rx_output channel",
            cert_digest, cert_round, batch_count
        );
    }
    Err(e) => {
        error!(
            "[CONSENSUS OUTPUT] CRITICAL: Failed to send certificate {} (round {}, {} batches) to rx_output channel: {}. This will cause batches to be stuck and not sent to UDS!",
            cert_digest, cert_round, batch_count, e
        );
    }
}
```

#### b. Transaction Hash Tracking
- **File:** `node/src/main.rs`
- **Logging:**
  - ✅ Log transaction hash khi thêm vào block
  - ✅ Log transaction hash khi gửi tới UDS
  - ✅ Track transaction cụ thể qua toàn bộ flow

**Code:**
```rust
// Khi thêm transaction vào block
log::info!(
    "[TX TRACK] Node ID {} ADDING transaction {} to block height {} (batch: {}, tx_idx: {}, worker: {})",
    node_id, tx_hash_hex, builder.height, batch_digest, tx_idx, worker_id
);

// Khi gửi block tới UDS
log::info!(
    "[UDS SEND] Node ID {} block height {} tx[{}]: hash={}, worker_id={}, size={} bytes",
    node_id, block.height, idx, tx_hash_hex, tx.worker_id, tx.digest.len()
);
```

### 2. Channel Capacity (⚠️ CẦN KIỂM TRA)

**Hiện tại:**
- `CHANNEL_CAPACITY = 10_000` cho `rx_output`
- Có thể đủ nhưng cần monitor

**Khuyến nghị:**
- Monitor log "Failed to output certificate"
- Nếu thấy nhiều lỗi, tăng capacity hoặc dùng unbounded channel

## CÁCH SỬ DỤNG LOGGING

### 1. Tìm Transaction Cụ Thể

```bash
# Tìm transaction hash trong log
grep "838c8b7c6684d173167ee51449eb15f41340ff7a5a7450eecabdd3feff92c476" *.log

# Tìm các log liên quan
grep -E "\[TX TRACK\]|\[UDS SEND\]|\[CONSENSUS OUTPUT\]" *.log | grep -i "838c8b7c"
```

### 2. Kiểm Tra Channel Đầy

```bash
# Tìm lỗi channel đầy
grep "CRITICAL: Failed to send certificate" *.log

# Đếm số lỗi
grep -c "CRITICAL: Failed to send certificate" *.log
```

### 3. Kiểm Tra Batch Processing

```bash
# Tìm batch chứa transaction
grep "\[BATCH PROCESSING\]" *.log | grep -i "838c8b7c"

# Tìm batch bị skip
grep "SKIP batch" *.log | grep -i "838c8b7c"
```

## NEXT STEPS

### Ngắn Hạn:
1. ✅ **Đã thêm logging** - chạy lại và xem log
2. ⚠️ **Monitor channel** - xem có đầy không
3. ⚠️ **Tìm transaction** trong log để xác định bottleneck

### Dài Hạn:
1. ⚠️ **Tăng channel capacity** nếu cần
2. ⚠️ **Retry mechanism** cho certificates không được gửi
3. ⚠️ **Health check** cho UDS connection

## KẾT LUẬN

**Đã thêm logging chi tiết để:**
- ✅ Track certificates từ consensus → rx_output
- ✅ Track transaction hash qua toàn bộ flow
- ✅ Phát hiện sớm khi channel đầy
- ✅ Debug khi transaction không được gửi tới UDS

**Cần làm tiếp:**
- Chạy lại hệ thống và xem log
- Tìm transaction `0x838c8b7c...` trong log
- Xác định bottleneck dựa trên log

