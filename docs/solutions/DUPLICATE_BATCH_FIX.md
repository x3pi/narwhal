# SỬA LỖI: BATCH BỊ COMMIT VÀ THỰC THI TRÙNG LẶP NHIỀU LẦN

## VẤN ĐỀ

Batch `HhWPJuh8Z9y6VAkimnN9XDTSMw90quDuxWWT1p/2GyQ=` và transaction `ed205ce24884ad57987b5fad8a5b6ccf17dd229e1a86d4b8b477fb0a6b508cf1` bị commit và thực thi trùng lặp nhiều lần.

### Nguyên nhân

1. **Logic extract batches từ parent certificates**: Khi proposer nhận parent certificates, nó extract batches từ các certificates này và thêm vào queue. Điều này làm cho batch xuất hiện trong queue của nhiều primaries.

2. **Batch xuất hiện trong nhiều certificates**: Batch được include trong nhiều headers khác nhau:
   - B660(AqJy7eip40qqZk7F) - round 660
   - B661(AuQGL6Xnz4NACJKr) - round 661
   - B661(ApvX+ZCVrGWssP/v) - round 661
   - B661(AgF2i8f4TnfU3Bjs) - round 661
   - B661(A1NJsf/JYzCzRtTm) - round 661

3. **Node xử lý batch từ nhiều certificates**: Node xử lý batch từ mỗi certificate, dẫn đến duplicate execution. Logic hiện tại chỉ kiểm tra duplicate batch trong cùng block (`batch_hashes` trong `BlockBuilder`), nhưng không kiểm tra duplicate batch giữa các blocks khác nhau.

## GIẢI PHÁP

### 1. Sửa logic xử lý batch trong node

Thêm kiểm tra `processed_batches` để skip batch đã được xử lý trong blocks trước đó:

```rust
// CRITICAL: Kiểm tra batch đã được xử lý trong blocks trước đó
// Đây là cần thiết vì batch có thể xuất hiện trong nhiều certificates khác nhau
// (do logic extract batches từ parent certificates)
// Tuy nhiên, chỉ skip nếu batch đã được xử lý trong một block đã được finalize
// (đảm bảo deterministic: tất cả nodes đều đã xử lý batch đó)
if processed_batches.contains(batch_digest) {
    // Batch đã được xử lý trong một block trước đó
    // Skip để tránh duplicate execution
    log::warn!(
        "[DUPLICATE BATCH DETECTION] Node ID {} detected batch {} already processed in a previous block. Skipping to avoid duplicate execution.",
        node_id,
        batch_digest
    );
    // Remove from batch_hashes vì đã skip
    builder.batch_hashes.remove(batch_digest);
    continue;
}
```

### 2. Đảm bảo deterministic

- `processed_batches` chỉ được cập nhật sau khi block được finalize và gửi tới UDS
- Tất cả nodes đều nhận cùng committed certificates, nên thứ tự xử lý batch là deterministic
- Nếu batch đã được xử lý trong một block đã được finalize, tất cả nodes đều đã xử lý batch đó

### 3. Logic cập nhật `processed_batches`

`processed_batches` được cập nhật tại 2 nơi:
1. Khi block được finalize bình thường (height tăng)
2. Khi block bị force flush (do height mới đến)

Cả hai trường hợp đều đảm bảo block đã được gửi tới UDS trước khi cập nhật `processed_batches`.

## KẾT QUẢ

- Batch chỉ được xử lý một lần, ngay cả khi xuất hiện trong nhiều certificates
- Logic vẫn đảm bảo deterministic (không fork)
- Batch không bị bỏ rơi (vẫn được xử lý trong block đầu tiên)

## LƯU Ý

- `processed_batches` là local state, nhưng chỉ được cập nhật sau khi block đã được finalize (dựa trên committed certificates)
- Tất cả nodes đều nhận cùng committed certificates, nên thứ tự xử lý batch là deterministic
- Nếu batch xuất hiện trong nhiều certificates, nó sẽ được xử lý trong block đầu tiên và skip trong các blocks sau

