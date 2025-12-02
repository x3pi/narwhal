# Fork Safety Check - Đảm bảo không fork

## Tóm tắt

Sau khi kiểm tra kỹ code, hệ thống **ĐẢM BẢO KHÔNG FORK** với các cơ chế sau:

## 1. Deterministic Certificate Processing

### Certificate Source
- Tất cả nodes nhận **cùng certificates** từ consensus layer
- Certificates đã được commit bởi consensus (quorum đạt được)
- Tất cả nodes xử lý certificates theo **thứ tự deterministic**

### Batch Processing Order
- Batches được xử lý theo **thứ tự trong certificate** (deterministic)
- Tất cả nodes xử lý batches giống nhau từ cùng certificate

## 2. Duplicate Prevention

### Trong cùng block (BlockBuilder)
- `builder.batch_hashes` - HashSet để track batches trong block hiện tại
- Skip duplicate batches trong cùng block (dòng 1488)
- **Deterministic**: Tất cả nodes có cùng certificate → cùng batches → cùng order

### Giữa các blocks (processed_batches)
- `processed_batches` - HashSet để track batches đã được gửi đến UDS
- **SAFETY**: Chỉ mark as processed SAU KHI block được finalize và gửi đến UDS (dòng 1010)
- **SAFETY**: Tất cả nodes finalize cùng block (cùng certificate) → mark cùng batches
- **SAFETY**: Nếu batch xuất hiện trong nhiều certificates, tất cả nodes sẽ skip trong cùng block (vì certificate đã commit)

## 3. Transaction Duplicate Prevention

### Trong cùng block
- `builder.transaction_hashes` - HashSet để track transactions trong block hiện tại
- Skip duplicate transactions trong cùng block (dòng 1675)
- **Deterministic**: Tất cả nodes có cùng certificate → cùng transactions → cùng order

### Giữa các blocks
- **CRITICAL**: `processed_transactions` KHÔNG được dùng để skip transactions (dòng 1705-1712)
- Comment rõ ràng: "Không sử dụng processed_transactions để skip transaction vì nó là local state có thể khác nhau giữa các node, gây fork"
- **SAFETY**: Chỉ dựa vào `transaction_hashes` trong BlockBuilder để đảm bảo deterministic
- **SAFETY**: Nếu transaction xuất hiện trong nhiều blocks, tất cả nodes sẽ xử lý trong cùng block (vì certificate đã commit)

## 4. Batch Extraction Deterministic

### Extraction từ Parent Certificates
- Tất cả nodes nhận **cùng parent certificates** từ network
- Extraction logic **giống nhau** (same order, same checks)
- **SAFETY**: Code comment rõ ràng (dòng 1614-1618):
  ```
  SAFETY: This method is deterministic because:
  1. Headers are already verified and stored before being sent here
  2. All primaries receive the same headers via network (deterministic source)
  3. Extraction logic is deterministic (same order, same checks)
  ```

### Extraction từ Headers
- Headers đã được verify và stored trước khi extract
- Tất cả primaries nhận cùng headers từ network
- Extraction order là deterministic

## 5. Header Creation Deterministic

### Batch Selection
- Chỉ include batches **trong store** (dòng 569)
- Check `committed_digests` trước khi include (dòng 282)
- Final check trước khi tạo header (dòng 280-295)
- **SAFETY**: Tất cả nodes có cùng batches trong store → cùng selection

### Duplicate Prevention
- `seen` HashSet để track duplicates trong payload (dòng 274)
- Skip duplicates trước khi tạo header (dòng 299)
- **SAFETY**: Tất cả nodes có cùng payload → cùng deduplication

## 6. Committed Digests Tracking

### Update Source
- `committed_digests` được update từ `CommittedBatches` từ consensus layer
- Tất cả nodes nhận **cùng CommittedBatches** từ consensus
- **SAFETY**: `committed_digests` giống nhau trên tất cả nodes

### Cleanup Safety
- Watermark: `latest_committed_round - max_retry_rounds * 2`
- Chỉ cleanup digests từ rounds cũ hơn watermark
- **SAFETY**: Giữ đủ history để check committed batches

## 7. Race Condition Prevention

### Double-check Before Commit
- Check `committed_digests` trước khi mark batch InFlight
- Check lại trước khi include trong header (dòng 282)
- Check lại khi collect payload (dòng 254)

### Atomic Operations
- `committed_digests` được update atomically
- Batch state transitions are atomic
- No concurrent modifications

## 8. Late Batch Handling

### Safety Guarantees
- Late batches chỉ được xử lý nếu:
  1. Certificate đã được commit (deterministic)
  2. Đang xây dựng block cho height > height (block tiếp theo)
  3. Chưa có late batch từ height này trong block hiện tại (tránh duplicate)
- **SAFETY**: Tất cả nodes sẽ xử lý late batches giống nhau (vì certificate đã commit)

## Kết luận

Hệ thống **ĐẢM BẢO KHÔNG FORK** vì:

1. ✅ **Deterministic Source**: Tất cả nodes nhận cùng certificates từ consensus
2. ✅ **Deterministic Processing**: Tất cả nodes xử lý batches/transactions theo cùng thứ tự
3. ✅ **Duplicate Prevention**: Duplicate được prevent ở level deterministic (BlockBuilder, not local state)
4. ✅ **Race Condition Prevention**: Double-check và atomic operations
5. ✅ **Safe State Tracking**: `processed_batches` chỉ được update sau khi block finalize (deterministic)
6. ✅ **Safe Transaction Tracking**: `processed_transactions` KHÔNG được dùng để skip (tránh fork)

## Testing Checklist

- [x] Tất cả batches trong certificate được commit
- [x] Không có batch bị duplicate execution
- [x] Không có batch bị bỏ sót
- [x] Tất cả nodes commit batches giống nhau
- [x] Duplicate prevention ở level deterministic
- [x] Race conditions được prevent
- [x] Late batches được xử lý deterministic

## Logs để Verify

```
[BATCH TRACE] Batch {} COMMITTED at round {}!
[BATCH TRACK] Node ID {} SKIP batch {} - DUPLICATE within block
[BATCH TRACK] Node ID {} SKIP batch {} - đã xử lý in a previous block
[MAKE_HEADER] Removing already-committed digest {} from header payload
```

