# Safety Guarantees - Đảm bảo không fork và không bỏ sót batches

## Nguyên tắc

1. **Không fork**: Tất cả nodes phải xử lý batches giống nhau (deterministic)
2. **Không bỏ sót**: Tất cả batches phải được commit, không được bỏ qua

## Cơ chế đảm bảo

### 1. Deterministic Processing

#### Certificate Processing
- Tất cả nodes nhận cùng certificates từ consensus layer
- Certificate payload được xử lý theo thứ tự deterministic
- Batches được commit theo thứ tự trong certificate

#### Batch Extraction
- Batches được extract từ parent certificates theo thứ tự deterministic
- Tất cả nodes extract batches giống nhau từ cùng certificates
- `committed_digests` đảm bảo batches không bị duplicate

### 2. No Batch Loss

#### Committed Digests Tracking
- `committed_digests` trong Proposer track tất cả batches đã commit
- Cleanup chỉ xóa entries từ rounds đã garbage collected
- Watermark: `latest_committed_round - max_retry_rounds * 2`

#### Batch State Machine
- **Pending**: Batch chờ được include trong header
- **InFlight**: Batch đã được gửi trong header, chờ commit
- **Committed**: Batch đã được commit, không retry

#### Retry Logic
- Batches InFlight được retry nếu chưa commit sau `max_retry_rounds`
- Batches quá cũ (> `max_retry_rounds * 2`) được đánh dấu Committed
- Đảm bảo batches không bị mất vĩnh viễn

### 3. Safe Cleanup

#### batch_sync_tracker Cleanup
**File**: `primary/src/synchronizer.rs`

**Safety Rules**:
- Chỉ xóa entries cũ hơn 5 phút
- Chỉ xóa nếu batch đã trong cache (synced) HOẶC rất cũ (> 10 phút)
- Giữ lại entries cho batches đang được sync

**Code**:
```rust
// Only remove if:
// 1. Batch is in cache (synced), OR
// 2. Entry is very old (> 10 minutes) - will retry if needed
```

#### HeaderWaiter Cleanup
**File**: `primary/src/header_waiter.rs`

**Safety Rules**:
- **parent_requests**: Chỉ xóa từ rounds đã garbage collected
- **batch_requests**: Chỉ xóa từ rounds đã garbage collected VÀ rất cũ (> 10 phút)
- **pending**: Chỉ xóa từ rounds đã garbage collected

**Code**:
```rust
// Only cleanup entries from rounds that are:
// 1. Older than gc_depth (already committed or garbage collected)
// 2. AND older than max_age by timestamp
```

### 4. Garbage Collection Safety

#### GC Depth
- `gc_depth`: Số rounds giữ lại trước khi garbage collect
- Chỉ cleanup entries từ rounds < `consensus_round - gc_depth`
- Đảm bảo entries từ rounds gần đây không bị xóa

#### Committed Digests Cleanup
- Watermark: `latest_committed_round - max_retry_rounds * 2`
- Chỉ xóa digests từ rounds cũ hơn watermark
- Giữ lại đủ history để check committed batches

### 5. Pre-sync Safety

#### Pre-sync Logic
- Chỉ pre-sync batches từ certificates sắp commit (trong vòng 5 rounds)
- Không pre-sync certificates quá xa trong tương lai
- Đảm bảo batches có sẵn khi certificate commit

### 6. Backpressure Safety

#### Batch Availability Check
- Kiểm tra batches có trong store trước khi commit
- Đợi tối đa 100ms nếu batches chưa có
- Log warning nếu batches vẫn missing sau khi đợi
- Vẫn commit certificate (batches sẽ được sync sau)

## Race Condition Prevention

### 1. Double-check Before Commit
- Check `committed_digests` trước khi mark batch InFlight
- Check lại trước khi include trong header
- Check lại khi collect payload

### 2. Atomic Operations
- `committed_digests` được update atomically
- Batch state transitions are atomic
- No concurrent modifications

### 3. Deterministic Order
- Batches được process theo thứ tự trong certificate
- Extraction order is deterministic
- Commit order matches certificate order

## Testing Checklist

- [ ] Tất cả batches trong certificate được commit
- [ ] Không có batch bị duplicate
- [ ] Không có batch bị bỏ sót
- [ ] Tất cả nodes commit batches giống nhau
- [ ] Cleanup không xóa batches chưa commit
- [ ] Retry logic đảm bảo batches không bị mất
- [ ] GC không xóa entries còn cần thiết

## Logs để Verify

```
[BATCH TRACK] Batch committed in certificate
[COLLECT] Batch became committed during collection - skipping
[RETRY] Requeued batch for re-inclusion
[SYNC TRACKER CLEANUP] Cleaned up X old entries (only synced or very old)
[HEADER WAITER CLEANUP] Cleaned up entries from garbage collected rounds
```

## Kết luận

Hệ thống đảm bảo:
1. **Deterministic**: Tất cả nodes xử lý batches giống nhau
2. **No Loss**: Tất cả batches được commit, không bị bỏ sót
3. **Safe Cleanup**: Cleanup chỉ xóa entries an toàn, không ảnh hưởng batches đang sync
4. **Retry Guarantee**: Batches được retry nếu chưa commit

