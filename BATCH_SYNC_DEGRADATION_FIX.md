# Fix Batch Sync Degradation - Giải quyết vấn đề sync chậm dần theo thời gian

## Vấn đề

Hệ thống bị "lão hóa" (degradation) theo thời gian: batch sync chậm hơn so với commit rate, mặc dù số lượng giao dịch không đổi. Điều này gây ra:
- Lỗi "batch không tìm thấy" tăng dần
- Sync success rate giảm dần
- Hệ thống không ổn định

## Nguyên nhân

### 1. Memory Leak trong Tracking Maps
- **batch_sync_tracker** trong `Synchronizer`: HashMap tích lũy entries và không bao giờ cleanup
- **batch_requests** trong `HeaderWaiter`: Tích lũy theo thời gian
- **parent_requests** trong `HeaderWaiter`: Tích lũy theo thời gian
- **pending** HashMap: Tích lũy các pending requests

### 2. Cache không có giới hạn
- **PayloadCache** (DashMap) không có size limit, có thể phình to vô hạn
- Không có cleanup mechanism cho cache

### 3. Store I/O Degradation
- Store có thể chậm dần do fragmentation hoặc size tăng
- Không có optimization cho batch reads

## Giải pháp đã triển khai

### 1. ✅ Cleanup batch_sync_tracker định kỳ
**File**: `primary/src/synchronizer.rs`

- Thêm cleanup function `cleanup_batch_sync_tracker()`
- Cleanup mỗi 60 giây
- Xóa entries cũ hơn 5 phút
- Tự động gọi trong `missing_payload()` trước khi check tracker

**Code**:
```rust
fn cleanup_batch_sync_tracker(&mut self) {
    // Remove entries older than 5 minutes
    // Called automatically every 60 seconds
}
```

### 2. ✅ Cleanup HeaderWaiter tracking maps
**File**: `primary/src/header_waiter.rs`

- Thêm cleanup function `cleanup_old_entries()`
- Cleanup mỗi 60 giây
- Xóa entries cũ hơn 5 phút hoặc từ rounds quá cũ
- Cleanup cả `parent_requests`, `batch_requests`, và `pending`

**Code**:
```rust
fn cleanup_old_entries(&mut self, now: u128) {
    // Cleanup parent_requests, batch_requests, pending
    // Remove entries older than 5 minutes or from very old rounds
}
```

### 3. ✅ Monitoring và Metrics
**File**: `primary/src/synchronizer.rs`, `primary/src/core.rs`

- Track sync metrics: total_checked, found_in_cache, found_in_store, missing
- Periodic logging mỗi 30 giây
- Alerts khi sync success rate < 90% hoặc missing batches > 50

## Kết quả mong đợi

1. **Giảm memory leak**: Tracking maps được cleanup định kỳ, không tích lũy vô hạn
2. **Ổn định performance**: Sync speed không giảm theo thời gian
3. **Giảm lỗi**: Ít lỗi "batch không tìm thấy" hơn
4. **Monitoring tốt hơn**: Có metrics để track performance degradation

## Cách hoạt động

### Cleanup Schedule
- **batch_sync_tracker**: Cleanup mỗi 60 giây, xóa entries > 5 phút
- **HeaderWaiter maps**: Cleanup mỗi 60 giây, xóa entries > 5 phút hoặc từ rounds quá cũ

### Monitoring
- Metrics được log mỗi 30 giây
- Alerts tự động khi có vấn đề
- Track sync success rate để phát hiện degradation sớm

## Testing

Để verify các cải thiện:
1. Chạy hệ thống trong thời gian dài (vài giờ)
2. Monitor memory usage - không nên tăng liên tục
3. Monitor sync metrics - success rate nên ổn định
4. Kiểm tra logs - cleanup messages nên xuất hiện định kỳ

## Cần làm thêm (tùy chọn)

1. **Cache size limit**: Thêm size limit cho PayloadCache và cleanup entries cũ
2. **Store optimization**: Batch reads thay vì sequential
3. **Connection pool cleanup**: Cleanup network connections không dùng
4. **Performance metrics**: Track store I/O latency, cache hit rate

## Logs để theo dõi

```
[SYNC TRACKER CLEANUP] Cleaned up X old entries from batch_sync_tracker
[HEADER WAITER CLEANUP] Cleaned up old entries: X parent_requests, Y batch_requests, Z pending
[SYNC METRICS] Total checked: X, Found in cache: Y, Found in store: Z, Missing: W, Success rate: N%
```

