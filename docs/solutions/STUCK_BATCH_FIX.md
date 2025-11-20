# GIẢI PHÁP: SỬA LỖI BATCH BỊ STUCK - NGĂN CHẶN HỆ THỐNG BỊ ĐỨNG

## VẤN ĐỀ

### Tình Trạng:
1. **Giao dịch `36d48e423784e9f86d49b8e94783b1db15364df340f83eca34cddd03ab05bcbf`:**
   - Batch: `568I4QfDKWSGnkBIFT9whjCxhKD/he1CQ7Aw8P0y9+4=`
   - Retry count: **110,027 lần**
   - Vẫn không được commit

2. **Giao dịch `fff758814803714e1c5e5401df990b402a77b09e188a99448625b5a7661ea8d9`:**
   - Vẫn không được commit

3. **Hệ thống bị đứng:**
   - Tất cả giao dịch mới không được thực thi
   - Hệ thống chỉ tạo EMPTY blocks (0 transactions)

## NGUYÊN NHÂN

1. **Headers Quá Cũ:**
   - Headers từ round 474900 đã quá cũ (>100,000 rounds)
   - Network không broadcast headers quá cũ
   - Các primary khác không nhận được headers từ primary-0

2. **Early Extraction Không Hoạt Động:**
   - Early extraction chỉ hoạt động cho headers **mới nhận được**
   - Headers quá cũ không được nhận → early extraction không áp dụng

3. **Extract Từ Parents Không Giúp:**
   - Extract từ parents chỉ hoạt động cho **certificates đã commit**
   - Certificates của primary-0 không được commit

4. **Retry Logic Vô Hạn:**
   - Batch bị retry liên tục (retry_count > 100,000)
   - Logic `is_extremely_old` không bao giờ xảy ra vì `sent_round` luôn được update

## GIẢI PHÁP

### Force Remove Batch Sau N Retries (Critical Fix)

**Ý tưởng:**
- Khi `retry_count > MAX_RETRY_COUNT` (1000), batch được coi là "stuck forever"
- Force remove batch và log error
- Đảm bảo hệ thống không bị đứng vĩnh viễn

**Implementation:**
```rust
// primary/src/proposer.rs:526-543
const MAX_RETRY_COUNT: usize = 1000; // Nếu retry > 1000, batch bị stuck forever
let is_stuck_forever = retry_count > MAX_RETRY_COUNT;

if is_stuck_forever {
    // Force remove batch - hệ thống không thể commit batch này
    warn!(
        "FORCE REMOVING stuck batch {} (retry_count={}, sent_round={}, current_round={}). Batch cannot be committed after {} retries - removing to prevent system deadlock. Transactions in this batch will be LOST.",
        entry.digest, retry_count, round, self.round, MAX_RETRY_COUNT
    );
    entry.state = BatchState::Committed; // Mark as committed to remove
    removed_too_old += 1;
    continue;
}
```

## CHI TIẾT THAY ĐỔI

### File: `primary/src/proposer.rs`

**Vị trí:** `retry_stale_batches()` function

**Thay đổi:**
1. Thêm constant `MAX_RETRY_COUNT = 1000`
2. Thêm check `is_stuck_forever = retry_count > MAX_RETRY_COUNT`
3. Nếu `is_stuck_forever`, force remove batch và log error

**Code:**
```rust
// CRITICAL FIX: If batch has been retried too many times (retry_count > MAX_RETRY_COUNT),
// batch is stuck forever and should be removed to prevent system deadlock
const MAX_RETRY_COUNT: usize = 1000; // Nếu retry > 1000, batch bị stuck forever
let is_stuck_forever = retry_count > MAX_RETRY_COUNT;

if is_stuck_forever {
    // Force remove batch - hệ thống không thể commit batch này
    // Đây là biện pháp cuối cùng để ngăn chặn hệ thống bị đứng vĩnh viễn
    warn!(
        "FORCE REMOVING stuck batch {} (retry_count={}, sent_round={}, current_round={}, rounds_since_sent={}). Batch cannot be committed after {} retries - removing to prevent system deadlock. Transactions in this batch will be LOST.",
        entry.digest, retry_count, round, self.round, rounds_since_sent, MAX_RETRY_COUNT
    );
    entry.state = BatchState::Committed; // Mark as committed to remove
    removed_too_old += 1;
    continue;
}
```

## IMPACT

### Nếu Không Sửa:
- **Hệ thống bị đứng hoàn toàn:**
  - Batches bị retry vô hạn
  - Không có giao dịch nào được thực thi
  - Hệ thống chỉ tạo EMPTY blocks
  - **Không thể sử dụng**

### Sau Khi Sửa:
- **Hệ thống vẫn hoạt động:**
  - Batches bị stuck sẽ bị remove sau 1000 retries
  - Giao dịch mới vẫn được xử lý
  - Hệ thống không bị đứng
  - **Có thể sử dụng, nhưng một số batches có thể bị drop**

### Trade-off:
- ✅ **Ngăn chặn hệ thống bị đứng:**
  - Hệ thống vẫn hoạt động
  - Giao dịch mới vẫn được xử lý

- ⚠️ **Một số batches có thể bị drop:**
  - Batches bị stuck sau 1000 retries sẽ bị remove
  - Giao dịch trong các batches này sẽ **KHÔNG được thực thi**
  - Nhưng tốt hơn là hệ thống bị đứng hoàn toàn

## SAFETY

### Determinism:
- ✅ **Không ảnh hưởng determinism:**
  - Logic force remove dựa trên `retry_count` (deterministic)
  - Tất cả primaries sẽ remove batch cùng lúc (same condition)

### No Duplicate:
- ✅ **Không ảnh hưởng duplicate detection:**
  - Batch được mark là Committed → không được xử lý lại
  - Batch được remove khỏi queue → không được retry lại

### No Fork:
- ✅ **Không ảnh hưởng fork:**
  - Batch được remove ở tất cả primaries (deterministic)
  - Tất cả primaries sẽ có cùng state (batch removed)

## TESTING

### Test Cases:
1. **Test với batch bị stuck:**
   - Tạo batch không được commit
   - Verify batch bị remove sau 1000 retries
   - Verify hệ thống vẫn hoạt động

2. **Test với batch bình thường:**
   - Tạo batch được commit bình thường
   - Verify batch không bị remove

3. **Test với multiple stuck batches:**
   - Tạo nhiều batches bị stuck
   - Verify tất cả batches bị remove sau 1000 retries
   - Verify hệ thống vẫn hoạt động

## MONITORING

### Metrics:
1. **Số batches bị force remove:**
   - Log: "FORCE REMOVING stuck batch ..."
   - Đếm số batches bị remove

2. **Retry count distribution:**
   - Theo dõi retry count của các batches
   - Identify batches có retry count cao

3. **System health:**
   - Verify hệ thống vẫn hoạt động
   - Verify giao dịch mới vẫn được xử lý

## NEXT STEPS

### Ngắn Hạn:
1. ✅ **Implement force remove:**
   - Đã implement
   - Đang test

2. **Monitor logs:**
   - Theo dõi số batches bị force remove
   - Identify patterns

### Dài Hạn:
1. **Cải thiện early extraction:**
   - Đảm bảo headers không bị skip do quá cũ
   - Cải thiện logic "PROCESSING old header"

2. **Priority inclusion:**
   - Ưu tiên batches có retry_count cao
   - Giảm retry count

3. **Network improvements:**
   - Đảm bảo headers không bị skip do quá cũ
   - Cải thiện broadcast logic

## KẾT LUẬN

### Giải Pháp:
- ✅ **Force Remove:** Ngăn chặn hệ thống bị đứng vĩnh viễn
- ⚠️ **Trade-off:** Một số batches có thể bị drop, nhưng tốt hơn là hệ thống bị đứng

### Status:
- ✅ **Implemented:** Force remove logic
- ✅ **Build:** Successful
- ⏳ **Testing:** In progress
- ⏳ **Deployment:** Pending

---

**Last Updated:** 2025-01-20
**Status:** ✅ **FIX IMPLEMENTED**
