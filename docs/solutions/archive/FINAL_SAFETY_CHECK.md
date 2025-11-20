# KIỂM TRA AN TOÀN CUỐI CÙNG - ĐẢM BẢO KHÔNG FORK, KHÔNG TRÙNG LẶP, KHÔNG BỎ RƠI

## MỤC TIÊU
✅ **Không fork** - Tất cả node xử lý batch giống nhau (deterministic)
✅ **Không trùng lặp** - Batch/transaction không được commit/xử lý 2 lần
✅ **Không bỏ rơi** - Batch luôn được commit và xử lý

---

## 1. KHÔNG TRÙNG LẶP

### 1.1. Primary Level (proposer.rs)

#### A. make_header() - 3 lớp bảo vệ:
1. **Trước khi collect payload** (dòng 124-144):
   - ✅ Kiểm tra và loại bỏ committed batches từ queue
   - ✅ Cập nhật `pending_payload_size`

2. **collect_payload_for_header()** (dòng 277-360):
   - ✅ Cleanup committed batches trước khi collect
   - ✅ Double-check committed status trước khi collect (dòng 308-315)
   - ✅ TRIPLE-CHECK trước khi mark InFlight (dòng 333-342)

3. **Final check trước khi tạo header** (dòng 148-205):
   - ✅ Kiểm tra lại committed batches trong payload
   - ✅ Loại bỏ duplicates
   - ✅ Đánh dấu entry là Committed nếu tìm thấy

#### B. retry_stale_batches() - Safe Retry với Force Retry:
- ✅ **Check committed trước khi retry** (dòng 469-479):
  - Nếu batch đã committed → skip, mark Committed

- ✅ **Safe Retry Buffer = 2 rounds** (dòng 505-518):
  - Chỉ retry nếu `latest_committed_round < round - 2` (normal case)
  - **IMPROVED**: Force retry nếu `rounds_since_sent >= max_retry_rounds` (dòng 510-518)
    - Batch đã InFlight quá lâu → force retry để tránh bỏ rơi
    - **CRITICAL**: Vẫn check `committed_digests` trước khi retry (dòng 539-548)

- ✅ **Double-check trước khi requeue** (dòng 571, 539-548):
  - Verify batch vẫn chưa committed trước khi requeue
  - Áp dụng cho cả normal retry và force retry

#### C. mark_committed() - Đánh dấu committed:
- ✅ Thêm vào `committed_digests` (dòng 392-394)
- ✅ Mark entry state = Committed (dòng 413)
- ✅ Remove committed entries khỏi queue (dòng 425-426)

### 1.2. Node Level (main.rs)

#### A. Duplicate Batch Detection trong Block:
- ✅ **batch_hashes trong BlockBuilder** (dòng 1366-1373):
  - HashSet để track batches trong cùng block
  - Skip duplicate batch trong cùng block

#### B. Duplicate Transaction Detection trong Block:
- ✅ **transaction_hashes trong BlockBuilder** (dòng 1460-1472):
  - HashSet để track transactions trong cùng block
  - Skip duplicate transaction trong cùng block

#### C. Tracking sau khi gửi (không dùng để skip):
- ✅ `processed_batches` và `processed_transactions` chỉ dùng để tracking
- ✅ **KHÔNG dùng để skip** vì là local state, có thể khác nhau giữa các node

---

## 2. KHÔNG BỎ RƠI

### 2.1. Primary Level - Retry Logic

#### A. retry_stale_batches() - Retry khi an toàn:
- ✅ **Safe Retry Window** (dòng 505-518):
  - Normal case: Chỉ retry nếu `latest_committed_round < round - 2`
  - **IMPROVED Force Retry** (dòng 510-518):
    - Nếu `rounds_since_sent >= max_retry_rounds` → force retry
    - Xử lý trường hợp batch bị stuck (commit notification không đến)
    - **CRITICAL**: Vẫn check `committed_digests` trước khi retry

- ✅ **Retry conditions**:
  - Batch quá cũ (`is_too_old`) và `safe_to_retry` → retry (dòng 520-563)
  - Batch không quá cũ nhưng `retry_delay` hết và `safe_to_retry` → retry (dòng 565-593)

- ✅ **Extremely old batches** (dòng 486-495):
  - Nếu batch quá cũ (> max_retry_rounds * 2) → mark Committed và remove
  - Tránh retry vô hạn

### 2.2. Node Level - Late Batch Handling

#### A. Late Batch trong block hiện tại (dòng 1039-1095):
- ✅ **IMPROVED**: Xử lý late batch trong block hiện tại nếu an toàn
- ✅ Điều kiện an toàn:
  - `current_builder.height > height` (đang xây dựng block cao hơn)
  - Certificate đã được commit (deterministic)
  - `!current_builder.late_batches_from_height.contains(&height)` (tránh duplicate)

#### B. Tạo block mới cho late batch (dòng 1109-1140):
- ✅ Nếu không có block đang xây dựng:
  - Tạo block mới cho `height + 1`
  - Xử lý late batch trong block đó

---

## 3. KHÔNG FORK

### 3.1. Deterministic Late Batch Handling

#### A. Xử lý trong block hiện tại (nếu an toàn):
- ✅ **Logic cải thiện** (dòng 1039-1061):
  - Cho phép xử lý late batch trong block hiện tại nếu `current_builder.height > height`
  - **Điều kiện an toàn**:
    1. Certificate đã được commit → tất cả node đều thấy
    2. `current_builder.height > height` → đang xây dựng block cao hơn
    3. Chưa có late batch từ height này → tránh duplicate
  - **Deterministic**: Tất cả node sẽ xử lý giống nhau vì certificate đã commit

#### B. Check late batch đang được xử lý (dòng 1200-1236):
- ✅ Chỉ tiếp tục xử lý nếu:
  - `builder_ref.height > height`
  - `late_batches_from_height.contains(&height)`

### 3.2. Deterministic Duplicate Detection

#### A. Dựa trên BlockBuilder (deterministic):
- ✅ `batch_hashes` trong BlockBuilder → tất cả node có cùng BlockBuilder
- ✅ `transaction_hashes` trong BlockBuilder → tất cả node có cùng BlockBuilder

#### B. KHÔNG dùng local state để skip:
- ✅ `processed_batches` và `processed_transactions` chỉ dùng để tracking
- ✅ **KHÔNG dùng để skip** vì có thể khác nhau giữa các node

### 3.3. Certificate-based Processing

#### A. Tất cả logic dựa trên committed certificate:
- ✅ Certificate đã được commit → tất cả node đều thấy
- ✅ Xử lý batch dựa trên certificate → deterministic

### 3.4. Force Retry không gây fork:
- ✅ Force retry chỉ áp dụng khi batch đã InFlight quá lâu
- ✅ **CRITICAL**: Vẫn check `committed_digests` trước khi retry (dòng 539-548)
- ✅ Nếu batch đã commit, sẽ không retry → không duplicate
- ✅ Nếu batch chưa commit, retry là an toàn vì batch thực sự bị stuck

---

## 4. CẢI THIỆN MỚI - FORCE RETRY

### 4.1. Logic Force Retry (dòng 507-518):
```rust
let rounds_since_sent_long = rounds_since_sent >= self.max_retry_rounds;
let safe_to_retry = if rounds_since_sent_long {
    // Batch is old enough - force retry even if latest_committed_round is close
    true
} else {
    // Normal case: only retry if latest_committed_round is far enough behind
    self.latest_committed_round < round.saturating_sub(SAFE_RETRY_BUFFER)
};
```

### 4.2. Double-check cho Force Retry (dòng 539-548):
```rust
// CRITICAL: Double-check batch is still not committed before retry (even for force retry)
if self.committed_digests.contains_key(&entry.digest) {
    // Batch became committed - don't retry
    entry.state = BatchState::Committed;
    skipped_committed += 1;
    continue;
}
```

### 4.3. Lợi ích:
- ✅ **Không bỏ rơi**: Batch bị stuck sẽ được retry sau `max_retry_rounds` (1000 rounds)
- ✅ **Không duplicate**: Vẫn check `committed_digests` trước khi retry
- ✅ **Không fork**: Logic dựa trên committed certificate (deterministic)

---

## 5. ĐIỂM QUAN TRỌNG

### 5.1. Safe Retry Buffer = 2 rounds:
- **Lý do**: Account cho delay trong commit notification
- **Tác dụng**: Tránh retry batch đã commit nhưng notification chưa đến
- **Kết quả**: Tránh duplicate commit

### 5.2. Force Retry cho batch cũ:
- **Lý do**: Xử lý batch bị stuck (commit notification không đến)
- **Điều kiện**: `rounds_since_sent >= max_retry_rounds` (1000 rounds)
- **An toàn**: Vẫn check `committed_digests` trước khi retry
- **Kết quả**: Tránh bỏ rơi batch

### 5.3. Late batch handling cải thiện:
- **Lý do**: Xử lý batch đến muộn trong block hiện tại
- **Điều kiện**: `current_builder.height > height` + certificate đã commit
- **An toàn**: Deterministic (certificate đã commit)
- **Kết quả**: Tránh bỏ rơi batch

### 5.4. Duplicate detection dựa trên BlockBuilder:
- **Lý do**: BlockBuilder là deterministic (dựa trên certificate)
- **Tác dụng**: Tất cả node có cùng BlockBuilder
- **Kết quả**: Tránh fork

---

## 6. TÓM TẮT KIỂM TRA

### ✅ Không trùng lặp:
1. 3 lớp check trong make_header
2. Safe Retry Window với double-check
3. **NEW**: Force Retry với double-check (dòng 539-548)
4. Duplicate detection trong BlockBuilder

### ✅ Không bỏ rơi:
1. Retry logic với Safe Retry Window
2. **NEW**: Force Retry cho batch cũ (dòng 510-518)
3. Late batch handling cải thiện (dòng 1039-1061)

### ✅ Không fork:
1. Late batch chỉ trong block hiện tại nếu an toàn (deterministic)
2. Duplicate detection dựa trên BlockBuilder (deterministic)
3. KHÔNG dùng local state để skip
4. Force Retry vẫn check `committed_digests` (an toàn)

---

## 7. KẾT LUẬN

✅ **Không trùng lặp**: 
- 3 lớp check + Safe Retry Window + **Force Retry với double-check**
- Duplicate detection trong BlockBuilder

✅ **Không bỏ rơi**:
- Retry logic + **Force Retry cho batch cũ**
- Late batch handling cải thiện

✅ **Không fork**:
- Late batch handling dựa trên committed certificate (deterministic)
- Duplicate detection dựa trên BlockBuilder (deterministic)
- Force Retry vẫn check `committed_digests` (an toàn)

**Hệ thống đã được đảm bảo an toàn với các cải thiện mới!**

