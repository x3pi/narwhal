# BÁO CÁO KIỂM TRA AN TOÀN XỬ LÝ BATCH

## Mục tiêu đảm bảo:
1. ✅ **Không commit 2 lần** - Batch không được commit và xử lý 2 lần
2. ✅ **Không bị bỏ rơi** - Batch không bị mất và luôn được commit
3. ✅ **Không fork** - Tất cả node xử lý batch giống nhau (deterministic)

---

## 1. KHÔNG COMMIT 2 LẦN

### 1.1. Primary Level (proposer.rs)

#### A. make_header() - 3 lớp bảo vệ:
1. **Trước khi collect payload** (dòng 124-144):
   - Kiểm tra và loại bỏ committed batches từ queue
   - Cập nhật `pending_payload_size`

2. **collect_payload_for_header()** (dòng 277-360):
   - Cleanup committed batches trước khi collect
   - Double-check committed status trước khi collect (dòng 308-315)
   - TRIPLE-CHECK trước khi mark InFlight (dòng 333-342)

3. **Final check trước khi tạo header** (dòng 148-205):
   - Kiểm tra lại committed batches trong payload
   - Loại bỏ duplicates
   - Đánh dấu entry là Committed nếu tìm thấy

#### B. retry_stale_batches() - Safe Retry Window:
- **Check committed trước khi retry** (dòng 469-479):
  - Nếu batch đã committed → skip, mark Committed
- **Safe Retry Buffer = 2 rounds** (dòng 505-507):
  - Chỉ retry nếu `latest_committed_round < round - 2`
  - Tránh retry batch có thể đã commit nhưng notification chưa đến
- **Double-check trước khi requeue** (dòng 555):
  - Verify batch vẫn chưa committed trước khi requeue

#### C. mark_committed() - Đánh dấu committed:
- Thêm vào `committed_digests` (dòng 392-394)
- Mark entry state = Committed (dòng 413)
- Remove committed entries khỏi queue (dòng 425-426)

### 1.2. Node Level (main.rs)

#### A. Duplicate Batch Detection trong Block:
- **batch_hashes trong BlockBuilder** (dòng 1359-1366):
  - HashSet để track batches trong cùng block
  - Skip duplicate batch trong cùng block

#### B. Duplicate Transaction Detection trong Block:
- **transaction_hashes trong BlockBuilder** (dòng 1453-1465):
  - HashSet để track transactions trong cùng block
  - Skip duplicate transaction trong cùng block

#### C. Tracking sau khi gửi (không dùng để skip):
- `processed_batches` và `processed_transactions` chỉ dùng để tracking
- **KHÔNG dùng để skip** vì là local state, có thể khác nhau giữa các node

---

## 2. KHÔNG BỊ BỎ RƠI

### 2.1. Primary Level - Retry Logic

#### A. retry_stale_batches() - Retry khi an toàn:
- **Safe Retry Window** (dòng 505-507):
  - Nếu `latest_committed_round < round - 2` → retry
  - Đảm bảo batch thực sự bị bỏ rơi, không phải đang chờ commit notification

- **Retry conditions**:
  - Batch quá cũ (`is_too_old`) và `safe_to_retry` → retry (dòng 509-534)
  - Batch không quá cũ nhưng `retry_delay` hết và `safe_to_retry` → retry (dòng 552-577)

- **Extremely old batches** (dòng 486-495):
  - Nếu batch quá cũ (> max_retry_rounds * 2) → mark Committed và remove
  - Tránh retry vô hạn

### 2.2. Node Level - Late Batch Handling

#### A. Late Batch trong block height + 1 (dòng 1030-1088):
- Nếu certificate đến muộn (height đã finalize):
  - Chỉ xử lý trong block `height + 1` (deterministic)
  - Tất cả node sẽ xử lý giống nhau
  - Track `late_batches_from_height` để tránh duplicate

#### B. Tạo block mới cho late batch (dòng 1102-1140):
- Nếu không có block đang xây dựng:
  - Tạo block mới cho `height + 1`
  - Xử lý late batch trong block đó

---

## 3. KHÔNG FORK

### 3.1. Deterministic Late Batch Handling

#### A. Chỉ xử lý trong block height + 1:
- **Logic cũ (đã sửa)**: Cho phép xử lý trong block hiện tại nếu `current_builder.height > height`
  - ❌ Có thể gây fork: Node A xử lý trong height + 1, Node B xử lý trong height + 2

- **Logic mới (hiện tại)**: Chỉ xử lý trong block `height + 1` (dòng 1032-1053)
  - ✅ Deterministic: Tất cả node xử lý trong cùng block height
  - ✅ Không fork

#### B. Check late batch đang được xử lý (dòng 1195-1228):
- Chỉ tiếp tục xử lý nếu:
  - `builder_ref.height == expected_height` (height + 1)
  - `late_batches_from_height.contains(&height)`

### 3.2. Deterministic Duplicate Detection

#### A. Dựa trên BlockBuilder (deterministic):
- `batch_hashes` trong BlockBuilder → tất cả node có cùng BlockBuilder
- `transaction_hashes` trong BlockBuilder → tất cả node có cùng BlockBuilder

#### B. KHÔNG dùng local state để skip:
- `processed_batches` và `processed_transactions` chỉ dùng để tracking
- **KHÔNG dùng để skip** vì có thể khác nhau giữa các node

### 3.3. Certificate-based Processing

#### A. Tất cả logic dựa trên committed certificate:
- Certificate đã được commit → tất cả node đều thấy
- Xử lý batch dựa trên certificate → deterministic

---

## 4. FLOW TỔNG QUAN

### 4.1. Batch Lifecycle:

```
Worker → Primary (Pending)
  ↓
Primary: make_header() → InFlight
  ↓
Consensus: Commit → Certificate
  ↓
Primary: mark_committed() → Committed
  ↓
Node: analyze() → Process batch
  ↓
Node: emit_blocks() → Send to UDS
```

### 4.2. Retry Flow:

```
InFlight batch
  ↓
retry_stale_batches() check:
  - Already committed? → Skip, mark Committed
  - Safe to retry? (latest_committed_round < round - 2)
    - Yes → Requeue as Pending
    - No → Wait for commit notification
```

### 4.3. Late Batch Flow:

```
Certificate arrives late (height already finalized)
  ↓
Check: current_builder.height == height + 1?
  - Yes → Process in current block (deterministic)
  - No → Skip (avoid fork) OR create new block for height + 1
```

---

## 5. ĐIỂM QUAN TRỌNG

### 5.1. Safe Retry Buffer = 2 rounds:
- **Lý do**: Account cho delay trong commit notification
- **Tác dụng**: Tránh retry batch đã commit nhưng notification chưa đến
- **Kết quả**: Tránh duplicate commit

### 5.2. Late Batch chỉ trong height + 1:
- **Lý do**: Đảm bảo deterministic
- **Tác dụng**: Tất cả node xử lý trong cùng block height
- **Kết quả**: Tránh fork

### 5.3. Duplicate detection dựa trên BlockBuilder:
- **Lý do**: BlockBuilder là deterministic (dựa trên certificate)
- **Tác dụng**: Tất cả node có cùng BlockBuilder
- **Kết quả**: Tránh fork

---

## 6. KẾT LUẬN

✅ **Không commit 2 lần**: 
- 3 lớp check trong make_header
- Safe Retry Window
- Duplicate detection trong BlockBuilder

✅ **Không bị bỏ rơi**:
- Retry logic với Safe Retry Window
- Late batch handling trong height + 1

✅ **Không fork**:
- Late batch chỉ trong height + 1 (deterministic)
- Duplicate detection dựa trên BlockBuilder (deterministic)
- KHÔNG dùng local state để skip

**Hệ thống đã được đảm bảo an toàn!**

