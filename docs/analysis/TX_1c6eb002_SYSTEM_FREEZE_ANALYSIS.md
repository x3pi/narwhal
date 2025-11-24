# PHÂN TÍCH: HỆ THỐNG ĐỨNG SAU 2 TIẾNG - GIAO DỊCH KHÔNG ĐƯỢC THỰC THI

## THÔNG TIN GIAO DỊCH

- **Transaction Hash**: `1c6eb0027dd2801c36cc3f6287ad24f3e00b15e833c779bded7936de5144ab35`
- **Worker**: Worker-0
- **Thời gian**: Sau khi hệ thống chạy được 2 tiếng
- **Vấn đề**: Giao dịch này bị đứng, sau đó tất cả giao dịch khác cũng không được thực thi

---

## PHÂN TÍCH NGUYÊN NHÂN

### 1. Vấn Đề Chính: Queue Đầy Batches Stuck

**Tình trạng:**
- Hệ thống chạy mượt 2 tiếng
- Sau đó một batch bị stuck (không được commit)
- Batch này bị retry liên tục nhưng không được commit
- Queue `digests` tích lũy nhiều batches stuck
- Queue đầy → không thể enqueue batch mới
- Hệ thống đứng hoàn toàn

**Nguyên nhân:**
1. **Primary-0 không phải leader** → Certificate không được commit
2. **Leader không extract batch** từ headers của primary-0
3. **Batch bị retry liên tục** nhưng không bao giờ commit
4. **MAX_RETRY_COUNT = 1000** → Batch phải retry 1000 lần mới bị force remove
5. **Queue không có giới hạn** → Tích lũy batches stuck

### 2. Channel Capacity Có Thể Đầy

**Thông tin:**
- `CHANNEL_CAPACITY = 1_000`
- Sau 2 tiếng, có thể có nhiều headers/certificates trong channel
- Nếu channel đầy, headers không được gửi đến proposer
- Proposer không extract batches → batches bị stuck

### 3. Memory Leak Từ Committed Digests

**Thông tin:**
- `committed_digests` được cleanup nhưng có thể không đủ
- Sau 2 tiếng, có thể tích lũy nhiều committed digests
- Memory tăng dần → có thể gây vấn đề

### 4. Queue Digests Không Có Giới Hạn

**Thông tin:**
- `digests: VecDeque::with_capacity(2 * header_size.max(1))`
- Không có giới hạn tối đa
- Sau 2 tiếng, queue có thể rất lớn
- Nếu có nhiều batches stuck, queue sẽ đầy

---

## TIMELINE DỰ ĐOÁN

```
T0: Hệ thống khởi động, chạy mượt
T1: Sau ~2 tiếng, một batch bị stuck (primary-0 không phải leader)
T2: Batch bị retry liên tục (retry_count tăng dần)
T3: Queue digests tích lũy batches stuck
T4: Queue đầy hoặc gần đầy
T5: Batch mới không thể enqueue
T6: Hệ thống đứng hoàn toàn
```

---

## VẤN ĐỀ CỤ THỂ

### 1. Queue Đầy Batches Stuck

**Vấn đề:**
- Queue `digests` không có giới hạn tối đa
- Nếu có nhiều batches stuck, queue sẽ tích lũy
- Sau 2 tiếng, có thể có hàng trăm/thousands batches stuck
- Queue đầy → không thể enqueue batch mới

**Code:**
```rust
// primary/src/proposer.rs:141
digests: VecDeque::with_capacity(2 * header_size.max(1)),
// Không có giới hạn tối đa!
```

### 2. MAX_RETRY_COUNT Quá Cao

**Vấn đề:**
- `MAX_RETRY_COUNT = 1000` → Batch phải retry 1000 lần mới bị force remove
- Sau 2 tiếng, một batch có thể retry hàng trăm lần
- Batch vẫn trong queue, chiếm chỗ
- Queue tích lũy batches stuck

**Code:**
```rust
// primary/src/proposer.rs:607
const MAX_RETRY_COUNT: usize = 1000; // Quá cao!
```

### 3. Channel Capacity Có Thể Đầy

**Vấn đề:**
- `CHANNEL_CAPACITY = 1_000`
- Nếu channel đầy, headers không được gửi đến proposer
- Proposer không extract batches → batches bị stuck

**Code:**
```rust
// primary/src/primary.rs:34
pub const CHANNEL_CAPACITY: usize = 1_000;
```

---

## GIẢI PHÁP

### Giải Pháp 1: Giảm MAX_RETRY_COUNT (URGENT)

**Mô tả:**
- Giảm `MAX_RETRY_COUNT` từ 1000 xuống 100 hoặc 50
- Force remove batch sớm hơn để giải phóng queue
- Tránh queue đầy batches stuck

**Trade-off:**
- ⚠️ Batch có thể bị mất nếu force remove quá sớm
- ✅ Nhưng hệ thống sẽ không bị đứng
- ✅ Queue không bị đầy

### Giải Pháp 2: Thêm Giới Hạn Queue Size (CRITICAL)

**Mô tả:**
- Thêm giới hạn tối đa cho queue `digests`
- Khi queue đầy, force remove batches cũ nhất
- Đảm bảo queue luôn có chỗ cho batch mới

**Implementation:**
```rust
const MAX_QUEUE_SIZE: usize = 10_000; // Giới hạn queue size

if self.digests.len() >= MAX_QUEUE_SIZE {
    // Force remove batches cũ nhất
    // Hoặc force remove batches có retry_count cao nhất
}
```

### Giải Pháp 3: Tăng Channel Capacity (MEDIUM-TERM)

**Mô tả:**
- Tăng `CHANNEL_CAPACITY` từ 1_000 lên 10_000 hoặc 50_000
- Tránh channel đầy sau 2 tiếng
- Đảm bảo headers luôn được gửi đến proposer

**Trade-off:**
- ✅ Tránh channel đầy
- ⚠️ Tăng memory usage

### Giải Pháp 4: Cải Thiện Leader Batch Extraction (LONG-TERM)

**Mô tả:**
- Đảm bảo leader **luôn extract batches** từ headers của non-leader primaries
- Không skip batch nếu batch chưa được commit
- Giảm số lượng batches stuck

**Implementation:**
- Cải thiện logic `extract_batches_from_headers`
- Không skip batch nếu batch chưa được commit

### Giải Pháp 5: Periodic Queue Cleanup (MEDIUM-TERM)

**Mô tả:**
- Thêm periodic cleanup cho queue `digests`
- Force remove batches quá cũ hoặc có retry_count quá cao
- Đảm bảo queue không tích lũy batches stuck

**Implementation:**
```rust
// Cleanup batches có retry_count > threshold hoặc quá cũ
fn cleanup_stuck_batches(&mut self) {
    // Remove batches với retry_count > 50
    // Hoặc batches quá cũ (rounds_since_sent > threshold)
}
```

---

## KHUYẾN NGHỊ

### Immediate (URGENT):
1. ⚠️ **Giảm MAX_RETRY_COUNT** từ 1000 xuống 100
2. ⚠️ **Thêm giới hạn queue size** để tránh queue đầy
3. ⚠️ **Tăng channel capacity** từ 1_000 lên 10_000

### Short-term:
1. ✅ **Periodic queue cleanup** để giải phóng batches stuck
2. ✅ **Cải thiện logging** để monitor queue size

### Long-term:
1. ✅ **Cải thiện leader batch extraction** để giảm batches stuck
2. ✅ **Thêm monitoring** cho queue size và channel capacity

---

## IMPACT

### Nếu Không Sửa:
- ❌ **Hệ thống sẽ đứng sau 2 tiếng** khi queue đầy batches stuck
- ❌ **Tất cả giao dịch mới không được thực thi**
- ❌ **Hệ thống mất liveness hoàn toàn**

### Sau Khi Sửa:
- ✅ **Queue không bị đầy** nhờ giới hạn queue size
- ✅ **Batches stuck được force remove sớm hơn** (retry_count < 100)
- ✅ **Channel không bị đầy** nhờ tăng capacity
- ✅ **Hệ thống tiếp tục hoạt động** ngay cả khi có batches stuck

