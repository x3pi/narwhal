# PHÂN TÍCH: PRIMARY-0 BỊ STUCK - DỪNG TẠO HEADERS

## VẤN ĐỀ

### Tình Trạng:
1. **Primary-0 dừng tạo headers sau round 584917 lúc 20:05:23**
2. **Core vẫn gửi parent certificates tới proposer (round 584897-584916)**
3. **Proposer vẫn nhận batches từ workers (lúc 00:13:52 - batch mới được enqueued)**
4. **Không có log "preparing header round" sau 584916**

### Timeline:

```
20:05:23.846 - Core: preparing header round 584916 with 4 parents
20:05:23.849 - Creating header for round 584917 (headers cuối cùng)
20:05:24.243 - Requeue batch ztzQKm+nrI2Dfu4N và 568I4QfDKWSGnkBI (retry)
...
00:13:52.874 - Batch 15fiCGx7M43Igi8w enqueued (batch mới)
00:13:52.874 - Proposer vẫn ở round 584917 (KHÔNG TẠO HEADERS MỚI)
```

## PHÂN TÍCH NGUYÊN NHÂN

### Vấn Đề 1: Proposer Không Nhận Được Parent Certificates Sau Round 584916

**Dấu hiệu:**
- Core vẫn gửi parent certificates (round 584916 là cuối cùng)
- Không có log "preparing header round" sau 584916
- Proposer vẫn ở round 584917 (không advance)

**Code logic:**
```rust
// primary/src/proposer.rs:1159-1177
Some((parents, round)) = self.rx_core.recv() => {
    if round < self.round {
        continue; // Skip if round < current round
    }
    // Advance to the next round
    self.round = round + 1;
    debug!("Dag moved to round {}", self.round);
    // ...
    self.last_parents = parents;
}
```

**Vấn đề:**
- Nếu proposer không nhận được parent certificates từ Core, `self.round` không được advance
- `self.last_parents` vẫn giữ giá trị cũ
- Proposer vẫn cố tạo headers với `last_parents` cũ, nhưng không thành công

### Vấn Đề 2: Điều Kiện Tạo Header Không Được Thỏa Mãn

**Code logic:**
```rust
// primary/src/proposer.rs:1143-1157
let enough_parents = !self.last_parents.is_empty();
let enough_digests = self.pending_payload_size >= self.header_size;
let timer_expired = header_timer.is_elapsed();

if (timer_expired || enough_digests) && enough_parents {
    // Make a new header
    if self.make_header().await {
        // ...
    }
}
```

**Vấn đề:**
- Nếu `enough_parents = false` (last_parents is empty), proposer không tạo headers
- Nếu `enough_digests = false` (pending_payload_size < header_size), proposer chỉ tạo headers khi timer expired
- Nếu timer không expired và không có enough digests, proposer không tạo headers

### Vấn Đề 3: Core Không Gửi Parent Certificates Sau Round 584916

**Dấu hiệu:**
- Không có log "preparing header round" sau 584916
- Core có thể đã dừng xử lý certificates sau round 584916

**Nguyên nhân có thể:**
1. **Network issues:** Core không nhận được certificates từ các primary khác
2. **Consensus issues:** Consensus không commit certificates sau round 584916
3. **Synchronizer issues:** Synchronizer không đồng bộ certificates

## KẾT LUẬN

### Nguyên Nhân Chính:
**Primary-0 đã bị bỏ rơi (lagging behind) các primary khác:**

1. **Headers quá cũ:**
   - Headers từ round 474900 đã quá cũ (>100,000 rounds)
   - Network không broadcast headers quá cũ
   - Các primary khác không nhận được headers từ primary-0

2. **Proposer không nhận parent certificates:**
   - Core có thể đã dừng gửi parent certificates sau round 584916
   - Hoặc proposer không nhận được parent certificates (channel full/closed)

3. **Round không advance:**
   - Proposer vẫn ở round 584917 (không advance)
   - Không tạo headers mới
   - Batches bị stuck trong queue

### Hệ Quả:
- **Primary-0 bị tách khỏi network:**
  - Headers quá cũ không được broadcast
  - Các primary khác không nhận được headers từ primary-0
  - Primary-0 không nhận được certificates từ các primary khác

- **Hệ thống bị đứng:**
  - Primary-0 không tạo headers mới
  - Batches bị stuck trong queue
  - Giao dịch mới không được xử lý

## GIẢI PHÁP

### Giải Pháp 1: Force Advance Round Khi Stuck (Recommended)

**Ý tưởng:**
- Nếu proposer không nhận được parent certificates trong thời gian dài, force advance round
- Hoặc: Nếu `last_parents` empty quá lâu, force advance round với empty parents

**Implementation:**
```rust
// Trong proposer run loop:
// Nếu không nhận được parent certificates trong N seconds, force advance
let last_parent_received = Instant::now();
const MAX_PARENT_WAIT: Duration = Duration::from_secs(10);

// Trong timer check:
if last_parent_received.elapsed() > MAX_PARENT_WAIT {
    // Force advance round với empty parents
    self.round += 1;
    self.last_parents = Vec::new();
    warn!("Force advancing round {} due to no parent certificates received", self.round);
}
```

### Giải Pháp 2: Cải Thiện Network Synchronization

**Ý tưởng:**
- Đảm bảo Core luôn nhận được certificates từ các primary khác
- Cải thiện synchronizer để sync certificates tốt hơn

### Giải Pháp 3: Force Remove Stuck Batches (Đã Implement)

**Ý tưởng:**
- Nếu batch bị retry quá nhiều lần (retry_count > 1000), force remove
- Đảm bảo hệ thống không bị đứng vĩnh viễn

**Status:** ✅ Đã implement trong `STUCK_BATCH_FIX.md`

## MONITORING

### Metrics Cần Theo Dõi:
1. **Round lag:** Round của primary-0 vs các primary khác
2. **Parent certificates:** Số parent certificates nhận được mỗi giây
3. **Header creation rate:** Số headers tạo mỗi giây
4. **Batch queue size:** Số batches trong queue

### Alerts:
1. **Round lag > N:** Primary đang lag quá nhiều
2. **No parent certificates trong N seconds:** Primary có thể bị tách khỏi network
3. **No headers created trong N seconds:** Proposer có thể bị stuck

## NEXT STEPS

### Ngắn Hạn:
1. **Implement Force Advance Round:** Ngăn chặn proposer bị stuck
2. **Monitor logs:** Theo dõi round lag và parent certificates

### Dài Hạn:
1. **Cải thiện network synchronization:** Đảm bảo các primary luôn đồng bộ
2. **Cải thiện early extraction:** Đảm bảo headers không bị skip do quá cũ
3. **Network improvements:** Đảm bảo headers không bị skip do quá cũ

---

**Last Updated:** 2025-01-20
**Status:** 🔴 **CRITICAL - Primary-0 Stuck**

