# PHÂN TÍCH: BATCH BỊ STUCK NGHIÊM TRỌNG - HỆ THỐNG BỊ ĐỨNG

## VẤN ĐỀ NGHIÊM TRỌNG

### Tình Trạng:
1. **Giao dịch `36d48e423784e9f86d49b8e94783b1db15364df340f83eca34cddd03ab05bcbf`:**
   - Batch: `568I4QfDKWSGnkBIFT9whjCxhKD/he1CQ7Aw8P0y9+4=`
   - Tạo lúc: 2025-11-19T12:10:03.457Z
   - Retry count: **110,027 lần** (retry_count=110027)
   - Vẫn không được commit

2. **Giao dịch `fff758814803714e1c5e5401df990b402a77b09e188a99448625b5a7661ea8d9`:**
   - Batch: `15fiCGx7M43Igi8waJhmOFreV5hYRH6tovjw72Q423w=`
   - Tạo lúc: 2025-11-20T00:13:52.873Z
   - Vẫn không được commit

3. **Hệ thống bị đứng:**
   - Tất cả giao dịch mới không được thực thi
   - Hệ thống chỉ tạo EMPTY blocks (0 transactions)
   - Batches bị retry hàng trăm nghìn lần nhưng không bao giờ commit

## PHÂN TÍCH NGUYÊN NHÂN

### Timeline:

```
T0: 2025-11-19T12:10:03.457Z - Worker tạo batch 568I4QfDKWSGnkBI và gửi tới primary-0
T1: Primary-0 đưa batch vào header round ~474900 (ước tính dựa trên timeline)
T2: Primary-0 broadcast header qua network
T3: Headers của primary-0 không được commit (primary-0 không phải leader)
T4: Batch bị retry liên tục
T5: 2025-11-19T20:05:23 - Current round ~584917, batch vẫn bị retry (retry_count=110027)
T6: Headers từ round 474900 đã quá cũ (>100,000 rounds)
T7: Các primary khác không nhận được headers từ primary-0 (quá cũ, network không broadcast)
T8: Early extraction không hoạt động (chỉ hoạt động cho headers mới nhận được)
T9: Batch không bao giờ được commit → HỆ THỐNG BỊ ĐỨNG
```

### Vấn Đề Chính:

1. **Headers Quá Cũ:**
   - Headers từ round 474900 đã quá cũ (>100,000 rounds so với round 584917)
   - Network có thể không broadcast headers quá cũ
   - Các primary khác không nhận được headers từ primary-0

2. **Early Extraction Không Hoạt Động:**
   - Early extraction chỉ hoạt động cho headers **mới nhận được** từ network
   - Headers quá cũ không được nhận → early extraction không áp dụng
   - Logic "PROCESSING old header" không được trigger vì headers không được nhận

3. **Extract từ Parents Không Giúp:**
   - Extract từ parents chỉ hoạt động cho **certificates đã commit**
   - Certificates của primary-0 không được commit
   - Do đó, batch không xuất hiện trong parent certificates

4. **Retry Logic Vô Hạn:**
   - Batch bị retry liên tục (retry_count > 100,000)
   - Nhưng vẫn không được commit vì:
     - Headers của primary-0 không được commit
     - Các primary khác không extract batch (headers quá cũ)
   - Logic `is_extremely_old` (max_retry_rounds * 2 = 2000) chưa xảy ra
   - Batch vẫn trong InFlight state và bị retry mãi

## NGUYÊN NHÂN GỐC RỄ

### 1. Headers Không Được Network Broadcast

**Vấn đề:**
- Network có thể không broadcast headers quá cũ
- Headers từ round 474900 đã quá cũ (>100,000 rounds)
- Các primary khác không nhận được headers từ primary-0

**Chứng cứ:**
- Không có log "EXTRACTED batch ... from header ... author: AqJy7eip40qqZk7F" trong logs của các primary khác
- Không có log "PROCESSING old header" trong logs

### 2. Early Extraction Chỉ Hoạt Động Cho Headers Mới

**Vấn đề:**
- Early extraction chỉ hoạt động cho headers **mới nhận được** từ network
- Headers quá cũ không được nhận → early extraction không áp dụng
- Logic "PROCESSING old header" không được trigger

**Code:**
```rust
// primary/src/core.rs:438-443
if header.author != self.name {
    if let Err(e) = self.tx_headers.send(header.clone()).await {
        // Header được gửi tới proposer EARLY
        // Nhưng chỉ khi header được NHẬN từ network
    }
}
```

### 3. Extract Từ Parents Không Giúp

**Vấn đề:**
- Extract từ parents chỉ hoạt động cho **certificates đã commit**
- Certificates của primary-0 không được commit
- Do đó, batch không xuất hiện trong parent certificates

**Code:**
```rust
// primary/src/proposer.rs:689-849
// Extract batches from parent certificates (ĐÃ COMMIT)
for parent_digest in parent_digests {
    match self.store.read(parent_digest.to_vec()).await {
        Ok(Some(bytes)) => {
            match bincode::deserialize::<Certificate>(&bytes) {
                // Chỉ extract từ certificates ĐÃ COMMIT
            }
        }
    }
}
```

### 4. Retry Logic Không Có Cơ Chế Dừng

**Vấn đề:**
- Batch bị retry liên tục (retry_count > 100,000)
- Logic `is_extremely_old` (max_retry_rounds * 2 = 2000) chưa xảy ra
- Batch vẫn trong InFlight state và bị retry mãi

**Code:**
```rust
// primary/src/proposer.rs:490-503
let is_extremely_old = self.round > round.saturating_add(self.max_retry_rounds * 2);

if is_extremely_old {
    // Remove extremely old batches
    entry.state = BatchState::Committed;
    removed_too_old += 1;
    continue;
}
```

**Vấn đề:** Logic này chỉ remove batch nếu `current_round > sent_round + 2000`, nhưng batch đang bị retry liên tục nên `sent_round` luôn được update → `is_extremely_old` không bao giờ xảy ra!

## GIẢI PHÁP ĐỀ XUẤT

### Giải Pháp 1: Dựa Vào Retry Count (Recommended)

**Ý tưởng:**
- Khi `retry_count > THRESHOLD` (ví dụ: 100), batch được coi là "stuck forever"
- Khi batch bị stuck, primary-0 nên **force include** batch vào header với **priority cao**
- Hoặc: Khi batch bị stuck, các primary khác (leader) nên **proactively extract** batch từ store

**Implementation:**
```rust
// Trong retry_stale_batches:
const STUCK_BATCH_THRESHOLD: usize = 100; // Nếu retry > 100, batch bị stuck

if retry_count > STUCK_BATCH_THRESHOLD {
    // Batch bị stuck - force include với priority cao
    // Hoặc: Broadcast batch digest tới các primary khác
    // Hoặc: Remove batch và log error
}
```

### Giải Pháp 2: Extract Từ Store (Không Khả Thi)

**Ý tưởng:**
- Khi batch bị stuck, leader extract batch từ store (từ headers đã stored)
- Tìm headers chứa batch trong store

**Vấn đề:**
- Store không có API để search headers theo batch digest
- Cần scan toàn bộ store → không hiệu quả
- Không khả thi

### Giải Pháp 3: Broadcast Batch Digest Khi Stuck

**Ý tưởng:**
- Khi batch bị retry quá nhiều lần, primary-0 broadcast batch digest tới các primary khác
- Các primary khác extract batch từ store (nếu có)

**Implementation:**
```rust
// Thêm PrimaryMessage::StuckBatchRequest(Digest, PublicKey)
// Khi retry_count > THRESHOLD, broadcast stuck batch request
// Các primary khác extract batch từ store (nếu có)
```

**Vấn đề:**
- Cần thêm network message type
- Có thể gây overhead nếu có nhiều stuck batches

### Giải Pháp 4: Force Remove Batch Sau N Retries (Critical Fix)

**Ý tưởng:**
- Khi `retry_count > THRESHOLD` (ví dụ: 1000), batch được coi là "stuck forever"
- Force remove batch và log error
- Đảm bảo hệ thống không bị đứng vĩnh viễn

**Implementation:**
```rust
const MAX_RETRY_COUNT: usize = 1000; // Nếu retry > 1000, batch bị stuck forever

if retry_count > MAX_RETRY_COUNT {
    // Force remove batch - hệ thống không thể commit batch này
    warn!(
        "FORCE REMOVING stuck batch {} (retry_count={}, sent_round={}, current_round={}). Batch cannot be committed - removing to prevent system deadlock.",
        entry.digest, retry_count, round, self.round
    );
    entry.state = BatchState::Committed; // Mark as committed to remove
    removed_too_old += 1;
    continue;
}
```

**Lợi ích:**
- Ngăn chặn hệ thống bị đứng vĩnh viễn
- Batch sẽ bị drop, nhưng hệ thống vẫn hoạt động
- Giao dịch mới vẫn được xử lý

**Rủi ro:**
- Batch bị drop → giao dịch không được thực thi
- Nhưng tốt hơn là hệ thống bị đứng hoàn toàn

### Giải Pháp 5: Priority Inclusion Khi Stuck

**Ý tưởng:**
- Khi batch bị retry quá nhiều lần, primary-0 nên **force include** batch vào header với **priority cao**
- Đảm bảo batch được include trong header tiếp theo, không cần đợi `collect_payload_for_header` bình thường

**Implementation:**
```rust
// Trong collect_payload_for_header:
// Ưu tiên batches có retry_count cao
let mut batches_sorted = self.digests.iter()
    .filter(|e| matches!(e.state, BatchState::Pending))
    .collect::<Vec<_>>();
batches_sorted.sort_by_key(|e| e.retry_count); // Sort by retry_count DESC
// Include batches có retry_count cao trước
```

**Lợi ích:**
- Batch được include sớm hơn
- Giảm retry count

**Vấn đề:**
- Vẫn không giải quyết vấn đề nếu headers của primary-0 không được commit

## KHUYẾN NGHỊ

### Giải Pháp Kết Hợp (Recommended):

1. **Giải Pháp 4 (Force Remove):** Ngăn chặn hệ thống bị đứng vĩnh viễn
   - Khi `retry_count > MAX_RETRY_COUNT` (ví dụ: 1000), force remove batch
   - Log error để tracking

2. **Giải Pháp 5 (Priority Inclusion):** Tăng cơ hội commit
   - Khi batch bị retry, ưu tiên include vào header
   - Giảm retry count

3. **Cải Thiện Early Extraction:**
   - Đảm bảo headers không bị skip do quá cũ
   - Cải thiện logic "PROCESSING old header"

## IMPACT

### Nếu Không Sửa:
- **Hệ thống bị đứng hoàn toàn:**
  - Batches bị retry vô hạn
  - Không có giao dịch nào được thực thi
  - Hệ thống chỉ tạo EMPTY blocks
  - **Không thể sử dụng**

### Sau Khi Sửa:
- **Hệ thống vẫn hoạt động:**
  - Batches bị stuck sẽ bị remove sau N retries
  - Giao dịch mới vẫn được xử lý
  - Hệ thống không bị đứng
  - **Có thể sử dụng, nhưng một số batches có thể bị drop**

---

**Last Updated:** 2025-01-20
**Status:** 🔴 **CRITICAL - System Deadlock**

