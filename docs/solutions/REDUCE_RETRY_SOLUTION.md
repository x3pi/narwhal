# GIẢI PHÁP: GIẢM RETRY - HỆ THỐNG MƯỢT MÀ HƠN

## VẤN ĐỀ

Retry nhiều làm hệ thống không mượt:
1. Batch được gửi đi gửi lại nhiều lần → tốn tài nguyên
2. Header chứa nhiều batches cũ → tăng kích thước header
3. Consensus phải xử lý nhiều headers với cùng batches → giảm throughput
4. Network traffic tăng → tăng latency

## NGUYÊN NHÂN

### 1. Batch bị stuck ở trạng thái InFlight

- Batch được gửi trong header của non-leader primary
- Certificate của primary đó không được commit
- Batch ở trạng thái InFlight, không được include trong header tiếp theo
- Batch phải chờ retry logic (sau 1000 rounds hoặc khi certificate không được commit)

### 2. Leader không thể include batch ngay lập tức

- Logic extract batches skip batch nếu nó đã có trong queue
- Nếu batch đang ở InFlight, leader không thể extract và include nó
- Batch phải chờ retry logic

## GIẢI PHÁP ĐÃ ÁP DỤNG

### 1. Convert InFlight to Pending khi extract từ parent certificates

**Logic cải thiện**:
- Nếu batch đã có trong queue ở trạng thái **Pending**: Skip extraction (nó sẽ được include trong header tiếp theo)
- Nếu batch đã có trong queue ở trạng thái **InFlight**: Convert InFlight → Pending để có thể được include ngay lập tức

**Code**:
```rust
// CRITICAL: Check if batch is already in queue
// If batch is already in queue and in Pending state, skip extraction
// (it will be included in next header anyway)
// If batch is in InFlight state and not committed, we should STILL extract it
// because this primary (possibly leader) can include it immediately,
// reducing the need for retry and making the system smoother
let already_in_queue = self.digests.iter().any(|entry| entry.digest == *batch_digest);
if already_in_queue {
    // Check if batch is in Pending state
    let is_pending = self.digests.iter()
        .any(|entry| entry.digest == *batch_digest 
            && matches!(entry.state, BatchState::Pending));
    
    if is_pending {
        // Batch is already in Pending state - skip extraction
        batches_skipped_duplicate += 1;
        continue;
    }
    
    // If batch is in InFlight state, convert it back to Pending state
    // This allows leader to include batch immediately in next header, reducing retry
    for entry in self.digests.iter_mut() {
        if entry.digest == *batch_digest 
            && matches!(entry.state, BatchState::InFlight { .. }) {
            // Convert InFlight to Pending to allow immediate inclusion
            self.pending_payload_size += entry.size;
            entry.state = BatchState::Pending;
            batches_added += 1;
            break;
        }
    }
    continue; // Skip adding new entry since we just converted existing one
}
```

### 2. Retry ngay lập tức khi certificate không được commit

**Logic cải thiện**:
- Nếu `latest_committed_round >= sent_round` nhưng batch không có trong `committed_digests`, có nghĩa là certificate của primary này không được commit
- Retry ngay lập tức (không chờ SAFE_RETRY_BUFFER)

**Code**:
```rust
let batch_is_actually_committed = self.committed_digests.contains_key(&entry.digest);
let own_certificate_not_committed = !batch_is_actually_committed 
    && self.latest_committed_round >= round;

let safe_to_retry = if rounds_since_sent_long || has_been_retried_multiple_times || own_certificate_not_committed {
    // Batch is old enough, has been retried multiple times, or certificate of this primary was not committed
    // Force retry to avoid batch being dropped forever
    true
} else {
    // Normal case: only retry if latest_committed_round is far enough behind
    self.latest_committed_round < round.saturating_sub(SAFE_RETRY_BUFFER)
};
```

## CÁCH HOẠT ĐỘNG

### Scenario 1: Batch từ Primary A, Leader là Primary B

**Trước khi sửa**:
1. Round N: Primary A tạo header với batch X
2. Round N: Certificate của Primary A không được commit (Primary B là leader)
3. Round N+1: Batch X ở trạng thái InFlight trong queue của Primary A
4. Round N+1: Primary B (leader) extract batch X từ parent certificates
5. Round N+1: Primary B skip extraction vì batch X đã có trong queue (InFlight)
6. Round N+1: Primary B không include batch X → Batch X phải chờ retry
7. Round N+1000: Batch X được retry → **KHÔNG MƯỢT!**

**Sau khi sửa**:
1. Round N: Primary A tạo header với batch X
2. Round N: Certificate của Primary A không được commit (Primary B là leader)
3. Round N+1: Batch X ở trạng thái InFlight trong queue của Primary A
4. Round N+1: Primary B (leader) extract batch X từ parent certificates
5. Round N+1: Primary B convert batch X từ InFlight → Pending trong queue của Primary B
6. Round N+1: Primary B include batch X trong header → Batch X được commit → **MƯỢT MÀ!**

### Scenario 2: Batch từ Primary A, Leader là Primary A

**Trước khi sửa**:
1. Round N: Primary A tạo header với batch X
2. Round N: Certificate của Primary A không được commit (certificate khác được commit thay thế)
3. Round N+1: Batch X ở trạng thái InFlight trong queue của Primary A
4. Round N+1: Primary A skip extraction vì batch X đã có trong queue (InFlight)
5. Round N+1: Primary A không include batch X → Batch X phải chờ retry
6. Round N+2: Primary A retry batch X (vì own_certificate_not_committed = true) → **TỐT HƠN NHƯNG VẪN CHẬM**

**Sau khi sửa**:
1. Round N: Primary A tạo header với batch X
2. Round N: Certificate của Primary A không được commit
3. Round N+1: Batch X ở trạng thái InFlight trong queue của Primary A
4. Round N+1: Primary A extract batch X từ parent certificates
5. Round N+1: Primary A convert batch X từ InFlight → Pending
6. Round N+1: Primary A include batch X trong header → Batch X được commit → **MƯỢT MÀ!**

## ĐẢM BẢO KHÔNG FORK, KHÔNG BỎ RƠI, KHÔNG TRÙNG LẶP

### 1. Không Fork

- Extract batches dựa trên parent certificates (deterministic)
- Convert InFlight → Pending dựa trên batch đã có trong queue (deterministic)
- Tất cả primaries đều nhận cùng parent certificates
- Logic extract và convert là deterministic

### 2. Không Bỏ Rơi

- Batch được convert từ InFlight → Pending khi extract từ parent certificates
- Batch được include trong header ngay lập tức
- Batch được commit ngay lập tức
- Retry logic vẫn hoạt động như backup

### 3. Không Trùng Lặp

- Logic `collect_payload_for_header` đã có deduplication (uses `seen_digests` HashSet)
- Convert InFlight → Pending chỉ thay đổi state của entry hiện tại, không thêm entry mới
- Double-check `committed_digests` trước khi include

## LỢI ÍCH

### 1. Giảm Retry

- Batch được include ngay lập tức khi extract từ parent certificates
- Batch không cần chờ retry logic
- Retry chỉ là backup khi extract không hoạt động

### 2. Hệ thống mượt mà hơn

- Batch được commit nhanh hơn (không chờ retry)
- Ít header với batches cũ
- Tăng throughput

### 3. Giảm network traffic

- Ít retry hơn → giảm network traffic
- Header nhỏ hơn → giảm latency

### 4. Tăng throughput

- Consensus xử lý ít header với batches cũ
- Batch được commit nhanh hơn

## KẾT LUẬN

**Retry là cần thiết nhưng không nên là cách chính để commit batches**

**Giải pháp**:
1. Leader extract batches từ parent certificates ngay lập tức
2. Convert InFlight → Pending để batch có thể được include ngay lập tức
3. Retry chỉ là backup khi extract không hoạt động

**Kết quả**:
- **Giảm retry**: Batch được include ngay lập tức
- **Hệ thống mượt mà hơn**: Ít retry hơn, batch được commit nhanh hơn
- **Tăng throughput**: Ít header với batches cũ
- **Đảm bảo**: Không fork, không bỏ rơi, không trùng lặp

