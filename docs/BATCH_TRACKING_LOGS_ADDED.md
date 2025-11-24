# BỔ SUNG LOGGING CHI TIẾT ĐỂ TÌM NGUYÊN NHÂN GỐC RỄ

## TÓM TẮT

Đã bổ sung logging chi tiết vào tất cả các điểm quan trọng trong quá trình batch từ khi tạo đến khi commit để tìm nguyên nhân gốc rễ tại sao batch không được commit.

---

## CÁC LOGS ĐÃ THÊM

### 1. **Core → Proposer: Gửi Header**

**Location:** `primary/src/core.rs` - `process_header()`

**Log:**
```
[BATCH TRACK CORE] Core {} sending header {} (round {}, author: {}) to proposer for batch extraction. Header contains {} batches: {:?}
```

**Giúp theo dõi:**
- Khi nào header được gửi từ Core đến Proposer
- Header chứa bao nhiêu batches
- Batches nào trong header

---

### 2. **Proposer: Nhận Header từ Core**

**Location:** `primary/src/proposer.rs` - `run()` loop

**Log:**
```
[BATCH TRACK PROPOSER] Proposer {} received header {} (round {}, author: {}) from Core for batch extraction. Header contains {} batches: {:?}
```

**Giúp theo dõi:**
- Khi nào Proposer nhận header từ Core
- Header chứa bao nhiêu batches
- Batches nào trong header

---

### 3. **Proposer: Bắt đầu Extract Batches**

**Location:** `primary/src/proposer.rs` - `extract_batches_from_headers()`

**Log:**
```
[BATCH EXTRACTION START] Primary {} starting extraction from header {} (round {}, author: {}). Header contains {} batches. Current round: {}, max_retry_rounds: {}
```

**Giúp theo dõi:**
- Khi nào bắt đầu extract batches từ header
- Thông tin về header và current round

---

### 4. **Proposer: Extract Batch Thành Công**

**Location:** `primary/src/proposer.rs` - `extract_batches_from_headers()`

**Log:**
```
[BATCH EXTRACTION SUCCESS] Primary {} EXTRACTED batch {} (worker {}) from header {} (round {}, author: {}) into queue. Batch from non-leader primary can now be committed by this primary. Batch size: {} bytes, queue_len: {}, pending_payload_size: {}
```

**Giúp theo dõi:**
- Batch nào được extract thành công
- Batch được thêm vào queue
- Trạng thái queue sau khi extract

---

### 5. **Proposer: Tóm Tắt Extraction**

**Location:** `primary/src/proposer.rs` - `extract_batches_from_headers()`

**Log:**
```
[BATCH EXTRACTION SUMMARY] Primary {} completed extraction from header {} (round {}, author: {}): {} total batches in header, {} extracted, {} added to queue, {} skipped (committed), {} skipped (duplicate), {} not in store. Current round: {}, queue_len: {}, pending_payload_size: {}
```

**Giúp theo dõi:**
- Tổng số batches trong header
- Số batches được extract
- Số batches được thêm vào queue
- Số batches bị skip (committed, duplicate, not in store)
- Trạng thái queue sau extraction

---

### 6. **Proposer: Tạo Header với Batches**

**Location:** `primary/src/proposer.rs` - `make_header()`

**Log:**
```
[BATCH TRACK HEADER] Primary {} creating header for round {} with {} batches: {:?}
```

**Giúp theo dõi:**
- Batches nào được include trong header
- Round nào header được tạo

---

### 7. **Proposer: Nhận Committed Batches Notification**

**Location:** `primary/src/proposer.rs` - `run()` loop

**Log:**
```
[BATCH COMMIT NOTIFICATION] Proposer {} received committed batches notification: {} batches committed at round {}
```

**Giúp theo dõi:**
- Khi nào Proposer nhận notification về committed batches
- Số batches được commit
- Round nào batches được commit

---

### 8. **Proposer: Mark Batches as Committed**

**Location:** `primary/src/proposer.rs` - `mark_committed()`

**Log:**
```
[BATCH COMMIT] Primary {} marking {} batches as committed at round {}: {:?}
```

**Giúp theo dõi:**
- Batches nào được mark as committed
- Round nào batches được commit

---

### 9. **Garbage Collector: Gửi Committed Batches**

**Location:** `primary/src/garbage_collector.rs` - `handle_committed_certificate()`

**Log:**
```
[BATCH TRACK GC] GarbageCollector sending {} committed batches to proposer at round {} from certificate {} (author: {}): {:?}
[BATCH TRACK GC] GarbageCollector: successfully informed proposer about committed round {} ({} batches) from certificate {} (author: {})
```

**Giúp theo dõi:**
- Khi nào Garbage Collector gửi committed batches
- Batches nào được commit
- Certificate nào chứa batches
- Author của certificate

---

## CÁCH SỬ DỤNG LOGS ĐỂ TÌM NGUYÊN NHÂN

### 1. **Track Batch Từ Khi Tạo Đến Khi Commit**

```bash
# Tìm batch digest
BATCH_DIGEST="0HicXny9SpyIs1ic"

# Track batch trong toàn bộ quá trình
grep "$BATCH_DIGEST" primary-0.log | grep -E "BATCH TRACK|BATCH EXTRACTION|BATCH COMMIT"
```

**Kỳ vọng:**
1. `[BATCH TRACK CORE]` - Core gửi header chứa batch đến Proposer
2. `[BATCH TRACK PROPOSER]` - Proposer nhận header
3. `[BATCH EXTRACTION START]` - Bắt đầu extract
4. `[BATCH EXTRACTION SUCCESS]` - Extract thành công (nếu là leader)
5. `[BATCH TRACK HEADER]` - Batch được include trong header của leader
6. `[BATCH TRACK GC]` - Garbage Collector gửi committed notification
7. `[BATCH COMMIT]` - Batch được mark as committed

### 2. **Kiểm Tra Header Extraction**

```bash
# Kiểm tra xem header có được gửi đến proposer không
grep "BATCH TRACK CORE.*header.*round.*author" primary-0.log | grep "AqJy7eip40qqZk7F"

# Kiểm tra xem proposer có nhận header không
grep "BATCH TRACK PROPOSER.*received header" primary-*.log | grep "AqJy7eip40qqZk7F"
```

**Nếu không có log:**
- Header không được gửi từ Core đến Proposer
- Có thể do channel đầy hoặc lỗi

### 3. **Kiểm Tra Batch Extraction**

```bash
# Kiểm tra extraction summary
grep "BATCH EXTRACTION SUMMARY" primary-*.log | tail -20

# Kiểm tra extraction success
grep "BATCH EXTRACTION SUCCESS" primary-*.log | grep "0HicXny9SpyIs1ic"
```

**Nếu không có `BATCH EXTRACTION SUCCESS`:**
- Batch không được extract (có thể do đã committed, duplicate, hoặc not in store)
- Kiểm tra `BATCH EXTRACTION SUMMARY` để xem lý do skip

### 4. **Kiểm Tra Batch Include trong Header**

```bash
# Kiểm tra xem batch có được include trong header của leader không
grep "BATCH TRACK HEADER" primary-*.log | grep "0HicXny9SpyIs1ic"
```

**Nếu không có log:**
- Batch không được include trong header của leader
- Có thể do leader không extract batch này

### 5. **Kiểm Tra Batch Commit**

```bash
# Kiểm tra xem batch có được commit không
grep "BATCH COMMIT\|BATCH TRACK GC" primary-*.log | grep "0HicXny9SpyIs1ic"
```

**Nếu không có log:**
- Batch không được commit
- Có thể do certificate không được commit hoặc batch không có trong certificate

---

## VÍ DỤ PHÂN TÍCH

### Batch Không Được Commit - Phân Tích Logs

```bash
# 1. Tìm batch trong logs
BATCH="0HicXny9SpyIs1ic"
grep "$BATCH" primary-0.log | grep -E "BATCH TRACK|EXTRACTION|COMMIT" | head -20

# 2. Kiểm tra header có được gửi không
grep "BATCH TRACK CORE.*$BATCH" primary-0.log

# 3. Kiểm tra extraction
grep "BATCH EXTRACTION.*$BATCH" primary-*.log

# 4. Kiểm tra commit
grep "BATCH COMMIT.*$BATCH\|BATCH TRACK GC.*$BATCH" primary-*.log
```

**Kết quả mong đợi:**
- Nếu có `BATCH TRACK CORE` nhưng không có `BATCH TRACK PROPOSER` → Header không được gửi đến Proposer
- Nếu có `BATCH TRACK PROPOSER` nhưng không có `BATCH EXTRACTION SUCCESS` → Batch không được extract (kiểm tra summary để xem lý do)
- Nếu có `BATCH EXTRACTION SUCCESS` nhưng không có `BATCH TRACK HEADER` → Batch không được include trong header của leader
- Nếu có `BATCH TRACK HEADER` nhưng không có `BATCH COMMIT` → Certificate không được commit hoặc batch không có trong certificate

---

## KẾT LUẬN

Đã bổ sung logging chi tiết vào tất cả các điểm quan trọng:
- ✅ Core gửi header đến Proposer
- ✅ Proposer nhận header
- ✅ Proposer extract batches
- ✅ Proposer tạo header với batches
- ✅ Proposer nhận committed notification
- ✅ Garbage Collector gửi committed batches

**Kết quả:** Có thể track batch từ khi tạo đến khi commit và tìm nguyên nhân gốc rễ tại sao batch không được commit.

