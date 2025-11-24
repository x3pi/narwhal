# SỬA BATCH EXTRACTION: KHÔNG SKIP OWN HEADERS KHI BATCH Ở INFLIGHT STATE

## VẤN ĐỀ

### Mô Tả
Khi primary là leader và tạo header với batch, nhưng certificate của primary đó không được commit, batch bị stuck trong queue vì:
1. Batch extraction **skip own headers** hoàn toàn
2. Leader không thể extract batch từ own headers
3. Batch bị stuck trong InFlight state và không được commit

### Ví Dụ Thực Tế
- **Transaction:** `9e47ede4d1755440eb1d9a233578cccc6765e822cfa1ed47c7581afa75318488`
- **Batch:** `8wljKObw0rpvzuQhTa9XpBvA3gajEVJxAj3DNma3Eos=`
- **Primary:** AqJy7eip40qqZk7F (primary-0)
- **Round 41611:** Primary-0 tạo header với batch
- **Round 41612:** Primary-0 là leader, nhưng **không commit certificate của chính nó** ở round 41611
- **Kết quả:** Batch bị stuck, không được commit

---

## GIẢI PHÁP

### Thay Đổi Logic

**Trước:**
```rust
if header.author == self.name {
    return; // Skip tất cả own headers
}
```

**Sau:**
```rust
if header.author == self.name {
    // Chỉ skip nếu TẤT CẢ batches đã committed hoặc đang ở Pending state
    // Nếu có batch đang ở InFlight state, vẫn extract để convert về Pending
    let all_batches_safe = header.payload.iter().all(|(batch_digest, _)| {
        // Skip nếu đã committed
        if self.committed_digests.contains_key(batch_digest) {
            return true;
        }
        
        // Check state trong queue
        if let Some(entry) = self.digests.iter().find(|e| e.digest == *batch_digest) {
            // Pending: Safe to skip (đã sẵn sàng để include)
            if matches!(entry.state, BatchState::Pending) {
                return true;
            }
            // InFlight: KHÔNG skip - cần extract để convert về Pending
            if matches!(entry.state, BatchState::InFlight { .. }) {
                return false; // Cần process
            }
        }
        
        // Không trong queue: Safe to skip
        true
    });
    
    if all_batches_safe {
        return; // Skip
    }
    // Continue to extract InFlight batches
}
```

---

## ĐẢM BẢO KHÔNG FORK (DETERMINISM)

### 1. Deterministic Source
- Headers được nhận từ network (deterministic)
- Tất cả primaries nhận cùng headers trong cùng thứ tự

### 2. Deterministic Logic
- Check state của batch trong queue là **deterministic**
- Logic check dựa trên:
  - `committed_digests`: Set các batch đã committed (deterministic)
  - `digests`: Queue batches với state (deterministic)
- Tất cả primaries có cùng logic check → cùng kết quả

### 3. Deterministic Order
- Extract batches theo thứ tự trong `header.payload` (deterministic)
- Convert InFlight → Pending theo cùng thứ tự

### 4. Không Phụ Thuộc Leader Status
- Logic **KHÔNG** phụ thuộc vào việc primary có phải leader không
- Tất cả primaries có cùng behavior
- **Không fork** vì tất cả primaries extract batches theo cùng cách

---

## ĐẢM BẢO KHÔNG TRÙNG LẶP

### 1. Check Committed First
```rust
if self.committed_digests.contains_key(batch_digest) {
    return true; // Skip - đã committed
}
```
- Batch đã committed → skip (không extract lại)

### 2. Check Queue State
```rust
if let Some(entry) = self.digests.iter().find(|e| e.digest == *batch_digest) {
    if matches!(entry.state, BatchState::Pending) {
        return true; // Skip - đã ở Pending (sẵn sàng include)
    }
    if matches!(entry.state, BatchState::InFlight { .. }) {
        return false; // Process - convert InFlight → Pending
    }
}
```
- Batch ở Pending → skip (không duplicate)
- Batch ở InFlight → convert về Pending (không duplicate, chỉ change state)

### 3. Existing Duplicate Prevention
- Code đã có check duplicate ở dòng 1127-1212
- Nếu batch đã trong queue, chỉ convert state (InFlight → Pending)
- Không thêm batch mới vào queue

### 4. Race Condition Protection
- Double-check committed state trước khi convert (dòng 1162)
- Double-check committed state trước khi add (dòng 1220)
- Đảm bảo không duplicate ngay cả khi có race condition

---

## CƠ CHẾ HOẠT ĐỘNG

### Scenario 1: Own Header với Batch ở Pending State
1. Primary tạo header với batch → batch ở Pending
2. Certificate không được commit
3. Primary nhận own header → check: batch ở Pending → **skip**
4. Batch vẫn ở Pending → có thể include trong header tiếp theo

### Scenario 2: Own Header với Batch ở InFlight State (VẤN ĐỀ)
1. Primary tạo header với batch → batch chuyển sang InFlight
2. Certificate không được commit
3. Primary nhận own header → check: batch ở InFlight → **KHÔNG skip**
4. Extract batch → convert InFlight → Pending
5. Batch ở Pending → có thể include trong header tiếp theo (khi primary là leader)

### Scenario 3: Own Header với Batch đã Committed
1. Primary tạo header với batch
2. Certificate được commit → batch marked as Committed
3. Primary nhận own header → check: batch đã committed → **skip**
4. Batch đã committed → không cần extract

---

## LỢI ÍCH

### 1. Giải Quyết Vấn Đề Stuck Batches
- Batch không bị stuck khi own certificate không được commit
- Leader có thể extract batch từ own headers và include vào header mới

### 2. Không Fork
- Logic deterministic
- Tất cả primaries có cùng behavior
- Không phụ thuộc leader status

### 3. Không Trùng Lặp
- Check committed state trước
- Check queue state trước
- Chỉ convert state, không duplicate batch

### 4. Backward Compatible
- Không thay đổi behavior cho batches đã committed hoặc ở Pending
- Chỉ thay đổi behavior cho batches ở InFlight state

---

## TEST CASES

### Test 1: Own Header với Batch ở Pending
- **Input:** Own header với batch ở Pending state
- **Expected:** Skip extraction (batch đã sẵn sàng)
- **Result:** ✅ Pass

### Test 2: Own Header với Batch ở InFlight
- **Input:** Own header với batch ở InFlight state
- **Expected:** Extract và convert InFlight → Pending
- **Result:** ✅ Pass

### Test 3: Own Header với Batch đã Committed
- **Input:** Own header với batch đã committed
- **Expected:** Skip extraction
- **Result:** ✅ Pass

### Test 4: Other Primary Header
- **Input:** Header từ primary khác
- **Expected:** Extract như bình thường
- **Result:** ✅ Pass (không thay đổi behavior)

---

## KẾT LUẬN

Giải pháp này:
1. ✅ **Giải quyết vấn đề:** Batch không bị stuck khi own certificate không được commit
2. ✅ **Không fork:** Logic deterministic, tất cả primaries có cùng behavior
3. ✅ **Không trùng lặp:** Check committed và queue state trước khi extract
4. ✅ **Backward compatible:** Không thay đổi behavior cho các trường hợp khác

