# CẢI THIỆN RECEIVER CHẬM (PROPOSER XỬ LÝ HEADERS)

## VẤN ĐỀ: RECEIVER CHẬM

### Nguyên Nhân:

1. **Linear Search Trong Queue Lớn (O(n))**
   ```rust
   // Mỗi batch phải search trong queue
   let already_in_queue = self.digests.iter().any(|entry| entry.digest == *batch_digest);
   // Nếu queue có 10,000 batches → 10,000 operations cho mỗi batch!
   // Nếu header có 100 batches → 1,000,000 operations!
   ```

2. **Sequential Processing**
   - Xử lý từng header một
   - Xử lý từng batch một
   - Không thể batch process nhiều headers cùng lúc

3. **Store Read Blocking**
   ```rust
   match self.store.read(batch_digest.to_vec()).await {
       // I/O operation có thể chậm
   }
   ```

4. **Multiple Iterations**
   - Check `already_in_queue` → O(n)
   - Check `is_pending` → O(n) lần nữa
   - Iterate để convert InFlight → O(n) lần nữa

---

## GIẢI PHÁP

### Giải Pháp 1: Thêm HashMap Index (CRITICAL)

**Vấn đề:**
- `digests` là `VecDeque<BatchEntry>` → linear search O(n)
- Mỗi lần extract batch phải search → chậm nếu queue lớn

**Giải pháp:**
- Thêm `HashMap<Digest, usize>` để map digest → index trong VecDeque
- Lookup O(1) thay vì O(n)

**Trade-off:**
- ✅ Lookup nhanh hơn 1000x nếu queue có 1000 batches
- ⚠️ Cần maintain index khi add/remove batches
- ⚠️ Tăng memory một chút

### Giải Pháp 2: Batch Process Nhiều Headers Cùng Lúc

**Vấn đề:**
- Xử lý từng header một → chậm nếu có nhiều headers trong channel

**Giải pháp:**
- Dùng `try_recv()` để lấy nhiều headers cùng lúc
- Process batch headers trong một lần

**Trade-off:**
- ✅ Xử lý nhanh hơn nếu có nhiều headers
- ⚠️ Cần đảm bảo không block quá lâu

### Giải Pháp 3: Spawn Task Riêng Để Extract

**Vấn đề:**
- `extract_batches_from_headers` block main loop
- Nếu extract chậm, không thể nhận headers mới

**Giải pháp:**
- Spawn task riêng để extract batches
- Main loop tiếp tục nhận headers mới

**Trade-off:**
- ✅ Không block main loop
- ⚠️ Cần đảm bảo thread safety
- ⚠️ Có thể cần channel riêng để gửi kết quả

### Giải Pháp 4: Cache Store Reads

**Vấn đề:**
- Store read có thể chậm (I/O)
- Có thể đọc cùng batch nhiều lần

**Giải pháp:**
- Cache store reads trong memory
- Chỉ đọc từ store nếu chưa có trong cache

**Trade-off:**
- ✅ Giảm I/O operations
- ⚠️ Tăng memory usage
- ⚠️ Cần cleanup cache định kỳ

---

## KHUYẾN NGHỊ

### Ưu Tiên 1: Thêm HashMap Index (HIGH IMPACT, LOW EFFORT)
- Tác động lớn nhất
- Dễ implement
- Không thay đổi logic nhiều

### Ưu Tiên 2: Batch Process Headers (MEDIUM IMPACT, MEDIUM EFFORT)
- Cải thiện throughput
- Cần refactor một chút

### Ưu Tiên 3: Spawn Task Riêng (LOW IMPACT, HIGH EFFORT)
- Cần đảm bảo thread safety
- Có thể phức tạp hơn

### Ưu Tiên 4: Cache Store Reads (LOW IMPACT, LOW EFFORT)
- Cải thiện nhỏ
- Dễ implement

---

## KẾT LUẬN

**Vấn đề chính:**
- Linear search trong queue lớn → O(n) cho mỗi batch
- Sequential processing → không tận dụng parallelism

**Giải pháp tốt nhất:**
- Thêm HashMap index để lookup O(1)
- Batch process nhiều headers cùng lúc

