# PHÂN TÍCH: TẠI SAO BATCH PHẢI RETRY HÀNG NGHÌN LẦN

## VẤN ĐỀ

Batch bị retry hàng nghìn lần (retry_count > 8000) trước khi được commit, dẫn đến:
- Giao dịch bị delay rất lâu
- Tốn tài nguyên (CPU, memory) cho retry
- Hệ thống không tối ưu

## PHÂN TÍCH FLOW HIỆN TẠI

### Timeline của một batch bị stuck:

```
T0: Worker tạo batch và gửi tới primary-0
T1: Primary-0 nhận batch, đưa vào queue
T2: Primary-0 tạo header với batch (round 82350)
T3: Primary-0 broadcast header qua network
T4: Các primary khác nhận header từ network (network delay)
T5: Header được verify (sanitize_header)
T6: Check parents (get_parents)
T7: Check payload (missing_payload)
T8: Header được stored
T9: Header được gửi tới proposer (tx_headers.send) ← QUÁ MUỘN!
T10: Proposer extract batch từ header
T11: Leader tạo header với batch đã extract
T12: Certificate được commit
```

### Vấn Đề Chính:

1. **Delay trong Network (T3 → T4):**
   - Header phải được broadcast qua network
   - Network có latency (vài ms đến vài trăm ms)
   - Các primary khác nhận header không đồng bộ

2. **Delay trong Verification (T4 → T8):**
   - Header phải được verify (sanitize_header)
   - Phải check parents (get_parents) - có thể phải sync từ network
   - Phải check payload (missing_payload) - có thể phải sync từ workers
   - Header phải được stored
   - **Tổng delay: có thể vài trăm ms đến vài giây**

3. **Delay trong Extraction (T8 → T10):**
   - Header chỉ được gửi tới proposer SAU KHI verify xong
   - Proposer chỉ extract batch khi nhận được header từ channel
   - **Delay: thêm vài ms đến vài trăm ms**

4. **Race Condition:**
   - Leader có thể đã tạo header của mình TRƯỚC KHI nhận được header từ primary-0
   - Leader không có batch trong header → batch không được commit
   - Batch phải đợi đến round tiếp theo → retry

### Ví Dụ Cụ Thể:

```
Round 82350:
- T0: Primary-0 tạo header với batch /3g5fgOWD6tzhT9/ (08:23:48.217Z)
- T1: Primary-0 broadcast header
- T2: Leader (primary-1) đã tạo header của mình TRƯỚC KHI nhận header từ primary-0
- T3: Leader commit certificate không chứa batch /3g5fgOWD6tzhT9/
- T4: Primary-0 nhận thấy certificate của mình không được commit
- T5: Batch bị retry

Round 82351-122554:
- Batch bị retry hàng nghìn lần
- Headers chứa batch trở nên quá cũ
- Các primary khác skip extract batch từ headers quá cũ
- Batch không bao giờ được commit
```

## NGUYÊN NHÂN GỐC RỄ

### 1. Header Chỉ Được Gửi Tới Proposer SAU KHI Verify

**Code hiện tại:**
```rust
// primary/src/core.rs:188-198
// Store the header.
let bytes = bincode::serialize(header).expect("Failed to serialize header");
self.store.write(header.id.to_vec(), bytes).await;

// IMPROVED: Send verified header to proposer for batch extraction
if header.author != self.name {
    if let Err(e) = self.tx_headers.send(header.clone()).await {
        debug!("Failed to send header {} to proposer for batch extraction: {}", header.id, e);
    }
}
```

**Vấn đề:**
- Header chỉ được gửi SAU KHI verify, check parents, check payload, và stored
- Delay có thể vài trăm ms đến vài giây
- Leader có thể đã tạo header TRƯỚC KHI nhận được header từ primary-0

### 2. Extraction Chỉ Xảy Ra Khi Nhận Header Từ Channel

**Code hiện tại:**
```rust
// primary/src/proposer.rs:1163-1173
Some(header) = self.rx_headers.recv() => {
    self.extract_batches_from_headers(&header).await;
}
```

**Vấn đề:**
- Proposer chỉ extract batch khi nhận được header từ channel
- Nếu channel bị full hoặc có delay, extraction bị delay
- Leader có thể đã tạo header TRƯỚC KHI extract batch

### 3. Không Có Priority Cho Leader

**Vấn đề:**
- Leader không có priority để extract batch ngay lập tức
- Leader có thể đã tạo header TRƯỚC KHI extract batch từ headers của các primary khác

## GIẢI PHÁP ĐỀ XUẤT

### Giải Pháp 1: Gửi Header Tới Proposer Sớm Hơn (Early Extraction)

**Ý tưởng:**
- Gửi header tới proposer NGAY KHI nhận được từ network
- Extract batch TRƯỚC KHI verify đầy đủ
- Chỉ extract batch nếu header có signature hợp lệ (đã verify signature)

**Lợi ích:**
- Giảm delay từ vài trăm ms xuống vài ms
- Leader có thể extract batch sớm hơn
- Batch được commit sớm hơn

**Rủi ro:**
- Cần verify signature trước khi extract (để tránh fork)
- Có thể extract batch từ header không hợp lệ (nhưng signature đã verify nên an toàn)

### Giải Pháp 2: Extract Batch Từ Own Headers

**Ý tưởng:**
- Khi primary tạo header, extract batch từ header đó NGAY LẬP TỨC
- Gửi batch tới proposer của các primary khác (qua network)
- Các primary khác extract batch từ message này

**Lợi ích:**
- Không cần đợi network và verification
- Batch được extract ngay lập tức
- Giảm delay đáng kể

**Rủi ro:**
- Cần thêm network message
- Có thể gây overhead nếu có nhiều primaries

### Giải Pháp 3: Priority Extraction Cho Leader

**Ý tưởng:**
- Khi primary là leader, extract batch từ headers NGAY LẬP TỨC
- Không đợi channel, extract trực tiếp từ store
- Đảm bảo leader luôn có batch mới nhất

**Lợi ích:**
- Leader luôn có batch mới nhất
- Batch được commit sớm hơn
- Giảm retry

**Rủi ro:**
- Cần detect leader (có thể dựa vào round và committee)
- Có thể gây overhead nếu check leader thường xuyên

### Giải Pháp 4: Batch Forwarding (Recommended)

**Ý tưởng:**
- Khi primary tạo header, gửi batch digests tới proposer của các primary khác NGAY LẬP TỨC
- Các primary khác extract batch từ message này (không cần đợi header)
- Batch được extract sớm hơn nhiều

**Lợi ích:**
- Giảm delay đáng kể (từ vài trăm ms xuống vài ms)
- Batch được extract ngay lập tức
- Không cần đợi network và verification

**Rủi ro:**
- Cần thêm network message (nhưng nhỏ, chỉ batch digests)
- Có thể gây overhead nếu có nhiều primaries

## KHUYẾN NGHỊ

**Giải Pháp 4 (Batch Forwarding) là tốt nhất vì:**
1. Giảm delay đáng kể (từ vài trăm ms xuống vài ms)
2. Batch được extract ngay lập tức
3. Không cần đợi network và verification
4. Overhead nhỏ (chỉ gửi batch digests, không phải toàn bộ header)

**Implementation:**
1. Khi primary tạo header, gửi batch digests tới proposer của các primary khác
2. Các primary khác extract batch từ message này
3. Batch được extract sớm hơn nhiều

---

**Last Updated:** 2025-01-19
**Status:** ⏳ Analysis Complete, Ready for Implementation

