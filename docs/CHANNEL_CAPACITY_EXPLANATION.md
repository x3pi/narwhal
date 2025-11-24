# GIẢI THÍCH: CHANNEL_CAPACITY VÀ CÁCH DỌN DẸP

## CHANNEL_CAPACITY LÀ GÌ?

`CHANNEL_CAPACITY` là **buffer size** của tokio mpsc channel. Đây là số lượng messages tối đa có thể được lưu trong channel trước khi `send().await` bị block.

### Cách Hoạt Động:

```rust
let (tx, rx) = channel(CHANNEL_CAPACITY);
// CHANNEL_CAPACITY = 50_000 nghĩa là:
// - Channel có thể lưu tối đa 50,000 messages
// - Nếu channel đầy, send().await sẽ block cho đến khi có chỗ
// - Nếu channel chưa đầy, send().await sẽ return ngay lập tức
```

### Ví Dụ:

```
CHANNEL_CAPACITY = 1_000:
- Channel có thể lưu 1,000 headers
- Nếu có 1,001 headers cần gửi, header thứ 1,001 sẽ bị block
- Nếu proposer xử lý chậm, channel sẽ đầy → headers bị delay

CHANNEL_CAPACITY = 50_000:
- Channel có thể lưu 50,000 headers
- Cần nhiều headers hơn mới bị block
- Nhưng nếu proposer xử lý chậm, vẫn có thể đầy
```

---

## CHANNEL CÓ ĐƯỢC DỌN DẸP KHÔNG?

### ✅ CÓ - Channel Tự Động Dọn Dẹp

**Cách hoạt động:**
1. **Sender gửi message** → Message được thêm vào buffer
2. **Receiver nhận message** → Message được remove khỏi buffer
3. **Buffer tự động dọn dẹp** khi receiver nhận messages

**Ví dụ:**
```
Buffer: [header1, header2, header3, ...] (capacity = 50_000)
         ↑
    Receiver nhận header1 → Buffer: [header2, header3, ...]
    Receiver nhận header2 → Buffer: [header3, ...]
    ...
```

### ⚠️ NHƯNG - Nếu Receiver Chậm Hơn Sender

**Vấn đề:**
- Nếu proposer xử lý headers chậm hơn core gửi headers
- Channel sẽ tích lũy headers
- Sau 2 tiếng, có thể có hàng nghìn headers trong channel
- Memory tăng dần

**Ví dụ:**
```
Core gửi: 100 headers/giây
Proposer xử lý: 50 headers/giây
→ Channel tích lũy: 50 headers/giây
→ Sau 2 tiếng: 50 * 7200 = 360,000 headers trong channel!
```

---

## TẠI SAO CẦN CAPACITY LỚN?

### Lý Do 1: Tránh Blocking

**Vấn đề:**
- Nếu channel đầy, `send().await` sẽ block
- Core main loop bị block → không thể xử lý messages khác
- Hệ thống bị delay

**Giải pháp:**
- Capacity lớn → ít khi đầy → ít khi block
- Nhưng không giải quyết root cause (receiver chậm)

### Lý Do 2: Buffer Cho Burst

**Vấn đề:**
- Có thể có burst headers (nhiều headers cùng lúc)
- Capacity nhỏ → dễ đầy → headers bị delay

**Giải pháp:**
- Capacity lớn → có thể buffer burst
- Nhưng nếu burst kéo dài, vẫn có thể đầy

---

## VẤN ĐỀ THỰC SỰ

### Vấn Đề Không Phải Là Capacity

**Vấn đề thực sự:**
1. **Proposer xử lý headers chậm** → Channel tích lũy
2. **Extract batches chậm** → Headers không được xử lý kịp
3. **Memory leak** → Channel tích lũy headers không được dọn dẹp

**Giải pháp đúng:**
- Đảm bảo proposer xử lý headers nhanh
- Đảm bảo extract batches nhanh
- Không cần capacity quá lớn nếu receiver xử lý nhanh

---

## GIẢI PHÁP TỐT HƠN

### Giải Pháp 1: Đảm Bảo Proposer Xử Lý Nhanh

**Mục tiêu:**
- Proposer phải xử lý headers nhanh hơn core gửi
- Không để headers tích lũy trong channel

**Implementation:**
- Tối ưu `extract_batches_from_headers` để xử lý nhanh
- Không block trong extraction logic
- Có thể spawn task riêng để extract batches

### Giải Pháp 2: Dùng Unbounded Channel

**Mục tiêu:**
- Không giới hạn capacity
- Không bao giờ block sender
- Nhưng cần đảm bảo receiver xử lý nhanh

**Trade-off:**
- ✅ Không bao giờ block
- ⚠️ Memory có thể tăng nếu receiver chậm
- ⚠️ Cần monitoring để đảm bảo receiver xử lý nhanh

### Giải Pháp 3: Drop Headers Cũ Nếu Channel Đầy

**Mục tiêu:**
- Nếu channel đầy, drop headers cũ nhất
- Giữ headers mới nhất
- Đảm bảo channel không đầy

**Trade-off:**
- ✅ Channel không bao giờ đầy
- ⚠️ Headers cũ có thể bị mất
- ⚠️ Batches trong headers cũ có thể không được extract

---

## KHUYẾN NGHỊ

### Ngắn Hạn:
1. ✅ **Giữ capacity lớn (50_000)** để tránh blocking
2. ✅ **Spawn task để gửi headers** không blocking main loop
3. ✅ **Monitor channel size** để phát hiện vấn đề sớm

### Dài Hạn:
1. ✅ **Tối ưu proposer** để xử lý headers nhanh hơn
2. ✅ **Đảm bảo extract batches nhanh** không block
3. ✅ **Có thể dùng unbounded channel** nếu cần

---

## KẾT LUẬN

**CHANNEL_CAPACITY:**
- Là buffer size của channel
- Channel tự động dọn dẹp khi receiver nhận
- Nhưng nếu receiver chậm, channel sẽ tích lũy

**Vấn đề:**
- Không phải capacity quá nhỏ
- Mà là receiver (proposer) xử lý chậm

**Giải pháp:**
- Capacity lớn là workaround tạm thời
- Giải pháp tốt nhất là đảm bảo proposer xử lý nhanh

