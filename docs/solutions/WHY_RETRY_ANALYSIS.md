# PHÂN TÍCH: TẠI SAO PHẢI RETRY VÀ LÀM SAO GIẢM RETRY

## VẤN ĐỀ

Người dùng hỏi: "Tại sao phải retry nếu hệ thống cứ retry nhiều thì sẽ chạy không mượt"

Đây là một câu hỏi rất hay - retry nhiều làm hệ thống không mượt, vì:
1. Batch được gửi đi gửi lại nhiều lần → tốn tài nguyên
2. Header chứa nhiều batches cũ → tăng kích thước header
3. Consensus phải xử lý nhiều headers với cùng batches → giảm throughput
4. Network traffic tăng → tăng latency

## TẠI SAO CẦN RETRY?

### Lý do cần retry

1. **Certificate của Primary không được commit**: 
   - Bullshark chỉ commit certificate của leader
   - Nếu batch được gửi bởi non-leader primary, certificate của primary đó sẽ không được commit
   - Batch cần được retry để được commit bởi leader

2. **Leader không phải lúc nào cũng là primary gửi batch**:
   - Leader được chọn bằng round-robin: `round % num_validators`
   - Batch được tạo bởi worker của primary A, nhưng leader có thể là primary B
   - Batch cần được retry hoặc được leader extract và include

3. **Network delay / Certificate đến muộn**:
   - Certificate có thể đến muộn do network delay
   - Batch có thể không được include kịp trong header

### Vấn đề với retry

1. **Retry quá chậm**: Batch phải chờ nhiều rounds mới được retry
2. **Retry quá nhiều**: Batch được retry nhiều lần, tốn tài nguyên
3. **Retry không đúng lúc**: Batch được retry khi không cần thiết (đã được commit)

## GIẢI PHÁP: GIẢM RETRY BẰNG CÁCH CẢI THIỆN EXTRACT BATCHES

### Ý tưởng chính

**Thay vì retry, leader nên extract batches từ parent certificates và include ngay lập tức**

### Logic hiện tại

1. Primary A tạo batch X
2. Primary A gửi batch X trong header B2860
3. Certificate của Primary A không được commit (Primary 3 là leader)
4. Primary A retry batch X sau 1000 rounds → KHÔNG MƯỢT!

### Logic cải thiện

1. Primary A tạo batch X
2. Primary A gửi batch X trong header B2860
3. Certificate của Primary A không được commit (Primary 3 là leader)
4. **Primary B (leader) extract batch X từ parent certificates (round 2860)**
5. **Primary B include batch X trong header B2861**
6. Certificate của Primary B được commit → batch X được commit → **MƯỢT MÀ!**

### Cải thiện cần thiết

1. **Leader nên extract batches từ parent certificates ngay cả khi batch đang ở InFlight ở primary khác**
2. **Extract batches nên được ưu tiên hơn retry**
3. **Leader nên include batches từ parent certificates ngay lập tức, không chờ retry**

## VẤN ĐỀ VỚI LOGIC HIỆN TẠI

### Extract batches bị skip

Từ log:
```
[EXTRACT BATCHES] Extracted 1 batches from 4 parent certificates (round 2841): 0 added to queue, 0 skipped (committed), 1 skipped (duplicate)
```

**Vấn đề**: Hầu hết batches đều bị skip vì "already in queue" (duplicate)

### Tại sao bị skip?

Logic hiện tại:
- Skip nếu batch đã có trong queue của chính primary đó
- Điều này có nghĩa là: nếu primary A đã extract batch X, primary B không thể extract batch X nữa

**Nhưng**: Điều này không đúng! Primary B (leader) nên có thể extract batch X từ parent certificates ngay cả khi primary A đã extract batch X trước đó.

## GIẢI PHÁP ĐỀ XUẤT

### 1. Leader nên extract batches từ parent certificates ngay cả khi batch đã có trong queue

**Logic**:
- Nếu batch đã có trong queue nhưng ở trạng thái `InFlight` và không được commit, leader vẫn có thể extract và include nó
- Logic extract batches nên check: nếu batch đang ở `InFlight` và không được commit, vẫn có thể extract

### 2. Extract batches nên được ưu tiên hơn retry

**Logic**:
- Thay vì retry batch sau nhiều rounds, leader nên extract batch từ parent certificates và include ngay lập tức
- Retry chỉ nên là backup khi extract không hoạt động (batch không có trong parent certificates)

### 3. Leader nên include batches từ parent certificates ngay lập tức

**Logic**:
- Khi leader extract batches từ parent certificates, nó nên include các batches này trong header ngay lập tức
- Không cần chờ retry logic

## ĐẢM BẢO KHÔNG FORK, KHÔNG BỎ RƠI, KHÔNG TRÙNG LẶP

### 1. Không Fork

- Extract batches dựa trên parent certificates (deterministic)
- Tất cả primaries đều nhận cùng parent certificates
- Logic extract batches là deterministic

### 2. Không Bỏ Rơi

- Leader extract batches từ parent certificates ngay lập tức
- Batch được include trong header của leader
- Batch được commit ngay lập tức

### 3. Không Trùng Lặp

- Logic `collect_payload_for_header` đã có deduplication
- Double-check `committed_digests` trước khi include
- Extract batches chỉ thêm batch vào queue nếu chưa có trong queue

## KẾT LUẬN

**Retry là cần thiết nhưng không nên là cách chính để commit batches**

**Giải pháp tốt hơn**:
1. Leader extract batches từ parent certificates ngay lập tức
2. Leader include batches này trong header ngay lập tức
3. Retry chỉ là backup khi extract không hoạt động

**Lợi ích**:
1. **Batch được commit nhanh hơn**: Không cần chờ retry
2. **Hệ thống mượt mà hơn**: Ít retry hơn
3. **Tăng throughput**: Ít header với batches cũ
4. **Giảm network traffic**: Ít retry hơn

