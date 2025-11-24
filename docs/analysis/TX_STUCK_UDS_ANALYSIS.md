# PHÂN TÍCH: GIAO DỊCH KHÔNG ĐƯỢC GỬI TỚI UDS

## VẤN ĐỀ

Giao dịch `0x838c8b7c6684d173167ee51449eb15f41340ff7a5a7450eecabdd3feff92c476` không được đưa tới unix domain socket để thực thi sau khi hệ thống chạy một thời gian.

## FLOW TỪ WORKER → UDS

```
1. Worker nhận transaction → tạo batch → gửi tới Primary
2. Primary nhận batch → đưa vào queue → tạo header → gửi tới Core
3. Core xử lý header → tạo certificate → gửi tới Consensus
4. Consensus commit certificate → gửi tới rx_output channel
5. Node nhận từ rx_output → extract batches → gửi tới UDS
```

## NGUYÊN NHÂN CÓ THỂ

### 1. Certificate Không Được Commit (Consensus Layer)

**Vấn đề:**
- Certificate chứa batch không được commit bởi consensus
- Consensus có thể bị stuck hoặc không đạt quorum

**Kiểm tra:**
- Log từ consensus về việc commit certificates
- Xem có certificate nào được tạo nhưng không được commit không

### 2. Channel rx_output Đầy Hoặc Bị Block

**Vấn đề:**
- Channel `rx_output` từ consensus → node có thể đầy
- Nếu channel đầy, certificates không được gửi → batches không được xử lý

**Kiểm tra:**
- `CHANNEL_CAPACITY` của `rx_output`
- Xem có log về việc channel đầy không

### 3. Certificate Bị Skip Do Duplicate Detection

**Vấn đề:**
- Logic duplicate detection có thể skip batch sai
- Batch bị đánh dấu là "already processed" dù chưa được gửi tới UDS

**Kiểm tra:**
- Log về "ALREADY PROCESSED" hoặc "SKIP batch"
- Xem batch có bị skip không đúng không

### 4. Batch Không Có Trong Store

**Vấn đề:**
- Batch không được lưu trong store
- Khi node cố extract batch từ certificate, không tìm thấy trong store

**Kiểm tra:**
- Log về "NOT FOUND in store"
- Xem batch có được lưu trong store không

### 5. Height Calculation Sai

**Vấn đề:**
- Height calculation có thể sai, dẫn đến batch bị skip
- Logic "late batch" có thể skip batch không đúng

**Kiểm tra:**
- Log về height calculation
- Xem batch có bị skip do height không đúng không

## GIẢI PHÁP

### Giải Pháp 1: Thêm Logging Chi Tiết (URGENT)

**Mục tiêu:**
- Track toàn bộ flow từ worker → UDS
- Phát hiện bottleneck sớm

**Logging cần thêm:**
1. **Worker → Primary:**
   - Log khi batch được gửi tới primary
   - Log batch digest và transactions trong batch

2. **Primary → Consensus:**
   - Log khi header được tạo với batch
   - Log khi certificate được tạo
   - Log khi certificate được gửi tới consensus

3. **Consensus → Node:**
   - Log khi certificate được commit
   - Log khi certificate được gửi tới rx_output
   - Log nếu channel đầy

4. **Node → UDS:**
   - Log khi nhận certificate từ rx_output
   - Log khi extract batches
   - Log khi gửi tới UDS
   - Log nếu batch bị skip và lý do

### Giải Pháp 2: Tăng Channel Capacity

**Mục tiêu:**
- Tránh channel đầy
- Đảm bảo certificates được gửi kịp thời

**Implementation:**
- Tăng `CHANNEL_CAPACITY` cho `rx_output`
- Hoặc dùng unbounded channel

### Giải Pháp 3: Fix Duplicate Detection Logic

**Mục tiêu:**
- Đảm bảo batch chỉ bị skip khi thực sự đã được xử lý
- Không skip batch nếu chưa được gửi tới UDS

**Implementation:**
- Review logic duplicate detection
- Chỉ mark batch là "processed" sau khi gửi tới UDS thành công

### Giải Pháp 4: Retry Mechanism

**Mục tiêu:**
- Nếu batch không được gửi tới UDS, retry
- Đảm bảo không mất batch

**Implementation:**
- Track batches chưa được gửi
- Retry sau một khoảng thời gian

## KHUYẾN NGHỊ

### Ngắn Hạn (URGENT):
1. ✅ **Thêm logging chi tiết** để track flow
2. ✅ **Tăng channel capacity** để tránh blocking
3. ✅ **Review duplicate detection logic**

### Dài Hạn:
1. ✅ **Retry mechanism** cho batches chưa được gửi
2. ✅ **Monitoring và alerting** khi batches bị stuck
3. ✅ **Health check** cho UDS connection

