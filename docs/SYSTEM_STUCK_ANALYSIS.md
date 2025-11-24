# PHÂN TÍCH: HỆ THỐNG ĐỨNG SAU 2 TIẾNG

## THÔNG TIN TRANSACTION

- **Transaction Hash**: `f6bfd3b7a092ff4ca169df00ade60c892106eb7b6b828dc6f764d89d55cfc700`
- **Batch Digest**: `0HicXny9SpyIs1icIU9x9zKXyf/6RRijC5C5GVTYvJM=`
- **Thời gian tạo**: `2025-11-23T03:22:48.105Z`
- **Primary**: Primary-0 (AqJy7eip40qqZk7FqFMJsfRtrZS16YviyXCQ4gpcVYWK)

---

## TÌNH TRẠNG HỆ THỐNG

### Hệ thống vẫn đang chạy

**Timestamp cuối cùng:** `2025-11-23T03:26:19.167Z` (sau khi transaction được tạo ~3.5 phút)

**Round hiện tại:**
- Primary-0: Round 43793
- Primary-1: Round 43793
- Primary-2: Round 43793
- Primary-3: Round 43793
- Primary-4: Round 43793

✅ **Tất cả primaries đều đồng bộ** - không có lag

### Catch-Up Mode

**Logs cho thấy:**
```
[CATCH-UP SYNC] Periodic check - our_proposer_round: 43783, network_round: 43782, lag: 0 rounds
```

✅ **Lag = 0 rounds** - hệ thống không lag, catch-up mode không cần thiết

### Consensus vẫn đang commit

**Logs cho thấy:**
```
GarbageCollector: informed proposer about committed round 43733-43736 (0 batches)
```

⚠️ **Vấn đề:** Consensus đang commit nhưng với **EMPTY payload (0 batches)**

---

## VẤN ĐỀ PHÁT HIỆN

### 1. Batch không được commit

**Batch `0HicXny9SpyIs1ic`:**
- Được tạo lúc: `03:22:47.484Z`
- Đang retry với `retry_count=504` (rất cao!)
- Certificate của primary-0 không được commit ở round batch được tạo

**Logs:**
```
Requeue batch 0HicXny9SpyIs1ic for retry (sent round 43730, current round 43732, latest_committed_round=43730). 
certificate of this primary was not committed at round 43730 - retry immediately
```

### 2. Consensus commit với EMPTY payload

**Logs cho thấy:**
```
[ANALYZE] Node ID 2 RECEIVED certificate ... for round 43715 with EMPTY payload (0 batches).
```

⚠️ **Nhiều certificates được commit nhưng không có batches** - có thể do:
- Leader không extract batches từ headers của primary khác
- Batches không được include trong headers
- Header-based batch extraction không hoạt động

### 3. Batch bị stuck trong retry loop

**Pattern:**
- Batch retry 504+ lần
- Certificate của primary-0 không được commit
- Batch không được leader extract

---

## NGUYÊN NHÂN CÓ THỂ

### 1. Primary-0 không phải Leader

**Vấn đề:** Bullshark chỉ commit certificates của leader. Nếu primary-0 không phải leader ở round batch được tạo, certificate không được commit.

**Giải pháp:** Header-based batch extraction - leader extract batches từ headers của primary khác.

### 2. Header-Based Batch Extraction không hoạt động

**Vấn đề:** Leader có thể không extract batches từ headers của primary-0.

**Kiểm tra cần thiết:**
- Xem leader có nhận được header của primary-0 không
- Xem leader có extract batch này không
- Xem batch có được include trong header của leader không

### 3. Batch Retry Logic không hiệu quả

**Vấn đề:** Batch retry 504+ lần nhưng vẫn không được commit.

**Nguyên nhân:**
- Primary-0 vẫn không phải leader ở các round retry
- Leader không extract batch này

---

## PHÂN TÍCH CHI TIẾT

### Catch-Up Mode hoạt động tốt

✅ **Logs cho thấy:**
- Periodic check chạy đúng (mỗi 2 giây)
- Lag calculation chính xác (lag = 0)
- Không có lag giữa các primaries

### Vấn đề không phải do lag

❌ **Hệ thống không lag:**
- Tất cả primaries ở cùng round
- Lag = 0 rounds
- Catch-up mode không cần thiết

### Vấn đề là batch không được commit

⚠️ **Root cause:**
- Primary-0 không phải leader
- Leader không extract batches từ primary-0
- Batch bị stuck trong retry loop

---

## GIẢI PHÁP

### 1. Cải thiện Header-Based Batch Extraction

**Vấn đề:** Leader có thể không extract batches từ headers của primary khác.

**Giải pháp:**
- Đảm bảo leader luôn extract batches từ tất cả headers
- Thêm logging để track batch extraction
- Verify batch extraction hoạt động đúng

### 2. Cải thiện Batch Retry Logic

**Vấn đề:** Batch retry quá nhiều lần mà không được commit.

**Giải pháp:**
- Thêm "Batch Rescue" mechanism: Khi batch retry >100 lần, gửi batch đến các primary khác
- Thêm timeout: Nếu batch không commit sau N rounds, mark as failed
- Thêm metrics để track batch retry success rate

### 3. Thêm Monitoring

**Giải pháp:**
- Track batch commit rate per primary
- Alert khi batch retry count > threshold
- Monitor batch extraction success rate

---

## KẾT LUẬN

### Tình trạng:
- ✅ Hệ thống vẫn đang chạy (không đứng)
- ✅ Không có lag giữa các primaries
- ✅ Catch-up mode hoạt động tốt
- ❌ Batch không được commit (retry 504+ lần)
- ❌ Consensus commit với EMPTY payload

### Root cause:
- Primary-0 không phải leader ở round batch được tạo
- Leader không extract batches từ primary-0
- Header-based batch extraction có thể không hoạt động đúng

### Hành động cần thiết:
1. ✅ Kiểm tra header-based batch extraction có hoạt động không
2. ✅ Thêm logging để track batch extraction
3. ✅ Cải thiện batch retry logic
4. ✅ Thêm batch rescue mechanism

---

## METRICS CẦN THEO DÕI

1. **Batch Commit Rate per Primary**: Tỷ lệ batch được commit của mỗi primary
2. **Batch Retry Count Distribution**: Phân bố số lần retry của batches
3. **Batch Extraction Success Rate**: Tỷ lệ thành công của batch extraction
4. **Empty Payload Rate**: Tỷ lệ certificates được commit với empty payload

