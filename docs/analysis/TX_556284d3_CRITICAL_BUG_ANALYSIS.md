# PHÂN TÍCH BUG NGHIÊM TRỌNG: HỆ THỐNG BỊ ĐỨNG SAU GIAO DỊCH 556284d3...

## TỔNG QUAN

**Transaction Hash:** `556284d3f1937a2070d2aaff2c856774e0ac3edd9fe6dfc8e9268b6d7aad0d19`  
**Batch Digest 1:** `AFf+EV5WjEzb8G9sZ/Xz4sLWK+1ReCIWbmoH/95YZN0=` (chứa giao dịch này)  
**Batch Digest 2:** `rbmITMsBm4S8Ox+Vs8zyrVu1KCkMqwxXjPZB1xnjEv8=` (batch tiếp theo)  
**Worker:** 0  
**Primary:** 0 (AqJy7eip40qqZk7F)  
**Status:** ❌ **BUG NGHIÊM TRỌNG - HỆ THỐNG BỊ ĐỨNG**

## VẤN ĐỀ

1. ❌ **Giao dịch không được thực thi**
2. ❌ **Sau giao dịch này, tất cả giao dịch sau đó đều không được thực thi**
3. ❌ **Hệ thống chỉ tạo empty blocks (0 transactions)**
4. ❌ **Hệ thống không thể tiếp tục hoạt động**

## TIMELINE CHI TIẾT

### 1. Worker Tạo Batch (08:06:42.880)

```
[2025-11-20T08:06:42.880Z] [TX LOG 0] Hash: 556284d3f1937a2070d2aaff2c856774e0ac3edd9fe6dfc8e9268b6d7aad0d19
[2025-11-20T08:06:42.880Z] [TX LOG BATCH] Worker: 0, Total: 1, Size: 656 bytes
```

**Phân tích:**
- ✅ Batch được tạo thành công
- ✅ Batch chứa giao dịch `556284d3...`

### 2. Primary Nhận Batch (08:06:42.794)

```
[2025-11-20T08:06:42.794Z] [BATCH TRACK PRIMARY] Primary AqJy7eip40qqZk7F COLLECTING batch AFf+EV5WjEzb8G9s from worker 0 for header round 78088 (size 32 bytes, retry_count=0)
```

**Phân tích:**
- ✅ Batch được primary-0 nhận và enqueue thành công
- ✅ Batch ở round 78088

### 3. Primary Tạo Headers Nhưng Không Được Commit

```
[2025-11-20T08:06:42.796Z] Created B78088(AqJy7eip40qqZk7F) -> AFf+EV5WjEzb8G9sZ/Xz4sLWK+1ReCIWbmoH/95YZN0=
[2025-11-20T08:06:42.991Z] Requeue batch AFf+EV5WjEzb8G9s for retry (sent round 78088, current round 78091, latest_committed_round=78088). certificate of this primary was not committed at round 78088 - retry immediately
```

**Phân tích:**
- ⚠️ Primary-0 tạo header round 78088 với batch
- ❌ **Certificate của primary-0 KHÔNG được commit** (primary khác là leader)
- ⚠️ Batch bị retry ngay lập tức

### 4. Vòng Lặp Retry Vô Hạn

```
[2025-11-20T08:06:43.003Z] Created B78092(AqJy7eip40qqZk7F) -> AFf+EV5WjEzb8G9sZ/Xz4sLWK+1ReCIWbmoH/95YZN0=
[2025-11-20T08:06:43.003Z] Created B78092(AqJy7eip40qqZk7F) -> rbmITMsBm4S8Ox+Vs8zyrVu1KCkMqwxXjPZB1xnjEv8=
[2025-11-20T08:06:43.394Z] Requeue batch AFf+EV5WjEzb8G9s for retry (sent round 78092, current round 78098, latest_committed_round=78096). certificate of this primary was not committed at round 78092 - retry immediately
[2025-11-20T08:06:43.394Z] Requeue batch rbmITMsBm4S8Ox+V for retry (sent round 78092, current round 78098, latest_committed_round=78096). certificate of this primary was not committed at round 78092 - retry immediately

[2025-11-20T08:06:43.411Z] Created B78099(AqJy7eip40qqZk7F) -> AFf+EV5WjEzb8G9sZ/Xz4sLWK+1ReCIWbmoH/95YZN0=
[2025-11-20T08:06:43.411Z] Created B78099(AqJy7eip40qqZk7F) -> rbmITMsBm4S8Ox+Vs8zyrVu1KCkMqwxXjPZB1xnjEv8=
[2025-11-20T08:06:43.595Z] Requeue batch AFf+EV5WjEzb8G9s for retry (sent round 78099, current round 78102, latest_committed_round=78100). certificate of this primary was not committed at round 78099 - retry immediately
[2025-11-20T08:06:43.595Z] Requeue batch rbmITMsBm4S8Ox+V for retry (sent round 78099, current round 78102, latest_committed_round=78100). certificate of this primary was not committed at round 78099 - retry immediately

... (retry liên tục với retry_count tăng dần: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14...)
```

**Phân tích:**
- ❌ **Primary-0 không phải leader** trong các rounds này
- ❌ **Certificates của primary-0 không được commit**
- ❌ **Batch bị retry ngay lập tức** vì "certificate of this primary was not committed"
- ❌ **Retry_count tăng dần** nhưng batch vẫn không được commit

### 5. Batch Bị Force Remove (08:11:05)

```
[2025-11-20T08:11:05.011Z] FORCE REMOVING stuck batch AFf+EV5WjEzb8G9s (retry_count=1001, sent_round=82691, current_round=82693, rounds_since_sent=2). Batch cannot be committed after 1000 retries - removing to prevent system deadlock. Transactions in this batch will be LOST.
[2025-11-20T08:11:05.413Z] FORCE REMOVING stuck batch rbmITMsBm4S8Ox+V (retry_count=1001, sent_round=82698, current_round=82701, rounds_since_sent=3). Batch cannot be committed after 1000 retries - removing to prevent system deadlock. Transactions in this batch will be LOST.
```

**Phân tích:**
- ❌ **Batch đã retry 1001 lần** (vượt quá MAX_RETRY_COUNT = 1000)
- ❌ **Batch bị force remove** để tránh hệ thống bị deadlock
- ❌ **Giao dịch trong batch bị LOST** - không bao giờ được thực thi

### 6. Hệ Thống Bị Đứng (08:09:54+)

```
[2025-11-20T08:09:54.348Z] [BATCH TRACK] Node ID 2 SUCCESSFULLY sent block height 40727 (leader round 81454) to UDS containing 0 transactions from 0 batches. Batches: []
[2025-11-20T08:09:54.459Z] [BATCH TRACK] Node ID 2 SUCCESSFULLY sent block height 40728 (leader round 81456) to UDS containing 0 transactions from 0 batches. Batches: []
[2025-11-20T08:09:54.665Z] [BATCH TRACK] Node ID 2 SUCCESSFULLY sent block height 40729 (leader round 81458) to UDS containing 0 transactions from 0 batches. Batches: []
... (hàng trăm empty blocks)
```

**Phân tích:**
- ❌ **Node chỉ tạo empty blocks** (0 transactions)
- ❌ **Không có batch nào được xử lý**
- ❌ **Hệ thống đã bị đứng hoàn toàn**

## ROOT CAUSE ANALYSIS

### Nguyên Nhân Chính: Primary-0 Không Phải Leader

**Vấn đề:**
1. Primary-0 (AqJy7eip40qqZk7F) tạo headers với batches
2. Nhưng primary-0 **không phải leader** trong các rounds này
3. Certificates của primary-0 **không được commit** bởi consensus
4. Batch bị retry ngay lập tức vì logic "certificate of this primary was not committed"
5. Batch bị retry liên tục nhưng **vẫn không được commit** vì primary-0 không phải leader

### Tại Sao Header-Based Extraction Không Giúp?

**Vấn đề:**
- Header-Based Extraction chỉ giúp **leader** extract batches từ headers của non-leader primaries
- Nhưng nếu **primary-0 không phải leader**, leader khác có thể extract batch từ header của primary-0
- Tuy nhiên, nếu leader khác **không extract batch** (do logic skip hoặc timing), batch sẽ bị stuck

### Tại Sao Hệ Thống Bị Đứng?

**Vấn đề:**
1. Batch bị retry liên tục với retry_count tăng dần (0 → 1 → 2 → ... → 900 → 901 → 902 → ... → 1000 → 1001)
2. Retry_count đạt MAX_RETRY_COUNT (1000) nhưng batch vẫn không được commit
3. Khi retry_count > 1000, batch sẽ bị **force remove** (mark as Committed và remove)
4. **Giao dịch trong batch bị LOST** - không bao giờ được thực thi
5. **Các batch mới** cũng gặp vấn đề tương tự (primary-0 không phải leader)
6. Hệ thống chỉ tạo empty blocks vì không có batch nào được commit

**Evidence:**
- Batch `AFf+EV5WjEzb8G9s` retry_count=1001 → FORCE REMOVING
- Batch `rbmITMsBm4S8Ox+V` retry_count=1001 → FORCE REMOVING
- Batch `871Ra2dXqe240GYx` retry_count=1001 → FORCE REMOVING
- Batch `qvuJhafqvxTvttee` retry_count=1001 → FORCE REMOVING

## IMPACT

### 1. Giao Dịch Không Được Thực Thi
- ❌ Giao dịch `556284d3...` không được thực thi (batch bị force remove sau 1001 retries)
- ❌ Tất cả giao dịch sau đó đều không được thực thi (các batch mới cũng gặp vấn đề tương tự)
- ❌ **Giao dịch bị LOST** - không bao giờ được thực thi

### 2. Hệ Thống Bị Đứng
- ❌ Node chỉ tạo empty blocks
- ❌ Không có batch nào được xử lý
- ❌ Hệ thống không thể tiếp tục hoạt động

### 3. Liveness Violation
- ❌ Hệ thống mất liveness - không thể commit batches mới
- ❌ Mặc dù consensus vẫn hoạt động (tạo empty blocks), nhưng không có transactions được xử lý

## GIẢI PHÁP

### Giải Pháp 1: Cải Thiện Leader Batch Extraction (URGENT)

**Mô tả:**
- Đảm bảo leader **luôn extract batches** từ headers của non-leader primaries
- Không skip batch nếu batch chưa được commit

**Implementation:**
- Cải thiện logic `extract_batches_from_headers` để **luôn extract** batches từ non-leader primaries
- Không skip batch nếu batch chưa được commit, ngay cả khi batch đã trong queue

### Giải Pháp 2: Force Remove Stuck Batches Sớm Hơn (SHORT-TERM) - ⚠️ KHÔNG ĐỦ

**Mô tả:**
- Giảm MAX_RETRY_COUNT từ 1000 xuống 100 hoặc 50
- Force remove batch sớm hơn để tránh hệ thống bị đứng

**Trade-off:**
- ⚠️ Batch có thể bị mất nếu force remove quá sớm
- ✅ Nhưng hệ thống sẽ không bị đứng
- ⚠️ **KHÔNG GIẢI QUYẾT ROOT CAUSE:** Các batch mới vẫn sẽ gặp vấn đề tương tự

### Giải Pháp 3: Cải Thiện Retry Logic (MEDIUM-TERM)

**Mô tả:**
- Không retry batch ngay lập tức nếu "certificate of this primary was not committed"
- Chờ một khoảng thời gian để leader có cơ hội extract batch
- Chỉ retry nếu batch thực sự bị stuck (retry_count > threshold)

**Implementation:**
- Thêm delay trước khi retry batch
- Chỉ retry nếu retry_count > 5 (thay vì retry ngay lập tức)

### Giải Pháp 4: Proactive Batch Extraction (LONG-TERM)

**Mô tả:**
- Leader **proactively extract batches** từ tất cả headers của non-leader primaries
- Không đợi batch được retry, mà extract ngay khi header được verify

**Implementation:**
- Cải thiện Header-Based Extraction để **luôn extract** batches
- Không skip batch nếu batch chưa được commit

## KHUYẾN NGHỊ

### Immediate (URGENT):
1. ⚠️ **Giảm MAX_RETRY_COUNT** từ 1000 xuống 100 (chỉ là workaround, không giải quyết root cause)
2. ✅ **Cải thiện Leader Batch Extraction** (CRITICAL) - đảm bảo leader luôn extract batches từ non-leader primaries
3. ✅ **Fix Retry Logic** - không retry ngay lập tức nếu "certificate of this primary was not committed", chờ leader extract batch

### Short-term:
1. ⏳ **Cải thiện Retry Logic** - không retry ngay lập tức, chờ leader extract batch
2. ⏳ **Thêm logging** để track batch extraction và retry

### Long-term:
1. ⏳ **Proactive Batch Extraction** - leader proactively extract batches từ tất cả headers
2. ⏳ **Cải thiện Network Synchronization** - đảm bảo tất cả nodes có cùng view của DAG

## METRICS

### Logs to Monitor:
1. **Batch retry:**
   ```
   Requeue batch {} for retry (sent round {}, current round {}, latest_committed_round={}). certificate of this primary was not committed at round {} - retry immediately
   ```

2. **Empty blocks:**
   ```
   SUCCESSFULLY sent block height {} to UDS containing 0 transactions from 0 batches
   ```

3. **Force remove:**
   ```
   FORCE REMOVING stuck batch {} (retry_count={}, sent_round={}, current_round={})
   ```

### Metrics to Track:
1. **Retry count per batch:** Số lần retry của mỗi batch
2. **Empty block rate:** Tỷ lệ empty blocks
3. **Batch commit rate:** Tỷ lệ batches được commit
4. **Leader extraction rate:** Tỷ lệ batches được leader extract

## KẾT LUẬN

### Status:
- ❌ **BUG NGHIÊM TRỌNG:** Hệ thống bị đứng sau giao dịch `556284d3...`
- ❌ **Root Cause:** Primary-0 không phải leader, certificates không được commit, batch bị retry vô hạn
- ❌ **Impact:** Hệ thống không thể tiếp tục hoạt động, chỉ tạo empty blocks

### Next Steps:
1. ⏳ **URGENT:** Giảm MAX_RETRY_COUNT và cải thiện Leader Batch Extraction
2. ⏳ **SHORT-TERM:** Cải thiện Retry Logic
3. ⏳ **LONG-TERM:** Proactive Batch Extraction

---

**Last Updated:** 2025-01-20  
**Status:** ❌ **CRITICAL BUG - SYSTEM HALTED**

