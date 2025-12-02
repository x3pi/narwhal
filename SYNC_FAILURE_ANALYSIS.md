# Phân tích: Tại sao Sync được Trigger nhưng Batches vẫn tiếp tục MISSING

## Tóm tắt Vấn đề

Sync được trigger (có log `[SYNC BATCHES SEND]`, `[SYNC BATCHES FANOUT SEND]`) nhưng batches vẫn tiếp tục MISSING trong các headers sau. Điều này cho thấy **sync không thành công hoặc quá chậm**.

## Flow Sync Mechanism

### 1. Primary → Worker (Sync Request)
```
Primary detect missing batches
  ↓
Send PrimaryWorkerMessage::Synchronize(digests, target) to worker
  ↓
Log: [SYNC BATCHES SEND] Requesting batches from worker
  ↓
Log: [SYNC BATCHES FANOUT SEND] Sending batch sync request to peer worker
```

### 2. Worker → Worker (Batch Request)
```
Worker nhận Synchronize message
  ↓
Check local store
  ↓
Nếu missing, gửi WorkerMessage::BatchRequest(missing, requestor) đến target worker
  ↓
Log: Worker requesting X missing batch(es) from target
```

### 3. Target Worker → Requestor Worker (Batch Response)
```
Target worker nhận BatchRequest
  ↓
Check store
  ↓
Nếu có, gửi batch về requestor worker
  ↓
Log: [WORKER HELPER] Worker sending batch X to requester
```

### 4. Worker → Primary (Batch Delivery)
```
Worker nhận batch từ target worker
  ↓
Lưu vào store
  ↓
Gửi WorkerPrimaryMessage::OthersBatch đến primary
  ↓
Log: [BATCH RECEIVED FROM WORKER] PayloadReceiver received batch
```

## Phân tích Logs

### Primary-3 (AgF2i8f4TnfU3Bjs)

#### Sync được Trigger:
```
13:17:50.504210Z [SYNC BATCHES SEND] Requesting batches from worker
13:17:50.504599Z [SYNC BATCHES FANOUT SEND] Sending batch sync request to peer worker
13:17:50.504961Z [SYNC BATCHES FANOUT SEND] Sending batch sync request to peer worker
13:17:50.505361Z [SYNC BATCHES FANOUT SEND] Sending batch sync request to peer worker
13:17:50.505743Z [SYNC BATCHES FANOUT SEND] Sending batch sync request to peer worker
```

#### Nhưng Batches vẫn MISSING:
```
13:17:50.668541Z [VOTE DEBUG - MISSING BATCHES] Primary AgF2i8f4TnfU3Bjs checking payload for header NWj9+SGfb16TWMCq (round 14017, author: AqJy7eip40qqZk7F). Total batches: 2, Found in cache: 0, Found in store: 0, MISSING: 2 batches from workers [0]: ["Wqf86btMR9eBnaFY", "+R4dnncc8WdDcqx8"]
```

**Khoảng thời gian**: Chỉ 164ms từ khi sync được trigger đến khi check lại → **Sync quá chậm hoặc không thành công**

### Primary-4 (ApvX+ZCVrGWssP/v)

#### Sync được Trigger:
```
13:17:50.507518Z [SYNC BATCHES SEND] Requesting batches from worker
13:17:50.509767Z [SYNC BATCHES FANOUT SEND] Sending batch sync request to peer worker
13:17:50.511627Z [SYNC BATCHES FANOUT SEND] Sending batch sync request to peer worker
13:17:50.513743Z [SYNC BATCHES FANOUT SEND] Sending batch sync request to peer worker
13:17:50.515058Z [SYNC BATCHES FANOUT SEND] Sending batch sync request to peer worker
```

#### Nhưng Batches vẫn MISSING:
```
13:17:50.661826Z [VOTE DEBUG - MISSING BATCHES] Primary ApvX+ZCVrGWssP/v checking payload for header NWj9+SGfb16TWMCq (round 14017, author: AqJy7eip40qqZk7F). Total batches: 2, Found in cache: 0, Found in store: 0, MISSING: 2 batches from workers [0]: ["Wqf86btMR9eBnaFY", "+R4dnncc8WdDcqx8"]
```

**Khoảng thời gian**: Chỉ 154ms từ khi sync được trigger đến khi check lại → **Sync quá chậm hoặc không thành công**

## Nguyên nhân Có thể

### 1. Worker không nhận được Sync Request (Network Issue)

**Triệu chứng**: 
- Primary gửi sync request nhưng worker không nhận được
- Không có log về worker nhận sync request

**Kiểm tra**: 
- Network connectivity giữa primary và worker
- Message delivery guarantee
- Channel capacity

### 2. Target Worker không có Batch trong Store

**Triệu chứng**:
- Worker nhận sync request và gửi batch request đến target worker
- Target worker không có batch trong store
- Log: `[WORKER HELPER] Worker missing batch X requested by Y`

**Nguyên nhân**:
- Batch chưa được gửi từ worker đến primary
- Batch chưa được replicate đến target worker
- Batch đã bị garbage collected

### 3. Target Worker không trả lời Batch Request (Timeout/Network Issue)

**Triệu chứng**:
- Worker gửi batch request nhưng không nhận được response
- Không có log về worker helper trả lời

**Nguyên nhân**:
- Network timeout
- Target worker quá tải
- Message loss

### 4. Worker nhận Batch nhưng không gửi đến Primary

**Triệu chứng**:
- Worker nhận batch từ target worker
- Nhưng primary không nhận được batch
- Không có log `[BATCH RECEIVED FROM WORKER]`

**Nguyên nhân**:
- Channel đầy (`tx_others_digests` channel full)
- Worker không gửi batch đến primary
- Primary không nhận được message

### 5. Sync quá Chậm (Timeout)

**Triệu chứng**:
- Sync được trigger nhưng mất quá nhiều thời gian
- Primary check lại trước khi sync hoàn thành
- Batches vẫn MISSING

**Nguyên nhân**:
- Network latency cao
- Worker store I/O chậm
- Multiple hops trong sync chain

### 6. Batch đã bị Garbage Collected

**Triệu chứng**:
- Batch đã được tạo và gửi trước đó
- Nhưng khi sync, batch không còn trong store

**Nguyên nhân**:
- Batch đã bị GC do quá cũ
- Store cleanup quá aggressive

## Phân tích Cụ thể

### Vấn đề: Sync Request không đến Worker

Từ logs, tôi **KHÔNG THẤY**:
- Log về worker nhận sync request: `Worker X-Y: received sync request for Z digest(s)`
- Log về worker gửi batch request: `Worker X-Y: requesting Z missing batch(es)`
- Log về worker helper trả lời: `[WORKER HELPER] Worker X replying to Y batch digests`
- Log về primary nhận batch sau sync: `[BATCH RECEIVED FROM WORKER]`

**Kết luận**: Sync request có thể **KHÔNG ĐẾN WORKER** hoặc worker **KHÔNG XỬ LÝ** sync request.

### Vấn đề: Timeout quá Ngắn

Primary check lại batches chỉ sau **154-164ms** từ khi sync được trigger. Đây là quá ngắn cho một sync operation qua network.

**Flow sync**:
1. Primary → Worker (sync request): ~10-50ms
2. Worker check store: ~1-10ms
3. Worker → Target Worker (batch request): ~10-50ms
4. Target Worker check store: ~1-10ms
5. Target Worker → Requestor Worker (batch): ~10-50ms
6. Worker → Primary (batch): ~10-50ms

**Tổng thời gian**: ~42-220ms (có thể lâu hơn nếu network chậm)

**Kết luận**: Primary check lại quá sớm, sync chưa kịp hoàn thành.

## Giải pháp Đề xuất

### 1. Thêm Logging Chi tiết (Ưu tiên cao)

**Thêm log tại**:
- Worker nhận sync request
- Worker gửi batch request
- Worker helper trả lời batch request
- Worker nhận batch từ target worker
- Worker gửi batch đến primary

**Mục đích**: Xác định chính xác điểm nào trong sync chain bị lỗi.

### 2. Tăng Timeout cho Sync

**Vấn đề**: Primary check lại quá sớm (154-164ms).

**Giải pháp**:
- Tăng timeout cho sync operation
- Đợi ít nhất 500ms-1s trước khi check lại
- Hoặc implement async wait cho sync completion

### 3. Cải thiện Batch Replication

**Vấn đề**: Batch không có sẵn tại target worker khi sync.

**Giải pháp**:
- Replicate batch ngay khi nhận từ worker
- Đảm bảo batch có sẵn tại tất cả primaries trước khi header được proposed

### 4. Parallel Sync từ Nhiều Peers

**Vấn đề**: Sync từ một worker có thể fail.

**Giải pháp**:
- Sync từ nhiều workers song song
- Sử dụng worker đầu tiên trả lời

### 5. Retry Logic

**Vấn đề**: Sync fail một lần thì không retry.

**Giải pháp**:
- Retry sync request nếu không nhận được response
- Exponential backoff cho retry

### 6. Pre-sync Batches

**Vấn đề**: Sync chỉ được trigger khi vote, quá muộn.

**Giải pháp**:
- Pre-sync batches trước khi vote
- Sync batches khi nhận header (không đợi vote)

## Kết luận

**Nguyên nhân chính**: Sync request có thể **KHÔNG ĐẾN WORKER** hoặc sync **QUÁ CHẬM** so với thời gian primary check lại.

**Giải pháp ưu tiên**:
1. Thêm logging chi tiết để xác định điểm lỗi
2. Tăng timeout cho sync operation
3. Cải thiện batch replication để giảm nhu cầu sync

