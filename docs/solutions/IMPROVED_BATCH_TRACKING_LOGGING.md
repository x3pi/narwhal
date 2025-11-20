# CẢI THIỆN LOGGING: TRACK BATCH TỐT HƠN

## TỔNG QUAN

Đã cải thiện logging để track batch từ đầu đến cuối trong toàn bộ hệ thống, từ worker -> primary -> consensus -> node -> UDS.

## CẢI THIỆN TRONG NODE

### 1. Log Prefix: `[BATCH TRACK]`

Tất cả log về batch tracking trong node sử dụng prefix `[BATCH TRACK]` để dễ dàng filter và search.

### 2. Log Lifecycle của Batch

#### A. Khi bắt đầu xử lý batch
```
[BATCH TRACK] Node ID {} PROCESSING batch {} from worker {} in certificate {} (round {}, height {}), current block height: {}
```

#### B. Khi tìm thấy batch trong store
```
[BATCH TRACK] Node ID {} FOUND batch {} from worker {} in store ({} bytes, certificate: {}, round {}, height {}, block height: {}).
```

#### C. Khi hoàn thành xử lý batch
```
[BATCH TRACK] Node ID {} COMPLETED processing batch {} from worker {} (certificate: {}, round {}, height {}, block height: {}). Batch contains {} transactions. Total transactions in block so far: {}
```

#### D. Khi skip batch - duplicate trong block
```
[BATCH TRACK] Node ID {} SKIP batch {} in certificate {} (round {}, height {}) - DUPLICATE within block height {} (already in block builder). This batch appears multiple times in the same certificate or was already added to this block.
```

#### E. Khi skip batch - already processed
```
[BATCH TRACK] Node ID {} SKIP batch {} in certificate {} (round {}, height {}) - ALREADY PROCESSED in a previous block (current block height: {}). This batch was already executed and sent to UDS in an earlier block. Skipping to avoid duplicate execution.
```

#### F. Khi batch không tìm thấy trong store
```
[BATCH TRACK] Node ID {} NOT FOUND batch {} from worker {} in certificate {} (round {}, height {}, block height: {}). Batch not found in store - may have been garbage collected or not received. WARNING: Transactions in this batch will be MISSING from the block!
```

#### G. Khi batch failed to deserialize
```
[BATCH TRACK] Node ID {} FAILED to deserialize batch {} from worker {} in certificate {} (round {}, height {}, block height: {}). Digest did not correspond to a Batch message.
```

#### H. Khi batch được gửi tới UDS
```
[BATCH TRACK] Node ID {} SUCCESSFULLY sent block height {} (leader round {}) to UDS containing {} transactions from {} batches. Batches: {:?}
```

#### I. Khi batch được mark as processed
```
[BATCH TRACK] Node ID {} MARKED batch {} as PROCESSED (sent to UDS in block height {})
```

### 3. Certificate Summary

Sau khi xử lý xong tất cả batches trong certificate, log summary:

```
[BATCH TRACK] Node ID {} CERTIFICATE SUMMARY: certificate {} (round {}, height {}, block height: {}) contains {} batches: {} processed, {} skipped (duplicate in block), {} skipped (already processed), {} not found, {} failed
```

Summary bao gồm:
- **batches_in_cert**: Tổng số batches trong certificate
- **batches_processed**: Số batches được xử lý thành công
- **batches_skipped_duplicate_in_block**: Số batches bị skip vì duplicate trong cùng block
- **batches_skipped_already_processed**: Số batches bị skip vì đã được xử lý trong block trước đó
- **batches_not_found**: Số batches không tìm thấy trong store
- **batches_failed**: Số batches failed (deserialize error, etc.)

## CẢI THIỆN TRONG PRIMARY

### 1. Log Prefix: `[BATCH TRACK PRIMARY]`

Tất cả log về batch tracking trong primary sử dụng prefix `[BATCH TRACK PRIMARY]` để dễ dàng filter.

### 2. Extract Batches từ Parent Certificates

#### A. Khi extract batch thành công
```
[BATCH TRACK PRIMARY] Primary {} EXTRACTED batch {} (worker {}) from parent certificate {} (round {}) into queue for round {} to help leader commit batches from other primaries. Batch size: {} bytes, retry_count: 0
```

#### B. Khi skip batch - already committed
```
[BATCH TRACK PRIMARY] Primary {} SKIP extracting batch {} from parent certificate {} (round {}) - ALREADY COMMITTED
```

#### C. Khi skip batch - already in queue
```
[BATCH TRACK PRIMARY] Primary {} SKIP extracting batch {} from parent certificate {} (round {}) - ALREADY IN QUEUE
```

### 3. Collect Batches cho Header

```
[BATCH TRACK PRIMARY] Primary {} COLLECTING batch {} from worker {} for header round {} (size {} bytes, retry_count={}). Accumulated payload = {} / target {} bytes
```

## LỢI ÍCH

### 1. Dễ dàng trace batch

Với prefix `[BATCH TRACK]` và `[BATCH TRACK PRIMARY]`, có thể dễ dàng trace batch qua toàn bộ hệ thống:

```bash
# Tìm tất cả log về một batch cụ thể
grep "QsX1N8yE13YO+JQjayx6znVJna/aWogridbwolfUINs=" benchmark/logs/*.log | grep "BATCH TRACK"

# Tìm tất cả batches được xử lý
grep "BATCH TRACK.*COMPLETED" benchmark/logs/node*.log

# Tìm tất cả batches bị skip
grep "BATCH TRACK.*SKIP" benchmark/logs/node*.log

# Tìm certificate summary
grep "BATCH TRACK.*CERTIFICATE SUMMARY" benchmark/logs/node*.log
```

### 2. Hiểu rõ batch lifecycle

Logs cho biết:
- Batch được tạo ở đâu (worker)
- Batch được thêm vào queue khi nào (primary)
- Batch được commit trong certificate nào (consensus)
- Batch được xử lý trong block nào (node)
- Batch được gửi tới UDS khi nào (node)

### 3. Debug dễ dàng hơn

Với certificate summary, có thể nhanh chóng xác định:
- Bao nhiêu batches trong certificate được xử lý thành công
- Bao nhiêu batches bị skip và lý do
- Bao nhiêu batches không tìm thấy trong store
- Bao nhiêu batches failed

### 4. Track batch qua nhiều certificates

Logs cho biết:
- Batch xuất hiện trong certificate nào (cert_digest, round, height)
- Batch được xử lý trong block nào (block height)
- Batch đã được xử lý trước đó hay chưa (already processed)

## VÍ DỤ SỬ DỤNG

### Tìm batch không được thực thi

```bash
# 1. Tìm batch trong worker log
grep "QsX1N8yE13YO+JQjayx6znVJna/aWogridbwolfUINs=" benchmark/logs/worker*.log

# 2. Tìm batch trong primary log
grep "QsX1N8yE13YO+JQjayx6znVJna/aWogridbwolfUINs=" benchmark/logs/primary*.log

# 3. Tìm batch trong node log
grep "QsX1N8yE13YO+JQjayx6znVJna/aWogridbwolfUINs=" benchmark/logs/node*.log | grep "BATCH TRACK"

# 4. Kiểm tra batch có bị skip không
grep "QsX1N8yE13YO+JQjayx6znVJna/aWogridbwolfUINs=" benchmark/logs/node*.log | grep "SKIP"

# 5. Kiểm tra batch có được xử lý không
grep "QsX1N8yE13YO+JQjayx6znVJna/aWogridbwolfUINs=" benchmark/logs/node*.log | grep "COMPLETED"

# 6. Kiểm tra batch có được gửi tới UDS không
grep "QsX1N8yE13YO+JQjayx6znVJna/aWogridbwolfUINs=" benchmark/logs/node*.log | grep "SUCCESSFULLY sent"
```

### Tìm certificate chứa batch

```bash
# Tìm tất cả log về một certificate cụ thể
grep "certificate.*<cert_digest>" benchmark/logs/node*.log | grep "BATCH TRACK"

# Xem summary của certificate
grep "CERTIFICATE SUMMARY.*<cert_digest>" benchmark/logs/node*.log
```

## KẾT LUẬN

Với các cải thiện logging này, có thể:
1. **Track batch từ đầu đến cuối**: Từ worker -> primary -> consensus -> node -> UDS
2. **Debug dễ dàng hơn**: Biết chính xác batch bị skip ở đâu và lý do
3. **Phân tích performance**: Xem batch được xử lý nhanh hay chậm
4. **Đảm bảo không bỏ rơi**: Dễ dàng phát hiện batch không được xử lý

