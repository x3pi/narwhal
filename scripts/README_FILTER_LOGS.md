# Filter Batch Logs - Hướng dẫn sử dụng

## Mục đích

Các script này giúp filter log và loại bỏ log của batch đã được gửi đến Unix Domain Socket (UDS) để thực thi. Điều này giúp giảm kích thước log file và chỉ giữ lại log của batch chưa được thực thi.

## Lưu ý quan trọng

**Không thể xóa log đã ghi trong Rust**, nhưng có thể:
1. Filter log file sau khi chạy để loại bỏ log của batch đã được gửi đến UDS
2. Sử dụng log target `narwhal_audit` để filter log khi chạy hệ thống
3. Sử dụng script để tự động filter log file

## Cách sử dụng

### 1. Filter log bằng script đơn giản

```bash
./scripts/filter_batch_logs.sh benchmark/logs/worker-0-0.log benchmark/logs/worker-0-0-filtered.log
```

Script này sẽ:
- Tìm tất cả batch IDs đã được gửi đến UDS
- Loại bỏ các log chứa batch_id đó
- Giữ lại log `[BATCH SENT TO UDS]` để đánh dấu

### 2. Filter log bằng script nâng cao

```bash
./scripts/filter_batch_logs_advanced.sh benchmark/logs/worker-0-0.log benchmark/logs/worker-0-0-filtered.log
```

Script này sẽ:
- Tìm tất cả batch IDs đã được gửi đến UDS
- Loại bỏ tất cả log của batch TRƯỚC khi batch được gửi đến UDS
- Giữ lại log `[BATCH SENT TO UDS]` để đánh dấu batch đã hoàn thành

### 3. Filter log khi chạy hệ thống (sử dụng RUST_LOG)

```bash
# Chỉ hiển thị log với target "narwhal_audit"
RUST_LOG=narwhal_audit=info ./target/release/node ...

# Hoặc filter log sau khi chạy
grep 'target":"narwhal_audit"' benchmark/logs/*.log > benchmark/logs/audit-only.log
```

### 4. Filter log bằng jq (cho JSON logs)

```bash
# Chỉ giữ lại log của batch chưa được gửi đến UDS
jq 'select(.fields.message | contains("[BATCH SENT TO UDS]") | not)' benchmark/logs/worker-0-0.log > benchmark/logs/worker-0-0-filtered.log
```

## Log markers

Các log marker quan trọng:
- `[BATCH SENT TO UDS]`: Batch đã được gửi đến UDS để thực thi
- `[BATCH TO UDS]`: Batch đã được gửi đến UDS (từ macro)
- `[BATCH COMMITTED]`: Batch đã được commit vào block
- `[BATCH TRACE] Batch CREATED`: Batch được tạo
- `[BATCH TRACE] Batch ADDED to queue`: Batch được thêm vào queue
- `[BATCH TRACE] Batch COLLECTED`: Batch được collect cho header
- `[BATCH TRACE] Batch COMMITTED`: Batch được commit

## Ví dụ

```bash
# Filter tất cả log files
for log_file in benchmark/logs/*.log; do
    filtered_file="${log_file%.log}-filtered.log"
    ./scripts/filter_batch_logs.sh "$log_file" "$filtered_file"
done

# Chỉ giữ lại log của batch chưa được gửi đến UDS
grep -v '\[BATCH SENT TO UDS\]' benchmark/logs/worker-0-0.log | \
    grep -v '\[BATCH TO UDS\]' > benchmark/logs/worker-0-0-uncommitted.log
```

## Lưu ý

1. Script sẽ tạo file mới, không sửa file gốc
2. Nên backup log file trước khi filter
3. Script có thể chạy chậm với log file lớn
4. Có thể sử dụng `parallel` để filter nhiều file cùng lúc

