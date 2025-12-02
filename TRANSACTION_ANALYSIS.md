# Phân tích Transaction không được thực thi

## Transaction Hash
`c0382f5f2966d112ab2a8db2c53f11bfdc6e0c02181bc43e43a72aaa7f09d4ad`

## Timeline

### 1. Transaction được nhận
- **Timestamp**: `2025-11-30T05:56:43.643479Z`
- **Log**: `[TX RECEIVED] Worker received transaction`
- **Worker ID**: 0
- **From**: `e730d4572f20a4d701ebb80b8b5afa99b36d5e49`
- **To**: `0000000000000000000000000000000000000000`
- **Size**: 464 bytes

### 2. Transaction KHÔNG được thêm vào batch
- **Không có log**: `[TX TO BATCH] Transaction đã được thêm vào batch`
- **Nguyên nhân có thể**: 
  - Batch đã được seal trước khi transaction được thêm vào
  - Race condition giữa timer và transaction receive
  - Transaction bị lost trong channel

### 3. Transaction tiếp theo
- **Timestamp**: `2025-11-30T05:56:43.740638Z` (97ms sau)
- **Hash**: `e4f12ae456b0b90df780d312375cf980`
- **Log**: `[TX TO BATCH] Transaction đã được thêm vào batch`
- **Batch ID**: `KIlzQshXydvAuQD/`

## Phân tích nguyên nhân

### Vấn đề: Race Condition trong BatchMaker

Trong `batch_maker.rs`, logic hoạt động như sau:

```rust
tokio::select! {
    Some(transaction) = self.rx_transaction.recv() => {
        self.current_batch_size += transaction.len();
        self.current_batch.push(transaction);
        if self.current_batch_size >= self.batch_size {
            self.seal().await;  // Seal batch ngay lập tức
        }
    },
    () = &mut timer => {
        if !self.current_batch.is_empty() {
            self.seal().await;  // Seal batch khi timer trigger
        }
    }
}
```

### Vấn đề tiềm ẩn:

1. **Timer có thể trigger trước khi transaction được thêm vào**
   - Timer có thể trigger ngay sau khi transaction được nhận
   - Nếu timer trigger trước khi transaction được push vào batch, transaction sẽ bị mất

2. **Không có log khi transaction được push vào current_batch**
   - Log `[TX TO BATCH]` chỉ xuất hiện khi batch được seal
   - Nếu batch chưa được seal, transaction không có log

3. **Transaction có thể bị mất nếu batch được seal giữa lúc nhận và push**

## Giải pháp đề xuất

### 1. Thêm log ngay khi transaction được push vào batch
- Log ngay sau khi `self.current_batch.push(transaction)`
- Đảm bảo mọi transaction được nhận đều có log

### 2. Đảm bảo transaction không bị mất
- Kiểm tra xem transaction có trong batch trước khi seal
- Nếu transaction chưa được thêm vào batch, đợi một chút trước khi seal

### 3. Thêm tracking cho transactions đã nhận nhưng chưa vào batch
- Track transactions đã nhận nhưng chưa được thêm vào batch
- Retry nếu transaction bị mất

## Kết luận

Transaction `c0382f5f2966d112ab2a8db2c53f11bfdc6e0c02181bc43e43a72aaa7f09d4ad`:
- ✅ Được nhận bởi worker
- ❌ KHÔNG được thêm vào batch
- ❌ KHÔNG được commit
- ❌ KHÔNG được thực thi

**Nguyên nhân**: Race condition trong BatchMaker - transaction được nhận nhưng batch đã được seal trước khi transaction được thêm vào, hoặc transaction bị lost trong quá trình xử lý.

