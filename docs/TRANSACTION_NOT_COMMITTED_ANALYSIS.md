# PHÂN TÍCH: TRANSACTION KHÔNG ĐƯỢC THỰC THI

## THÔNG TIN TRANSACTION

- **Transaction Hash**: `446a6e3f7f0cf78d2938f7a74f45ea5248a6ce56e563863f94a0440466780950`
- **Batch Digest**: `g0+3k3M34P8IOAJMFDrNcGw0WkZ8wGADImuMBbm0d80=`
- **Primary**: `AqJy7eip40qqZk7FqFMJsfRtrZS16YviyXCQ4gpcVYWK` (Primary-0)
- **Worker**: Worker-0 của Primary-0
- **Thời gian tạo batch**: `2025-11-23T01:14:46.313Z`

---

## PHÂN TÍCH LOGS

### 1. Batch được tạo và gửi

```
[2025-11-23T01:14:46.313Z] Batch g0+3k3M34P8IOAJM contains sample tx 167946
[2025-11-23T01:14:46.313Z] Processor: Sending OurBatch message for batch g0+3k3M34P8IOAJM (684 bytes) from worker 0 to primary at 127.0.0.1:11000
```

✅ **Batch được tạo thành công và gửi đến primary-0**

### 2. Primary-0 nhận batch và tạo headers

```
[2025-11-23T01:14:46.314Z] Batch g0+3k3M34P8IOAJM enqueued from worker 0 at round 503633
[2025-11-23T01:14:46.330Z] Primary AqJy7eip40qqZk7F COLLECTING batch g0+3k3M34P8IOAJM from worker 0 for header round 503633
[2025-11-23T01:14:46.330Z] Creating header for round 503633 with digests [g0+3k3M34P8IOAJM...]
[2025-11-23T01:14:46.331Z] Created B503633(AqJy7eip40qqZk7F) -> g0+3k3M34P8IOAJM...
```

✅ **Primary-0 nhận batch và tạo header ở round 503633**

### 3. Certificate không được commit

```
[2025-11-23T01:14:46.710Z] Requeue batch g0+3k3M34P8IOAJM for retry (sent round 503633, current round 503636, latest_committed_round=503634). 
certificate of this primary was not committed at round 503633 - retry immediately
```

❌ **Certificate của primary-0 ở round 503633 không được commit**

### 4. Batch bị retry liên tục

```
Round 503633: retry_count=0 → certificate not committed
Round 503636: retry_count=1 → certificate not committed  
Round 503638: retry_count=2 → certificate not committed
Round 503641: retry_count=3 → certificate not committed
...
Round 505188: retry_count=612 → vẫn retry
```

❌ **Batch bị retry 612+ lần nhưng vẫn không được commit**

---

## NGUYÊN NHÂN

### 1. **Primary-0 không phải Leader**

**Bullshark Consensus Leader Selection:**
```rust
// consensus/src/lib.rs
fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
    let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
    keys.sort();
    let leader_pk = &keys[round as usize % self.committee.size()];
    dag.get(&round).and_then(|x| x.get(leader_pk))
}
```

**Leader được chọn bằng round-robin:**
- Round 503633: Leader = `keys[503633 % 5]` = **KHÔNG PHẢI Primary-0**
- Round 503636: Leader = `keys[503636 % 5]` = **KHÔNG PHẢI Primary-0**
- Round 503638: Leader = `keys[503638 % 5]` = **KHÔNG PHẢI Primary-0**

**Kết quả:**
- ❌ Primary-0 không phải leader ở các round này
- ❌ Certificate của primary-0 không được commit
- ❌ Batch trong header của primary-0 không được commit

### 2. **Header-Based Batch Extraction không hoạt động**

**Cơ chế hiện tại:**
- Leader có thể extract batches từ headers của các primary khác
- Nhưng batch `g0+3k3M34P8IOAJM` **KHÔNG được extract** bởi leader

**Có thể do:**
- Leader không nhận được header của primary-0 (network issue)
- Leader extract nhưng batch không được include trong header mới
- Batch extraction logic có vấn đề

### 3. **Retry Logic không hiệu quả**

**Vấn đề:**
- Batch bị retry 612+ lần
- Nhưng primary-0 vẫn không phải leader ở các round retry
- **Kết quả:** Batch mãi không được commit

---

## SO SÁNH VỚI CÁC BATCH KHÁC

### Batch được commit từ Primary-0:

```
[2025-11-23T01:14:40.110Z] Bullshark: committed 10 certificates via leader round 503592
  → "{round: 503590, origin: AqJy7eip40qqZk7FqFMJsfRtrZS16YviyXCQ4gpcVYWK, digest: FxMEfF0vUB1Ym9iP1jwO+DjWQ4mBdTADpc7DAooF6RM=}"
```

✅ **Primary-0 là leader ở round 503590 → Certificate được commit**

### Batch không được commit:

```
Round 503633: Primary-0 không phải leader → Certificate không được commit
Round 503636: Primary-0 không phải leader → Certificate không được commit
Round 503638: Primary-0 không phải leader → Certificate không được commit
```

❌ **Primary-0 không phải leader ở các round này → Certificate không được commit**

---

## GIẢI PHÁP

### 1. **Cải thiện Header-Based Batch Extraction**

**Vấn đề hiện tại:**
- Leader có thể extract batches từ headers của primary khác
- Nhưng batch extraction có thể không hoạt động đúng

**Giải pháp:**
- Đảm bảo leader luôn extract batches từ headers của tất cả primaries
- Log rõ ràng khi batch được extract và include vào header mới
- Monitor batch extraction success rate

### 2. **Cải thiện Retry Logic**

**Vấn đề hiện tại:**
- Batch retry 612+ lần nhưng vẫn không được commit
- Retry không hiệu quả khi primary không phải leader

**Giải pháp:**
- **Thêm cơ chế "Batch Rescue"**: Khi batch retry quá nhiều lần (>100), gửi batch đến các primary khác để họ include vào headers của họ
- **Thêm timeout**: Nếu batch không được commit sau N rounds, mark as failed và log warning
- **Thêm metrics**: Track batch retry count và success rate

### 3. **Cải thiện Leader Selection**

**Vấn đề hiện tại:**
- Round-robin leader selection có thể không fair nếu một primary tạo nhiều batches

**Giải pháp:**
- **Weighted round-robin**: Ưu tiên primary có nhiều pending batches
- **Batch-aware leader selection**: Leader selection dựa trên số lượng batches pending

### 4. **Thêm Monitoring và Alerting**

**Giải pháp:**
- Track batch commit rate per primary
- Alert khi batch retry count > threshold
- Alert khi primary không có batch được commit trong N rounds

---

## KẾT LUẬN

**Nguyên nhân chính:**
1. ❌ Primary-0 không phải leader ở các round batch được tạo
2. ❌ Header-based batch extraction không hoạt động (batch không được leader extract)
3. ❌ Retry logic không hiệu quả (retry 612+ lần nhưng vẫn không commit)

**Giải pháp ngắn hạn:**
- ✅ Cải thiện header-based batch extraction để đảm bảo leader extract batches từ tất cả primaries
- ✅ Thêm batch rescue mechanism để gửi batch đến các primary khác khi retry quá nhiều

**Giải pháp dài hạn:**
- ✅ Cải thiện leader selection để fair hơn
- ✅ Thêm monitoring và alerting để phát hiện vấn đề sớm

---

## METRICS CẦN THEO DÕI

1. **Batch Commit Rate per Primary**: Tỷ lệ batch được commit của mỗi primary
2. **Batch Retry Count Distribution**: Phân bố số lần retry của batches
3. **Batch Extraction Success Rate**: Tỷ lệ thành công của batch extraction
4. **Leader Selection Fairness**: Đảm bảo tất cả primaries đều được chọn làm leader đều đặn

