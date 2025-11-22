# PHÂN TÍCH CƠ CHẾ CATCH-UP HIỆN TẠI

## YÊU CẦU CỦA CƠ CHẾ CATCH-UP

Theo mô tả, cơ chế catch-up cần có 5 bước:

1. ✅ **Phát hiện node chậm** - Node nhận ra mình đang ở block thấp hơn
2. ❌ **Ngưng tham gia đồng thuận** - Node tạm thời ngừng tạo block mới và vote
3. ✅ **Fetch dữ liệu (Pull-based)** - Chủ động gửi yêu cầu đến peers để lấy dữ liệu thiếu
4. ✅ **Xác thực và Thực thi** - Xác thực chữ ký, thực thi giao dịch, cập nhật state
5. ⚠️ **Gia nhập lại** - Khi đã bắt kịp, quay lại tham gia vote

---

## PHÂN TÍCH CODE HIỆN TẠI

### 1. ✅ PHÁT HIỆN NODE CHẬM - **ĐÃ CÓ**

**Vị trí:** `primary/src/core.rs:527-565`

```rust
async fn periodic_catchup_sync_check(&mut self, current_round: Round) -> DagResult<()> {
    // Calculate approximate our round
    let our_round = self.gc_round + self.gc_depth;
    
    // Check if we're lagging behind
    const LAG_THRESHOLD: Round = 100; // Alert if lag > 100 rounds
    if our_round < current_round.saturating_sub(LAG_THRESHOLD) {
        let lag = current_round.saturating_sub(our_round);
        warn!(
            "[CATCH-UP SYNC] Primary {} is {} rounds behind (our: {}, current: {}, gc_round: {}). Node is lagging - this is expected for catch-up sync to work.",
            self.name, lag, our_round, current_round, self.gc_round
        );
        // ...
    }
}
```

**Đánh giá:**
- ✅ Check mỗi **2 giây** (`catchup_sync_check_interval`)
- ✅ Phát hiện lag khi `our_round < current_round - 100`
- ✅ Log warning khi phát hiện lag
- ⚠️ **THIẾU:** Chỉ log, chưa có action cụ thể để ngưng đồng thuận

**Kết luận:** Đã phát hiện được node chậm, nhưng chưa có hành động ngưng đồng thuận.

---

### 2. ❌ NGƯNG THAM GIA ĐỒNG THUẬN - **CHƯA CÓ**

#### 2.1 Proposer vẫn tạo headers

**Vị trí:** `primary/src/proposer.rs:1265-1490`

**Phân tích:**
- Proposer **KHÔNG kiểm tra** xem node có đang lag không
- Proposer vẫn tiếp tục tạo headers ngay cả khi node lag
- Không có logic để **pause** proposer khi lag

**Code hiện tại:**
```rust
let ready_to_make_header =
    (timer_expired || enough_digests) && (enough_parents || force_advance);

if ready_to_make_header {
    // Make a new header - KHÔNG KIỂM TRA LAG
    if self.make_header().await {
        // ...
    }
}
```

**Vấn đề:**
- Node lag vẫn tạo headers cho rounds mới
- Headers này có thể không có ý nghĩa vì node chưa có đủ dữ liệu từ rounds trước
- Lãng phí tài nguyên khi node đang cần sync

#### 2.2 Consensus vẫn nhận certificates

**Vị trí:** `primary/src/core.rs:279-393`

**Phân tích:**
- Core vẫn gửi certificates đến consensus ngay cả khi node lag
- Không có check để skip consensus khi lag quá nhiều

**Code hiện tại:**
```rust
async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
    // ... process certificate ...
    
    // Send it to the consensus layer - KHÔNG KIỂM TRA LAG
    if let Err(e) = self.tx_consensus.send(certificate).await {
        // ...
    }
}
```

**Vấn đề:**
- Node lag vẫn vote cho certificates
- Votes này có thể không có ý nghĩa vì node chưa có đủ context
- Tuy nhiên, trong Narwhal, votes vẫn có thể hữu ích nếu node có đủ dữ liệu

**Kết luận:** **CHƯA CÓ** cơ chế ngưng tham gia đồng thuận khi lag.

---

### 3. ✅ FETCH DỮ LIỆU (PULL-BASED) - **ĐÃ CÓ**

**Vị trí:** `primary/src/header_waiter.rs` và `primary/src/synchronizer.rs`

#### 3.1 Sync Batches

**Code:**
```rust
// Gửi đến TẤT CẢ workers ngay lập tức
self.network.send(author_address, Bytes::from(bytes.clone())).await;

// Gửi đến TẤT CẢ workers của các node khác
let other_workers: Vec<_> = self.committee.others_primaries(&self.name)
    .iter()
    .filter_map(|(other_author, _)| {
        self.committee.worker(other_author, &worker_id).ok()
            .map(|addr| addr.primary_to_worker)
    })
    .collect(); // KHÔNG GIỚI HẠN

for worker_addr in other_workers {
    self.network.send(worker_addr, Bytes::from(bytes_other)).await;
}
```

**Đánh giá:**
- ✅ Gửi đến **TẤT CẢ nodes** ngay lập tức
- ✅ Pull-based (chủ động request)
- ✅ Retry nhanh (30ms delay)

#### 3.2 Sync Parents

**Code:**
```rust
// Gửi đến TẤT CẢ nodes ngay lập tức
self.network.send(author_address, Bytes::from(bytes.clone())).await;

// Gửi đến TẤT CẢ nodes khác
let other_addresses: Vec<_> = self.committee.others_primaries(&self.name)
    .iter()
    .filter(|(pk, _)| *pk != author)
    .map(|(_, x)| x.primary_to_primary)
    .collect(); // KHÔNG GIỚI HẠN

for addr in other_addresses {
    self.network.send(addr, Bytes::from(bytes.clone())).await;
}
```

**Đánh giá:**
- ✅ Gửi đến **TẤT CẢ nodes** ngay lập tức
- ✅ Pull-based (chủ động request)
- ✅ Retry nhanh (30ms delay)

#### 3.3 Catch-up Sync Trigger

**Code:**
```rust
fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
    if header.round < self.gc_round {
        let round_diff = self.gc_round.saturating_sub(header.round);
        const MAX_CATCHUP_ROUNDS: Round = 50000; // Window lớn
        
        if round_diff <= MAX_CATCHUP_ROUNDS {
            // Trigger sync nhưng vẫn reject
            warn!("[CATCH-UP SYNC] Header {} is {} rounds behind...", ...);
            // Sync sẽ được trigger trong main loop
        }
        
        return Err(DagError::TooOld(header.id.clone(), header.round));
    }
}
```

**Đánh giá:**
- ✅ Phát hiện headers/certificates cũ
- ✅ Trigger sync cho parents/batches
- ✅ Window lớn (50,000 rounds)

**Kết luận:** **ĐÃ CÓ** cơ chế fetch dữ liệu pull-based tốt.

---

### 4. ✅ XÁC THỰC VÀ THỰC THI - **ĐÃ CÓ**

#### 4.1 Xác thực (Verification)

**Vị trí:** `primary/src/core.rs:396-441`

**Code:**
```rust
fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
    // Verify the header's signature.
    header.verify(&self.committee)?;
    // ...
}

fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
    // Certificate được verify trong process_certificate
    // ...
}
```

**Đánh giá:**
- ✅ Verify signature trước khi process
- ✅ Verify quorum của parents
- ✅ Verify parents từ round trước đó

#### 4.2 Thực thi (Execution)

**Vị trí:** `primary/src/core.rs:164-276`

**Code:**
```rust
async fn process_header(&mut self, header: &Header) -> DagResult<()> {
    // Ensure we have the parents
    let parents = self.synchronizer.get_parents(header).await?;
    
    // Check the parent certificates - ensure quorum
    let mut stake = 0;
    for x in parents {
        ensure!(x.round() + 1 == header.round, ...);
        stake += self.committee.stake(&x.origin());
    }
    ensure!(stake >= self.committee.quorum_threshold(), ...);
    
    // Ensure we have the payload
    if self.synchronizer.missing_payload(header).await? {
        return Ok(()); // Wait for payload
    }
    
    // Store the header
    self.store.write(header.id.to_vec(), bytes).await;
    
    // Send to proposer and process
    // ...
}
```

**Đánh giá:**
- ✅ Process headers/certificates theo thứ tự
- ✅ Store dữ liệu sau khi verify
- ✅ Gửi đến proposer và consensus để thực thi

**Kết luận:** **ĐÃ CÓ** cơ chế xác thực và thực thi đầy đủ.

---

### 5. ⚠️ GIA NHẬP LẠI - **CHƯA RÕ RÀNG**

**Vị trí:** `primary/src/core.rs:527-565`

**Phân tích:**
- Có check periodic để phát hiện lag
- Có log khi node up to date
- **THIẾU:** Không có logic rõ ràng để:
  - Detect khi node đã bắt kịp (lag < threshold)
  - Resume proposer/consensus khi đã bắt kịp
  - Thông báo node đã quay lại tham gia

**Code hiện tại:**
```rust
async fn periodic_catchup_sync_check(&mut self, current_round: Round) -> DagResult<()> {
    let our_round = self.gc_round + self.gc_depth;
    const LAG_THRESHOLD: Round = 100;
    
    if our_round < current_round.saturating_sub(LAG_THRESHOLD) {
        // Node lag - log warning
        warn!("[CATCH-UP SYNC] Primary {} is {} rounds behind...", ...);
    } else {
        // Node is up to date - chỉ log debug
        debug!("[CATCH-UP SYNC] Periodic check - node {} is up to date...", ...);
    }
}
```

**Vấn đề:**
- Không có state để track "đang trong catch-up mode"
- Không có logic để resume khi đã bắt kịp
- Proposer/consensus không biết khi nào nên resume

**Kết luận:** **CHƯA RÕ RÀNG** - Có phát hiện khi up to date nhưng không có logic resume.

---

## TỔNG KẾT

| Bước | Trạng thái | Mô tả |
|------|------------|-------|
| 1. Phát hiện node chậm | ✅ **ĐÃ CÓ** | Check mỗi 2 giây, phát hiện lag > 100 rounds |
| 2. Ngưng tham gia đồng thuận | ❌ **CHƯA CÓ** | Proposer và consensus vẫn hoạt động khi lag |
| 3. Fetch dữ liệu (Pull-based) | ✅ **ĐÃ CÓ** | Gửi đến tất cả nodes, retry nhanh |
| 4. Xác thực và Thực thi | ✅ **ĐÃ CÓ** | Verify signature, quorum, process đầy đủ |
| 5. Gia nhập lại | ⚠️ **CHƯA RÕ RÀNG** | Có phát hiện up to date nhưng không có logic resume |

---

## KHUYẾN NGHỊ TRIỂN KHAI

### 1. Thêm cơ chế "Catch-up Mode"

**Thêm state vào Core:**
```rust
pub struct Core {
    // ... existing fields ...
    
    /// Track if node is in catch-up mode
    is_catchup_mode: bool,
    /// Last time we entered catch-up mode
    catchup_mode_entered_at: Option<Instant>,
    /// Last detected lag
    last_detected_lag: Round,
}
```

### 2. Ngưng Proposer khi lag

**Trong Proposer:**
```rust
// Check if we should pause due to lag
if self.is_catchup_mode {
    // Skip creating headers - focus on catching up
    debug!("[CATCH-UP] Proposer paused - node is catching up (lag: {} rounds)", lag);
    continue;
}
```

### 3. Ngưng Consensus khi lag quá nhiều

**Trong Core:**
```rust
async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
    // Check if we should skip consensus due to lag
    if self.is_catchup_mode && self.last_detected_lag > LAG_SKIP_CONSENSUS_THRESHOLD {
        debug!("[CATCH-UP] Skipping consensus for certificate {} - node is catching up", certificate.digest());
        // Still process certificate for state, but don't send to consensus
        // ...
        return Ok(()); // Skip consensus
    }
    
    // Normal processing
    // ...
}
```

### 4. Resume khi đã bắt kịp

**Trong periodic_catchup_sync_check:**
```rust
async fn periodic_catchup_sync_check(&mut self, current_round: Round) -> DagResult<()> {
    let our_round = self.gc_round + self.gc_depth;
    let lag = current_round.saturating_sub(our_round);
    const LAG_THRESHOLD: Round = 100;
    const RESUME_THRESHOLD: Round = 50; // Resume khi lag < 50 rounds
    
    if lag >= LAG_THRESHOLD {
        // Enter catch-up mode
        if !self.is_catchup_mode {
            self.is_catchup_mode = true;
            self.catchup_mode_entered_at = Some(Instant::now());
            warn!("[CATCH-UP] Entering catch-up mode - lag: {} rounds", lag);
        }
        self.last_detected_lag = lag;
    } else if lag < RESUME_THRESHOLD {
        // Resume normal operation
        if self.is_catchup_mode {
            self.is_catchup_mode = false;
            let catchup_duration = self.catchup_mode_entered_at
                .map(|t| t.elapsed())
                .unwrap_or_default();
            info!("[CATCH-UP] Resuming normal operation - caught up in {:?} (lag: {} rounds)", 
                catchup_duration, lag);
        }
    }
    
    Ok(())
}
```

### 5. Thông báo Proposer về catch-up mode

**Thêm channel từ Core đến Proposer:**
```rust
// In Core
if self.is_catchup_mode {
    self.tx_proposer_catchup.send(true).await?;
} else {
    self.tx_proposer_catchup.send(false).await?;
}

// In Proposer
if let Ok(is_catchup) = self.rx_catchup_mode.try_recv() {
    self.is_catchup_mode = is_catchup;
}
```

---

## KẾT LUẬN

**Code hiện tại đã có:**
- ✅ Phát hiện node chậm
- ✅ Fetch dữ liệu pull-based tốt
- ✅ Xác thực và thực thi đầy đủ

**Code hiện tại THIẾU:**
- ❌ Ngưng tham gia đồng thuận khi lag
- ⚠️ Logic rõ ràng để resume khi đã bắt kịp

**Khuyến nghị:**
- Triển khai cơ chế "Catch-up Mode" với state tracking
- Ngưng proposer khi lag quá nhiều
- Ngưng consensus khi lag quá nhiều (tùy chọn)
- Resume tự động khi đã bắt kịp

