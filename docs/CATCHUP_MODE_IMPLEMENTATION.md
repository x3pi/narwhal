# TRIỂN KHAI CƠ CHẾ CATCH-UP MODE

## TỔNG QUAN

Đã triển khai đầy đủ cơ chế "Catch-up Mode" để node chậm có thể:
1. ✅ Phát hiện khi lag
2. ✅ **Ngưng tham gia đồng thuận** (mới thêm)
3. ✅ Fetch dữ liệu pull-based
4. ✅ Xác thực và thực thi
5. ✅ **Gia nhập lại tự động** (mới thêm)

---

## CÁC THAY ĐỔI ĐÃ TRIỂN KHAI

### 1. Thêm Catch-up Mode State vào Core

**File:** `primary/src/core.rs`

**Thêm các field:**
```rust
/// CATCH-UP MODE: Track if node is in catch-up mode
is_catchup_mode: bool,
/// CATCH-UP MODE: Last time we entered catch-up mode
catchup_mode_entered_at: Option<Instant>,
/// CATCH-UP MODE: Last detected lag
last_detected_lag: Round,
/// CATCH-UP MODE: Channel to notify proposer about catch-up mode
tx_proposer_catchup: Sender<bool>,
```

**Khởi tạo:**
```rust
is_catchup_mode: false,
catchup_mode_entered_at: None,
last_detected_lag: 0,
tx_proposer_catchup, // Passed from primary.rs
```

---

### 2. Cập nhật periodic_catchup_sync_check

**File:** `primary/src/core.rs:539-625`

**Chức năng:**
- **Enter catch-up mode:** Khi `lag >= 100 rounds`
- **Resume normal operation:** Khi `lag < 50 rounds`
- **Notify proposer:** Gửi thông báo qua channel khi thay đổi mode

**Code:**
```rust
const LAG_THRESHOLD: Round = 100; // Enter catch-up mode if lag >= 100 rounds
const RESUME_THRESHOLD: Round = 50; // Resume normal operation if lag < 50 rounds

if lag >= LAG_THRESHOLD {
    if !self.is_catchup_mode {
        // Enter catch-up mode
        self.is_catchup_mode = true;
        self.catchup_mode_entered_at = Some(Instant::now());
        warn!("[CATCH-UP MODE] Primary {} ENTERING catch-up mode...", ...);
        
        // Notify proposer to pause
        self.tx_proposer_catchup.send(true).await?;
    }
} else if lag < RESUME_THRESHOLD {
    if self.is_catchup_mode {
        // Resume normal operation
        self.is_catchup_mode = false;
        info!("[CATCH-UP MODE] Primary {} RESUMING normal operation...", ...);
        
        // Notify proposer to resume
        self.tx_proposer_catchup.send(false).await?;
    }
}
```

---

### 3. Thêm Channel từ Core đến Proposer

**File:** `primary/src/primary.rs`

**Tạo channel:**
```rust
// CATCH-UP MODE: Create channel to notify proposer about catch-up mode
let (tx_proposer_catchup, rx_proposer_catchup) = channel(10);

// Pass sender to Core
Core::spawn(..., tx_proposer_catchup, ...);

// Pass receiver to Proposer
Proposer::spawn(..., rx_proposer_catchup, ...);
```

---

### 4. Cập nhật Proposer để Pause khi Catch-up Mode

**File:** `primary/src/proposer.rs`

**Thêm field:**
```rust
/// CATCH-UP MODE: Receive catch-up mode notifications from Core
rx_catchup_mode: Receiver<bool>,
/// CATCH-UP MODE: Track if node is in catch-up mode
is_catchup_mode: bool,
```

**Cập nhật run() loop:**
```rust
loop {
    // CATCH-UP MODE: Check for catch-up mode notifications
    if let Ok(is_catchup) = self.rx_catchup_mode.try_recv() {
        self.is_catchup_mode = is_catchup;
        if is_catchup {
            info!("[CATCH-UP] Proposer entering catch-up mode - pausing header creation");
        } else {
            info!("[CATCH-UP] Proposer resuming normal operation");
        }
    }

    // CATCH-UP MODE: Skip creating headers if in catch-up mode
    if self.is_catchup_mode {
        debug!("[CATCH-UP] Proposer paused - node is catching up, skipping header creation");
        // Still process other messages (parents, headers, batches) but don't create new headers
    }

    // ... rest of loop ...
    
    // CATCH-UP MODE: Don't create headers when in catch-up mode
    let should_create_header = ready_to_make_header && !self.is_catchup_mode;

    if should_create_header {
        // Make header
    }
}
```

**Kết quả:**
- ✅ Proposer **pause** tạo headers khi catch-up mode
- ✅ Vẫn xử lý parents, headers, batches (để sync)
- ✅ **Resume** tự động khi đã bắt kịp

---

### 5. Cập nhật Core để Skip Consensus khi Catch-up Mode

**File:** `primary/src/core.rs:394-410`

**Thêm logic:**
```rust
// CATCH-UP MODE: Skip consensus when in catch-up mode and lag is too large
const LAG_SKIP_CONSENSUS_THRESHOLD: Round = 200; // Skip consensus if lag > 200 rounds
if self.is_catchup_mode && self.last_detected_lag > LAG_SKIP_CONSENSUS_THRESHOLD {
    debug!(
        "[CATCH-UP MODE] Skipping consensus for certificate {} - node is catching up (lag: {} rounds). Certificate still processed for state.",
        certificate.digest(),
        self.last_detected_lag
    );
    // Still return Ok() - certificate is processed for state, just not sent to consensus
    return Ok(());
}
```

**Kết quả:**
- ✅ Skip consensus khi lag > 200 rounds
- ✅ Vẫn process certificate cho state (để sync)
- ✅ Không vote khi không có đủ context

---

## CÁC THAM SỐ

### Thresholds

| Tham số | Giá trị | Mô tả |
|---------|---------|-------|
| `LAG_THRESHOLD` | 100 rounds | Enter catch-up mode khi lag >= 100 rounds |
| `RESUME_THRESHOLD` | 50 rounds | Resume normal operation khi lag < 50 rounds |
| `LAG_SKIP_CONSENSUS_THRESHOLD` | 200 rounds | Skip consensus khi lag > 200 rounds |

### Logic

```
lag >= 100 rounds  → Enter catch-up mode
  ↓
  - Proposer pause (không tạo headers)
  - Skip consensus nếu lag > 200 rounds
  - Vẫn sync dữ liệu
  ↓
lag < 50 rounds    → Resume normal operation
  ↓
  - Proposer resume (tạo headers lại)
  - Gửi certificates đến consensus lại
```

---

## LUỒNG HOẠT ĐỘNG

### 1. Phát hiện lag

```
Periodic check (mỗi 2 giây)
  ↓
Calculate lag = current_round - our_round
  ↓
lag >= 100 rounds?
  ↓ YES
Enter catch-up mode
  - Set is_catchup_mode = true
  - Notify proposer (pause)
```

### 2. Catch-up mode

```
Proposer:
  - Nhận notification: is_catchup_mode = true
  - Pause tạo headers
  - Vẫn xử lý parents/headers/batches (để sync)

Core:
  - Skip consensus nếu lag > 200 rounds
  - Vẫn process certificates cho state
  - Vẫn sync dữ liệu
```

### 3. Resume

```
Periodic check
  ↓
lag < 50 rounds?
  ↓ YES
Resume normal operation
  - Set is_catchup_mode = false
  - Notify proposer (resume)
  - Proposer tạo headers lại
  - Core gửi certificates đến consensus lại
```

---

## LOG MESSAGES

### Enter Catch-up Mode
```
[CATCH-UP MODE] Primary {} ENTERING catch-up mode - lag: {} rounds...
[CATCH-UP] Proposer entering catch-up mode - pausing header creation...
```

### In Catch-up Mode
```
[CATCH-UP MODE] Primary {} still catching up - lag: {} rounds, duration: {:?}
[CATCH-UP] Proposer paused - node is catching up, skipping header creation
[CATCH-UP MODE] Skipping consensus for certificate {} - node is catching up...
```

### Resume
```
[CATCH-UP MODE] Primary {} RESUMING normal operation - caught up in {:?}...
[CATCH-UP] Proposer resuming normal operation - node has caught up
```

---

## KẾT QUẢ

### Trước khi triển khai:
- ❌ Proposer vẫn tạo headers khi lag
- ❌ Consensus vẫn nhận certificates khi lag
- ⚠️ Không có logic resume rõ ràng

### Sau khi triển khai:
- ✅ Proposer **pause** khi lag >= 100 rounds
- ✅ Consensus **skip** khi lag > 200 rounds
- ✅ **Resume tự động** khi lag < 50 rounds
- ✅ Node tập trung vào sync thay vì tạo headers không cần thiết
- ✅ Tiết kiệm tài nguyên khi đang catch-up

---

## TESTING

### Build thành công:
```bash
cargo build --release -p primary
# Finished `release` profile [optimized] target(s) in 4.06s
```

### Không có lỗi lint:
- ✅ No linter errors found

---

## FILES CHANGED

1. `primary/src/core.rs`
   - Thêm catch-up mode state
   - Cập nhật periodic_catchup_sync_check
   - Skip consensus khi catch-up mode

2. `primary/src/proposer.rs`
   - Thêm catch-up mode receiver
   - Pause tạo headers khi catch-up mode

3. `primary/src/primary.rs`
   - Tạo channel giữa Core và Proposer
   - Pass channel vào Core và Proposer

---

## NEXT STEPS

1. ✅ **Đã hoàn thành:** Triển khai catch-up mode
2. ⏭️ **Có thể cải thiện:**
   - Tune thresholds dựa trên thực tế
   - Thêm metrics để monitor catch-up mode
   - Thêm tests cho catch-up mode logic

---

## KẾT LUẬN

Đã triển khai đầy đủ cơ chế catch-up mode với:
- ✅ Phát hiện lag và enter catch-up mode
- ✅ Pause proposer khi lag
- ✅ Skip consensus khi lag quá nhiều
- ✅ Resume tự động khi đã bắt kịp
- ✅ Logging đầy đủ để debug

Hệ thống giờ đây có thể tự động pause khi lag và resume khi đã bắt kịp, giúp node chậm tập trung vào sync thay vì tạo headers không cần thiết.

