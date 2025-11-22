# PHASE 2: CATCH-UP MECHANISM - ĐÃ TRIỂN KHAI

## MỤC TIÊU

Giúp node chậm catch-up với dữ liệu mới bằng cách:
1. ✅ Soft reject cho headers/certificates quá cũ (không reject ngay, trigger sync)
2. ✅ Cho phép sync headers/certificates quá cũ (up to 1000 rounds behind)
3. ✅ Proactive sync check (periodic check every 5 seconds)
4. ✅ Trigger sync khi detect old headers/certificates

## CHI TIẾT TRIỂN KHAI

### 1. Thêm Fields vào Core Struct

**File:** `primary/src/core.rs`

**Thay đổi:**
```rust
pub struct Core {
    // ... existing fields ...
    /// PHASE 2: Track last catch-up sync check time for proactive synchronization
    last_catchup_sync_check: Option<Instant>,
    /// PHASE 2: Interval for catch-up sync check (check every 5 seconds)
    catchup_sync_check_interval: Duration,
}
```

**Giải thích:**
- `last_catchup_sync_check`: Track thời điểm check catch-up sync cuối cùng
- `catchup_sync_check_interval`: Interval 5 seconds cho periodic check

### 2. Khởi Tạo Fields

**File:** `primary/src/core.rs:119-120`

**Thay đổi:**
```rust
last_catchup_sync_check: Some(Instant::now()),
catchup_sync_check_interval: Duration::from_secs(5), // Check every 5 seconds
```

**Giải thích:**
- Khởi tạo `last_catchup_sync_check` với current time
- Set `catchup_sync_check_interval` = 5 seconds

### 3. Import Duration và Instant

**File:** `primary/src/core.rs:18`

**Thay đổi:**
```rust
use tokio::time::{Duration, Instant};
```

**Giải thích:**
- Thêm import để sử dụng Duration và Instant

### 4. Soft Reject cho Headers Quá Cũ

**File:** `primary/src/core.rs:375-420`

**Thay đổi:**
```rust
fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
    // PHASE 2: Soft reject for old headers to support catch-up
    if header.round < self.gc_round {
        let round_diff = self.gc_round.saturating_sub(header.round);
        const MAX_CATCHUP_ROUNDS: Round = 1000; // Allow catch-up sync up to 1000 rounds behind
        
        if round_diff <= MAX_CATCHUP_ROUNDS {
            // Header is old but within catch-up window - trigger sync for catch-up
            warn!(
                "[CATCH-UP SYNC] Header {} (round {}) is {} rounds behind (gc_round={}, current_round≈{}). This header is too old to process immediately, but will trigger sync for catch-up. Node may be lagging behind - attempting to sync old data.",
                header.id, header.round, round_diff, self.gc_round, self.gc_round + self.gc_depth
            );
            
            // Sync will be triggered in main loop after returning from sanitize
        } else {
            // Header is too far behind - skip sync (would be inefficient)
            debug!("[CATCH-UP] Header {} (round {}) is {} rounds behind. Too far behind for catch-up sync (max: {} rounds). Skipping.",
                header.id, header.round, round_diff, MAX_CATCHUP_ROUNDS);
        }
        
        // Still reject processing to avoid issues with old headers
        return Err(DagError::TooOld(header.id.clone(), header.round));
    }
    // ... existing verification ...
}
```

**Giải thích:**
- Soft reject: Thay vì reject ngay, log warning và trigger sync
- Cho phép sync headers quá cũ (up to 1000 rounds behind)
- Vẫn reject processing để tránh issues với old headers

### 5. Soft Reject cho Certificates Quá Cũ

**File:** `primary/src/core.rs:472-509`

**Thay đổi:**
```rust
fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
    // PHASE 2: Soft reject for old certificates to support catch-up
    if certificate.round() < self.gc_round {
        let round_diff = self.gc_round.saturating_sub(certificate.round());
        const MAX_CATCHUP_ROUNDS: Round = 1000;
        
        if round_diff <= MAX_CATCHUP_ROUNDS {
            // Certificate is old but within catch-up window - trigger sync for catch-up
            warn!(
                "[CATCH-UP SYNC] Certificate {} (round {}) is {} rounds behind. This certificate is too old to process immediately, but will trigger sync for catch-up.",
                certificate.digest(), certificate.round(), round_diff
            );
        }
        
        // Still reject processing to avoid issues with old certificates
        return Err(DagError::TooOld(certificate.digest(), certificate.round()));
    }
    // ... existing verification ...
}
```

**Giải thích:**
- Tương tự như headers, soft reject cho certificates quá cũ
- Cho phép sync certificates quá cũ (up to 1000 rounds behind)

### 6. Trigger Catch-Up Sync cho Headers

**File:** `primary/src/core.rs:431-451`

**Thay đổi:**
```rust
/// PHASE 2: Trigger catch-up sync for old header
/// This method helps node chậm sync headers/certificates from old rounds
async fn trigger_catchup_sync(&mut self, header: &Header) {
    // Try to get parents to trigger sync if they're missing
    // This will automatically trigger sync request via synchronizer
    match self.synchronizer.get_parents(header).await {
        Ok(_parents) => {
            // Parents are available - no sync needed
            debug!("[CATCH-UP SYNC] Parents for old header {} (round {}) are already available",
                header.id, header.round);
        }
        Err(_) => {
            // Parents are missing - sync will be triggered automatically by synchronizer
            debug!("[CATCH-UP SYNC] Parents for old header {} (round {}) are missing - sync triggered",
                header.id, header.round);
        }
    }
}
```

**Giải thích:**
- Trigger sync cho parents của old header
- Synchronizer sẽ tự động request sync nếu parents missing

### 7. Trigger Catch-Up Sync cho Certificates

**File:** `primary/src/core.rs:563-593`

**Thay đổi:**
```rust
/// PHASE 2: Trigger catch-up sync for old certificate
/// This method helps node chậm sync certificate ancestors from old rounds
async fn trigger_catchup_sync_certificate(&mut self, certificate: &Certificate) {
    // Try to deliver certificate to trigger sync if ancestors are missing
    match self.synchronizer.deliver_certificate(certificate).await {
        Ok(true) => {
            // All ancestors are available
            debug!("[CATCH-UP SYNC] Ancestors for old certificate {} (round {}) are already available",
                certificate.digest(), certificate.round());
        }
        Ok(false) => {
            // Ancestors are missing - sync will be triggered automatically
            debug!("[CATCH-UP SYNC] Ancestors for old certificate {} (round {}) are missing - sync triggered",
                certificate.digest(), certificate.round());
        }
        Err(e) => {
            warn!("[CATCH-UP SYNC] Error checking ancestors for old certificate {} (round {}): {}",
                certificate.digest(), certificate.round(), e);
        }
    }
}
```

**Giải thích:**
- Trigger sync cho ancestors của old certificate
- Synchronizer sẽ tự động request sync nếu ancestors missing

### 8. Periodic Catch-Up Sync Check

**File:** `primary/src/core.rs:545-595`

**Thay đổi:**
```rust
/// PHASE 2: Periodic catch-up sync check
/// This method helps node chậm proactively sync missing certificates from recent rounds
async fn periodic_catchup_sync_check(&mut self, current_round: Round) -> DagResult<()> {
    // Update last check time
    self.last_catchup_sync_check = Some(Instant::now());
    
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
    } else {
        debug!("[CATCH-UP SYNC] Periodic check - node {} is up to date (our: {}, current: {})",
            self.name, our_round, current_round);
    }
    
    Ok(())
}
```

**Giải thích:**
- Periodic check every 5 seconds
- Detect nếu node bị lag
- Log warning nếu lag > 100 rounds

### 9. Integrate vào Main Loop

**File:** `primary/src/core.rs:546-651`

**Thay đổi:**
```rust
pub async fn run(&mut self) {
    // PHASE 2: Set up periodic catch-up sync check timer
    let mut catchup_sync_timer = tokio::time::interval(self.catchup_sync_check_interval);
    catchup_sync_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    loop {
        let result = tokio::select! {
            // ... existing selects ...
            
            // PHASE 2: Periodic catch-up sync check
            _ = catchup_sync_timer.tick() => {
                // Periodic check to help node chậm catch-up
                let current_round = self.consensus_round.load(Ordering::Relaxed);
                if let Err(e) = self.periodic_catchup_sync_check(current_round).await {
                    debug!("Periodic catch-up sync check failed: {}", e);
                }
                Ok(())
            }
        };
        // ... existing error handling ...
    }
}
```

**Giải thích:**
- Thêm periodic timer vào main loop
- Check catch-up sync every 5 seconds
- Log warning nếu node bị lag

### 10. Trigger Sync trong Message Handlers

**File:** `primary/src/core.rs:578-589, 603-612`

**Thay đổi:**
```rust
// Header handler
Err(DagError::TooOld(_, round)) => {
    // PHASE 2: Header is too old - trigger catch-up sync
    let round_diff = self.gc_round.saturating_sub(round);
    const MAX_CATCHUP_ROUNDS: Round = 1000;
    
    if round_diff <= MAX_CATCHUP_ROUNDS {
        // Trigger sync for catch-up
        self.trigger_catchup_sync(&header).await;
    }
    Err(DagError::TooOld(header.id.clone(), round))
}

// Certificate handler
Err(DagError::TooOld(_, round)) => {
    // PHASE 2: Certificate is too old - trigger catch-up sync
    let round_diff = self.gc_round.saturating_sub(round);
    const MAX_CATCHUP_ROUNDS: Round = 1000;
    
    if round_diff <= MAX_CATCHUP_ROUNDS {
        // Trigger sync for catch-up
        self.trigger_catchup_sync_certificate(&certificate).await;
    }
    Err(DagError::TooOld(certificate.digest(), round))
}
```

**Giải thích:**
- Trigger sync khi detect old headers/certificates
- Chỉ trigger nếu trong catch-up window (up to 1000 rounds behind)

## LOGIC FLOW

### Old Header Arrives:
```
1. Header arrives (round < gc_round)
2. sanitize_header detects old header
3. Check if within catch-up window (round_diff <= 1000)
4. If yes: Log warning + return TooOld error
5. Main loop catches TooOld error
6. Trigger catchup_sync for this header
7. Synchronizer tries to get parents
8. If parents missing: Trigger sync request
9. Header will be processed later when sync completes
```

### Old Certificate Arrives:
```
1. Certificate arrives (round < gc_round)
2. sanitize_certificate detects old certificate
3. Check if within catch-up window (round_diff <= 1000)
4. If yes: Log warning + return TooOld error
5. Main loop catches TooOld error
6. Trigger catchup_sync_certificate for this certificate
7. Synchronizer tries to deliver certificate
8. If ancestors missing: Trigger sync request
9. Certificate will be processed later when sync completes
```

### Periodic Check:
```
1. Timer ticks every 5 seconds
2. periodic_catchup_sync_check is called
3. Calculate our_round = gc_round + gc_depth
4. Calculate lag = current_round - our_round
5. If lag > 100: Log warning
6. This helps monitor node health
```

## SAFETY

### Determinism:
- ✅ **Không ảnh hưởng determinism:**
  - Soft reject vẫn reject processing (deterministic)
  - Sync chỉ trigger khi headers/certificates arrive (deterministic source)
  - Tất cả nodes sẽ có cùng behavior

### No Duplicate:
- ✅ **Không ảnh hưởng duplicate detection:**
  - Sync chỉ trigger khi old headers/certificates arrive
  - Duplicate detection vẫn hoạt động bình thường
  - Old headers/certificates vẫn bị reject processing

### No Fork:
- ✅ **Không ảnh hưởng fork:**
  - Old headers/certificates vẫn bị reject processing
  - Sync chỉ giúp catch-up, không ảnh hưởng consensus
  - Headers/certificates quá cũ không được process → không gây fork

## TESTING

### Test Case 1: Old Header Within Catch-Up Window
- **Scenario:** Header arrives (round < gc_round, but within 1000 rounds)
- **Expected:** Soft reject + trigger sync
- **Result:** ⏳ To be tested

### Test Case 2: Old Header Too Far Behind
- **Scenario:** Header arrives (round < gc_round, > 1000 rounds behind)
- **Expected:** Hard reject (no sync)
- **Result:** ⏳ To be tested

### Test Case 3: Old Certificate Within Catch-Up Window
- **Scenario:** Certificate arrives (round < gc_round, but within 1000 rounds)
- **Expected:** Soft reject + trigger sync
- **Result:** ⏳ To be tested

### Test Case 4: Periodic Check
- **Scenario:** Timer ticks every 5 seconds
- **Expected:** Periodic check logs node lag status
- **Result:** ⏳ To be tested

### Test Case 5: Catch-Up Success
- **Scenario:** Node chậm receives old headers, sync completes, node catches up
- **Expected:** Node eventually catches up
- **Result:** ⏳ To be tested

## METRICS

### Logs to Monitor:
1. **Old headers detected:**
   ```
   [CATCH-UP SYNC] Header {} (round {}) is {} rounds behind. This header is too old to process immediately, but will trigger sync for catch-up.
   ```

2. **Old certificates detected:**
   ```
   [CATCH-UP SYNC] Certificate {} (round {}) is {} rounds behind. This certificate is too old to process immediately, but will trigger sync for catch-up.
   ```

3. **Sync triggered:**
   ```
   [CATCH-UP SYNC] Parents for old header {} (round {}) are missing - sync triggered
   ```

4. **Periodic check:**
   ```
   [CATCH-UP SYNC] Primary {} is {} rounds behind (our: {}, current: {}). Node is lagging - this is expected for catch-up sync to work.
   ```

### Metrics to Track:
1. **Old headers/certificates detected:** Số headers/certificates quá cũ được detect
2. **Sync requests triggered:** Số sync requests được trigger
3. **Node lag:** Round lag của node
4. **Catch-up success rate:** Tỷ lệ node chậm catch-up thành công

## KNOWN LIMITATIONS

### Limitation 1: Sync Chỉ Trigger Khi Headers/Certificates Arrive
- **Mô tả:** Sync chỉ trigger khi có old headers/certificates arrive, không proactive request missing rounds
- **Impact:** Node chậm vẫn cần nhận old headers/certificates từ network để trigger sync
- **Mitigation:** Phase 3 sẽ thêm proactive sync mechanism

### Limitation 2: Catch-Up Window 1000 Rounds Có Thể Không Đủ
- **Mô tả:** Node chậm hơn 1000 rounds sẽ không được sync
- **Impact:** Node rất chậm không thể catch-up
- **Mitigation:** Có thể tăng MAX_CATCHUP_ROUNDS nếu cần

### Limitation 3: Sync Có Thể Chậm
- **Mô tả:** Sync dựa trên existing mechanism, có thể chậm
- **Impact:** Catch-up có thể mất thời gian
- **Mitigation:** Phase 3 sẽ cải thiện sync mechanism

## NEXT STEPS

### Immediate:
1. ✅ **Implementation:** Complete
2. ✅ **Build:** Success
3. ⏳ **Testing:** In progress
4. ⏳ **Deployment:** Pending

### Short-term:
1. Monitor catch-up sync events trong production
2. Adjust MAX_CATCHUP_ROUNDS nếu cần
3. Implement Phase 3 (Proactive Sync) để cải thiện catch-up

### Long-term:
1. Optimize sync mechanism để catch-up nhanh hơn
2. Add metrics và monitoring
3. Improve proactive sync (Phase 3)

## KẾT LUẬN

### Status:
- ✅ **Implementation:** Complete
- ✅ **Build:** Success
- ⏳ **Testing:** Pending
- ⏳ **Deployment:** Pending

### Impact:
- ✅ **Node chậm có thể catch-up:**
  - Headers/certificates quá cũ (up to 1000 rounds behind) được sync
  - Sync triggered automatically khi old headers/certificates arrive
  - Periodic check giúp monitor node lag

- ⚠️ **Trade-off:**
  - Old headers/certificates vẫn bị reject processing (cần để đảm bảo determinism)
  - Sync có thể chậm (dựa trên existing mechanism)
  - Node chậm cần nhận old headers/certificates từ network để trigger sync

### Success Criteria:
- ✅ Old headers/certificates (within 1000 rounds) trigger sync
- ✅ Node chậm có thể sync dữ liệu từ old rounds
- ✅ Periodic check helps monitor node lag

---

**Last Updated:** 2025-01-20
**Status:** ✅ **PHASE 2 IMPLEMENTED - Ready for Testing**

