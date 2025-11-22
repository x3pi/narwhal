# KẾ HOẠCH: CẢI THIỆN NETWORK SYNCHRONIZATION

## MỤC TIÊU

Cải thiện network synchronization để:
1. ✅ Ngăn chặn primary bị tách khỏi network (lagging behind)
2. ✅ Đảm bảo proposer luôn nhận được parent certificates
3. ✅ Xử lý tốt hơn headers/certificates quá cũ
4. ✅ Recover nhanh khi primary bị stuck
5. ✅ Proactive synchronization thay vì reactive

## PHÂN TÍCH VẤN ĐỀ HIỆN TẠI

### Vấn Đề 1: Headers/Certificates Quá Cũ Bị Reject

**Code hiện tại:**
```rust
// primary/src/core.rs:375-379
fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
    ensure!(
        self.gc_round <= header.round,
        DagError::TooOld(header.id.clone(), header.round)
    );
    // ...
}
```

**Vấn đề:**
- Headers/certificates quá cũ (round < gc_round) bị reject ngay lập tức
- Primary bị lag sẽ không nhận được headers/certificates cũ nhưng cần thiết
- Không có cơ chế sync lại headers/certificates đã bị GC

**Impact:**
- Primary bị lag không thể catch up
- Headers của primary lag không được các primary khác nhận
- Hệ thống bị tách thành nhiều partitions

### Vấn Đề 2: Proposer Không Nhận Parent Certificates

**Code hiện tại:**
```rust
// primary/src/proposer.rs:1143-1157
let enough_parents = !self.last_parents.is_empty();
let enough_digests = self.pending_payload_size >= self.header_size;
let timer_expired = header_timer.is_elapsed();

if (timer_expired || enough_digests) && enough_parents {
    // Make a new header
}
```

**Vấn đề:**
- Nếu proposer không nhận được parent certificates, `last_parents` empty
- Proposer không thể tạo headers mới
- Round không advance → proposer bị stuck

**Impact:**
- Primary bị stuck ở một round cố định
- Batches bị stuck trong queue
- Hệ thống không thể tiếp tục

### Vấn Đề 3: Synchronizer Chỉ Reactive

**Code hiện tại:**
```rust
// primary/src/synchronizer.rs:85-114
pub async fn get_parents(&mut self, header: &Header) -> DagResult<Vec<Certificate>> {
    // ...
    if missing.is_empty() {
        return Ok(parents);
    }
    // Sync missing parents
    self.tx_header_waiter.send(WaiterMessage::SyncParents(missing, header.clone())).await;
    Ok(Vec::new())
}
```

**Vấn đề:**
- Synchronizer chỉ sync khi có header/certificate cần process
- Không proactive sync để đảm bảo data sẵn sàng
- Không có periodic sync để catch up

**Impact:**
- Primary bị lag không thể catch up
- Headers/certificates bị missing không được sync kịp thời

### Vấn Đề 4: Network Không Có Health Check

**Vấn đề:**
- Không có mechanism để detect primary bị lag
- Không có mechanism để recover khi primary stuck
- Không có metrics về network health

**Impact:**
- Không thể detect vấn đề sớm
- Không thể recover tự động

## KẾ HOẠCH TRIỂN KHAI

### Phase 1: Force Advance Round Khi Stuck (Priority: CRITICAL)

**Mục tiêu:** Ngăn chặn proposer bị stuck khi không nhận được parent certificates

**Implementation:**

#### 1.1. Thêm Timeout cho Parent Certificates

```rust
// primary/src/proposer.rs
pub struct Proposer {
    // ... existing fields ...
    last_parent_received_at: Option<Instant>, // Track khi nhận parent certificates cuối cùng
    max_parent_wait: Duration, // Max time chờ parent certificates
}

impl Proposer {
    pub fn spawn(...) {
        // ...
        Self {
            // ...
            last_parent_received_at: None,
            max_parent_wait: Duration::from_secs(10), // 10 seconds timeout
        }
    }

    pub async fn run(&mut self) {
        // ...
        loop {
            // ...
            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    // Update last_parent_received_at
                    self.last_parent_received_at = Some(Instant::now());
                    // ... existing logic ...
                }
                // ... other selects ...
            }

            // Check if we're stuck (no parent certificates received for too long)
            if let Some(last_received) = self.last_parent_received_at {
                if last_received.elapsed() > self.max_parent_wait {
                    // Force advance round with empty parents
                    warn!(
                        "[FORCE ADVANCE] Primary {} force advancing round {} -> {} due to no parent certificates received for {} seconds",
                        self.name,
                        self.round,
                        self.round + 1,
                        last_received.elapsed().as_secs()
                    );
                    self.round += 1;
                    self.last_parents = Vec::new(); // Clear old parents
                    self.last_parent_received_at = Some(Instant::now()); // Reset timer
                }
            }
        }
    }
}
```

**Files to modify:**
- `primary/src/proposer.rs`

**Testing:**
- Test với proposer không nhận parent certificates
- Verify round được force advance sau timeout
- Verify headers vẫn được tạo với empty parents

#### 1.2. Thêm Fallback: Tạo Header Với Empty Parents

```rust
// primary/src/proposer.rs:1143-1157
// IMPROVED: Allow creating header with empty parents if stuck too long
let enough_parents = !self.last_parents.is_empty();
let force_advance = self.last_parent_received_at
    .map(|t| t.elapsed() > self.max_parent_wait)
    .unwrap_or(false);

if (timer_expired || enough_digests) && (enough_parents || force_advance) {
    // Make a new header (even with empty parents if force_advance)
    if self.make_header().await {
        // ...
    }
}
```

**Files to modify:**
- `primary/src/proposer.rs`

### Phase 2: Cải Thiện Xử Lý Headers/Certificates Quá Cũ (Priority: HIGH)

**Mục tiêu:** Cho phép sync headers/certificates quá cũ khi cần thiết

**Implementation:**

#### 2.1. Soft Reject cho Headers/Certificates Quá Cũ

```rust
// primary/src/core.rs:375-387
fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
    // Check if header is too old
    if header.round < self.gc_round {
        // IMPROVED: Soft reject - allow sync but don't process immediately
        warn!(
            "[SYNC OLD] Header {} (round {}) is too old (gc_round={}). Will sync for catch-up but may not process immediately.",
            header.id,
            header.round,
            self.gc_round
        );
        
        // Trigger sync for this header to help other nodes catch up
        // But still reject processing to avoid issues
        return Err(DagError::TooOld(header.id.clone(), header.round));
    }

    // ... existing verification ...
}
```

**Files to modify:**
- `primary/src/core.rs`

#### 2.2. Proactive Sync cho Headers/Certificates Quá Cũ

```rust
// primary/src/core.rs:375-387
fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
    if header.round < self.gc_round {
        // IMPROVED: If header is from a known primary and not too far behind,
        // trigger proactive sync to help catch up
        let round_diff = self.gc_round.saturating_sub(header.round);
        const MAX_SYNC_BEHIND: Round = 1000; // Sync headers up to 1000 rounds behind
        
        if round_diff <= MAX_SYNC_BEHIND {
            // Trigger sync for this header to help other nodes catch up
            // This allows primary to sync headers even if they're old
            debug!(
                "[PROACTIVE SYNC] Triggering sync for old header {} (round {}, {} rounds behind)",
                header.id,
                header.round,
                round_diff
            );
            // TODO: Add sync mechanism here
        }
        
        return Err(DagError::TooOld(header.id.clone(), header.round));
    }
    // ...
}
```

**Files to modify:**
- `primary/src/core.rs`
- `primary/src/synchronizer.rs`

### Phase 3: Proactive Synchronization (Priority: MEDIUM)

**Mục tiêu:** Proactive sync để đảm bảo data sẵn sàng trước khi cần

**Implementation:**

#### 3.1. Periodic Sync Check

```rust
// primary/src/synchronizer.rs
pub struct Synchronizer {
    // ... existing fields ...
    last_sync_check: Instant,
    sync_check_interval: Duration,
}

impl Synchronizer {
    pub fn new(...) -> Self {
        Self {
            // ...
            last_sync_check: Instant::now(),
            sync_check_interval: Duration::from_secs(5), // Check every 5 seconds
        }
    }

    /// Periodic check for missing data
    pub async fn periodic_sync_check(&mut self, current_round: Round) -> DagResult<()> {
        let now = Instant::now();
        if now.duration_since(self.last_sync_check) < self.sync_check_interval {
            return Ok(());
        }
        self.last_sync_check = now;

        // Check for missing certificates in recent rounds
        const SYNC_WINDOW: Round = 100; // Check last 100 rounds
        let start_round = current_round.saturating_sub(SYNC_WINDOW);
        
        for round in start_round..current_round {
            // Check if we have quorum certificates for this round
            // If not, trigger sync
            // TODO: Implement certificate count check
        }

        Ok(())
    }
}
```

**Files to modify:**
- `primary/src/synchronizer.rs`
- `primary/src/core.rs` (call periodic_sync_check)

#### 3.2. Round-Based Sync Request

```rust
// primary/src/core.rs
// Trong run loop, thêm periodic sync check
tokio::select! {
    // ... existing selects ...
    () = sync_timer.tick() => {
        // Periodic sync check
        if let Err(e) = self.synchronizer.periodic_sync_check(
            self.consensus_round.load(Ordering::Relaxed)
        ).await {
            warn!("Periodic sync check failed: {}", e);
        }
    }
}
```

**Files to modify:**
- `primary/src/core.rs`

### Phase 4: Network Health Monitoring (Priority: MEDIUM)

**Mục tiêu:** Monitor network health và detect issues sớm

**Implementation:**

#### 4.1. Round Lag Monitoring

```rust
// primary/src/core.rs
pub struct Core {
    // ... existing fields ...
    round_lag_threshold: Round, // Alert if lag > this threshold
    last_round_received: HashMap<PublicKey, Round>, // Track rounds from each primary
}

impl Core {
    /// Check round lag for all primaries
    fn check_round_lag(&self, current_round: Round) {
        for (primary, last_round) in &self.last_round_received {
            let lag = current_round.saturating_sub(*last_round);
            if lag > self.round_lag_threshold {
                warn!(
                    "[NETWORK HEALTH] Primary {:?} is lagging: last round {}, current round {}, lag {}",
                    primary,
                    last_round,
                    current_round,
                    lag
                );
            }
        }
    }
}
```

**Files to modify:**
- `primary/src/core.rs`

#### 4.2. Health Metrics Logging

```rust
// primary/src/core.rs
// Trong run loop, thêm periodic health check
tokio::select! {
    // ... existing selects ...
    () = health_check_timer.tick() => {
        let current_round = self.consensus_round.load(Ordering::Relaxed);
        self.check_round_lag(current_round);
        
        // Log health metrics
        info!(
            "[NETWORK HEALTH] Current round: {}, GC round: {}, Processing: {} headers, Certificates aggregators: {}",
            current_round,
            self.gc_round,
            self.processing.len(),
            self.certificates_aggregators.len()
        );
    }
}
```

**Files to modify:**
- `primary/src/core.rs`

### Phase 5: Improved Retry và Recovery (Priority: LOW)

**Mục tiêu:** Cải thiện retry mechanism cho failed operations

**Implementation:**

#### 5.1. Exponential Backoff cho Sync Requests

```rust
// primary/src/header_waiter.rs
pub struct HeaderWaiter {
    // ... existing fields ...
    sync_retry_delays: HashMap<Digest, Duration>, // Track retry delays per digest
}

impl HeaderWaiter {
    fn get_retry_delay(&mut self, digest: &Digest) -> Duration {
        let delay = self.sync_retry_delays
            .entry(digest.clone())
            .and_modify(|d| {
                // Exponential backoff: double delay each retry, max 30 seconds
                *d = (*d * 2).min(Duration::from_secs(30));
            })
            .or_insert_with(|| Duration::from_secs(1))
            .clone();
        delay
    }
}
```

**Files to modify:**
- `primary/src/header_waiter.rs`

#### 5.2. Failed Broadcast Retry

```rust
// primary/src/core.rs:121-143
async fn process_own_header(&mut self, header: Header) -> DagResult<()> {
    // ... existing logic ...
    
    // IMPROVED: Retry failed broadcasts
    let mut retry_count = 0;
    const MAX_BROADCAST_RETRIES: u32 = 3;
    
    loop {
        let handlers = self.network.broadcast(addresses.clone(), Bytes::from(bytes.clone())).await;
        
        // Check if broadcast was successful
        let failed_count = handlers.iter()
            .filter(|h| h.is_err())
            .count();
        
        if failed_count == 0 || retry_count >= MAX_BROADCAST_RETRIES {
            // All successful or max retries reached
            self.cancel_handlers
                .entry(header.round)
                .or_insert_with(Vec::new)
                .extend(handlers);
            break;
        }
        
        // Retry with exponential backoff
        retry_count += 1;
        let delay = Duration::from_millis(100 * (1 << retry_count)); // 200ms, 400ms, 800ms
        tokio::time::sleep(delay).await;
    }
    
    // ... existing logic ...
}
```

**Files to modify:**
- `primary/src/core.rs`

## IMPLEMENTATION TIMELINE

### Week 1: Phase 1 (CRITICAL)
- [ ] Day 1-2: Implement force advance round timeout
- [ ] Day 3: Testing force advance round
- [ ] Day 4-5: Implement fallback empty parents

**Deliverables:**
- Code changes for Phase 1
- Unit tests
- Integration tests

### Week 2: Phase 2 (HIGH)
- [ ] Day 1-2: Implement soft reject for old headers
- [ ] Day 3: Implement proactive sync
- [ ] Day 4-5: Testing

**Deliverables:**
- Code changes for Phase 2
- Unit tests
- Integration tests

### Week 3: Phase 3 & 4 (MEDIUM)
- [ ] Day 1-2: Implement periodic sync check
- [ ] Day 3: Implement round-based sync request
- [ ] Day 4: Implement round lag monitoring
- [ ] Day 5: Implement health metrics logging

**Deliverables:**
- Code changes for Phase 3 & 4
- Monitoring dashboard (optional)
- Documentation

### Week 4: Phase 5 (LOW) + Polish
- [ ] Day 1-2: Implement exponential backoff
- [ ] Day 3: Implement failed broadcast retry
- [ ] Day 4-5: Polish, documentation, final testing

**Deliverables:**
- Code changes for Phase 5
- Complete documentation
- Final testing report

## TESTING STRATEGY

### Unit Tests
- Force advance round timeout
- Soft reject old headers
- Proactive sync trigger
- Round lag detection

### Integration Tests
- Primary stuck scenario
- Network partition scenario
- Catch-up scenario
- High load scenario

### Performance Tests
- Latency impact của sync operations
- Throughput impact
- Resource usage

## METRICS TO MONITOR

### Network Health
1. **Round lag:** Round của mỗi primary vs current round
2. **Parent certificates received rate:** Số parent certificates nhận được mỗi giây
3. **Header creation rate:** Số headers tạo mỗi giây
4. **Sync requests rate:** Số sync requests mỗi giây

### System Health
1. **Force advance events:** Số lần force advance round
2. **Stuck batches:** Số batches bị stuck (retry > 1000)
3. **Old headers rejected:** Số headers quá cũ bị reject
4. **Failed broadcasts:** Số broadcasts thất bại

## ROLLOUT PLAN

### Stage 1: Internal Testing (Week 1-2)
- Deploy trên test environment
- Monitor metrics
- Fix issues

### Stage 2: Staging (Week 3)
- Deploy trên staging environment
- Load testing
- Performance analysis

### Stage 3: Production (Week 4)
- Gradual rollout (10% → 50% → 100%)
- Monitor closely
- Rollback plan ready

## RISK ASSESSMENT

### Risks
1. **Force advance có thể gây fork:** Risk LOW (empty parents không ảnh hưởng determinism)
2. **Proactive sync có thể gây overhead:** Risk MEDIUM (cần monitor)
3. **Network health check có thể impact performance:** Risk LOW (async, low frequency)

### Mitigation
1. Gradual rollout
2. Comprehensive testing
3. Rollback plan
4. Monitoring và alerting

## SUCCESS CRITERIA

### Short-term (Week 1-2)
- ✅ No primary stuck for > 10 seconds
- ✅ No system deadlock
- ✅ All batches eventually committed or removed

### Medium-term (Week 3-4)
- ✅ Network health metrics available
- ✅ Proactive sync working
- ✅ Reduced round lag

### Long-term (Month 2+)
- ✅ Zero system deadlocks
- ✅ Network health > 99%
- ✅ Catch-up time < 1 minute

## NEXT STEPS

### Immediate (This Week)
1. ✅ Review và approve plan
2. ✅ Set up development environment
3. ✅ Create feature branches

### Next Steps
1. Start Phase 1 implementation
2. Set up monitoring
3. Prepare testing infrastructure

---

**Created:** 2025-01-20
**Status:** 📋 **PLAN READY FOR IMPLEMENTATION**
**Priority:** 🔴 **CRITICAL - Start with Phase 1**

