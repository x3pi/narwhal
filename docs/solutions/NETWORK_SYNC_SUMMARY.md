# TÓM TẮT: KẾ HOẠCH CẢI THIỆN NETWORK SYNCHRONIZATION

## VẤN ĐỀ CHÍNH

1. **Primary bị stuck:** Proposer không nhận được parent certificates → không tạo headers mới
2. **Headers quá cũ bị reject:** Headers/certificates quá cũ (round < gc_round) bị reject ngay
3. **Synchronization reactive:** Chỉ sync khi cần, không proactive
4. **Không có health monitoring:** Không detect được primary bị lag

## GIẢI PHÁP (5 PHASES)

### 🔴 Phase 1: Force Advance Round Khi Stuck (CRITICAL)
**Thời gian:** Week 1  
**Mục tiêu:** Ngăn chặn proposer bị stuck

**Các bước:**
1. Thêm timeout cho parent certificates (10 seconds)
2. Force advance round nếu không nhận parent certificates
3. Cho phép tạo header với empty parents khi force advance

**Files to modify:**
- `primary/src/proposer.rs`

### 🟠 Phase 2: Xử Lý Headers/Certificates Quá Cũ (HIGH)
**Thời gian:** Week 2  
**Mục tiêu:** Cho phép sync headers/certificates quá cũ

**Các bước:**
1. Soft reject cho headers/certificates quá cũ
2. Proactive sync cho headers quá cũ (up to 1000 rounds behind)
3. Sync mechanism để help nodes catch up

**Files to modify:**
- `primary/src/core.rs`
- `primary/src/synchronizer.rs`

### 🟡 Phase 3: Proactive Synchronization (MEDIUM)
**Thời gian:** Week 3  
**Mục tiêu:** Proactive sync để đảm bảo data sẵn sàng

**Các bước:**
1. Periodic sync check (every 5 seconds)
2. Round-based sync request
3. Check missing certificates in recent rounds

**Files to modify:**
- `primary/src/synchronizer.rs`
- `primary/src/core.rs`

### 🟡 Phase 4: Network Health Monitoring (MEDIUM)
**Thời gian:** Week 3  
**Mục tiêu:** Monitor network health và detect issues sớm

**Các bước:**
1. Round lag monitoring
2. Health metrics logging
3. Alert khi primary bị lag

**Files to modify:**
- `primary/src/core.rs`

### 🟢 Phase 5: Improved Retry và Recovery (LOW)
**Thời gian:** Week 4  
**Mục tiêu:** Cải thiện retry mechanism

**Các bước:**
1. Exponential backoff cho sync requests
2. Failed broadcast retry
3. Better error handling

**Files to modify:**
- `primary/src/header_waiter.rs`
- `primary/src/core.rs`

## TIMELINE

```
Week 1: Phase 1 (CRITICAL) - Force Advance Round
Week 2: Phase 2 (HIGH) - Old Headers Handling
Week 3: Phase 3 & 4 (MEDIUM) - Proactive Sync + Monitoring
Week 4: Phase 5 (LOW) + Polish
```

## METRICS TO MONITOR

### Network Health
- Round lag (primary vs current round)
- Parent certificates received rate
- Header creation rate
- Sync requests rate

### System Health
- Force advance events
- Stuck batches (retry > 1000)
- Old headers rejected
- Failed broadcasts

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
2. ✅ Start Phase 1 implementation
3. ✅ Set up monitoring

### Phase 1 Implementation
1. Add timeout tracking in Proposer
2. Implement force advance logic
3. Test với proposer stuck scenario

---

**Full Plan:** `NETWORK_SYNCHRONIZATION_IMPROVEMENT_PLAN.md`  
**Status:** 📋 **PLAN READY - Start with Phase 1**

