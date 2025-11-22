# PHÂN TÍCH: FORCE ADVANCE ROUND VÀ CƠ CHẾ CATCH-UP

## FORCE ADVANCE ROUND LÀ GÌ?

### Định Nghĩa:
**Force Advance Round** là cơ chế cho phép proposer tự động advance round khi **KHÔNG nhận được parent certificates từ Core trong một khoảng thời gian nhất định (10 seconds)**.

### Mục Đích:
1. **Ngăn chặn proposer bị stuck:**
   - Nếu Core không gửi parent certificates, proposer không thể tạo headers mới
   - Force advance giúp proposer tiếp tục hoạt động

2. **Duy trì liveness:**
   - Đảm bảo proposer không bị block vĩnh viễn
   - Cho phép proposer advance round ngay cả khi thiếu parent certificates

### Cách Hoạt Động:
```
1. Proposer đang ở round N
2. Proposer KHÔNG nhận parent certificates từ Core > 10 seconds
3. Force advance: round N -> round N+1
4. Clear last_parents: last_parents = Vec::new()
5. Tạo header với empty parents (force_advance = true)
6. Header có thể không được commit (cần quorum parents), nhưng proposer không bị stuck
```

### Code Implementation:
```rust
// primary/src/proposer.rs:1168-1182
if let Some(last_received) = self.last_parent_received_at {
    if last_received.elapsed() > self.max_parent_wait { // 10 seconds
        // Force advance round with empty parents
        warn!("[FORCE ADVANCE] Primary {} force advancing round {} -> {}",
            self.name, self.round, self.round + 1);
        self.round += 1;
        self.last_parents = Vec::new(); // Clear old parents
        self.last_parent_received_at = Some(Instant::now()); // Reset timer
    }
}
```

## VẤN ĐỀ: FORCE ADVANCE KHÔNG GIÚP CATCH-UP

### Vấn Đề Chính:

**Force Advance Round CHỈ giúp proposer không bị stuck, nhưng KHÔNG giúp node chậm catch-up với dữ liệu mới.**

### Lý Do:

#### 1. Headers/Certificates Quá Cũ Bị Reject

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
- Headers/certificates quá cũ (round < gc_round) bị **reject ngay lập tức**
- Node chậm không thể nhận headers/certificates cũ nhưng cần thiết để catch-up
- `gc_round` được tính: `gc_round = current_round - gc_depth` (ví dụ: current_round = 1000, gc_depth = 100 → gc_round = 900)

**Ví dụ:**
```
Current round: 1000
gc_depth: 100
gc_round: 900

Node chậm đang ở round 850
Headers từ round 850-899 → BỊ REJECT (TooOld)
→ Node chậm KHÔNG THỂ catch-up
```

#### 2. Force Advance Chỉ Advance Round, Không Sync Dữ Liệu

**Vấn đề:**
- Force advance chỉ tăng `self.round += 1`
- **KHÔNG sync dữ liệu** (headers/certificates) từ các node khác
- Node vẫn thiếu dữ liệu từ các rounds cũ

**Ví dụ:**
```
Node chậm: round 850, thiếu certificates từ round 851-1000
Force advance: round 850 -> 851
Nhưng vẫn THIẾU certificates từ round 851-1000
→ Node vẫn không catch-up được
```

#### 3. Synchronizer Chỉ Reactive

**Code hiện tại:**
```rust
// primary/src/synchronizer.rs:85-114
pub async fn get_parents(&mut self, header: &Header) -> DagResult<Vec<Certificate>> {
    // ...
    if missing.is_empty() {
        return Ok(parents);
    }
    // Sync missing parents - CHỈ khi có header cần process
    self.tx_header_waiter.send(WaiterMessage::SyncParents(missing, header.clone())).await;
    Ok(Vec::new())
}
```

**Vấn đề:**
- Synchronizer chỉ sync **khi có header cần process**
- Nhưng headers quá cũ bị reject → **không trigger sync**
- Node chậm không có cơ chế **proactive sync** để catch-up

## GIẢI PHÁP: CẢI THIỆN CATCH-UP MECHANISM

### Phase 2: Xử Lý Headers/Certificates Quá Cũ (Catch-Up Support)

**Mục tiêu:** Cho phép node chậm nhận và sync headers/certificates quá cũ để catch-up

#### Giải Pháp 1: Soft Reject với Sync Trigger

**Ý tưởng:**
- Thay vì reject ngay, **soft reject** (trigger sync nhưng không process)
- Cho phép sync headers/certificates quá cũ (up to N rounds behind)

**Implementation:**
```rust
// primary/src/core.rs:375-387
fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
    if header.round < self.gc_round {
        // IMPROVED: Soft reject - allow sync but don't process immediately
        let round_diff = self.gc_round.saturating_sub(header.round);
        const MAX_CATCHUP_ROUNDS: Round = 1000; // Allow catch-up up to 1000 rounds behind
        
        if round_diff <= MAX_CATCHUP_ROUNDS {
            // Trigger sync for catch-up
            warn!(
                "[CATCH-UP SYNC] Header {} (round {}) is {} rounds behind (gc_round={}). Triggering sync for catch-up.",
                header.id, header.round, round_diff, self.gc_round
            );
            // TODO: Trigger sync mechanism here
            // Still reject processing to avoid issues
        }
        
        return Err(DagError::TooOld(header.id.clone(), header.round));
    }
    // ... existing verification ...
}
```

#### Giải Pháp 2: Proactive Sync cho Node Chậm

**Ý tưởng:**
- Node chậm tự động request sync dữ liệu từ các node khác
- Sync các rounds còn thiếu

**Implementation:**
```rust
// primary/src/core.rs
// Periodic catch-up sync check
async fn catchup_sync_check(&mut self) -> DagResult<()> {
    let current_round = self.consensus_round.load(Ordering::Relaxed);
    let our_round = self.gc_round + self.gc_depth; // Approximate our round
    
    if our_round < current_round.saturating_sub(100) {
        // We're more than 100 rounds behind - trigger catch-up
        warn!(
            "[CATCH-UP] Primary {} is {} rounds behind (our: {}, current: {}). Triggering catch-up sync.",
            self.name,
            current_round.saturating_sub(our_round),
            our_round,
            current_round
        );
        
        // Request certificates from missing rounds
        let start_round = our_round;
        let end_round = current_round.min(our_round + 1000); // Sync up to 1000 rounds ahead
        
        for round in start_round..end_round {
            // Request certificates from this round
            // TODO: Implement certificate request mechanism
        }
    }
    
    Ok(())
}
```

#### Giải Pháp 3: Network Request Missing Rounds

**Ý tưởng:**
- Node chậm broadcast request để các node khác gửi certificates từ rounds còn thiếu
- Các node khác respond với certificates từ rounds requested

**Implementation:**
```rust
// primary/src/core.rs
// Request certificates from missing rounds
async fn request_missing_certificates(&mut self, start_round: Round, end_round: Round) {
    let addresses: Vec<_> = self
        .committee
        .others_primaries(&self.name)
        .iter()
        .map(|(_, x)| x.primary_to_primary)
        .collect();
    
    // Create request message
    let message = PrimaryMessage::CertificatesRequestRange(start_round, end_round, self.name);
    let bytes = bincode::serialize(&message).expect("Failed to serialize request");
    
    // Broadcast request
    self.network.broadcast(addresses, Bytes::from(bytes)).await;
}
```

**Cần thêm message type:**
```rust
// primary/src/primary.rs:42-48
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryMessage {
    Header(Header),
    Vote(Vote),
    Certificate(Certificate),
    CertificatesRequest(Vec<Digest>, PublicKey),
    CertificatesRequestRange(Round, Round, PublicKey), // NEW: Request certificates from range
}
```

## KẾT LUẬN

### Force Advance Round:
- ✅ **Giúp proposer không bị stuck**
- ❌ **KHÔNG giúp node chậm catch-up**
- ✅ **Duy trì liveness nhưng không sync dữ liệu**

### Cần Cải Thiện:
1. **Phase 2 (HIGH):** Xử lý headers/certificates quá cũ
   - Soft reject với sync trigger
   - Cho phép sync headers/certificates quá cũ (up to 1000 rounds behind)

2. **Phase 3 (MEDIUM):** Proactive synchronization
   - Periodic catch-up sync check
   - Node chậm tự động request sync dữ liệu

3. **Network Improvements:**
   - CertificatesRequestRange message type
   - Broadcast request cho missing rounds
   - Respond với certificates từ requested rounds

### Timeline:
- **Week 1:** ✅ Phase 1 (Force Advance) - **COMPLETED**
- **Week 2:** ⏳ Phase 2 (Old Headers Handling) - **NEXT**
- **Week 3:** ⏳ Phase 3 (Proactive Sync) - **PENDING**

---

**Last Updated:** 2025-01-20
**Status:** 📋 **ANALYSIS COMPLETE - Need Phase 2 for Catch-Up**

