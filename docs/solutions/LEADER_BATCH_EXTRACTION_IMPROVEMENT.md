# CẢI THIỆN LEADER BATCH EXTRACTION - Đề Xuất Giải Pháp

## VẤN ĐỀ HIỆN TẠI

### Mô Tả
Khi một primary (không phải leader) tạo batch, batch này có thể bị **stuck** trong queue của primary đó vô thời hạn nếu:
- Primary đó không phải leader trong các rounds liên tiếp
- Certificate của primary đó không bao giờ được commit
- Leader chỉ extract batches từ **certificates đã commit**, không từ queue của các primary khác

### Ví Dụ Thực Tế
Transaction `0ecd11a6f2f44034ed64c5aef6c8233afdfa82503bbab68987fa56d0dd40f4da`:
- Batch: `O18SFd0IKspXKDURh9c7cOSm1aKNL56EwizVoOmm/vc=`
- Primary tạo: `AqJy7eip40qqZk7F` (primary-0)
- Được đưa vào **hàng trăm headers** (round 103886 → 125389+)
- **KHÔNG BAO GIỜ** được commit → Transaction không bao giờ được thực thi

### Root Cause
1. **Leader chỉ extract từ certificates đã commit:**
   ```rust
   async fn extract_batches_from_parents(&mut self, parent_digests: &[Digest], parent_round: Round) {
       // Chỉ extract từ certificates đã commit (có trong store)
       match self.store.read(parent_digest.to_vec()).await {
           // ...
       }
   }
   ```

2. **Batch chưa commit không có trong parent certificates:**
   - Batch trong queue của primary-0 (Pending/InFlight)
   - Certificate của primary-0 không được commit
   - Leader không biết về batch này

3. **Retry logic không giải quyết vấn đề:**
   - Primary-0 retry batch này liên tục
   - Nhưng vẫn không được commit vì primary-0 không phải leader

## GIẢI PHÁP ĐỀ XUẤT

### PHƯƠNG ÁN 1: Header-Based Batch Extraction (KHUYẾN NGHỊ) ⭐

#### Ý Tưởng
**Leader extract batches trực tiếp từ HEADERS của các primary khác**, không cần đợi certificates commit.

#### Cơ Chế
1. **Khi leader nhận header từ primary khác:**
   - Extract batches từ `header.payload` (đã có sẵn trong header)
   - Thêm vào queue của leader nếu batch chưa commit
   - Leader có thể include batch này trong header của mình

2. **Lợi Ích:**
   - ✅ **Không cần đợi certificate commit** - Extract ngay từ header
   - ✅ **Không cần thêm network message** - Header đã được broadcast
   - ✅ **Đơn giản và hiệu quả** - Tận dụng data sẵn có
   - ✅ **Tương thích với architecture hiện tại** - Header đã có payload

#### Implementation

**1. Thêm method extract batches từ headers:**

```rust
impl Proposer {
    /// Extract batches from headers (not just certificates)
    /// This allows leader to include batches from non-leader primaries immediately
    async fn extract_batches_from_headers(&mut self, headers: &[Header]) {
        for header in headers {
            // Skip our own headers (we already know about these batches)
            if header.author == self.name {
                continue;
            }

            // Extract batches from header payload
            for (batch_digest, worker_id) in header.payload.iter() {
                // Skip if already committed
                if self.committed_digests.contains_key(batch_digest) {
                    continue;
                }

                // Skip if already in queue
                if self.digests.iter().any(|entry| entry.digest == *batch_digest) {
                    continue;
                }

                // Check if batch is in store (required for inclusion)
                match self.store.read(batch_digest.to_vec()).await {
                    Ok(Some(_batch_data)) => {
                        let size = batch_digest.size();
                        
                        // Add to queue as Pending
                        self.digests.push_back(BatchEntry {
                            digest: batch_digest.clone(),
                            worker_id: *worker_id,
                            size,
                            state: BatchState::Pending,
                            retry_count: 0,
                        });
                        self.pending_payload_size += size;

                        info!(
                            "[BATCH EXTRACTION] Primary {} (leader) EXTRACTED batch {} from header {} (round {}, author: {}) into queue. Batch from non-leader primary can now be committed by leader.",
                            self.name,
                            batch_digest,
                            header.id,
                            header.round,
                            header.author
                        );
                    }
                    Ok(None) => {
                        // Batch not in store yet - will be synced later
                        debug!(
                            "[BATCH EXTRACTION] Batch {} from header {} (round {}, author: {}) not in store yet, will be synced later",
                            batch_digest,
                            header.id,
                            header.round,
                            header.author
                        );
                    }
                    Err(e) => {
                        warn!(
                            "[BATCH EXTRACTION] Error reading batch {} from store: {}",
                            batch_digest, e
                        );
                    }
                }
            }
        }
    }
}
```

**2. Sử dụng trong proposer loop:**

```rust
// Trong proposer loop, khi nhận headers từ core:
Some((parents, round)) = self.rx_core.recv() => {
    // ... existing parent processing ...

    // IMPROVED: Also extract batches from headers in the current round
    // This allows leader to include batches from non-leader primaries
    // Headers are already available through the core's header processing
    // We need to get recent headers from the core or store
    
    // Option 1: Pass headers from core to proposer
    // Option 2: Query store for recent headers (more complex)
    
    // For now, we can extract from headers we receive from core
    // (Need to extend rx_core to also send headers, or add new channel)
}
```

**3. Mở rộng interface Core → Proposer:**

```rust
// Thêm channel mới để core gửi headers đến proposer
rx_headers: Receiver<Header>,

// Hoặc mở rộng rx_core để gửi cả headers:
rx_core: Receiver<(Vec<Digest>, Round, Option<Vec<Header>>)>,
```

**4. Core gửi headers đến proposer:**

```rust
// Trong core.rs, khi xử lý header:
async fn process_header(&mut self, header: &Header) -> DagResult<()> {
    // ... existing processing ...
    
    // If we're the leader for this round, send headers to proposer for batch extraction
    if self.is_leader_for_round(header.round) {
        // Send header to proposer
        self.tx_headers.send(header.clone()).await.ok();
    }
    
    // ...
}
```

#### Ưu Điểm
- ✅ **Đơn giản**: Tận dụng data sẵn có (headers)
- ✅ **Hiệu quả**: Không cần thêm network traffic
- ✅ **Nhanh**: Extract ngay khi nhận header, không cần đợi certificate
- ✅ **An toàn**: Header đã được verify, batch digests đã được kiểm tra
- ✅ **Tương thích**: Không cần thay đổi nhiều architecture hiện tại

#### Nhược Điểm
- ⚠️ **Cần batch trong store**: Batch phải được sync trước khi leader có thể include
- ⚠️ **Cần mở rộng interface**: Core cần gửi headers đến proposer
- ⚠️ **Chỉ leader extract**: Các primary khác vẫn không biết về batches của nhau

#### Cost Estimate
- **Complexity**: Medium (cần mở rộng interface Core-Proposer)
- **Network**: No additional overhead
- **Storage**: No additional overhead
- **Performance**: Minimal impact (chỉ thêm processing cho headers)

---

### PHƯƠNG ÁN 2: Batch Sharing Message (Phức Tạp Hơn)

#### Ý Tưởng
**Primary gửi batch metadata (digests) đến leader** khi có batch mới hoặc định kỳ.

#### Cơ Chế
1. **Primary announce batches chưa commit đến leader:**
   - Khi có batch mới (Pending state)
   - Định kỳ (ví dụ: mỗi N rounds)
   - Khi retry batch (InFlight → Pending)

2. **Leader query các primary về batches chưa commit:**
   - Leader định kỳ query các primary khác
   - Primary trả về list batches chưa commit

3. **Thêm message type mới:**
   ```rust
   pub enum PrimaryMessage {
       Header(Header),
       Vote(Vote),
       Certificate(Certificate),
       CertificatesRequest(Vec<Digest>, PublicKey),
       // NEW:
       BatchAnnouncement {
           batches: Vec<(Digest, WorkerId)>,
           round: Round,
       },
       BatchQueryRequest {
           requestor: PublicKey,
           round: Round,
       },
       BatchQueryResponse {
           batches: Vec<(Digest, WorkerId)>,
           round: Round,
       },
   }
   ```

#### Implementation Sketch

**1. Primary announce batches đến leader:**

```rust
// Trong proposer.rs
async fn announce_batches_to_leader(&mut self, leader: PublicKey) {
    // Collect uncommitted batches
    let uncommitted_batches: Vec<(Digest, WorkerId)> = self.digests
        .iter()
        .filter(|entry| !matches!(entry.state, BatchState::Committed))
        .map(|entry| (entry.digest.clone(), entry.worker_id))
        .collect();

    if !uncommitted_batches.is_empty() {
        let message = PrimaryMessage::BatchAnnouncement {
            batches: uncommitted_batches,
            round: self.round,
        };
        
        // Send to leader (need channel to core)
        // self.tx_core.send(...)
    }
}
```

**2. Leader query batches:**

```rust
// Trong proposer.rs (leader)
async fn query_batches_from_primaries(&mut self, primaries: &[PublicKey]) {
    for primary in primaries {
        let message = PrimaryMessage::BatchQueryRequest {
            requestor: self.name.clone(),
            round: self.round,
        };
        
        // Send query (need channel to core)
        // self.tx_core.send(...)
    }
}
```

**3. Primary trả lời query:**

```rust
// Trong core.rs
match message {
    PrimaryMessage::BatchQueryRequest { requestor, round } => {
        // Get uncommitted batches from proposer (need access)
        let batches = self.get_uncommitted_batches();
        
        let response = PrimaryMessage::BatchQueryResponse {
            batches,
            round,
        };
        
        // Send response
        self.network.send(address, serialize(response)).await;
    }
    // ...
}
```

#### Ưu Điểm
- ✅ **Chủ động**: Leader có thể query batches bất cứ lúc nào
- ✅ **Chi tiết**: Primary có thể gửi metadata chi tiết (retry_count, age, etc.)
- ✅ **Linh hoạt**: Có thể adjust frequency và strategy

#### Nhược Điểm
- ❌ **Phức tạp**: Cần thêm message types, handlers
- ❌ **Network overhead**: Thêm network traffic
- ❌ **Timing issues**: Cần sync giữa proposer và core
- ❌ **Dễ bị spam**: Primary có thể gửi quá nhiều announcements

#### Cost Estimate
- **Complexity**: High (cần thêm nhiều components)
- **Network**: Medium (thêm query/response messages)
- **Storage**: Low (chỉ metadata)
- **Performance**: Medium (thêm processing)

---

### PHƯƠNG ÁN 3: Enhanced Parent Extraction (Cải Thiện Hiện Tại)

#### Ý Tưởng
**Cải thiện logic extract từ parent certificates** để bao gồm cả headers chưa commit.

#### Cơ Chế
1. **Extract từ headers trong parent certificates:**
   - Hiện tại chỉ extract từ `certificate.header.payload`
   - Giữ nguyên logic, nhưng cải thiện timing và coverage

2. **Extract từ headers của các rounds gần đây:**
   - Query store cho headers gần đây (không chỉ certificates)
   - Extract batches từ tất cả headers trong last N rounds

3. **Cải thiện retry logic:**
   - Batch trong InFlight state → Convert to Pending khi leader extract
   - Batch retry nhiều lần → ưu tiên cho leader extraction

#### Implementation Sketch

```rust
async fn extract_batches_from_recent_headers(&mut self, current_round: Round) {
    // Extract from headers in last N rounds (not just certificates)
    let lookback_rounds = 10; // Configurable
    
    for round in (current_round.saturating_sub(lookback_rounds)..current_round).rev() {
        // Query store for headers in this round
        // (Need to implement header indexing by round)
        
        // Extract batches from each header
        // Similar to extract_batches_from_parents
    }
}
```

#### Ưu Điểm
- ✅ **Minimal changes**: Chỉ cải thiện logic hiện có
- ✅ **No new messages**: Sử dụng existing infrastructure
- ✅ **Backward compatible**: Không breaking changes

#### Nhược Điểm
- ⚠️ **Store dependency**: Cần headers được index theo round
- ⚠️ **Limited coverage**: Chỉ cover headers trong store (đã được process)
- ⚠️ **Performance**: Query store nhiều có thể chậm

#### Cost Estimate
- **Complexity**: Low-Medium (cần header indexing)
- **Network**: No additional overhead
- **Storage**: Low (cần index headers)
- **Performance**: Medium (query overhead)

---

## SO SÁNH CÁC PHƯƠNG ÁN

| Tiêu Chí | Phương Án 1: Header-Based | Phương Án 2: Batch Sharing | Phương Án 3: Enhanced Extraction |
|----------|---------------------------|----------------------------|----------------------------------|
| **Complexity** | Medium | High | Low-Medium |
| **Network Overhead** | None | Medium | None |
| **Latency** | Low (immediate) | Medium (query delay) | Medium (store query) |
| **Coverage** | All headers | All primaries | Limited (store) |
| **Compatibility** | Good | Requires changes | Excellent |
| **Performance** | Excellent | Good | Medium |
| **Implementation Time** | 2-3 days | 5-7 days | 1-2 days |

## KHUYẾN NGHỊ: PHƯƠNG ÁN 1 - Header-Based Extraction ⭐

### Lý Do

1. **Hiệu quả nhất**: Extract ngay từ headers, không cần đợi certificates
2. **Đơn giản nhất**: Tận dụng data sẵn có, minimal changes
3. **Không tăng network**: Headers đã được broadcast
4. **Giải quyết được vấn đề**: Leader có thể include batches từ non-leader primaries ngay

### Implementation Plan

#### Phase 1: Mở Rộng Interface Core-Proposer (1 ngày)
- [ ] Thêm channel `rx_headers: Receiver<Header>` trong Proposer
- [ ] Thêm `tx_headers: Sender<Header>` trong Core
- [ ] Core gửi headers đến proposer khi process

#### Phase 2: Implement Extraction Logic (1 ngày)
- [ ] Implement `extract_batches_from_headers()` method
- [ ] Thêm logic skip own headers
- [ ] Thêm validation (check committed, check queue)
- [ ] Thêm logging cho batch extraction

#### Phase 3: Integration (1 ngày)
- [ ] Integrate extraction vào proposer loop
- [ ] Test với multiple primaries
- [ ] Verify leader có thể extract batches từ non-leader primaries
- [ ] Performance testing

#### Phase 4: Testing & Documentation (1 ngày)
- [ ] Unit tests
- [ ] Integration tests
- [ ] Update documentation
- [ ] Monitor trong production

### Testing Strategy

1. **Unit Tests:**
   - Test `extract_batches_from_headers()` với various scenarios
   - Test skip logic (own headers, committed batches, duplicates)
   - Test batch addition to queue

2. **Integration Tests:**
   - Test với 3+ primaries, 1 leader
   - Verify leader extracts batches từ non-leader primaries
   - Verify batches được commit nhanh hơn

3. **Performance Tests:**
   - Measure overhead của extraction logic
   - Measure impact on header processing time
   - Verify no memory leaks

### Rollout Plan

1. **Development**: Implement và test trong dev environment
2. **Staging**: Deploy và monitor trong staging
3. **Production**: Deploy với feature flag (có thể disable nếu có vấn đề)
4. **Monitoring**: Monitor metrics và logs
5. **Iteration**: Adjust dựa trên feedback

### Metrics to Monitor

1. **Batch Extraction Rate:**
   - Số batches leader extract từ non-leader primaries
   - Tỷ lệ batches được extract vs total batches

2. **Commit Latency:**
   - Time từ batch creation đến commit
   - Compare trước và sau implementation

3. **Stuck Batches:**
   - Số batches bị stuck > N rounds
   - Should decrease sau implementation

4. **Performance:**
   - Header processing time
   - Proposer loop latency
   - Memory usage

### Risks & Mitigations

1. **Risk: Batch not in store khi extract**
   - **Mitigation**: Skip batch, sẽ được extract lại sau khi sync
   - **Impact**: Low - chỉ delay, không mất batch

2. **Risk: Race condition giữa extract và commit**
   - **Mitigation**: Double-check `committed_digests` trước khi add
   - **Impact**: Low - đã có logic check

3. **Risk: Performance overhead**
   - **Mitigation**: Limit số headers process mỗi loop
   - **Impact**: Low - chỉ process headers mới, không lặp lại

## ALTERNATIVE: Hybrid Approach

Nếu Phương Án 1 không đủ, có thể kết hợp:

1. **Header-Based Extraction** (Phương Án 1) - Primary mechanism
2. **Periodic Batch Query** (Phương Án 2) - Backup mechanism cho batches bị miss

Leader định kỳ query primaries về batches chưa commit để catch any missed batches.

## KẾT LUẬN

**Phương Án 1 (Header-Based Extraction)** là giải pháp tối ưu vì:
- ✅ Đơn giản và hiệu quả
- ✅ Không tăng network overhead
- ✅ Giải quyết được vấn đề liveness
- ✅ Dễ implement và maintain

**Next Steps:**
1. Review và approve phương án
2. Implement theo plan ở trên
3. Test thoroughly
4. Deploy và monitor

---

**Tài liệu này được tạo để hỗ trợ implementation. Mọi feedback và suggestions đều được hoan nghênh.**

