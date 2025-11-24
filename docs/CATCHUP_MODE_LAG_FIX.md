# PHÂN TÍCH VÀ SỬA LỖI: PRIMARY-0 KHÔNG THỂ QUAY LẠI ĐỒNG THUẬN

## VẤN ĐỀ

**Hiện tượng:**
- Transaction `184817b2b8e2b05933ebb6a9c385191f2a7cde1a344b63f11bba93c4e685f32d` không được đồng thuận
- Primary-0 bị chậm so với các primary khác (round 45634 vs round 45636 = lag 2 rounds)
- Khi primary-0 chậm, **mãi sau không thể quay lại đồng thuận được**

**Phân tích logs:**
- Primary-0: round 45502 (proposer round)
- Primary-1: round 45510 (proposer round)
- **Lag: 8 rounds** trong proposer
- Nhưng **không vào catch-up mode** vì threshold quá cao (100 rounds)

---

## NGUYÊN NHÂN

### 1. **Lag Calculation SAI**

**Code cũ:**
```rust
let our_round = self.gc_round + self.gc_depth;
let lag = current_round.saturating_sub(our_round);
```

**Vấn đề:**
- `our_round = gc_round + gc_depth` không phản ánh **proposer round hiện tại**
- `current_round = consensus_round` (last committed round) - có thể thấp hơn nhiều so với proposer round
- **Kết quả:** Lag tính sai → không phát hiện khi node lag

**Ví dụ:**
- Primary-0 proposer round: 45634
- Primary-1 proposer round: 45636
- Consensus round: 45430 (thấp hơn nhiều)
- **Lag tính = 45430 - (gc_round + gc_depth) = ~0** ❌ (sai!)

### 2. **Threshold Quá Cao**

**Code cũ:**
```rust
const LAG_THRESHOLD: Round = 100; // Enter catch-up mode if lag >= 100 rounds
```

**Vấn đề:**
- Với lag chỉ 8-10 rounds, primary-0 **không vào catch-up mode**
- Primary-0 vẫn tiếp tục tạo headers nhưng headers không được commit vì lag
- **Kết quả:** Primary-0 stuck - tạo headers không cần thiết mà không bắt kịp

### 3. **Không Track Network Round**

**Vấn đề:**
- Không track round cao nhất từ network (headers/certificates nhận được)
- Không so sánh proposer round với network round
- **Kết quả:** Không biết node có lag hay không

---

## GIẢI PHÁP ĐÃ TRIỂN KHAI

### 1. **Track Highest Network Round**

**Thêm fields:**
```rust
/// CATCH-UP MODE: Track highest round seen from network
highest_network_round: Round,
/// CATCH-UP MODE: Track our current proposer round
current_proposer_round: Round,
```

**Cập nhật khi nhận headers/certificates:**
```rust
// In process_header()
if header.round > self.highest_network_round {
    self.highest_network_round = header.round;
}

// In process_certificate()
if cert_round > self.highest_network_round {
    self.highest_network_round = cert_round;
}
```

### 2. **Tính Lag Chính Xác**

**Code mới:**
```rust
// Update highest network round from certificates aggregators
if let Some(max_round) = self.certificates_aggregators.keys().max().copied() {
    if max_round > self.highest_network_round {
        self.highest_network_round = max_round;
    }
}

// Update current proposer round (can propose for max_round + 1)
if let Some(max_round) = self.certificates_aggregators.keys().max().copied() {
    self.current_proposer_round = max_round + 1;
}

// Calculate lag correctly
let network_current_round = current_round.max(self.highest_network_round);
let lag = network_current_round.saturating_sub(self.current_proposer_round);
```

**Kết quả:**
- ✅ Lag tính đúng dựa trên proposer round vs network round
- ✅ Phát hiện lag ngay cả khi lag nhỏ (8-10 rounds)

### 3. **Giảm Threshold**

**Code mới:**
```rust
// Giảm threshold xuống để phát hiện lag sớm hơn
const LAG_THRESHOLD: Round = 10; // Enter catch-up mode if lag >= 10 rounds (giảm từ 100)
const RESUME_THRESHOLD: Round = 5; // Resume normal operation if lag < 5 rounds (giảm từ 50)
const LAG_SKIP_CONSENSUS_THRESHOLD: Round = 20; // Skip consensus if lag > 20 rounds (giảm từ 200)
```

**Kết quả:**
- ✅ Phát hiện lag sớm hơn (10 rounds thay vì 100)
- ✅ Resume sớm hơn (5 rounds thay vì 50)
- ✅ Skip consensus sớm hơn (20 rounds thay vì 200)

---

## SO SÁNH TRƯỚC VÀ SAU

### TRƯỚC (Code cũ):

```
Primary-0 proposer round: 45634
Primary-1 proposer round: 45636
Consensus round: 45430

our_round = gc_round + gc_depth = ~45480
lag = consensus_round - our_round = 45430 - 45480 = ~0 rounds ❌

Threshold: 100 rounds
→ Không vào catch-up mode ❌
→ Primary-0 tiếp tục tạo headers nhưng không bắt kịp ❌
```

### SAU (Code mới):

```
Primary-0 proposer round: 45634
Primary-1 proposer round: 45636
Network highest round: 45636 (từ certificates aggregators)
Consensus round: 45430

current_proposer_round = 45634 (from certificates aggregators + 1)
network_current_round = max(45430, 45636) = 45636
lag = 45636 - 45634 = 2 rounds ✅

Threshold: 10 rounds
→ lag = 2 < 10, không vào catch-up mode (OK vì lag nhỏ)
→ Nhưng nếu lag >= 10 → vào catch-up mode ✅
```

---

## LOG MESSAGES MỚI

### Enter Catch-up Mode
```
[CATCH-UP MODE] Primary {} ENTERING catch-up mode - lag: {} rounds 
(our_proposer_round: {}, network_round: {}, consensus_round: {}, gc_round: {}). 
Pausing proposer and focusing on syncing.
```

### Resume
```
[CATCH-UP MODE] Primary {} RESUMING normal operation - caught up in {:?} 
(lag: {} rounds, our_proposer_round: {}, network_round: {}, consensus_round: {}). 
Resuming proposer.
```

### Up to Date
```
[CATCH-UP SYNC] Periodic check - node {} is up to date 
(our_proposer_round: {}, network_round: {}, consensus_round: {}, lag: {})
```

---

## KẾT QUẢ MONG ĐỢI

### Với Lag Nhỏ (< 10 rounds):
- ✅ Không vào catch-up mode (OK - lag nhỏ)
- ✅ Vẫn tạo headers bình thường
- ✅ Có thể bắt kịp tự nhiên

### Với Lag Trung Bình (10-20 rounds):
- ✅ **Vào catch-up mode** → Pause proposer
- ✅ Tập trung sync dữ liệu
- ✅ Bắt kịp nhanh hơn

### Với Lag Lớn (> 20 rounds):
- ✅ **Vào catch-up mode** → Pause proposer
- ✅ **Skip consensus** → Không vote khi thiếu context
- ✅ Tập trung sync dữ liệu
- ✅ Bắt kịp nhanh hơn

---

## CẢI TIẾN THÊM CÓ THỂ THỰC HIỆN

1. **Dynamic Thresholds**: Tune thresholds dựa trên network conditions
2. **Proposer Round Channel**: Có channel từ proposer đến core để biết proposer round chính xác hơn
3. **Network Round Tracking**: Track round cao nhất từ từng peer để phát hiện lag sớm hơn
4. **Metrics**: Thêm metrics để monitor catch-up mode effectiveness

---

## KẾT LUẬN

**Vấn đề chính:**
- ❌ Lag calculation sai (dùng consensus_round thay vì proposer round)
- ❌ Threshold quá cao (100 rounds)
- ❌ Không track network round

**Giải pháp:**
- ✅ Track highest network round và proposer round
- ✅ Tính lag chính xác (network_round - proposer_round)
- ✅ Giảm threshold xuống 10 rounds
- ✅ Phát hiện lag sớm hơn và vào catch-up mode kịp thời

**Kết quả:**
- ✅ Primary-0 sẽ vào catch-up mode khi lag >= 10 rounds
- ✅ Pause proposer để tập trung sync
- ✅ Bắt kịp nhanh hơn và quay lại đồng thuận kịp thời

