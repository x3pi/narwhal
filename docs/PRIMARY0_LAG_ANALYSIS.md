# PHÂN TÍCH: PRIMARY-0 BỊ CHẬM VÀ CHƯA ĐUỔI KỊP

## TÌNH TRẠNG HIỆN TẠI

### Round Comparison (01:22:40)

| Primary | Proposer Round | Consensus Round | Lag vs Primary-1 |
|---------|----------------|------------------|------------------|
| Primary-0 | 506647 | 506741 | **37 rounds** |
| Primary-1 | 506684 | - | - |

**Kết luận:** Primary-0 **đang lag 37 rounds** so với Primary-1

### Lag Calculation

```
Consensus Round: 506741
Primary-0 Proposer Round: 506647
Lag (Consensus vs Proposer): 94 rounds
```

---

## PHÂN TÍCH CHI TIẾT

### 1. **Primary-0 có bị chậm không?**

✅ **CÓ** - Primary-0 đang lag 37 rounds so với Primary-1

**Timeline:**
- **01:14:46** (khi batch được tạo): Primary-0 round 503633
- **01:22:40** (hiện tại): Primary-0 round 506647
- **Tăng:** 3014 rounds trong ~8 phút (~377 rounds/phút)

**So sánh:**
- Primary-1 hiện tại: 506684
- Primary-0 hiện tại: 506647
- **Lag: 37 rounds** (khoảng 3-4 giây với ~50ms/round)

### 2. **Primary-0 đã đuổi kịp chưa?**

❌ **CHƯA** - Primary-0 vẫn đang lag 37 rounds

**Tốc độ bắt kịp:**
- Primary-0: ~377 rounds/phút
- Primary-1: ~377 rounds/phút (ước tính)
- **Không có dấu hiệu bắt kịp** - lag vẫn duy trì ở mức 37 rounds

### 3. **Catch-Up Mode có hoạt động không?**

❌ **KHÔNG** - Không có log về catch-up mode

**Vấn đề:**
- Không có log `[CATCH-UP MODE] ENTERING catch-up mode`
- Không có log `[CATCH-UP MODE] RESUMING normal operation`
- Không có log về lag calculation

**Nguyên nhân có thể:**
1. **Lag calculation sai:**
   - `current_proposer_round` được tính từ `certificates_aggregators.keys().max() + 1`
   - Nếu primary-0 lag, `certificates_aggregators` có thể không có round cao nhất từ network
   - → `current_proposer_round` tính sai → lag tính sai

2. **Threshold chưa đạt:**
   - Threshold: 10 rounds
   - Lag thực tế: 37 rounds (proposer vs proposer) hoặc 94 rounds (consensus vs proposer)
   - → Nên đã trigger catch-up mode, nhưng không có log

3. **Log level:**
   - Một số log có thể ở level `debug` thay vì `info`/`warn`
   - → Không thấy trong logs

---

## VẤN ĐỀ VỚI LAG CALCULATION

### Code hiện tại:

```rust
// Update current proposer round from certificates aggregators
if let Some(max_round) = self.certificates_aggregators.keys().max().copied() {
    self.current_proposer_round = max_round + 1;
}

// Calculate lag
let network_current_round = current_round.max(self.highest_network_round);
let lag = network_current_round.saturating_sub(self.current_proposer_round);
```

### Vấn đề:

1. **`certificates_aggregators` chỉ chứa rounds có quorum:**
   - Nếu primary-0 lag, có thể không có quorum ở round cao nhất
   - → `current_proposer_round` tính sai (thấp hơn thực tế)

2. **`highest_network_round` có thể không được update đúng:**
   - Chỉ update từ `certificates_aggregators`
   - Nếu không có quorum, không update được

3. **`consensus_round` có thể thấp hơn proposer round:**
   - Consensus chậm hơn DAG
   - → `network_current_round` tính sai

---

## GIẢI PHÁP

### 1. **Sửa Lag Calculation**

**Vấn đề:** `current_proposer_round` tính từ `certificates_aggregators` → sai khi lag

**Giải pháp:** Dùng proposer round thực tế từ proposer

```rust
// Option 1: Track proposer round từ proposer
// Cần channel từ proposer đến core để báo proposer round

// Option 2: Dùng round cao nhất từ headers/certificates nhận được
// Thay vì dùng certificates_aggregators, dùng highest round từ network
```

### 2. **Thêm Logging**

**Vấn đề:** Không có log về catch-up mode

**Giải pháp:** Thêm log ở level `info`/`warn`:

```rust
info!(
    "[CATCH-UP SYNC] Periodic check - our_proposer_round: {}, network_round: {}, consensus_round: {}, lag: {}",
    self.current_proposer_round, network_current_round, current_round, lag
);
```

### 3. **Cải thiện Network Round Tracking**

**Vấn đề:** `highest_network_round` không được update đúng

**Giải pháp:** Update từ headers/certificates nhận được (đã có trong code, nhưng cần verify)

---

## KẾT LUẬN

### Tình trạng:
- ✅ Primary-0 **đang lag 37 rounds** so với Primary-1
- ❌ Primary-0 **chưa đuổi kịp** - lag vẫn duy trì
- ❌ Catch-up mode **không hoạt động** - không có log

### Nguyên nhân:
1. Lag calculation có thể sai (dùng `certificates_aggregators` thay vì proposer round thực tế)
2. Không có log để verify catch-up mode có hoạt động không
3. Network round tracking có thể không đúng

### Hành động cần thiết:
1. ✅ Sửa lag calculation để dùng proposer round thực tế
2. ✅ Thêm logging để verify catch-up mode
3. ✅ Cải thiện network round tracking

---

## METRICS CẦN THEO DÕI

1. **Proposer Round per Primary**: So sánh proposer round của tất cả primaries
2. **Lag Distribution**: Phân bố lag giữa các primaries
3. **Catch-Up Mode Events**: Số lần vào/ra catch-up mode
4. **Catch-Up Duration**: Thời gian để bắt kịp

