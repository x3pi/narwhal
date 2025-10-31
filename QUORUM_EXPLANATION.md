# Giải thích về Quorum và Ảnh hưởng đến Tạo Block

## 1. Các loại Quorum Threshold

### A. Quorum Threshold (cho Certificates)

**Static Quorum:**
```rust
quorum_threshold() = 2 * total_votes / 3 + 1
```
- Với 5 nodes (mỗi node stake = 1): `2 * 5 / 3 + 1 = 4`
- Đây là **2f+1 quorum** (f = số node có thể fail)
- Dùng để form certificates từ votes

**Dynamic Quorum:**
```rust
quorum_threshold_dynamic(active_cert_count) = 2 * active_cert_count / 3 + 1
```
- Với 5 nodes active: `2 * 5 / 3 + 1 = 4`
- Với 4 nodes active: `2 * 4 / 3 + 1 = 3`
- Với 3 nodes active: `2 * 3 / 3 + 1 = 3`
- Dùng để form certificates khi có nodes offline

### B. Validity Threshold (cho Commit Leaders)

```rust
validity_threshold() = (total_votes + 2) / 3
```
- Với 5 nodes (mỗi node stake = 1): `(5 + 2) / 3 = 2`
- Đây là **f+1 quorum** (ít hơn quorum threshold)
- Dùng để commit leaders trong Bullshark consensus

## 2. Tình trạng hiện tại từ Log

### Vấn đề:
```
[Bullshark][E11] Committing leader at round 286 with stake 2/2  ✅
[Bullshark][E11] Leader at round 288 has insufficient stake (0/2)  ❌
[Bullshark][E11] Leader at round 436 has insufficient stake (1/2)  ❌
[Bullshark][E11] Committing leader at round 296 with stake 2/2  ✅
[Bullshark][E11] Leader at round 298 has insufficient stake (0/2)  ❌
```

### Phân tích:
- **Validity threshold = 2** (cần ít nhất 2 stake để commit)
- Nhiều leaders chỉ có **0/2** hoặc **1/2** stake → không commit được
- Chỉ commit được khi có đủ **2/2** stake
- Điều này gây **gián đoạn** vì nhiều round chẵn bị skip

## 3. Ảnh hưởng đến Quá trình Tạo Block

### Vấn đề:
1. **Commit không đều:** Nhiều round chẵn không commit được → blocks không được tạo đều
2. **Gián đoạn:** Khi validity threshold = 2 và chỉ có 1-2 nodes active, không đủ để commit
3. **Lag:** Cần đợi đến khi có đủ support stake từ round tiếp theo

### Giải pháp đã implement:
- **Lookahead support:** Cho phép tìm support từ các round tiếp theo (tối đa 3 rounds)
- Nhưng vẫn cần **đủ validity threshold (2)** để commit

## 4. Đề xuất Cải thiện

### Option 1: Dynamic Validity Threshold
```rust
fn validity_threshold_dynamic(&self, active_count: usize) -> Stake {
    if active_count == 0 {
        return self.validity_threshold();
    }
    let active = active_count.min(self.authorities.len());
    (active + 2) / 3  // f+1 với f dựa trên active nodes
}
```

### Option 2: Giảm Validity Threshold khi có ít nodes
```rust
fn validity_threshold(&self) -> Stake {
    let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
    let threshold = (total_votes + 2) / 3;
    // Cho phép threshold thấp hơn khi có ít nodes
    threshold.max(1)  // Tối thiểu là 1
}
```

### Option 3: Giữ nguyên nhưng tăng lookahead rounds
- Tăng `max_lookahead_rounds` từ 3 lên 5-10
- Cho phép commit với support từ các round xa hơn

## 5. Kết luận

**Quorum hiện tại:**
- **Quorum threshold (certificates):** 4 (static) hoặc dynamic (3-4)
- **Validity threshold (commit):** 2 (fixed)

**Vấn đề:**
- Validity threshold = 2 **quá cao** khi chỉ có 2-3 nodes active
- Gây gián đoạn commit và tạo block không đều

**Giải pháp:**
- Implement dynamic validity threshold hoặc giảm threshold khi có ít nodes
- Hoặc tăng lookahead rounds để tìm support từ xa hơn

