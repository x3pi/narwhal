# Phân tích An toàn Fork cho Logic Tối ưu Bullshark

## Vấn đề tiềm ẩn

Logic tối ưu commit tất cả certificates có trong DAG, không chỉ trong path của leader. Điều này có thể gây fork nếu:

1. **DAG khác nhau giữa các nodes**: Node A có certificate round 443, nhưng Node B chưa nhận được (do network delay)
2. **Commit không đồng bộ**: Node A commit certificate 443, Node B không commit → Fork!

## Phân tích Logic Hiện tại

### Điều kiện An toàn Hiện tại

Logic tối ưu có các điều kiện:
```rust
1. Certificate phải có trong DAG (đã được certified với 2f+1 votes)
2. Certificate từ round TRƯỚC leader_round (đã có thời gian propagate)
3. Certificate chưa được commit trước đó
```

### Vấn đề

**Điều kiện 2 không đủ để đảm bảo tất cả nodes đều có certificate trong DAG!**

Ví dụ:
- Round 443: Certificate được tạo và certified
- Round 444: Leader round commit
- Node A: Đã nhận certificate 443 → commit
- Node B: Chưa nhận certificate 443 (network delay) → không commit
- **→ FORK!**

## Giải pháp An toàn

### Option 1: Chỉ commit certificates có trong parents của leader (AN TOÀN NHẤT)

Logic này đảm bảo chỉ commit certificates đã được "nhìn thấy" bởi leader:
- Nếu certificate có trong parents của leader, tất cả nodes commit leader đều sẽ commit nó
- An toàn 100% nhưng vẫn có delay

### Option 2: Commit certificates có trong parents của ÍT NHẤT 2f+1 certificates từ round hiện tại

Đảm bảo certificate đã được "nhìn thấy" bởi đa số nodes:
```rust
// Kiểm tra certificate có trong parents của ít nhất 2f+1 certificates từ round hiện tại
let mut supporting_votes = 0;
for (_, cert) in state.dag.get(&round)?.values() {
    if cert.header.parents.contains(&candidate_digest) {
        supporting_votes += committee.stake(&cert.origin());
    }
}
if supporting_votes >= committee.validity_threshold() {
    // Safe to commit - majority has seen it
}
```

### Option 3: Chỉ commit khi leader round có quorum (HIỆN TẠI - CẦN KIỂM TRA)

Nếu leader round chỉ commit khi có 2f+1 votes từ round hiện tại, và các certificates từ rounds trước đã có trong DAG của đa số nodes trước đó.

## Recommendation: Option 2 (BALANCED)

Kết hợp Option 2 để đảm bảo:
1. ✅ An toàn: Chỉ commit certificates đã được nhìn thấy bởi đa số (2f+1)
2. ✅ Tối ưu: Commit sớm hơn logic cũ
3. ✅ Deterministic: Tất cả nodes có cùng quyết định

