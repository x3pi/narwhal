# Đề xuất tối ưu Bullshark để commit batches sớm hơn

## Vấn đề hiện tại

Header round 443 chứa batch nhưng chưa kịp được certified trước khi round 444 commit, dẫn đến delay 45 rounds.

## Nguyên nhân

1. **Timing issue**: Header được tạo ở 10:59:45.534 nhưng round 444 commit ở 10:59:45.938 (chỉ 404ms sau)
2. **Certification delay**: Header chưa kịp nhận đủ votes để trở thành certificate
3. **Commit logic**: `order_dag` chỉ commit certificates đã có trong DAG, không commit headers chưa certified

## Giải pháp đề xuất

### Option 1: Commit tất cả certificates có trong DAG của leader round

Thay vì chỉ commit certificates trong path của leader, commit tất cả certificates có trong DAG từ `last_committed_round` đến `leader_round`:

```rust
// Trong order_dag hoặc process_certificate
// Commit tất cả certificates có sẵn trong DAG
for round in (state.last_committed_round + 1)..=leader_round {
    if let Some(certificates) = state.dag.get(&round) {
        for (digest, cert) in certificates.values() {
            // Commit nếu chưa commit và có trong DAG
            if !already_committed(digest) {
                sequence.push(cert.clone());
            }
        }
    }
}
```

### Option 2: Cải thiện parent selection để include headers gần đây

Khi tạo header ở leader round, ưu tiên include certificates từ rounds gần đây làm parents:

```rust
// Trong proposer, khi chọn parents
// Ưu tiên certificates từ rounds gần đây (ví dụ: rounds 441-443)
// để đảm bảo chúng được commit sớm
```

### Option 3: Thêm logic commit "catch-up" cho certificates bị miss

Sau khi commit leader round, kiểm tra và commit các certificates từ rounds trước đó đã có trong DAG nhưng chưa được commit:

```rust
// Sau khi commit leader round
// Tìm và commit các certificates bị miss từ rounds trước
for round in (state.last_committed_round + 1)..leader_round {
    commit_missed_certificates(round, &state.dag);
}
```

### Option 4: Cải thiện broadcast/vote timing

Đảm bảo headers được broadcast ngay lập tức và nodes vote nhanh hơn trong môi trường local:

- Giảm network delay simulation
- Tăng priority cho vote messages
- Batch votes để giảm overhead

## Recommendation

**Kết hợp Option 1 và Option 3**: Commit tất cả certificates có sẵn trong DAG, không chỉ trong path của leader. Điều này đảm bảo:

1. ✅ Batches được commit sớm nhất có thể
2. ✅ Không cần đợi đến leader round của node tạo batch
3. ✅ Tận dụng tối đa việc lan truyền DAG trên mạng
4. ✅ Vẫn đảm bảo tính nhất quán (consistency) của Bullshark

## Trade-offs

- **Latency**: Giảm đáng kể (từ 45 rounds xuống ~1-2 rounds)
- **Throughput**: Không thay đổi nhiều
- **Complexity**: Tăng một chút do phải scan DAG
- **Safety**: Vẫn an toàn vì chỉ commit certificates đã có trong DAG

