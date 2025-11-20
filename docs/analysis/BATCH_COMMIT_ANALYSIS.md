# PHÂN TÍCH VẤN ĐỀ: BATCH KHÔNG ĐƯỢC COMMIT NGAY TỪ ĐẦU

## VẤN ĐỀ

Batch `Nhg8bVipFRsy3D/8gg68eJXPgQjbwdE6c+pA+Flhr3s=` không được commit ở round 216, phải retry nhiều lần.

## TIMELINE

### Round 216 (16:51:18.499Z)
1. ✅ **Header được tạo**: `Created B216(AqJy7eip40qqZk7F) -> Nhg8bVipFRsy3D/8gg68eJXPgQjbwdE6c+pA+Flhr3s=`
2. ✅ **Header được gửi tới Core**: Header được gửi từ Proposer tới Core
3. ✅ **Core chuẩn bị header round 216**: `Core: preparing header round 216 with 4 parents`
4. ❌ **Certificate KHÔNG được gửi tới consensus**: Không thấy log "Sending certificate" cho certificate round 216 từ primary `AqJy7eip40qqZk7F`
5. ✅ **Certificate round 216 từ primary khác được commit**: `{round: 216, origin: ApvX+ZCVrGWssP/vAcNbnOL25dkQ/ZURCiKZrBFJhFrQ, digest: XS8p0T/MUjijaayC7xHK6KinkBCK/OOSbS8H9irY06k=}`

## NGUYÊN NHÂN GỐC RỄ

### Vấn đề 1: Header chưa được vote đủ để tạo certificate
- Header được tạo và gửi tới Core
- Nhưng để tạo certificate, header cần được vote bởi quorum (2/5 nodes trong trường hợp này)
- Nếu header chưa được vote đủ, certificate sẽ không được tạo
- Certificate không được tạo → không được gửi tới consensus → không được commit

### Vấn đề 2: Certificate được tạo nhưng không được gửi tới consensus
- Có thể do check ở `core.rs` line 339-347:
  ```rust
  if certificate.round() <= state.last_committed_round {
      debug!("Certificate {} already committed, skipping consensus", ...);
      return Ok(());
  }
  ```
- Nếu `last_committed_round` đã >= 216, certificate sẽ bị skip

### Vấn đề 3: Certificate được gửi nhưng không được commit bởi consensus
- Consensus chỉ commit certificate của leader
- Leader round 216 là `ApvX+ZCVrGWssP/vAcNbnOL25dkQ/ZURCiKZrBFJhFrQ`
- Certificate từ primary `AqJy7eip40qqZk7F` không phải leader → không được commit

## GIẢI PHÁP

### Giải pháp 1: Đảm bảo header được vote đủ nhanh
- Cải thiện network latency
- Tối ưu hóa quá trình vote
- Đảm bảo header được broadcast nhanh chóng

### Giải pháp 2: Retry mechanism (đã implement)
- Nếu batch không được commit, retry ở round tiếp theo
- Đây là giải pháp hiện tại, nhưng không lý tưởng vì:
  - Batch phải chờ retry
  - Tốn thêm network bandwidth
  - Không mượt mà

### Giải pháp 3: Cải thiện leader selection
- Đảm bảo tất cả primaries đều có cơ hội làm leader
- Hoặc đảm bảo batch được include trong certificate của leader

## KẾT LUẬN

**Vấn đề chính**: Certificate round 216 từ primary `AqJy7eip40qqZk7F` không được commit vì:
1. Certificate không được gửi tới consensus (có thể do chưa được vote đủ)
2. Hoặc certificate được gửi nhưng không phải leader → không được commit

**Giải pháp hiện tại (retry)**: Hoạt động nhưng không lý tưởng vì batch phải chờ retry.

**Giải pháp tốt hơn**: Cần cải thiện quá trình vote và leader selection để batch được commit ngay từ đầu.

