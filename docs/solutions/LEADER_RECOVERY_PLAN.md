# LEADER RECOVERY PLAN (PHASE 3)

## Bối cảnh

- Primary-0 thường xuyên không kịp trở thành leader vì DAG bị bỏ lại phía sau.
- Batch bị retry hàng trăm lần, cuối cùng bị force remove ⇒ giao dịch mất vĩnh viễn.
- Cần một cơ chế “cứu hộ” để leader khác có thể nhận payload ngay cả khi worker gốc không kịp đồng bộ.

## Mục tiêu

1. ✅ Ngăn batch bị retry vô hạn do chỉ một primary giữ payload.
2. ✅ Đảm bảo leader bất kỳ cũng có thể include batch khi đến lượt.
3. ✅ Giữ hệ thống balanced: không để một primary bị bỏ lại dẫn tới empty blocks.

## Kế hoạch hành động

### Phase 3A – Batch Rescue Broadcast (đã triển khai trong commit này)

| Bước | Mô tả | Trạng thái |
| --- | --- | --- |
| 1 | Khi `retry_count` vượt ngưỡng (`>= 25`), proposer đánh dấu batch là “cần cứu hộ”. | ✅ |
| 2 | Proposer đọc payload từ `Store` và gửi `BatchRescue` lên `Core`. | ✅ |
| 3 | `Core` lưu payload vào `Store + PayloadCache` và broadcast `PrimaryMessage::BatchReplica` tới tất cả primaries khác. | ✅ |
| 4 | Các primary khác lưu payload cục bộ, vì vậy khi chúng nhận header chứa batch đó, sẽ include ngay, không cần đợi worker sync. | ✅ |

### Phase 3B – Proactive Sync (tiếp theo)

- **Periodic DAG scan:** `Core` sử dụng timer để kiểm tra round lag và trigger sync nếu tụt > X rounds.
- **Health metrics:** log round lag, số batch rescue, số lần force advance để theo dõi sức khỏe mạng.

### Phase 3C – Retry Policy Update

- Tăng dần backoff thay vì spam retry mỗi vòng, ưu tiên rescue/broadcast.
- Nếu batch đã rescue mà vẫn không commit trong Y rounds ⇒ cảnh báo + auto-cleanup.

## Kết quả mong đợi

- Leader bất kỳ cũng có payload ⇒ batch stuck được commit nhanh hơn.
- Primary-0 không còn bị bỏ rơi ⇒ traffic không còn toàn empty blocks.
- Giảm số lần `FORCE REMOVING stuck batch …`.

## Theo dõi

- Log mới: `[BATCH RESCUE] …` ở proposer & core.
- Metrics đề xuất:
  - `rescue_triggered_total`
  - `batch_replica_received_total`
  - `round_lag_max`

---

**Next actions:**
1. ✅ Hoàn thiện Phase 3A (đã merge).
2. ⏳ Thực hiện Phase 3B – periodic sync & health monitoring.
3. ⏳ Adjust retry/backoff logic (Phase 3C).

