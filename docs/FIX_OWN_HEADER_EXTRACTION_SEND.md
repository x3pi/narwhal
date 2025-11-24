# FIX: GỬI OWN HEADERS ĐẾN PROPOSER ĐỂ EXTRACT BATCHES

## VẤN ĐỀ

Giao dịch `4dd0f42a5755f6c1f62492ca0065645c7fa7f137dde8a3ca312915afbc6b9068` và tất cả giao dịch sau đó từ worker-0 không được thực thi.

**Nguyên nhân gốc rễ:**
- Batch `RNuqowuuqkc6L5Boa2EX1HK7PitjTgmWKP6UDSV8PY0=` bị stuck trong `InFlight` state
- Primary-0 tạo headers liên tục với batch này nhưng không được commit
- Khi primary-0 là leader, nó không thể extract batch này từ own headers vì **own headers không được gửi đến proposer để extract**

## PHÂN TÍCH CODE

### Trước Khi Sửa

**File: `primary/src/core.rs` (dòng 877-892)**

```rust
if header.author != self.name {
    // BATCH TRACKING: Log when sending header to proposer for batch extraction
    info!(
        "[BATCH TRACK CORE] Core {} sending header {} (round {}, author: {}) to proposer for batch extraction...",
        ...
    );
    if let Err(e) = self.tx_headers.send(header.clone()).await {
        ...
    }
}
```

**Vấn đề:**
- Chỉ gửi headers của các primary khác (`header.author != self.name`)
- Own headers không được gửi đến proposer
- Khi primary-0 là leader, nó không thể extract batches từ own headers

### Logic Extract Đã Có Sẵn

**File: `primary/src/proposer.rs` (dòng 1047-1092)**

Logic extract đã được thiết kế để xử lý own headers:

```rust
if header.author == self.name {
    // Check if all batches in this own header are either:
    // 1. Already committed, OR
    // 2. Already in queue in Pending state (not InFlight)
    let all_batches_safe = header.payload.iter().all(|(batch_digest, _)| {
        // ...
        if matches!(entry.state, BatchState::InFlight { .. }) {
            return false; // Not safe to skip - need to process
        }
        // ...
    });
    
    if all_batches_safe {
        return; // Skip extraction
    } else {
        // Continue to extract - convert InFlight to Pending
    }
}
```

**Logic này đã đúng:**
- Chỉ extract từ own headers nếu batches ở `InFlight` state
- Convert `InFlight` -> `Pending` để allow immediate inclusion
- Check `committed_digests` để prevent duplicates

**Nhưng vấn đề là:** Own headers không được gửi đến proposer, nên logic này không bao giờ được gọi!

## GIẢI PHÁP

### Sau Khi Sửa

**File: `primary/src/core.rs` (dòng 866-895)**

```rust
// OPTIMIZATION: Send header to proposer EARLY (right after signature verification)
// This allows proposer to extract batches immediately, reducing delay from
// hundreds of ms to just a few ms. This prevents batches from being stuck
// and reduces retry count significantly.
//
// CRITICAL FIX: Also send OWN headers to proposer for batch extraction.
// This allows leader to extract batches from own headers when they're in InFlight state,
// preventing batches from being stuck when own certificate is not committed.
// SAFETY: extract_batches_from_headers already handles own headers correctly:
// - It only extracts if batches are in InFlight state (not Pending or Committed)
// - It converts InFlight -> Pending to allow immediate inclusion
// - It checks committed_digests to prevent duplicates
//
// SAFETY: Signature is already verified, so this is safe. Even if header
// is later found to be invalid (e.g., missing parents), batch extraction
// is safe because:
// 1. Batch will only be added to queue, not committed immediately
// 2. Batch will be checked again when creating header
// 3. Invalid headers won't be committed anyway
// BATCH TRACKING: Log when sending header to proposer for batch extraction
info!(
    "[BATCH TRACK CORE] Core {} sending header {} (round {}, author: {}) to proposer for batch extraction. Header contains {} batches: {:?}",
    self.name,
    header.id,
    header.round,
    header.author,
    header.payload.len(),
    header.payload.keys().take(5).collect::<Vec<_>>()
);
if let Err(e) = self.tx_headers.send(header.clone()).await {
    // Channel closed or full - not critical, just log
    debug!("Failed to send header {} to proposer for early batch extraction: {}", header.id, e);
}
```

**Thay đổi:**
- **Bỏ điều kiện `if header.author != self.name`** - Gửi tất cả headers (bao gồm own headers) đến proposer
- **Thêm comment giải thích** - Tại sao gửi own headers là an toàn và cần thiết

## AN TOÀN

### Tại Sao An Toàn?

1. **Logic Extract Đã Xử Lý Own Headers:**
   - Chỉ extract nếu batches ở `InFlight` state
   - Skip nếu batches đã `Committed` hoặc `Pending`
   - Check `committed_digests` để prevent duplicates

2. **Không Gây Duplicate:**
   - Extract chỉ convert `InFlight` -> `Pending`
   - Không add batch mới nếu đã có trong queue
   - Check `committed_digests` trước khi add

3. **Không Gây Fork:**
   - Extract chỉ thay đổi state của batch trong queue
   - Không thay đổi header đã tạo
   - Header vẫn được broadcast và vote như bình thường

## KẾT QUẢ MONG ĐỢI

1. **Own Headers Được Gửi Đến Proposer:**
   - Khi primary-0 tạo header, header được gửi đến proposer
   - Proposer có thể extract batches từ own headers

2. **Batches Không Bị Stuck:**
   - Khi primary-0 là leader, nó extract batches từ own headers
   - Convert `InFlight` -> `Pending` để include trong header mới
   - Batches được commit nhanh hơn

3. **Giao Dịch Được Thực Thi:**
   - Giao dịch `4dd0f42a5755f6c1f62492ca0065645c7fa7f137dde8a3ca312915afbc6b9068` được commit
   - Các giao dịch sau đó cũng được commit bình thường

## TESTING

**Kiểm tra logs sau khi fix:**
1. `[BATCH TRACK CORE] Core ... sending header ... (author: AqJy7eip40qqZk7F)` - Own headers được gửi
2. `[BATCH EXTRACTION] Primary ... PROCESSING own header ... - contains InFlight batches` - Own headers được extract
3. `[BATCH EXTRACTION] Primary ... CONVERTED batch ... from InFlight to Pending` - Batches được convert
4. `[BATCH COMMIT]` - Batches được commit

## LƯU Ý

- Fix này chỉ hoạt động khi primary là leader
- Nếu primary không phải leader, batches vẫn có thể bị stuck cho đến khi primary là leader
- Retry logic vẫn cần thiết để handle trường hợp primary không phải leader trong thời gian dài

