# CẢI THIỆN: LEADER CÓ THỂ COMMIT BATCHES TỪ CÁC PRIMARIES KHÁC

## MỤC TIÊU

Cải thiện leader để có thể commit batches từ các primaries khác, đảm bảo:
1. **Không fork**: Tất cả node xử lý giống nhau (deterministic)
2. **Không trùng lặp**: Batch không được commit 2 lần
3. **Không bỏ rơi**: Batch cuối cùng sẽ được commit

## GIẢI PHÁP ĐÃ ÁP DỤNG

### 1. Extract Batches từ Parent Certificates

Khi proposer nhận được parents (certificates từ round trước), nó sẽ:
- Đọc các parent certificates từ store
- Extract batches từ payload của các certificates
- Thêm các batches chưa commit vào queue

### 2. Logic Extract

```rust
async fn extract_batches_from_parents(&mut self, parent_digests: &[Digest], parent_round: Round) {
    for parent_digest in parent_digests {
        // Read certificate from store
        if let Ok(Some(bytes)) = self.store.read(parent_digest.to_vec()).await {
            if let Ok(certificate) = bincode::deserialize::<Certificate>(&bytes) {
                // Extract batches from certificate header payload
                for (batch_digest, worker_id) in certificate.header.payload.iter() {
                    // Skip if already committed
                    if self.committed_digests.contains_key(batch_digest) {
                        continue;
                    }
                    
                    // CRITICAL: Check if batch is already in queue
                    // If batch is in Pending state, skip extraction (will be included in next header)
                    // If batch is in InFlight state, convert to Pending to allow immediate inclusion
                    let already_in_queue = self.digests.iter().any(|entry| entry.digest == *batch_digest);
                    if already_in_queue {
                        let is_pending = self.digests.iter()
                            .any(|entry| entry.digest == *batch_digest 
                                && matches!(entry.state, BatchState::Pending));
                        if is_pending {
                            continue; // Skip if already in Pending state
                        }
                        // Convert InFlight → Pending to allow immediate inclusion
                        for entry in self.digests.iter_mut() {
                            if entry.digest == *batch_digest 
                                && matches!(entry.state, BatchState::InFlight { .. }) {
                                self.pending_payload_size += entry.size;
                                entry.state = BatchState::Pending;
                                break;
                            }
                        }
                        continue; // Skip adding new entry since we converted existing one
                    }
                    
                    // Try to read batch from store
                    if let Ok(Some(_)) = self.store.read(batch_digest.to_vec()).await {
                        // Add to queue as Pending
                        self.digests.push_back(BatchEntry {
                            digest: batch_digest.clone(),
                            worker_id: *worker_id,
                            size: batch_digest.size(),
                            state: BatchState::Pending,
                            retry_count: 0,
                        });
                        self.pending_payload_size += size;
                    }
                }
            }
        }
    }
}
```

### 3. Đảm bảo Không Fork

- **Deterministic**: Tất cả node nhận cùng parent certificates (quorum)
- **Deterministic extraction**: Tất cả node extract batches từ cùng parent certificates
- **Deterministic inclusion**: Leader include batches theo cùng thứ tự (FIFO queue)

### 4. Đảm bảo Không Trùng Lặp

- **Check committed**: Skip batches đã commit (`committed_digests`)
- **Check queue**: Skip batches đã có trong queue
- **Check trong header**: Logic hiện tại đã có deduplication trong `make_header`

### 5. Đảm bảo Không Bỏ Rơi

- **Extract từ parents**: Batches từ parent certificates được thêm vào queue
- **Leader include**: Leader sẽ include batches này trong header của mình
- **Commit**: Khi leader là leader, batches sẽ được commit

## CÁCH HOẠT ĐỘNG

### Scenario 1: Batch từ Primary A, Leader là Primary B

1. **Round N**: Primary A tạo header với batch X
2. **Round N+1**: 
   - Primary B (leader) nhận parent certificates (bao gồm certificate từ Primary A)
   - Primary B extract batch X từ parent certificate
   - Primary B thêm batch X vào queue
   - Primary B tạo header với batch X
   - Certificate của Primary B được commit → batch X được commit

### Scenario 2: Batch từ Primary A, Leader là Primary A

1. **Round N**: Primary A tạo header với batch X
2. **Round N+1**:
   - Primary A (leader) nhận parent certificates
   - Primary A extract batch X từ parent certificate (hoặc đã có trong queue từ worker)
   - Primary A tạo header với batch X
   - Certificate của Primary A được commit → batch X được commit

## LỢI ÍCH

1. **Faster commit**: Batch được commit nhanh hơn vì leader có thể include batches từ primaries khác
2. **Reduced retry**: Ít cần retry hơn vì batch được commit sớm hơn
3. **Better throughput**: Hệ thống mượt mà hơn, không phải chờ retry

## HẠN CHẾ

1. **Batch phải có trong store**: Nếu batch chưa được sync tới store của leader, nó sẽ không được include
   - **Giải pháp**: Synchronizer sẽ sync batch sau, và retry mechanism sẽ đảm bảo batch được commit

2. **Network latency**: Nếu parent certificates đến muộn, batches có thể không được extract kịp
   - **Giải pháp**: Retry mechanism vẫn hoạt động như backup

## KẾT LUẬN

Cơ chế này giúp leader commit batches từ các primaries khác nhanh hơn, giảm nhu cầu retry và làm cho hệ thống mượt mà hơn. Tuy nhiên, retry mechanism vẫn cần thiết như một backup để đảm bảo batch cuối cùng sẽ được commit.

