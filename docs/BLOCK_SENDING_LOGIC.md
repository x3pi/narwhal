# Logic Gửi Block Qua Unix Domain Socket

## Tổng Quan

Trong Narwhal, các block được commit từ consensus layer được gửi qua Unix Domain Socket (UDS) tới executor để xử lý. Logic này đảm bảo:

1. **Mọi round chẵn đều có block**: Trong Bullshark, chỉ round chẵn được commit làm leader. Nếu một round chẵn không được commit, vẫn cần tạo fake block rỗng để đảm bảo tính liên tục của blockchain.

2. **Gom giao dịch từ các certificate**: Các certificate được commit trong cùng một round được gom lại thành một block duy nhất.

3. **Persistent connection**: Sử dụng kết nối UDS persistent để tối ưu hiệu suất.

## Cấu Trúc Dữ Liệu

### CommittedBlock
```protobuf
message CommittedBlock {
    uint64 epoch = 1;    // Epoch hiện tại
    uint64 height = 2;    // Block height (tính từ round: height = round / 2)
    repeated Transaction transactions = 3;  // Danh sách giao dịch
}
```

### CommittedEpochData
```protobuf
message CommittedEpochData {
    repeated CommittedBlock blocks = 1;  // Có thể gửi nhiều blocks cùng lúc
}
```

## Flow Xử Lý

### 1. Nhận Committed SubDags từ Consensus

```rust
// Trong analyze_hot_swap
Ok((dags, committee, _skipped_round_option)) => {
    let message_epoch = committee.epoch;
    if !dags.is_empty() {
        finalize_and_send_epoch(
            message_epoch,
            dags,
            &mut store,
            &socket_path,
            &mut all_committed_txs_by_epoch,
            &mut last_committed_even_round_per_epoch,
            &tx_blocks,
        ).await;
    }
}
```

### 2. Xử Lý Committed SubDags

Trong `finalize_and_send_epoch`:

#### a. Xử lý từng SubDag:
- Mỗi SubDag có một leader certificate với round chẵn
- Tính `height_relative = round / 2`
- Tính `block_number_absolute = (epoch - 1) * BLOCKS_PER_EPOCH + height_relative`

#### b. Gom giao dịch từ tất cả certificates trong SubDag:
- Lặp qua tất cả certificates trong SubDag
- Đọc batches từ store cho mỗi batch digest trong payload
- Gom tất cả transactions vào `block_transactions`
- Loại bỏ duplicate transactions (dùng HashSet)

#### c. Tạo block:
```rust
blocks_to_send.push(committed_block {
    epoch,
    height: block_number_absolute,
    transactions: block_transactions,
});
```

### 3. Tạo Fake Block Rỗng Cho Round Chẵn Bị Skip

Sau khi xử lý tất cả SubDags:

```rust
// Track round chẵn lớn nhất đã commit trong batch này
let last_committed_even_round = committed_even_rounds.iter().max();
let previous_last_committed_even_round = last_committed_even_round_per_epoch
    .get(&epoch)
    .copied()
    .unwrap_or(0);

// Tạo fake block cho các round chẵn bị skip
if last_committed_even_round > previous_last_committed_even_round + 2 {
    let mut skipped_round = previous_last_committed_even_round + 2;
    while skipped_round < last_committed_even_round {
        let skipped_height_relative = skipped_round / 2;
        let skipped_block_number_absolute = 
            (epoch - 1) * BLOCKS_PER_EPOCH + skipped_height_relative;
        
        fake_blocks.push(CommittedBlock {
            epoch,
            height: skipped_block_number_absolute,
            transactions: Vec::new(), // Fake block rỗng
        });
        
        skipped_round += 2; // Chỉ xử lý round chẵn
    }
}
```

### 4. Gửi Qua UDS

#### a. Background Channel (Non-blocking):
```rust
// Tạo background channel
let (tx_blocks, mut rx_blocks) = 
    tokio::sync::mpsc::unbounded_channel::<comm::CommittedEpochData>();

// Spawn background task
tokio::spawn(async move {
    let bg_uds_sender = UdsSender::new(socket_path_bg);
    while let Some(epoch_data) = rx_blocks.recv().await {
        bg_uds_sender.send(epoch_data, "background").await;
    }
});

// Gửi blocks qua channel (non-blocking)
tx_blocks.send(epoch_data).await;
```

#### b. UDS Sender với Persistent Connection:
```rust
struct UdsSender {
    socket_path: String,
    connection: Arc<tokio::sync::Mutex<Option<UnixStream>>>,
}

impl UdsSender {
    async fn ensure_connected(&self) -> Result<(), String> {
        let mut conn_guard = self.connection.lock().await;
        if conn_guard.is_some() {
            return Ok(()); // Đã có connection
        }
        
        // Connect nếu chưa có
        let stream = UnixStream::connect(&self.socket_path).await?;
        *conn_guard = Some(stream);
        Ok(())
    }
    
    async fn send(&self, epoch_data: CommittedEpochData, context: &str) -> Result<()> {
        // Ensure connection
        self.ensure_connected().await?;
        
        // Encode Protobuf
        let mut proto_buf = BytesMut::new();
        epoch_data.encode(&mut proto_buf)?;
        
        // Write length (4 bytes)
        let len = proto_buf.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        
        // Write payload
        stream.write_all(&proto_buf).await?;
        
        // Retry logic nếu connection bị broken
        // ...
    }
}
```

## Cải Thiện Đề Xuất

### Vấn Đề Hiện Tại:

1. **Không đảm bảo tính liên tục**: Fake block chỉ được tạo khi có batch commit mới. Nếu không có commit trong một khoảng thời gian dài, các round chẵn ở giữa sẽ không có block.

2. **Không theo dõi round hiện tại**: Không biết round hiện tại của consensus, nên không thể tạo fake block cho các round chẵn đã qua nhưng chưa commit.

### Giải Pháp:

1. **Tracking Round Hiện Tại**: 
   - Theo dõi `current_round` từ consensus state
   - Khi nhận batch commit mới, kiểm tra và tạo fake block cho tất cả round chẵn từ `last_committed_even_round + 2` đến `current_round - 2`

2. **Periodic Fake Block Generation**:
   - Có thể thêm timer để định kỳ kiểm tra và tạo fake block cho các round chẵn còn thiếu

3. **Đảm Bảo Tính Liên Tục**:
   - Luôn đảm bảo `last_committed_even_round_per_epoch` được cập nhật đúng
   - Tạo fake block ngay khi phát hiện gap

## Ví Dụ

### Scenario:
- Epoch 1
- Round 100 được commit (height = 50)
- Round 104 được commit (height = 52)
- Round 102 không được commit

### Blocks được tạo:
1. Block height 50: Chứa transactions từ round 100
2. Block height 51: **Fake block rỗng** (từ round 102)
3. Block height 52: Chứa transactions từ round 104

Tất cả được gửi trong một `CommittedEpochData` message qua UDS.

## Kết Luận

Logic hiện tại đã xử lý tốt việc:
- Gom giao dịch từ các certificate thành block
- Tạo fake block rỗng cho round chẵn bị skip
- Gửi qua UDS với persistent connection

Cần cải thiện:
- Tracking round hiện tại để đảm bảo tính liên tục hoàn toàn
- Xử lý trường hợp không có commit trong thời gian dài

