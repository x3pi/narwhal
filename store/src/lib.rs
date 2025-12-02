// Copyright(C) Facebook, Inc. and its affiliates.
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "tests/store_tests.rs"]
pub mod store_tests;

pub type StoreError = rocksdb::Error;
type StoreResult<T> = Result<T, StoreError>;

type Key = Vec<u8>;
type Value = Vec<u8>;

pub enum StoreCommand {
    Write(Key, Value),
    Read(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    NotifyRead(Key, oneshot::Sender<StoreResult<Value>>),
}

#[derive(Clone)]
pub struct Store {
    channel: Sender<StoreCommand>,
}

impl Store {
    pub fn new(path: &str) -> StoreResult<Self> {
        // CRITICAL OPTIMIZATION: Tối ưu RocksDB settings cho high write throughput
        // và giảm compaction overhead
        let mut opts = rocksdb::Options::default();
        
        // 0. CRITICAL: Enable create_if_missing để tự động tạo database nếu chưa tồn tại
        // Nếu không set, sẽ lỗi "does not exist (create_if_missing is false)"
        opts.create_if_missing(true);
        
        // 1. Write Buffer Optimization - tăng buffer size để giảm số lần flush
        // Default: 64MB, tăng lên 128MB để batch nhiều writes hơn
        opts.set_write_buffer_size(128 * 1024 * 1024); // 128MB
        
        // 2. Max Write Buffer Number - cho phép nhiều memtables trước khi flush
        // Default: 2, tăng lên 4 để tăng write throughput
        opts.set_max_write_buffer_number(4);
        
        // 3. Min Write Buffer Number To Merge - số memtables tối thiểu để trigger merge
        // Default: 1, giữ nguyên để merge sớm
        opts.set_min_write_buffer_number_to_merge(1);
        
        // 4. Compaction Optimization - giảm compaction overhead
        // Leveled compaction với nhiều levels hơn để giảm write amplification
        opts.set_level_compaction_dynamic_level_bytes(true);
        
        // 5. Block Cache và Block Size - tối ưu cho reads nhanh hơn
        // Sử dụng BlockBasedTableOptions để cấu hình block cache và block size
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        
        // Block Cache - tăng cache size cho reads nhanh hơn
        // Default: 8MB, tăng lên 64MB
        let cache = rocksdb::Cache::new_lru_cache(64 * 1024 * 1024); // 64MB
        block_opts.set_block_cache(&cache);
        
        // Block Size - tối ưu cho random reads
        // Default: 4KB, tăng lên 16KB cho batch reads tốt hơn
        block_opts.set_block_size(16 * 1024); // 16KB
        
        // Apply block options
        opts.set_block_based_table_factory(&block_opts);
        
        // 7. Compression - disable compression để tăng write speed (trade-off với disk space)
        // Nếu cần tiết kiệm disk, có thể dùng Snappy hoặc LZ4
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        
        // 8. Max Background Jobs - tăng số threads cho compaction và flush
        // Default: 2, tăng lên 4 để xử lý compaction song song
        opts.set_max_background_jobs(4);
        
        // 9. Bytes Per Sync - sync ít thường xuyên hơn để tăng throughput
        // Default: 1MB, tăng lên 4MB
        opts.set_bytes_per_sync(4 * 1024 * 1024); // 4MB
        
        // 10. WAL (Write-Ahead Log) Optimization
        // Disable WAL sync mỗi write để tăng tốc (trade-off với durability)
        // Nếu cần durability cao, có thể enable lại
        opts.set_atomic_flush(false);
        
        // 11. Target File Size - tăng file size để giảm số files và compaction
        // Default: 64MB, tăng lên 128MB
        opts.set_target_file_size_base(128 * 1024 * 1024); // 128MB
        
        // 12. Max Bytes For Level Base - tăng base level size
        // Default: 256MB, tăng lên 1GB
        opts.set_max_bytes_for_level_base(1024 * 1024 * 1024); // 1GB
        
        // 13. Disable stats collection để giảm overhead (optional)
        // opts.set_stats_dump_period_sec(0);
        
        // 14. Increase parallelism for reads
        opts.set_advise_random_on_open(true);
        
        // Open database với optimized options
        let db = rocksdb::DB::open(&opts, path)?;
        //HashMap này sẽ được dùng để theo dõi các yêu cầu NotifyRead đang chờ dữ liệu. Hashmap lưu nhiều quue[oneshoot]
        let mut obligations = HashMap::<_, VecDeque<oneshot::Sender<_>>>::new();
        // CRITICAL FIX: Tăng channel capacity từ 100 lên 10,000 để tránh block khi store I/O chậm
        // Nếu channel đầy, store.write() sẽ block và làm hệ thống đứng hẳn
        // Capacity lớn hơn cho phép nhiều write requests được queue mà không block caller
        const STORE_CHANNEL_CAPACITY: usize = 10_000;
        let (tx, mut rx) = channel(STORE_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    //Ghi cặp (key, value) vào RocksDB.
                    // Kiểm tra xem có yêu cầu NotifyRead nào đang chờ key này trong obligations không.
                    // Nếu có, nó sẽ gửi value vừa được ghi cho tất cả những người đang chờ thông qua các kênh oneshot của họ và xóa key khỏi obligations.
                    StoreCommand::Write(key, value) => {
                        // CRITICAL OPTIMIZATION: Sử dụng WriteOptions với sync=false để tăng write speed
                        // Sync=false có nghĩa là không flush ngay lập tức, RocksDB sẽ flush sau
                        // Trade-off: Nếu crash, có thể mất một số writes gần đây
                        // Nhưng với blockchain, chúng ta có thể chấp nhận risk này để tăng performance
                        let mut write_opts = rocksdb::WriteOptions::default();
                        write_opts.set_sync(false); // Không sync mỗi write - tăng tốc đáng kể
                        write_opts.disable_wal(false); // Vẫn giữ WAL cho durability
                        let _ = db.put_opt(&key, &value, &write_opts);
                        if let Some(mut senders) = obligations.remove(&key) {
                            while let Some(s) = senders.pop_front() {
                                let _ = s.send(Ok(value.clone()));
                            }
                        }
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = db.get(&key);
                        let _ = sender.send(response);
                    }
                    // Cố gắng đọc key từ RocksDB.
                    // Nếu key đã tồn tại, nó sẽ gửi ngay giá trị tìm được cho người yêu cầu.
                    // Nếu key chưa tồn tại (Ok(None)), thay vì trả về None, nó sẽ lưu sender (kênh oneshot) vào obligations dưới key đó.
                    // Khi key này được ghi vào (thông qua lệnh Write), tác vụ nền sẽ tìm thấy sender này và gửi giá trị mới qua nó.
                    StoreCommand::NotifyRead(key, sender) => {
                        let response = db.get(&key);
                        match response {
                            Ok(None) => obligations
                                .entry(key)
                                .or_insert_with(VecDeque::new)
                                .push_back(sender),
                            _ => {
                                let _ = sender.send(response.map(|x| x.unwrap()));
                            }
                        }
                    }
                }
            }
        });
        Ok(Self { channel: tx })
    }

    pub async fn write(&mut self, key: Key, value: Value) {
        // CRITICAL FIX: Sử dụng try_send trước, nếu channel đầy thì fallback sang blocking send
        // Điều này giúp phát hiện sớm khi store I/O chậm và channel đầy
        match self.channel.try_send(StoreCommand::Write(key.clone(), value.clone())) {
            Ok(()) => {
                // Success - write queued
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                // Channel đầy - log warning và fallback sang blocking send
                #[cfg(not(test))]
                eprintln!("[STORE CHANNEL FULL] Store write channel is FULL! Store I/O may be slow. Falling back to blocking send. This may indicate RocksDB compaction or disk I/O bottleneck.");
                // Fallback: blocking send (sẽ đợi cho đến khi có chỗ)
                if let Err(e) = self.channel.send(StoreCommand::Write(key, value)).await {
                    #[cfg(not(test))]
                    eprintln!("[STORE WRITE ERROR] Failed to send Write command to store (blocking send also failed): {}", e);
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                #[cfg(not(test))]
                eprintln!("[STORE CHANNEL CLOSED] Store write channel is CLOSED! Store may have crashed.");
            }
        }
    }

    pub async fn read(&mut self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::Read(key, sender)).await {
            panic!("Failed to send Read command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read command from store")
    }

    pub async fn notify_read(&mut self, key: Key) -> StoreResult<Value> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self
            .channel
            .send(StoreCommand::NotifyRead(key, sender))
            .await
        {
            panic!("Failed to send NotifyRead command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to NotifyRead command from store")
    }
}
