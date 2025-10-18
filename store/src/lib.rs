// Copyright(C) Facebook, Inc. and its affiliates.
use rocksdb::IteratorMode;
use std::collections::{HashMap, VecDeque};
use thiserror::Error;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot; // <-- Cần thêm 'thiserror' vào Cargo.toml

#[cfg(test)]
#[path = "tests/store_tests.rs"]
pub mod store_tests;

// --- BẮT ĐẦU SỬA ĐỔI: Định nghĩa một Error Enum tùy chỉnh ---
#[derive(Error, Debug)]
pub enum StoreError {
    #[error("Lỗi truy cập cơ sở dữ liệu: {0}")]
    DBError(#[from] rocksdb::Error),

    #[error("Không tìm thấy column family '{0}'")]
    ColumnFamilyNotFound(String),

    #[error("Không nhận được phản hồi từ store actor: {0}")]
    ReceiverError(#[from] tokio::sync::oneshot::error::RecvError),
}
// --- KẾT THÚC SỬA ĐỔI ---

type StoreResult<T> = Result<T, StoreError>;

type Key = Vec<u8>;
type Value = Vec<u8>;

// --- CẬP NHẬT: Bổ sung hằng số và lệnh mới ---
pub const ROUND_INDEX_CF: &str = "round_index";
/// Tên của column family cho các batch đang chờ được đồng thuận.
pub const PENDING_BATCHES_CF: &str = "pending_batches";

/// Danh sách tên của tất cả các column families.
pub const CFS: &[&str] = &["default", ROUND_INDEX_CF, PENDING_BATCHES_CF];

pub enum StoreCommand {
    Write(Key, Value),
    Read(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    NotifyRead(Key, oneshot::Sender<StoreResult<Value>>),
    WriteCf(String, Key, Value),
    ReadCf(String, Key, oneshot::Sender<StoreResult<Option<Value>>>),
    /// Lệnh mới để xóa một key trong một column family được chỉ định.
    DeleteCf(String, Key),
    /// Lệnh mới để lặp qua tất cả các key-value trong một column family.
    IterCf(String, oneshot::Sender<StoreResult<Vec<(Key, Value)>>>),
}
// --- KẾT THÚC CẬP NHẬT ---

#[derive(Clone)]
pub struct Store {
    channel: Sender<StoreCommand>,
}

impl Store {
    pub fn new(path: &str) -> StoreResult<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Mở DB với tất cả các CF đã được định nghĩa.
        let db = rocksdb::DB::open_cf(&options, path, CFS)?;

        let mut obligations = HashMap::<_, VecDeque<oneshot::Sender<_>>>::new();
        let (tx, mut rx) = channel(100);

        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    StoreCommand::Write(key, value) => {
                        let _ = db.put(&key, &value);
                        if let Some(mut senders) = obligations.remove(&key) {
                            while let Some(s) = senders.pop_front() {
                                let _ = s.send(Ok(value.clone()));
                            }
                        }
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = db.get(&key).map_err(StoreError::from);
                        let _ = sender.send(response);
                    }
                    StoreCommand::NotifyRead(key, sender) => match db.get(&key) {
                        Ok(None) => obligations
                            .entry(key)
                            .or_insert_with(VecDeque::new)
                            .push_back(sender),
                        Ok(Some(value)) => {
                            let _ = sender.send(Ok(value));
                        }
                        Err(e) => {
                            let _ = sender.send(Err(e.into()));
                        }
                    },
                    StoreCommand::WriteCf(cf_name, key, value) => {
                        if let Some(handle) = db.cf_handle(&cf_name) {
                            let _ = db.put_cf(handle, &key, &value);
                        }
                    }
                    // --- BẮT ĐẦU SỬA ĐỔI: Xử lý lỗi một cách chính xác ---
                    StoreCommand::ReadCf(cf_name, key, sender) => {
                        let response = if let Some(handle) = db.cf_handle(&cf_name) {
                            db.get_cf(handle, &key).map_err(StoreError::from)
                        } else {
                            Err(StoreError::ColumnFamilyNotFound(cf_name))
                        };
                        let _ = sender.send(response);
                    }
                    StoreCommand::DeleteCf(cf_name, key) => {
                        if let Some(handle) = db.cf_handle(&cf_name) {
                            let _ = db.delete_cf(handle, &key);
                        }
                    }
                    StoreCommand::IterCf(cf_name, sender) => {
                        let response = if let Some(handle) = db.cf_handle(&cf_name) {
                            let iter = db.iterator_cf(handle, IteratorMode::Start);
                            let results: Vec<(Key, Value)> = iter
                                .filter_map(|res| res.ok())
                                .map(|(k, v)| (k.into_vec(), v.into_vec()))
                                .collect();
                            Ok(results)
                        } else {
                            Err(StoreError::ColumnFamilyNotFound(cf_name))
                        };
                        let _ = sender.send(response);
                    } // --- KẾT THÚC SỬA ĐỔI ---
                }
            }
        });
        Ok(Self { channel: tx })
    }

    pub async fn write(&mut self, key: Key, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::Write(key, value)).await {
            panic!("Failed to send Write command to store: {}", e);
        }
    }

    // --- BẮT ĐẦU SỬA ĐỔI: Cập nhật các hàm public để xử lý lỗi tốt hơn ---
    pub async fn read(&mut self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::Read(key, sender)).await {
            panic!("Failed to send Read command to store: {}", e);
        }
        receiver.await?
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
        receiver.await?
    }

    pub async fn write_cf(&mut self, cf_name: String, key: Key, value: Value) {
        let command = StoreCommand::WriteCf(cf_name, key, value);
        if let Err(e) = self.channel.send(command).await {
            panic!("Failed to send WriteCf command to store: {}", e);
        }
    }

    pub async fn read_cf(&mut self, cf_name: String, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        let command = StoreCommand::ReadCf(cf_name, key, sender);
        if let Err(e) = self.channel.send(command).await {
            panic!("Failed to send ReadCf command to store: {}", e);
        }
        receiver.await?
    }

    /// Xóa một key khỏi column family được chỉ định.
    pub async fn delete_cf(&mut self, cf_name: String, key: Key) {
        let command = StoreCommand::DeleteCf(cf_name, key);
        if let Err(e) = self.channel.send(command).await {
            panic!("Failed to send DeleteCf command to store: {}", e);
        }
    }

    /// Lặp qua tất cả các cặp key-value trong một column family.
    pub async fn iter_cf(&mut self, cf_name: String) -> StoreResult<Vec<(Key, Value)>> {
        let (sender, receiver) = oneshot::channel();
        let command = StoreCommand::IterCf(cf_name, sender);
        if let Err(e) = self.channel.send(command).await {
            panic!("Failed to send IterCf command to store: {}", e);
        }
        receiver.await?
    }
    // --- KẾT THÚC SỬA ĐỔI ---
}
