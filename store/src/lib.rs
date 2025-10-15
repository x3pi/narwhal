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

// --- BEGIN: Bổ sung hằng số và lệnh mới ---
/// Tên của column family dùng để tạo chỉ mục round -> [digest].
pub const ROUND_INDEX_CF: &str = "round_index";

pub enum StoreCommand {
    Write(Key, Value),
    Read(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    NotifyRead(Key, oneshot::Sender<StoreResult<Value>>),
    // Lệnh để ghi vào một column family được chỉ định.
    WriteCf(String, Key, Value),
    // Lệnh để đọc từ một column family được chỉ định.
    ReadCf(String, Key, oneshot::Sender<StoreResult<Option<Value>>>),
}
// --- END: Bổ sung hằng số và lệnh mới ---

#[derive(Clone)]
pub struct Store {
    channel: Sender<StoreCommand>,
}

impl Store {
    pub fn new(path: &str) -> StoreResult<Self> {
        // --- BEGIN: Mở DB với hỗ trợ Column Family ---
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let db = rocksdb::DB::open_cf(&options, path, vec!["default", ROUND_INDEX_CF])?;
        // --- END: Mở DB với hỗ trợ Column Family ---

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
                        let response = db.get(&key);
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
                            let _ = sender.send(Err(e));
                        }
                    },
                    // --- BEGIN: Xử lý các lệnh mới cho Column Family ---
                    StoreCommand::WriteCf(cf_name, key, value) => {
                        let handle = db
                            .cf_handle(&cf_name)
                            .unwrap_or_else(|| panic!("Column family '{}' not found", cf_name));
                        let _ = db.put_cf(handle, &key, &value);
                        // Lưu ý: NotifyRead chưa được implement cho CF để giữ code đơn giản.
                    }
                    StoreCommand::ReadCf(cf_name, key, sender) => {
                        let handle = db
                            .cf_handle(&cf_name)
                            .unwrap_or_else(|| panic!("Column family '{}' not found", cf_name));
                        let response = db.get_cf(handle, &key);
                        let _ = sender.send(response);
                    } // --- END: Xử lý các lệnh mới ---
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

    // --- BEGIN: Bổ sung các hàm public mới ---
    /// Ghi một cặp key-value vào một column family được chỉ định.
    pub async fn write_cf(&mut self, cf_name: String, key: Key, value: Value) {
        let command = StoreCommand::WriteCf(cf_name, key, value);
        if let Err(e) = self.channel.send(command).await {
            panic!("Failed to send WriteCf command to store: {}", e);
        }
    }

    /// Đọc một value từ một column family được chỉ định.
    pub async fn read_cf(&mut self, cf_name: String, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        let command = StoreCommand::ReadCf(cf_name, key, sender);
        if let Err(e) = self.channel.send(command).await {
            panic!("Failed to send ReadCf command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to ReadCf command from store")
    }
    // --- END: Bổ sung các hàm public mới ---
}
