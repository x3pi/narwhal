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
        let db = rocksdb::DB::open_default(path)?;
        //HashMap này sẽ được dùng để theo dõi các yêu cầu NotifyRead đang chờ dữ liệu. Hashmap lưu nhiều quue[oneshoot]
        let mut obligations = HashMap::<_, VecDeque<oneshot::Sender<_>>>::new();
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    //Ghi cặp (key, value) vào RocksDB.
                    // Kiểm tra xem có yêu cầu NotifyRead nào đang chờ key này trong obligations không.
                    // Nếu có, nó sẽ gửi value vừa được ghi cho tất cả những người đang chờ thông qua các kênh oneshot của họ và xóa key khỏi obligations.
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
}
