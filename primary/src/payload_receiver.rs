// Copyright(C) Facebook, Inc. and its affiliates.
use crate::primary::PayloadCache; // <--- THÊM USE
use config::WorkerId;
use crypto::Digest;
use store::Store;
use tokio::sync::mpsc::Receiver;

pub struct PayloadReceiver {
    store: Store,
    cache: PayloadCache, // <--- THÊM TRƯỜNG CACHE
    rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
}

impl PayloadReceiver {
    pub fn spawn(
        store: Store,
        cache: PayloadCache, // <--- NHẬN CACHE
        rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
    ) {
        tokio::spawn(async move {
            Self {
                store,
                cache,
                rx_workers,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // QUAN TRỌNG: Tiếp tục xử lý và ghi batches vào store cho đến khi channel đóng hoàn toàn
        // Đảm bảo analyze() có đủ batch data trong store khi đọc
        // Channel sẽ đóng khi Primary shutdown, nhưng phải đảm bảo tất cả batches đã nhận đều được ghi vào store
        while let Some((digest, _worker_id, batch)) = self.rx_workers.recv().await {
            // Ghi vào cache (nhanh)
            self.cache.insert(digest.clone(), batch.clone());

            // Ghi vào store để lưu trữ lâu dài (chậm)
            // analyze() sẽ đọc từ store này để lấy batch data
            self.store.write(digest.to_vec(), batch).await;
        }
    }
}
