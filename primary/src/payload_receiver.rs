// Copyright(C) Facebook, Inc. and its affiliates.
use crate::primary::PayloadCache; // <--- THÊM USE
use config::WorkerId;
use crypto::Digest;
use store::Store;
use tokio::sync::mpsc::Receiver;

pub struct PayloadReceiver {
    store: Store,
    cache: PayloadCache,
    rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
}

impl PayloadReceiver {
    pub fn spawn(
        store: Store,
        cache: PayloadCache,
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
        while let Some((digest, _worker_id, batch)) = self.rx_workers.recv().await {
            // Ghi vào cache (nhanh)
            self.cache.insert(digest.clone(), batch.clone());

            // Ghi vào store để lưu trữ lâu dài (chậm)
            self.store.write(digest.to_vec(), batch).await;
        }
    }
}
