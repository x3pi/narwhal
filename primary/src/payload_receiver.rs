// Copyright(C) Facebook, Inc. and its affiliates.
use crate::primary::{BatchRescue, PayloadCache}; // <--- THÊM USE
use config::WorkerId;
use crypto::{Digest, PublicKey};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct PayloadReceiver {
    store: Store,
    cache: PayloadCache, // <--- THÊM TRƯỜNG CACHE
    rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
    tx_batch_rescue: Sender<BatchRescue>, // CRITICAL: Channel để replicate batch ngay
    name: PublicKey, // CRITICAL: Cần name để set origin cho batch rescue
}

impl PayloadReceiver {
    pub fn spawn(
        store: Store,
        cache: PayloadCache, // <--- NHẬN CACHE
        rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
        tx_batch_rescue: Sender<BatchRescue>, // CRITICAL: Channel để replicate batch ngay
        name: PublicKey, // CRITICAL: Cần name để set origin cho batch rescue
    ) {
        tokio::spawn(async move {
            Self {
                store,
                cache,
                rx_workers,
                tx_batch_rescue,
                name,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digest, worker_id, batch)) = self.rx_workers.recv().await {
            // CRITICAL: Log khi nhận batch từ worker (có thể là từ sync hoặc từ worker trực tiếp)
            tracing::info!(
                target: "narwhal_audit",
                "[BATCH RECEIVED FROM WORKER] PayloadReceiver received batch {} from worker {} ({} bytes). Batch will be cached and stored. This batch may have been synced from another worker.",
                digest,
                worker_id,
                batch.len()
            );
            
            // Ghi vào cache (nhanh) - CRITICAL: Cache trước để batch có thể được sử dụng ngay
            let cache_start = std::time::Instant::now();
            self.cache.insert(digest.clone(), batch.clone());
            let cache_duration = cache_start.elapsed();
            
            // CRITICAL: Log sau khi cache batch
            if cache_duration.as_millis() > 10 {
                tracing::warn!(
                    target: "narwhal_audit",
                    "[BATCH CACHE SLOW] PayloadReceiver cached batch {} in {}ms - cache may be slow!",
                    digest, cache_duration.as_millis()
                );
            } else {
                tracing::debug!(
                    target: "narwhal_audit",
                    "[BATCH CACHED] PayloadReceiver cached batch {} in {}ms. Batch is now available for immediate use.",
                    digest, cache_duration.as_millis()
                );
            }

            // CRITICAL FIX: Ghi vào store không blocking - spawn task riêng để không block PayloadReceiver
            // Nếu store chậm, PayloadReceiver vẫn có thể nhận batch mới và cache chúng
            // Batch đã được cache nên có thể được sử dụng ngay cả khi store.write() chưa hoàn thành
            let mut store = self.store.clone();
            let digest_for_store = digest.clone();
            let batch_for_store = batch.clone();
            tokio::spawn(async move {
                // CRITICAL: Non-blocking write - nếu store chậm, không block PayloadReceiver
                // Batch đã được cache nên có thể được sử dụng ngay
                // Note: store.write() không trả về Result, nó sẽ panic nếu channel closed
                // Nhưng vì đã spawn task riêng, panic không ảnh hưởng đến PayloadReceiver
                store.write(digest_for_store.to_vec(), batch_for_store).await;
            });

            // CRITICAL FIX: Replicate batch ngay khi nhận từ worker để đảm bảo các primaries khác có batch
            // trước khi header được proposed. Điều này ngăn chặn missing batches → BLOCK VOTE.
            // Batch đã được cache nên có thể được replicate ngay mà không cần đợi store.write() hoàn thành.
            // CRITICAL: Sử dụng try_send để không block PayloadReceiver nếu channel đầy.
            // Nếu channel đầy, batch vẫn đã được cache nên có thể replicate sau (qua proposer rescue mechanism).
            let tx_batch_rescue = self.tx_batch_rescue.clone();
            let digest_for_replicate = digest.clone();
            let digest_for_log = digest.clone(); // Clone riêng cho log
            let batch_for_replicate = batch.clone();
            let worker_id_for_replicate = worker_id;
            let name_for_replicate = self.name.clone();
            
            let rescue = BatchRescue {
                digest: digest_for_replicate,
                worker_id: worker_id_for_replicate,
                batch: batch_for_replicate,
                origin: name_for_replicate,
            };
            
            // CRITICAL: Sử dụng try_send để không block PayloadReceiver
            // Nếu channel đầy, log warning và continue - batch đã được cache nên có thể replicate sau
            match tx_batch_rescue.try_send(rescue) {
                Ok(()) => {
                    tracing::debug!(
                        target: "narwhal_audit",
                        "[BATCH REPLICATE TRIGGERED] PayloadReceiver triggered immediate replication for batch {} (worker {}). Batch will be replicated to all primaries to prevent missing batches.",
                        digest_for_log, worker_id_for_replicate
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    // Channel đầy - không block, chỉ log warning
                    // Batch đã được cache nên có thể replicate sau qua proposer rescue mechanism
                    tracing::warn!(
                        target: "narwhal_audit",
                        "[BATCH REPLICATE CHANNEL FULL] PayloadReceiver cannot send batch {} for immediate replication - channel is full. Batch is cached and will be replicated later via proposer rescue mechanism if needed.",
                        digest_for_log
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    // Channel đóng - critical error
                    tracing::error!(
                        target: "narwhal_audit",
                        "[BATCH REPLICATE CHANNEL CLOSED] PayloadReceiver cannot send batch {} for immediate replication - channel is closed. This is a critical error!",
                        digest_for_log
                    );
                }
            }
        }
    }
}
