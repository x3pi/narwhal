// In node/src/state_syncer.rs

use config::Committee;
use crypto::PublicKey;
use log::{info, warn};
use network::ReliableSender;
use std::sync::Arc;
use store::Store;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

/// StateSyncer chịu trách nhiệm giữ cho một node không phải là validator được cập nhật
/// bằng cách lấy dữ liệu từ các node validator khác.
pub struct StateSyncer {
    /// Tên (PublicKey) của node này.
    name: PublicKey,
    /// Committee hiện tại, để biết ai là validator và địa chỉ p2p của họ.
    committee: Arc<RwLock<Committee>>,
    /// Kho lưu trữ cục bộ để ghi dữ liệu đã đồng bộ.
    _store: Store,
    /// Mạng gửi để yêu cầu dữ liệu.
    _network: ReliableSender,
    /// Kênh nhận tín hiệu dừng.
    shutdown_receiver: broadcast::Receiver<()>,
}

impl StateSyncer {
    /// Khởi chạy một StateSyncer mới trong một tác vụ Tokio riêng biệt.
    pub fn spawn(
        name: PublicKey,
        committee: Arc<RwLock<Committee>>,
        store: Store,
        shutdown_receiver: broadcast::Receiver<()>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                _store: store,
                _network: ReliableSender::new(),
                shutdown_receiver,
            }
            .run()
            .await;
        });
    }

    /// Vòng lặp chính của StateSyncer.
    async fn run(&mut self) {
        info!("[{:?}] Starting StateSyncer in Follower mode.", self.name);

        // Tạo một bộ đếm thời gian để định kỳ cố gắng đồng bộ hóa.
        let mut sync_timer = interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                // Chờ đến lượt đồng bộ tiếp theo.
                _ = sync_timer.tick() => {
                    self.synchronize().await;
                },
                // Lắng nghe tín hiệu dừng.
                _ = self.shutdown_receiver.recv() => {
                    info!("[{:?}] Shutdown signal received, stopping StateSyncer.", self.name);
                    break;
                }
            }
        }
        info!("[{:?}] StateSyncer has been terminated.", self.name);
    }

    /// Thực hiện logic đồng bộ hóa thực tế.
    async fn synchronize(&self) {
        let committee = self.committee.read().await;

        // SỬA LỖI: Lấy danh sách các địa chỉ p2p của các validator.
        let peer_addresses: Vec<_> = committee
            .authorities
            .values()
            .map(|auth| auth.p2p_address.clone())
            .collect();

        if peer_addresses.is_empty() {
            warn!(
                "[{:?}] No peer P2P addresses available in the committee to sync from.",
                self.name
            );
            return;
        }

        info!(
            "[{:?}] Attempting to sync state from peers: {:?}",
            self.name, peer_addresses
        );

        //
        // --- LOGIC ĐỒNG BỘ THỰC TẾ SẼ ĐƯỢC THÊM VÀO ĐÂY ---
        //
        // 1. Chọn một hoặc nhiều peer từ `peer_addresses`.
        // 2. Gửi một tin nhắn yêu cầu dữ liệu (ví dụ: `StateRequestMessage`).
        //    `self.network.send(peer_address, serialized_request).await;`
        // 3. Chờ phản hồi.
        // 4. Xử lý phản hồi (giải mã, xác thực, và ghi vào `self.store`).
        //
        // Tạm thời, chúng ta sẽ chỉ ghi lại một thông báo giả lập.
        //
        warn!(
            "[{:?}] State synchronization logic not yet implemented.",
            self.name
        );
    }
}
