// Module logging tiếng Việt tập trung vào trace giao dịch

/// Macro để log giao dịch được nhận
#[macro_export]
macro_rules! log_tx_nhan {
    ($tx_hash:expr, $worker_id:expr, $batch_id:expr, $height:expr) => {
        tracing::info!(
            target: "tx_trace",
            tx_hash = %$tx_hash,
            worker_id = $worker_id,
            batch_id = %$batch_id,
            height = $height,
            "📥 Nhận giao dịch"
        );
    };
}

/// Macro để log giao dịch được thêm vào block
#[macro_export]
macro_rules! log_tx_them_vao_block {
    ($tx_hash:expr, $height:expr, $tx_index:expr) => {
        tracing::info!(
            target: "tx_trace",
            tx_hash = %$tx_hash,
            height = $height,
            tx_index = $tx_index,
            "✅ Thêm giao dịch vào block"
        );
    };
}

/// Macro để log giao dịch được gửi đến executor (lúc vào)
#[macro_export]
macro_rules! log_tx_gui_executor {
    ($tx_hash:expr, $height:expr, $tx_index:expr) => {
        tracing::info!(
            target: "tx_trace",
            tx_hash = %$tx_hash,
            height = $height,
            tx_index = $tx_index,
            "🚀 [TX IN] Gửi giao dịch đến executor (lúc vào)"
        );
    };
}

/// Macro để log giao dịch được thực thi thành công (lúc ra)
#[macro_export]
macro_rules! log_tx_thuc_thi_thanh_cong {
    ($tx_hash:expr, $height:expr, $tx_index:expr) => {
        tracing::info!(
            target: "tx_trace",
            tx_hash = %$tx_hash,
            height = $height,
            tx_index = $tx_index,
            "✅ [TX OUT] Giao dịch được thực thi thành công (lúc ra)"
        );
    };
}

/// Macro để log block được commit
#[macro_export]
macro_rules! log_block_commit {
    ($height:expr, $tx_count:expr, $epoch:expr) => {
        tracing::info!(
            target: "block_trace",
            height = $height,
            tx_count = $tx_count,
            epoch = $epoch,
            "📦 Block đã commit"
        );
    };
}

/// Macro để log batch được xử lý
#[macro_export]
macro_rules! log_batch_xu_ly {
    ($batch_id:expr, $tx_count:expr, $height:expr) => {
        tracing::debug!(
            target: "batch_trace",
            batch_id = %$batch_id,
            tx_count = $tx_count,
            height = $height,
            "📋 Xử lý batch"
        );
    };
}

/// Macro để log batch được commit thành công (thêm vào block)
#[macro_export]
macro_rules! log_batch_commit {
    ($batch_id:expr, $tx_count:expr, $height:expr, $worker_id:expr) => {
        tracing::info!(
            target: "batch_commit",
            batch_id = %$batch_id,
            tx_count = $tx_count,
            height = $height,
            worker_id = $worker_id,
            "✅ [BATCH COMMIT] Batch đã được commit thành công và thêm vào block"
        );
    };
}

/// Macro để log transaction được commit thành công (thêm vào block)
#[macro_export]
macro_rules! log_tx_commit {
    ($tx_hash:expr, $height:expr, $tx_index:expr, $batch_id:expr) => {
        tracing::info!(
            target: "tx_commit",
            tx_hash = %$tx_hash,
            height = $height,
            tx_index = $tx_index,
            batch_id = %$batch_id,
            "✅ [TX COMMIT] Transaction đã được commit thành công và thêm vào block"
        );
    };
}

/// Macro để log batch được gửi đến Unix Domain Socket để thực thi
#[macro_export]
macro_rules! log_batch_gui_uds {
    ($batch_id:expr, $height:expr, $node_id:expr) => {
        tracing::info!(
            target: "narwhal_audit",
            batch_id = %$batch_id,
            height = $height,
            node_id = $node_id,
            "🚀 [BATCH TO UDS] Batch đã được gửi đến Unix Domain Socket để thực thi sau khi commit"
        );
    };
}

/// Macro để log lỗi quan trọng
#[macro_export]
macro_rules! log_loi {
    ($($arg:tt)*) => {
        tracing::error!($($arg)*);
    };
}

/// Macro để log cảnh báo
#[macro_export]
macro_rules! log_canh_bao {
    ($($arg:tt)*) => {
        tracing::warn!($($arg)*);
    };
}

/// Macro để log thông tin chung
#[macro_export]
macro_rules! log_thong_tin {
    ($($arg:tt)*) => {
        tracing::info!($($arg)*);
    };
}

/// Log giao dịch với đầy đủ thông tin để trace
pub fn log_giao_dich_chi_tiet(
    tx_hash: &str,
    from: &str,
    to: &str,
    amount: &str,
    height: u64,
    tx_index: usize,
    worker_id: u32,
    batch_id: &str,
) {
    tracing::info!(
        target: "tx_detail",
        tx_hash = %tx_hash,
        from = %from,
        to = %to,
        amount = %amount,
        height = height,
        tx_index = tx_index,
        worker_id = worker_id,
        batch_id = %batch_id,
        "Giao dịch chi tiết"
    );
}

/// Log tóm tắt block
pub fn log_tom_tat_block(
    height: u64,
    epoch: u64,
    tx_count: usize,
    batch_count: usize,
) {
    tracing::info!(
        target: "block_summary",
        height = height,
        epoch = epoch,
        tx_count = tx_count,
        batch_count = batch_count,
        "Tóm tắt block"
    );
}

/// Log batch đến muộn
pub fn log_batch_muon(
    batch_id: &str,
    original_height: u64,
    current_height: u64,
    worker_id: u32,
) {
    tracing::warn!(
        target: "narwhal_audit",
        batch_id = %batch_id,
        original_height = original_height,
        current_height = current_height,
        worker_id = worker_id,
        "⚠️ [LATE BATCH] Batch đến muộn"
    );
}

/// Log giao dịch bị trùng lặp
pub fn log_tx_trung_lap(
    tx_hash: &str,
    height: u64,
    batch_id: &str,
) {
    tracing::warn!(
        target: "duplicate_tx",
        tx_hash = %tx_hash,
        height = height,
        batch_id = %batch_id,
        "⚠️ Giao dịch trùng lặp, bỏ qua"
    );
}

/// Log batch không tìm thấy
pub fn log_batch_khong_tim_thay(
    batch_id: &str,
    worker_id: u32,
    height: u64,
) {
    tracing::error!(
        target: "missing_batch",
        batch_id = %batch_id,
        worker_id = worker_id,
        height = height,
        "❌ Không tìm thấy batch trong store"
    );
}

/// Log sync metrics for monitoring
pub fn log_sync_metrics(
    total_checked: u64,
    found_in_cache: u64,
    found_in_store: u64,
    missing: u64,
    sync_success_rate: f64,
) {
    tracing::info!(
        target: "narwhal_audit",
        total_checked = total_checked,
        found_in_cache = found_in_cache,
        found_in_store = found_in_store,
        missing = missing,
        sync_success_rate = sync_success_rate,
        "[SYNC METRICS] Batch sync statistics"
    );
}

