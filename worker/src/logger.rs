// Module logging tiếng Việt tập trung vào trace giao dịch cho worker
// Wrapper đơn giản để sử dụng trong worker module

/// Macro để log giao dịch được nhận từ client
#[macro_export]
macro_rules! log_tx_nhan_tu_client {
    ($tx_hash:expr, $worker_id:expr, $size:expr) => {
        tracing::info!(
            target: "tx_trace",
            tx_hash = %$tx_hash,
            worker_id = $worker_id,
            size = $size,
            "📥 Worker nhận giao dịch từ client"
        );
    };
}

/// Macro để log batch được tạo
#[macro_export]
macro_rules! log_batch_tao {
    ($batch_id:expr, $tx_count:expr, $size:expr, $worker_id:expr) => {
        tracing::info!(
            target: "narwhal_audit",
            batch_id = %$batch_id,
            tx_count = $tx_count,
            size = $size,
            worker_id = $worker_id,
            "📦 [BATCH CREATED] Worker tạo batch"
        );
    };
}

/// Macro để log batch được gửi đến primary
#[macro_export]
macro_rules! log_batch_gui_primary {
    ($batch_id:expr, $worker_id:expr, $size:expr) => {
        tracing::info!(
            target: "batch_trace",
            batch_id = %$batch_id,
            worker_id = $worker_id,
            size = $size,
            "📤 Worker gửi batch đến primary"
        );
    };
}

/// Macro để log transaction được thêm vào batch
#[macro_export]
macro_rules! log_tx_them_vao_batch {
    ($tx_hash:expr, $batch_id:expr, $worker_id:expr, $tx_index:expr) => {
        tracing::info!(
            target: "narwhal_audit",
            tx_hash = %$tx_hash,
            batch_id = %$batch_id,
            worker_id = $worker_id,
            tx_index = $tx_index,
            "📋 [TX TO BATCH] Transaction đã được thêm vào batch"
        );
    };
}

/// Macro để log lỗi
#[macro_export]
macro_rules! log_loi_worker {
    ($($arg:tt)*) => {
        tracing::error!($($arg)*);
    };
}

/// Macro để log cảnh báo
#[macro_export]
macro_rules! log_canh_bao_worker {
    ($($arg:tt)*) => {
        tracing::warn!($($arg)*);
    };
}

/// Macro để log thông tin chung (chỉ log khi cần thiết)
#[macro_export]
macro_rules! log_thong_tin_worker {
    ($($arg:tt)*) => {
        tracing::debug!($($arg)*);
    };
}

