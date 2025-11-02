// Copyright(C) Facebook, Inc. and its affiliates.
// Shutdown handle dùng chung cho tất cả các component

use log::info;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// ShutdownHandle dùng chung để tất cả các component có thể kiểm tra và dừng khi cần
#[derive(Clone, Debug)]
pub struct ShutdownHandle {
    shutdown: Arc<AtomicBool>,
}

impl ShutdownHandle {
    /// Tạo một ShutdownHandle mới
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Kiểm tra xem có tín hiệu shutdown không
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Gửi tín hiệu shutdown đến tất cả các component
    pub fn shutdown(&self) {
        if !self.is_shutdown() {
            self.shutdown.store(true, Ordering::Relaxed);
            info!("[SHUTDOWN] Shutdown signal đã được gửi đến tất cả các component");
        }
    }

    /// Reset shutdown signal (chủ yếu cho testing)
    #[cfg(test)]
    pub fn reset(&self) {
        self.shutdown.store(false, Ordering::Relaxed);
    }
}

impl Default for ShutdownHandle {
    fn default() -> Self {
        Self::new()
    }
}
