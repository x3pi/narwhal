// In network/src/transport.rs

use async_trait::async_trait;
use bytes::Bytes;
use std::error::Error;
use std::net::SocketAddr;
use tokio::sync::mpsc;

/// Lỗi chung cho tầng giao vận.
pub type TransportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Đại diện cho một kết nối hai chiều.
/// SỬA LỖI: Thêm `Send` trait bound.
#[async_trait]
pub trait Connection: Send {
    async fn send(&mut self, data: Bytes) -> TransportResult<()>;
    async fn recv(&mut self) -> TransportResult<Option<Bytes>>;
}

/// Lắng nghe các kết nối đến.
/// SỬA LỖI: Thêm `Send` trait bound.
#[async_trait]
pub trait Listener: Send {
    async fn accept(&mut self) -> TransportResult<(Box<dyn Connection>, SocketAddr)>;
}

/// Giao diện chính để tạo kết nối và listener.
/// SỬA LỖI: Thêm `Send` trait bound.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn connect(&self, address: SocketAddr) -> TransportResult<Box<dyn Connection>>;
    async fn listen(&self, address: SocketAddr) -> TransportResult<Box<dyn Listener>>;
}