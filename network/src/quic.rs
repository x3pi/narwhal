// In network/src/quic.rs

use super::transport::{Connection, Listener, Transport, TransportResult};
use async_trait::async_trait;
use bytes::Bytes;
use quinn::{ClientConfig, Endpoint, ServerConfig, TransportConfig, VarInt};
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

// --- Quic Connection Implementation ---
pub struct QuicConnection {
    connection: quinn::Connection,
}

#[async_trait]
impl Connection for QuicConnection {
    async fn send(&mut self, data: Bytes) -> TransportResult<()> {
        let mut send_stream = self.connection.open_uni().await?;
        send_stream.write_all(&data).await?;
        send_stream.finish().await?;
        Ok(())
    }

    async fn recv(&mut self) -> TransportResult<Option<Bytes>> {
        match self.connection.accept_uni().await {
            Ok(mut recv_stream) => {
                let buffer = recv_stream.read_to_end(128 * 1024 * 1024).await?;
                Ok(Some(Bytes::from(buffer)))
            }
            Err(quinn::ConnectionError::ApplicationClosed(_)) => Ok(None),
            Err(quinn::ConnectionError::LocallyClosed) => Ok(None),
            Err(e) => Err(Box::new(e)),
        }
    }
}

// --- Quic Listener Implementation ---
pub struct QuicListener {
    endpoint: Endpoint,
}

#[async_trait]
impl Listener for QuicListener {
    async fn accept(&mut self) -> TransportResult<(Box<dyn Connection>, SocketAddr)> {
        let connecting = match self.endpoint.accept().await {
            Some(conn) => conn,
            None => return Err("Endpoint closed".into()),
        };

        let connection = connecting.await?;
        let addr = connection.remote_address();
        let conn = Box::new(QuicConnection { connection });
        Ok((conn, addr))
    }
}

// --- Quic Transport Main Implementation ---
#[derive(Clone)]
pub struct QuicTransport {
    // SỬA ĐỔI: Giữ một Endpoint duy nhất cho client để tái sử dụng.
    client_endpoint: Endpoint,
    server_config: ServerConfig,
}

impl QuicTransport {
    pub fn new() -> Self {
        let (server_config, client_config) = configure_certificates();

        // SỬA ĐỔI: Tạo client endpoint (socket) MỘT LẦN DUY NHẤT khi khởi tạo.
        let mut client_endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
            .expect("Failed to create QUIC client endpoint");
        client_endpoint.set_default_client_config(client_config);

        Self {
            client_endpoint,
            server_config,
        }
    }
}

impl Default for QuicTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for QuicTransport {
    async fn connect(&self, address: SocketAddr) -> TransportResult<Box<dyn Connection>> {
        // SỬA ĐỔI: Tái sử dụng endpoint đã được tạo sẵn thay vì tạo mới.
        let connection = self.client_endpoint.connect(address, "localhost")?.await?;
        Ok(Box::new(QuicConnection { connection }))
    }

    async fn listen(&self, address: SocketAddr) -> TransportResult<Box<dyn Listener>> {
        let endpoint = Endpoint::server(self.server_config.clone(), address)?;
        Ok(Box::new(QuicListener { endpoint }))
    }
}

// --- Certificate Generation & Performance Tuning ---
fn configure_certificates() -> (ServerConfig, ClientConfig) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = cert.serialize_private_key_der();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];

    let mut transport_config = TransportConfig::default();

    // Cấu hình hiệu năng
    transport_config.max_concurrent_uni_streams(VarInt::from_u32(10_000));
    transport_config.stream_receive_window(VarInt::from_u32(2 * 1024 * 1024)); // 2MB
    transport_config.receive_window(VarInt::from_u32(4 * 1024 * 1024)); // 4MB

    // SỬA ĐỔI: Cấu hình timeout ngắn và keep-alive thường xuyên để phát hiện mất kết nối nhanh hơn.
    transport_config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
    transport_config.keep_alive_interval(Some(Duration::from_secs(3)));

    let transport = Arc::new(transport_config);

    // Cấu hình Server
    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key).unwrap();
    server_config.transport = transport.clone();

    // Cấu hình Client
    let client_crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();

    let mut client_config = ClientConfig::new(Arc::new(client_crypto));
    client_config.transport_config(transport);

    (server_config, client_config)
}

// Helper struct để bỏ qua xác thực certificate của server (chỉ dùng cho môi trường dev)
struct SkipServerVerification;

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}
