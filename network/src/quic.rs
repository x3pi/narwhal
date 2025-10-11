// In network/src/quic.rs

use super::transport::{Connection, Listener, Transport, TransportResult};
use async_trait::async_trait;
use bytes::Bytes;
use quinn::{ClientConfig, Endpoint, ServerConfig, TransportConfig};
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
        // Sử dụng write_all_chunks để gửi hiệu quả hơn
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
            Err(quinn::ConnectionError::ApplicationClosed(_)) => {
                Ok(None)
            }
            Err(e) => {
                Err(Box::new(e))
            }
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
        let connecting = self.endpoint.accept().await.unwrap();
        let connection = connecting.await?;
        let addr = connection.remote_address();
        let conn = Box::new(QuicConnection { connection });
        Ok((conn, addr))
    }
}

// --- Quic Transport Main Implementation ---
#[derive(Clone)]
pub struct QuicTransport {
    client_config: ClientConfig,
    server_config: ServerConfig,
}

impl QuicTransport {
    pub fn new() -> Self {
        let (server_config, client_config) = configure_certificates();
        Self {
            client_config,
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
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())?;
        endpoint.set_default_client_config(self.client_config.clone());

        let connection = endpoint
            .connect(address, "localhost")?
            .await?;
        Ok(Box::new(QuicConnection { connection }))
    }

    async fn listen(&self, address: SocketAddr) -> TransportResult<Box<dyn Listener>> {
        let endpoint = Endpoint::server(self.server_config.clone(), address)?;
        Ok(Box::new(QuicListener { endpoint }))
    }
}

// --- Certificate Generation & Performance Tuning ---
// SỬA LỖI: Sửa kiểu trả về thành (ServerConfig, ClientConfig)
fn configure_certificates() -> (ServerConfig, ClientConfig) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = cert.serialize_private_key_der();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];

    // --- Cấu hình chung cho Transport ---
    let mut transport_config = TransportConfig::default();
    transport_config.max_concurrent_uni_streams(20_000_u32.into());
    transport_config.stream_receive_window((2 * 1024 * 1024_u32).into());
    transport_config.send_window(20 * 1024 * 1024);
    transport_config.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));
    transport_config.keep_alive_interval(Some(Duration::from_secs(10)));
    let transport = Arc::new(transport_config);

    // --- Cấu hình cho Server ---
    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key).unwrap();
    server_config.transport = transport.clone();


    // --- Cấu hình cho Client ---
    let client_crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    let mut client_config = ClientConfig::new(Arc::new(client_crypto));
    client_config.transport_config(transport);


    (server_config, client_config)
}

// Helper struct to skip server certificate verification during tests.
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