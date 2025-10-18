// In network/src/quic.rs

use super::transport::{Connection, Listener, Transport, TransportResult};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use quinn::{
    ClientConfig, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig, VarInt,
};
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// --- Triển khai Connection cho QUIC ---
pub struct QuicConnection {
    framed: Framed<QuicStream, LengthDelimitedCodec>,
}
struct QuicStream {
    sender: SendStream,
    receiver: RecvStream,
}
impl tokio::io::AsyncRead for QuicStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.receiver).poll_read(cx, buf)
    }
}
impl tokio::io::AsyncWrite for QuicStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::pin::Pin::new(&mut self.sender).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.sender).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.sender).poll_shutdown(cx)
    }
}
#[async_trait]
impl Connection for QuicConnection {
    async fn send(&mut self, data: Bytes) -> TransportResult<()> {
        self.framed.send(data).await?;
        Ok(())
    }
    async fn recv(&mut self) -> TransportResult<Option<Bytes>> {
        match self.framed.next().await {
            Some(Ok(bytes)) => Ok(Some(bytes.freeze())),
            Some(Err(e)) => Err(Box::new(e)),
            None => Ok(None),
        }
    }
}

// --- Triển khai Listener cho QUIC ---
pub struct QuicListener {
    listener: Endpoint,
}
#[async_trait]
impl Listener for QuicListener {
    async fn accept(&mut self) -> TransportResult<(Box<dyn Connection>, SocketAddr)> {
        let connecting = self.listener.accept().await.unwrap();
        let connection = connecting.await?;
        let addr = connection.remote_address();
        let (sender, receiver) = connection.accept_bi().await?;
        let stream = QuicStream { sender, receiver };
        let framed = Framed::new(stream, LengthDelimitedCodec::new());
        let conn = Box::new(QuicConnection { framed });
        Ok((conn, addr))
    }
}

// --- Triển khai Transport chính ---
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
    pub fn get_client_config(&self) -> ClientConfig {
        self.client_config.clone()
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
        // SỬA LỖI: Tạo và cấu hình endpoint ngay tại đây để đảm bảo mọi kết nối đều đúng.
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())?;
        endpoint.set_default_client_config(self.client_config.clone());

        let connection = endpoint.connect(address, "localhost")?.await?;
        let (sender, receiver) = connection.open_bi().await?;
        let stream = QuicStream { sender, receiver };
        let framed = Framed::new(stream, LengthDelimitedCodec::new());
        Ok(Box::new(QuicConnection { framed }))
    }

    async fn listen(&self, address: SocketAddr) -> TransportResult<Box<dyn Listener>> {
        let listener = Endpoint::server(self.server_config.clone(), address)?;
        Ok(Box::new(QuicListener { listener }))
    }
}

// --- Cấu hình chứng chỉ & Hiệu năng ---
fn configure_certificates() -> (ServerConfig, ClientConfig) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = cert.serialize_private_key_der();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];
    let mut transport_config = TransportConfig::default();
    transport_config.max_concurrent_uni_streams(VarInt::from_u32(100_000));
    transport_config.max_concurrent_bidi_streams(VarInt::from_u32(100_000));
    const MAX_STREAM_WINDOW: u32 = 20 * 1024 * 1024;
    const MAX_CONN_WINDOW: u32 = 40 * 1024 * 1024;
    transport_config.stream_receive_window(VarInt::from_u32(MAX_STREAM_WINDOW));
    transport_config.receive_window(VarInt::from_u32(MAX_CONN_WINDOW));
    transport_config.send_window((MAX_CONN_WINDOW as u64).into());
    transport_config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
    transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
    let transport = Arc::new(transport_config);
    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key).unwrap();
    server_config.transport = transport.clone();
    let client_crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    let mut client_config = ClientConfig::new(Arc::new(client_crypto));
    client_config.transport_config(transport);
    (server_config, client_config)
}
struct SkipServerVerification;
impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _: &rustls::Certificate,
        _: &[rustls::Certificate],
        _: &rustls::ServerName,
        _: &mut dyn Iterator<Item = &[u8]>,
        _: &[u8],
        _: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}
