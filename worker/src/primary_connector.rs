// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use bytes::Bytes;
use network::SimpleSender;
use std::net::SocketAddr;
use tokio::sync::mpsc::Receiver;

// Send batches' digests to the primary.
pub struct PrimaryConnector {
    /// The primary network address.
    primary_address: SocketAddr,
    /// Input channel to receive the digests to send to the primary.
    rx_digest: Receiver<SerializedBatchDigestMessage>,
    /// A network sender to send the baches' digests to the primary.
    network: SimpleSender,
}

impl PrimaryConnector {
    pub fn spawn(primary_address: SocketAddr, rx_digest: Receiver<SerializedBatchDigestMessage>) {
        tokio::spawn(async move {
            Self {
                primary_address,
                rx_digest,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // đưa rx vào để lắng nghe
        while let Some(digest) = self.rx_digest.recv().await {
            // DEBUG: Log địa chỉ trước khi gửi digest đến primary
            if self.primary_address.ip().to_string() == "0.0.0.0"
                || self.primary_address.port() == 0
            {
                warn!(
                    "[PrimaryConnector] ⚠️ SENDING DIGEST TO INVALID PRIMARY ADDRESS: {}",
                    self.primary_address
                );
            }

            // Send the digest through the network.
            self.network
                .send(self.primary_address, Bytes::from(digest))
                .await;
        }
    }
}
