// Copyright(C) Facebook, Inc. and its affiliates.
use crate::certificate_cache::CertificateCache;
use crate::primary::{PrimaryMessage, Round};
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey};
use log::{debug, error, warn};
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::Receiver;

#[derive(Debug)]
pub enum HelperRequest {
    Certificates {
        digests: Vec<Digest>,
        requester: PublicKey,
    },
    StateSync {
        requester: PublicKey,
        since_round: Round,
        max_rounds: Round,
    },
}

/// A task dedicated to help other authorities by replying to their certificates requests.
pub struct Helper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive certificates requests.
    rx_primaries: Receiver<HelperRequest>,
    /// A network sender to reply to the sync requests.
    network: SimpleSender,
    /// Cache of recent certificates for fast state sync.
    certificate_cache: CertificateCache,
}

impl Helper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_primaries: Receiver<HelperRequest>,
        certificate_cache: CertificateCache,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_primaries,
                network: SimpleSender::new(),
                certificate_cache,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(request) = self.rx_primaries.recv().await {
            match request {
                HelperRequest::Certificates { digests, requester } => {
                    self.handle_certificate_request(digests, requester).await;
                }
                HelperRequest::StateSync {
                    requester,
                    since_round,
                    max_rounds,
                } => {
                    self.handle_state_sync_request(requester, since_round, max_rounds)
                        .await;
                }
            }
        }
    }

    async fn handle_certificate_request(&mut self, digests: Vec<Digest>, origin: PublicKey) {
        let address = match self.committee.primary(&origin) {
            Ok(x) => x.primary_to_primary,
            Err(e) => {
                warn!("Unexpected certificate request: {}", e);
                return;
            }
        };

        for digest in digests {
            match self.store.read(digest.to_vec()).await {
                Ok(Some(data)) => {
                    let certificate = bincode::deserialize(&data)
                        .expect("Failed to deserialize our own certificate");
                    let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate))
                        .expect("Failed to serialize our own certificate");
                    self.network.send(address, Bytes::from(bytes)).await;
                }
                Ok(None) => (),
                Err(e) => error!("{}", e),
            }
        }
    }

    async fn handle_state_sync_request(
        &mut self,
        requester: PublicKey,
        since_round: Round,
        max_rounds: Round,
    ) {
        let address = match self.committee.primary(&requester) {
            Ok(x) => x.primary_to_primary,
            Err(e) => {
                warn!("[STATE SYNC] Unexpected request from {}: {}", requester, e);
                return;
            }
        };

        let limit = max_rounds as usize;
        let certificates = {
            let cache = self.certificate_cache.lock().await;
            cache.snapshot_since(since_round, limit)
        };

        if certificates.is_empty() {
            warn!(
                "[STATE SYNC] No cached certificates >= round {} to serve request from {}",
                since_round, requester
            );
            return;
        }

        debug!(
            "[STATE SYNC] Serving {} cached certificates (round >= {}) to requester {}",
            certificates.len(),
            since_round,
            requester
        );

        for certificate in certificates {
            let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate))
                .expect("Failed to serialize certificate for state sync");
            self.network.send(address, Bytes::from(bytes)).await;
        }
    }
}
