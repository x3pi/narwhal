// In primary/src/helper.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::PrimaryMessage;
use bytes::Bytes;
use config::Committee;
use crypto::Digest;
use log::{debug, error, warn};
use network::SimpleSender;
use store::{Store, ROUND_INDEX_CF};
use tokio::sync::mpsc::Receiver;

const MAX_BUNDLE_SIZE: usize = 60_000;

pub struct Helper {
    committee: Committee,
    store: Store,
    rx_helper: Receiver<PrimaryMessage>,
    network: SimpleSender,
}

impl Helper {
    pub fn spawn(committee: Committee, store: Store, rx_helper: Receiver<PrimaryMessage>) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_helper,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(message) = self.rx_helper.recv().await {
            let (requestor, address) = match &message {
                PrimaryMessage::CertificatesRequest(_, pk) => {
                    (pk.clone(), self.committee.primary(pk))
                }
                PrimaryMessage::CertificateRangeRequest { requestor, .. } => {
                    (requestor.clone(), self.committee.primary(requestor))
                }
                _ => continue,
            };

            let address = match address {
                Ok(x) => x.primary_to_primary,
                Err(e) => {
                    warn!("Request from unknown authority {}: {}", requestor, e);
                    continue;
                }
            };

            match message {
                PrimaryMessage::CertificatesRequest(digests, _) => {
                    for digest in digests {
                        if let Ok(Some(data)) = self.store.read(digest.to_vec()).await {
                            let certificate: Certificate = bincode::deserialize(&data)
                                .expect("Failed to deserialize certificate");
                            let response = PrimaryMessage::Certificate(certificate);
                            let bytes = bincode::serialize(&response)
                                .expect("Failed to serialize certificate");
                            self.network.send(address, Bytes::from(bytes)).await;
                        }
                    }
                }

                PrimaryMessage::CertificateRangeRequest {
                    start_round,
                    end_round,
                    ..
                } => {
                    debug!(
                        "Received certificate range request from {} for rounds {} to {}",
                        requestor, start_round, end_round
                    );

                    let mut certificates_to_send = Vec::new();
                    let mut current_bundle_size = 0;

                    for round in start_round..=end_round {
                        let round_key = round.to_le_bytes().to_vec();
                        if let Ok(Some(digests_bytes)) = self
                            .store
                            .read_cf(ROUND_INDEX_CF.to_string(), round_key)
                            .await
                        {
                            let digests: Vec<Digest> = match bincode::deserialize(&digests_bytes) {
                                Ok(d) => d,
                                Err(e) => {
                                    error!(
                                        "Failed to deserialize digest list for round {}: {}",
                                        round, e
                                    );
                                    continue;
                                }
                            };

                            for digest in digests {
                                if let Ok(Some(cert_bytes)) = self.store.read(digest.to_vec()).await
                                {
                                    if current_bundle_size + cert_bytes.len() > MAX_BUNDLE_SIZE
                                        && !certificates_to_send.is_empty()
                                    {
                                        let bundle_to_send = PrimaryMessage::CertificateBundle(
                                            certificates_to_send.drain(..).collect(),
                                        );
                                        let bytes = bincode::serialize(&bundle_to_send)
                                            .expect("Failed to serialize bundle");
                                        self.network.send(address, Bytes::from(bytes)).await;
                                        current_bundle_size = 0;
                                    }

                                    if let Ok(certificate) = bincode::deserialize(&cert_bytes) {
                                        certificates_to_send.push(certificate);
                                        current_bundle_size += cert_bytes.len();
                                    }
                                }
                            }
                        }
                    }

                    if !certificates_to_send.is_empty() {
                        let final_bundle = PrimaryMessage::CertificateBundle(certificates_to_send);
                        let bytes = bincode::serialize(&final_bundle)
                            .expect("Failed to serialize final bundle");
                        self.network.send(address, Bytes::from(bytes)).await;
                    }
                }
                _ => {}
            }
        }
    }
}
