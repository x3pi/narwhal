// In primary/src/helper.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::{PrimaryMessage, ReconfigureNotification}; // <-- THÊM ReconfigureNotification
use crate::Epoch;
use bytes::Bytes;
use config::Committee;
use crypto::Digest;
use log::{debug, error, info, trace, warn}; // <-- THÊM trace
use network::SimpleSender;
use std::sync::Arc;
use store::{Store, ROUND_INDEX_CF};
use tokio::sync::broadcast; // <-- THÊM
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;

const MAX_BUNDLE_SIZE: usize = 60_000;

pub struct Helper {
    committee: Arc<RwLock<Committee>>, // SỬA ĐỔI
    store: Store,
    rx_helper: Receiver<PrimaryMessage>,
    // *** THAY ĐỔI: Thêm Receiver ***
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    network: SimpleSender,
    epoch: Epoch, // Track current epoch to quickly filter stale requests
}

impl Helper {
    pub fn spawn(
        committee: Arc<RwLock<Committee>>, // SỬA ĐỔI
        store: Store,
        rx_helper: Receiver<PrimaryMessage>,
        // *** THAY ĐỔI: Thêm tham số mới ***
        rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    ) {
        tokio::spawn(async move {
            let initial_epoch = committee.read().await.epoch;
            Self {
                committee,
                store,
                rx_helper,
                // *** THAY ĐỔI: Khởi tạo trường mới ***
                rx_reconfigure,
                network: SimpleSender::new(),
                epoch: initial_epoch,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // *** THAY ĐỔI: Chuyển sang dùng loop + select! ***
        loop {
            tokio::select! {
                Some(message) = self.rx_helper.recv() => {
                    // SỬA ĐỔI: Khóa committee để đọc
                    let committee = self.committee.read().await;
                    let current_epoch = committee.epoch;
                    // Update internal epoch tracker
                    self.epoch = current_epoch;

                    let (requestor, address) = match &message {
                        PrimaryMessage::CertificatesRequest(_, pk) => (pk.clone(), committee.primary(pk)),
                        PrimaryMessage::CertificateRangeRequest { requestor, .. } => {
                            (requestor.clone(), committee.primary(requestor))
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

                    // Giải phóng lock sớm
                    drop(committee);

                    match message {
                        PrimaryMessage::CertificatesRequest(digests, _) => {
                            for digest in digests {
                                if let Ok(Some(data)) = self.store.read(digest.to_vec()).await {
                                    let certificate: Certificate = match bincode::deserialize(&data) {
                                        Ok(c) => c,
                                        Err(e) => {
                                            error!("Failed to deserialize certificate {}: {}", digest, e);
                                            continue;
                                        }
                                    };

                                    // *** THAY ĐỔI: Chỉ gửi nếu certificate thuộc epoch hiện tại ***
                                    if certificate.epoch() != current_epoch {
                                        // Reduce log spam: only log at trace level for skipped certificates
                                        trace!("Helper skipping certificate {} from epoch {} (current: {})", digest, certificate.epoch(), current_epoch);
                                        continue;
                                    }

                                    // DEBUG: Log địa chỉ trước khi gửi certificate
                                    if address.ip().to_string() == "0.0.0.0" || address.port() == 0 {
                                        warn!("[Helper] ⚠️ SENDING CERTIFICATE TO INVALID ADDRESS: {}", address);
                                    }

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
                            requestor,
                        } => {
                            info!(
                                "[Helper][E{}] Received certificate range request from {} for rounds {} to {}",
                                current_epoch, requestor, start_round, end_round
                            );

                            let mut certificates_to_send = Vec::new();
                            let mut current_bundle_size = 0;

                            for round in start_round..=end_round {
                                // *** THAY ĐỔI: Thêm epoch vào key tra cứu ***
                                let round_key = bincode::serialize(&(current_epoch, round))
                                    .expect("Failed to serialize round index key");

                                if let Ok(Some(digests_bytes)) = self
                                    .store
                                    .read_cf(ROUND_INDEX_CF.to_string(), round_key)
                                    .await
                                {
                                    let digests: Vec<Digest> = match bincode::deserialize(&digests_bytes) {
                                        Ok(d) => d,
                                        Err(e) => {
                                            error!(
                                                "Failed to deserialize digest list for epoch {} round {}: {}",
                                                current_epoch, round, e
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
                                                // DEBUG: Log địa chỉ trước khi gửi certificate bundle
                                                if address.ip().to_string() == "0.0.0.0" || address.port() == 0 {
                                                    warn!("[Helper] ⚠️ SENDING CERTIFICATE BUNDLE TO INVALID ADDRESS: {}", address);
                                                }

                                                let bundle_to_send = PrimaryMessage::CertificateBundle(
                                                    certificates_to_send.drain(..).collect(),
                                                );
                                                let bytes = bincode::serialize(&bundle_to_send)
                                                    .expect("Failed to serialize bundle");
                                                self.network.send(address, Bytes::from(bytes)).await;
                                                current_bundle_size = 0;
                                            }

                                            // Filter by epoch before adding to bundle (defense in depth)
                                            if let Ok(certificate) = bincode::deserialize::<Certificate>(&cert_bytes) {
                                                if certificate.epoch() == current_epoch {
                                                    certificates_to_send.push(certificate);
                                                    current_bundle_size += cert_bytes.len();
                                                } else {
                                                    trace!("Helper skipping certificate {} from epoch {} in range request (current: {})", digest, certificate.epoch(), current_epoch);
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if !certificates_to_send.is_empty() {
                                // DEBUG: Log địa chỉ trước khi gửi final bundle
                                if address.ip().to_string() == "0.0.0.0" || address.port() == 0 {
                                    warn!("[Helper] ⚠️ SENDING FINAL CERTIFICATE BUNDLE TO INVALID ADDRESS: {}", address);
                                }

                                info!(
                                    "[Helper][E{}] Sending {} certificate(s) for rounds {} to {} to requestor {} at {}",
                                    current_epoch, certificates_to_send.len(), start_round, end_round, requestor, address
                                );
                                let final_bundle = PrimaryMessage::CertificateBundle(certificates_to_send);
                                let bytes = bincode::serialize(&final_bundle)
                                    .expect("Failed to serialize final bundle");
                                self.network.send(address, Bytes::from(bytes)).await;
                            } else {
                                warn!(
                                    "[Helper][E{}] No certificates found for rounds {} to {} to send to requestor {}",
                                    current_epoch, start_round, end_round, requestor
                                );
                            }
                        }
                        _ => {}
                    }
                },

                // *** SỬA ĐỔI BẮT ĐẦU: Sửa nhánh reconfigure ***
                result = self.rx_reconfigure.recv() => {
                     match result {
                        Ok(_notification) => {
                            // Đọc ủy ban MỚI NHẤT từ Arc để log
                            let new_committee = self.committee.read().await;
                            let new_epoch = new_committee.epoch;
                            drop(new_committee);

                            // Update internal epoch tracker
                            self.epoch = new_epoch;
                            info!("Helper received reconfigure for epoch {}. Updated internal epoch tracker.", new_epoch);
                            // Không có state nội bộ cần xóa, chỉ cần acknowledge
                        },
                        Err(e) => {
                            warn!("Reconfigure channel error in Helper: {}", e);
                             if e == broadcast::error::RecvError::Closed {
                                break; // Thoát vòng lặp nếu kênh bị đóng
                            }
                        }
                    }
                }
                // *** SỬA ĐỔI KẾT THÚC ***
            }
        }
    }
}
