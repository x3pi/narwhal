// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::PrimaryMessage; // Sửa lỗi: Sử dụng đường dẫn import đúng
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey};
use log::{error, warn};
use network::SimpleSender; // Giữ lại SimpleSender từ code cũ
use store::{Store, ROUND_INDEX_CF};
use tokio::sync::mpsc::Receiver;

/// A task dedicated to help other authorities by replying to their certificates requests.
pub struct Helper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive various types of requests.
    rx_helper: Receiver<PrimaryMessage>,
    /// A network sender to reply to the sync requests.
    network: SimpleSender,
}

impl Helper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        // Kênh bây giờ nhận PrimaryMessage để xử lý được nhiều loại yêu cầu
        rx_helper: Receiver<PrimaryMessage>,
    ) {
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
            // Xác định node yêu cầu và địa chỉ của nó
            let (requestor, address) = match &message {
                PrimaryMessage::CertificatesRequest(_, pk) => {
                    (pk.clone(), self.committee.primary(pk))
                }
                PrimaryMessage::CertificateRangeRequest { requestor, .. } => {
                    (requestor.clone(), self.committee.primary(requestor))
                }
                _ => continue, // Bỏ qua các message không phải là yêu cầu dữ liệu
            };

            let address = match address {
                Ok(x) => x.primary_to_primary,
                Err(e) => {
                    warn!("Request from unknown authority {}: {}", requestor, e);
                    continue;
                }
            };

            match message {
                // --- LUỒNG CŨ: Xử lý yêu cầu certificate đơn lẻ ---
                PrimaryMessage::CertificatesRequest(digests, _) => {
                    for digest in digests {
                        match self.store.read(digest.to_vec()).await {
                            Ok(Some(data)) => {
                                // Gửi lại từng certificate như code cũ
                                let certificate: Certificate = bincode::deserialize(&data)
                                    .expect("Failed to deserialize our own certificate");
                                let response = PrimaryMessage::Certificate(certificate);
                                let bytes = bincode::serialize(&response)
                                    .expect("Failed to serialize our own certificate");
                                self.network.send(address, Bytes::from(bytes)).await;
                            }
                            Ok(None) => (),
                            Err(e) => error!("{}", e),
                        }
                    }
                }

                // --- LUỒNG MỚI: Xử lý yêu cầu đồng bộ hàng loạt ---
                PrimaryMessage::CertificateRangeRequest {
                    start_round,
                    end_round,
                    ..
                } => {
                    let mut certificates = Vec::new();
                    for round in start_round..=end_round {
                        let round_key = round.to_le_bytes().to_vec();

                        // Đọc danh sách digest từ chỉ mục
                        if let Ok(Some(digests_value)) = self
                            .store
                            .read_cf(ROUND_INDEX_CF.to_string(), round_key)
                            .await
                        {
                            let digests: Vec<Digest> = match bincode::deserialize(&digests_value) {
                                Ok(d) => d,
                                Err(e) => {
                                    error!(
                                        "Failed to deserialize digest list for round {}: {}",
                                        round, e
                                    );
                                    continue;
                                }
                            };

                            // Đọc từng certificate từ store chính
                            for digest in digests {
                                if let Ok(Some(cert_value)) = self.store.read(digest.to_vec()).await
                                {
                                    if let Ok(certificate) = bincode::deserialize(&cert_value) {
                                        certificates.push(certificate);
                                    }
                                }
                            }
                        }
                    }

                    if !certificates.is_empty() {
                        let bundle = PrimaryMessage::CertificateBundle(certificates);
                        let bytes = bincode::serialize(&bundle)
                            .expect("Failed to serialize certificate bundle");
                        self.network.send(address, Bytes::from(bytes)).await;
                    }
                }
                _ => {}
            }
        }
    }
}
