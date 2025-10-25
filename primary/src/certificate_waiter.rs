// In primary/src/certificate_waiter.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::Certificate;
use crate::primary::ReconfigureNotification; // <-- THÊM
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{error, info, warn}; // <-- THÊM info, warn
use store::Store;
use tokio::sync::broadcast; // <-- THÊM
use tokio::sync::mpsc::{Receiver, Sender};

/// Waits to receive all the ancestors of a certificate before looping it back to the `Core`
/// for further processing.
pub struct CertificateWaiter {
    /// The persistent storage.
    store: Store,
    /// Receives sync commands from the `Synchronizer`.
    rx_synchronizer: Receiver<Certificate>,
    /// Loops back to the core certificates for which we got all parents.
    tx_core: Sender<Certificate>,
    // *** THAY ĐỔI: Thêm Receiver ***
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
}

impl CertificateWaiter {
    pub fn spawn(
        store: Store,
        rx_synchronizer: Receiver<Certificate>,
        tx_core: Sender<Certificate>,
        // *** THAY ĐỔI: Thêm tham số mới ***
        rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    ) {
        tokio::spawn(async move {
            Self {
                store,
                rx_synchronizer,
                tx_core,
                // *** THAY ĐỔI: Khởi tạo trường mới ***
                rx_reconfigure,
            }
            .run()
            .await
        });
    }

    /// Helper function. It waits for particular data to become available in the storage
    /// and then delivers the specified header.
    async fn waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: Certificate,
    ) -> DagResult<Certificate> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();

        try_join_all(waiting)
            .await
            .map(|_| deliver)
            .map_err(DagError::from)
    }

    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(certificate) = self.rx_synchronizer.recv() => {
                    // Add the certificate to the waiter pool. The waiter will return it to us
                    // when all its parents are in the store.
                    let wait_for = certificate
                        .header
                        .parents
                        .iter()
                        .cloned()
                        .map(|x| (x.to_vec(), self.store.clone()))
                        .collect();
                    let fut = Self::waiter(wait_for, certificate);
                    waiting.push(fut);
                }
                Some(result) = waiting.next() => match result {
                    Ok(certificate) => {
                        self.tx_core.send(certificate).await.expect("Failed to send certificate");
                    },
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                // *** SỬA ĐỔI BẮT ĐẦU: Sửa nhánh reconfigure ***
                result = self.rx_reconfigure.recv() => {
                    match result {
                        Ok(_notification) => {
                            // Component này không có committee Arc, chỉ cần dọn dẹp state.
                            // Sửa log để không hiển thị epoch cũ (gây nhầm lẫn).
                            info!("CertificateWaiter received reconfigure signal. Clearing pending certificates.");
                            // Xóa tất cả các future đang chờ
                            // (Chúng sẽ bị hủy khi bị drop khỏi FuturesUnordered)
                            waiting.clear();
                        },
                        Err(e) => {
                            warn!("Reconfigure channel error in CertificateWaiter: {}", e);
                            if e == broadcast::error::RecvError::Closed {
                                break; // Thoát vòng lặp nếu kênh bị đóng
                            }
                        }
                    }
                },
                // *** SỬA ĐỔI KẾT THÚC ***
            }
        }
    }
}
