// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::DagResult;
use crate::header_waiter::WaiterMessage;
use crate::messages::{Certificate, Header};
use crate::primary::PayloadCache;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{info, warn};
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::Sender;
/// The `Synchronizer` checks if we have all batches and parents referenced by a header. If we don't, it sends
/// a command to the `Waiter` to request the missing data.
pub struct Synchronizer {
    /// The public key of this primary.
    name: PublicKey,
    /// The persistent storage.
    store: Store,
    /// Send commands to the `HeaderWaiter`.
    tx_header_waiter: Sender<WaiterMessage>,
    /// Send commands to the `CertificateWaiter`.
    tx_certificate_waiter: Sender<Certificate>,
    /// The genesis and its digests.
    genesis: Vec<(Digest, Certificate)>,

    cache: PayloadCache, // <--- THÊM TRƯỜNG CACHE
}

impl Synchronizer {
    pub fn new(
        name: PublicKey,
        committee: &Committee,
        store: Store,
        cache: PayloadCache, // <--- NHẬN CACHE
        tx_header_waiter: Sender<WaiterMessage>,
        tx_certificate_waiter: Sender<Certificate>,
    ) -> Self {
        Self {
            name,
            store,
            cache,
            tx_header_waiter,
            tx_certificate_waiter,
            genesis: Certificate::genesis(committee)
                .into_iter()
                .map(|x| (x.digest(), x))
                .collect(),
        }
    }

    /// Returns `true` if we have all transactions of the payload. If we don't, we return false,
    /// synchronize with other nodes (through our workers), and re-schedule processing of the
    /// header for when we will have its complete payload.
    pub async fn missing_payload(&mut self, header: &Header) -> DagResult<bool> {
        if header.author == self.name {
            return Ok(false);
        }

        let mut missing = HashMap::new();
        let mut found_in_cache = 0usize;
        let mut found_in_store = 0usize;
        
        for (digest, worker_id) in header.payload.iter() {
            // KIỂM TRA CACHE TRƯỚC
            if self.cache.contains_key(digest) {
                found_in_cache += 1;
                continue; // Tìm thấy trong RAM, không cần làm gì thêm
            }

            // Nếu không có trong cache, kiểm tra store (phương án dự phòng)
            match self.store.read(digest.to_vec()).await? {
                Some(_) => {
                    found_in_store += 1;
                    // Batch có trong store - OK
                }
                None => {
                    // CRITICAL: Batch không có trong cache và store - cần sync
                    missing.insert(digest.clone(), *worker_id);
                }
            }
        }

        if missing.is_empty() {
            return Ok(false);
        }

        // CRITICAL: Log chi tiết về missing batches để debug
        let missing_count = missing.len();
        let missing_digests: Vec<_> = missing.keys().take(5).cloned().collect();
        warn!(
            "[SYNC TRIGGER] Primary {} detected {} missing batches in header {} (round {}, author: {}). Found {} in cache, {} in store, {} missing. Triggering sync request to HeaderWaiter.",
            self.name,
            missing_count,
            header.id,
            header.round,
            header.author,
            found_in_cache,
            found_in_store,
            missing_count
        );
        
        if missing_count > 0 {
            info!(
                "[SYNC TRIGGER DETAIL] Primary {} missing batches (sample): {:?} from header {} (round {}, author: {}). Sync request will be sent to HeaderWaiter.",
                self.name,
                missing_digests,
                header.id,
                header.round,
                header.author
            );
        }

        self.tx_header_waiter
            .send(WaiterMessage::SyncBatches(missing, header.clone()))
            .await
            .expect("Failed to send sync batch request");
        Ok(true)
    }
    /// Returns the parents of a header if we have them all. If at least one parent is missing,
    /// we return an empty vector, synchronize with other nodes, and re-schedule processing
    /// of the header for when we will have all the parents.
    pub async fn get_parents(&mut self, header: &Header) -> DagResult<Vec<Certificate>> {
        let mut missing = Vec::new();
        let mut parents = Vec::new();
        for digest in &header.parents {
            if let Some(genesis) = self
                .genesis
                .iter()
                .find(|(x, _)| x == digest)
                .map(|(_, x)| x)
            {
                parents.push(genesis.clone());
                continue;
            }

            match self.store.read(digest.to_vec()).await? {
                Some(certificate) => parents.push(bincode::deserialize(&certificate)?),
                None => missing.push(digest.clone()),
            };
        }

        if missing.is_empty() {
            return Ok(parents);
        }

        self.tx_header_waiter
            .send(WaiterMessage::SyncParents(missing, header.clone()))
            .await
            .expect("Failed to send sync parents request");
        Ok(Vec::new())
    }

    /// Check whether we have all the ancestors of the certificate. If we don't, send the certificate to
    /// the `CertificateWaiter` which will trigger re-processing once we have all the missing data.
    pub async fn deliver_certificate(&mut self, certificate: &Certificate) -> DagResult<bool> {
        for digest in &certificate.header.parents {
            if self.genesis.iter().any(|(x, _)| x == digest) {
                continue;
            }

            if self.store.read(digest.to_vec()).await?.is_none() {
                self.tx_certificate_waiter
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send sync certificate request");
                return Ok(false);
            };
        }
        Ok(true)
    }
}
