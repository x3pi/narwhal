// In primary/src/synchronizer.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::DagResult;
use crate::header_waiter::WaiterMessage;
use crate::messages::{Certificate, Header};
use crate::primary::PayloadCache;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use std::collections::HashMap;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

pub struct Synchronizer {
    name: PublicKey,
    committee: Arc<RwLock<Committee>>, // SỬA ĐỔI
    store: Store,
    tx_header_waiter: Sender<WaiterMessage>,
    tx_certificate_waiter: Sender<Certificate>,
    cache: PayloadCache,
}

impl Synchronizer {
    pub fn new(
        name: PublicKey,
        committee: Arc<RwLock<Committee>>, // SỬA ĐỔI
        store: Store,
        cache: PayloadCache,
        tx_header_waiter: Sender<WaiterMessage>,
        tx_certificate_waiter: Sender<Certificate>,
    ) -> Self {
        Self {
            name,
            committee,
            store,
            cache,
            tx_header_waiter,
            tx_certificate_waiter,
        }
    }

    pub async fn missing_payload(&mut self, header: &Header) -> DagResult<bool> {
        if header.author == self.name {
            return Ok(false);
        }

        let mut missing = HashMap::new();
        for (digest, worker_id) in header.payload.iter() {
            if self.cache.contains_key(digest) {
                continue;
            }
            if self.store.read(digest.to_vec()).await?.is_none() {
                missing.insert(digest.clone(), *worker_id);
            }
        }

        if missing.is_empty() {
            return Ok(false);
        }

        self.tx_header_waiter
            .send(WaiterMessage::SyncBatches(missing, header.clone()))
            .await
            .expect("Failed to send sync batch request");
        Ok(true)
    }

    async fn get_genesis(&self) -> Vec<(Digest, Certificate)> {
        let committee = self.committee.read().await;
        Certificate::genesis(&committee)
            .into_iter()
            .map(|x| (x.digest(), x))
            .collect()
    }

    pub async fn get_parents(&mut self, header: &Header) -> DagResult<Vec<Certificate>> {
        let mut missing = Vec::new();
        let mut parents = Vec::new();
        let genesis = self.get_genesis().await;

        for digest in &header.parents {
            if let Some(genesis_cert) = genesis.iter().find(|(x, _)| x == digest).map(|(_, x)| x) {
                parents.push(genesis_cert.clone());
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

    pub async fn deliver_certificate(&mut self, certificate: &Certificate) -> DagResult<bool> {
        let genesis = self.get_genesis().await;
        for digest in &certificate.header.parents {
            if genesis.iter().any(|(x, _)| x == digest) {
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
