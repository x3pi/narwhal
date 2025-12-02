use crate::messages::Certificate;
use crate::primary::Round;
use crypto::{Digest, Hash};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct CertificateCacheEntry {
    pub digest: Digest,
    pub round: Round,
    pub certificate: Certificate,
}

pub struct CertificateCacheInner {
    max_entries: usize,
    entries: VecDeque<CertificateCacheEntry>,
    digests: HashSet<Digest>,
}

impl CertificateCacheInner {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            entries: VecDeque::new(),
            digests: HashSet::new(),
        }
    }

    pub fn insert(&mut self, certificate: Certificate) {
        let digest = certificate.digest();
        if self.digests.contains(&digest) {
            return;
        }

        let round = certificate.round();
        self.entries.push_back(CertificateCacheEntry {
            digest: digest.clone(),
            round,
            certificate,
        });
        self.digests.insert(digest);

        while self.entries.len() > self.max_entries {
            if let Some(old) = self.entries.pop_front() {
                self.digests.remove(&old.digest);
            }
        }
    }

    pub fn snapshot_since(&self, since_round: Round, max_entries: usize) -> Vec<Certificate> {
        let mut results = Vec::new();
        for entry in self.entries.iter().rev() {
            if entry.round >= since_round {
                results.push(entry.certificate.clone());
                if results.len() >= max_entries {
                    break;
                }
            }
        }
        results
    }
}

pub type CertificateCache = Arc<Mutex<CertificateCacheInner>>;

pub fn new_certificate_cache(max_entries: usize) -> CertificateCache {
    Arc::new(Mutex::new(CertificateCacheInner::new(max_entries)))
}

