// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature};
use std::collections::HashSet;

/// Aggregates votes for a particular header into a certificate.
pub struct VotesAggregator {
    weight: Stake,
    votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
}

impl VotesAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
        active_cert_count: usize,
    ) -> DagResult<Option<Certificate>> {
        let author = vote.author;

        // Ensure it is the first time this authority votes.
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));

        self.votes.push((author, vote.signature));
        self.weight += committee.stake(&author);

        // *** THAY ĐỔI: Sử dụng quorum động dựa trên số certificate đã có trong round trước đó ***
        let quorum_threshold = if active_cert_count > 0 {
            committee.quorum_threshold_dynamic(active_cert_count)
        } else {
            committee.quorum_threshold()
        };

        if self.weight >= quorum_threshold {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(Certificate {
                header: header.clone(),
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}

/// Aggregate certificates and check if we reach a quorum.
pub struct CertificatesAggregator {
    weight: Stake,
    certificates: Vec<Digest>,
    used: HashSet<PublicKey>,
}

impl CertificatesAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            certificates: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
        active_cert_count: usize,
    ) -> DagResult<Option<Vec<Digest>>> {
        let origin = certificate.origin();

        // Ensure it is the first time this authority votes.
        if !self.used.insert(origin) {
            return Ok(None);
        }

        self.certificates.push(certificate.digest());
        self.weight += committee.stake(&origin);

        // *** THAY ĐỔI: Sử dụng quorum động dựa trên số certificate đã có trong round trước đó ***
        let quorum_threshold = if active_cert_count > 0 {
            committee.quorum_threshold_dynamic(active_cert_count)
        } else {
            committee.quorum_threshold()
        };

        if self.weight >= quorum_threshold {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(self.certificates.drain(..).collect()));
        }
        Ok(None)
    }

    /// Get current certificates collected so far (even if quorum not reached)
    pub fn get_certificates(&self) -> &Vec<Digest> {
        &self.certificates
    }
}
