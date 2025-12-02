// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature};
use log::debug;
use tracing;
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
    ) -> DagResult<Option<Certificate>> {
        let author = vote.author;

        // Ensure it is the first time this authority votes.
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));

        self.votes.push((author, vote.signature));
        self.weight += committee.stake(&author);

        let threshold = committee.quorum_threshold();
        let current_weight = self.weight;

        if current_weight >= threshold {
            // CRITICAL: Log khi quorum đạt được - certificate sẽ được tạo
            let voters: Vec<String> = self.used.iter().map(|v| format!("{}", v)).collect();
            tracing::info!(
                target: "narwhal_audit",
                "[VOTE AGGREGATION - QUORUM REACHED] Header {} (round {}, author: {}, {} batches) reached quorum: {}/{} stake. Certificate will be created. Voters ({}): {:?}",
                header.id,
                header.round,
                header.author,
                header.payload.len(),
                current_weight,
                threshold,
                voters.len(),
                voters
            );
            
            debug!(
                "VotesAggregator: quorum reached for header {} (round {}) with voters: {:?}",
                header.id, header.round, self.used
            );
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(Certificate {
                header: header.clone(),
                votes: self.votes.clone(),
            }));
        }

        let missing_stake = threshold.saturating_sub(current_weight);
        if missing_stake > 0 {
            let mut missing: Vec<_> = committee
                .authorities
                .keys()
                .filter(|authority| !self.used.contains(authority))
                .cloned()
                .collect();
            missing.sort();
            
            let current_voters: Vec<String> = self.used.iter().map(|v| format!("{}", v)).collect();
            let missing_authorities: Vec<String> = missing.iter().map(|v| format!("{}", v)).collect();
            
            // MONITORING: Upgrade to info level để dễ kiểm tra tại sao không đủ votes
            log::info!(
                "[VOTE AGGREGATION] Header {} (round {}) has stake {}/{} (missing {}). Waiting for authorities: {:?}. Current voters: {:?}",
                header.id,
                header.round,
                current_weight,
                threshold,
                missing_stake,
                missing,
                self.used.iter().collect::<Vec<_>>()
            );
            // CRITICAL: Log với narwhal_audit target để dễ filter
            tracing::warn!(
                target: "narwhal_audit",
                "[VOTE AGGREGATION - INSUFFICIENT VOTES] Header {} (round {}, author: {}, {} batches) has insufficient votes: {}/{} stake (missing {}). Current voters ({}): {:?}. Missing votes from {} authorities: {:?}. This may be because other primaries are missing payload and cannot vote.",
                header.id,
                header.round,
                header.author,
                header.payload.len(),
                current_weight,
                threshold,
                missing_stake,
                current_voters.len(),
                current_voters,
                missing_authorities.len(),
                missing_authorities
            );
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
    ) -> DagResult<Option<Vec<Digest>>> {
        let origin = certificate.origin();

        // Ensure it is the first time this authority votes.
        if !self.used.insert(origin) {
            return Ok(None);
        }

        self.certificates.push(certificate.digest());
        self.weight += committee.stake(&origin);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(self.certificates.drain(..).collect()));
        }
        Ok(None)
    }
}
