// In primary/src/messages.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::{Epoch, Round};
use config::{Committee, WorkerId};
use crypto::{ConsensusPublicKey, Digest, Hash, PublicKey, Signature, SignatureService};
use serde::{Deserialize, Serialize};
use sha3::Digest as Sha3Digest;
use sha3::Sha3_512 as Sha512;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::convert::TryInto;
use std::fmt;
// dùng macro fully-qualified log::debug! để tránh cần import crate

#[derive(Clone, Serialize, Deserialize)]
pub struct Header {
    pub author: PublicKey,
    pub round: Round,
    pub epoch: Epoch, // BỔ SUNG: Thêm trường epoch
    pub payload: BTreeMap<Digest, WorkerId>,
    pub parents: BTreeSet<Digest>,
    pub id: Digest,
    pub signature: Signature,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            author: PublicKey::default(),
            round: 0,
            epoch: 0, // BỔ SUNG
            payload: BTreeMap::new(),
            parents: BTreeSet::new(),
            id: Digest::default(),
            signature: Signature::default(),
        }
    }
}

impl Header {
    pub async fn new(
        author: PublicKey,
        round: Round,
        epoch: Epoch, // BỔ SUNG
        payload: BTreeMap<Digest, WorkerId>,
        parents: BTreeSet<Digest>,
        signature_service: &mut SignatureService,
    ) -> Self {
        let header = Self {
            author,
            round,
            epoch, // BỔ SUNG
            payload,
            parents,
            id: Digest::default(),
            signature: Signature::default(),
        };
        let id = header.digest();
        let signature = signature_service.request_signature(id.clone()).await;
        Self {
            id,
            signature,
            ..header
        }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Đảm bảo header đến từ đúng kỷ nguyên.
        ensure!(self.epoch == committee.epoch, DagError::InvalidEpoch);

        ensure!(self.digest() == self.id, DagError::InvalidHeaderId);

        let voting_rights = committee.stake(&self.author);
        if voting_rights == 0 {
            log::info!(
                "[Header::verify] UnknownAuthority: author={}, round={}, epoch(header)={}, epoch(committee)={}, has_consensus_key={}",
                self.author,
                self.round,
                self.epoch,
                committee.epoch,
                committee.consensus_key(&self.author).is_some()
            );
            bail!(DagError::UnknownAuthority(self.author.clone()));
        }

        for worker_id in self.payload.values() {
            committee
                .worker(&self.author, &worker_id)
                .map_err(|_| DagError::MalformedHeader(self.id.clone()))?;
        }

        let consensus_pk = if let Some(pk) = committee.consensus_key(&self.author) {
            pk
        } else {
            log::info!(
                "[Header::verify] Missing consensus_key -> UnknownAuthority: author={}, round={}, epoch(header)={}, epoch(committee)={}",
                self.author,
                self.round,
                self.epoch,
                committee.epoch
            );
            bail!(DagError::UnknownAuthority(self.author.clone()));
        };
        self.signature
            .verify(&self.id, &consensus_pk)
            .map_err(DagError::from)
    }
}

impl Hash for Header {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.as_ref());
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes()); // BỔ SUNG: Hash cả epoch
        for (x, y) in &self.payload {
            hasher.update(x.as_ref());
            hasher.update(y.to_le_bytes());
        }
        for x in &self.parents {
            hasher.update(x.as_ref());
        }
        Digest((&hasher.finalize()[..32]).try_into().unwrap())
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: E{}B{}({}, {})", // BỔ SUNG: Thêm Epoch vào format
            self.id,
            self.epoch,
            self.round,
            self.author,
            self.payload.keys().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "E{}B{}({})", self.epoch, self.round, self.author)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub id: Digest,
    pub round: Round,
    pub epoch: Epoch, // BỔ SUNG
    pub origin: PublicKey,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        header: &Header,
        author: &PublicKey,
        signature_service: &mut SignatureService,
    ) -> Self {
        let vote = Self {
            id: header.id.clone(),
            round: header.round,
            epoch: header.epoch, // BỔ SUNG: Lấy epoch từ header
            origin: header.author.clone(),
            author: author.clone(),
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        ensure!(self.epoch == committee.epoch, DagError::InvalidEpoch);

        if committee.stake(&self.author) == 0 {
            log::info!(
                "[Vote::verify] UnknownAuthority: vote_author={}, origin={}, round={}, epoch(vote)={}, epoch(committee)={}, has_consensus_key={}",
                self.author,
                self.origin,
                self.round,
                self.epoch,
                committee.epoch,
                committee.consensus_key(&self.author).is_some()
            );
            bail!(DagError::UnknownAuthority(self.author.clone()));
        }

        let consensus_pk = if let Some(pk) = committee.consensus_key(&self.author) {
            pk
        } else {
            log::info!(
                "[Vote::verify] Missing consensus_key -> UnknownAuthority: vote_author={}, origin={}, round={}, epoch(vote)={}, epoch(committee)={}",
                self.author,
                self.origin,
                self.round,
                self.epoch,
                committee.epoch
            );
            bail!(DagError::UnknownAuthority(self.author.clone()));
        };
        self.signature
            .verify(&self.digest(), &consensus_pk)
            .map_err(DagError::from)
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.id.as_ref());
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes()); // BỔ SUNG
        hasher.update(self.origin.as_ref());
        Digest((&hasher.finalize()[..32]).try_into().unwrap())
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: V-E{}B{}({}, {})", // BỔ SUNG
            self.digest(),
            self.epoch,
            self.round,
            self.author,
            self.id
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Certificate {
    pub header: Header,
    pub votes: Vec<(PublicKey, Signature)>,
}

impl Certificate {
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities
            .keys()
            .map(|name| {
                // Tạo một header tạm thời chỉ để tính toán digest (id)
                let temp_header = Header {
                    author: name.clone(),
                    round: 0,
                    epoch: committee.epoch, // Sử dụng epoch chính xác từ committee
                    payload: BTreeMap::new(), // Genesis không có payload
                    parents: BTreeSet::new(), // Genesis không có parents
                    id: Digest::default(),  // Placeholder, sẽ được thay thế
                    signature: Signature::default(), // Genesis không có chữ ký
                };
                let header_id = temp_header.digest(); // Tính toán digest thực sự

                // Tạo Certificate cuối cùng với header_id chính xác
                Self {
                    header: Header {
                        id: header_id, // Sử dụng id đã tính toán
                        ..temp_header  // Giữ các trường khác từ temp_header
                    },
                    votes: Vec::new(), // Genesis không có votes
                }
            })
            .collect()
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        self.verify_with_active_cert_count(committee, None)
    }

    pub fn verify_with_active_cert_count(
        &self,
        committee: &Committee,
        active_cert_count: Option<usize>,
    ) -> DagResult<()> {
        // Genesis certs không cần verify đầy đủ.
        if self.header.round == 0 {
            // SỬA LỖI: Gọi epoch() như một phương thức
            ensure!(self.epoch() == committee.epoch, DagError::InvalidEpoch);
            return Ok(());
        }

        self.header.verify(committee)?;

        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes.iter() {
            ensure!(!used.contains(name), DagError::AuthorityReuse(name.clone()));
            let voting_rights = committee.stake(name);
            if voting_rights == 0 {
                log::info!(
                    "[Certificate::verify] UnknownAuthority in votes: voter={}, header_author={}, round={}, epoch(cert)={}, epoch(committee)={}, has_consensus_key={}",
                    name,
                    self.origin(),
                    self.round(),
                    self.epoch(),
                    committee.epoch,
                    committee.consensus_key(name).is_some()
                );
                bail!(DagError::UnknownAuthority(name.clone()));
            }
            used.insert(name.clone());
            weight += voting_rights;
        }

        // *** ƯU TIÊN: Sử dụng số parents trong header để tính quorum động (quyết định tất định theo header) ***
        let parents_count = self.header.parents.len();
        let quorum_threshold = if parents_count > 0 {
            committee.quorum_threshold_dynamic(parents_count)
        } else if let Some(active_count) = active_cert_count {
            if active_count > 0 {
                committee.quorum_threshold_dynamic(active_count)
            } else {
                committee.quorum_threshold()
            }
        } else {
            committee.quorum_threshold()
        };

        ensure!(
            weight >= quorum_threshold,
            DagError::CertificateRequiresQuorum
        );

        let votes_to_verify: DagResult<Vec<(ConsensusPublicKey, Signature)>> = self
            .votes
            .iter()
            .map(|(pk, sig)| {
                if let Some(consensus_pk) = committee.consensus_key(pk) {
                    Ok((consensus_pk, sig.clone()))
                } else {
                    log::info!(
                        "[Certificate::verify] Missing consensus_key -> UnknownAuthority: voter={}, header_author={}, round={}, epoch(cert)={}, epoch(committee)={}",
                        pk,
                        self.origin(),
                        self.round(),
                        self.epoch(),
                        committee.epoch
                    );
                    Err(DagError::UnknownAuthority(pk.clone()))
                }
            })
            .collect();

        // Sử dụng digest của Vote, đã bao gồm epoch
        let vote_digest = Vote {
            id: self.header.id.clone(),
            round: self.round(),
            epoch: self.epoch(),
            origin: self.origin(),
            author: PublicKey::default(), // Author không quan trọng khi chỉ cần digest
            signature: Signature::default(),
        }
        .digest();

        Signature::verify_batch(&vote_digest, &votes_to_verify?).map_err(DagError::from)
    }

    pub fn round(&self) -> Round {
        self.header.round
    }

    pub fn epoch(&self) -> Epoch {
        self.header.epoch
    }

    pub fn origin(&self) -> PublicKey {
        self.header.author.clone()
    }
}

impl Hash for Certificate {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.header.id.as_ref());
        hasher.update(self.round().to_le_bytes());
        hasher.update(self.epoch().to_le_bytes()); // BỔ SUNG
        hasher.update(self.origin().as_ref());
        Digest((&hasher.finalize()[..32]).try_into().unwrap())
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: C-E{}B{}({}, {})", // BỔ SUNG
            self.digest(),
            self.epoch(),
            self.round(),
            self.origin(),
            self.header.id
        )
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}
