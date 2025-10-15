// In primary/src/core.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, VotesAggregator};
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use crate::primary::{PrimaryMessage, Round};
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use store::{Store, ROUND_INDEX_CF};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

const SYNC_CHUNK_SIZE: Round = 1000;
const SYNC_RETRY_DELAY: u64 = 5_000;
const SYNC_MAX_RETRIES: u32 = 10;

struct SyncState {
    final_target_round: Round,
    current_chunk_target: Round,
    retry_count: u32,
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    store: Store,
    synchronizer: Synchronizer,
    signature_service: SignatureService,
    consensus_round: Arc<AtomicU64>,
    gc_depth: Round,
    rx_primaries: Receiver<PrimaryMessage>,
    rx_header_waiter: Receiver<Header>,
    rx_certificate_waiter: Receiver<Certificate>,
    rx_proposer: Receiver<Header>,
    tx_consensus: Sender<Certificate>,
    tx_proposer: Sender<(Vec<Digest>, Round)>,
    tx_primaries: Sender<PrimaryMessage>,
    gc_round: Round,
    dag_round: Round,
    last_voted: HashMap<Round, HashSet<PublicKey>>,
    processing: HashMap<Round, HashSet<Digest>>,
    current_header: Header,
    votes_aggregator: VotesAggregator,
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>,
    network: ReliableSender,
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>,
    sync_state: Option<SyncState>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        synchronizer: Synchronizer,
        signature_service: SignatureService,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_primaries: Receiver<PrimaryMessage>,
        rx_header_waiter: Receiver<Header>,
        rx_certificate_waiter: Receiver<Certificate>,
        rx_proposer: Receiver<Header>,
        tx_primaries: Sender<PrimaryMessage>,
        tx_consensus: Sender<Certificate>,
        tx_proposer: Sender<(Vec<Digest>, Round)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                synchronizer,
                signature_service,
                consensus_round,
                gc_depth,
                rx_primaries,
                rx_header_waiter,
                rx_certificate_waiter,
                rx_proposer,
                tx_consensus,
                tx_proposer,
                tx_primaries,
                gc_round: 0,
                dag_round: 0,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(),
                votes_aggregator: VotesAggregator::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
                sync_state: None,
            }
            .run()
            .await;
        });
    }

    async fn request_sync_chunk(&mut self, start: Round, end: Round) {
        info!(
            "Requesting certificate sync chunk from round {} to {}",
            start, end
        );
        let message = PrimaryMessage::CertificateRangeRequest {
            start_round: start,
            end_round: end,
            requestor: self.name.clone(),
        };
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&message).expect("Failed to serialize cert range request");
        self.network.broadcast(addresses, Bytes::from(bytes)).await;
    }

    async fn advance_sync(&mut self) {
        if let Some(state) = &mut self.sync_state {
            if self.dag_round >= state.final_target_round {
                info!(
                    "Synchronization complete. Now at round {}. Switching to Running state.",
                    self.dag_round
                );
                self.sync_state = None;
                self.last_voted.clear();
                self.processing.clear();
                return;
            }

            if state.retry_count >= SYNC_MAX_RETRIES {
                warn!(
                    "Sync failed after {} retries for target round {}. Forcing exit from Syncing state at round {}",
                    SYNC_MAX_RETRIES, state.current_chunk_target, self.dag_round
                );
                self.sync_state = None;
                self.last_voted.clear();
                self.processing.clear();
                return;
            }

            let start = self.dag_round + 1;
            let end = (start + SYNC_CHUNK_SIZE - 1).min(state.final_target_round);
            state.current_chunk_target = end;
            state.retry_count += 1;

            info!(
                "Sync attempt #{}: requesting rounds {} to {} (final target {})",
                state.retry_count, start, end, state.final_target_round
            );
            self.request_sync_chunk(start, end).await;
        }
    }

    async fn process_own_header(&mut self, header: Header) -> DagResult<()> {
        let round = header.round;
        self.dag_round = round;
        self.current_header = header.clone();
        self.votes_aggregator = VotesAggregator::new();

        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
            .expect("Failed to serialize header");
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;

        self.cancel_handlers
            .entry(round)
            .or_insert_with(Vec::new)
            .extend(handlers);

        // Not syncing, so pass `false`
        self.process_header(&header, false).await
    }

    #[async_recursion]
    async fn process_header(&mut self, header: &Header, syncing: bool) -> DagResult<()> {
        debug!("Processing {:?}", header);
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());
        let parents = self.synchronizer.get_parents(header).await?;
        if parents.is_empty() {
            debug!("Processing of {} suspended: missing parent(s)", header.id);
            return Ok(());
        }
        let mut stake = 0;
        for x in parents {
            ensure!(
                x.round() + 1 == header.round,
                DagError::MalformedHeader(header.id.clone())
            );
            stake += self.committee.stake(&x.origin());
        }
        ensure!(
            stake >= self.committee.quorum_threshold(),
            DagError::HeaderRequiresQuorum(header.id.clone())
        );
        if self.synchronizer.missing_payload(header).await? {
            debug!("Processing of {} suspended: missing payload", header);
            return Ok(());
        }
        let bytes = bincode::serialize(header).expect("Failed to serialize header");
        self.store.write(header.id.to_vec(), bytes).await;

        if !syncing {
            if self
                .last_voted
                .entry(header.round)
                .or_insert_with(HashSet::new)
                .insert(header.author)
            {
                let vote = Vote::new(header, &self.name, &mut self.signature_service).await;
                debug!("Created {:?}", vote);
                if vote.origin == self.name {
                    self.process_vote(vote)
                        .await
                        .expect("Failed to process our own vote");
                } else {
                    let address = self
                        .committee
                        .primary(&header.author)
                        .expect("Author not in committee")
                        .primary_to_primary;
                    let bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
                        .expect("Failed to serialize vote");
                    let handler = self.network.send(address, Bytes::from(bytes)).await;
                    self.cancel_handlers
                        .entry(header.round)
                        .or_insert_with(Vec::new)
                        .push(handler);
                }
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        debug!("Processing {:?}", vote);
        if let Some(certificate) =
            self.votes_aggregator
                .append(vote, &self.committee, &self.current_header)?
        {
            debug!("Assembled {:?}", certificate);

            let cert_round = certificate.round();
            let addresses = self
                .committee
                .others_primaries(&self.name)
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect();
            let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate.clone()))
                .expect("Failed to serialize certificate");
            let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;

            // Not syncing, so pass `false`
            self.process_certificate(certificate, false)
                .await
                .expect("Failed to process valid certificate");

            self.cancel_handlers
                .entry(cert_round)
                .or_insert_with(Vec::new)
                .extend(handlers);
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_certificate(
        &mut self,
        certificate: Certificate,
        syncing: bool,
    ) -> DagResult<()> {
        debug!("Processing {:?}", certificate);
        if !self
            .processing
            .get(&certificate.header.round)
            .map_or(false, |x| x.contains(&certificate.header.id))
        {
            // Pass the syncing flag down
            self.process_header(&certificate.header, syncing).await?;
        }
        if !self.synchronizer.deliver_certificate(&certificate).await? {
            debug!(
                "Processing of {:?} suspended: missing ancestors",
                certificate
            );
            return Ok(());
        }
        let digest = certificate.digest();
        if self.store.read(digest.to_vec()).await?.is_none() {
            let value =
                bincode::serialize(&certificate).map_err(|e| DagError::SerializationError(e))?;
            self.store.write(digest.to_vec(), value).await;
            let round_key = certificate.round().to_le_bytes().to_vec();
            let cf_name = ROUND_INDEX_CF.to_string();
            let mut digests: Vec<Digest> = self
                .store
                .read_cf(cf_name.clone(), round_key.clone())
                .await?
                .map(|v| bincode::deserialize(&v).unwrap_or_default())
                .unwrap_or_default();
            if !digests.contains(&digest) {
                digests.push(digest);
                let digests_value =
                    bincode::serialize(&digests).map_err(|e| DagError::SerializationError(e))?;
                self.store.write_cf(cf_name, round_key, digests_value).await;
            }
        }
        if let Some(parents) = self
            .certificates_aggregators
            .entry(certificate.round())
            .or_insert_with(|| Box::new(CertificatesAggregator::new()))
            .append(certificate.clone(), &self.committee)?
        {
            if !syncing {
                self.tx_proposer
                    .send((parents, certificate.round()))
                    .await
                    .expect("Failed to send certificate to proposer");
            }
        }
        let id = certificate.header.id.clone();
        if let Err(e) = self.tx_consensus.send(certificate).await {
            warn!(
                "Failed to deliver certificate {} to the consensus: {}",
                id, e
            );
        }
        Ok(())
    }

    fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        ensure!(
            self.gc_round <= header.round,
            DagError::TooOld(header.id.clone(), header.round)
        );
        header.verify(&self.committee)
    }

    fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        ensure!(
            self.current_header.round <= vote.round,
            DagError::TooOld(vote.digest(), vote.round)
        );
        ensure!(
            vote.id == self.current_header.id
                && vote.origin == self.current_header.author
                && vote.round == self.current_header.round,
            DagError::UnexpectedVote(vote.id.clone())
        );
        vote.verify(&self.committee).map_err(DagError::from)
    }

    fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        ensure!(
            self.gc_round <= certificate.round(),
            DagError::TooOld(certificate.digest(), certificate.round())
        );
        certificate.verify(&self.committee).map_err(DagError::from)
    }

    async fn handle_message(&mut self, message: PrimaryMessage) -> DagResult<()> {
        if self.sync_state.is_some() {
            // Logic when in sync state.
            match message {
                PrimaryMessage::CertificateBundle(certificates) => {
                    let mut state = self.sync_state.take().unwrap();
                    if certificates.is_empty() {
                        warn!("Received empty certificate bundle, will retry sync");
                    } else {
                        info!(
                            "Processing a sync bundle of {} certificates.",
                            certificates.len()
                        );
                        let mut latest_round_in_bundle = self.dag_round;
                        for certificate in certificates {
                            if self.sanitize_certificate(&certificate).is_ok() {
                                // We are in sync state, so pass `true`
                                if self
                                    .process_certificate(certificate.clone(), true)
                                    .await
                                    .is_ok()
                                {
                                    latest_round_in_bundle =
                                        latest_round_in_bundle.max(certificate.round());
                                }
                            }
                        }
                        if latest_round_in_bundle > self.dag_round {
                            self.dag_round = latest_round_in_bundle;
                        }
                        state.retry_count = 0;
                    }
                    self.sync_state = Some(state);
                    self.advance_sync().await;
                }
                PrimaryMessage::Certificate(certificate) => {
                    let target_round = self.sync_state.as_ref().map_or(0, |s| s.final_target_round);
                    let cert_round = certificate.round();

                    if cert_round > target_round && self.sanitize_certificate(&certificate).is_ok()
                    {
                        info!(
                            "Sync target updated to a newer round {}. Still syncing.",
                            cert_round
                        );
                        if let Some(state) = self.sync_state.as_mut() {
                            state.final_target_round = cert_round;
                        }
                    }
                }
                _ => {}
            }
            return Ok(());
        }

        // Logic when not in sync state
        match message {
            PrimaryMessage::Header(header) => {
                self.sanitize_header(&header)?;
                // Not syncing, so pass `false`
                self.process_header(&header, false).await
            }
            PrimaryMessage::Vote(vote) => {
                self.sanitize_vote(&vote)?;
                self.process_vote(vote).await
            }
            PrimaryMessage::Certificate(certificate) => {
                const LAG_THRESHOLD: Round = 50;
                if certificate.round() > self.dag_round.saturating_add(LAG_THRESHOLD) {
                    info!(
                        "We are lagging by {} rounds. Switching to Syncing state to catch up to round {}",
                        certificate.round() - self.dag_round,
                        certificate.round()
                    );
                    self.sync_state = Some(SyncState {
                        final_target_round: certificate.round(),
                        current_chunk_target: 0,
                        retry_count: 0,
                    });
                    self.advance_sync().await;
                } else {
                    self.sanitize_certificate(&certificate)?;
                    self.dag_round = self.dag_round.max(certificate.round());
                    // Not syncing, so pass `false`
                    self.process_certificate(certificate, false).await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub async fn run(&mut self) {
        let mut sync_retry_timer = tokio::time::interval(Duration::from_millis(SYNC_RETRY_DELAY));
        sync_retry_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let result = tokio::select! {
                Some(message) = self.rx_primaries.recv() => self.handle_message(message).await,
                Some(header) = self.rx_header_waiter.recv(), if self.sync_state.is_none() => self.process_header(&header, false).await,
                Some(certificate) = self.rx_certificate_waiter.recv(), if self.sync_state.is_none() => self.process_certificate(certificate, false).await,
                Some(header) = self.rx_proposer.recv(), if self.sync_state.is_none() => self.process_own_header(header).await,

                _ = sync_retry_timer.tick(), if self.sync_state.is_some() => {
                    warn!("Sync request timed out. Retrying...");
                    self.advance_sync().await;
                    Ok(())
                },
            };
            match result {
                Ok(()) => (),
                Err(DagError::StoreError(e)) => {
                    error!("{}", e);
                    panic!("Storage failure: killing node.");
                }
                Err(e @ DagError::TooOld(..)) => debug!("{}", e),
                Err(e) => warn!("{}", e),
            }

            if self.sync_state.is_none() {
                let round = self.consensus_round.load(Ordering::Relaxed);
                if round > self.gc_depth {
                    let gc_round = round - self.gc_depth;
                    self.last_voted.retain(|k, _| k >= &gc_round);
                    self.processing.retain(|k, _| k >= &gc_round);
                    self.certificates_aggregators.retain(|k, _| k >= &gc_round);
                    self.cancel_handlers.retain(|k, _| k >= &gc_round);
                    self.gc_round = gc_round;
                }
            }
        }
    }
}
