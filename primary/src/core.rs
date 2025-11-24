// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, VotesAggregator};
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use crate::primary::{BatchRescue, PayloadCache, PrimaryMessage, Round};
// RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{Duration, Instant};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub struct Core {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Handles synchronization with other nodes and our workers.
    synchronizer: Synchronizer,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receiver for dag messages (headers, votes, certificates).
    rx_primaries: Receiver<PrimaryMessage>,
    /// Receives loopback headers from the `HeaderWaiter`.
    rx_header_waiter: Receiver<Header>,
    /// Receives loopback certificates from the `CertificateWaiter`.
    rx_certificate_waiter: Receiver<Certificate>,
    /// Receives our newly created headers from the `Proposer`.
    rx_proposer: Receiver<Header>,
    /// Output all certificates to the consensus layer.
    tx_consensus: Sender<Certificate>,
    /// Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    tx_proposer: Sender<(Vec<Digest>, Round)>,
    /// Send verified headers to the `Proposer` for batch extraction.
    tx_headers: Sender<Header>,
    /// Receives batch rescue events from the proposer (to replicate stuck batches).
    rx_batch_rescue: Receiver<BatchRescue>,
    /// Shared payload cache for storing replicated batches.
    payload_cache: PayloadCache,

    /// The last garbage collected round.
    gc_round: Round,
    /// The authors of the last voted headers.
    last_voted: HashMap<Round, HashSet<PublicKey>>,
    /// The set of headers we are currently processing.
    processing: HashMap<Round, HashSet<Digest>>,
    /// The last header we proposed (for which we are waiting votes).
    current_header: Header,
    /// Aggregates votes into a certificate.
    votes_aggregator: VotesAggregator,
    /// Aggregates certificates to use as parents for new headers.
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>,
    /// A network sender to send the batches to the other workers.
    network: ReliableSender,
    /// Keeps the cancel handlers of the messages we sent.
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>,
    // RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
    /// PHASE 2: Track last catch-up sync check time for proactive synchronization
    last_catchup_sync_check: Option<Instant>,
    /// PHASE 2: Interval for catch-up sync check (check every 5 seconds)
    catchup_sync_check_interval: Duration,
    /// CATCH-UP MODE: Track if node is in catch-up mode
    is_catchup_mode: bool,
    /// CATCH-UP MODE: Last time we entered catch-up mode
    catchup_mode_entered_at: Option<Instant>,
    /// CATCH-UP MODE: Last detected lag
    last_detected_lag: Round,
    /// CATCH-UP MODE: Channel to notify proposer about catch-up mode
    tx_proposer_catchup: Sender<bool>,
    /// CATCH-UP MODE: Track highest round seen from network (to compare with our proposer round)
    highest_network_round: Round,
    /// CATCH-UP MODE: Track our current proposer round (updated from certificates aggregators and headers from proposer)
    current_proposer_round: Round,
    /// CATCH-UP MODE: Track proposer round from headers we created (most accurate)
    proposer_round_from_headers: Round,
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
        tx_consensus: Sender<Certificate>,
        tx_proposer: Sender<(Vec<Digest>, Round)>,
        tx_headers: Sender<Header>,
        rx_batch_rescue: Receiver<BatchRescue>,
        payload_cache: PayloadCache,
        // RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
        tx_proposer_catchup: Sender<bool>, // CATCH-UP MODE: Channel to notify proposer
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
                tx_headers,
                rx_batch_rescue,
                payload_cache,
                gc_round: 0,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(),
                votes_aggregator: VotesAggregator::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
                // RATE CONTROL ĐÃ BỊ BỎ - Không còn sử dụng
                last_catchup_sync_check: Some(Instant::now()),
                catchup_sync_check_interval: Duration::from_secs(2), // Check every 2 seconds để phát hiện lag sớm hơn
                // CATCH-UP MODE: Initialize catch-up mode state
                is_catchup_mode: false,
                catchup_mode_entered_at: None,
                last_detected_lag: 0,
                tx_proposer_catchup,
                highest_network_round: 0,
                current_proposer_round: 1,
                proposer_round_from_headers: 1,
            }
            .run()
            .await;
        });
    }

    async fn process_own_header(&mut self, header: Header) -> DagResult<()> {
        // CATCH-UP MODE: Update proposer round from our own headers (most accurate)
        if header.round > self.proposer_round_from_headers {
            self.proposer_round_from_headers = header.round;
            debug!(
                "[CATCH-UP TRACKING] Updated proposer_round_from_headers to {} (from header round {})",
                self.proposer_round_from_headers, header.round
            );
        }
        
        // Reset the votes aggregator.
        self.current_header = header.clone();
        self.votes_aggregator = VotesAggregator::new();

        // Broadcast the new header in a reliable manner.
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
            .expect("Failed to serialize our own header");
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers
            .entry(header.round)
            .or_insert_with(Vec::new)
            .extend(handlers);

        // Process the header.
        self.process_header(&header).await
    }

    #[async_recursion]
    async fn process_header(&mut self, header: &Header) -> DagResult<()> {
        debug!("Processing {:?}", header);
        // RATE CONTROL ĐÃ BỊ BỎ - Không còn record header
        
        // CATCH-UP MODE: Update highest network round when receiving headers
        if header.round > self.highest_network_round {
            self.highest_network_round = header.round;
        }
        
        // Indicate that we are processing this header.
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());

        // Ensure we have the parents. If at least one parent is missing, the synchronizer returns an empty
        // vector; it will gather the missing parents (as well as all ancestors) from other nodes and then
        // reschedule processing of this header.
        let parents = self.synchronizer.get_parents(header).await?;
        if parents.is_empty() {
            debug!("Processing of {} suspended: missing parent(s)", header.id);
            return Ok(());
        }

        // Check the parent certificates. Ensure the parents form a quorum and are all from the previous round.
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

        // Ensure we have the payload. If we don't, the synchronizer will ask our workers to get it, and then
        // reschedule processing of this header once we have it.
        if self.synchronizer.missing_payload(header).await? {
            debug!("Processing of {} suspended: missing payload", header);
            return Ok(());
        }

        // Store the header.
        let bytes = bincode::serialize(header).expect("Failed to serialize header");
        self.store.write(header.id.to_vec(), bytes).await;

        // NOTE: Header was already sent to proposer EARLY (right after signature verification)
        // in the message handler. This reduces delay significantly and prevents batches from
        // being stuck. We don't send it again here to avoid duplicate processing.

        // Check if we can vote for this header.
        if self
            .last_voted
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.author)
        {
            // Make a vote and send it to the header's creator.
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
                    .expect("Author of valid header is not in the committee")
                    .primary_to_primary;
                let bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
                    .expect("Failed to serialize our own vote");
                let handler = self.network.send(address, Bytes::from(bytes)).await;
                self.cancel_handlers
                    .entry(header.round)
                    .or_insert_with(Vec::new)
                    .push(handler);
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        debug!("Processing {:?}", vote);

        // Add it to the votes' aggregator and try to make a new certificate.
        if let Some(certificate) =
            self.votes_aggregator
                .append(vote, &self.committee, &self.current_header)?
        {
            info!(
                "[BATCH TRACK CERTIFICATE] Core {} assembled certificate {} (round {}) from header {} (author: {}) containing {} batches: {:?}",
                self.name,
                certificate.digest(),
                certificate.round(),
                certificate.header.id,
                certificate.origin(),
                certificate.header.payload.len(),
                certificate.header.payload.keys().take(5).collect::<Vec<_>>()
            );
            debug!("Assembled {:?}", certificate);

            // Broadcast the certificate.
            let addresses = self
                .committee
                .others_primaries(&self.name)
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect();
            let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate.clone()))
                .expect("Failed to serialize our own certificate");
            let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
            self.cancel_handlers
                .entry(certificate.round())
                .or_insert_with(Vec::new)
                .extend(handlers);

            // Process the new certificate.
            self.process_certificate(certificate)
                .await
                .expect("Failed to process valid certificate");
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
        debug!("Processing {:?}", certificate);
        // RATE CONTROL ĐÃ BỊ BỎ - Không còn record certificate
        
        // CATCH-UP MODE: Update highest network round when receiving certificates
        let cert_round = certificate.round();
        if cert_round > self.highest_network_round {
            self.highest_network_round = cert_round;
        }

        // Process the header embedded in the certificate if we haven't already voted for it (if we already
        // voted, it means we already processed it). Since this header got certified, we are sure that all
        // the data it refers to (ie. its payload and its parents) are available. We can thus continue the
        // processing of the certificate even if we don't have them in store right now.
        if !self
            .processing
            .get(&certificate.header.round)
            .map_or_else(|| false, |x| x.contains(&certificate.header.id))
        {
            // This function may still throw an error if the storage fails.
            self.process_header(&certificate.header).await?;
        }

        // Ensure we have all the ancestors of this certificate yet. If we don't, the synchronizer will gather
        // them and trigger re-processing of this certificate.
        if !self.synchronizer.deliver_certificate(&certificate).await? {
            debug!(
                "Processing of {:?} suspended: missing ancestors",
                certificate
            );
            return Ok(());
        }

        // Store the certificate.
        let bytes = bincode::serialize(&certificate).expect("Failed to serialize certificate");
        self.store.write(certificate.digest().to_vec(), bytes).await;

        // Check if we have enough certificates to enter a new dag round and propose a header.
        if let Some(parents) = self
            .certificates_aggregators
            .entry(certificate.round())
            .or_insert_with(|| Box::new(CertificatesAggregator::new()))
            .append(certificate.clone(), &self.committee)?
        {
            let header_round = certificate.round();
            let mut parent_summaries = Vec::new();
            let mut missing_in_store = Vec::new();

            for digest in &parents {
                match self.store.read(digest.to_vec()).await {
                    Ok(Some(bytes)) => match bincode::deserialize::<Certificate>(&bytes) {
                        Ok(parent_cert) => {
                            parent_summaries.push(format!(
                                "{{round: {}, origin: {:?}}}",
                                parent_cert.round(),
                                parent_cert.origin()
                            ));
                        }
                        Err(e) => {
                            parent_summaries
                                .push(format!("{{digest: {:?}, decode_error: {}}}", digest, e));
                        }
                    },
                    Ok(None) => {
                        missing_in_store.push(format!("{:?}", digest));
                    }
                    Err(e) => {
                        missing_in_store.push(format!("{:?} (store_error: {})", digest, e));
                    }
                }
            }

            info!(
                "Core: preparing header round {} with {} parents. parents_details={:?} missing_in_store={:?}",
                header_round,
                parents.len(),
                parent_summaries,
                missing_in_store
            );

            // Send it to the `Proposer`.
            self.tx_proposer
                .send((parents, certificate.round()))
                .await
                .expect("Failed to send certificate");
        }

        // Quick check: Đọc consensus state để tránh gửi certificate đã commit
        let consensus_state_key = b"consensus_state".to_vec();
        if let Ok(Some(bytes)) = self.store.read(consensus_state_key.clone()).await {
            #[derive(serde::Deserialize)]
            struct ConsensusState {
                last_committed_round: Round,
            }
            if let Ok(state) = bincode::deserialize::<ConsensusState>(&bytes) {
                if certificate.round() <= state.last_committed_round {
                    debug!(
                        "Certificate {} already committed (round {} <= {}), skipping consensus",
                        certificate.digest(),
                        certificate.round(),
                        state.last_committed_round
                    );
                    return Ok(());
                }
            }
        }

        // CATCH-UP MODE: Skip consensus when in catch-up mode and lag is too large
        // This prevents node from voting on certificates when it doesn't have enough context
        // Giảm threshold xuống để skip consensus sớm hơn khi lag
        const LAG_SKIP_CONSENSUS_THRESHOLD: Round = 20; // Skip consensus if lag > 20 rounds (giảm từ 200)
        if self.is_catchup_mode && self.last_detected_lag > LAG_SKIP_CONSENSUS_THRESHOLD {
            debug!(
                "[CATCH-UP MODE] Skipping consensus for certificate {} (round {}) - node is catching up (lag: {} rounds). Certificate still processed for state.",
                certificate.digest(),
                certificate.round(),
                self.last_detected_lag
            );
            // Still return Ok() - certificate is processed for state, just not sent to consensus
            return Ok(());
        }

        info!(
            "[BATCH TRACK CONSENSUS] Core {} sending certificate {} (round {}) to consensus. Certificate contains {} batches: {:?}",
            self.name,
            certificate.digest(),
            certificate.round(),
            certificate.header.payload.len(),
            certificate.header.payload.keys().take(5).collect::<Vec<_>>()
        );

        // Send it to the consensus layer.
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
        // PHASE 2: Soft reject for old headers to support catch-up
        // If header is too old but within catch-up window, trigger sync but still reject processing
        if header.round < self.gc_round {
            let round_diff = self.gc_round.saturating_sub(header.round);
            const MAX_CATCHUP_ROUNDS: Round = 50000; // Tăng lên 50000 rounds để hỗ trợ node lag nhiều hơn

            if round_diff <= MAX_CATCHUP_ROUNDS {
                // PHASE 2: Header is old but within catch-up window - trigger sync for catch-up
                // This allows node chậm to sync headers/certificates from old rounds
                warn!(
                    "[CATCH-UP SYNC] Header {} (round {}) is {} rounds behind (gc_round={}, current_round≈{}). This header is too old to process immediately, but will trigger sync for catch-up. Node may be lagging behind - attempting to sync old data.",
                    header.id,
                    header.round,
                    round_diff,
                    self.gc_round,
                    self.gc_round + self.gc_depth
                );

                // PHASE 2: Trigger proactive sync for this header to help catch-up
                // Sync parents of this header so node can eventually catch up
                // Note: We'll trigger sync in the main loop after returning from sanitize
                // This header will be processed later when sync completes
            } else {
                // Header is too far behind - skip sync (would be inefficient)
                debug!(
                    "[CATCH-UP] Header {} (round {}) is {} rounds behind (gc_round={}). Too far behind for catch-up sync (max: {} rounds). Skipping.",
                    header.id,
                    header.round,
                    round_diff,
                    self.gc_round,
                    MAX_CATCHUP_ROUNDS
                );
            }

            // Still reject processing to avoid issues with old headers
            return Err(DagError::TooOld(header.id.clone(), header.round));
        }

        // Verify the header's signature.
        header.verify(&self.committee)?;

        // TODO [issue #3]: Prevent bad nodes from sending junk headers with high round numbers.

        Ok(())
    }

    /// PHASE 2: Trigger catch-up sync for old header
    /// This method helps node chậm sync headers/certificates from old rounds
    async fn trigger_catchup_sync(&mut self, header: &Header) {
        // Try to get parents to trigger sync if they're missing
        // This will automatically trigger sync request via synchronizer
        match self.synchronizer.get_parents(header).await {
            Ok(_parents) => {
                // Parents are available - no sync needed
                debug!(
                    "[CATCH-UP SYNC] Parents for old header {} (round {}) are already available",
                    header.id, header.round
                );
            }
            Err(_) => {
                // Parents are missing - sync will be triggered automatically by synchronizer
                debug!(
                    "[CATCH-UP SYNC] Parents for old header {} (round {}) are missing - sync triggered",
                    header.id,
                    header.round
                );
            }
        }
    }

    fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        ensure!(
            self.current_header.round <= vote.round,
            DagError::TooOld(vote.digest(), vote.round)
        );

        // Ensure we receive a vote on the expected header.
        ensure!(
            vote.id == self.current_header.id
                && vote.origin == self.current_header.author
                && vote.round == self.current_header.round,
            DagError::UnexpectedVote(vote.id.clone())
        );

        // Verify the vote.
        vote.verify(&self.committee).map_err(DagError::from)
    }

    fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        // PHASE 2: Soft reject for old certificates to support catch-up
        // Similar to headers, allow catch-up sync for certificates within catch-up window
        if certificate.round() < self.gc_round {
            let round_diff = self.gc_round.saturating_sub(certificate.round());
            const MAX_CATCHUP_ROUNDS: Round = 50000; // Tăng lên 50000 rounds để hỗ trợ node lag nhiều hơn

            if round_diff <= MAX_CATCHUP_ROUNDS {
                // PHASE 2: Certificate is old but within catch-up window - trigger sync for catch-up
                warn!(
                    "[CATCH-UP SYNC] Certificate {} (round {}) is {} rounds behind (gc_round={}, current_round≈{}). This certificate is too old to process immediately, but will trigger sync for catch-up. Node may be lagging behind - attempting to sync old data.",
                    certificate.digest(),
                    certificate.round(),
                    round_diff,
                    self.gc_round,
                    self.gc_round + self.gc_depth
                );

                // PHASE 2: Trigger sync for certificate ancestors to help catch-up
                // This will be handled in the main loop after returning from sanitize
            } else {
                // Certificate is too far behind - skip sync (would be inefficient)
                debug!(
                    "[CATCH-UP] Certificate {} (round {}) is {} rounds behind (gc_round={}). Too far behind for catch-up sync (max: {} rounds). Skipping.",
                    certificate.digest(),
                    certificate.round(),
                    round_diff,
                    self.gc_round,
                    MAX_CATCHUP_ROUNDS
                );
            }

            // Still reject processing to avoid issues with old certificates
            return Err(DagError::TooOld(certificate.digest(), certificate.round()));
        }

        // Verify the certificate (and the embedded header).
        certificate.verify(&self.committee).map_err(DagError::from)
    }

    /// PHASE 2: Periodic catch-up sync check
    /// This method helps node chậm proactively sync missing certificates from recent rounds
    /// CATCH-UP MODE: Now also tracks and manages catch-up mode state
    async fn periodic_catchup_sync_check(&mut self, current_round: Round) -> DagResult<()> {
        // Update last check time
        self.last_catchup_sync_check = Some(Instant::now());

        // CATCH-UP MODE: Update highest network round from certificates aggregators
        // The highest round in certificates_aggregators represents the highest round we've seen from network
        let old_highest_network_round = self.highest_network_round;
        if let Some(max_round) = self.certificates_aggregators.keys().max().copied() {
            if max_round > self.highest_network_round {
                self.highest_network_round = max_round;
                debug!(
                    "[CATCH-UP TRACKING] Updated highest_network_round from {} to {} (from certificates_aggregators)",
                    old_highest_network_round, self.highest_network_round
                );
            }
        }
        
        // CATCH-UP MODE: Update current proposer round from the highest round we can create headers for
        // This is the highest round where we have quorum of certificates from previous round
        let old_proposer_round = self.current_proposer_round;
        if let Some(max_round) = self.certificates_aggregators.keys().max().copied() {
            // Our proposer round is max_round + 1 (we can propose headers for this round)
            self.current_proposer_round = max_round + 1;
            if self.current_proposer_round != old_proposer_round {
                debug!(
                    "[CATCH-UP TRACKING] Updated current_proposer_round from {} to {} (from certificates_aggregators max_round: {})",
                    old_proposer_round, self.current_proposer_round, max_round
                );
            }
        }
        
        // CATCH-UP MODE: Use the higher of current_proposer_round (from certificates) and proposer_round_from_headers
        // proposer_round_from_headers is more accurate as it's from actual headers we created
        let actual_proposer_round = self.current_proposer_round.max(self.proposer_round_from_headers);
        if actual_proposer_round != self.current_proposer_round {
            debug!(
                "[CATCH-UP TRACKING] Using proposer_round_from_headers {} instead of current_proposer_round {} (more accurate)",
                self.proposer_round_from_headers, self.current_proposer_round
            );
            self.current_proposer_round = actual_proposer_round;
        }

        // CATCH-UP MODE: Calculate lag as difference between network's highest round and our proposer round
        // Use the higher of consensus_round and highest_network_round as network current round
        let network_current_round = current_round.max(self.highest_network_round);
        let lag = network_current_round.saturating_sub(self.current_proposer_round);
        
        // LOGGING: Always log catch-up check details for monitoring
        info!(
            "[CATCH-UP SYNC] Periodic check - our_proposer_round: {} (from_certs: {}, from_headers: {}), network_round: {} (consensus: {}, highest_network: {}), lag: {} rounds, certificates_aggregators_count: {}, gc_round: {}",
            self.current_proposer_round,
            self.current_proposer_round.saturating_sub(1), // approximate from certs
            self.proposer_round_from_headers,
            network_current_round,
            current_round,
            self.highest_network_round,
            lag,
            self.certificates_aggregators.len(),
            self.gc_round
        );

        // CATCH-UP MODE: Thresholds for entering and resuming catch-up mode
        // Tăng threshold lên để tránh vào catch-up mode quá sớm (50 rounds thay vì 10)
        // Lag 10 rounds là bình thường trong DAG và không nên trigger catch-up mode
        // Điều này tránh proposer bị pause khiến giao dịch không được thực thi
        const LAG_THRESHOLD: Round = 50; // Enter catch-up mode if lag >= 50 rounds
        const RESUME_THRESHOLD: Round = 30; // Resume normal operation if lag < 30 rounds

        // Update last detected lag
        self.last_detected_lag = lag;

        // CATCH-UP MODE: Enter catch-up mode if lag is significant
        if lag >= LAG_THRESHOLD {
            if !self.is_catchup_mode {
                // Enter catch-up mode
                self.is_catchup_mode = true;
                self.catchup_mode_entered_at = Some(Instant::now());
                warn!(
                    "[CATCH-UP MODE] Primary {} ENTERING catch-up mode - lag: {} rounds (>= threshold: {}) (our_proposer_round: {}, network_round: {}, consensus_round: {}, gc_round: {}). Pausing proposer and focusing on syncing.",
                    self.name,
                    lag,
                    LAG_THRESHOLD,
                    self.current_proposer_round,
                    network_current_round,
                    current_round,
                    self.gc_round
                );
                
                // Notify proposer to pause
                if let Err(e) = self.tx_proposer_catchup.send(true).await {
                    warn!("[CATCH-UP MODE] Failed to notify proposer: {}", e);
                } else {
                    info!("[CATCH-UP MODE] Successfully notified proposer to pause");
                }
            } else {
                // Already in catch-up mode - log progress (upgrade to info for monitoring)
                let catchup_duration = self.catchup_mode_entered_at
                    .map(|t| t.elapsed())
                    .unwrap_or_default();
                info!(
                    "[CATCH-UP MODE] Primary {} still catching up - lag: {} rounds (>= threshold: {}), duration: {:?} (our_proposer_round: {}, network_round: {})",
                    self.name, lag, LAG_THRESHOLD, catchup_duration, self.current_proposer_round, network_current_round
                );
                
                // PROACTIVE SYNC: When in catch-up mode, proactively request certificates from missing rounds
                // This helps node catch up faster by actively requesting data instead of waiting passively
                if lag > 20 && catchup_duration.as_secs() % 5 == 0 {
                    // Request certificates from missing rounds every 5 seconds when lag > 20
                    self.proactive_sync_missing_certificates(network_current_round).await;
                }
            }
        } else if lag < RESUME_THRESHOLD {
            // CATCH-UP MODE: Resume normal operation if lag is small
            if self.is_catchup_mode {
                // Resume normal operation
                self.is_catchup_mode = false;
                let catchup_duration = self.catchup_mode_entered_at
                    .map(|t| t.elapsed())
                    .unwrap_or_default();
                info!(
                    "[CATCH-UP MODE] Primary {} RESUMING normal operation - caught up in {:?} (lag: {} rounds < threshold: {}) (our_proposer_round: {}, network_round: {}, consensus_round: {}). Resuming proposer.",
                    self.name,
                    catchup_duration,
                    lag,
                    RESUME_THRESHOLD,
                    self.current_proposer_round,
                    network_current_round,
                    current_round
                );
                
                // Notify proposer to resume
                if let Err(e) = self.tx_proposer_catchup.send(false).await {
                    warn!("[CATCH-UP MODE] Failed to notify proposer: {}", e);
                } else {
                    info!("[CATCH-UP MODE] Successfully notified proposer to resume");
                }
                
                self.catchup_mode_entered_at = None;
            } else {
                // Node is up to date
                debug!(
                    "[CATCH-UP SYNC] Periodic check - node {} is up to date (lag: {} rounds < threshold: {}) (our_proposer_round: {}, network_round: {}, consensus_round: {})",
                    self.name, lag, RESUME_THRESHOLD, self.current_proposer_round, network_current_round, current_round
                );
            }
        } else {
            // Lag is between RESUME_THRESHOLD and LAG_THRESHOLD
            // Stay in current mode (catch-up or normal)
            if self.is_catchup_mode {
                info!(
                    "[CATCH-UP MODE] Primary {} still in catch-up mode - lag: {} rounds (between thresholds: {} < lag < {})",
                    self.name, lag, RESUME_THRESHOLD, LAG_THRESHOLD
                );
            } else {
                debug!(
                    "[CATCH-UP SYNC] Periodic check - lag: {} rounds (between thresholds: {} < lag < {}), staying in normal mode",
                    lag, RESUME_THRESHOLD, LAG_THRESHOLD
                );
            }
        }

        Ok(())
    }

    /// PROACTIVE SYNC: Log catch-up progress and ensure synchronizer is active
    /// When in catch-up mode, we rely on the existing sync mechanism which is triggered
    /// when we receive headers/certificates. This method ensures we're actively monitoring
    /// catch-up progress and the synchronizer will handle requesting missing data.
    async fn proactive_sync_missing_certificates(&mut self, network_current_round: Round) {
        // Calculate how many rounds we need to catch up
        let rounds_to_catchup = network_current_round.saturating_sub(self.current_proposer_round);
        
        if rounds_to_catchup > 0 {
            info!(
                "[PROACTIVE SYNC] Primary {} in catch-up mode - need to catch up {} rounds (current: {}, network: {}). Synchronizer will handle requesting missing certificates when headers/certificates are received.",
                self.name, rounds_to_catchup, self.current_proposer_round, network_current_round
            );
            
            // The existing sync mechanism (HeaderWaiter, CertificateWaiter) will handle
            // requesting missing certificates when we receive headers/certificates from network.
            // This proactive sync ensures we're aware of the catch-up progress and the
            // synchronizer will automatically request missing data when needed.
        }
    }

    /// PHASE 2: Trigger catch-up sync for old certificate
    /// This method helps node chậm sync certificate ancestors from old rounds
    async fn trigger_catchup_sync_certificate(&mut self, certificate: &Certificate) {
        // Try to deliver certificate to trigger sync if ancestors are missing
        // This will automatically trigger sync request via synchronizer
        match self.synchronizer.deliver_certificate(certificate).await {
            Ok(true) => {
                // All ancestors are available - certificate can be processed
                debug!(
                    "[CATCH-UP SYNC] Ancestors for old certificate {} (round {}) are already available",
                    certificate.digest(),
                    certificate.round()
                );
            }
            Ok(false) => {
                // Ancestors are missing - sync will be triggered automatically by synchronizer
                debug!(
                    "[CATCH-UP SYNC] Ancestors for old certificate {} (round {}) are missing - sync triggered",
                    certificate.digest(),
                    certificate.round()
                );
            }
            Err(e) => {
                warn!(
                    "[CATCH-UP SYNC] Error checking ancestors for old certificate {} (round {}): {}",
                    certificate.digest(),
                    certificate.round(),
                    e
                );
            }
        }
    }

    async fn handle_local_batch_rescue(&mut self, rescue: BatchRescue) -> DagResult<()> {
        // Store locally to guarantee proposer/node can read immediately.
        let inserted = self
            .persist_replicated_batch(&rescue.digest, &rescue.batch)
            .await?;
        self.payload_cache
            .insert(rescue.digest.clone(), rescue.batch.clone());

        if inserted {
            info!(
                "[BATCH RESCUE] Primary {} stored local batch {} (worker {}) for replication (retry_count escalation)",
                self.name, rescue.digest, rescue.worker_id
            );
        } else {
            debug!(
                "[BATCH RESCUE] Primary {} already had batch {} locally; still broadcasting replica",
                self.name, rescue.digest
            );
        }

        let message = PrimaryMessage::BatchReplica {
            digest: rescue.digest,
            worker_id: rescue.worker_id,
            batch: rescue.batch,
            origin: rescue.origin,
        };
        let serialized = bincode::serialize(&message).map_err(DagError::SerializationError)?;

        let addresses: Vec<_> = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();

        if addresses.is_empty() {
            return Ok(());
        }

        self.network
            .broadcast(addresses, Bytes::from(serialized))
            .await;
        Ok(())
    }

    async fn handle_remote_batch_replica(
        &mut self,
        digest: Digest,
        worker_id: WorkerId,
        batch: Vec<u8>,
        origin: PublicKey,
    ) -> DagResult<()> {
        let inserted = self.persist_replicated_batch(&digest, &batch).await?;
        self.payload_cache.insert(digest.clone(), batch.clone());

        if inserted {
            info!(
                "[BATCH RESCUE] Stored replicated batch {} (worker {}) provided by {}",
                digest, worker_id, origin
            );
        } else {
            debug!(
                "[BATCH RESCUE] Ignoring replica for batch {} from {} (already have it)",
                digest, origin
            );
        }
        Ok(())
    }

    async fn persist_replicated_batch(&mut self, digest: &Digest, batch: &[u8]) -> DagResult<bool> {
        if self.store.read(digest.to_vec()).await?.is_some() {
            return Ok(false);
        }
        self.store.write(digest.to_vec(), batch.to_vec()).await;
        Ok(true)
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        // PHASE 2: Set up periodic catch-up sync check timer
        let mut catchup_sync_timer = tokio::time::interval(self.catchup_sync_check_interval);
        catchup_sync_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let result = tokio::select! {
                // We receive here messages from other primaries.
                Some(message) = self.rx_primaries.recv() => {
                    match message {
                        PrimaryMessage::Header(header) => {
                            match self.sanitize_header(&header) {
                                Ok(()) => {
                                    // OPTIMIZATION: Send header to proposer EARLY (right after signature verification)
                                    // This allows proposer to extract batches immediately, reducing delay from
                                    // hundreds of ms to just a few ms. This prevents batches from being stuck
                                    // and reduces retry count significantly.
                                    //
                                    // CRITICAL FIX: Also send OWN headers to proposer for batch extraction.
                                    // This allows leader to extract batches from own headers when they're in InFlight state,
                                    // preventing batches from being stuck when own certificate is not committed.
                                    // SAFETY: extract_batches_from_headers already handles own headers correctly:
                                    // - It only extracts if batches are in InFlight state (not Pending or Committed)
                                    // - It converts InFlight -> Pending to allow immediate inclusion
                                    // - It checks committed_digests to prevent duplicates
                                    //
                                    // SAFETY: Signature is already verified, so this is safe. Even if header
                                    // is later found to be invalid (e.g., missing parents), batch extraction
                                    // is safe because:
                                    // 1. Batch will only be added to queue, not committed immediately
                                    // 2. Batch will be checked again when creating header
                                    // 3. Invalid headers won't be committed anyway
                                    // BATCH TRACKING: Log when sending header to proposer for batch extraction
                                    let header_id = header.id.clone();
                                    let header_round = header.round;
                                    let header_author = header.author;
                                    let header_payload_len = header.payload.len();
                                    let header_payload_keys: Vec<_> = header.payload.keys().take(5).collect();
                                    info!(
                                        "[BATCH TRACK CORE] Core {} sending header {} (round {}, author: {}) to proposer for batch extraction. Header contains {} batches: {:?}",
                                        self.name,
                                        header_id,
                                        header_round,
                                        header_author,
                                        header_payload_len,
                                        header_payload_keys
                                    );
                                    
                                    // CRITICAL FIX: Spawn task để gửi header không blocking main loop
                                    // Điều này đảm bảo headers được gửi ngay lập tức, không bị delay
                                    // Nếu channel đầy, task sẽ đợi nhưng không block main loop
                                    let tx_headers = self.tx_headers.clone();
                                    let header_clone = header.clone();
                                    let header_id_log = header_id.clone();
                                    tokio::spawn(async move {
                                        match tx_headers.send(header_clone).await {
                                            Ok(()) => {
                                                debug!(
                                                    "[BATCH TRACK CORE] Successfully sent header {} (round {}, author: {}) to proposer for batch extraction",
                                                    header_id_log, header_round, header_author
                                                );
                                            }
                                            Err(e) => {
                                                // Channel closed or full - CRITICAL ERROR - log as warning
                                                warn!(
                                                    "[BATCH TRACK CORE] CRITICAL: Failed to send header {} (round {}, author: {}) to proposer for batch extraction: {}. This may cause batches to be stuck!",
                                                    header_id_log, header_round, header_author, e
                                                );
                                            }
                                        }
                                    });
                                    self.process_header(&header).await
                                },
                                Err(DagError::TooOld(_, round)) => {
                                    // PHASE 2: Header is too old - trigger catch-up sync nếu trong window
                                    // sanitize_header đã check và trigger sync rồi, nhưng chúng ta vẫn reject để tránh fork
                                    // Lưu ý: Header quá cũ sẽ KHÔNG được process (tránh fork), nhưng sync vẫn được trigger để node bắt kịp
                                    Err(DagError::TooOld(header.id.clone(), round))
                                },
                                error => error
                            }

                        },
                        PrimaryMessage::Vote(vote) => {
                            match self.sanitize_vote(&vote) {
                                Ok(()) => self.process_vote(vote).await,
                                error => error
                            }
                        },
                        PrimaryMessage::Certificate(certificate) => {
                            match self.sanitize_certificate(&certificate) {
                                Ok(()) => self.process_certificate(certificate).await,
                                Err(DagError::TooOld(_, round)) => {
                                    // PHASE 2: Certificate is too old - trigger catch-up sync nếu trong window
                                    // sanitize_certificate đã check và trigger sync rồi, nhưng chúng ta vẫn reject để tránh fork
                                    // Lưu ý: Certificate quá cũ sẽ KHÔNG được process (tránh fork), nhưng sync vẫn được trigger để node bắt kịp
                                    Err(DagError::TooOld(certificate.digest(), round))
                                },
                                error => error
                            }
                        },
                        PrimaryMessage::BatchReplica { digest, worker_id, batch, origin } => {
                            self.handle_remote_batch_replica(digest, worker_id, batch, origin).await
                        },
                        _ => panic!("Unexpected core message")
                    }
                },

                // We receive here loopback headers from the `HeaderWaiter`. Those are headers for which we interrupted
                // execution (we were missing some of their dependencies) and we are now ready to resume processing.
                Some(header) = self.rx_header_waiter.recv() => self.process_header(&header).await,

                // We receive here loopback certificates from the `CertificateWaiter`. Those are certificates for which
                // we interrupted execution (we were missing some of their ancestors) and we are now ready to resume
                // processing.
                Some(certificate) = self.rx_certificate_waiter.recv() => {
                    // Kiểm tra store để tránh duplicate processing từ certificate waiter loopback
                    let digest = certificate.digest();
                    match self.store.read(digest.to_vec()).await {
                        Ok(Some(_)) => {
                            debug!("Certificate {} from waiter already processed, skipping.", digest);
                            Ok(())
                        }
                        _ => self.process_certificate(certificate).await
                    }
                },

                // We also receive here our new headers created by the `Proposer`.
                Some(header) = self.rx_proposer.recv() => self.process_own_header(header).await,

                // Receive batch rescue events from our proposer (replicate stuck batches to peers).
                Some(rescue) = self.rx_batch_rescue.recv() => {
                    self.handle_local_batch_rescue(rescue).await
                },

                // PHASE 2: Periodic catch-up sync check
                _ = catchup_sync_timer.tick() => {
                    // Periodic check to help node chậm catch-up
                    let current_round = self.consensus_round.load(Ordering::Relaxed);
                    if let Err(e) = self.periodic_catchup_sync_check(current_round).await {
                        debug!("Periodic catch-up sync check failed: {}", e);
                    }
                    Ok(())
                }
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

            // Cleanup internal state.
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
