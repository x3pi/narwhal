// In primary/src/core.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, VotesAggregator};
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use crate::primary::{PrimaryMessage, ReconfigureNotification};
use crate::synchronizer::Synchronizer;
use crate::{Epoch, Round};
use async_recursion::async_recursion;
use bytes::Bytes;
use config::{Committee, PrimaryAddresses}; // <-- THÊM PrimaryAddresses
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, trace, warn}; // <-- THÊM trace
use network::{CancelHandler, ReliableSender};
use rayon::prelude::*; // Keep Rayon for parallel verification if used
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr; // <-- THÊM SocketAddr
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
// use std::time::Duration; // <-- BỎ Duration (không dùng trực tiếp)
use store::{Store, ROUND_INDEX_CF};
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::{interval, sleep, Duration, Instant}; // <-- THÊM Duration, interval ở đây

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

// Constants for synchronization logic
const SYNC_CHUNK_SIZE: Round = 1000; // How many rounds to request in one sync attempt
const SYNC_RETRY_DELAY: u64 = 10_000; // Delay between sync retries (milliseconds)
const SYNC_MAX_RETRIES: u32 = 10; // Maximum retries for a sync chunk before giving up
const LAG_THRESHOLD: Round = 50; // Round difference triggering sync mode

// Interval for triggering committee reconfiguration checks
pub const RECONFIGURE_INTERVAL: Round = 100;

// *** THAY ĐỔI BẮT ĐẦU: Giảm grace period và thêm quiet period ***
// Số round đệm (rỗng) để chạy trước khi kích hoạt reconfigure.
// Đặt là 1 để R100 là round cuối cùng, trigger ở R101.
pub const GRACE_PERIOD_ROUNDS: Round = 1;
// Số round "yên tĩnh" trước khi reconfigure (R95-R100).
pub const QUIET_PERIOD_ROUNDS: Round = 5;
// *** THAY ĐỔI KẾT THÚC ***

// Internal state structure for synchronization process
struct SyncState {
    final_target_round: Round,   // The ultimate round we aim to sync up to
    current_chunk_target: Round, // The target round for the current sync request chunk
    retry_count: u32,            // Number of retries for the current chunk
    last_request_time: Instant,  // Track when the last request was sent
}

// Core component managing the DAG, certificate creation, and interaction with other components.
pub struct Core {
    // Identity
    name: PublicKey,
    committee: Arc<RwLock<Committee>>, // Shared committee information
    epoch: Epoch,                      // Internal epoch tracker

    // Storage
    store: Store, // Persistent storage

    // Consensus interaction
    consensus_round: Arc<AtomicU64>, // Last round decided by consensus (shared with GC)
    tx_consensus: Sender<Certificate>, // Send finalized certificates to consensus

    // Proposer interaction
    tx_proposer: Sender<(Vec<Digest>, Round)>, // Send parent cert digests and round to Proposer

    // Reconfiguration signaling
    tx_reconfigure: broadcast::Sender<ReconfigureNotification>, // Broadcast channel for reconfiguration
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>, // Receive reconfiguration signals

    // Input channels
    rx_primaries: Receiver<PrimaryMessage>, // Receive messages from other primaries
    rx_header_waiter: Receiver<Header>,     // Receive headers after dependencies met
    rx_certificate_waiter: Receiver<Certificate>, // Receive certificates after dependencies met
    rx_proposer: Receiver<Header>,          // Receive own headers from Proposer

    // Internal components & state
    synchronizer: Synchronizer, // Handles fetching missing dependencies
    signature_service: SignatureService, // Signs votes and headers
    gc_depth: Round,            // Garbage collection depth
    gc_round: Round,            // The round below which certificates are GC'd
    dag_round: Round,           // Highest round seen or processed in the DAG
    current_header: Header,     // The latest header proposed by this node
    votes_aggregator: VotesAggregator, // Aggregates votes for the current_header
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>, // Aggregates certs per round for parents
    last_voted: HashMap<Round, HashSet<PublicKey>>, // Tracks authorities voted for per round
    processing: HashMap<Round, HashSet<Digest>>, // Tracks digests currently being processed (avoid re-processing)
    network: ReliableSender,                     // Network sender for reliable P2P messages
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>, // Handles for cancelling network requests
    sync_state: Option<SyncState>, // State indicating if the node is currently syncing
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Arc<RwLock<Committee>>,
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
        tx_reconfigure: broadcast::Sender<ReconfigureNotification>,
        rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
        _epoch_transitioning: Arc<AtomicBool>, // Bỏ qua, không dùng cờ này trong Core
    ) {
        tokio::spawn(async move {
            // Read initial epoch from the committee
            let initial_epoch = committee.read().await.epoch;
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
                tx_reconfigure,
                rx_reconfigure,
                epoch: initial_epoch, // Initialize internal epoch tracker
                gc_round: 0,
                dag_round: 0, // Start DAG round at 0
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(), // Will be replaced by H1
                votes_aggregator: VotesAggregator::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
                sync_state: None, // Start in Running state
            }
            .run()
            .await;
        });
    }

    // Calculates the start round of the last reconfiguration interval.
    pub fn calculate_last_reconfiguration_round(current_round: Round) -> Round {
        (current_round / RECONFIGURE_INTERVAL) * RECONFIGURE_INTERVAL
    }

    // Requests a chunk of certificates from peers for synchronization.
    async fn request_sync_chunk(&mut self, start: Round, end: Round) {
        let current_epoch = self.epoch; // Use internal epoch
        info!(
            "[Core][E{}] Requesting certificate sync chunk for rounds {} to {}",
            current_epoch, start, end
        );
        let message = PrimaryMessage::CertificateRangeRequest {
            start_round: start,
            end_round: end,
            requestor: self.name.clone(), // Include own name
        };
        // Get peer addresses from the current committee state
        let addresses: Vec<SocketAddr> = {
            // <-- THÊM kiểu dữ liệu
            // Acquire read lock briefly
            let committee_guard = self.committee.read().await;
            // Ensure we only request from peers of the *current* epoch
            if committee_guard.epoch != current_epoch {
                warn!("[Core][E{}] Committee epoch changed ({}) during sync request preparation. Aborting request.",
                      current_epoch, committee_guard.epoch);
                return; // Abort if committee changed mid-operation
            }
            committee_guard
                .others_primaries(&self.name)
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect::<Vec<_>>()
            // Lock released here
        };

        // Serialize and broadcast the request
        if !addresses.is_empty() {
            let bytes =
                bincode::serialize(&message).expect("Failed to serialize cert range request");
            self.network.broadcast(addresses, Bytes::from(bytes)).await;
            debug!("[Core][E{}] Sync request broadcasted.", current_epoch);
        } else {
            warn!(
                "[Core][E{}] No peers found to request sync chunk from.",
                current_epoch
            );
        }
    }

    // Advances the synchronization process based on the current state.
    async fn advance_sync(&mut self) {
        if let Some(state) = &mut self.sync_state {
            // Check if synchronization is complete
            if self.dag_round >= state.final_target_round {
                info!(
                    "[Core][E{}] Synchronization complete. Reached round {}. Switching back to Running state.",
                    self.epoch, self.dag_round
                );
                self.sync_state = None; // Exit sync mode
                                        // Clear potentially stale state from sync process
                self.last_voted.clear();
                self.processing.clear();
                self.certificates_aggregators.clear(); // Clear aggregators too
                return;
            }

            // Check if maximum retries reached
            if state.retry_count >= SYNC_MAX_RETRIES {
                warn!(
                    "[Core][E{}] Sync failed after {} retries for target round {}. Exiting Syncing state at round {}.",
                    self.epoch, SYNC_MAX_RETRIES, state.current_chunk_target, self.dag_round
                );
                self.sync_state = None; // Exit sync mode
                self.last_voted.clear();
                self.processing.clear();
                self.certificates_aggregators.clear();
                return;
            }

            // Calculate the next chunk range to request
            // Start from the round *after* the highest round we've currently processed.
            let start = self.dag_round + 1;
            // End at either start + chunk_size or the final target, whichever is smaller.
            let end = (start + SYNC_CHUNK_SIZE - 1).min(state.final_target_round);

            // Update sync state for the new chunk request
            state.current_chunk_target = end;
            state.retry_count += 1; // Increment retry count for this chunk attempt
            state.last_request_time = Instant::now(); // Record time of request

            info!(
                "[Core][E{}] Sync attempt #{}: requesting rounds {} to {} (final target {})",
                self.epoch, state.retry_count, start, end, state.final_target_round
            );
            // Send the actual sync request to peers
            self.request_sync_chunk(start, end).await;
        } else {
            warn!(
                "[Core][E{}] advance_sync called but not in Syncing state.",
                self.epoch
            );
        }
    }

    // Processes a header proposed by this node itself.
    async fn process_own_header(&mut self, header: Header) -> DagResult<()> {
        // Basic sanity checks - should always pass for own headers if Proposer is correct.
        // ensure!(header.author == self.name, DagError::...); // <-- Removed InvalidPublicKey check
        ensure!(header.epoch == self.epoch, DagError::InvalidEpoch); // Check epoch match

        // Update internal tracking of the highest round seen/processed.
        // This is important even before full processing, especially for sync logic.
        if header.round > self.dag_round {
            debug!(
                "[Core][E{}] Advancing internal dag_round from {} to {} upon receiving own H{}",
                self.epoch, self.dag_round, header.round, header.round
            );
            self.dag_round = header.round;
        } else if header.round < self.dag_round {
            warn!("[Core][E{}] Received own header H{} which is older than current dag_round {}. Potential issue?",
                  self.epoch, header.round, self.dag_round);
            // This might indicate a problem if it happens often.
        }

        // Store own header immediately (we trust it).
        let bytes = bincode::serialize(&header).expect("Failed to serialize own header");
        self.store.write(header.id.to_vec(), bytes).await;

        // Reset votes aggregator for this new header.
        self.current_header = header.clone();
        self.votes_aggregator = VotesAggregator::new();
        debug!(
            "[Core][E{}] Initialized votes aggregator for own H{}",
            self.epoch, header.round
        );

        // Add header digest to the processing set for its round.
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());

        // Broadcast the header to other primaries.
        let addresses: Vec<SocketAddr> = {
            // <-- THÊM kiểu dữ liệu
            let committee_guard = self.committee.read().await;
            // Check epoch before getting addresses
            if committee_guard.epoch != self.epoch {
                warn!("[Core][E{}] Committee epoch changed during own header processing. Aborting broadcast for H{}.", self.epoch, header.round);
                return Ok(()); // Avoid broadcasting if epoch changed
            }
            let addrs = committee_guard
                .others_primaries(&self.name)
                .iter()
                .map(|(name, x)| {
                    let addr = x.primary_to_primary;
                    if addr.ip().to_string() == "0.0.0.0" || addr.port() == 0 {
                        warn!(
                            "[Core] ⚠️ INVALID PRIMARY ADDRESS for {} when broadcasting H{}: {}",
                            name, header.round, addr
                        );
                    }
                    addr
                })
                .collect();
            addrs
        };

        // DEBUG: Log địa chỉ trước khi broadcast
        info!(
            "[Core] Broadcasting header H{} to {} addresses: {:?}",
            header.round,
            addresses.len(),
            addresses
        );

        if !addresses.is_empty() {
            let bytes_msg = bincode::serialize(&PrimaryMessage::Header(header.clone()))
                .expect("Failed to serialize header message");
            let handlers = self
                .network
                .broadcast(addresses, Bytes::from(bytes_msg))
                .await;
            // Store cancel handlers for potential future cancellation (e.g., on GC).
            self.cancel_handlers
                .entry(header.round)
                .or_insert_with(Vec::new)
                .extend(handlers);
        } else {
            warn!(
                "[Core][E{}] No peers found to broadcast own H{} to.",
                self.epoch, header.round
            );
        }

        // Process own header locally (check parents, payload, vote for it).
        // Pass `syncing = false` as this is normal operation.
        self.process_header(&header, false).await
    }

    // Processes a header received from self or peers (after basic validation).
    #[async_recursion]
    async fn process_header(&mut self, header: &Header, syncing: bool) -> DagResult<()> {
        debug!(
            "[Core][E{}] Processing Header H{}({}) sync={}",
            self.epoch, header.round, header.author, syncing
        );

        // Ensure header epoch matches current core epoch. Sanitize should catch this, but double-check.
        ensure!(header.epoch == self.epoch, DagError::InvalidEpoch);

        // Avoid re-processing headers already in the pipeline for this round.
        if self
            .processing
            .get(&header.round)
            .map_or(false, |s| s.contains(&header.id))
            && header.author != self.name
        {
            trace!(
                "[Core][E{}] Header H{}({}) already processing. Skipping.",
                self.epoch,
                header.round,
                header.author
            );
            return Ok(());
        }

        // Add to processing set (or update if own header).
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());

        // --- Dependency Checks ---

        // 1. Check Parents: Ensure all parent certificates are available locally.
        let parents = self.synchronizer.get_parents(header).await?;
        if parents.is_empty() && header.round > 0 {
            // Genesis (R0) has no parents.
            // If parents are missing, synchronizer will request them. Suspend processing.
            debug!(
                "[Core][E{}] Processing of H{}({}) suspended: missing parent(s). Synchronizer notified.",
                 self.epoch, header.round, header.author
            );
            // Remove from processing? No, keep it so we don't request sync repeatedly if header arrives again.
            // HeaderWaiter will re-feed it once parents arrive.
            return Ok(());
        }

        // 2. Verify Parent Rounds and Quorum Stake.
        let (stake, _valid_rounds) = {
            // <-- Corrected variable name
            // Use a block to manage committee lock scope
            let committee_guard = self.committee.read().await;
            // Ensure committee epoch didn't change while waiting for parents
            if committee_guard.epoch != self.epoch {
                warn!("[Core][E{}] Committee epoch changed after fetching parents for H{}({}). Aborting processing.", self.epoch, header.round, header.author);
                // Clean up processing state? Maybe remove from `processing` set.
                self.processing.entry(header.round).and_modify(|s| {
                    s.remove(&header.id);
                });
                return Ok(());
            }

            let mut current_stake = 0;
            for p in &parents {
                // Ensure parent epoch matches.
                ensure!(p.epoch() == self.epoch, DagError::InvalidEpoch);
                // Ensure parent round is exactly one less than header round.
                if p.round() + 1 != header.round {
                    error!("[Core][E{}] Header H{}({}) has parent C{}({}) with incorrect round! Discarding header.",
                           self.epoch, header.round, header.author, p.round(), p.origin());
                    // Remove invalid header from processing? Yes.
                    self.processing.entry(header.round).and_modify(|s| {
                        s.remove(&header.id);
                    });
                    // Return specific error?
                    bail!(DagError::MalformedHeader(header.id.clone()));
                }
                current_stake += committee_guard.stake(&p.origin());
            }
            (current_stake, true)
            // Lock released here
        };

        // If parent rounds were invalid, we already bailed.
        // Check if the combined stake of parents meets the quorum threshold.
        let quorum_threshold = self.committee.read().await.quorum_threshold(); // Re-acquire lock briefly
        ensure!(
            stake >= quorum_threshold, // <-- Corrected variable name
            DagError::HeaderRequiresQuorum(header.id.clone())  // Not enough parent stake.
        );

        // 3. Check Payload: Ensure all referenced batch digests are available locally.
        if self.synchronizer.missing_payload(header).await? {
            // If payload is missing, synchronizer will request it. Suspend processing.
            debug!(
                "[Core][E{}] Processing of H{}({}) suspended: missing payload. Synchronizer notified.",
                 self.epoch, header.round, header.author
            );
            // Keep in processing set. HeaderWaiter will re-feed later.
            return Ok(());
        }

        // --- Header is Valid - Store and Vote (if applicable) ---
        info!(
            "[Core][E{}] H{}({}) passed dependency checks. Proceeding to store/vote...",
            self.epoch, header.round, header.author
        ); // <-- THÊM LOG

        // Store the header if it wasn't already (e.g., own header stored earlier).
        // Use read-modify-write on store? For now, simple write is likely okay.
        let header_bytes = bincode::serialize(header).expect("Failed to serialize header");
        self.store.write(header.id.to_vec(), header_bytes).await;
        debug!(
            "[Core][E{}] Stored valid H{}({})",
            self.epoch, header.round, header.author
        );

        // Only vote if we are not in sync mode.
        if !syncing {
            // Check if we already voted for *any* header by this author at this round.
            // Prevents voting multiple times if equivocating headers arrive.
            let not_already_voted = self
                .last_voted // <-- SỬA tên biến cho rõ nghĩa
                .entry(header.round)
                .or_insert_with(HashSet::new)
                .insert(header.author); // `insert` returns true if value was NOT present

            if not_already_voted {
                // If true, this is the first time voting for this author at this round
                info!(
                    "[Core][E{}] Creating vote for H{}({})",
                    self.epoch, header.round, header.author
                ); // <-- ADD LOG
                let vote = Vote::new(header, &self.name, &mut self.signature_service).await;
                debug!(
                    "[Core][E{}] Created Vote V{}({}) for H{}({})",
                    self.epoch, vote.round, vote.author, header.round, header.author
                );

                // Acquire committee lock to get target address.
                let target_primary_address_result: Result<PrimaryAddresses, DagError> = {
                    // <-- Corrected type and name
                    let committee_guard = self.committee.read().await;
                    // Check epoch again before sending vote
                    if committee_guard.epoch != self.epoch {
                        warn!("[Core][E{}] Committee epoch changed before sending vote for H{}({}). Aborting vote.", self.epoch, header.round, header.author);
                        return Ok(());
                    }
                    committee_guard
                        .primary(&header.author)
                        // *** THAY ĐỔI BẮT ĐẦU: Sửa lỗi unused variable ***
                        .map_err(|_| DagError::UnknownAuthority(header.author)) // Map ConfigError to DagError
                                                                                // *** THAY ĐỔI KẾT THÚC ***
                                                                                // Lock released here
                };

                // Handle sending the vote or processing it locally.
                if vote.origin == self.name {
                    // If we are voting for our own header, process the vote directly.
                    info!(
                        "[Core][E{}] Processing own vote for H{}",
                        self.epoch, header.round
                    ); // <-- ADD LOG
                    self.process_vote(vote)
                        .await
                        .expect("Failed to process own vote");
                } else if let Ok(addresses) = target_primary_address_result {
                    // <-- Corrected pattern matching
                    // If voting for a peer's header, send the vote reliably.

                    // DEBUG: Log địa chỉ trước khi gửi vote để trace
                    if addresses.primary_to_primary.ip().to_string() == "0.0.0.0"
                        || addresses.primary_to_primary.port() == 0
                    {
                        warn!("[Core] ⚠️ SENDING VOTE TO INVALID ADDRESS {} for H{}({}), header author: {}, vote origin: {}, epoch: {}", 
                              addresses.primary_to_primary, header.round, header.author, header.author, vote.origin, self.epoch);
                    } else {
                        info!(
                            "[Core] Sending vote for H{}({}) to {}",
                            header.round, header.author, addresses.primary_to_primary
                        );
                    }

                    let vote_bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
                        .expect("Failed to serialize vote message");
                    let handler = self
                        .network
                        .send(addresses.primary_to_primary, Bytes::from(vote_bytes))
                        .await;
                    // Store cancel handler.
                    self.cancel_handlers
                        .entry(header.round)
                        .or_insert_with(Vec::new)
                        .push(handler);
                } else {
                    // Should not happen if header verification passed, but handle defensively.
                    error!("[Core][E{}] Author {} of valid H{} not found in committee! Cannot send vote.", self.epoch, header.author, header.round);
                }
            } else {
                debug!("[Core][E{}] Already voted for author {} at round {}. Ignoring H{}({}) for voting.", self.epoch, header.author, header.round, header.round, header.author);
            }
        } else {
            debug!(
                "[Core][E{}] In sync mode. Skipping vote for H{}({}).",
                self.epoch, header.round, header.author
            );
        }
        Ok(())
    }

    // Processes a vote received from a peer or self.
    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        debug!(
            "[Core][E{}] Processing Vote V{}({}) for H{}({})",
            self.epoch, vote.round, vote.author, vote.round, vote.origin
        );

        // Ensure vote epoch matches current core epoch. Sanitize should catch this.
        ensure!(vote.epoch == self.epoch, DagError::InvalidEpoch);

        // Check if the vote is for the *current header* this node is trying to certify.
        // Votes for older headers are ignored by this aggregator.
        if vote.id != self.current_header.id
            || vote.round != self.current_header.round
            || vote.origin != self.current_header.author
        {
            debug!(
                "[Core][E{}] Vote V{}({}) is for a different header ({}, R{}, Origin {}). Ignoring.",
                self.epoch, vote.round, vote.author, vote.id, vote.round, vote.origin
            );
            return Ok(());
        }

        // Try appending the vote to the aggregator.
        let certificate_option = {
            // Acquire committee lock briefly to check quorum.
            let committee_guard = self.committee.read().await;
            // Check epoch consistency before aggregating.
            if committee_guard.epoch != self.epoch {
                warn!("[Core][E{}] Committee epoch changed while processing V{}({}). Aborting aggregation.", self.epoch, vote.round, vote.author);
                return Ok(());
            }
            self.votes_aggregator
                .append(vote, &committee_guard, &self.current_header)? // Pass committee ref
                                                                       // Lock released here
        };

        // If aggregation resulted in a new certificate...
        if let Some(certificate) = certificate_option {
            info!(
                "[Core][E{}] Assembled Certificate C{}({}) digest {}",
                self.epoch,
                certificate.round(),
                certificate.origin(),
                certificate.digest()
            );
            info!(
                "[Core][E{}] Calling process_certificate for own C{}",
                self.epoch,
                certificate.round()
            ); // <-- ADD LOG

            // Store the new certificate locally.
            // Note: process_certificate also stores, maybe redundant? Check process_certificate logic.
            // Let's keep it here for now, ensures cert is stored before broadcast.
            let cert_bytes_store = bincode::serialize(&certificate)?;
            self.store
                .write(certificate.digest().to_vec(), cert_bytes_store)
                .await;

            // Broadcast the new certificate to peers.
            let addresses: Vec<SocketAddr> = {
                // <-- THÊM kiểu dữ liệu
                let committee_guard = self.committee.read().await;
                if committee_guard.epoch != self.epoch {
                    warn!("[Core][E{}] Committee epoch changed before broadcasting C{}({}). Aborting broadcast.", self.epoch, certificate.round(), certificate.origin());
                    return Ok(());
                }
                let addrs = committee_guard
                    .others_primaries(&self.name)
                    .iter()
                    .map(|(name, x)| {
                        let addr = x.primary_to_primary;
                        if addr.ip().to_string() == "0.0.0.0" || addr.port() == 0 {
                            warn!("[Core] ⚠️ INVALID PRIMARY ADDRESS for {}: {}", name, addr);
                        }
                        addr
                    })
                    .collect();
                addrs
            };

            // DEBUG: Log địa chỉ trước khi broadcast
            info!(
                "[Core] Broadcasting certificate C{}({}) to {} addresses: {:?}",
                certificate.round(),
                certificate.origin(),
                addresses.len(),
                addresses
            );

            if !addresses.is_empty() {
                let cert_bytes_msg =
                    bincode::serialize(&PrimaryMessage::Certificate(certificate.clone()))
                        .expect("Failed to serialize certificate message");
                let handlers = self
                    .network
                    .broadcast(addresses, Bytes::from(cert_bytes_msg))
                    .await;
                // Store cancel handlers.
                self.cancel_handlers
                    .entry(certificate.round())
                    .or_insert_with(Vec::new)
                    .extend(handlers);
            }

            // Process the certificate locally (adds to DAG, checks parents for next round, sends to consensus).
            // Pass `syncing = false`.
            // Use expect here? If we created it, it should be valid. Maybe handle error?
            self.process_certificate(certificate, false)
                .await
                .expect("Failed to process own certificate"); // Panic if processing own cert fails
        }
        Ok(())
    }

    // Processes a certificate received from peers, self, or waiters.
    #[async_recursion]
    async fn process_certificate(
        &mut self,
        certificate: Certificate,
        syncing: bool,
    ) -> DagResult<()> {
        debug!(
            "[Core][E{}] Processing Certificate C{}({}) sync={}",
            self.epoch,
            certificate.round(),
            certificate.origin(),
            syncing
        );

        // Ensure certificate epoch matches current core epoch. Sanitize should catch this.
        ensure!(certificate.epoch() == self.epoch, DagError::InvalidEpoch);

        // Update internal tracking of the highest round seen/processed.
        // Crucial for sync logic and GC round calculation.
        if certificate.round() > self.dag_round {
            debug!(
                "[Core][E{}] Advancing internal dag_round from {} to {} upon processing C{}",
                self.epoch,
                self.dag_round,
                certificate.round(),
                certificate.round()
            );
            self.dag_round = certificate.round();
        }

        // Check if the header embedded in the certificate needs processing
        // (i.e., if its dependencies like parents/payload were met).
        // Avoids redundant header processing if it arrived and was processed before the cert.
        // Check the `processing` map for the *header's* digest.
        let header_digest = certificate.header.id.clone();
        if !self
            .processing
            .get(&certificate.header.round)
            .map_or(false, |x| x.contains(&header_digest))
        {
            debug!(
                "[Core][E{}] Header H{}({}) for C{}({}) needs processing first.",
                self.epoch,
                certificate.header.round,
                certificate.header.author,
                certificate.round(),
                certificate.origin()
            );
            // Process the header (checks parents/payload). Pass `syncing` flag along.
            self.process_header(&certificate.header, syncing).await?;
            // If process_header suspended due to missing deps, we should exit here too.
            // Check processing map again? If it's still not marked as processed, dependencies are still missing.
            if !self
                .processing
                .get(&certificate.header.round)
                .map_or(false, |x| x.contains(&header_digest))
            {
                debug!("[Core][E{}] Processing of C{}({}) suspended: Header dependencies still missing after calling process_header.",
                       self.epoch, certificate.round(), certificate.origin());
                return Ok(()); // Dependencies still missing, wait for waiter.
            }
            debug!(
                "[Core][E{}] Header H{}({}) processed successfully.",
                self.epoch, certificate.header.round, certificate.header.author
            );
        } else {
            trace!(
                "[Core][E{}] Header H{}({}) for C{}({}) already processed.",
                self.epoch,
                certificate.header.round,
                certificate.header.author,
                certificate.round(),
                certificate.origin()
            );
        }

        // --- Certificate Dependencies Met - Proceed ---

        // Check if all *ancestor certificates* (parents of the header) are locally available.
        // This ensures the causal history is complete before sending to consensus or using for parent aggregation.
        if !self.synchronizer.deliver_certificate(&certificate).await? {
            // If ancestors are missing, synchronizer will request them. Suspend processing.
            debug!(
                "[Core][E{}] Processing of C{}({}) suspended: missing ancestors. Synchronizer notified.",
                 self.epoch, certificate.round(), certificate.origin()
            );
            // CertificateWaiter will re-feed this certificate once ancestors arrive.
            return Ok(());
        }
        debug!(
            "[Core][E{}] All ancestors for C{}({}) are present.",
            self.epoch,
            certificate.round(),
            certificate.origin()
        );

        // Store the certificate if it's not already present.
        let digest = certificate.digest();
        if self.store.read(digest.to_vec()).await?.is_none() {
            let value = bincode::serialize(&certificate)?;
            self.store.write(digest.to_vec(), value).await;
            debug!(
                "[Core][E{}] Stored C{}({})",
                self.epoch,
                certificate.round(),
                certificate.origin()
            );

            // Update the round index (mapping round number to certificate digests).
            // Key includes epoch to prevent cross-epoch collisions.
            let key = bincode::serialize(&(self.epoch, certificate.round()))
                .expect("Failed to serialize round index key");
            let cf_name = ROUND_INDEX_CF.to_string(); // Column family name

            // Read existing digests for this round/epoch, or default to empty list.
            let mut digests_for_round: Vec<Digest> = self
                .store
                .read_cf(cf_name.clone(), key.clone())
                .await? // Read from column family
                .and_then(|v| bincode::deserialize(&v).ok()) // Deserialize if present
                .unwrap_or_default();

            // Add the new digest if not already present (shouldn't be, as we checked store above)
            if !digests_for_round.contains(&digest) {
                digests_for_round.push(digest.clone()); // Use cloned digest
                let digests_value = bincode::serialize(&digests_for_round)?;
                self.store.write_cf(cf_name, key, digests_value).await; // Write back to column family
                trace!(
                    "[Core][E{}] Added digest {} to round index for R{}",
                    self.epoch,
                    digest,
                    certificate.round()
                );
            }
        } else {
            trace!(
                "[Core][E{}] Certificate C{}({}) already in store.",
                self.epoch,
                certificate.round(),
                certificate.origin()
            );
        }

        // --- Parent Aggregation for Next Round ---

        // Try appending the certificate to the aggregator for its round.
        let parents_option = {
            let committee_guard = self.committee.read().await;
            // Check epoch consistency before aggregating.
            if committee_guard.epoch != self.epoch {
                warn!("[Core][E{}] Committee epoch changed while aggregating parents for C{}({}). Aborting aggregation.", self.epoch, certificate.round(), certificate.origin());
                return Ok(());
            }
            self.certificates_aggregators
                .entry(certificate.round()) // Get or create aggregator for this round
                .or_insert_with(|| Box::new(CertificatesAggregator::new()))
                .append(certificate.clone(), &committee_guard)? // Append cert, check for quorum
                                                                // Lock released here
        };

        // If quorum of certificates for this round is reached...
        if let Some(parents) = parents_option {
            // Parents are the digests of the quorum of certificates forming round N.
            // These will be used by the Proposer to create Header for round N+1.
            let formed_round = certificate.round();
            info!(
                "[Core][E{}] Quorum reached for round {}. Sending {} parents to Proposer (for R{}).", // <-- SỬA LOG
                self.epoch,
                formed_round,
                parents.len(),
                formed_round + 1 // <-- THÊM
            );

            // *** THAY ĐỔI BẮT ĐẦU: Logic Reconfigure đã được sửa đổi ***
            // --- Reconfiguration Check ---
            // Check if the *next* round (N+1) is a reconfiguration boundary.
            let proposing_round = formed_round + 1;

            // TÍNH TOÁN ROUND KÍCH HOẠT MỚI (100 + 1 = 101)
            let reconfigure_trigger_round = RECONFIGURE_INTERVAL + GRACE_PERIOD_ROUNDS;

            if proposing_round > 0 && proposing_round == reconfigure_trigger_round {
                info!(
                    "[Core][E{}] Round {} IS the reconfiguration trigger round {}. Signaling committee reconfiguration.", // Sửa log
                     self.epoch, proposing_round, reconfigure_trigger_round
                );
                // Send notification *before* sending parents to proposer for this round.
                // The notification includes the *current* committee (epoch N).
                let notification = ReconfigureNotification {
                    round: proposing_round, // The round triggering the change
                    committee: self.committee.read().await.clone(), // Clone current committee
                };
                // Send on broadcast channel. Ignore error if no receivers (e.g., if not main node).
                if self.tx_reconfigure.send(notification).is_err() {
                    warn!(
                        "[Core][E{}] No receivers for reconfigure signal (this is okay).",
                        self.epoch
                    );
                }
            }
            // *** THAY ĐỔI KẾT THÚC ***

            // Send parents to Proposer
            // (Không cần cờ 'epoch_transitioning' ở đây nữa, Proposer sẽ tự kiểm tra round)
            if let Err(e) = self
                .tx_proposer
                .send((parents, formed_round)) // Send (parent_digests, round_N)
                .await
            {
                error!(
                    "[Core][E{}] Failed to send parents for round {} to Proposer: {}",
                    self.epoch, formed_round, e
                );
            }
        }

        // --- Send to Consensus ---
        // Send the certificate to the consensus layer for ordering, unless syncing.
        // Certificates processed during sync mode shouldn't be sent to consensus
        // until sync is complete and normal operation resumes.
        if !syncing {
            let cert_digest = certificate.digest(); // Get digest before moving certificate
            info!(
                "[Core][E{}] Attempting to send C{} ({}) to consensus.",
                self.epoch,
                certificate.round(),
                cert_digest
            ); // <-- ADD LOG
            if self.tx_consensus.is_closed() {
                // <-- THÊM kiểm tra kênh đóng
                warn!(
                    "[Core][E{}] Consensus channel is closed. Cannot deliver certificate {} (R{})",
                    self.epoch,
                    cert_digest,
                    certificate.round()
                );
            } else if let Err(e) = self.tx_consensus.send(certificate).await {
                // If the consensus channel is closed, it's a critical issue.
                warn!(
                    "[Core][E{}] Failed to deliver certificate {} (R{}) to consensus: {}",
                    self.epoch,
                    cert_digest,
                    self.dag_round, // Ghi log dag_round tại thời điểm lỗi
                    e
                );
                // Depending on design, might need to panic or attempt recovery.
                // For now, just log the warning.
            } else {
                trace!(
                    "[Core][E{}] Certificate {} (R{}) sent to consensus.",
                    self.epoch,
                    cert_digest,
                    self.dag_round // Ghi log dag_round
                );
            }
        } else {
            debug!(
                "[Core][E{}] In sync mode. Skipping sending C{}({}) to consensus.",
                self.epoch,
                certificate.round(),
                certificate.origin()
            );
        }

        Ok(())
    }

    // --- Message Sanitization ---
    // Basic validation checks performed *before* full processing.

    // Validates a received header.
    async fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        // Acquire read lock for committee checks.
        let committee_guard = self.committee.read().await;
        // Check 1: Epoch must match the core's current epoch.
        ensure!(
            header.epoch == self.epoch,
            DagError::InvalidEpoch // Use specific error
        );
        // Check 2: Header round must not be older than the garbage collection round.
        ensure!(
            self.gc_round <= header.round,
            DagError::TooOld(header.id.clone(), header.round)
        );
        // Check 3: Verify header integrity and signature using the committee.
        header.verify(&committee_guard)?; // Propagate verification errors.
                                          // Lock released here
        Ok(())
    }

    // Validates a received vote.
    async fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        // Acquire read lock for committee checks.
        let committee_guard = self.committee.read().await;
        // Check 1: Epoch must match.
        ensure!(vote.epoch == self.epoch, DagError::InvalidEpoch);
        // Check 2: Vote round must not be older than the header we are currently aggregating votes for.
        // Allows votes for the current header even if dag_round advanced slightly.
        ensure!(
            self.current_header.round <= vote.round, // Allow votes for current or slightly future rounds? Strict check is better.
            // Let's be strict: vote must be for the *exact* round of the current header.
            // self.current_header.round == vote.round,
            // Relaxed: Allow votes for current header even if parents for next round arrived.
            DagError::TooOld(vote.digest(), vote.round)
        );
        // Check 3: Vote must be for the specific header ID and origin we are currently processing.
        ensure!(
            vote.id == self.current_header.id
                && vote.origin == self.current_header.author
                && vote.round == self.current_header.round,
            DagError::UnexpectedVote(vote.id.clone()) // Vote is for a different header.
        );
        // Check 4: Verify vote signature.
        vote.verify(&committee_guard)?; // Propagate verification errors.
                                        // Lock released here
        Ok(())
    }

    // Validates a received certificate.
    async fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        // Acquire read lock for committee checks.
        let committee_guard = self.committee.read().await;
        // Check 1: Epoch must match.
        ensure!(certificate.epoch() == self.epoch, DagError::InvalidEpoch);
        // Check 2: Certificate round must not be older than the GC round.
        ensure!(
            self.gc_round <= certificate.round(),
            DagError::TooOld(certificate.digest(), certificate.round())
        );
        // Check 3: Verify certificate integrity, header, and quorum signatures.
        certificate.verify(&committee_guard)?; // Propagate verification errors.
                                               // Lock released here
        Ok(())
    }

    // Main message handling function, routes messages based on type and current state (Running vs Syncing).
    async fn handle_message(&mut self, message: PrimaryMessage) -> DagResult<()> {
        // --- Handling logic when in Syncing State ---
        if self.sync_state.is_some() {
            match message {
                // Primary Message specific to Syncing: Certificate Bundle
                PrimaryMessage::CertificateBundle(certificates) => {
                    // Temporarily take ownership of sync_state to avoid borrow checker issues.
                    let mut state = self.sync_state.take().unwrap(); // Should always exist if we are here.

                    if certificates.is_empty() {
                        warn!("[Core][E{}] Received empty certificate bundle during sync. Will retry.", self.epoch);
                        // No progress made, keep retry count, re-insert state and advance (which will retry).
                    } else {
                        info!(
                            "[Core][E{}] Processing sync bundle of {} certificates (Current dag_round: {}, Target: {}).",
                             self.epoch, certificates.len(), self.dag_round, state.final_target_round
                        );

                        // *** FIX: Filter and Verify Certificates *Before* Processing ***
                        let current_epoch_local = self.epoch; // Capture epoch for closure
                        let committee_clone = self.committee.clone(); // Clone Arc for blocking task

                        // Perform verification in a blocking task to avoid blocking the async runtime.
                        let verified_certificates = tokio::task::spawn_blocking(move || {
                            let committee_guard = committee_clone.blocking_read(); // Blocking read inside task
                            certificates
                                .into_par_iter() // Use Rayon for parallel iteration
                                // Filter 1: Keep only certificates belonging to the *current* sync epoch.
                                .filter(|cert| {
                                    if cert.epoch() == current_epoch_local {
                                        true
                                    } else {
                                        warn!(
                                            "[Core][Sync][E{}] Discarding certificate C{}({}) from wrong epoch {} in sync bundle.",
                                            current_epoch_local, cert.round(), cert.origin(), cert.epoch()
                                        );
                                        false
                                    }
                                })
                                // Filter 2: Verify certificate integrity and signatures.
                                .filter_map(|cert| {
                                    match cert.verify(&committee_guard) {
                                        Ok(_) => Some(cert), // Keep valid certificates.
                                        Err(e) => {
                                            warn!(
                                                "[Core][Sync][E{}] Discarding invalid certificate C{}({}) in sync bundle: {}",
                                                current_epoch_local, cert.round(), cert.origin(), e
                                            );
                                            None // Discard invalid certificates.
                                        }
                                    }
                                })
                                .collect::<Vec<Certificate>>() // Collect valid certificates.
                        })
                        .await
                        .expect("Sync verification task panicked"); // Handle panic in blocking task.

                        // Process the *verified* certificates.
                        let mut latest_round_in_bundle = self.dag_round; // Track highest round processed from this bundle.
                        if !verified_certificates.is_empty() {
                            info!(
                                "[Core][E{}] Processing {} verified certificates from sync bundle.",
                                self.epoch,
                                verified_certificates.len()
                            );
                            for certificate in verified_certificates {
                                // Process each valid certificate using the normal logic, but flag as `syncing = true`.
                                // This stores the cert, updates round index, but skips voting/consensus sending.
                                if self
                                    .process_certificate(certificate.clone(), true)
                                    .await
                                    .is_ok()
                                {
                                    // Update latest round processed only if processing succeeds.
                                    latest_round_in_bundle =
                                        latest_round_in_bundle.max(certificate.round());
                                } else {
                                    // Log if processing fails even after verification (should be rare).
                                    warn!("[Core][E{}] Failed to process verified sync certificate C{}({})", self.epoch, certificate.round(), certificate.origin());
                                }
                            }
                            // Update the core's main `dag_round` only if the bundle contained newer rounds.
                            if latest_round_in_bundle > self.dag_round {
                                debug!("[Core][E{}] Sync bundle processed. Advanced dag_round from {} to {}.", self.epoch, self.dag_round, latest_round_in_bundle);
                                self.dag_round = latest_round_in_bundle;
                            } else {
                                debug!(
                                    "[Core][E{}] Sync bundle processed. dag_round remains at {}.",
                                    self.epoch, self.dag_round
                                );
                            }
                            // Reset retry count as progress was made.
                            state.retry_count = 0;
                        } else {
                            warn!("[Core][E{}] Sync bundle contained no valid certificates for the current epoch.", self.epoch);
                            // Do not reset retry count, as no progress was made.
                        }
                    }
                    // Put sync_state back and potentially request the next chunk.
                    self.sync_state = Some(state);
                    self.advance_sync().await; // Check completion or request next chunk/retry.
                }

                // Handling individual Certificates received during Syncing
                PrimaryMessage::Certificate(certificate) => {
                    // Check if this certificate could potentially advance our sync target.
                    let target_round = self.sync_state.as_ref().map_or(0, |s| s.final_target_round);
                    let cert_round = certificate.round();
                    let cert_epoch = certificate.epoch();

                    // If cert is from the current epoch and significantly newer than target...
                    if cert_epoch == self.epoch && cert_round > target_round + LAG_THRESHOLD {
                        // Validate the certificate briefly before potentially updating target.
                        // Avoid full processing, just basic checks.
                        if self.sanitize_certificate(&certificate).await.is_ok() {
                            info!(
                                "[Core][Sync][E{}] Received certificate C{} significantly newer than sync target {}. Updating target.",
                                 self.epoch, cert_round, target_round
                            );
                            // Update the final target round in the sync state.
                            if let Some(state) = self.sync_state.as_mut() {
                                state.final_target_round = cert_round;
                                // Reset retry count? Maybe not, let current chunk finish first.
                            }
                        } else {
                            warn!("[Core][Sync][E{}] Received potential future certificate C{}, but it failed sanitization.", self.epoch, cert_round);
                        }
                    } else if cert_epoch == self.epoch && cert_round > self.dag_round {
                        // If cert is for current epoch and newer than dag_round, but not drastically so,
                        // process it in sync mode. This helps fill gaps while waiting for bundles.
                        if self.sanitize_certificate(&certificate).await.is_ok() {
                            debug!("[Core][Sync][E{}] Processing individual certificate C{}({}) received during sync.", self.epoch, cert_round, certificate.origin());
                            let _ = self.process_certificate(certificate, true).await;
                            // Ignore result? Log error?
                            // Do *not* advance sync here, let bundle processing drive progress.
                        }
                    } else {
                        trace!(
                            "[Core][Sync][E{}] Ignoring certificate C{}({}) (epoch {}, round {}) during sync.",
                            self.epoch, certificate.round(), certificate.origin(), cert_epoch, cert_round
                        );
                    }
                }

                // Ignore other message types while syncing.
                _ => {
                    trace!(
                        "[Core][Sync][E{}] Ignoring message {:?} while in Syncing state.",
                        self.epoch,
                        message
                    );
                }
            }
            return Ok(()); // Return after handling message in sync state.
        }

        // --- Handling logic when in Running State ---
        // (Không cần cờ epoch_transitioning ở đây nữa)

        match message {
            // Received Header from peer or loopback (after dependencies met)
            PrimaryMessage::Header(header) => {
                // Sanitize first (checks epoch, GC round, signature).
                self.sanitize_header(&header).await?;
                // Full processing (checks parents, payload, votes).
                self.process_header(&header, false).await
            }
            // Received Vote from peer
            PrimaryMessage::Vote(vote) => {
                // Sanitize first (checks epoch, GC round, matches current_header, signature).
                self.sanitize_vote(&vote).await?;
                // Full processing (aggregates votes, potentially creates/processes certificate).
                self.process_vote(vote).await
            }
            // Received Certificate from peer or loopback (after dependencies met)
            PrimaryMessage::Certificate(certificate) => {
                // *** FIX: Lagging Check only considers certificates from the CURRENT epoch ***
                if certificate.epoch() == self.epoch
                    && certificate.round() > self.dag_round.saturating_add(LAG_THRESHOLD)
                {
                    // Certificate is significantly ahead, likely lagging. Enter Syncing state.
                    info!(
                        "[Core][E{}] Detected lag: Received C{} while at dag_round {}. Switching to Syncing state to catch up.",
                         self.epoch, certificate.round(), self.dag_round
                    );
                    // Sanitize the certificate *before* using its round as the target.
                    self.sanitize_certificate(&certificate).await?; // Ensure it's valid first
                                                                    // Initialize sync state.
                    self.sync_state = Some(SyncState {
                        final_target_round: certificate.round(), // Target the round of the future certificate.
                        current_chunk_target: 0,                 // Will be set by advance_sync.
                        retry_count: 0,
                        last_request_time: Instant::now(), // Initialize timer
                    });
                    // Immediately request the first chunk.
                    self.advance_sync().await;
                    // We are now in Syncing state, return.
                    return Ok(());
                } else {
                    // Certificate is not significantly ahead, or from wrong epoch. Sanitize and process normally.
                    self.sanitize_certificate(&certificate).await?;
                    // Update dag_round immediately upon receiving a valid certificate for the current epoch.
                    if certificate.epoch() == self.epoch {
                        self.dag_round = self.dag_round.max(certificate.round());
                    }
                    // Full processing (stores cert, updates round index, aggregates for parents, sends to consensus).
                    self.process_certificate(certificate, false).await?;
                }
                Ok(())
            }
            // Ignore Reconfigure messages received directly from peers. Reconfiguration is driven internally.
            PrimaryMessage::Reconfigure(_) => {
                info!(
                    "[Core][E{}] Ignoring external Reconfigure message.",
                    self.epoch
                );
                Ok(())
            }
            // Ignore Certificate Bundles when not in Syncing state.
            PrimaryMessage::CertificateBundle(_) => {
                warn!(
                    "[Core][E{}] Received CertificateBundle while not in Syncing state. Ignoring.",
                    self.epoch
                );
                Ok(())
            }
            // Certificate Request messages are handled by Helper, not Core.
            PrimaryMessage::CertificatesRequest(_, _)
            | PrimaryMessage::CertificateRangeRequest { .. } => {
                warn!("[Core][E{}] Received Certificate Request message unexpectedly. Should be handled by Helper. Ignoring.", self.epoch);
                Ok(())
            }
        }
    }

    // Resets the internal state of Core for a new epoch.
    fn reset_state_for_new_epoch(&mut self, new_epoch: Epoch) {
        info!(
            "[Core] Detected epoch change from {} to {}. Resetting internal state.",
            self.epoch, new_epoch
        );

        // Update internal epoch tracker.
        self.epoch = new_epoch;
        // Reset round counters.
        self.gc_round = 0;
        self.dag_round = 0; // Crucial: Reset DAG round tracker.

        // Clear state maps.
        self.last_voted.clear();
        self.processing.clear();
        self.certificates_aggregators.clear();
        self.cancel_handlers.clear(); // Cancel pending network requests? Maybe let them timeout. Clearing handlers is safer.

        // Reset current header and aggregators.
        self.current_header = Header::default();
        self.votes_aggregator = VotesAggregator::new();

        // Ensure we exit sync state if we were syncing during transition.
        self.sync_state = None;

        // Note: `consensus_round` (Arc<AtomicU64>) is reset by GarbageCollector.
        // Note: `committee` (Arc<RwLock<Committee>>) is updated by the main node loop.

        info!(
            "[Core] State reset complete for epoch {}. Ready.",
            new_epoch
        );
    }

    // Main execution loop for the Core task.
    pub async fn run(&mut self) {
        // Timer for sync retries.
        let mut sync_check_interval = interval(Duration::from_millis(SYNC_RETRY_DELAY / 2)); // Check more frequently
        sync_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!("[Core][E{}] Starting main loop.", self.epoch);
        loop {
            // Select the next event. Prioritize reconfiguration.
            let result = tokio::select! {
                biased; // Prioritize reconfigure channel

                // Handle Reconfiguration Signal
                result = self.rx_reconfigure.recv() => {
                    match result {
                        Ok(notification) => {
                             // --- Anti-Race Condition Logic for Core ---
                            if notification.committee.epoch != self.epoch {
                                warn!(
                                    "[Core] Ignoring stale reconfigure signal for epoch {} (current is {})",
                                    notification.committee.epoch, self.epoch
                                );
                            } else {
                                // Wait for the shared Committee Arc to be updated.
                                info!("[Core] Received reconfigure signal for end of epoch {}. Waiting for committee update...", self.epoch);
                                let mut updated_committee_epoch = self.epoch;
                                while updated_committee_epoch <= self.epoch {
                                    sleep(Duration::from_millis(100)).await;
                                    let committee_guard = self.committee.read().await;
                                    updated_committee_epoch = committee_guard.epoch;
                                    drop(committee_guard);
                                }
                                info!("[Core] Committee Arc updated to epoch {}. Resetting internal state.", updated_committee_epoch);
                                // Perform the state reset for the new epoch.
                                self.reset_state_for_new_epoch(updated_committee_epoch);
                                // (Không cần cờ epoch_transitioning nữa)
                                info!("[Core][E{}] State reset for new epoch complete.", self.epoch); // <-- SỬA LOG

                            }
                            Ok(()) // Continue loop after handling signal
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("[Core] Reconfigure receiver lagged by {}. Missed epoch transitions!", n);
                            // Consider shutdown or requesting full state sync.
                            Ok(())
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            error!("[Core] Reconfigure channel closed unexpectedly. Shutting down.");
                            break; // Exit main loop
                        }
                    }
                },

                // *** THAY ĐỔI BẮT ĐẦU: Sửa lỗi borrow ***
                // Handle messages from other primaries (Headers, Votes, Certificates)
                // Receive message first, then handle based on sync_state.
                Some(message) = self.rx_primaries.recv() => {
                    self.handle_message(message).await // handle_message already checks sync_state internally
                },
                // *** THAY ĐỔI KẾT THÚC ***


                // Handle headers returned by HeaderWaiter (dependencies met)
                 // Chỉ xử lý nếu không syncing.
                Some(header) = self.rx_header_waiter.recv(), if self.sync_state.is_none() => {
                    // Sanitize checks epoch/GC round. Process checks parents/payload/votes.
                    match self.sanitize_header(&header).await {
                        Ok(_) => self.process_header(&header, false).await,
                        Err(e) => {
                            warn!("[Core] Header from waiter failed sanitization: {}", e);
                            Ok(()) // Ignore invalid header
                        }
                    }
                },

                // Handle certificates returned by CertificateWaiter (dependencies met)
                 // Chỉ xử lý nếu không syncing.
                Some(certificate) = self.rx_certificate_waiter.recv(), if self.sync_state.is_none() => {
                     // Sanitize checks epoch/GC round. Process handles storage, aggregation, consensus send.
                     match self.sanitize_certificate(&certificate).await {
                        Ok(_) => {
                            // Update dag_round immediately if cert is newer.
                             if certificate.epoch() == self.epoch {
                                self.dag_round = self.dag_round.max(certificate.round());
                            }
                            self.process_certificate(certificate, false).await
                        },
                        Err(e) => {
                            warn!("[Core] Certificate from waiter failed sanitization: {}", e);
                            Ok(()) // Ignore invalid certificate
                        }
                    }
                },

                // Handle own headers created by Proposer
                 // Chỉ xử lý nếu không syncing.
                Some(header) = self.rx_proposer.recv(), if self.sync_state.is_none() => {
                    // Assumes own header is sanitized correctly by Proposer.
                    self.process_own_header(header).await
                },

                // Handle Sync Timeout Check (only active if sync_state is Some)
                _ = sync_check_interval.tick(), if self.sync_state.is_some() => {
                     // Determine if sync timeout check is needed based on last request time.
                     let sync_timeout_check = self.sync_state.as_ref().map_or(false, |s| {
                         s.last_request_time.elapsed() >= Duration::from_millis(SYNC_RETRY_DELAY)
                     });

                     // If sync timeout occurred...
                     if sync_timeout_check {
                         warn!("[Core][E{}] Sync request timed out (Round {}). Retrying...", self.epoch, self.sync_state.as_ref().map_or(0, |s| s.current_chunk_target));
                         self.advance_sync().await; // Retry or advance sync state.
                     }
                    Ok(()) // Continue loop after check
                }


                // Break loop if all input channels are closed (unlikely in normal operation)
                 else => {
                    info!("[Core][E{}] All input channels closed. Shutting down.", self.epoch);
                    break
                }
            }; // End tokio::select!

            // Handle results from the select! block.
            match result {
                Ok(()) => (), // Continue loop normally
                Err(DagError::StoreError(e)) => {
                    error!(
                        "[Core][E{}] Storage failure: {}. Shutting down node.",
                        self.epoch, e
                    );
                    panic!("Storage failure: killing node."); // Critical error
                }
                // Log recoverable errors but continue operation.
                Err(e @ DagError::TooOld(..)) | Err(e @ DagError::InvalidEpoch) => {
                    debug!("[Core][E{}] {}", self.epoch, e)
                } // Expected during transitions or late arrivals
                Err(e @ DagError::AuthorityReuse(..)) => {
                    warn!("[Core][E{}] Authority reuse detected: {}", self.epoch, e)
                } // Potential misbehavior
                Err(e) => warn!("[Core][E{}] General DAG error: {}", self.epoch, e), // Other errors
            }

            // --- Garbage Collection Logic (Run only when not syncing) ---
            if self.sync_state.is_none() {
                // Get the last round committed by consensus.
                let consensus_gc_round = self.consensus_round.load(Ordering::Relaxed);
                // Calculate the GC threshold round.
                if consensus_gc_round > self.gc_depth {
                    let new_gc_round = consensus_gc_round - self.gc_depth;
                    // Only perform GC if the threshold has actually advanced.
                    if new_gc_round > self.gc_round {
                        debug!(
                            "[Core][E{}] Advancing GC round from {} to {}",
                            self.epoch, self.gc_round, new_gc_round
                        );
                        // Remove old entries from internal state maps.
                        self.last_voted.retain(|k, _| k >= &new_gc_round);
                        self.processing.retain(|k, _| k >= &new_gc_round);
                        self.certificates_aggregators
                            .retain(|k, _| k >= &new_gc_round);
                        self.cancel_handlers.retain(|k, _| k >= &new_gc_round); // Also cancel old network requests?
                        self.gc_round = new_gc_round; // Update the GC round tracker.
                    }
                }
            }
        } // End main loop
        info!("[Core][E{}] Main loop terminated.", self.epoch);
    } // End fn run
} // End impl Core
