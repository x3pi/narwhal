// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::Header;
use crate::primary::{PrimaryMessage, PrimaryWorkerMessage, Round};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error, info, warn};
use network::SimpleSender;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

/// The resolution of the timer that checks whether we received replies to our sync requests, and triggers
/// new sync requests if we didn't.
const TIMER_RESOLUTION: u64 = 100; // Giảm xuống 100ms để check và retry nhanh hơn
const BROADCAST_RETRY_THRESHOLD: u32 = 1; // Broadcast ngay từ lần retry đầu tiên
const BATCH_RECOVERY_RETRY_THRESHOLD: u32 = 3;
const BATCH_RECOVERY_COOLDOWN_MS: u128 = 5_000;

#[derive(Clone, Debug)]
struct BatchRequestInfo {
    round: Round,
    worker_id: WorkerId,
    author: PublicKey,
    last_request_ms: u128,
    attempts: u32,
    last_recovery_ms: Option<u128>,
}

/// The commands that can be sent to the `Waiter`.
#[derive(Debug)]
pub enum WaiterMessage {
    SyncBatches {
        missing: HashMap<Digest, WorkerId>,
        header: Header,
        committed: bool,
    },
    SyncParents(Vec<Digest>, Header),
}

/// Waits for missing parent certificates and batches' digests.
pub struct HeaderWaiter {
    /// The name of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,
    /// The delay to wait before re-trying sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-request.
    sync_retry_nodes: usize,

    /// Receives sync commands from the `Synchronizer`.
    rx_synchronizer: Receiver<WaiterMessage>,
    /// Loops back to the core headers for which we got all parents and batches.
    tx_core: Sender<Header>,

    /// Network driver allowing to send messages.
    network: SimpleSender,
    /// Keeps the digests of the all certificates for which we sent a sync request,
    /// along with a timestamp (`u128`) indicating when we sent the request.
    parent_requests: HashMap<Digest, (Round, u128, u32)>,
    /// Keeps the digests of the all tx batches for which we sent a sync request,
    /// similarly to `header_requests`.
    batch_requests: HashMap<Digest, BatchRequestInfo>,
    /// List of digests (either certificates, headers or tx batch) that are waiting
    /// to be processed. Their processing will resume when we get all their dependencies.
    pending: HashMap<Digest, (Round, Sender<()>)>,
    /// Last cleanup time for tracking maps
    last_cleanup: Option<u128>,
    /// Cleanup interval (every 60 seconds)
    cleanup_interval_ms: u128,
    /// Maximum age for entries (5 minutes)
    max_entry_age_ms: u128,
}

impl HeaderWaiter {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_synchronizer: Receiver<WaiterMessage>,
        tx_core: Sender<Header>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                consensus_round,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_synchronizer,
                tx_core,
                network: SimpleSender::new(),
                parent_requests: HashMap::new(),
                batch_requests: HashMap::new(),
                pending: HashMap::new(),
                last_cleanup: None,
                cleanup_interval_ms: 60_000, // 60 seconds
                max_entry_age_ms: 300_000, // 5 minutes
            }
            .run()
            .await;
        });
    }

    /// Cleanup old entries from tracking maps to prevent memory leak
    /// SAFETY: Only removes entries that are:
    /// 1. From rounds older than gc_depth (already committed or will never commit)
    /// 2. Older than max_age AND from rounds that are far behind consensus
    /// This ensures no batches are lost and system remains deterministic
    fn cleanup_old_entries(&mut self, now: u128) {
        let cutoff_time = now.saturating_sub(self.max_entry_age_ms);
        let consensus_round = self.consensus_round.load(Ordering::Relaxed);
        let gc_watermark = consensus_round.saturating_sub(self.gc_depth);
        
        // SAFETY: Only cleanup entries from rounds that are:
        // 1. Older than gc_depth (already committed or garbage collected)
        // 2. OR very old (> 10 minutes) and from rounds far behind consensus
        // This prevents removing entries for batches that are still being synced
        
        // Cleanup parent_requests - only remove from rounds that are garbage collected
        let before_parents = self.parent_requests.len();
        self.parent_requests.retain(|_, (round, timestamp, _)| {
            // Keep if round is recent (not garbage collected)
            if *round > gc_watermark {
                return true;
            }
            // For old rounds, only remove if also old by timestamp
            // This ensures we don't remove recent requests for old rounds
            *timestamp > cutoff_time
        });
        let after_parents = self.parent_requests.len();
        let removed_parents = before_parents.saturating_sub(after_parents);
        
        // Cleanup batch_requests - SAFETY: Only remove entries from rounds that are:
        // 1. Older than gc_depth (already committed or garbage collected)
        // 2. AND older than max_age by timestamp
        // This ensures we don't remove entries for batches still being synced
        let before_batches = self.batch_requests.len();
        let very_old_cutoff = now.saturating_sub(600_000); // 10 minutes
        self.batch_requests.retain(|_, info| {
            // Keep if round is recent (not garbage collected)
            if info.round > gc_watermark {
                return true;
            }
            // For old rounds (garbage collected), only remove if also very old by timestamp
            // This ensures we don't remove recent retry attempts for old rounds
            // Batches from garbage collected rounds should have been synced by now
            // If not, they will be retried through normal sync mechanisms
            let very_old = info.last_request_ms < very_old_cutoff;
            if very_old {
                false // Remove - round is GC'd and entry is very old
            } else {
                true // Keep - might still be syncing
            }
        });
        let after_batches = self.batch_requests.len();
        let removed_batches = before_batches.saturating_sub(after_batches);
        
        // Cleanup pending - only remove from rounds that are garbage collected
        let before_pending = self.pending.len();
        self.pending.retain(|_, (round, _)| {
            *round > gc_watermark
        });
        let after_pending = self.pending.len();
        let removed_pending = before_pending.saturating_sub(after_pending);
        
        if removed_parents > 0 || removed_batches > 0 || removed_pending > 0 {
            debug!(
                "[HEADER WAITER CLEANUP] Cleaned up old entries: {} parent_requests ({} -> {}), {} batch_requests ({} -> {}), {} pending ({} -> {}). Only removed entries from garbage collected rounds or very old entries.",
                removed_parents, before_parents, after_parents,
                removed_batches, before_batches, after_batches,
                removed_pending, before_pending, after_pending
            );
        }
    }

    /// Helper function. It waits for particular data to become available in the storage
    /// and then delivers the specified header.
    async fn waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: Header,
        mut handler: Receiver<()>,
    ) -> DagResult<Option<Header>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    // Main loop listening to the `Synchronizer` messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.rx_synchronizer.recv() => {
                    match message {
                        WaiterMessage::SyncBatches { missing, header, committed } => {
                            let header_id = header.id.clone();
                            let round = header.round;
                            let author = header.author.clone();
                            let missing_count = missing.len();
                            let _missing_digests_sample: Vec<_> = missing.keys().take(5).cloned().collect();
                            let priority = if committed { "COMMITTED" } else { "PENDING" };

                            tracing::info!(
                                target: "narwhal_audit",
                                priority = priority,
                                missing_count = missing_count,
                                header_id = %header_id,
                                round = round,
                                author = %author,
                                "[SYNC BATCHES REQUEST] HeaderWaiter received sync request"
                            );

                            let already_pending = self.pending.contains_key(&header_id);

                            // Add the header to the waiter pool only once.
                            if !already_pending {
                                let wait_for = missing
                                    .keys()
                                    .map(|digest| (digest.to_vec(), self.store.clone()))
                                    .collect();

                                let (tx_cancel, rx_cancel) = channel(1);
                                self.pending.insert(header_id.clone(), (round, tx_cancel));
                                let fut = Self::waiter(wait_for, header.clone(), rx_cancel);
                                waiting.push(fut);
                            } else {
                                info!(
                                    "[SYNC BATCHES REQUEST][{}] Header {} already pending, re-using existing waiter",
                                    priority, header_id
                                );
                            }

                            // Determine which digests still require network sync.
                            let mut requires_sync: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();

                            for (digest, worker_id) in missing.into_iter() {
                                match self.batch_requests.entry(digest.clone()) {
                                    Entry::Occupied(mut entry) => {
                                        if committed {
                                            let info = entry.get_mut();
                                            info.last_request_ms = now;
                                            info.attempts = info.attempts.saturating_add(1);
                                            requires_sync.entry(worker_id).or_insert_with(Vec::new).push(digest.clone());
                                        }
                                    }
                                    Entry::Vacant(entry) => {
                                        requires_sync.entry(worker_id).or_insert_with(Vec::new).push(digest.clone());
                                        entry.insert(BatchRequestInfo {
                                            round,
                                            worker_id,
                                            author: author.clone(),
                                            last_request_ms: now,
                                            attempts: 1,
                                            last_recovery_ms: None,
                                        });
                                    }
                                }
                            }

                            if committed && requires_sync.is_empty() {
                                debug!(
                                    "[SYNC BATCHES REQUEST][COMMITTED] Header {} already has outstanding sync requests. Forcing immediate retry.",
                                    header_id
                                );
                                // Force another round of sync by refreshing timestamps.
                                for (digest, info) in self.batch_requests.iter_mut() {
                                    if info.round == round && info.author == author {
                                        info.last_request_ms = now;
                                        info.attempts = info.attempts.saturating_add(1);
                                        requires_sync
                                            .entry(info.worker_id)
                                            .or_insert_with(Vec::new)
                                            .push(digest.clone());
                                    }
                                }
                            }
                            // ĐỒNG BỘ SIÊU NHANH: Gửi đến nhiều workers song song để tăng tốc độ
                            for (worker_id, digests) in requires_sync {
                                let batch_count = digests.len();

                                let batch_list: Vec<String> = digests.iter().take(10).map(|d| format!("{}", d)).collect();
                                
                                // CRITICAL: Log chi tiết trước khi gửi sync request
                                tracing::info!(
                                    target: "narwhal_audit",
                                    priority = priority,
                                    worker_id = worker_id,
                                    author = %author,
                                    batch_count = batch_count,
                                    header_id = %header_id,
                                    round = round,
                                    "[SYNC BATCHES SEND] HeaderWaiter {} sending sync request for {} batches from worker {} (author: {}) for header {} (round {}). Batches: {:?}",
                                    self.name, batch_count, worker_id, author, header_id, round, batch_list
                                );

                                let author_address = self.committee
                                    .worker(&author, &worker_id)
                                    .expect("Author of valid header is not in the committee")
                                    .primary_to_worker;
                                let message = PrimaryWorkerMessage::Synchronize(digests.clone(), author);
                                let bytes = bincode::serialize(&message)
                                    .expect("Failed to serialize batch sync request");

                                // CRITICAL: Track thời gian gửi sync request
                                let sync_send_start = std::time::Instant::now();
                                
                                // ĐỒNG BỘ SIÊU NHANH: Gửi đến TẤT CẢ workers ngay lập tức để tăng tốc độ sync tối đa
                                // Gửi đến worker của author trước
                                info!(
                                    "[SYNC BATCHES SEND][{}] Dispatching request to author worker {} ({}) for {} batch(es)",
                                    priority,
                                    worker_id,
                                    author_address,
                                    batch_count
                                );
                                self.network
                                    .send(author_address, Bytes::from(bytes.clone()))
                                    .await;
                                
                                // CRITICAL: Log sau khi gửi sync request thành công
                                let sync_send_duration = sync_send_start.elapsed();
                                tracing::info!(
                                    target: "narwhal_audit",
                                    priority = priority,
                                    worker_id = worker_id,
                                    author = %author,
                                    batch_count = batch_count,
                                    duration_ms = sync_send_duration.as_millis(),
                                    "[SYNC BATCHES SENT] HeaderWaiter {} successfully sent sync request for {} batches to worker {} (author: {}) in {}ms. Waiting for worker response.",
                                    self.name, batch_count, worker_id, author, sync_send_duration.as_millis()
                                );

                                // Gửi đến TẤT CẢ workers của các node khác để tăng tốc độ sync tối đa
                                let other_workers: Vec<_> = self.committee.others_primaries(&self.name)
                                    .iter()
                                    .filter_map(|(other_author, _)| {
                                        self.committee.worker(other_author, &worker_id).ok()
                                            .map(|addr| addr.primary_to_worker)
                                    })
                                    .collect(); // Gửi đến TẤT CẢ workers, không giới hạn

                                let other_workers_count = other_workers.len();
                                if other_workers_count > 0 {
                                    info!(
                                        "[SYNC BATCHES FANOUT][{}] {} sending sync request to {} peer workers for {} batch(es) (worker {})",
                                        priority,
                                        self.name,
                                        other_workers_count,
                                        batch_count,
                                        worker_id
                                    );
                                }

                                // Gửi tuần tự đến tất cả workers khác (SimpleSender đã có connection pooling)
                                for worker_addr in other_workers {
                                    let message_other = PrimaryWorkerMessage::Synchronize(digests.clone(), author);
                                    let bytes_other = bincode::serialize(&message_other)
                                        .expect("Failed to serialize batch sync request");
                                    tracing::info!(
                                        target: "narwhal_audit",
                                        priority = priority,
                                        worker_address = %worker_addr,
                                        batch_count = batch_count,
                                        author = %author,
                                        "[SYNC BATCHES FANOUT SEND] Sending batch sync request to peer worker"
                                    );
                                    self.network
                                        .send(worker_addr, Bytes::from(bytes_other))
                                        .await;
                                }
                            }
                        }

                        WaiterMessage::SyncParents(missing, header) => {
                            info!("[SYNC PARENTS] HeaderWaiter {} synching the parents of header {} (round {}, author: {}). Missing {} parents.", self.name, header.id, header.round, header.author, missing.len());
                            let header_id = header.id.clone();
                            let round = header.round;
                            let author = header.author;

                            // Ensure we sync only once per header.
                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            // Add the header to the waiter pool. The waiter will return it to us
                            // when all its parents are in the store.
                            let wait_for = missing
                                .iter()
                                .cloned()
                                .map(|x| (x.to_vec(), self.store.clone()))
                                .collect();
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (round, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);

                            // Ensure we didn't already sent a sync request for these parents.
                            // Optimistically send the sync request to the node that created the certificate.
                            // If this fails (after a timeout), we broadcast the sync request.
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            let mut requires_sync = Vec::new();
                            for missing in missing {
                                self.parent_requests
                                    .entry(missing.clone())
                                    .or_insert_with(|| {
                                        requires_sync.push(missing.clone());
                                        (round, now, 0)
                                    });
                            }
                            if !requires_sync.is_empty() {
                                for digest in &requires_sync {
                                    if let Some((_, ts, attempts)) =
                                        self.parent_requests.get_mut(digest)
                                    {
                                        *ts = now;
                                        *attempts = attempts.saturating_add(1);
                                    }
                                }

                                let author_address = self.committee
                                    .primary(&author)
                                    .expect("Author of valid header not in the committee")
                                    .primary_to_primary;
                                let message = PrimaryMessage::CertificatesRequest(
                                    requires_sync.clone(),
                                    self.name,
                                );
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");

                                // ĐỒNG BỘ SIÊU NHANH: Gửi đến TẤT CẢ nodes ngay lập tức để tăng tốc độ sync tối đa
                                // Gửi đến author trước
                                self.network.send(author_address, Bytes::from(bytes.clone())).await;

                                // Gửi đến TẤT CẢ nodes khác ngay lập tức (không giới hạn số lượng)
                                let other_addresses: Vec<_> = self.committee.others_primaries(&self.name)
                                    .iter()
                                    .filter(|(pk, _)| *pk != author) // Không gửi lại đến author
                                    .map(|(_, x)| x.primary_to_primary)
                                    .collect(); // Gửi đến TẤT CẢ nodes, không giới hạn

                                // Gửi tuần tự đến tất cả nodes khác (SimpleSender đã có connection pooling)
                                for addr in other_addresses {
                                    self.network.send(addr, Bytes::from(bytes.clone())).await;
                                }
                            }
                        }
                    }
                },

                Some(result) = waiting.next() => match result {
                    Ok(Some(header)) => {
                        let _ = self.pending.remove(&header.id);
                        for x in header.payload.keys() {
                            let _ = self.batch_requests.remove(x);
                        }
                        for x in &header.parents {
                            let _ = self.parent_requests.remove(x);
                        }
                        self.tx_core.send(header).await.expect("Failed to send header");
                    },
                    Ok(None) => {
                        // This request has been canceled.
                    },
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                () = &mut timer => {
                    // CRITICAL: Cleanup old entries periodically to prevent memory leak
                    // This prevents system degradation over time
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();
                    
                    // Cleanup old entries if enough time has passed
                    if self.last_cleanup.map_or(true, |last| {
                        now.saturating_sub(last) >= self.cleanup_interval_ms
                    }) {
                        self.cleanup_old_entries(now);
                        self.last_cleanup = Some(now);
                    }

                    // We optimistically sent sync requests to a single node. If this timer triggers,
                    // it means we were wrong to trust it. We are done waiting for a reply and we now
                    // broadcast the request to all nodes.

                    let mut retry_targeted = Vec::new();
                    let mut retry_broadcast = Vec::new();
                    for (digest, (_, timestamp, attempts)) in self.parent_requests.iter_mut() {
                        if *timestamp + (self.sync_retry_delay as u128) <= now {
                            debug!(
                                "Requesting sync for certificate {} (retry #{})",
                                digest,
                                attempts.saturating_add(1)
                            );
                            *timestamp = now;
                            *attempts = attempts.saturating_add(1);
                            if *attempts >= BROADCAST_RETRY_THRESHOLD {
                                retry_broadcast.push(digest.clone());
                            } else {
                                retry_targeted.push(digest.clone());
                            }
                        }
                    }

                    if !retry_targeted.is_empty() {
                        // ĐỒNG BỘ SIÊU NHANH: Khi retry, gửi đến TẤT CẢ nodes để tăng tốc độ sync tối đa
                        let addresses: Vec<_> = self
                            .committee
                            .others_primaries(&self.name)
                            .iter()
                            .map(|(_, x)| x.primary_to_primary)
                            .collect();
                        let message = PrimaryMessage::CertificatesRequest(
                            retry_targeted.clone(),
                            self.name,
                        );
                        let bytes =
                            Bytes::from(bincode::serialize(&message).expect("Failed to serialize cert request"));

                        // Gửi đến TẤT CẢ nodes khi retry để tăng tốc độ sync tối đa
                        self.network.broadcast(addresses, bytes).await;
                    }

                    if !retry_broadcast.is_empty() {
                        let addresses: Vec<_> = self
                            .committee
                            .others_primaries(&self.name)
                            .iter()
                            .map(|(_, x)| x.primary_to_primary)
                            .collect();
                        let message =
                            PrimaryMessage::CertificatesRequest(retry_broadcast.clone(), self.name);
                        let bytes =
                            Bytes::from(bincode::serialize(&message).expect("Failed to serialize cert request"));
                        self.network.broadcast(addresses, bytes).await;
                    }

                    // Batch sync retry & recovery
                    let mut retry_batches: HashMap<(PublicKey, WorkerId), Vec<Digest>> = HashMap::new();
                    let mut recovery_requests: Vec<(Digest, PublicKey, WorkerId, Round, u32)> = Vec::new();
                    for (digest, info) in self.batch_requests.iter_mut() {
                        if now.saturating_sub(info.last_request_ms)
                            >= u128::from(self.sync_retry_delay)
                        {
                            info.last_request_ms = now;
                            info.attempts = info.attempts.saturating_add(1);
                            retry_batches
                                .entry((info.author.clone(), info.worker_id))
                                .or_insert_with(Vec::new)
                                .push(digest.clone());
                        }

                        if info.attempts >= BATCH_RECOVERY_RETRY_THRESHOLD {
                            let should_send_recovery = match info.last_recovery_ms {
                                Some(last) => now.saturating_sub(last) >= BATCH_RECOVERY_COOLDOWN_MS,
                                None => true,
                            };
                            if should_send_recovery {
                                info.last_recovery_ms = Some(now);
                                recovery_requests.push((
                                    digest.clone(),
                                    info.author.clone(),
                                    info.worker_id,
                                    info.round,
                                    info.attempts,
                                ));
                                
                                // Structured log: Batch sync retry nhiều lần
                                // Note: HeaderWaiter không có structured_logger, sẽ log qua batch log file
                                warn!(
                                    "[SYNC BATCH RETRY HIGH] HeaderWaiter {} batch {} sync retry #{} (worker {}, author: {}, round: {}). Batch may be stuck!",
                                    self.name, digest, info.attempts, info.worker_id, info.author, info.round
                                );
                            }
                        }
                    }

                    for ((author, worker_id), digests) in retry_batches {
                        let batch_count = digests.len();
                        let digests_sample: Vec<_> = digests.iter().take(3).cloned().collect();
                        info!(
                            "[SYNC BATCHES RETRY] HeaderWaiter {} retrying sync for {} batches (worker {}, author: {}) after {} ms. Sample: {:?}",
                            self.name,
                            batch_count,
                            worker_id,
                            author,
                            self.sync_retry_delay,
                            digests_sample
                        );

                        let author_address = self
                            .committee
                            .worker(&author, &worker_id)
                            .expect("Author of valid header is not in the committee")
                            .primary_to_worker;
                        let message =
                            PrimaryWorkerMessage::Synchronize(digests.clone(), author.clone());
                        let bytes =
                            bincode::serialize(&message).expect("Failed to serialize batch sync request");

                        self.network.send(author_address, Bytes::from(bytes.clone())).await;

                        let other_workers: Vec<_> = self
                            .committee
                            .others_primaries(&self.name)
                            .iter()
                            .filter_map(|(other_author, _)| {
                                self.committee.worker(other_author, &worker_id).ok().map(|addr| addr.primary_to_worker)
                            })
                            .collect();

                        for worker_addr in other_workers {
                            self.network
                                .send(worker_addr, Bytes::from(bytes.clone()))
                                .await;
                        }
                    }

                    for (digest, author, worker_id, round, attempts) in recovery_requests {
                        let author_address = self
                            .committee
                            .primary(&author)
                            .expect("Author of valid header not in the committee")
                            .primary_to_primary;
                        let recovery_message = PrimaryMessage::BatchSyncRecovery {
                            digest: digest.clone(),
                            worker_id,
                            author: author.clone(),
                            requester: self.name,
                            round,
                            attempts,
                        };
                        let bytes = bincode::serialize(&recovery_message)
                            .expect("Failed to serialize batch recovery request");
                        self.network.send(author_address, Bytes::from(bytes)).await;
                        info!(
                            "[BATCH RECOVERY REQUEST] HeaderWaiter {} requested author {} to re-broadcast batch {} (worker {}, round {}, attempts={})",
                            self.name,
                            author,
                            digest,
                            worker_id,
                            round,
                            attempts
                        );
                    }

                    if !(retry_targeted.is_empty() && retry_broadcast.is_empty()) {
                        info!(
                            "HeaderWaiter {:?}: retrying {} parent certificates after {} ms (pending headers={}, targeted={}, broadcast={})",
                            self.name,
                            retry_targeted.len() + retry_broadcast.len(),
                            self.sync_retry_delay,
                            self.pending.len(),
                            retry_targeted.len(),
                            retry_broadcast.len()
                        );
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let mut gc_round = round - self.gc_depth;

                for (r, handler) in self.pending.values() {
                    if r <= &gc_round {
                        let _ = handler.send(()).await;
                    }
                }
                self.pending.retain(|_, (r, _)| r > &mut gc_round);
                self.batch_requests.retain(|_, info| info.round > gc_round);
                self.parent_requests
                    .retain(|_, (r, _, _)| r > &mut gc_round);
            }
        }
    }
}
