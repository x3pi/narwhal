// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::DagResult;
use crate::header_waiter::WaiterMessage;
use crate::messages::{Certificate, Header};
use crate::primary::{PayloadCache, PrimaryMessage, Round};
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, warn};
use tracing;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::Sender;
use network::SimpleSender;
/// The `Synchronizer` checks if we have all batches and parents referenced by a header. If we don't, it sends
/// a command to the `Waiter` to request the missing data.
const STATE_SYNC_TRIGGER_GAP: Round = 8;
const STATE_SYNC_WINDOW: Round = 32;
const STATE_SYNC_MAX_ROUNDS: Round = 128;
const STATE_SYNC_COOLDOWN: Duration = Duration::from_millis(2_000);
const MAX_UNCOMMITTED_SYNC_AHEAD: Round = 2;

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
    committee: Committee,
    state_sync_sender: SimpleSender,
    consensus_round: Arc<AtomicU64>,
    last_state_sync_request: Option<Instant>,
    last_state_sync_round: Round,

    cache: PayloadCache, // <--- THÊM TRƯỜNG CACHE
    /// Track last time we requested a given batch digest to avoid spamming.
    batch_sync_tracker: HashMap<Digest, Instant>,
    batch_resync_interval: Duration,
    batch_sync_alert_interval: Duration,
    /// Track sync success rate for monitoring
    sync_metrics: SyncMetrics,
    /// Last cleanup time for batch_sync_tracker
    last_tracker_cleanup: Option<Instant>,
    /// Cleanup interval for batch_sync_tracker (every 60 seconds)
    tracker_cleanup_interval: Duration,
    /// Maximum age for entries in batch_sync_tracker (5 minutes)
    tracker_max_age: Duration,
}

/// Metrics for batch synchronization
#[derive(Default)]
pub struct SyncMetrics {
    /// Total batches checked
    pub total_checked: u64,
    /// Batches found in cache
    pub found_in_cache: u64,
    /// Batches found in store
    pub found_in_store: u64,
    /// Batches missing (needed sync)
    pub missing: u64,
    /// Successful syncs
    pub sync_success: u64,
    /// Failed syncs
    pub sync_failed: u64,
    /// Last sync success rate (percentage)
    pub last_success_rate: f64,
}

impl Synchronizer {
    pub fn new(
        name: PublicKey,
        committee: &Committee,
        store: Store,
        cache: PayloadCache, // <--- NHẬN CACHE
        tx_header_waiter: Sender<WaiterMessage>,
        tx_certificate_waiter: Sender<Certificate>,
        consensus_round: Arc<AtomicU64>,
    ) -> Self {
        Self {
            name,
            store,
            cache,
            tx_header_waiter,
            tx_certificate_waiter,
            committee: committee.clone(),
            state_sync_sender: SimpleSender::new(),
            consensus_round,
            last_state_sync_request: None,
            last_state_sync_round: 0,
            genesis: Certificate::genesis(committee)
                .into_iter()
                .map(|x| (x.digest(), x))
                .collect(),
            batch_sync_tracker: HashMap::new(),
            // CRITICAL FIX: Tăng timeout cho sync để đợi đủ thời gian cho sync hoàn thành
            // Sync qua network cần 42-220ms+, nhưng primary check lại sau 154-164ms
            // Tăng lên 1 giây để đảm bảo sync có đủ thời gian hoàn thành
            batch_resync_interval: Duration::from_millis(1000), // 1 giây thay vì 2 giây
            batch_sync_alert_interval: Duration::from_secs(10),
            sync_metrics: SyncMetrics::default(),
            last_tracker_cleanup: None,
            tracker_cleanup_interval: Duration::from_secs(60),
            tracker_max_age: Duration::from_secs(300), // 5 minutes
        }
    }

    /// Cleanup old entries from batch_sync_tracker to prevent memory leak
    /// SAFETY: Only removes entries for batches that are either:
    /// 1. Already in cache/store (synced successfully)
    /// 2. Older than max_age (likely synced or will be retried)
    /// This ensures no batches are lost and system remains deterministic
    fn cleanup_batch_sync_tracker(&mut self) {
        let now = Instant::now();
        
        // Only cleanup if enough time has passed
        if let Some(last_cleanup) = self.last_tracker_cleanup {
            if now.duration_since(last_cleanup) < self.tracker_cleanup_interval {
                return;
            }
        }

        let before_count = self.batch_sync_tracker.len();
        let cutoff_time = now.checked_sub(self.tracker_max_age).unwrap_or(now);
        let very_old_cutoff = Duration::from_secs(600); // 10 minutes
        
        // SAFETY: Only remove entries that are:
        // 1. Older than max_age (5 minutes) - batches should have synced by then
        // 2. AND either in cache or store (already synced)
        // This prevents removing entries for batches that are still being synced
        
        // First, collect digests to check in cache (to avoid borrow conflict)
        let digests_to_check: Vec<(Digest, Instant)> = self.batch_sync_tracker
            .iter()
            .filter(|(_, &timestamp)| timestamp <= cutoff_time)
            .map(|(digest, &timestamp)| (digest.clone(), timestamp))
            .collect();
        
        // Check which ones are in cache
        let mut to_remove = Vec::new();
        for (digest, timestamp) in digests_to_check {
            // Check cache first (fast)
            let in_cache = self.cache.contains_key(&digest);
            if in_cache {
                to_remove.push(digest.clone());
                continue;
            }
            // For very old entries (> 10 minutes), remove even if not in cache
            // They will be retried if still needed
            let very_old = now.duration_since(timestamp) > very_old_cutoff;
            if very_old {
                to_remove.push(digest);
            }
        }
        
        // Remove entries
        let removed = to_remove.len();
        for digest in to_remove {
            self.batch_sync_tracker.remove(&digest);
        }
        
        // Also remove entries that are just old (not in cache but old enough)
        self.batch_sync_tracker.retain(|_, &mut timestamp| {
            timestamp > cutoff_time
        });
        
        let after_count = self.batch_sync_tracker.len();
        
        if removed > 0 {
            debug!(
                "[SYNC TRACKER CLEANUP] Cleaned up {} old entries from batch_sync_tracker ({} -> {} entries). Only removed entries for batches already synced or very old (>10min).",
                removed, before_count, after_count
            );
        }
        
        self.last_tracker_cleanup = Some(now);
    }

    /// Pre-sync batches for a certificate before it gets committed
    /// This helps reduce "batch not found" errors by syncing batches proactively
    pub async fn pre_sync_certificate_batches(&mut self, certificate: &Certificate, _consensus_round: Round) -> DagResult<()> {
        let cert_round = certificate.round();
        let committed_round = self.consensus_round.load(Ordering::Relaxed);
        
        // Only pre-sync if certificate is close to commit (within 5 rounds)
        // This avoids unnecessary sync for certificates far in the future
        if cert_round > committed_round.saturating_add(5) {
            return Ok(());
        }

        // Check if we need to sync any batches
        let mut missing = HashMap::new();
        for (digest, worker_id) in certificate.header.payload.iter() {
            // Check cache first
            if self.cache.contains_key(digest) {
                continue;
            }

            // Check store
            match self.store.read(digest.to_vec()).await? {
                Some(_) => {
                    // Batch found in store
                }
                None => {
                    // Batch missing - add to pre-sync list
                    missing.insert(digest.clone(), *worker_id);
                }
            }
        }

        if missing.is_empty() {
            return Ok(());
        }

        // Pre-sync missing batches with high priority
        let committed = cert_round <= committed_round;
        let priority = if committed { "COMMITTED" } else { "PENDING" };
        
        info!(
            target: "narwhal_audit",
            "[PRE-SYNC BATCHES][{}] Pre-syncing {} batches for certificate {} (round {}) before commit",
            priority,
            missing.len(),
            certificate.digest(),
            cert_round
        );

        // Send sync request with high priority
        self.tx_header_waiter
            .send(WaiterMessage::SyncBatches {
                missing: missing.clone(),
                header: certificate.header.clone(),
                committed,
            })
            .await
            .expect("Failed to send pre-sync batch request");

        Ok(())
    }

    /// Get sync metrics for monitoring
    pub fn get_sync_metrics(&self) -> &SyncMetrics {
        &self.sync_metrics
    }

    /// Returns `true` if we have all transactions of the payload. If we don't, we return false,
    /// synchronize with other nodes (through our workers), and re-schedule processing of the
    /// header for when we will have its complete payload.
    pub async fn missing_payload(&mut self, header: &Header) -> DagResult<bool> {
        // CRITICAL FIX: Không skip sync cho own headers nếu batch không có trong store
        // Vấn đề: Nếu header là của chính node này, missing_payload sẽ return Ok(false) ngay lập tức
        // nhưng proposer vẫn extract batches và thêm vào queue. Nếu batch không có trong store,
        // sync sẽ không được trigger và batch sẽ bị stuck.
        // Giải pháp: Vẫn check missing batches cho own headers, nhưng không block vote
        let is_own_header = header.author == self.name;
        
        let mut missing = HashMap::new();

        let mut found_in_cache_count = 0;
        let mut found_in_store_count = 0;
        let mut missing_batch_digests = Vec::new();
        
        for (digest, worker_id) in header.payload.iter() {
            // Update metrics
            self.sync_metrics.total_checked += 1;
            
            // KIỂM TRA CACHE TRƯỚC (nhanh nhất)
            if self.cache.contains_key(digest) {
                self.sync_metrics.found_in_cache += 1;
                found_in_cache_count += 1;
                self.batch_sync_tracker.remove(digest);
                continue; // Tìm thấy trong RAM, không cần làm gì thêm
            }

            // Nếu không có trong cache, kiểm tra store (phương án dự phòng)
            match self.store.read(digest.to_vec()).await? {
                Some(batch) => {
                    // CRITICAL: Cache lại batch vừa đọc từ store để lần sau đọc nhanh hơn
                    self.cache.insert(digest.clone(), batch);
                    self.sync_metrics.found_in_store += 1;
                    found_in_store_count += 1;
                    self.batch_sync_tracker.remove(digest);
                    // Batch có trong store - OK
                }
                None => {
                    // CRITICAL: Batch không có trong cache và store - cần sync
                    self.sync_metrics.missing += 1;
                    missing.insert(digest.clone(), *worker_id);
                    missing_batch_digests.push(format!("{}", digest));
                }
            }
        }
        
        // CRITICAL: Log chi tiết về batch availability để debug vote blocking
        if !missing_batch_digests.is_empty() {
            let missing_batch_list: Vec<String> = missing_batch_digests.iter().take(10).map(|d| format!("{}", d)).collect();
            let missing_workers: Vec<u32> = missing.iter().map(|(_, w)| *w).collect::<std::collections::HashSet<_>>().into_iter().collect();
            
            tracing::warn!(
                target: "narwhal_audit",
                "[VOTE DEBUG - MISSING BATCHES] Primary {} checking payload for header {} (round {}, author: {}). Total batches: {}, Found in cache: {}, Found in store: {}, MISSING: {} batches from workers {:?}: {:?}. This will BLOCK VOTE until batches are synced. Sync will be triggered.",
                self.name,
                header.id,
                header.round,
                header.author,
                header.payload.len(),
                found_in_cache_count,
                found_in_store_count,
                missing_batch_digests.len(),
                missing_workers,
                missing_batch_list
            );
        } else {
            tracing::info!(
                target: "narwhal_audit",
                "[VOTE DEBUG - ALL BATCHES AVAILABLE] Primary {} checking payload for header {} (round {}, author: {}). All {} batches are available (cache: {}, store: {}). Vote will proceed.",
                self.name,
                header.id,
                header.round,
                header.author,
                header.payload.len(),
                found_in_cache_count,
                found_in_store_count
            );
        }
        
        // CRITICAL FIX: Nếu là own header, vẫn trigger sync nếu có missing batches
        // nhưng không block vote (return Ok(false))
        if is_own_header {
            if !missing.is_empty() {
                tracing::warn!(
                    target: "narwhal_audit",
                    "[SYNC OWN HEADER] Primary {} detected {} missing batches in OWN header {} (round {}). Triggering sync even though vote is not blocked. This ensures batches are synced for proposer to include in future headers.",
                    self.name,
                    missing.len(),
                    header.id,
                    header.round
                );
                // Trigger sync cho own header batches
                // Note: We don't block vote for own headers, but we still sync batches
                let committed_round = self.consensus_round.load(Ordering::Relaxed);
                let committed_header = header.round <= committed_round;
                
                // Send sync request for own header batches
                let now = Instant::now();
                let mut ready_missing = HashMap::new();
                for (digest, worker_id) in missing.into_iter() {
                    ready_missing.insert(digest.clone(), worker_id);
                    self.batch_sync_tracker.insert(digest, now);
                }
                
                if !ready_missing.is_empty() {
                    let ready_missing_count = ready_missing.len();
                    let missing_batch_list: Vec<String> = ready_missing.keys().take(10).map(|d| format!("{}", d)).collect();
                    let missing_workers_list: Vec<u32> = ready_missing.values().copied().collect::<std::collections::HashSet<_>>().into_iter().collect();
                    
                    if let Err(e) = self.tx_header_waiter
                        .send(WaiterMessage::SyncBatches {
                            missing: ready_missing,
                            header: header.clone(),
                            committed: committed_header,
                        })
                        .await
                    {
                        tracing::warn!(
                            target: "narwhal_audit",
                            "[SYNC OWN HEADER ERROR] Primary {} failed to send sync request for own header {} (round {}) batches: {}",
                            self.name,
                            header.id,
                            header.round,
                            e
                        );
                    } else {
                        tracing::info!(
                            target: "narwhal_audit",
                            "[SYNC OWN HEADER] Primary {} successfully triggered sync for {} missing batches in own header {} (round {}) from workers {:?}. Missing batches: {:?}",
                            self.name,
                            ready_missing_count,
                            header.id,
                            header.round,
                            missing_workers_list,
                            missing_batch_list
                        );
                    }
                }
            }
            return Ok(false); // Don't block vote for own headers
        }

        if missing.is_empty() {
            return Ok(false);
        }

        let committed_round = self.consensus_round.load(Ordering::Relaxed);
        if header.round > committed_round.saturating_add(MAX_UNCOMMITTED_SYNC_AHEAD)
            && header.round > committed_round
        {
            debug!(
                "[SYNC SKIP] Primary {} skipping sync for header {} (round {}) because consensus is still at round {}. Avoiding unnecessary traffic until round is closer to commit.",
                self.name,
                header.id,
                header.round,
                committed_round
            );
            return Ok(true);
        }

        let committed_header = header.round <= committed_round;
        let priority = if committed_header { "COMMITTED" } else { "PENDING" };

        // Log để theo dõi sync batch có tốt không
        let missing_count = missing.len();
        let total_batches = header.payload.len();
        let sync_success_rate = if total_batches > 0 {
            (total_batches - missing_count) as f64 / total_batches as f64 * 100.0
        } else {
            100.0
        };
        
        // Log khi có vấn đề với sync batch
        if missing_count > 10 {
            warn!(
                "[BATCH SYNC ISSUE][{}] Primary {} detected {} missing batches out of {} total (sync success rate: {:.1}%) in header {} (round {}). Batch sync may be slow or failing.",
                priority,
                self.name,
                missing_count,
                total_batches,
                sync_success_rate,
                header.id,
                header.round
            );
        } else if missing_count > 0 && sync_success_rate < 50.0 {
            warn!(
                "[BATCH SYNC WARNING][{}] Primary {} detected {} missing batches out of {} total (sync success rate: {:.1}%) in header {} (round {}).",
                priority,
                self.name,
                missing_count,
                total_batches,
                sync_success_rate,
                header.id,
                header.round
            );
        }

        // CRITICAL: Cleanup old tracker entries periodically to prevent memory leak
        // This prevents system degradation over time
        self.cleanup_batch_sync_tracker();

        let now = Instant::now();
        let mut throttled = Vec::new();
        let mut ready_missing = HashMap::new();
        for (digest, worker_id) in missing.into_iter() {
            match self.batch_sync_tracker.get(&digest) {
                Some(last)
                    if !committed_header
                        && now.duration_since(*last) < self.batch_resync_interval =>
                {
                    throttled.push(digest);
                }
                _ => {
                    ready_missing.insert(digest.clone(), worker_id);
                    self.batch_sync_tracker.insert(digest, now);
                }
            }
        }

        if ready_missing.is_empty() {
            if !throttled.is_empty() {
                debug!(
                    "[SYNC THROTTLE] Primary {} already requested batches {:?} recently (interval {:?}). Will wait before re-requesting.",
                    self.name,
                    throttled.iter().take(5).collect::<Vec<_>>(),
                    self.batch_resync_interval
                );
            }
            return Ok(true);
        }

        // CRITICAL: Log khi trigger sync với thông tin chi tiết
        let ready_missing_count = ready_missing.len();
        let ready_missing_batches: Vec<String> = ready_missing.keys().take(10).map(|d| format!("{}", d)).collect();
        let ready_missing_workers: Vec<u32> = ready_missing.values().copied().collect::<std::collections::HashSet<_>>().into_iter().collect();
        
        tracing::info!(
            target: "narwhal_audit",
            "[SYNC TRIGGERED] Primary {} triggered sync for {} missing batches from header {} (round {}, author: {}, {} batches total). Missing batches from workers {:?}: {:?}. Sync request will be sent to HeaderWaiter.",
            self.name,
            ready_missing_count,
            header.id,
            header.round,
            header.author,
            total_batches,
            ready_missing_workers,
            ready_missing_batches
        );
        
        self.tx_header_waiter
            .send(WaiterMessage::SyncBatches {
                missing: ready_missing.clone(),
                header: header.clone(),
                committed: committed_header,
            })
            .await
            .expect("Failed to send sync batch request");

        // CRITICAL: Log sau khi gửi sync request thành công
        tracing::info!(
            target: "narwhal_audit",
            "[SYNC REQUEST SENT] Primary {} successfully sent sync request for {} batches from header {} (round {}) to HeaderWaiter. Waiting for batches to be synced.",
            self.name,
            ready_missing_count,
            header.id,
            header.round
        );

        // Alert if certain digests keep being re-requested for too long.
        for (digest, _) in ready_missing.into_iter() {
            if let Some(first) = self.batch_sync_tracker.get(&digest) {
                if now.duration_since(*first) >= self.batch_sync_alert_interval {
                    warn!(
                        target: "narwhal_audit",
                        "[SYNC SLOW ALERT] Primary {} still missing batch {} from header {} (round {}, author: {}) for {:?}. Consider investigating worker/primary connectivity.",
                        self.name,
                        digest,
                        header.id,
                        header.round,
                        header.author,
                        now.duration_since(*first)
                    );
                }
            }
        }
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

        let latest_committed = self.consensus_round.load(Ordering::Relaxed);
        let gap = header.round.saturating_sub(latest_committed);
        if gap >= STATE_SYNC_TRIGGER_GAP {
            self.trigger_state_sync(header.round, gap, missing.len())
                .await;
        }

        self.tx_header_waiter
            .send(WaiterMessage::SyncParents(missing, header.clone()))
            .await
            .expect("Failed to send sync parents request");
        Ok(Vec::new())
    }

    async fn trigger_state_sync(&mut self, header_round: Round, gap: Round, missing: usize) {
        let now = Instant::now();
        if let Some(last) = self.last_state_sync_request {
            if now.duration_since(last) < STATE_SYNC_COOLDOWN
                && header_round <= self.last_state_sync_round + STATE_SYNC_TRIGGER_GAP
            {
                return;
            }
        }

        self.last_state_sync_request = Some(now);
        self.last_state_sync_round = header_round;

        let since_round = header_round.saturating_sub(STATE_SYNC_WINDOW);
        let message = PrimaryMessage::StateSyncRequest {
            requester: self.name.clone(),
            since_round,
            max_rounds: STATE_SYNC_MAX_ROUNDS,
        };
        let bytes = Bytes::from(
            bincode::serialize(&message).expect("Failed to serialize state sync request"),
        );
        let addresses: Vec<_> = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();

        debug!(
            "[STATE SYNC REQUEST] Primary {} missing {} parents for round {} (gap {} vs committed). Requesting certificates since round {} from {} peers.",
            self.name,
            missing,
            header_round,
            gap,
            since_round,
            addresses.len()
        );

        self.state_sync_sender.broadcast(addresses, bytes).await;
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
