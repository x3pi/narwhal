// In primary/src/proposer.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::{CommittedBatches, PendingBatches, ReconfigureNotification};
// SỬA LỖI: Thêm Epoch
use crate::{Epoch, Round};
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::info;
use log::{debug, error, trace, warn};
use std::sync::atomic::{AtomicBool, Ordering}; // <-- Thêm Ordering
use std::sync::Arc;
use store::Store;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

// THÊM: Import hằng số từ core
// *** THAY ĐỔI BẮT ĐẦU: Import QUIET_PERIOD_ROUNDS ***
use crate::core::{QUIET_PERIOD_ROUNDS, RECONFIGURE_INTERVAL};
// *** THAY ĐỔI KẾT THÚC ***

pub struct Proposer {
    name: PublicKey,
    committee: Arc<RwLock<Committee>>,
    signature_service: SignatureService,
    store: Store,
    header_size: usize,
    max_header_delay: u64,

    pending_batches: PendingBatches,
    committed_batches: CommittedBatches,
    rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,

    rx_core: Receiver<(Vec<Digest>, Round)>, // Receives parents (digests) and the round they form (Round N)
    rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>, // Receives own batches from workers
    rx_repropose: Receiver<(Digest, WorkerId, Vec<u8>)>, // Receives batches to re-propose
    tx_core: Sender<Header>,                 // Sends newly created headers to Core

    epoch: Epoch,                         // Internal epoch tracker
    round: Round, // Current round for which we are collecting payload to propose Header(Round)
    last_parents: Vec<Digest>, // Parent certificates' digests for the *next* header (Header(Round))
    digests: Vec<(Digest, WorkerId)>, // Payload digests collected for the *next* header (Header(Round))
    payload_size: usize,              // Current size of the payload collected
    epoch_transitioning: Arc<AtomicBool>, // Flag indicating reconfiguration is in progress
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Arc<RwLock<Committee>>,
        signature_service: SignatureService,
        store: Store,
        header_size: usize,
        max_header_delay: u64,
        pending_batches: PendingBatches,
        committed_batches: CommittedBatches,
        epoch_transitioning: Arc<AtomicBool>, // Pass the flag
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_workers: Receiver<(Digest, WorkerId, Vec<u8>)>,
        rx_repropose: Receiver<(Digest, WorkerId, Vec<u8>)>,
        tx_core: Sender<Header>,
        rx_reconfigure: broadcast::Receiver<ReconfigureNotification>,
    ) {
        tokio::spawn(async move {
            // Get initial committee state and genesis parents
            let initial_committee = committee.read().await;
            let genesis = Certificate::genesis(&*initial_committee)
                .iter()
                .map(|x| x.digest())
                .collect();
            let initial_epoch = initial_committee.epoch;
            drop(initial_committee); // Release lock

            Self {
                name,
                committee,
                signature_service,
                store,
                header_size,
                max_header_delay,
                pending_batches,
                committed_batches,
                rx_reconfigure,
                rx_core,
                rx_workers,
                rx_repropose,
                tx_core,
                epoch: initial_epoch,  // Initialize internal epoch
                round: 1,              // Start at round 1 for the first header
                last_parents: genesis, // Genesis certs are parents for Round 1 header
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                epoch_transitioning, // Store the flag
            }
            .run()
            .await;
        });
    }

    // Creates a new header based on the current state (round, parents, payload).
    async fn make_header(&mut self) {
        // Drain collected payload digests for this header.
        let digests_for_header: Vec<_> = self.digests.drain(..).collect();
        let current_payload_size = self.payload_size; // Store size before reset
        self.payload_size = 0; // Reset payload size for the next round

        // Ensure we have parents before creating a header. Should always be true except maybe at init/reset error.
        if self.last_parents.is_empty() && self.round > 1 {
            // Avoid creating header if parents are missing (unless it's round 1 using genesis)
            warn!(
                "[Proposer][E{}] make_header called for round {} with no parents. Skipping.",
                self.epoch, self.round
            );
            // Put digests back if we skip header creation? No, they belong to this round attempt.
            return;
        }

        let current_epoch = self.epoch; // Use internal epoch tracker.

        // Create the header using current round, epoch, payload, and parents.
        // Parents are drained from `last_parents`.
        let header = Header::new(
            self.name,
            self.round,
            current_epoch,
            digests_for_header.iter().cloned().collect(), // Payload map
            self.last_parents.drain(..).collect(),        // Parent set, drains last_parents
            &mut self.signature_service,
        )
        .await;

        // *** THAY ĐỔI: Log nếu header rỗng trong thời gian đệm ***
        if digests_for_header.is_empty()
            && self.round >= (RECONFIGURE_INTERVAL.saturating_sub(QUIET_PERIOD_ROUNDS))
        {
            info!(
                "[Proposer][E{}] Created EMPTY quiet period Header H{}({})",
                header.epoch, header.round, header.author
            );
        } else {
            debug!(
                "[Proposer][E{}] Created Header H{}({}) with {} digests ({} bytes), {} parents.",
                header.epoch,
                header.round,
                header.author,
                digests_for_header.len(),
                current_payload_size, // Log size before reset
                header.parents.len()
            );
        }

        // Add the included digests to the pending map (to avoid re-proposing them immediately).
        for (digest, worker_id) in digests_for_header {
            self.pending_batches.insert(digest, (worker_id, self.round));
        }

        // Benchmark logging (optional).
        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            info!("Created {} -> {:?}", header, digest);
        }

        // Send the newly created header to the Core component.
        if let Err(e) = self.tx_core.send(header).await {
            error!(
                "[Proposer][E{}] Failed to send header for round {} to Core: {}",
                self.epoch, self.round, e
            );
        }

        // Advance to the next round. Parents for this next round are now unknown.
        self.round += 1;
        debug!(
            "[Proposer][E{}] Advanced to round {}",
            self.epoch, self.round
        );
        // last_parents is now empty, waiting for Core to send parents for the new round.
    }

    // Main execution loop for the Proposer task.
    pub async fn run(&mut self) {
        debug!(
            "[Proposer][E{}] Starting proposer task, initial round {}",
            self.epoch, self.round
        );
        // Initialize the timer for the maximum header delay.
        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // *** THAY ĐỔI BẮT ĐẦU: Thay 'is_grace_period' bằng 'is_quiet_period' ***
            // (Round >= 95 VÀ CHƯA reset sang epoch mới)
            let is_quiet_period =
                self.round >= (RECONFIGURE_INTERVAL.saturating_sub(QUIET_PERIOD_ROUNDS)); // 100 - 5 = 95
                                                                                          // *** THAY ĐỔI KẾT THÚC ***

            // Check if it's time to propose a header. Conditions:
            // 1. We have parents for the current round (`!last_parents.is_empty()`). Genesis provides parents for R1.
            // 2. EITHER the timer expired OR payload size threshold met OR (***MỚI***) ta đang trong round đệm (để ép tạo header rỗng)
            // 3. We are NOT currently in the middle of an epoch transition.
            // *** THAY ĐỔI BẮT ĐẦU: Sử dụng is_quiet_period ***
            let should_propose = !self.last_parents.is_empty()
                && (timer.is_elapsed() || self.payload_size >= self.header_size || is_quiet_period) // <-- THAY ĐỔI
                && !self.epoch_transitioning.load(Ordering::Relaxed);
            // *** THAY ĐỔI KẾT THÚC ***

            // If conditions met, create a header.
            if should_propose {
                // We only create a header if either the timer expired (forces proposal, possibly empty)
                // or if we have accumulated payload digests.
                // *** THAY ĐỔI BẮT ĐẦU: Sử dụng is_quiet_period ***
                if !self.digests.is_empty() || timer.is_elapsed() || is_quiet_period {
                    if is_quiet_period && self.digests.is_empty() {
                        // Log riêng cho trường hợp tạo header rỗng
                        info!(
                            "[Proposer][E{}] Quiet period (R{}). Timer expired. Proposing empty header.", // <-- THAY ĐỔI
                            self.epoch, self.round
                        );
                    } else {
                        info!(
                            "[Proposer][E{}] Conditions met for round {}: TimerExpired={}, PayloadSize={}/{}, ParentsExist={}. Creating header.",
                            self.epoch, self.round, timer.is_elapsed(), self.payload_size, self.header_size, !self.last_parents.is_empty()
                        );
                    }
                    self.make_header().await; // Creates header, advances round, clears parents/payload
                } else {
                    // This case (payload threshold met but no digests) shouldn't happen due to payload_size reset.
                    // If timer expired and no digests, make_header handles the empty case.
                    debug!(
                        "[Proposer][E{}] Conditions met for round {} but no payload and timer not expired. Waiting.",
                         self.epoch, self.round
                     );
                }
                // Reset the timer regardless of whether a header was created (make_header advances round).
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            // Select the next event.
            tokio::select! {
                // Handle reconfiguration signal.
                result = self.rx_reconfigure.recv() => {
                    match result {
                        Ok(notification) => {
                            // --- Anti-Race Condition Logic ---
                             // Check 1: Is this signal for the *end* of our current epoch?
                            if notification.committee.epoch != self.epoch {
                                warn!(
                                    "[Proposer] Ignoring stale reconfigure signal for epoch {} (current is {})",
                                    notification.committee.epoch, self.epoch
                                );
                                continue; // Ignore signal if it's for a different epoch.
                            }

                             // Check 2: Wait until the shared Committee Arc has actually been updated.
                            info!("[Proposer] Received reconfigure signal for end of epoch {}. Waiting for committee update...", self.epoch);
                            let mut updated_committee_epoch = self.epoch;
                            let mut new_committee_for_reset = None;

                            while updated_committee_epoch <= self.epoch {
                                sleep(Duration::from_millis(100)).await;
                                let committee_guard = self.committee.read().await;
                                updated_committee_epoch = committee_guard.epoch;
                                if updated_committee_epoch > self.epoch {
                                    new_committee_for_reset = Some(committee_guard.clone());
                                }
                                drop(committee_guard);
                            }

                            if let Some(new_committee) = new_committee_for_reset {
                                info!(
                                    "[Proposer] Committee Arc updated to epoch {}. Full reset initiated.",
                                    updated_committee_epoch
                                );
                                self.epoch_transitioning.store(true, Ordering::Relaxed); // Signal transition start

                                // 1. Update internal epoch tracker.
                                self.epoch = updated_committee_epoch;

                                // 2. Perform FULL state reset for the new epoch.
                                self.round = 1; // Reset round counter to 1.
                                self.digests.clear(); // Clear old payload digests from previous epoch
                                self.payload_size = 0; // Reset payload size
                                self.last_parents.clear(); // Clear old parents.

                                // 3. Get NEW genesis parents for the new epoch.
                                self.last_parents = Certificate::genesis(&new_committee)
                                    .iter()
                                    .map(|x| x.digest())
                                    .collect();

                                // 4. Log clean state
                                info!(
                                    "[Proposer] Starting epoch {} with clean state (0 digests, 0 bytes)",
                                    self.epoch
                                );


                                // 5. Immediately propose the first header (Round 1) if genesis parents exist.
                                if !self.last_parents.is_empty() {
                                    info!(
                                        "[Proposer] Creating initial header for epoch {} round {}",
                                        self.epoch, self.round
                                    );
                                    // Make header will use genesis parents, create H1, clear parents, advance round to 2.
                                    // Uses empty payload (digests already cleared for clean epoch transition)
                                    self.make_header().await;
                                } else {
                                    // This is a critical error if genesis fails.
                                    error!(
                                        "[Proposer] CRITICAL - No genesis parents found after committee update for epoch {}!",
                                        self.epoch
                                    );
                                    // State is inconsistent, maybe panic or shut down?
                                }

                                // 6. Reset the timer for the next round (Round 2).
                                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                                timer.as_mut().reset(deadline);

                                info!(
                                    "[Proposer] Reset complete for epoch {}. Now at round {}. Ready.",
                                     self.epoch, self.round // Should be 2 now
                                );
                                // Drain any stale parents from previous epoch buffered in rx_core to avoid cross-epoch leakage
                                let mut drained = 0usize;
                                loop {
                                    match self.rx_core.try_recv() {
                                        Ok((_parents, r)) => {
                                            drained += 1;
                                            warn!(
                                                "[Proposer][E{}] Drained stale parents from round {} after epoch switch.",
                                                self.epoch, r
                                            );
                                            // keep draining
                                        }
                                        Err(_e) => break,
                                    }
                                }
                                if drained > 0 {
                                    info!(
                                        "[Proposer][E{}] Drained {} stale parent message(s) after epoch reset.",
                                        self.epoch, drained
                                    );
                                }

                                self.epoch_transitioning.store(false, Ordering::Relaxed); // Signal transition end
                            } else {
                                error!("[Proposer] Logic error during reconfigure: Committee epoch updated but data not captured.");
                            }
                            // --- End Anti-Race Condition Logic ---
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("[Proposer] Reconfigure receiver lagged by {}. Missed epoch transitions!", n);
                            // Consider requesting state sync or shutdown.
                        },
                        Err(broadcast::error::RecvError::Closed) => {
                            error!("[Proposer] Reconfigure channel closed unexpectedly. Shutting down.");
                            break; // Exit the loop.
                        }
                    }
                },

                // Handle receiving parent certificates from the Core.
                // The tuple contains (parent_digests, round_number_N)
                // These parents are needed to propose Header for Round N+1.
                Some((parents, round)) = self.rx_core.recv() => {
                    // *** FIX: Stale Parent Check ***
                    // Only accept parents if they are exactly for the round preceding the one we are currently trying to build.
                    // `self.round` is the round we are *about* to propose.
                    // The parents received should be from `self.round - 1`.
                    // Also check: round should be reasonable (not from a completely different epoch)
                    let expected_parent_round = if self.round > 1 { self.round - 1 } else { 0 };

                    // If the received round is much larger than current round, it's likely from an old epoch
                    // and should be ignored
                    if round > RECONFIGURE_INTERVAL && self.round < 10 {
                        warn!(
                            "[Proposer][E{}] Ignoring parents from round {} (likely from old epoch). Current round: {}.",
                            self.epoch, round, self.round
                        );
                        continue;
                    }

                    if round == expected_parent_round {
                        // Accept these parents for the header of `self.round`.
                        if self.last_parents.is_empty() { // Only replace if we don't have parents yet for this round
                           self.last_parents = parents;
                           debug!("[Proposer][E{}] Received valid parents from round {} for header {}", self.epoch, round, self.round);

                            // *** THAY ĐỔI BẮT ĐẦU: Sử dụng is_quiet_period ***
                            // Check if receiving parents triggers immediate proposal (if payload full or timer expired)
                            if !self.epoch_transitioning.load(Ordering::Relaxed) &&
                               (self.payload_size >= self.header_size || timer.is_elapsed() || is_quiet_period) { // <-- THAY ĐỔI

                                if !self.digests.is_empty() || timer.is_elapsed() || is_quiet_period { // <-- THAY ĐỔI
                                    info!(
                                        "[Proposer][E{}] Parents received for round {}, payload/timer/quiet condition met. Proposing header {}.", // <-- THAY ĐỔI
                                        self.epoch, round, self.round
                                    );
                                    self.make_header().await; // Creates header, advances round, clears parents/payload
                                     // Reset timer after proposing.
                                    let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                                    timer.as_mut().reset(deadline);
                                }
                            }
                            // *** THAY ĐỔI KẾT THÚC ***
                        } else {
                             debug!("[Proposer][E{}] Received duplicate parents from round {} for header {}. Ignoring.", self.epoch, round, self.round);
                        }

                    } else {
                        // Received parents are for a different round than expected (likely stale or future).
                        warn!(
                            "[Proposer][E{}] Ignoring parents from unexpected round {} (expected parents for round {}).",
                             self.epoch, round, self.round
                        );
                    }
                },

                // *** THAY ĐỔI BẮT ĐẦU: Thêm '&& !is_quiet_period' ***
                // Handle receiving new batch digests from own workers.
                // Ignore batches if epoch transition is happening OR in quiet period.
                Some((digest, worker_id, batch)) = self.rx_workers.recv(), if !self.epoch_transitioning.load(Ordering::Relaxed) && !is_quiet_period => {
                // *** THAY ĐỔI KẾT THÚC ***
                    // Check if the batch is already committed or pending proposal. Avoid duplicates.
                    if !self.committed_batches.contains_key(&digest) && !self.pending_batches.contains_key(&digest) {
                         // Only store and add if the proposer isn't already aware of this batch.
                        if self.store.read(digest.to_vec()).await.map_or(true, |d| d.is_none()) {
                            // Write batch to store before adding to payload (important for recovery)
                           self.store.write(digest.to_vec(), batch).await;
                        } else {
                            debug!("[Proposer][E{}] Batch {} already in store, skipping write.", self.epoch, digest);
                        }

                        // Add digest to the current payload if size limit not yet reached drastically.
                         // Allow exceeding slightly to avoid complex logic for exact fit.
                        if self.payload_size < self.header_size * 2 { // Allow some overflow
                           self.payload_size += digest.size(); // Use actual digest size if available, approximation is ok too.
                            self.digests.push((digest.clone(), worker_id));
                            trace!("[Proposer][E{}] Added batch {} to payload for round {}. Current size: {}/{}", self.epoch, digest, self.round, self.payload_size, self.header_size);
                        } else {
                            // If payload is full, maybe send back to re-propose? Or just drop for now.
                             warn!("[Proposer][E{}] Payload full for round {}. Batch {} might be delayed.", self.epoch, self.round, digest);
                             // Consider sending to tx_repropose channel if needed.
                        }
                    } else {
                        trace!("[Proposer][E{}] Ignoring already known batch {}", self.epoch, digest);
                    }
                },

                // *** THAY ĐỔI BẮT ĐẦU: Thêm '&& !is_quiet_period' ***
                // Handle receiving batches that need to be re-proposed (e.g., after GC removed them from pending).
                 // Ignore batches if epoch transition is happening OR in quiet period.
                Some((digest, worker_id, batch_data)) = self.rx_repropose.recv(), if !self.epoch_transitioning.load(Ordering::Relaxed) && !is_quiet_period => {
                // *** THAY ĐỔI KẾT THÚC ***
                    // Check again if committed or pending (state might have changed).
                    if !self.committed_batches.contains_key(&digest) && !self.pending_batches.contains_key(&digest) {
                        warn!("[Proposer][E{}] Re-proposing batch {}", self.epoch, digest);
                         // Ensure it's in the store (might have been GC'd from store too, unlikely but possible)
                        if self.store.read(digest.to_vec()).await.map_or(true, |d| d.is_none()) {
                           self.store.write(digest.to_vec(), batch_data).await;
                        }
                        // Add to current payload if space allows.
                         if self.payload_size < self.header_size * 2 {
                           self.payload_size += digest.size();
                            self.digests.push((digest, worker_id));
                        } else {
                             warn!("[Proposer][E{}] Payload full during re-propose for round {}. Batch {} might be delayed again.", self.epoch, self.round, digest);
                        }
                    } else {
                        trace!("[Proposer][E{}] Batch {} to re-propose is already known. Ignoring.", self.epoch, digest);
                    }
                },

                // Handle timer expiration.
                () = &mut timer => {
                    // Only propose if we have parents and are not transitioning.
                     if !self.last_parents.is_empty() && !self.epoch_transitioning.load(Ordering::Relaxed) {
                         // *** THAY ĐỔI BẮT ĐẦU: Cập nhật log timer ***
                         info!(
                            "[Proposer][E{}] Timer expired for round {}. Proposing header (payload size {}, quiet_period={}).",
                             self.epoch, self.round, self.payload_size, is_quiet_period
                        );
                        // *** THAY ĐỔI KẾT THÚC ***
                        self.make_header().await; // Creates header (possibly empty), advances round, clears parents/payload.
                    } else if self.epoch_transitioning.load(Ordering::Relaxed){
                         debug!("[Proposer][E{}] Timer expired during epoch transition. Waiting.", self.epoch);
                    }
                     else {
                         debug!(
                            "[Proposer][E{}] Timer expired but no parents available for round {}. Waiting.",
                             self.epoch, self.round
                        );
                    }
                    // Always reset the timer after it fires.
                     let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                     timer.as_mut().reset(deadline);
                }
            }
        }
    }
} // end impl Proposer
