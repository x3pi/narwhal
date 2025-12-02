// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::{CommittedBatches, PrimaryWorkerMessage};
use bytes::Bytes;
use config::Committee;
use crypto::{Hash, PublicKey};
use log::{error, info, warn};
use network::SimpleSender;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    rx_consensus: Receiver<Certificate>,
    /// The network addresses of our workers.
    addresses: Vec<SocketAddr>,
    /// A network sender to notify our workers of cleanup events.
    network: SimpleSender,
    /// Notify the proposer about committed batches so it can release in-flight digests.
    tx_committed: Sender<CommittedBatches>,
}

impl GarbageCollector {
    pub fn spawn(
        name: &PublicKey,
        committee: &Committee,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<Certificate>,
        tx_committed: Sender<CommittedBatches>,
    ) {
        let addresses = committee
            .our_workers(name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();

        tokio::spawn(async move {
            Self {
                consensus_round,
                rx_consensus,
                addresses,
                network: SimpleSender::new(),
                tx_committed,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;
        info!(
            target: "narwhal_audit",
            "[GARBAGE COLLECTOR] GarbageCollector started and waiting for certificates from consensus"
        );
        while let Some(certificate) = self.rx_consensus.recv().await {
            let round = certificate.round();
            info!(
                target: "narwhal_audit",
                "[GARBAGE COLLECTOR] GarbageCollector received certificate {} (round {}, author: {}, {} batches) from consensus",
                certificate.digest(),
                round,
                certificate.origin(),
                certificate.header.payload.len()
            );

            // Update consensus round if this is a new round
            if round > last_committed_round {
                last_committed_round = round;
                self.consensus_round.store(round, Ordering::Relaxed);

                // Trigger cleanup on the workers only once per round
                let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                    .expect("Failed to serialize our own message");
                self.network
                    .broadcast(self.addresses.clone(), Bytes::from(bytes))
                    .await;
            }

            // IMPORTANT: Send batch digests from ALL certificates committed in this round
            // This ensures proposer knows about batches committed in certificates from other primaries
            // as well as its own certificate
            let digests: Vec<_> = certificate.header.payload.keys().cloned().collect();

            // BATCH TRACKING: Log when sending committed batches to proposer
            info!(
                target: "narwhal_audit",
                "[BATCH TRACK GC] GarbageCollector sending {} committed batches to proposer at round {} from certificate {} (author: {}): {:?}",
                digests.len(),
                round,
                certificate.header.id,
                certificate.origin(),
                digests.iter().take(10).collect::<Vec<_>>()
            );

            // CRITICAL: Use try_send first to avoid blocking, then fallback to blocking send
            // This ensures proposer always gets notified about committed rounds
            match self.tx_committed.try_send(CommittedBatches {
                round,
                digests: digests.clone(),
            }) {
                Ok(()) => {
                    info!(
                        target: "narwhal_audit",
                        "[COMMITTED BATCHES SENT] GarbageCollector successfully sent committed round {} ({} batches) to proposer from certificate {} (author: {})",
                        round,
                        digests.len(),
                        certificate.header.id,
                        certificate.origin()
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    // Channel is full - use blocking send to ensure message is delivered
                    warn!(
                        target: "narwhal_audit",
                        "[COMMITTED BATCHES CHANNEL FULL] GarbageCollector channel is full! Using blocking send for committed round {} ({} batches). This may indicate proposer is slow or channel capacity is too small.",
                        round,
                        digests.len()
                    );
                    if let Err(e) = self
                        .tx_committed
                        .send(CommittedBatches {
                            round,
                            digests: digests.clone(),
                        })
                        .await
                    {
                        error!(
                            target: "narwhal_audit",
                            "[COMMITTED BATCHES SEND FAILED] GarbageCollector CRITICAL: failed to notify proposer about committed round {} ({} batches): {}. Channel may be closed! This will cause latest_committed_round to not be updated and ROUND GUARD will block header creation!",
                            round, digests.len(), e
                        );
                    } else {
                        warn!(
                            target: "narwhal_audit",
                            "[COMMITTED BATCHES SENT BLOCKING] GarbageCollector sent committed round {} ({} batches) via blocking send. Channel was full - consider increasing channel capacity.",
                            round,
                            digests.len()
                        );
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    error!(
                        target: "narwhal_audit",
                        "[COMMITTED BATCHES CHANNEL CLOSED] GarbageCollector CRITICAL: channel to proposer is CLOSED! Cannot notify about committed round {} ({} batches). This will cause latest_committed_round to not be updated and system will be stuck!",
                        round, digests.len()
                    );
                }
            }
        }
    }
}
