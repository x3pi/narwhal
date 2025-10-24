// In node/src/state_syncer.rs

use crate::Shutdown; // Import the Shutdown enum
use config::Committee;
use crypto::PublicKey;
use log::{info, warn};
use network::ReliableSender;
use std::sync::Arc;
use store::Store;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

/// StateSyncer is responsible for keeping a non-validator node up to date
/// by fetching data from other validator nodes.
pub struct StateSyncer {
    /// The name (PublicKey) of this node.
    name: PublicKey,
    /// The current committee, to know who the validators are and their p2p addresses.
    committee: Arc<RwLock<Committee>>,
    /// The local storage to write synchronized data to.
    _store: Store,
    /// The network sender to request data.
    _network: ReliableSender,
    /// Channel to receive the shutdown signal.
    shutdown_receiver: broadcast::Receiver<Shutdown>, // FIX: Changed type to Shutdown
}

impl StateSyncer {
    /// Spawns a new StateSyncer in a separate Tokio task.
    pub fn spawn(
        name: PublicKey,
        committee: Arc<RwLock<Committee>>,
        store: Store,
        shutdown_receiver: broadcast::Receiver<Shutdown>, // FIX: Changed type to Shutdown
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                _store: store,
                _network: ReliableSender::new(),
                shutdown_receiver,
            }
            .run()
            .await;
        });
    }

    /// The main loop of the StateSyncer.
    async fn run(&mut self) {
        info!("[{:?}] Starting StateSyncer in Follower mode.", self.name);

        // Create a timer to periodically attempt synchronization.
        let mut sync_timer = interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                // Wait for the next sync tick.
                _ = sync_timer.tick() => {
                    self.synchronize().await;
                },
                // Listen for the shutdown signal.
                _ = self.shutdown_receiver.recv() => {
                    info!("[{:?}] Shutdown signal received, stopping StateSyncer.", self.name);
                    break;
                }
            }
        }
        info!("[{:?}] StateSyncer has been terminated.", self.name);
    }

    /// Performs the actual synchronization logic.
    async fn synchronize(&self) {
        let committee = self.committee.read().await;

        let peer_addresses: Vec<_> = committee
            .authorities
            .values()
            .map(|auth| auth.p2p_address.clone())
            .collect();

        if peer_addresses.is_empty() {
            warn!(
                "[{:?}] No peer P2P addresses available in the committee to sync from.",
                self.name
            );
            return;
        }

        info!(
            "[{:?}] Attempting to sync state from peers: {:?}",
            self.name, peer_addresses
        );

        //
        // --- ACTUAL SYNC LOGIC TO BE ADDED HERE ---
        //
        // 1. Select one or more peers from `peer_addresses`.
        // 2. Send a data request message (e.g., `StateRequestMessage`).
        //    `self.network.send(peer_address, serialized_request).await;`
        // 3. Wait for the response.
        // 4. Process the response (deserialize, validate, and write to `self.store`).
        //
        // For now, we will just log a placeholder message.
        //
        warn!(
            "[{:?}] State synchronization logic not yet implemented.",
            self.name
        );
    }
}
