use std::error::Error;

use crate::batch_loader::BatchLoader;
use crate::core::Core;
use async_trait::async_trait;
use batch_loader::SerializedBatchMessage;
use bytes::Bytes;
use config::{Committee, Parameters};
use crypto::PublicKey;
use log::info;
use network::Receiver as NetworkReceiver;
use network::{MessageHandler, Writer};
use primary::Certificate;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

mod batch_loader;
mod core;
pub mod transaction;

/// The default channel capacity for each channel of the executor.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Executor;

impl Executor {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store_path: &str,
        rx_consensus: Receiver<Certificate>,
    ) {
        let (tx_worker, rx_worker) = channel(CHANNEL_CAPACITY);
        let (tx_core, rx_core) = channel(CHANNEL_CAPACITY);

        // Spawn the network receiver listening to messages from the other primaries.
        let mut address = committee
            .executor(&name)
            .expect("Our public key is not in the committee")
            .worker_to_executor;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            ExecutorReceiverHandler { tx_worker },
        );
        info!("Executor {} listening to batches on {}", name, address);

        // Make the data store.
        let store = Store::new(store_path).expect("Failed to create a store");

        // The `BatchLoader` download the batches of all certificates referenced by sequenced
        // certificates into the local store.
        BatchLoader::spawn(
            name,
            committee,
            store,
            rx_consensus,
            rx_worker,
            tx_core,
            parameters.sync_retry_delay,
        );

        // The execution `Core` execute every sequenced transaction.
        Core::spawn(/* rx_batch_loader */ rx_core);
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct ExecutorReceiverHandler {
    tx_worker: Sender<SerializedBatchMessage>,
}

#[async_trait]
impl MessageHandler for ExecutorReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        self.tx_worker
            .send(serialized.to_vec())
            .await
            .expect("failed to send batch to executor");
        Ok(())
    }
}
