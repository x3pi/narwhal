use ::prometheus::{
    register_int_counter_vec_with_registry, register_int_counter_with_registry, IntCounter,
    IntCounterVec, Registry,
};

#[derive(Clone)]
pub struct ConsensusMetrics {
    pub committed_certificates_total: IntCounter,
    // pub batch_sealed_total: IntCounterVec,
    // pub quorum_waiter_time_millis_total: IntCounter,
    // pub batch_persisted_total: IntCounterVec,

    // pub batch_requests_received_total: IntCounterVec,
    // pub batch_request_replies_total: IntCounterVec,

    // pub batch_sync_total: IntCounterVec,
    // pub batch_sync_retries_total: IntCounterVec,
}

impl ConsensusMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            committed_certificates_total: register_int_counter_with_registry!(
                "committed_certificates_total",
                "Total committed certificates",
                registry,
            )
            .unwrap(),
            // batch_sealed_total: register_int_counter_vec_with_registry!(
            //     "batch_sealed_total",
            //     "Total number of sealed batches",
            //     &["reason"], // Batch full or timeout.
            //     registry,
            // )
            // .unwrap(),
            // quorum_waiter_time_millis_total: register_int_counter_with_registry!(
            //     "quorum_waiter_time_millis_total",
            //     "Total time to disseminate batches [ms]",
            //     registry,
            // )
            // .unwrap(),
            // batch_persisted_total: register_int_counter_vec_with_registry!(
            //     "batch_persisted_total",
            //     "Total number of batch persisted",
            //     &["origin"], // Owned or from other workers.
            //     registry
            // )
            // .unwrap(),
            // batch_requests_received_total: register_int_counter_vec_with_registry!(
            //     "batch_requests_total",
            //     "Total number of batch requests received",
            //     &["origin"], // The requestor
            //     registry
            // )
            // .unwrap(),
            // batch_request_replies_total: register_int_counter_vec_with_registry!(
            //     "batch_request_replies_total",
            //     "Total number of batch replies",
            //     &["origin"], // The requestor
            //     registry
            // )
            // .unwrap(),
            // batch_sync_total: register_int_counter_vec_with_registry!(
            //     "batch_sync_total",
            //     "Total number of batch requested",
            //     &["target"], // The destination
            //     registry
            // )
            // .unwrap(),
            // batch_sync_retries_total: register_int_counter_vec_with_registry!(
            //     "batch_sync_retries_total",
            //     "Total number of batch sync retries",
            //     &["original_target"], // The original target of our sync request
            //     registry
            // )
            // .unwrap(),
        }
    }
}
