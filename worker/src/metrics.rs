use prometheus::{
    register_int_counter_vec_with_registry, register_int_counter_with_registry, IntCounter,
    IntCounterVec, Registry,
};

#[derive(Clone)]
pub struct WorkerMetrics {
    pub batch_size_bytes_total: IntCounter,
    pub batch_sealed_total: IntCounterVec,
    pub quorum_waiter_time_millis_total: IntCounter,
    pub batch_persisted_total: IntCounterVec,
    // pub commands_received: IntCounterVec,
    // pub emergency_breaks: IntCounter,
    // pub direction: IntCounterVec,
    // pub speed: IntCounterVec,
}

impl WorkerMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            batch_size_bytes_total: register_int_counter_with_registry!(
                "batch_size_bytes_total",
                "Total batch size [B]",
                registry,
            )
            .unwrap(),
            batch_sealed_total: register_int_counter_vec_with_registry!(
                "batch_sealed_total",
                "Total number of sealed batches",
                &["reason"], // Full or timeout.
                registry,
            )
            .unwrap(),
            quorum_waiter_time_millis_total: register_int_counter_with_registry!(
                "quorum_waiter_time_millis_total",
                "Total time to disseminate batches [ms]",
                registry,
            )
            .unwrap(),
            batch_persisted_total: register_int_counter_vec_with_registry!(
                "batch_persisted_total",
                "Total number of batch persisted",
                &["origin"], // Owned or from other workers.
                registry
            )
            .unwrap(),
            // commands_received: register_int_counter_vec_with_registry!(
            //     "commands_received",
            //     "Number of commands received",
            //     &["status"],
            //     registry
            // )
            // .unwrap(),
            // emergency_breaks: register_int_counter_with_registry!(
            //     "emergency_breaks",
            //     "Emergency breaks",
            //     registry,
            // )
            // .unwrap(),
            // direction: register_int_counter_vec_with_registry!(
            //     "direction",
            //     "Direction",
            //     &["direction"],
            //     registry
            // )
            // .unwrap(),
            // speed: register_int_counter_vec_with_registry!("speed", "Speed", &["speed"], registry)
            //     .unwrap(),
        }
    }
}
