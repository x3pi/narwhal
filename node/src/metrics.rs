use ::prometheus::{register_int_counter_with_registry, IntCounter, Registry};

#[derive(Clone)]
pub struct ConsensusMetrics {
    pub committed_certificates_total: IntCounter,
    pub committed_sample_transactions_total: IntCounter,
    pub latency_total: IntCounter,
    pub latency_square_total: IntCounter,
    pub committed_bytes_total: IntCounter,
    pub first_sent_transaction: IntCounter,
    pub last_committed_transaction: IntCounter,
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
            committed_sample_transactions_total: register_int_counter_with_registry!(
                "committed_sample_transactions_total",
                "Total committed sample transactions",
                registry,
            )
            .unwrap(),
            latency_total: register_int_counter_with_registry!(
                "latency_total",
                "Total latency of sample transactions [ms]",
                registry,
            )
            .unwrap(),
            latency_square_total: register_int_counter_with_registry!(
                "latency_square_total",
                "Total square latency of sample transactions [ms]",
                registry,
            )
            .unwrap(),
            committed_bytes_total: register_int_counter_with_registry!(
                "committed_bytes_total",
                "Total number of committed bytes",
                registry
            )
            .unwrap(),
            first_sent_transaction: register_int_counter_with_registry!(
                "first_sent_transaction",
                "Timestamp of the first transaction sent",
                registry
            )
            .unwrap(),
            last_committed_transaction: register_int_counter_with_registry!(
                "last_committed_transaction",
                "Timestamp of the last committed transaction",
                registry
            )
            .unwrap(),
        }
    }
}
