use crate::messages::{Certificate, Header};
use crate::primary::Round;
use config::{Committee, Parameters};
use crypto::PublicKey;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RateControlConfig {
    pub enabled: bool,
    pub lag_tolerance_rounds: Round,
    pub severe_lag_threshold_rounds: Round,
    pub priority_window_rounds: Round,
    pub high_load_threshold: usize,
    pub min_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub priority_wait_ms: u64,
}

impl RateControlConfig {
    pub fn from_parameters(parameters: &Parameters) -> Self {
        Self {
            enabled: parameters.adaptive_rate_enabled,
            lag_tolerance_rounds: parameters.adaptive_rate_lag_tolerance,
            severe_lag_threshold_rounds: parameters.adaptive_rate_severe_lag,
            priority_window_rounds: parameters.adaptive_rate_priority_window,
            high_load_threshold: parameters.adaptive_rate_high_load_threshold,
            min_backoff_ms: parameters.adaptive_rate_min_delay_ms,
            max_backoff_ms: parameters.adaptive_rate_max_delay_ms,
            priority_wait_ms: parameters.adaptive_rate_priority_wait_ms,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RateDecision {
    Allow,
    Delay { duration: Duration, reason: String },
}

#[derive(Debug)]
struct NodeRateState {
    last_header_round: Round,
    last_certificate_round: Round,
    last_payload_items: usize,
    last_high_load_round: Round,
    last_high_load_payload: usize,
    last_activity: Instant,
    /// Track previous lag to detect if node is catching up
    previous_lag: Round,
    /// Track when we last saw this node catching up
    last_catchup_detected: Option<Instant>,
}

impl NodeRateState {
    fn new() -> Self {
        Self {
            last_header_round: 0,
            last_certificate_round: 0,
            last_payload_items: 0,
            last_high_load_round: 0,
            last_high_load_payload: 0,
            last_activity: Instant::now(),
            previous_lag: 0,
            last_catchup_detected: None,
        }
    }

    fn mark_header(&mut self, round: Round, payload_items: usize, high_load_threshold: usize) {
        self.last_header_round = round;
        self.last_payload_items = payload_items;
        self.last_activity = Instant::now();
        if payload_items >= high_load_threshold {
            self.last_high_load_round = round;
            self.last_high_load_payload = payload_items;
        }
    }

    fn mark_certificate(&mut self, round: Round) {
        self.last_certificate_round = round;
        self.last_activity = Instant::now();
    }

    /// Check if node is catching up (lag is decreasing)
    fn is_catching_up(&mut self, current_lag: Round) -> bool {
        if self.previous_lag == 0 {
            self.previous_lag = current_lag;
            return false;
        }

        // Node is catching up if lag decreased by at least 1 round
        let is_catching = current_lag < self.previous_lag;
        if is_catching {
            self.last_catchup_detected = Some(Instant::now());
        }
        self.previous_lag = current_lag;
        is_catching
    }

    /// Check if node was recently catching up (within last 5 seconds)
    fn was_recently_catching_up(&self) -> bool {
        if let Some(last_catchup) = self.last_catchup_detected {
            last_catchup.elapsed() < Duration::from_secs(5)
        } else {
            false
        }
    }

    fn is_high_load_recent(&self, highest_round: Round, window: Round) -> bool {
        if window == 0 {
            return false;
        }
        highest_round.saturating_sub(self.last_high_load_round) <= window
            && self.last_high_load_payload > 0
    }
}

#[derive(Debug)]
struct AdaptiveRateInner {
    nodes: HashMap<PublicKey, NodeRateState>,
    highest_round_seen: Round,
    local_queue_len: usize,
    local_pending_payload: usize,
}

pub type AdaptiveRateControllerHandle = Arc<AdaptiveRateController>;

pub struct AdaptiveRateController {
    config: RateControlConfig,
    inner: RwLock<AdaptiveRateInner>,
}

impl AdaptiveRateController {
    pub fn new(config: RateControlConfig, committee: &Committee) -> AdaptiveRateControllerHandle {
        let mut nodes = HashMap::new();
        for authority in committee.authorities.keys() {
            nodes.insert(*authority, NodeRateState::new());
        }

        Arc::new(Self {
            config,
            inner: RwLock::new(AdaptiveRateInner {
                nodes,
                highest_round_seen: 0,
                local_queue_len: 0,
                local_pending_payload: 0,
            }),
        })
    }

    pub async fn record_header(&self, header: &Header) {
        if !self.config.enabled {
            return;
        }
        let mut inner = self.inner.write().await;
        inner
            .nodes
            .entry(header.author)
            .or_insert_with(NodeRateState::new)
            .mark_header(
                header.round,
                header.payload.len(),
                self.config.high_load_threshold,
            );
        inner.highest_round_seen = inner.highest_round_seen.max(header.round);
    }

    pub async fn record_certificate(&self, certificate: &Certificate) {
        if !self.config.enabled {
            return;
        }
        let mut inner = self.inner.write().await;
        inner
            .nodes
            .entry(certificate.origin())
            .or_insert_with(NodeRateState::new)
            .mark_certificate(certificate.round());
        inner.highest_round_seen = inner.highest_round_seen.max(certificate.round());
    }

    pub async fn record_local_queue(&self, queue_len: usize, pending_payload: usize) {
        if !self.config.enabled {
            return;
        }
        let mut inner = self.inner.write().await;
        inner.local_queue_len = queue_len;
        inner.local_pending_payload = pending_payload;
    }

    pub async fn evaluate_for_proposer(
        &self,
        our_round: Round,
        local_queue_len: usize,
        has_urgent_batches: bool,
    ) -> RateDecision {
        if !self.config.enabled {
            return RateDecision::Allow;
        }

        let inner = self.inner.read().await;
        if inner.nodes.is_empty() {
            return RateDecision::Allow;
        }

        // CRITICAL: Bypass rate control trong giai đoạn khởi động (round <= 2)
        // Điều này đảm bảo hệ thống có thể tạo header đầu tiên và bắt đầu hoạt động
        if our_round <= 2 {
            log::debug!(
                "[RATE CTRL] Bypass delay vì đang ở giai đoạn khởi động (round={})",
                our_round
            );
            return RateDecision::Allow;
        }

        // CRITICAL: Bypass rate control nếu có batch cần rescue hoặc queue quá lớn
        // Điều này đảm bảo batch không bị stuck vĩnh viễn do rate control
        const URGENT_QUEUE_THRESHOLD: usize = 50; // Nếu queue > 50 batches, bypass delay
        if has_urgent_batches || local_queue_len > URGENT_QUEUE_THRESHOLD {
            if has_urgent_batches {
                log::info!(
                    "[RATE CTRL] Bypass delay vì có batch cần rescue (queue_len={})",
                    local_queue_len
                );
            } else {
                log::info!(
                    "[RATE CTRL] Bypass delay vì queue quá lớn (queue_len={} > {})",
                    local_queue_len,
                    URGENT_QUEUE_THRESHOLD
                );
            }
            return RateDecision::Allow;
        }

        let highest_round = inner.highest_round_seen.max(our_round);
        let total_nodes = inner.nodes.len().max(1);
        let severe_cutoff = self.config.severe_lag_threshold_rounds;

        // Need mutable access to update catch-up state
        let mut inner_mut = self.inner.write().await;
        let mut priority_candidate: Option<(PublicKey, Round, bool)> = None; // (author, lag, is_catching_up)
        let mut throttled_nodes = 0usize;
        let mut catching_up_nodes = 0usize;

        for (author, state) in inner_mut.nodes.iter_mut() {
            let lag = highest_round.saturating_sub(state.last_certificate_round);
            if lag == 0 {
                continue;
            }

            // Check if node is catching up
            let is_catching_up = state.is_catching_up(lag);
            let was_recently_catching = state.was_recently_catching_up();

            // Prioritize nodes with high load that are catching up
            if state.is_high_load_recent(highest_round, self.config.priority_window_rounds)
                && lag <= self.config.priority_window_rounds
            {
                priority_candidate = Some((*author, lag, is_catching_up || was_recently_catching));
                break;
            }

            if lag >= self.config.lag_tolerance_rounds && lag <= severe_cutoff {
                throttled_nodes += 1;
                if is_catching_up || was_recently_catching {
                    catching_up_nodes += 1;
                }
            }
        }
        drop(inner_mut); // Release write lock

        // If we have catching up nodes, be more patient
        let patience_multiplier = if catching_up_nodes > 0 {
            // Increase delay by 1.5x to 2x when nodes are catching up
            let multiplier = 1.5 + (catching_up_nodes as f32 / throttled_nodes.max(1) as f32) * 0.5;
            log::info!(
                "[RATE CTRL] {} / {} node đang catch-up, tăng độ kiên nhẫn (multiplier={:.2})",
                catching_up_nodes,
                throttled_nodes,
                multiplier
            );
            multiplier
        } else {
            1.0
        };

        if let Some((author, lag, is_catching_up)) = priority_candidate {
            // Tăng delay nếu node đang catch-up để cho họ thời gian
            let base_duration_ms = self.config.priority_wait_ms.max(1);
            let duration_ms = if is_catching_up {
                (base_duration_ms as f32 * 1.5).ceil() as u64
            } else {
                base_duration_ms
            };
            let duration = Duration::from_millis(duration_ms);
            log::info!(
                "[RATE CTRL] Delay proposer {:?}ms để chờ node {:?} có backlog lớn (lag {} round, catching_up={})",
                duration.as_millis(),
                author,
                lag,
                is_catching_up
            );
            return RateDecision::Delay {
                duration,
                reason: format!(
                    "Ưu tiên node {:?} với backlog lớn (lag {} round, catching_up={})",
                    author, lag, is_catching_up
                ),
            };
        }

        if throttled_nodes == 0 {
            return RateDecision::Allow;
        }
        let severity_ratio = throttled_nodes as f32 / total_nodes as f32;
        let backoff_span = self
            .config
            .max_backoff_ms
            .saturating_sub(self.config.min_backoff_ms);
        let dynamic_ms =
            self.config.min_backoff_ms + (backoff_span as f32 * severity_ratio).ceil() as u64;

        // Apply patience multiplier if nodes are catching up
        let final_ms = (dynamic_ms as f32 * patience_multiplier).ceil() as u64;
        let duration = Duration::from_millis(final_ms.max(1));

        log::info!(
            "[RATE CTRL] Throttle proposer {:?}ms vì {} / {} node đang lag trong ngưỡng ({} đang catch-up, multiplier={:.2})",
            duration.as_millis(),
            throttled_nodes,
            total_nodes,
            catching_up_nodes,
            patience_multiplier
        );

        RateDecision::Delay {
            duration,
            reason: format!(
                "{} / {} node đang chậm (lag >= {} round, {} đang catch-up)",
                throttled_nodes, total_nodes, self.config.lag_tolerance_rounds, catching_up_nodes
            ),
        }
    }
}
