use axum::{
    http::StatusCode,
    routing::{get, post},
    Extension, Json, Router, Server,
};
use config::UpdatableParameters;
use prometheus::{Registry, TextEncoder};
use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
};
use tokio::task::JoinHandle;

const METRICS_ROUTE: &str = "/metrics";
const PARAMETERS_ROUTE: &str = "/parameters";

pub fn start_prometheus_server(
    address: SocketAddr,
    registry: &Registry,
    parameters: Arc<RwLock<UpdatableParameters>>,
) -> JoinHandle<Result<(), hyper::Error>> {
    let app = Router::new()
        .route(METRICS_ROUTE, get(metrics))
        .layer(Extension(registry.clone()))
        .route(PARAMETERS_ROUTE, post(update_parameters))
        .layer(Extension(parameters));

    log::info!("Prometheus server booted on {address}");
    tokio::spawn(async move { Server::bind(&address).serve(app.into_make_service()).await })
}

async fn metrics(registry: Extension<Registry>) -> (StatusCode, String) {
    let metrics_families = registry.gather();
    match TextEncoder.encode_to_string(&metrics_families) {
        Ok(metrics) => (StatusCode::OK, metrics),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unable to encode metrics: {error}"),
        ),
    }
}

async fn update_parameters(
    parameters: Extension<Arc<RwLock<UpdatableParameters>>>,
    Json(new_parameters): Json<UpdatableParameters>,
) -> (StatusCode, String) {
    // Update the parameters.
    let mut guard = parameters.write().unwrap();
    *guard = new_parameters;

    // Log the new parameters.
    guard.log();

    // Reply with a status code.
    (StatusCode::OK, "Ok".to_string())
}
