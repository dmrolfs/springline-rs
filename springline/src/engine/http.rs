use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing;
use axum::{BoxError, Json, Router};
use proctor::phases::sense::clearinghouse::ClearinghouseSnapshot;
use serde_json::json;
use settings_loader::common::http::HttpServerSettings;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::engine::service::{EngineApiError, EngineCmd, EngineServiceApi, HealthReport};
use crate::phases::plan::PerformanceHistory;
use crate::settings::Settings;

#[allow(dead_code)]
struct State {
    tx_api: EngineServiceApi,
}

pub type TxHttpGracefulShutdown = oneshot::Sender<()>;
pub type HttpJoinHandle = JoinHandle<Result<(), EngineApiError>>;

#[tracing::instrument(level = "trace", skip(tx_api))]
pub fn run_http_server<'s>(
    tx_api: EngineServiceApi, settings: &HttpServerSettings,
) -> Result<(HttpJoinHandle, TxHttpGracefulShutdown), EngineApiError> {
    let shared_state = Arc::new(State { tx_api });

    let middleware_stack = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_engine_error))
        .timeout(Duration::from_secs(10))
        // .layer(tower::limit::RateLimitLayer::new(10, Duration::from_millis(100))) // arbitrary limit
        .layer(TraceLayer::new_for_http())
        .layer(Extension(shared_state))
        .into_inner();

    let app = Router::new()
        .route("/health", routing::get(health))
        .route("/health/ready", routing::get(readiness))
        .route("/health/live", routing::get(liveness))
        .route("/clearinghouse", routing::get(get_clearinghouse_snapshot))
        .route("/settings", routing::get(get_settings))
        .route("/performance_history", routing::get(get_performance_history))
        .route("/engine/restart", routing::post(restart))
        .route("/engine/error", routing::post(induce_failure))
        .layer(middleware_stack);

    let host = settings.host.clone();
    let port = settings.port;

    let (tx_shutdown, rx_shutdown) = oneshot::channel::<()>();
    let handle: JoinHandle<Result<(), EngineApiError>> = tokio::spawn(async move {
        let address = format!("{host}:{port}");
        let listener = tokio::net::TcpListener::bind(&address).await?;
        tracing::info!("{:?} autoscale engine API listening on {address}: {listener:?}", std::env::current_exe());

        let std_listener = listener.into_std()?;
        let builder = axum::Server::from_tcp(std_listener)?;
        let server = builder.serve(app.into_make_service());
        let graceful = server.with_graceful_shutdown(async { rx_shutdown.await.ok(); });
        graceful.await?;
        tracing::info!("{:?} autoscale engine API shutting down", std::env::current_exe());
        Ok(())
    });

    Ok((handle, tx_shutdown))
}

#[tracing::instrument(level = "info", skip(tx))]
pub fn shutdown_http_server(tx: oneshot::Sender<()>) -> Result<(), EngineApiError> {
    if tx.send(()) == Err(()) {
        tracing::error!("failed to send shutdown signal to Autoscale engine API");
        return Err(EngineApiError::GracefulShutdown);
    }

    Ok(())
}

const STATUS_UP: &str = "up";
const STATUS_NOT_READY: &str = "not_ready";
const STATUS_ERROR: &str = "error";
const STATUS_DOWN: &str = "down";

#[tracing::instrument(level = "trace", skip(engine))]
async fn health(Extension(engine): Extension<Arc<State>>) -> impl IntoResponse {
    match EngineCmd::check_health(&engine.tx_api).await {
        Ok(HealthReport::Up) => (StatusCode::OK, Json(json!({ "status": STATUS_UP }))),
        Ok(HealthReport::NotReady(phases_waiting)) => {
            let phases: Vec<String> = phases_waiting.iter().map(|p| p.to_string()).collect();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": STATUS_NOT_READY, "phases": phases })),
            )
        },
        Ok(HealthReport::Down) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "status": STATUS_DOWN }))),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "status": STATUS_ERROR, "error": err.to_string() })),
        ),
    }
}

#[tracing::instrument(level = "trace", skip(engine))]
async fn readiness(Extension(engine): Extension<Arc<State>>) -> impl IntoResponse {
    match EngineCmd::check_health(&engine.tx_api).await {
        Ok(HealthReport::Down) => StatusCode::SERVICE_UNAVAILABLE,
        Err(err) => {
            tracing::error!(error=?err, "readiness check failed");
            StatusCode::INTERNAL_SERVER_ERROR
        },
        _ => StatusCode::OK,
    }
}

#[tracing::instrument(level = "trace", skip(engine))]
async fn liveness(Extension(engine): Extension<Arc<State>>) -> impl IntoResponse {
    match EngineCmd::check_health(&engine.tx_api).await {
        Ok(HealthReport::Down) => StatusCode::SERVICE_UNAVAILABLE,
        Err(err) => {
            tracing::error!(error=?err, "liveness check failed");
            StatusCode::INTERNAL_SERVER_ERROR
        },
        _ => StatusCode::OK,
    }
}

#[tracing::instrument(level = "trace", skip(engine))]
async fn get_clearinghouse_snapshot(
    subscription: Option<Path<String>>, Extension(engine): Extension<Arc<State>>,
) -> Result<Json<ClearinghouseSnapshot>, EngineApiError> {
    EngineCmd::report_on_clearinghouse(&engine.tx_api, subscription.map(|s| s.0))
        .await
        .map(Json)
}

#[tracing::instrument(level = "trace", skip(engine))]
async fn get_settings(Extension(engine): Extension<Arc<State>>) -> Result<Json<Settings>, EngineApiError> {
    EngineCmd::get_settings(&engine.tx_api)
        .await
        .map(|s| s.as_ref().clone())
        .map(Json)
}

#[tracing::instrument(level = "trace", skip(engine))]
async fn get_performance_history(
    Extension(engine): Extension<Arc<State>>,
) -> Result<Json<PerformanceHistory>, EngineApiError> {
    EngineCmd::get_performance_history(&engine.tx_api)
        .await
        .map(|ph| ph.as_ref().clone())
        .map(Json)
}

#[tracing::instrument(level = "trace", skip())]
async fn handle_engine_error(method: Method, uri: Uri, error: BoxError) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("`{method} {uri}` failed with {error}"),
    )
}

#[tracing::instrument(level = "info", skip(engine))]
async fn restart(Extension(engine): Extension<Arc<State>>) -> Result<(StatusCode, String), EngineApiError> {
    EngineCmd::restart(&engine.tx_api)
        .await
        .map(|_| (StatusCode::ACCEPTED, "restarting autoscale engine".to_string()))
}

#[tracing::instrument(level = "error", skip(engine))]
async fn induce_failure(Extension(engine): Extension<Arc<State>>) -> Result<(StatusCode, String), EngineApiError> {
    EngineCmd::induce_failure(&engine.tx_api).await.map(|_| {
        (
            StatusCode::ACCEPTED,
            "inducing failure -- restarting if restarts left".to_string(),
        )
    })
}
