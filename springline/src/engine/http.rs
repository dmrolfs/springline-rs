use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{BoxError, Json, Router};
use proctor::phases::sense::ClearinghouseSnapshot;
use serde_json::json;
use settings_loader::common::http::HttpServerSettings;
use tokio::task::JoinHandle;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::engine::service::{EngineApiError, EngineCmd, EngineServiceApi, HealthReport, MetricsSpan};

#[allow(dead_code)]
struct State {
    tx_api: EngineServiceApi,
}

#[tracing::instrument(level = "trace", skip(tx_api))]
pub fn run_http_server<'s>(
    tx_api: EngineServiceApi, settings: &HttpServerSettings,
) -> Result<JoinHandle<Result<(), EngineApiError>>, EngineApiError> {
    let shared_state = Arc::new(State { tx_api });

    let middleware_stack = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_engine_error))
        .timeout(Duration::from_secs(10))
        // .layer(tower::limit::RateLimitLayer::new(10, Duration::from_millis(100))) // arbitrary limit
        .layer(TraceLayer::new_for_http())
        .layer(Extension(shared_state))
        .into_inner();

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(get_metrics))
        .route("/clearinghouse", get(get_clearinghouse_snapshot))
        .layer(middleware_stack);

    // debug_router!(app);

    let host = settings.host.clone();
    let port = settings.port;

    let handle: JoinHandle<Result<(), EngineApiError>> = tokio::spawn(async move {
        let address = format!("{host}:{port}");
        let listener = tokio::net::TcpListener::bind(&address).await?;
        let std_listener = listener.into_std()?;

        tracing::info!(
            "{:?} autoscale engine API listening on {}",
            std::env::current_exe(),
            address
        );
        let builder = axum::Server::from_tcp(std_listener)?;
        let server = builder.serve(app.into_make_service());
        server.await?;
        Ok(())
    });

    Ok(handle)
}

#[tracing::instrument(level = "trace", skip(engine))]
async fn health(Extension(engine): Extension<Arc<State>>) -> impl IntoResponse {
    match EngineCmd::check_health(&engine.tx_api).await {
        Ok(HealthReport::Ok) => (StatusCode::OK, Json(json!({ "status": "ok" }))),
        Ok(HealthReport::NotReady(phases_waiting)) => {
            let phases: Vec<String> = phases_waiting.iter().map(|p| p.to_string()).collect();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "not_ready", "phases": phases })),
            )
        },
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "status": "error", "error": err.to_string() })),
        ),
    }
}

// #[debug_handler]
#[tracing::instrument(level = "trace", skip(engine))]
async fn get_metrics(
    span: Option<Path<MetricsSpan>>, Extension(engine): Extension<Arc<State>>,
) -> Result<String, EngineApiError> {
    let span = match span {
        Some(Path(s)) => s,
        None => MetricsSpan::default(),
    };

    EngineCmd::gather_metrics(&engine.tx_api, span).await.map(|mr| mr.0)
}

#[tracing::instrument(level = "trace", skip(engine))]
async fn get_clearinghouse_snapshot(
    subscription: Option<Path<String>>, Extension(engine): Extension<Arc<State>>,
) -> Result<Json<ClearinghouseSnapshot>, EngineApiError> {
    EngineCmd::report_on_clearinghouse(&engine.tx_api, subscription.map(|s| s.0))
        .await
        .map(Json)
}

#[tracing::instrument(level = "trace", skip())]
async fn handle_engine_error(method: Method, uri: Uri, error: BoxError) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("`{} {}` failed with {}", method, uri, error),
    )
}
