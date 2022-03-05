use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode, Uri};
use axum::routing::get;
use axum::{BoxError, Json, Router};
use proctor::phases::sense::ClearinghouseSnapshot;
use settings_loader::common::http::HttpServerSettings;
use tokio::task::JoinHandle;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::engine::service::{EngineApiError, EngineCmd, EngineServiceApi, MetricsSpan};

#[allow(dead_code)]
struct State {
    tx_api: EngineServiceApi,
}

#[tracing::instrument(level = "info", skip(tx_api))]
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

        tracing::warn!(
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

// #[debug_handler]
#[tracing::instrument(level = "info", skip(engine))]
async fn get_metrics<'r>(
    span: Option<Path<MetricsSpan>>, Extension(engine): Extension<Arc<State>>,
) -> Result<String, EngineApiError> {
    let span = match span {
        Some(Path(s)) => s,
        None => MetricsSpan::default(),
    };

    let (cmd, rx) = EngineCmd::gather_metrics(span);
    engine.tx_api.send(cmd)?;
    rx.await?.map(|mr| mr.0)
}

#[tracing::instrument(level = "info", skip(engine))]
async fn get_clearinghouse_snapshot<'r>(
    subscription: Option<Path<String>>, Extension(engine): Extension<Arc<State>>,
) -> Result<Json<ClearinghouseSnapshot>, EngineApiError> {
    let (cmd, rx) = EngineCmd::report_on_clearinghouse(subscription.map(|s| s.0));
    engine.tx_api.send(cmd)?;
    rx.await?.map(Json)
}

#[tracing::instrument(level = "info", skip())]
async fn handle_engine_error(method: Method, uri: Uri, error: BoxError) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("`{} {}` failed with {}", method, uri, error),
    )
}
