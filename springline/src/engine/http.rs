use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode, Uri};
use axum::routing::get;
use axum::{AddExtensionLayer, BoxError, Router};
use settings_loader::common::http::HttpServerSettings;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::engine::service::{EngineApiError, EngineCmd, MetricsSpan, Service};

struct State<'r> {
    engine: Service<'r>,
}

#[tracing::instrument(level = "info", skip())]
async fn run_http_server(engine: Service<'static>, settings: &HttpServerSettings) -> Result<(), EngineApiError> {
    let shared_state = Arc::new(State { engine });

    let middleware_stack = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_engine_error))
        .timeout(Duration::from_secs(10))
        // .layer(tower::limit::RateLimitLayer::new(10, Duration::from_millis(100))) // arbitrary limit
        .layer(TraceLayer::new_for_http())
        .layer(AddExtensionLayer::new(shared_state))
        .into_inner();

    let app = Router::new().route("/metrics", get(get_metrics)).layer(middleware_stack);

    // debug_router!(app);

    let address = format!("{}:{}", settings.host, settings.port);
    let listener = TcpListener::bind(&address).await?.into_std()?;
    // let port = listener.local_addr()?.port();

    tracing::info!("autoscale engine API listening on {}", address);
    axum::Server::from_tcp(listener)?.serve(app.into_make_service()).await?;

    Ok(())
}

// #[debug_handler]
#[tracing::instrument(level = "info", skip(engine))]
async fn get_metrics<'r>(
    span: Option<Path<MetricsSpan>>, Extension(engine): Extension<Arc<State<'r>>>,
) -> Result<String, EngineApiError> {
    let span = match span {
        Some(Path(s)) => s,
        None => MetricsSpan::default(),
    };

    let (cmd, rx) = EngineCmd::gather_metrics(span);
    engine.engine.tx_api().send(cmd)?;
    rx.await?.map(|mr| mr.0)
}

#[tracing::instrument(level = "info", skip())]
async fn handle_engine_error(method: Method, uri: Uri, error: BoxError) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("`{} {}` failed with {}", method, uri, error),
    )
}