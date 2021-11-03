use crate::engine::service::{EngineApiError, EngineCmd, MetricsSpan, Service};
use axum::extract::{Extension, Path};
use axum::routing::get;
use axum::{AddExtensionLayer, BoxError, Router};
use axum_debug::{debug_handler, debug_router};
use settings_loader::common::http::HttpServerSettings;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tower::timeout::TimeoutLayer;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{
    body::{Body, BoxBody},
    error_handling::HandleErrorLayer,
    http::{Request, Response},
};
use std::convert::Infallible;
use tower::util::AndThenLayer;

struct State<'r> {
    engine: Service<'r>,
}

#[tracing::instrument(level = "info", skip())]
async fn run_http_server(engine: Service<'static>, settings: &HttpServerSettings) -> Result<(), EngineApiError> {
    let shared_state = Arc::new(State { engine });

    let middleware_stack = ServiceBuilder::new()
        // .layer(RateLimitLayer::new(10, Duration::from_millis(100))) // arbitrary limit
        .layer(HandleErrorLayer::new(handle_engine_error))
        .layer(TimeoutLayer::new(Duration::from_secs(10))) // arbitrary limit
        .layer(TraceLayer::new_for_http())
        .layer(AddExtensionLayer::new(shared_state));
    // .into_inner();

    let app = Router::new()
        .route("/metrics", get(get_metrics))
        .layer(middleware_stack)
        // .handle_error(handle_engine_error)
    ;

    // debug_router!(app);

    let address = format!("{}:{}", settings.host, settings.port);
    let listener = TcpListener::bind(&address).await?.into_std()?;
    // let port = listener.local_addr()?.port();

    tracing::info!("autoscale engine API listening on {}", address);
    axum::Server::from_tcp(listener)?.serve(app.into_make_service()).await?;

    Ok(())
}

#[debug_handler]
#[tracing::instrument(level = "info", skip())]
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
fn handle_engine_error(error: BoxError) -> impl IntoResponse {
    StatusCode::INTERNAL_SERVER_ERROR
}
