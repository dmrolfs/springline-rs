use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode, Uri};
use axum::routing;
use axum::{BoxError, Router};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::engine::service::{EngineApiError, EngineCmd, EngineServiceApi, MetricsSpan};
use crate::settings::Settings;

#[allow(dead_code)]
struct State {
    tx_api: EngineServiceApi,
}

pub type TxExporterGracefulShutdown = oneshot::Sender<()>;
pub type ExporterJoinHandle = JoinHandle<Result<(), EngineApiError>>;

#[tracing::instrument(level = "trace", skip(tx_api))]
pub fn run_metrics_exporter<'s>(
    tx_api: EngineServiceApi, settings: &Settings,
) -> Result<(ExporterJoinHandle, TxExporterGracefulShutdown), EngineApiError> {
    let shared_state = Arc::new(State { tx_api });

    let middleware_stack = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_engine_error))
        .timeout(Duration::from_secs(10))
        // .layer(tower::limit::RateLimitLayer::new(10, Duration::from_millis(100))) // arbitrary limit
        .layer(TraceLayer::new_for_http())
        .layer(Extension(shared_state))
        .into_inner();

    let app = Router::new()
        .route("/metrics", routing::get(get_metrics))
        .layer(middleware_stack);

    let host = settings.http.host.clone();
    let port = settings.prometheus.port;

    let (tx_shutdown, rx_shutdown) = oneshot::channel::<()>();
    let handle: JoinHandle<Result<(), EngineApiError>> = tokio::spawn(async move {
        let address = format!("{host}:{port}");
        let listener = tokio::net::TcpListener::bind(&address).await?;
        tracing::info!(
            "{:?} metrics exporter listening on {address}: {listener:?}",
            std::env::current_exe(),
        );

        let std_listener = listener.into_std()?;
        let builder = axum::Server::from_tcp(std_listener)?;
        let server = builder.serve(app.into_make_service());
        let graceful = server.with_graceful_shutdown(async {
            rx_shutdown.await.ok();
        });
        graceful.await?;
        tracing::info!("{:?} metrics exporter shutting down", std::env::current_exe());
        Ok(())
    });

    Ok((handle, tx_shutdown))
}

#[tracing::instrument(level = "info", skip(tx))]
pub fn shutdown_exporter(tx: oneshot::Sender<()>) -> Result<(), EngineApiError> {
    if tx.send(()) == Err(()) {
        tracing::error!("failed to send shutdown signal to metrics exporter");
        return Err(EngineApiError::GracefulShutdown);
    }

    Ok(())
}

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

#[tracing::instrument(level = "trace", skip())]
async fn handle_engine_error(method: Method, uri: Uri, error: BoxError) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("`{method} {uri}` failed with {error}"),
    )
}
