use proctor::phases::collection::{ClearinghouseApi, ClearinghouseCmd, ClearinghouseSnapshot};
use prometheus::{Registry, TextEncoder};
use regex::RegexSet;
use std::fmt;
use tokio::sync::mpsc;

pub use protocol::{EngineApiError, EngineCmd, EngineServiceApi, MetricsReport, MetricsSpan};

mod protocol {
    use axum::body::Body;
    use axum::http::{Response, StatusCode};
    use axum::response::IntoResponse;
    use enum_display_derive::Display;
    use lazy_static::lazy_static;
    use proctor::phases::collection::{ClearinghouseCmd, ClearinghouseSnapshot};
    use regex::RegexSet;
    use serde::Deserialize;
    use std::collections::HashMap;
    use std::fmt::Display;
    use thiserror::Error;
    use tokio::sync::{mpsc, oneshot};

    pub type EngineServiceApi = mpsc::UnboundedSender<EngineCmd>;

    #[derive(Debug)]
    pub enum EngineCmd {
        GatherMetrics {
            domain: MetricsSpan,
            tx: oneshot::Sender<Result<MetricsReport, EngineApiError>>,
        },
        ReportOnClearinghouse {
            subscription: Option<String>,
            tx: oneshot::Sender<Result<ClearinghouseSnapshot, EngineApiError>>,
        },
    }

    impl EngineCmd {
        #[inline]
        pub fn gather_metrics(domain: MetricsSpan) -> (Self, oneshot::Receiver<Result<MetricsReport, EngineApiError>>) {
            let (tx, rx) = oneshot::channel();
            (Self::GatherMetrics { domain, tx }, rx)
        }

        #[inline]
        pub fn report_on_clearinghouse(
            subscription: Option<impl Into<String>>,
        ) -> (Self, oneshot::Receiver<Result<ClearinghouseSnapshot, EngineApiError>>) {
            let (tx, rx) = oneshot::channel();
            let subscription = subscription.map(|s| s.into());
            (Self::ReportOnClearinghouse { subscription, tx }, rx)
        }
    }

    #[derive(Debug, Default)]
    pub struct MetricsReport(pub String);

    /// Engine API failed to satisfy request.
    #[derive(Debug, Error)]
    pub enum EngineApiError {
        #[error("Failed to start autoscale engine API: {0}")]
        BootstrapError(#[from] hyper::Error),

        #[error("Could not connect to autoscale engine API: {0}")]
        EngineSendError(#[from] mpsc::error::SendError<EngineCmd>),

        #[error("Failure in prometheus: {0}")]
        PrometheusError(#[from] prometheus::Error),

        #[error("Could not receive response from telemetry clearinghouse: {0}")]
        ClearinghouseSendError(#[from] mpsc::error::SendError<ClearinghouseCmd>),

        #[error("Could not send command to telemetry clearinghouse: {0}")]
        ClearinghouseRecvError(#[from] oneshot::error::RecvError),

        #[error("Could not open or bind to a TCP address for the autoscale engine's API: {0}")]
        IOError(#[from] std::io::Error),
    }

    impl IntoResponse for EngineApiError {
        type Body = Body;
        type BodyError = <Self::Body as axum::body::HttpBody>::Error;

        fn into_response(self) -> Response<Self::Body> {
            tracing::error!(error=?self, "failure in autoscale engine API");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(self.to_string()))
                .expect("Autoscale engine API failed to build error response.")
        }
    }

    #[derive(Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
    #[serde(rename_all(deserialize = "lowercase"))]
    pub enum MetricsSpan {
        All,
        Collection,
        Eligibility,
        Decision,
        Plan,
        Governance,
        Execution,
        Clearinghouse,
        Graph,
        Policy,
    }

    impl Default for MetricsSpan {
        fn default() -> Self {
            Self::All
        }
    }

    lazy_static! {
        static ref METRIC_REGEX: HashMap<MetricsSpan, RegexSet> = maplit::hashmap! {
            MetricsSpan::All => RegexSet::new(&[r".*",]).unwrap(),
            MetricsSpan::Collection => RegexSet::new(&[r"(?i)catalog", r"(?i)clearinghouse",]).unwrap(),
            MetricsSpan::Eligibility => RegexSet::new(&[r"(?i)eligibility",]).unwrap(),
            MetricsSpan::Decision => RegexSet::new(&[r"(?i)decision",]).unwrap(),
            MetricsSpan::Plan => RegexSet::new(&[r"(?i)plan",]).unwrap(),
            MetricsSpan::Governance => RegexSet::new(&[r"(?i)governance",]).unwrap(),
            MetricsSpan::Graph => RegexSet::new(&[r"(?i)governance", r"(?i)stage",]).unwrap(),
            MetricsSpan::Policy => RegexSet::new(&[r"(?i)policy",]).unwrap(),
        };
    }

    impl MetricsSpan {
        pub fn regex(&self) -> &RegexSet {
            METRIC_REGEX.get(self).unwrap()
        }
    }
}

pub struct Service<'r> {
    tx_api: EngineServiceApi,
    rx_api: mpsc::UnboundedReceiver<EngineCmd>,
    tx_clearinghouse_api: ClearinghouseApi,
    metrics_registry: Option<&'r Registry>,
}

impl<'r> fmt::Debug for Service<'r> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Service")
            .field("metrics_registry", &self.metrics_registry)
            .finish()
    }
}

impl<'r> Service<'r> {
    pub fn new(tx_clearinghouse_api: ClearinghouseApi, metrics_registry: Option<&'r Registry>) -> Self {
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        Self {
            tx_api,
            rx_api,
            tx_clearinghouse_api,
            metrics_registry,
        }
    }

    pub fn tx_api(&self) -> EngineServiceApi {
        self.tx_api.clone()
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(cmd) = self.rx_api.recv() => {
                    match cmd {
                        EngineCmd::GatherMetrics { domain, tx } => {
                            let report = self.get_metrics_report(&domain);
                            if report.is_ok() {
                                tracing::info!(?domain, "reporting on metrics domain.");
                            } else {
                                tracing::warn!(?domain, ?report, "failed to gather metrics report");
                            }
                            let _ = tx.send(report);
                        },

                        EngineCmd::ReportOnClearinghouse { subscription, tx } => {
                            let snapshot = self.get_clearinghouse_snapshot(&subscription).await;
                            if snapshot.is_ok() {
                                tracing::info!(
                                    ?subscription,
                                    "reporting on clearinghouse subscription{}.",
                                    if subscription.is_none() { "s" } else { "" }
                                );
                            } else {
                                tracing::warn!(?snapshot, "failed to get clearinghouse snapshot.");
                            }
                            let _ = tx.send(snapshot);
                        },
                    }
                },

                else => {
                    tracing::info!("springline engine service stopping...");
                    break;
                }
            }
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    fn get_metrics_report(&self, span: &MetricsSpan) -> Result<MetricsReport, EngineApiError> {
        fn filter_report(original: String, span: &MetricsSpan) -> String {
            let rs: &RegexSet = span.regex();
            let filtered = original.lines().filter(|ln| rs.is_match(ln)).collect();
            tracing::info!(%span, %original, %filtered, "filtered metrics report for span.");
            filtered
        }

        if let Some(registry) = self.metrics_registry {
            let metrics = registry.gather();
            let encoder = TextEncoder::default();
            let report = encoder.encode_to_string(&metrics)?;
            let report = filter_report(report, span);
            Ok(MetricsReport(report))
        } else {
            Ok(MetricsReport::default())
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn get_clearinghouse_snapshot(
        &self, subscription: &Option<String>,
    ) -> Result<ClearinghouseSnapshot, EngineApiError> {
        let (cmd, rx) = subscription
            .as_ref()
            .map(ClearinghouseCmd::get_subscription_snapshot)
            .unwrap_or(ClearinghouseCmd::get_clearinghouse_snapshot());
        let _ = self.tx_clearinghouse_api.send(cmd)?;
        let snapshot = rx.await?;
        Ok(snapshot)
    }
}
