use std::fmt;

use proctor::graph::stage::tick;
use proctor::phases::sense::{ClearinghouseApi, ClearinghouseCmd, ClearinghouseSnapshot};
use prometheus::{Encoder, Registry, TextEncoder};
pub use protocol::{EngineApiError, EngineCmd, EngineServiceApi, MetricsReport, MetricsSpan};
use regex::RegexSet;
use tokio::sync::mpsc;

mod protocol {
    use std::collections::HashMap;
    use std::fmt::Display;

    use axum::body::{self, BoxBody};
    use axum::http::{Response, StatusCode};
    use axum::response::IntoResponse;
    use either::Left;
    use enum_display_derive::Display;
    use itertools::Either;
    use once_cell::sync::Lazy;
    use proctor::error::MetricLabel;
    use proctor::graph::stage::tick::TickMsg;
    use proctor::phases::sense::{ClearinghouseCmd, ClearinghouseSnapshot};
    use proctor::SharedString;
    use regex::RegexSet;
    use serde::Deserialize;
    use thiserror::Error;
    use tokio::sync::{mpsc, oneshot};

    pub type EngineServiceApi = mpsc::UnboundedSender<EngineCmd>;

    #[allow(dead_code)]
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
        StopFlinkSensor {
            tx: oneshot::Sender<Result<(), EngineApiError>>,
        },
    }

    #[allow(dead_code)]
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

        #[inline]
        pub fn stop_flink_sensor() -> (Self, oneshot::Receiver<Result<(), EngineApiError>>) {
            let (tx, rx) = oneshot::channel();
            (Self::StopFlinkSensor { tx }, rx)
        }
    }

    #[derive(Debug, Default)]
    pub struct MetricsReport(pub String);

    /// Engine API failed to satisfy request.
    #[derive(Debug, Error)]
    pub enum EngineApiError {
        #[error("Failed to start autoscale engine API: {0}")]
        Bootstrap(#[from] hyper::Error),

        #[error("Could not connect to autoscale engine API: {0}")]
        EngineSend(#[from] mpsc::error::SendError<EngineCmd>),

        #[error("Failure in prometheus: {0}")]
        Prometheus(#[from] prometheus::Error),

        #[error("Could not send command to telemetry clearinghouse: {0}")]
        ClearinghouseSend(#[from] mpsc::error::SendError<ClearinghouseCmd>),

        #[error("Could not send command to flink sensor: {0}")]
        FlinkSensorSend(#[from] mpsc::error::SendError<TickMsg>),

        #[error("Could not receive response from underlying API handler: {0}")]
        ApiHandlerRecv(#[from] oneshot::error::RecvError),

        #[error("Could not open or bind to a TCP address for the autoscale engine's API: {0}")]
        IO(#[from] std::io::Error),

        #[error("{0}")]
        Handler(#[from] anyhow::Error),
    }

    impl IntoResponse for EngineApiError {
        fn into_response(self) -> Response<BoxBody> {
            tracing::error!(error=?self, "failure in autoscale engine API");
            let body = body::boxed(body::Full::from(format!("Engine API Failure: {}", self)));

            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(body)
                .expect("Autoscale engine API failed to build error response.")
        }
    }

    impl MetricLabel for EngineApiError {
        fn slug(&self) -> SharedString {
            "engine_api".into()
        }

        fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
            Left("engine".into())
        }
    }

    #[derive(Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
    #[serde(rename_all(deserialize = "lowercase"))]
    pub enum MetricsSpan {
        All,
        Sense,
        Eligibility,
        Decision,
        Plan,
        Governance,
        Act,
        Clearinghouse,
        Graph,
        Policy,
    }

    impl Default for MetricsSpan {
        fn default() -> Self {
            Self::All
        }
    }

    static METRIC_REGEX: Lazy<HashMap<MetricsSpan, RegexSet>> = Lazy::new(|| {
        maplit::hashmap! {
            MetricsSpan::All => RegexSet::new(&[r".*",]).unwrap(),
            MetricsSpan::Sense => RegexSet::new(&[r"(?i)catalog", r"(?i)clearinghouse",]).unwrap(),
            MetricsSpan::Eligibility => RegexSet::new(&[r"(?i)eligibility",]).unwrap(),
            MetricsSpan::Decision => RegexSet::new(&[r"(?i)decision",]).unwrap(),
            MetricsSpan::Plan => RegexSet::new(&[r"(?i)plan",]).unwrap(),
            MetricsSpan::Governance => RegexSet::new(&[r"(?i)governance",]).unwrap(),
            MetricsSpan::Graph => RegexSet::new(&[r"(?i)governance", r"(?i)stage",]).unwrap(),
            MetricsSpan::Policy => RegexSet::new(&[r"(?i)policy",]).unwrap(),
        }
    });

    impl MetricsSpan {
        pub fn regex(&self) -> &RegexSet {
            METRIC_REGEX.get(self).unwrap()
        }
    }
}

pub struct Service<'r> {
    tx_api: EngineServiceApi,
    rx_api: mpsc::UnboundedReceiver<EngineCmd>,
    tx_stop_flink_sensor: tick::TickApi,
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
    pub fn new(
        tx_stop_flink_sensor: tick::TickApi, tx_clearinghouse_api: ClearinghouseApi,
        metrics_registry: Option<&'r Registry>,
    ) -> Self {
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        Self {
            tx_api,
            rx_api,
            tx_stop_flink_sensor,
            tx_clearinghouse_api,
            metrics_registry,
        }
    }

    #[allow(dead_code)]
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

                        EngineCmd::StopFlinkSensor { tx } => {
                            let stop_response = self.stop_flink_sensor().await;

                            if stop_response.is_ok() {
                                tracing::info!("Engine stopped Flink sensor by command.");
                            } else {
                                tracing::error!(error=?stop_response, "Engine failed to stop Flink sensor.");
                            }

                            let _ = tx.send(stop_response);
                        }
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
            let mut buffer = vec![];
            let encoder = TextEncoder::default();
            let metrics_families = registry.gather();
            encoder.encode(&metrics_families, &mut buffer)?;

            let report = String::from_utf8(buffer).map_err(|err| EngineApiError::Handler(err.into()))?;
            let report = if span != &MetricsSpan::All {
                filter_report(report, span)
            } else {
                report
            };

            Ok(MetricsReport(report))
        } else {
            tracing::warn!("no metrics_registry - creating default");
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
            .unwrap_or_else(ClearinghouseCmd::get_clearinghouse_snapshot);
        let _ = self.tx_clearinghouse_api.send(cmd)?;
        let snapshot = rx.await?;
        Ok(snapshot)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn stop_flink_sensor(&self) -> Result<(), EngineApiError> {
        let (cmd, rx) = tick::TickMsg::stop();
        self.tx_stop_flink_sensor.send(cmd)?;
        rx.await?.map_err(|err| EngineApiError::Handler(err.into()))
    }
}
