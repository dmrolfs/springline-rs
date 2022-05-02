use std::fmt;

use proctor::graph::stage::tick;
use proctor::phases::sense::{ClearinghouseApi, ClearinghouseCmd, ClearinghouseSnapshot};
use prometheus::{Encoder, Registry, TextEncoder};
pub use protocol::{EngineApiError, EngineCmd, EngineServiceApi, Health, HealthReport, MetricsReport, MetricsSpan};
use regex::RegexSet;
use tokio::sync::mpsc;

use crate::engine::PhaseFlags;

mod protocol {
    use std::collections::HashMap;
    use std::fmt::Display;

    use axum::body::{self, BoxBody};
    use axum::http::{Response, StatusCode};
    use axum::response::IntoResponse;
    use either::{Left, Right};
    use enum_display_derive::Display;
    use itertools::Either;
    use once_cell::sync::Lazy;
    use proctor::error::MetricLabel;
    use proctor::graph::stage::tick;
    use proctor::phases::sense::ClearinghouseSnapshot;
    use regex::RegexSet;
    use serde::Deserialize;
    use thiserror::Error;
    use tokio::sync::{mpsc, oneshot};

    use crate::engine::PhaseFlags;

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
        CheckHealth(oneshot::Sender<Result<HealthReport, EngineApiError>>),
        UpdateHealth(Health, oneshot::Sender<Result<(), EngineApiError>>),
        Restart(bool, oneshot::Sender<Result<(), EngineApiError>>),
    }

    #[allow(dead_code)]
    impl EngineCmd {
        pub async fn gather_metrics(
            api: &EngineServiceApi, domain: MetricsSpan,
        ) -> Result<MetricsReport, EngineApiError> {
            let (tx, rx) = oneshot::channel();
            api.send(Self::GatherMetrics { domain, tx })?;
            rx.await?
        }

        pub async fn report_on_clearinghouse(
            api: &EngineServiceApi, subscription: Option<String>,
        ) -> Result<ClearinghouseSnapshot, EngineApiError> {
            let (tx, rx) = oneshot::channel();
            api.send(Self::ReportOnClearinghouse { subscription, tx })?;
            rx.await?
        }

        pub async fn stop_flink_sensor(api: &EngineServiceApi) -> Result<(), EngineApiError> {
            let (tx, rx) = oneshot::channel();
            api.send(Self::StopFlinkSensor { tx })?;
            rx.await?
        }

        pub async fn check_health(api: &EngineServiceApi) -> Result<HealthReport, EngineApiError> {
            let (tx, rx) = oneshot::channel();
            api.send(Self::CheckHealth(tx))?;
            rx.await?
        }

        pub async fn update_health(api: &EngineServiceApi, health: Health) -> Result<(), EngineApiError> {
            let (tx, rx) = oneshot::channel();
            api.send(Self::UpdateHealth(health, tx))?;
            rx.await?
        }

        pub async fn restart(api: &EngineServiceApi) -> Result<(), EngineApiError> {
            let (tx, rx) = oneshot::channel();
            api.send(Self::Restart(false, tx))?;
            rx.await?
        }

        pub async fn induce_failure(api: &EngineServiceApi) -> Result<(), EngineApiError> {
            let (tx, rx) = oneshot::channel();
            api.send(Self::Restart(true, tx))?;
            rx.await?
        }
    }

    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct MetricsReport(pub String);

    #[derive(Debug, Clone, PartialEq)]
    pub enum HealthReport {
        Up,
        NotReady(PhaseFlags),
        Down,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum Health {
        Ready(PhaseFlags),
        GraphStopped,
    }

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
        Clearinghouse(#[from] proctor::error::SenseError),

        #[error("Could not send command to flink sensor: {0}")]
        FlinkSensorSend(#[from] mpsc::error::SendError<tick::TickCmd>),

        #[error("Could not receive response from underlying API handler: {0}")]
        ApiHandlerRecv(#[from] oneshot::error::RecvError),

        #[error("Could not open or bind to a TCP address for the autoscale engine's API: {0}")]
        IO(#[from] std::io::Error),

        #[error("{0}")]
        Handler(#[from] anyhow::Error),

        #[error("failed to send graceful shutdown command to autoscale engine API.")]
        GracefulShutdown,
    }

    impl IntoResponse for EngineApiError {
        fn into_response(self) -> Response<BoxBody> {
            tracing::trace!(error=?self, "failure in autoscale engine API");
            let body = body::boxed(body::Full::from(format!("Engine API Failure: {}", self)));

            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(body)
                .expect("Autoscale engine API failed to build error response.")
        }
    }

    impl MetricLabel for EngineApiError {
        fn slug(&self) -> String {
            "engine_api".into()
        }

        fn next(&self) -> Either<String, Box<&dyn MetricLabel>> {
            match self {
                Self::Bootstrap(_) => Left("bootstrap".into()),
                Self::EngineSend(_) => Left("command".into()),
                Self::Prometheus(_) => Left("prometheus".into()),
                Self::Clearinghouse(e) => Right(Box::new(e)),
                Self::FlinkSensorSend(_) => Left("sensor".into()),
                Self::ApiHandlerRecv(_) => Left("handler::response".into()),
                Self::IO(_) => Left("io".into()),
                Self::Handler(_) => Left("handler".into()),
                Self::GracefulShutdown => Left("shutdown".into()),
            }
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

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn run(mut self) {
        let mut ready_phases = PhaseFlags::default();
        let mut is_up = true;

        while let Some(cmd) = self.rx_api.recv().await {
            match cmd {
                EngineCmd::GatherMetrics { domain, tx } => {
                    let report = self.get_metrics_report(&domain);
                    if report.is_ok() {
                        tracing::trace!(?domain, "reporting on metrics domain.");
                    } else {
                        tracing::warn!(?domain, ?report, "failed to gather metrics report");
                    }
                    let _ = tx.send(report);
                },

                EngineCmd::ReportOnClearinghouse { subscription, tx } => {
                    let snapshot = self.get_clearinghouse_snapshot(&subscription).await;
                    if snapshot.is_ok() {
                        tracing::trace!(
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
                        tracing::trace!("Engine stopped Flink sensor by command.");
                    } else {
                        tracing::error!(error=?stop_response, "Engine failed to stop Flink sensor.");
                    }

                    let _ = tx.send(stop_response);
                },

                EngineCmd::UpdateHealth(health, tx) => {
                    Self::handle_health_update(health, &mut ready_phases, &mut is_up);
                    let _ = tx.send(Ok(()));
                },

                EngineCmd::CheckHealth(tx) => {
                    if is_up {
                        tracing::trace!(?ready_phases, "checking health...");
                        if ready_phases.is_all() {
                            tracing::debug!(?ready_phases, "Engine is ready to autoscale.");
                            let _ = tx.send(Ok(HealthReport::Up));
                        } else {
                            tracing::info!(?ready_phases, "Engine is not ready to autoscale.");
                            let _ = tx.send(Ok(HealthReport::NotReady(!ready_phases)));
                        }
                    } else {
                        tracing::warn!(?ready_phases, "engine is down.");
                        let _ = tx.send(Ok(HealthReport::Down));
                    }
                },

                EngineCmd::Restart(false, tx) => {
                    tracing::info!("restarting engine...");
                    self.rx_api.close();
                    let _ = tx.send(Ok(()));
                    break;
                },

                EngineCmd::Restart(true, tx) => {
                    tracing::error!("induced failure engine restart...");
                    self.rx_api.close();
                    let _ = tx.send(Ok(()));
                    panic!("induced failure");
                },
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn get_metrics_report(&self, span: &MetricsSpan) -> Result<MetricsReport, EngineApiError> {
        fn filter_report(original: String, span: &MetricsSpan) -> String {
            let rs: &RegexSet = span.regex();
            let filtered = original.lines().filter(|ln| rs.is_match(ln)).collect();
            tracing::debug!(%span, %original, %filtered, "filtered metrics report for span.");
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
            tracing::info!("no metrics_registry - creating default");
            Ok(MetricsReport::default())
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_clearinghouse_snapshot(
        &self, subscription: &Option<String>,
    ) -> Result<ClearinghouseSnapshot, EngineApiError> {
        let snapshot = match subscription.as_ref() {
            Some(s) => ClearinghouseCmd::get_subscription_snapshot(&self.tx_clearinghouse_api, s).await,
            None => ClearinghouseCmd::get_clearinghouse_snapshot(&self.tx_clearinghouse_api).await,
        };

        snapshot.map_err(|err| err.into())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn stop_flink_sensor(&self) -> Result<(), EngineApiError> {
        tick::TickCmd::stop(&self.tx_stop_flink_sensor)
            .await
            .map_err(|err| EngineApiError::Handler(err.into()))
    }

    #[tracing::instrument(level = "trace")]
    fn handle_health_update(health: Health, ready_phases: &mut PhaseFlags, is_up: &mut bool) {
        match health {
            Health::Ready(phases) => *ready_phases = phases,
            Health::GraphStopped => {
                *is_up = false;
            },
        }
    }
}
