use once_cell::sync::Lazy;
use pretty_snowflake::{AlphabetCodec, IdPrettifier};
use proctor::elements::telemetry;
use std::sync::Mutex;
use crate::flink::{AppDataWindowBuilder, ClusterMetrics, CorrelationGenerator, FlowMetrics, JobHealthMetrics};
use super::*;

pub fn arb_metric_catalog_window<M>(
    start: impl Strategy<Value = Timestamp>,
    delay: impl Strategy<Value = Duration> + 'static,
    interval: impl Strategy<Value = Duration> + 'static,
    window: impl Strategy<Value = Duration> + 'static,
    make_metric_catalog: M
) -> impl Strategy<Value = AppDataWindow<MetricCatalog>>
where
    M: Fn(Timestamp) -> BoxedStrategy<MetricCatalog> + 'static,
{
    let delay = delay.boxed();
    let interval = interval.boxed();
    let window = window.boxed();

    start
        .prop_flat_map(move |start| arb_timestamp_window(start, delay.clone(), interval.clone(), window.clone()))
        .prop_flat_map(move |(timestamps, time_window)| {
            let builder_strategy = timestamps
                .into_iter()
                .fold(
                    Just(AppDataWindow::builder().with_time_window(time_window)).boxed(),
                    |acc, ts| do_next_arb_metric_catalog(acc, make_metric_catalog(ts))
                );

            builder_strategy.prop_map(|builder| builder.build().expect("failed to build valid metric catalog data window"))
        })
    // window
    //todo -- DMR - WORK HERE
    // Just(AppDataWindow::from_time_window(MetricCatalog::empty(), Duration::from_secs(120)))
}

fn do_next_arb_metric_catalog(
    builder: BoxedStrategy<AppDataWindowBuilder<MetricCatalog>>,
    metric_catalog: impl Strategy<Value = MetricCatalog> + 'static,
) -> BoxedStrategy<AppDataWindowBuilder<MetricCatalog>> {
    (builder, metric_catalog)
        .prop_map(|(builder, metric_catalog)| {
            builder.with_item(metric_catalog)
        })
        .boxed()
}

#[derive(Debug, Default, Clone)]
pub struct MetricCatalogStrategyBuilder {
    pub recv_timestamp: Option<BoxedStrategy<Timestamp>>,
    pub health: Option<BoxedStrategy<JobHealthMetrics>>,
    pub flow: Option<BoxedStrategy<FlowMetrics>>,
    pub cluster: Option<BoxedStrategy<ClusterMetrics>>,
    pub custom: Option<BoxedStrategy<telemetry::TableType>>,
}

static CORRELATION_GEN: Lazy<Mutex<CorrelationGenerator>> = Lazy::new(|| {
    Mutex::new(CorrelationGenerator::distributed(MachineNode::new(1,1).unwrap(), IdPrettifier::<AlphabetCodec>::default()))
});

#[allow(dead_code)]
impl MetricCatalogStrategyBuilder {
    pub fn strategy() -> impl Strategy<Value = MetricCatalog> {
        Self::new().finish()
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn recv_timestamp(self, timestamp: impl Strategy<Value = Timestamp> + 'static) -> Self {
        let mut new = self;
        new.recv_timestamp = Some(timestamp.boxed());
        new
    }

    pub fn just_recv_timestamp(self, timestamp: impl Into<Timestamp>) -> Self {
        self.recv_timestamp(Just(timestamp.into()))
    }

    pub fn health(self, health: impl Strategy<Value = JobHealthMetrics> + 'static) -> Self {
        let mut new = self;
        new.health = Some(health.boxed());
        new
    }

    pub fn just_health(self, health: impl Into<JobHealthMetrics>) -> Self {
        self.health(Just(health.into()))
    }

    pub fn flow(self, flow: impl Strategy<Value = FlowMetrics> + 'static) -> Self {
        let mut new = self;
        new.flow = Some(flow.boxed());
        new
    }

    pub fn just_flow(self, flow: impl Into<FlowMetrics>) -> Self {
        self.flow(Just(flow.into()))
    }

    pub fn cluster(self, cluster: impl Strategy<Value = ClusterMetrics> + 'static) -> Self {
        let mut new = self;
        new.cluster = Some(cluster.boxed());
        new
    }

    pub fn just_cluster(self, cluster: impl Into<ClusterMetrics>) -> Self {
        self.cluster(Just(cluster.into()))
    }

    pub fn custom(self, custom: impl Strategy<Value = telemetry::TableType> + 'static) -> Self {
        let mut new = self;
        new.custom = Some(custom.boxed());
        new
    }

    pub fn just_custom(self, custom: impl Into<telemetry::TableType>) -> Self {
        self.custom(Just(custom.into()))
    }

    pub fn finish(self) -> impl Strategy<Value = MetricCatalog> {
        tracing::info!(?self, "DMR: building metric catalog strategy");
        let recv_timestamp = self.recv_timestamp.unwrap_or(arb_timestamp().boxed());
        let health = self.health.unwrap_or(arb_job_health_metrics().boxed());
        let flow = self.flow.unwrap_or(arb_flow_metrics().boxed());
        let cluster = self.cluster.unwrap_or(arb_cluster_metrics().boxed());
        let custom = self.custom.unwrap_or(arb_telemetry_table_type().boxed());

        (recv_timestamp, health, flow, cluster, custom)
            .prop_map(|(recv_timestamp, health, flow, cluster, custom)| {
                let mut correlation_gen = CORRELATION_GEN.lock().unwrap();
                let correlation_id = correlation_gen.next_id();
                tracing::info!(%correlation_id, %recv_timestamp, ?health, ?flow, ?cluster, ?custom, "DMR: making metric catalog...");
                MetricCatalog { correlation_id, recv_timestamp, health, flow, cluster, custom, }
            })
    }
}

//todo - DMR - WORK HERE
pub fn arb_job_health_metrics() -> impl Strategy<Value = JobHealthMetrics> {
    Just(JobHealthMetrics::default())
}

pub fn arb_flow_metrics() -> impl Strategy<Value = FlowMetrics> {
    Just(FlowMetrics::default())
}

pub fn arb_cluster_metrics() -> impl Strategy<Value = ClusterMetrics> {
    Just(ClusterMetrics::default())
}
