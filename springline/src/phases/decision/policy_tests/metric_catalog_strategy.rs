use super::*;
use crate::flink::{AppDataWindowBuilder, ClusterMetrics, CorrelationGenerator, FlowMetrics, JobHealthMetrics};
use once_cell::sync::Lazy;
use pretty_snowflake::{AlphabetCodec, IdPrettifier};
use proctor::elements::telemetry;
use std::sync::Mutex;

pub fn arb_metric_catalog_window_from_timestamp_window<M>(
    timestamps: impl Strategy<Value = (Vec<Timestamp>, Duration)>, make_data_strategy: M,
) -> impl Strategy<Value = AppDataWindow<MetricCatalog>>
where
    M: FnMut(Timestamp) -> BoxedStrategy<MetricCatalog> + Clone + 'static,
{
    timestamps
        .prop_flat_map(move |(timestamps, window)| {
            let acc = AppDataWindowBuilder::default().with_time_window(window);
            data_loop(timestamps, Just(acc), make_data_strategy.clone())
        })
        .prop_map(|builder| builder.build().expect("failed to build valid metric catalog data window"))
}

fn data_loop<M>(
    timestamps: Vec<Timestamp>, acc: impl Strategy<Value = AppDataWindowBuilder<MetricCatalog>> + 'static,
    mut make_data_strategy: M,
) -> impl Strategy<Value = AppDataWindowBuilder<MetricCatalog>>
where
    M: FnMut(Timestamp) -> BoxedStrategy<MetricCatalog> + Clone + 'static,
{
    let mut ts_iter = timestamps.into_iter();
    let next_ts = ts_iter.next();
    let remaining: Vec<Timestamp> = ts_iter.collect();
    let make_data_strategy_0 = make_data_strategy.clone();
    let acc = acc.boxed();
    next_ts.map_or(acc.clone(), move |recv_ts| {
        let make_data_strategy_1 = make_data_strategy_0.clone();
        (acc, make_data_strategy(recv_ts))
            .prop_flat_map(move |(acc_0, data)| {
                let mut acc_1 = acc_0.clone();
                acc_1.push(data);
                data_loop(remaining.clone(), Just(acc_1), make_data_strategy_1.clone()).boxed()
            })
            .boxed()
    })
}

pub fn arb_metric_catalog_window<M>(
    start: Timestamp, window: impl Strategy<Value = Duration>, interval: impl Strategy<Value = Duration> + 'static,
    make_data_strategy: M,
) -> impl Strategy<Value = AppDataWindow<MetricCatalog>>
where
    M: Fn(Timestamp) -> BoxedStrategy<MetricCatalog> + Clone + 'static,
{
    let interval = interval.boxed();
    let interval = move || interval.clone();

    window.prop_flat_map(move |window| {
        let builder = AppDataWindowBuilder::default()
            .with_quorum_percentage(0.6)
            .with_time_window(window);
        let acc_start = Just((builder, Some((start, window))));
        do_arb_metric_catalog_window_loop(acc_start, interval.clone(), make_data_strategy.clone())
            .prop_map(move |(data, _)| data.build().expect("failed to generate valid metric catalog data window"))
    })
}

#[tracing::instrument(level = "info", skip(acc, make_interval_strategy, make_data_strategy))]
fn do_arb_metric_catalog_window_loop<I, M>(
    acc: impl Strategy<Value = (AppDataWindowBuilder<MetricCatalog>, Option<(Timestamp, Duration)>)>,
    make_interval_strategy: I, make_data_strategy: M,
) -> impl Strategy<Value = (AppDataWindowBuilder<MetricCatalog>, Option<(Timestamp, Duration)>)>
where
    I: Fn() -> BoxedStrategy<Duration> + Clone + 'static,
    M: Fn(Timestamp) -> BoxedStrategy<MetricCatalog> + Clone + 'static,
{
    (acc, make_interval_strategy()).prop_flat_map(move |((acc_data, next_remaining), interval)| {
        match next_remaining {
            None => Just((acc_data, None)).boxed(),
            Some((_, remaining)) if remaining < interval => Just((acc_data, None)).boxed(),
            Some((last_ts, remaining)) => {
                let recv_ts = last_ts + interval;
                let make_interval_strategy_0 = make_interval_strategy.clone();
                let make_data_strategy_0 = make_data_strategy.clone();

                make_data_strategy(recv_ts)
                    .prop_flat_map(move |data| {
                        let mut acc_data_0 = acc_data.clone();

                        let next_remaining = remaining - interval;

                        acc_data_0.push(data);

                        let next_acc: (AppDataWindowBuilder<MetricCatalog>, Option<(Timestamp, Duration)>) =
                            (acc_data_0, Some((recv_ts, next_remaining)));

                        do_arb_metric_catalog_window_loop(
                            Just(next_acc),
                            make_interval_strategy_0.clone(),
                            make_data_strategy_0.clone(),
                        )
                        .boxed()
                    })
                    .boxed()
            },
        }
        .boxed()
    })
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
    Mutex::new(CorrelationGenerator::distributed(
        MachineNode::new(1, 1).unwrap(),
        IdPrettifier::<AlphabetCodec>::default(),
    ))
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

    // #[tracing::instrument(level="info")]
    pub fn finish(self) -> impl Strategy<Value = MetricCatalog> {
        // tracing::info!(?self, "DMR: building metric catalog strategy");
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
