use super::*;
use crate::flink::{
    AppDataWindowBuilder, ClusterMetrics, CorrelationGenerator, FlowMetrics, JobHealthMetrics,
};
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
        .prop_map(|builder| {
            builder.build().expect("failed to build valid metric catalog data window")
        })
}

fn data_loop<M>(
    timestamps: Vec<Timestamp>,
    acc: impl Strategy<Value = AppDataWindowBuilder<MetricCatalog>> + 'static,
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
    start: Timestamp, window: impl Strategy<Value = Duration>,
    interval: impl Strategy<Value = Duration> + 'static, make_data_strategy: M,
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
            .prop_map(move |(data, _)| {
                data.build().expect("failed to generate valid metric catalog data window")
            })
    })
}

#[tracing::instrument(level = "debug", skip(acc, make_interval_strategy, make_data_strategy))]
fn do_arb_metric_catalog_window_loop<I, M>(
    acc: impl Strategy<
        Value = (
            AppDataWindowBuilder<MetricCatalog>,
            Option<(Timestamp, Duration)>,
        ),
    >,
    make_interval_strategy: I, make_data_strategy: M,
) -> impl Strategy<
    Value = (
        AppDataWindowBuilder<MetricCatalog>,
        Option<(Timestamp, Duration)>,
    ),
>
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

                        let next_acc: (
                            AppDataWindowBuilder<MetricCatalog>,
                            Option<(Timestamp, Duration)>,
                        ) = (acc_data_0, Some((recv_ts, next_remaining)));

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
    recv_timestamp: Option<BoxedStrategy<Timestamp>>,
    health: Option<BoxedStrategy<JobHealthMetrics>>,
    flow: Option<BoxedStrategy<FlowMetrics>>,
    cluster: Option<BoxedStrategy<ClusterMetrics>>,
    custom: Option<BoxedStrategy<telemetry::TableType>>,
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

    pub fn finish(self) -> impl Strategy<Value = MetricCatalog> {
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

pub fn arb_job_health_metrics() -> impl Strategy<Value = JobHealthMetrics> {
    JobHealthMetricsStrategyBuilder::strategy()
}

#[derive(Debug, Default, Clone)]
pub struct JobHealthMetricsStrategyBuilder {
    job_max_parallelism: Option<BoxedStrategy<u32>>,
    job_uptime_millis: Option<BoxedStrategy<u32>>,
    job_nr_restarts: Option<BoxedStrategy<u32>>,
    job_nr_completed_checkpoints: Option<BoxedStrategy<u32>>,
    job_nr_failed_checkpoints: Option<BoxedStrategy<u32>>,
}

#[allow(dead_code)]
impl JobHealthMetricsStrategyBuilder {
    pub fn strategy() -> impl Strategy<Value = JobHealthMetrics> {
        Self::new().finish()
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn job_max_parallelism(
        self, job_max_parallelism: impl Strategy<Value = u32> + 'static,
    ) -> Self {
        let mut new = self;
        new.job_max_parallelism = Some(job_max_parallelism.boxed());
        new
    }

    pub fn just_job_max_parallelism(self, job_max_parallelism: impl Into<u32>) -> Self {
        self.job_max_parallelism(Just(job_max_parallelism.into()))
    }

    pub fn job_uptime_millis(
        self, job_uptime_millis: impl Strategy<Value = u32> + 'static,
    ) -> Self {
        let mut new = self;
        new.job_uptime_millis = Some(job_uptime_millis.boxed());
        new
    }

    pub fn just_job_uptime_millis(self, job_uptime_millis: impl Into<u32>) -> Self {
        self.job_uptime_millis(Just(job_uptime_millis.into()))
    }

    pub fn job_nr_restarts(self, job_nr_restarts: impl Strategy<Value = u32> + 'static) -> Self {
        let mut new = self;
        new.job_nr_restarts = Some(job_nr_restarts.boxed());
        new
    }

    pub fn just_job_nr_restarts(self, job_nr_restarts: impl Into<u32>) -> Self {
        self.job_nr_restarts(Just(job_nr_restarts.into()))
    }

    pub fn job_nr_completed_checkpoints(
        self, job_nr_completed_checkpoints: impl Strategy<Value = u32> + 'static,
    ) -> Self {
        let mut new = self;
        new.job_nr_completed_checkpoints = Some(job_nr_completed_checkpoints.boxed());
        new
    }

    pub fn just_job_nr_completed_checkpoints(
        self, job_nr_completed_checkpoints: impl Into<u32>,
    ) -> Self {
        self.job_nr_completed_checkpoints(Just(job_nr_completed_checkpoints.into()))
    }

    pub fn job_nr_failed_checkpoints(
        self, job_nr_failed_checkpoints: impl Strategy<Value = u32> + 'static,
    ) -> Self {
        let mut new = self;
        new.job_nr_failed_checkpoints = Some(job_nr_failed_checkpoints.boxed());
        new
    }

    pub fn just_job_nr_failed_checkpoints(self, job_nr_failed_checkpoints: impl Into<u32>) -> Self {
        self.job_nr_failed_checkpoints(Just(job_nr_failed_checkpoints.into()))
    }

    pub fn finish(self) -> impl Strategy<Value = JobHealthMetrics> {
        let job_max_parallelism = self.job_max_parallelism.unwrap_or(any::<u32>().boxed());
        let job_uptime_millis = self.job_uptime_millis.unwrap_or(any::<u32>().boxed());
        let job_nr_restarts = self.job_nr_restarts.unwrap_or(any::<u32>().boxed());
        let job_nr_completed_checkpoints =
            self.job_nr_completed_checkpoints.unwrap_or(any::<u32>().boxed());
        let job_nr_failed_checkpoints =
            self.job_nr_failed_checkpoints.unwrap_or(any::<u32>().boxed());

        (
            job_max_parallelism,
            job_uptime_millis,
            job_nr_restarts,
            job_nr_completed_checkpoints,
            job_nr_failed_checkpoints,
        )
            .prop_map(
                |(
                    job_max_parallelism,
                    job_uptime_millis,
                    job_nr_restarts,
                    job_nr_completed_checkpoints,
                    job_nr_failed_checkpoints,
                )| {
                    JobHealthMetrics {
                        job_max_parallelism,
                        job_uptime_millis,
                        job_nr_restarts,
                        job_nr_completed_checkpoints,
                        job_nr_failed_checkpoints,
                    }
                },
            )
    }
}

pub fn arb_flow_metrics() -> impl Strategy<Value = FlowMetrics> {
    FlowMetricsStrategyBuilder::strategy()
}

#[derive(Debug, Default, Clone)]
pub struct FlowMetricsStrategyBuilder {
    records_in_per_sec: Option<BoxedStrategy<f64>>,
    records_out_per_sec: Option<BoxedStrategy<f64>>,
    idle_time_millis_per_sec: Option<BoxedStrategy<f64>>,
    source_back_pressured_time_millis_per_sec: Option<BoxedStrategy<f64>>,
    forecasted_timestamp: Option<BoxedStrategy<Option<Timestamp>>>,
    forecasted_records_in_per_sec: Option<BoxedStrategy<Option<f64>>>,
    source_records_lag_max: Option<BoxedStrategy<Option<u32>>>,
    source_assigned_partitions: Option<BoxedStrategy<Option<u32>>>,
    source_records_consumed_rate: Option<BoxedStrategy<Option<f64>>>,
    source_millis_behind_latest: Option<BoxedStrategy<Option<u32>>>,
}

#[allow(dead_code)]
impl FlowMetricsStrategyBuilder {
    pub fn strategy() -> impl Strategy<Value = FlowMetrics> {
        Self::new().finish()
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn records_in_per_sec(
        self, records_in_per_sec: impl Strategy<Value = f64> + 'static,
    ) -> Self {
        let mut new = self;
        new.records_in_per_sec = Some(records_in_per_sec.boxed());
        new
    }

    pub fn just_records_in_per_sec(self, records_in_per_sec: impl Into<f64>) -> Self {
        self.records_in_per_sec(Just(records_in_per_sec.into()))
    }

    pub fn records_out_per_sec(
        self, records_out_per_sec: impl Strategy<Value = f64> + 'static,
    ) -> Self {
        let mut new = self;
        new.records_out_per_sec = Some(records_out_per_sec.boxed());
        new
    }

    pub fn just_records_out_per_sec(self, records_out_per_sec: impl Into<f64>) -> Self {
        self.records_out_per_sec(Just(records_out_per_sec.into()))
    }

    pub fn idle_time_millis_per_sec(
        self, idle_time_millis_per_sec: impl Strategy<Value = f64> + 'static,
    ) -> Self {
        let mut new = self;
        new.idle_time_millis_per_sec = Some(idle_time_millis_per_sec.boxed());
        new
    }

    pub fn just_idle_time_millis_per_sec(self, idle_time_millis_per_sec: impl Into<f64>) -> Self {
        self.idle_time_millis_per_sec(Just(idle_time_millis_per_sec.into()))
    }

    pub fn source_back_pressured_time_millie_per_sec(
        self, source_back_pressured_time_millie_per_sec: impl Strategy<Value = f64> + 'static,
    ) -> Self {
        let mut new = self;
        new.source_back_pressured_time_millis_per_sec =
            Some(source_back_pressured_time_millie_per_sec.boxed());
        new
    }

    pub fn just_source_back_pressured_time_millie_per_sec(
        self, source_back_pressured_time_millie_per_sec: impl Into<f64>,
    ) -> Self {
        self.source_back_pressured_time_millie_per_sec(Just(
            source_back_pressured_time_millie_per_sec.into(),
        ))
    }

    pub fn forecasted_timestamp(
        self, forecasted_timestamp: impl Strategy<Value = Option<Timestamp>> + 'static,
    ) -> Self {
        let mut new = self;
        new.forecasted_timestamp = Some(forecasted_timestamp.boxed());
        new
    }

    pub fn just_forecasted_timestamp(
        self, forecasted_timestamp: impl Into<Option<Timestamp>>,
    ) -> Self {
        self.forecasted_timestamp(Just(forecasted_timestamp.into()))
    }

    pub fn forecasted_records_in_per_sec(
        self, forecasted_records_in_per_sec: impl Strategy<Value = Option<f64>> + 'static,
    ) -> Self {
        let mut new = self;
        new.forecasted_records_in_per_sec = Some(forecasted_records_in_per_sec.boxed());
        new
    }

    pub fn just_forecasted_records_in_per_sec(
        self, forecasted_records_in_per_sec: impl Into<Option<f64>>,
    ) -> Self {
        self.forecasted_records_in_per_sec(Just(forecasted_records_in_per_sec.into()))
    }

    pub fn source_records_lag_max(
        self, source_records_lag_max: impl Strategy<Value = Option<u32>> + 'static,
    ) -> Self {
        let mut new = self;
        new.source_records_lag_max = Some(source_records_lag_max.boxed());
        new
    }

    pub fn just_source_records_lag_max(
        self, source_records_lag_max: impl Into<Option<u32>>,
    ) -> Self {
        self.source_records_lag_max(Just(source_records_lag_max.into()))
    }

    pub fn source_assigned_partitions(
        self, source_assigned_partitions: impl Strategy<Value = Option<u32>> + 'static,
    ) -> Self {
        let mut new = self;
        new.source_assigned_partitions = Some(source_assigned_partitions.boxed());
        new
    }

    pub fn just_source_assigned_partitions(
        self, source_assigned_partitions: impl Into<Option<u32>>,
    ) -> Self {
        self.source_assigned_partitions(Just(source_assigned_partitions.into()))
    }

    pub fn source_records_consumed_rate(
        self, source_records_consumed_rate: impl Strategy<Value = Option<f64>> + 'static,
    ) -> Self {
        let mut new = self;
        new.source_records_consumed_rate = Some(source_records_consumed_rate.boxed());
        new
    }

    pub fn just_source_records_consumed_rate(
        self, source_records_consumed_rate: impl Into<Option<f64>>,
    ) -> Self {
        self.source_records_consumed_rate(Just(source_records_consumed_rate.into()))
    }

    pub fn source_millis_behind_latest(
        self, source_millis_behind_latest: impl Strategy<Value = Option<u32>> + 'static,
    ) -> Self {
        let mut new = self;
        new.source_millis_behind_latest = Some(source_millis_behind_latest.boxed());
        new
    }

    pub fn just_source_millis_behind_latest(
        self, source_millis_behind_latest: impl Into<Option<u32>>,
    ) -> Self {
        self.source_millis_behind_latest(Just(source_millis_behind_latest.into()))
    }

    pub fn finish(self) -> impl Strategy<Value = FlowMetrics> {
        let records_in_per_sec = self.records_in_per_sec.unwrap_or(any::<f64>().boxed());
        let records_out_per_sec = self.records_out_per_sec.unwrap_or(any::<f64>().boxed());
        let idle_time_millis_per_sec =
            self.idle_time_millis_per_sec.unwrap_or(any::<f64>().boxed());
        let source_back_pressured_time_millis_per_sec = self
            .source_back_pressured_time_millis_per_sec
            .unwrap_or(any::<f64>().boxed());
        let forecasted_timestamp = self.forecasted_timestamp.unwrap_or(
            prop::option::of(arb_timestamp_after(
                Timestamp::now(),
                arb_range_duration(1..=1200),
            ))
            .boxed(),
        );
        let forecasted_records_in_per_sec = self
            .forecasted_records_in_per_sec
            .unwrap_or(prop::option::of(any::<f64>()).boxed());
        let source_records_lag_max = self
            .source_records_lag_max
            .unwrap_or(prop::option::of(any::<u32>()).boxed());
        let source_assigned_partitions = self
            .source_assigned_partitions
            .unwrap_or(prop::option::of(any::<u32>()).boxed());
        let source_records_consumed_rate = self
            .source_records_consumed_rate
            .unwrap_or(prop::option::of(any::<f64>()).boxed());
        let source_millis_behind_latest = self
            .source_millis_behind_latest
            .unwrap_or(prop::option::of(any::<u32>()).boxed());

        (
            records_in_per_sec,
            records_out_per_sec,
            idle_time_millis_per_sec,
            source_back_pressured_time_millis_per_sec,
            forecasted_timestamp,
            forecasted_records_in_per_sec,
            source_records_lag_max,
            source_assigned_partitions,
            source_records_consumed_rate,
            source_millis_behind_latest,
        )
            .prop_map(
                |(
                    records_in_per_sec,
                    records_out_per_sec,
                    idle_time_millis_per_sec,
                    source_back_pressured_time_millis_per_sec,
                    forecasted_timestamp,
                    forecasted_records_in_per_sec,
                    source_records_lag_max,
                    source_assigned_partitions,
                    source_records_consumed_rate,
                    source_millis_behind_latest,
                )| {
                    let source_total_lag = source_records_lag_max
                        .zip(source_assigned_partitions)
                        .map(|(lag, partitions)| lag.saturating_mul(partitions));

                    FlowMetrics {
                        records_in_per_sec,
                        records_out_per_sec,
                        idle_time_millis_per_sec,
                        source_back_pressured_time_millis_per_sec,
                        forecasted_timestamp,
                        forecasted_records_in_per_sec,
                        source_records_lag_max,
                        source_assigned_partitions,
                        source_total_lag,
                        source_records_consumed_rate,
                        source_millis_behind_latest,
                    }
                },
            )
    }
}

pub fn arb_cluster_metrics() -> impl Strategy<Value = ClusterMetrics> {
    ClusterMetricsStrategyBuilder::strategy()
}

#[derive(Debug, Default, Clone)]
pub struct ClusterMetricsStrategyBuilder {
    nr_active_jobs: Option<BoxedStrategy<u32>>,
    nr_task_managers: Option<BoxedStrategy<u32>>,
    task_cpu_load: Option<BoxedStrategy<f64>>,
    task_heap_memory_used: Option<BoxedStrategy<f64>>,
    task_heap_memory_committed: Option<BoxedStrategy<f64>>,
    task_nr_threads: Option<BoxedStrategy<u32>>,
    task_network_input_queue_len: Option<BoxedStrategy<f64>>,
    task_network_input_pool_usage: Option<BoxedStrategy<f64>>,
    task_network_output_queue_len: Option<BoxedStrategy<f64>>,
    task_network_output_pool_usage: Option<BoxedStrategy<f64>>,
}

#[allow(dead_code)]
impl ClusterMetricsStrategyBuilder {
    pub fn strategy() -> impl Strategy<Value = ClusterMetrics> {
        Self::new().finish()
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn nr_active_jobs(self, nr_active_jobs: impl Strategy<Value = u32> + 'static) -> Self {
        let mut new = self;
        new.nr_active_jobs = Some(nr_active_jobs.boxed());
        new
    }

    pub fn just_nr_active_jobs(self, nr_active_jobs: impl Into<u32>) -> Self {
        self.nr_active_jobs(Just(nr_active_jobs.into()))
    }

    pub fn nr_task_managers(self, nr_task_managers: impl Strategy<Value = u32> + 'static) -> Self {
        let mut new = self;
        new.nr_task_managers = Some(nr_task_managers.boxed());
        new
    }

    pub fn just_nr_task_managers(self, nr_task_managers: impl Into<u32>) -> Self {
        self.nr_task_managers(Just(nr_task_managers.into()))
    }

    pub fn task_cpu_load(self, task_cpu_load: impl Strategy<Value = f64> + 'static) -> Self {
        let mut new = self;
        new.task_cpu_load = Some(task_cpu_load.boxed());
        new
    }

    pub fn just_task_cpu_load(self, task_cpu_load: impl Into<f64>) -> Self {
        self.task_cpu_load(Just(task_cpu_load.into()))
    }

    pub fn task_heap_memory_used(
        self, task_heap_memory_used: impl Strategy<Value = f64> + 'static,
    ) -> Self {
        let mut new = self;
        new.task_heap_memory_used = Some(task_heap_memory_used.boxed());
        new
    }

    pub fn just_heap_memory_used(self, task_heap_memory_used: impl Into<f64>) -> Self {
        self.task_heap_memory_used(Just(task_heap_memory_used.into()))
    }

    pub fn task_heap_memory_committed(
        self, task_heap_memory_committed: impl Strategy<Value = f64> + 'static,
    ) -> Self {
        let mut new = self;
        new.task_heap_memory_committed = Some(task_heap_memory_committed.boxed());
        new
    }

    pub fn just_task_heap_memory_committed(
        self, task_heap_memory_committed: impl Into<f64>,
    ) -> Self {
        self.task_heap_memory_committed(Just(task_heap_memory_committed.into()))
    }

    pub fn task_nr_threads(self, task_nr_threads: impl Strategy<Value = u32> + 'static) -> Self {
        let mut new = self;
        new.task_nr_threads = Some(task_nr_threads.boxed());
        new
    }

    pub fn just_task_nr_threads(self, task_nr_threads: impl Into<u32>) -> Self {
        self.task_nr_threads(Just(task_nr_threads.into()))
    }

    pub fn task_network_input_queue_len(
        self, task_network_input_queue_len: impl Strategy<Value = f64> + 'static,
    ) -> Self {
        let mut new = self;
        new.task_network_input_queue_len = Some(task_network_input_queue_len.boxed());
        new
    }

    pub fn just_task_network_input_queue_len(
        self, task_network_input_queue_len: impl Into<f64>,
    ) -> Self {
        self.task_network_input_queue_len(Just(task_network_input_queue_len.into()))
    }

    pub fn task_network_output_queue_len(
        self, task_network_output_queue_len: impl Strategy<Value = f64> + 'static,
    ) -> Self {
        let mut new = self;
        new.task_network_output_queue_len = Some(task_network_output_queue_len.boxed());
        new
    }

    pub fn just_task_network_output_queue_len(
        self, task_network_output_queue_len: impl Into<f64>,
    ) -> Self {
        self.task_network_output_queue_len(Just(task_network_output_queue_len.into()))
    }

    pub fn task_network_output_pool_usage(
        self, task_network_output_pool_usage: impl Strategy<Value = f64> + 'static,
    ) -> Self {
        let mut new = self;
        new.task_network_output_pool_usage = Some(task_network_output_pool_usage.boxed());
        new
    }

    pub fn just_task_network_output_pool_usage(
        self, task_network_output_pool_usage: impl Into<f64>,
    ) -> Self {
        self.task_network_output_pool_usage(Just(task_network_output_pool_usage.into()))
    }

    pub fn finish(self) -> impl Strategy<Value = ClusterMetrics> {
        let nr_active_jobs = self.nr_active_jobs.unwrap_or(any::<u32>().boxed());
        let nr_task_managers = self.nr_task_managers.unwrap_or(any::<u32>().boxed());
        let task_cpu_load = self.task_cpu_load.unwrap_or(any::<f64>().boxed());
        let task_heap_memory_used = self.task_heap_memory_used.unwrap_or(any::<f64>().boxed());
        let task_heap_memory_committed =
            self.task_heap_memory_committed.unwrap_or(any::<f64>().boxed());
        let task_nr_threads = self.task_nr_threads.unwrap_or(any::<u32>().boxed());
        let task_network_input_queue_len =
            self.task_network_input_queue_len.unwrap_or(any::<f64>().boxed());
        let task_network_input_pool_usage =
            self.task_network_input_pool_usage.unwrap_or(any::<f64>().boxed());
        let task_network_output_queue_len =
            self.task_network_output_queue_len.unwrap_or(any::<f64>().boxed());
        let task_network_output_pool_usage =
            self.task_network_output_pool_usage.unwrap_or(any::<f64>().boxed());

        (
            nr_active_jobs,
            nr_task_managers,
            task_cpu_load,
            task_heap_memory_used,
            task_heap_memory_committed,
            task_nr_threads,
            task_network_input_queue_len,
            task_network_input_pool_usage,
            task_network_output_queue_len,
            task_network_output_pool_usage,
        )
            .prop_map(
                |(
                    nr_active_jobs,
                    nr_task_managers,
                    task_cpu_load,
                    task_heap_memory_used,
                    task_heap_memory_committed,
                    task_nr_threads,
                    task_network_input_queue_len,
                    task_network_input_pool_usage,
                    task_network_output_queue_len,
                    task_network_output_pool_usage,
                )| {
                    ClusterMetrics {
                        nr_active_jobs,
                        nr_task_managers,
                        task_cpu_load,
                        task_heap_memory_used,
                        task_heap_memory_committed,
                        task_nr_threads,
                        task_network_input_queue_len,
                        task_network_input_pool_usage,
                        task_network_output_queue_len,
                        task_network_output_pool_usage,
                    }
                },
            )
    }
}
