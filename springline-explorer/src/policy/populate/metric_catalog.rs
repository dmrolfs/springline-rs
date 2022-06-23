use std::str::FromStr;
use std::time::Duration;

use dialoguer::FuzzySelect;
use once_cell::sync::Lazy;
use pretty_snowflake::{AlphabetCodec, Id, IdPrettifier, Label, Labeling};
use proctor::elements::Timestamp;
use proctor::ProctorIdGenerator;
use springline::flink::{AppDataPortfolio, ClusterMetrics, FlowMetrics, JobHealthMetrics};
use tailcall::tailcall;
use trim_margin::MarginTrimmable;

use super::*;
use crate::THEME;

impl PopulateData for AppDataPortfolio<MetricCatalog> {
    fn make(now: DateTime<Utc>, settings: &Settings) -> Result<Self>
    where
        Self: Sized,
    {
        <MetricCatalog as PopulateData>::make(now, settings)
            .map(|catalog| AppDataPortfolio::from_time_window(catalog, Duration::from_secs(600)))
    }
}

impl PopulateData for MetricCatalog {
    fn make(now: DateTime<Utc>, _settings: &Settings) -> Result<Self>
    where
        Self: Sized,
    {
        let mut id_gen = ProctorIdGenerator::default();
        let baseline = MetricCatalog {
            recv_timestamp: now.into(),
            correlation_id: id_gen.next_id(),
            health: JobHealthMetrics::default(),
            flow: FlowMetrics::default(),
            cluster: ClusterMetrics::default(),
            custom: HashMap::default(),
        };

        let facets: [(&str, Option<MetricCatalogLens>); 21] = [
            (
                "correlation_id",
                Some(MetricCatalogLens::Root(MetricCatalogRootLens::CorrelationId)),
            ),
            (
                "timestamp",
                Some(MetricCatalogLens::Root(MetricCatalogRootLens::Timestamp)),
            ),
            (
                "health.job_uptime_millis",
                Some(MetricCatalogLens::Health(JobHealthLens::UptimeMillis)),
            ),
            (
                "health.job_nr_restarts",
                Some(MetricCatalogLens::Health(JobHealthLens::NrRestarts)),
            ),
            (
                "health.job_nr_completed_checkpoints",
                Some(MetricCatalogLens::Health(JobHealthLens::NrCompletedCheckpoints)),
            ),
            (
                "health.job_nr_failed_checkpoints",
                Some(MetricCatalogLens::Health(JobHealthLens::NrFailedCheckpoints)),
            ),
            (
                "flow.records_in_per_sec",
                Some(MetricCatalogLens::Flow(FlowLens::RecordsInPerSec)),
            ),
            (
                "flow.records_out_per_sec",
                Some(MetricCatalogLens::Flow(FlowLens::RecordsOutPerSec)),
            ),
            (
                "flow.input_records_lag_max",
                Some(MetricCatalogLens::Flow(FlowLens::InputRecordsLagMax)),
            ),
            (
                "flow.input_millis_behind_latest",
                Some(MetricCatalogLens::Flow(FlowLens::InputMillisBehindLatest)),
            ),
            (
                "cluster.nr_active_jobs",
                Some(MetricCatalogLens::Cluster(ClusterLens::NrActiveJobs)),
            ),
            (
                "cluster.nr_task_managers",
                Some(MetricCatalogLens::Cluster(ClusterLens::NrTaskManagers)),
            ),
            (
                "cluster.task_cpu_load",
                Some(MetricCatalogLens::Cluster(ClusterLens::TaskCpuLoad)),
            ),
            (
                "cluster.task_heap_memory_used",
                Some(MetricCatalogLens::Cluster(ClusterLens::TaskHeapMemoryUsed)),
            ),
            (
                "cluster.task_heap_memory_committed",
                Some(MetricCatalogLens::Cluster(ClusterLens::TaskHeapMemoryCommitted)),
            ),
            (
                "cluster.task_nr_threads",
                Some(MetricCatalogLens::Cluster(ClusterLens::TaskNrThreads)),
            ),
            (
                "cluster.task_network_input_queue_len",
                Some(MetricCatalogLens::Cluster(ClusterLens::TaskNetworkInputQueueLen)),
            ),
            (
                "cluster.task_network_input_pool_usage",
                Some(MetricCatalogLens::Cluster(ClusterLens::TaskNetworkInputPoolUsage)),
            ),
            (
                "cluster.task_network_output_queue_len",
                Some(MetricCatalogLens::Cluster(ClusterLens::TaskNetworkOutputQueueLen)),
            ),
            (
                "cluster.task_network_output_pool_usage",
                Some(MetricCatalogLens::Cluster(ClusterLens::TaskNetworkOutputPoolUsage)),
            ),
            ("Exit", None),
        ];

        #[allow(unreachable_code)]
        #[tailcall]
        fn do_loop(
            mut telemetry: MetricCatalog, facets: &[(&str, Option<MetricCatalogLens>)],
        ) -> anyhow::Result<MetricCatalog> {
            let selections: Vec<&str> = facets.iter().map(|s| s.0).collect();
            let selection = FuzzySelect::with_theme(&*THEME)
                .with_prompt(
                    r##"
                |Change any telemetry values (Esc to stop)?
                |Defaults are set to corresponding value in policy if possible.
                |"##
                    .trim_margin_with("|")
                    .unwrap(),
                )
                .items(selections.as_slice())
                .interact()?;

            let (label, telemetry_lens) = &facets[selection];
            match telemetry_lens {
                None => return Ok(telemetry),
                Some(lens) => {
                    let value = Input::with_theme(&*THEME)
                        .with_prompt(*label)
                        .default(lens.get(&telemetry))
                        .interact()?;

                    lens.set(&mut telemetry, value)?;
                },
            };

            do_loop(telemetry, facets)
        }

        do_loop(baseline, &facets)
    }
}

trait Lens {
    type T;
    fn get(&self, telemetry: &Self::T) -> String;
    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()>;
}

enum MetricCatalogLens {
    Root(MetricCatalogRootLens),
    Health(JobHealthLens),
    Flow(FlowLens),
    Cluster(ClusterLens),
}

impl Lens for MetricCatalogLens {
    type T = MetricCatalog;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::Root(lens) => lens.get(telemetry),
            Self::Health(lens) => lens.get(&telemetry.health),
            Self::Flow(lens) => lens.get(&telemetry.flow),
            Self::Cluster(lens) => lens.get(&telemetry.cluster),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::Root(lens) => lens.set(telemetry, value_rep),
            Self::Health(lens) => lens.set(&mut telemetry.health, value_rep),
            Self::Flow(lens) => lens.set(&mut telemetry.flow, value_rep),
            Self::Cluster(lens) => lens.set(&mut telemetry.cluster, value_rep),
        }
    }
}

enum MetricCatalogRootLens {
    CorrelationId,
    Timestamp,
}

static METRIC_CATALOG_LABEL: Lazy<String> = Lazy::new(|| <MetricCatalog as Label>::labeler().label().to_string());

impl Lens for MetricCatalogRootLens {
    type T = MetricCatalog;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::CorrelationId => format!("{:#}", telemetry.correlation_id),
            Self::Timestamp => format!("{:#}", telemetry.recv_timestamp),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::CorrelationId => {
                let snowflake = i64::from_str(value_rep.as_ref())?;
                telemetry.correlation_id = Id::new(
                    &*METRIC_CATALOG_LABEL,
                    snowflake,
                    &IdPrettifier::<AlphabetCodec>::default(),
                );
            },
            Self::Timestamp => {
                telemetry.recv_timestamp = Timestamp::from_str(value_rep.as_ref())?;
            },
        }

        Ok(())
    }
}

enum JobHealthLens {
    UptimeMillis,
    NrRestarts,
    NrCompletedCheckpoints,
    NrFailedCheckpoints,
}

impl Lens for JobHealthLens {
    type T = JobHealthMetrics;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::UptimeMillis => format!("{}", telemetry.job_uptime_millis),
            Self::NrRestarts => format!("{}", telemetry.job_nr_restarts),
            Self::NrCompletedCheckpoints => format!("{}", telemetry.job_nr_completed_checkpoints),
            Self::NrFailedCheckpoints => format!("{}", telemetry.job_nr_failed_checkpoints),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::UptimeMillis => telemetry.job_uptime_millis = i64::from_str(value_rep.as_ref())?,
            Self::NrRestarts => telemetry.job_nr_restarts = i64::from_str(value_rep.as_ref())?,
            Self::NrCompletedCheckpoints => telemetry.job_nr_completed_checkpoints = i64::from_str(value_rep.as_ref())?,
            Self::NrFailedCheckpoints => telemetry.job_nr_failed_checkpoints = i64::from_str(value_rep.as_ref())?,
        }

        Ok(())
    }
}

enum FlowLens {
    RecordsInPerSec,
    RecordsOutPerSec,
    InputRecordsLagMax,
    InputMillisBehindLatest,
}

impl Lens for FlowLens {
    type T = FlowMetrics;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::RecordsInPerSec => format!("{}", telemetry.records_in_per_sec),
            Self::RecordsOutPerSec => format!("{}", telemetry.records_out_per_sec),
            Self::InputRecordsLagMax => telemetry
                .input_records_lag_max
                .map(|t| format!("{}", t))
                .unwrap_or_default(),
            Self::InputMillisBehindLatest => telemetry
                .input_millis_behind_latest
                .map(|t| format!("{}", t))
                .unwrap_or_default(),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::RecordsInPerSec => telemetry.records_in_per_sec = f64::from_str(value_rep.as_ref())?,
            Self::RecordsOutPerSec => telemetry.records_out_per_sec = f64::from_str(value_rep.as_ref())?,
            Self::InputRecordsLagMax => {
                telemetry.input_records_lag_max = if value_rep.as_ref().is_empty() {
                    None
                } else {
                    Some(i64::from_str(value_rep.as_ref())?)
                };
            },
            Self::InputMillisBehindLatest => {
                telemetry.input_millis_behind_latest = if value_rep.as_ref().is_empty() {
                    None
                } else {
                    Some(i64::from_str(value_rep.as_ref())?)
                };
            },
        }

        Ok(())
    }
}

enum ClusterLens {
    NrActiveJobs,
    NrTaskManagers,
    TaskCpuLoad,
    TaskHeapMemoryUsed,
    TaskHeapMemoryCommitted,
    TaskNrThreads,
    TaskNetworkInputQueueLen,
    TaskNetworkInputPoolUsage,
    TaskNetworkOutputQueueLen,
    TaskNetworkOutputPoolUsage,
}

impl Lens for ClusterLens {
    type T = ClusterMetrics;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::NrActiveJobs => format!("{}", telemetry.nr_active_jobs),
            Self::NrTaskManagers => format!("{}", telemetry.nr_task_managers),
            Self::TaskCpuLoad => format!("{}", telemetry.task_cpu_load),
            Self::TaskHeapMemoryUsed => format!("{}", telemetry.task_heap_memory_used),
            Self::TaskHeapMemoryCommitted => format!("{}", telemetry.task_heap_memory_committed),
            Self::TaskNrThreads => format!("{}", telemetry.task_nr_threads),
            Self::TaskNetworkInputQueueLen => format!("{}", telemetry.task_network_input_queue_len),
            Self::TaskNetworkInputPoolUsage => format!("{}", telemetry.task_network_input_pool_usage),
            Self::TaskNetworkOutputQueueLen => format!("{}", telemetry.task_network_output_queue_len),
            Self::TaskNetworkOutputPoolUsage => format!("{}", telemetry.task_network_output_pool_usage),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::NrActiveJobs => telemetry.nr_active_jobs = u32::from_str(value_rep.as_ref())?,
            Self::NrTaskManagers => telemetry.nr_task_managers = u32::from_str(value_rep.as_ref())?,
            Self::TaskCpuLoad => telemetry.task_cpu_load = f64::from_str(value_rep.as_ref())?,
            Self::TaskHeapMemoryUsed => telemetry.task_heap_memory_used = f64::from_str(value_rep.as_ref())?,
            Self::TaskHeapMemoryCommitted => telemetry.task_heap_memory_committed = f64::from_str(value_rep.as_ref())?,
            Self::TaskNrThreads => telemetry.task_nr_threads = i64::from_str(value_rep.as_ref())?,
            Self::TaskNetworkInputQueueLen => {
                telemetry.task_network_input_queue_len = f64::from_str(value_rep.as_ref())?
            },
            Self::TaskNetworkInputPoolUsage => {
                telemetry.task_network_input_pool_usage = f64::from_str(value_rep.as_ref())?
            },
            Self::TaskNetworkOutputQueueLen => {
                telemetry.task_network_output_queue_len = f64::from_str(value_rep.as_ref())?
            },
            Self::TaskNetworkOutputPoolUsage => {
                telemetry.task_network_output_pool_usage = f64::from_str(value_rep.as_ref())?
            },
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[ignore]
    #[test]
    fn test_metric_catalog_populate_data() {
        todo!()
    }
}
