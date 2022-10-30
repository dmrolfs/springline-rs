use dialoguer::FuzzySelect;
use springline::flink::{ClusterMetrics, FlowMetrics, JobHealthMetrics};
use springline::model::NrReplicas;
use std::str::FromStr;
use tailcall::tailcall;
use trim_margin::MarginTrimmable;

use super::*;
use crate::THEME;

impl PopulateData for MetricCatalog {
    fn make(_now: DateTime<Utc>, _settings: &Settings) -> Result<Self>
    where
        Self: Sized,
    {
        let baseline = MetricCatalog {
            health: JobHealthMetrics::default(),
            flow: FlowMetrics::default(),
            cluster: ClusterMetrics::default(),
            custom: HashMap::default(),
        };

        let facets: [(&str, Option<MetricCatalogLens>); 19] = [
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
                Some(MetricCatalogLens::Health(
                    JobHealthLens::NrCompletedCheckpoints,
                )),
            ),
            (
                "health.job_nr_failed_checkpoints",
                Some(MetricCatalogLens::Health(
                    JobHealthLens::NrFailedCheckpoints,
                )),
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
                "flow.source_records_lag_max",
                Some(MetricCatalogLens::Flow(FlowLens::SourceRecordsLagMax)),
            ),
            (
                "flow.source_millis_behind_latest",
                Some(MetricCatalogLens::Flow(FlowLens::SourceMillisBehindLatest)),
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
                Some(MetricCatalogLens::Cluster(
                    ClusterLens::TaskHeapMemoryCommitted,
                )),
            ),
            (
                "cluster.task_nr_threads",
                Some(MetricCatalogLens::Cluster(ClusterLens::TaskNrThreads)),
            ),
            (
                "cluster.task_network_input_queue_len",
                Some(MetricCatalogLens::Cluster(
                    ClusterLens::TaskNetworkInputQueueLen,
                )),
            ),
            (
                "cluster.task_network_input_pool_usage",
                Some(MetricCatalogLens::Cluster(
                    ClusterLens::TaskNetworkInputPoolUsage,
                )),
            ),
            (
                "cluster.task_network_output_queue_len",
                Some(MetricCatalogLens::Cluster(
                    ClusterLens::TaskNetworkOutputQueueLen,
                )),
            ),
            (
                "cluster.task_network_output_pool_usage",
                Some(MetricCatalogLens::Cluster(
                    ClusterLens::TaskNetworkOutputPoolUsage,
                )),
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
    Health(JobHealthLens),
    Flow(FlowLens),
    Cluster(ClusterLens),
}

impl Lens for MetricCatalogLens {
    type T = MetricCatalog;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::Health(lens) => lens.get(&telemetry.health),
            Self::Flow(lens) => lens.get(&telemetry.flow),
            Self::Cluster(lens) => lens.get(&telemetry.cluster),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::Health(lens) => lens.set(&mut telemetry.health, value_rep),
            Self::Flow(lens) => lens.set(&mut telemetry.flow, value_rep),
            Self::Cluster(lens) => lens.set(&mut telemetry.cluster, value_rep),
        }
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
            Self::UptimeMillis => format!("{:?}", telemetry.job_uptime_millis),
            Self::NrRestarts => format!("{:?}", telemetry.job_nr_restarts),
            Self::NrCompletedCheckpoints => format!("{:?}", telemetry.job_nr_completed_checkpoints),
            Self::NrFailedCheckpoints => format!("{:?}", telemetry.job_nr_failed_checkpoints),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::UptimeMillis => {
                telemetry.job_uptime_millis = Some(u32::from_str(value_rep.as_ref())?)
            },
            Self::NrRestarts => {
                telemetry.job_nr_restarts = Some(u32::from_str(value_rep.as_ref())?)
            },
            Self::NrCompletedCheckpoints => {
                telemetry.job_nr_completed_checkpoints = Some(u32::from_str(value_rep.as_ref())?)
            },
            Self::NrFailedCheckpoints => {
                telemetry.job_nr_failed_checkpoints = Some(u32::from_str(value_rep.as_ref())?)
            },
        }

        Ok(())
    }
}

enum FlowLens {
    RecordsInPerSec,
    RecordsOutPerSec,
    SourceRecordsLagMax,
    SourceMillisBehindLatest,
}

impl Lens for FlowLens {
    type T = FlowMetrics;

    fn get(&self, telemetry: &Self::T) -> String {
        match self {
            Self::RecordsInPerSec => format!("{}", telemetry.records_in_per_sec),
            Self::RecordsOutPerSec => format!("{}", telemetry.records_out_per_sec),
            Self::SourceRecordsLagMax => telemetry
                .source_records_lag_max
                .map(|t| format!("{}", t))
                .unwrap_or_default(),
            Self::SourceMillisBehindLatest => telemetry
                .source_millis_behind_latest
                .map(|t| format!("{}", t))
                .unwrap_or_default(),
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::RecordsInPerSec => {
                telemetry.records_in_per_sec = f64::from_str(value_rep.as_ref())?
            },
            Self::RecordsOutPerSec => {
                telemetry.records_out_per_sec = f64::from_str(value_rep.as_ref())?
            },
            Self::SourceRecordsLagMax => {
                telemetry.source_records_lag_max = if value_rep.as_ref().is_empty() {
                    None
                } else {
                    Some(u32::from_str(value_rep.as_ref())?)
                };
            },
            Self::SourceMillisBehindLatest => {
                telemetry.source_millis_behind_latest = if value_rep.as_ref().is_empty() {
                    None
                } else {
                    Some(u32::from_str(value_rep.as_ref())?)
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
            Self::TaskNetworkInputPoolUsage => {
                format!("{}", telemetry.task_network_input_pool_usage)
            },
            Self::TaskNetworkOutputQueueLen => {
                format!("{}", telemetry.task_network_output_queue_len)
            },
            Self::TaskNetworkOutputPoolUsage => {
                format!("{}", telemetry.task_network_output_pool_usage)
            },
        }
    }

    fn set(&self, telemetry: &mut Self::T, value_rep: impl AsRef<str>) -> anyhow::Result<()> {
        match self {
            Self::NrActiveJobs => telemetry.nr_active_jobs = u32::from_str(value_rep.as_ref())?,
            Self::NrTaskManagers => {
                telemetry.nr_task_managers = NrReplicas::new(u32::from_str(value_rep.as_ref())?)
            },
            Self::TaskCpuLoad => telemetry.task_cpu_load = f64::from_str(value_rep.as_ref())?,
            Self::TaskHeapMemoryUsed => {
                telemetry.task_heap_memory_used = f64::from_str(value_rep.as_ref())?
            },
            Self::TaskHeapMemoryCommitted => {
                telemetry.task_heap_memory_committed = f64::from_str(value_rep.as_ref())?
            },
            Self::TaskNrThreads => telemetry.task_nr_threads = u32::from_str(value_rep.as_ref())?,
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
