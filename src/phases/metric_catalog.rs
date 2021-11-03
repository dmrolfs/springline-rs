use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::ops::Add;

// use ::serde_with::{serde_as, TimestampSeconds};
use oso::PolarClass;
use serde::{Deserialize, Serialize};

use crate::phases::UpdateMetrics;
use lazy_static::lazy_static;
use pretty_snowflake::Id;
use proctor::elements::{telemetry, Telemetry, TelemetryValue, Timestamp};
use proctor::error::{ProctorError, TelemetryError};
use proctor::phases::collection::SubscriptionRequirements;
use proctor::SharedString;
use prometheus::{Gauge, IntGauge};

// #[serde_as]
#[derive(PolarClass, Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct MetricCatalog {
    pub correlation_id: Id,

    #[polar(attribute)]
    pub timestamp: Timestamp,

    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub job_health: JobHealthMetrics,

    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub flow: FlowMetrics,

    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub cluster: ClusterMetrics,

    #[polar(attribute)]
    #[serde(flatten)] // flatten to collect extra properties.
    pub custom: telemetry::TableType,
}

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct JobHealthMetrics {
    /// The time that the job has been running without interruption.
    //
    // Returns -1 for completed jobs (in milliseconds).
    #[polar(attribute)]
    pub job_uptime_millis: i64,

    /// The total number of restarts since this job was submitted, including full restarts and
    /// fine-grained restarts.
    #[polar(attribute)]
    pub job_nr_restarts: i64,

    /// The number of successfully completed checkpoints.
    #[polar(attribute)]
    pub job_nr_completed_checkpoints: i64,

    /// The number of failed checkpoints.
    #[polar(attribute)]
    pub job_nr_failed_checkpoints: i64,
}

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct FlowMetrics {
    // this will need to be in context:  historical_input_messages_per_sec: VecDeque<(f64, DateTime<Utc>)>,
    #[polar(attribute)]
    pub records_in_per_sec: f64,

    #[polar(attribute)]
    pub records_out_per_sec: f64,

    #[polar(attribute)]
    pub input_consumer_lag: f64,
}

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct ClusterMetrics {
    #[polar(attribute)]
    pub nr_task_managers: u16,

    /// The recent CPU usage of the JVM.
    #[polar(attribute)]
    pub task_cpu_load: f64,

    /// The amount of heap memory currently used (in bytes).
    #[polar(attribute)]
    pub task_heap_memory_used: f64,

    /// The amount of heap memory guaranteed to be available to the JVM (in bytes).
    #[polar(attribute)]
    pub task_heap_memory_committed: f64,

    /// The total number of live threads.
    #[polar(attribute)]
    pub nr_threads: i64,

    /// The number of queued input buffers.
    #[polar(attribute)]
    pub task_network_input_queue_len: i64,

    /// An estimate of the input buffers usage.
    #[polar(attribute)]
    pub task_network_input_pool_usage: i64,

    /// The number of queued input buffers.
    #[polar(attribute)]
    pub task_network_output_queue_len: i64,

    /// An estimate of the output buffers usage.
    #[polar(attribute)]
    pub task_network_output_pool_usage: i64,
}

#[cfg(test)]
use chrono::{DateTime, Utc};
#[cfg(test)]
use pretty_snowflake::{AlphabetCodec, IdPrettifier};
#[cfg(test)]
use proctor::IdGenerator;
#[cfg(test)]
use std::sync::Mutex;

#[cfg(test)]
lazy_static! {
    static ref ID_GENERATOR: Mutex<IdGenerator> =
        Mutex::new(IdGenerator::single_node(IdPrettifier::<AlphabetCodec>::default()));
}

impl MetricCatalog {
    #[cfg(test)]
    pub(crate) fn for_test_with_timestamp(timestamp: Timestamp, custom: telemetry::TableType) -> Self {
        let generator = &mut ID_GENERATOR.lock().unwrap();

        Self {
            correlation_id: generator.next_id(),
            timestamp,
            job_health: JobHealthMetrics::default(),
            flow: FlowMetrics::default(),
            cluster: ClusterMetrics::default(),
            custom,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test_with_datetime(timestamp: DateTime<Utc>, custom: telemetry::TableType) -> Self {
        Self::for_test_with_timestamp(timestamp.into(), custom)
    }

    // todo limited usefulness by itself; keys? iter support for custom and for entire catalog?
    pub fn custom<T: TryFrom<TelemetryValue>>(&self, metric: &str) -> Option<Result<T, TelemetryError>>
    where
        T: TryFrom<TelemetryValue>,
        TelemetryError: From<<T as TryFrom<TelemetryValue>>::Error>,
    {
        self.custom.get(metric).map(|telemetry| {
            let value = T::try_from(telemetry.clone())?;
            Ok(value)
        })
    }
}

impl Add for MetricCatalog {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let mut lhs = self.custom;
        lhs.extend(rhs.custom);
        MetricCatalog { custom: lhs, ..self }
    }
}

impl Add<&Self> for MetricCatalog {
    type Output = Self;

    fn add(self, rhs: &Self) -> Self::Output {
        let mut lhs = self.custom;
        lhs.extend(rhs.custom.clone());
        Self { custom: lhs, ..self }
    }
}

impl SubscriptionRequirements for MetricCatalog {
    fn required_fields() -> HashSet<proctor::SharedString> {
        maplit::hashset! {
            // JobHealthMetrics
            "job_uptime_millis".into(),
            "job_nr_restarts".into(),
            "job_nr_completed_checkpoints".into(),
            "job_nr_failed_checkpoints".into(),

            // FlowMetrics
            "records_in_per_sec".into(),
            "records_out_per_sec".into(),
            "input_consumer_lag".into(),

            // ClusterMetrics
            "nr_task_managers".into(),
            "task_cpu_load".into(),
            "task_heap_memory_used".into(),
            "task_heap_memory_committed".into(),
            "nr_threads".into(),
            "task_network_input_queue_len".into(),
            "task_network_input_pool_usage".into(),
            "task_network_output_queue_len".into(),
            "task_network_output_pool_usage".into(),
        }
    }
}

impl UpdateMetrics for MetricCatalog {
    fn update_metrics_for(name: SharedString) -> Box<dyn Fn(&str, &Telemetry) -> () + Send + Sync + 'static> {
        let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry
            .clone()
            .try_into::<MetricCatalog>()
        {
            Ok(catalog) => {
                METRIC_CATALOG_TIMESTAMP.set(catalog.timestamp.as_secs());

                METRIC_CATALOG_JOB_HEALTH_UPTIME.set(catalog.job_health.job_uptime_millis);
                METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS.set(catalog.job_health.job_nr_restarts);
                METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS.set(catalog.job_health.job_nr_completed_checkpoints);
                METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS.set(catalog.job_health.job_nr_failed_checkpoints);

                METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC.set(catalog.flow.records_in_per_sec);
                METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC.set(catalog.flow.records_out_per_sec);
                METRIC_CATALOG_FLOW_INPUT_CONSUMER_LAG.set(catalog.flow.input_consumer_lag);

                METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS.set(catalog.cluster.nr_task_managers as i64);
                METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD.set(catalog.cluster.task_cpu_load);
                METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED.set(catalog.cluster.task_heap_memory_used);
                METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED.set(catalog.cluster.task_heap_memory_committed);
                METRIC_CATALOG_CLUSTER_TASK_NR_THREADS.set(catalog.cluster.nr_threads);
                METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_QUEUE_LEN.set(catalog.cluster.task_network_input_queue_len);
                METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_POOL_USAGE.set(catalog.cluster.task_network_input_pool_usage);
                METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_QUEUE_LEN.set(catalog.cluster.task_network_output_queue_len);
                METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_POOL_USAGE
                    .set(catalog.cluster.task_network_output_pool_usage);
            }

            Err(err) => {
                tracing::warn!(
                    error=?err, phase_name=%name,
                    "failed to update data collection metrics on subscription: {}", subscription_name
                );
                proctor::track_errors(name.as_ref(), &ProctorError::CollectionError(err.into()));
            }
        };

        Box::new(update_fn)
    }
}

lazy_static! {
    pub(crate) static ref METRIC_CATALOG_TIMESTAMP: IntGauge = IntGauge::new(
        "metric_catalog_timestamp",
        "UNIX timestamp in seconds of last operational reading"
    )
    .expect("failed creating metric_catalog_timestamp metric");
    pub(crate) static ref METRIC_CATALOG_JOB_HEALTH_UPTIME: IntGauge = IntGauge::new(
        "metric_catalog_job_health_uptime",
        "The time that the job has been running without interruption."
    )
    .expect("failed creating metric_catalog_job_health_uptime metric");
    pub(crate) static ref METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS: IntGauge = IntGauge::new(
        "metric_catalog_job_health_nr_restarts",
        "The total number of restarts since this job was submitted, including full restarts and fine-grained restarts."
    )
    .expect("failed creating metric_catalog_job_health_nr_restarts metric");
    pub(crate) static ref METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS: IntGauge = IntGauge::new(
        "metric_catalog_job_health_nr_completed_checkpoints",
        "The number of successfully completed checkpoints."
    )
    .expect("failed creating metric_catalog_job_health_nr_completed_checkpoints metric");
    pub(crate) static ref METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS: IntGauge = IntGauge::new(
        "metric_catalog_job_health_nr_failed_checkpoints",
        "The number of failed checkpoints."
    )
    .expect("failed creating metric_catalog_job_health_nr_failed_checkpoints metric");
    pub(crate) static ref METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC: Gauge = Gauge::new(
        "metric_catalog_flow_records_in_per_sec",
        "Current records ingress per second"
    )
    .expect("failed creating metric_catalog_flow_records_in_per_sec metric");
    pub(crate) static ref METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC: Gauge = Gauge::new(
        "metric_catalog_flow_records_out_per_sec",
        "Current records egress per second"
    )
    .expect("failed creating metric_catalog_flow_records_out_per_sec metric");
    pub(crate) static ref METRIC_CATALOG_FLOW_INPUT_CONSUMER_LAG: Gauge = Gauge::new(
        "metric_catalog_flow_input_consumer_lag",
        "Current lag in handling messages from the ingress topic"
    )
    .expect("failed creating metric_catalog_flow_input_consumer_lag metric");
    pub(crate) static ref METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS: IntGauge = IntGauge::new(
        "metric_catalog_cluster_nr_task_managers",
        "Number of active task managers in the cluster"
    )
    .expect("failed creating metric_catalog_cluster_nr_task_managers metric");
    pub(crate) static ref METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD: Gauge = Gauge::new(
        "metric_catalog_cluster_task_cpu_load",
        "The recent CPU usage of the JVM."
    )
    .expect("failed creating metric_catalog_cluster_task_cpu_load metric");
    pub(crate) static ref METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED: Gauge = Gauge::new(
        "metric_catalog_cluster_task_heap_memory_used",
        "The amount of heap memory currently used (in bytes)."
    )
    .expect("failed creating metric_catalog_cluster_task_heap_memory_used metric");
    pub(crate) static ref METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED: Gauge = Gauge::new(
        "metric_catalog_cluster_task_heap_memory_committed",
        "The amount of heap memory guaranteed to be available to the JVM (in bytes)."
    )
    .expect("failed creating metric_catalog_cluster_task_heap_memory_committed metric");
    pub(crate) static ref METRIC_CATALOG_CLUSTER_TASK_NR_THREADS: IntGauge = IntGauge::new(
        "metric_catalog_cluster_task_nr_threads",
        "The total number of live threads."
    )
    .expect("failed creating metric_catalog_cluster_task_nr_threads metric");
    pub(crate) static ref METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_QUEUE_LEN: IntGauge = IntGauge::new(
        "metric_catalog_cluster_task_network_input_queue_len",
        "The number of queued input buffers."
    )
    .expect("failed creating metric_catalog_cluster_task_network_input_queue_len metric");
    pub(crate) static ref METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_POOL_USAGE: IntGauge = IntGauge::new(
        "metric_catalog_cluster_task_network_input_pool_usage",
        "An estimate of the input buffers usage. "
    )
    .expect("failed creating metric_catalog_cluster_task_network_input_pool_usage metric");
    pub(crate) static ref METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_QUEUE_LEN: IntGauge = IntGauge::new(
        "metric_catalog_cluster_task_network_output_queue_len",
        "The number of queued output buffers."
    )
    .expect("failed creating metric_catalog_cluster_task_network_output_queue_len metric");
    pub(crate) static ref METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_POOL_USAGE: IntGauge = IntGauge::new(
        "metric_catalog_cluster_task_network_output_pool_usage",
        "An estimate of the output buffers usage. "
    )
    .expect("failed creating metric_catalog_cluster_task_network_output_pool_usage metric");
}

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use chrono::TimeZone;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};

    use super::*;
    use proctor::elements::telemetry::ToTelemetry;
    use proctor::elements::Telemetry;
    use proctor::error::TypeExpectation;
    use proctor::phases::collection::{SUBSCRIPTION_CORRELATION, SUBSCRIPTION_TIMESTAMP};

    #[derive(PartialEq, Debug)]
    struct Bar(String);

    impl TryFrom<TelemetryValue> for Bar {
        type Error = TelemetryError;

        fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
            match value {
                TelemetryValue::Text(rep) => Ok(Bar(rep)),
                v => Err(TelemetryError::TypeError {
                    expected: format!("telementry value {}", TypeExpectation::Text),
                    actual: Some(format!("{:?}", v)),
                }),
            }
        }
    }
    // impl FromStr for Bar {
    //     type Err = ();
    //     fn from_str(s: &str) -> Result<Self, Self::Err> {
    //         Ok(Bar(s.to_string()))
    //     }
    // }

    #[test]
    fn test_custom_metric() {
        let cdata = maplit::hashmap! {
            "foo".to_string() => "17".to_telemetry(),
            "otis".to_string() => "Otis".to_telemetry(),
            "bar".to_string() => "Neo".to_telemetry(),
        };
        let data = MetricCatalog::for_test_with_datetime(Utc::now(), cdata);
        assert_eq!(data.custom::<i64>("foo").unwrap().unwrap(), 17_i64);
        assert_eq!(data.custom::<f64>("foo").unwrap().unwrap(), 17.0_f64);
        assert_eq!(data.custom::<String>("otis").unwrap().unwrap(), "Otis".to_string());
        assert_eq!(data.custom::<Bar>("bar").unwrap().unwrap(), Bar("Neo".to_string()));
        assert_eq!(data.custom::<String>("bar").unwrap().unwrap(), "Neo".to_string());
        assert!(data.custom::<i64>("zed").is_none());
    }

    #[test]
    fn test_metric_add() {
        let ts = Utc::now();
        let data = MetricCatalog::for_test_with_datetime(ts.clone(), std::collections::HashMap::default());
        let am1 = maplit::hashmap! {
            "foo.1".to_string() => "f-1".to_telemetry(),
            "bar.1".to_string() => "b-1".to_telemetry(),
        };
        let a1 = MetricCatalog::for_test_with_datetime(ts.clone(), am1.clone());
        let d1 = data.clone() + a1.clone();
        assert_eq!(d1.custom, am1);

        let am2 = maplit::hashmap! {
            "foo.2".to_string() => "f-2".to_telemetry(),
            "bar.2".to_string() => "b-2".to_telemetry(),
        };
        let a2 = MetricCatalog::for_test_with_datetime(ts.clone(), am2.clone());
        let d2 = d1.clone() + a2.clone();
        let mut exp2 = am1.clone();
        exp2.extend(am2.clone());
        assert_eq!(d2.custom, exp2);
    }

    lazy_static! {
        static ref CORR_ID: Id = Id::direct(12, "L");
        static ref CORR_ID_REP: &'static str = "L";
    }

    #[test]
    fn test_metric_catalog_serde() {
        let ts: Timestamp = Utc.ymd(1988, 5, 30).and_hms(9, 1, 17).into();
        let (ts_secs, ts_nsecs) = ts.as_pair();
        let metrics = MetricCatalog {
            correlation_id: CORR_ID.clone(),
            timestamp: ts,
            job_health: JobHealthMetrics {
                job_uptime_millis: 1_234_567,
                job_nr_restarts: 3,
                job_nr_completed_checkpoints: 12_345,
                job_nr_failed_checkpoints: 7,
            },
            flow: FlowMetrics {
                records_in_per_sec: 17.,
                input_consumer_lag: 3.14,
                records_out_per_sec: 0.0,
            },
            cluster: ClusterMetrics {
                nr_task_managers: 4,
                task_cpu_load: 0.65,
                task_heap_memory_used: 92_987_f64,
                task_heap_memory_committed: 103_929_920_f64,
                nr_threads: 8,
                task_network_input_queue_len: 12,
                task_network_input_pool_usage: 8,
                task_network_output_queue_len: 13,
                task_network_output_pool_usage: 5,
            },
            custom: maplit::hashmap! {
                "bar".to_string() => 33.to_telemetry(),
            },
        };

        assert_tokens(
            &metrics,
            &vec![
                Token::Map { len: None },
                Token::Str(SUBSCRIPTION_CORRELATION),
                Token::Struct { name: "Id", len: 2 },
                Token::Str("snowflake"),
                Token::I64(CORR_ID.clone().into()),
                Token::Str("pretty"),
                Token::Str(&CORR_ID_REP),
                Token::StructEnd,
                Token::Str(SUBSCRIPTION_TIMESTAMP),
                Token::TupleStruct { name: "Timestamp", len: 2 },
                Token::I64(ts_secs),
                Token::U32(ts_nsecs),
                Token::TupleStructEnd,
                Token::Str("job_uptime_millis"),
                Token::I64(1_234_567),
                Token::Str("job_nr_restarts"),
                Token::I64(3),
                Token::Str("job_nr_completed_checkpoints"),
                Token::I64(12_345),
                Token::Str("job_nr_failed_checkpoints"),
                Token::I64(7),
                Token::Str("records_in_per_sec"),
                Token::F64(17.),
                Token::Str("records_out_per_sec"),
                Token::F64(0.),
                Token::Str("input_consumer_lag"),
                Token::F64(3.14),
                Token::Str("nr_task_managers"),
                Token::U16(4),
                Token::Str("task_cpu_load"),
                Token::F64(0.65),
                Token::Str("task_heap_memory_used"),
                Token::F64(92_987.),
                Token::Str("task_heap_memory_committed"),
                Token::F64(103_929_920.),
                Token::Str("nr_threads"),
                Token::I64(8),
                Token::Str("task_network_input_queue_len"),
                Token::I64(12),
                Token::Str("task_network_input_pool_usage"),
                Token::I64(8),
                Token::Str("task_network_output_queue_len"),
                Token::I64(13),
                Token::Str("task_network_output_pool_usage"),
                Token::I64(5),
                Token::Str("bar"),
                Token::I64(33),
                Token::MapEnd,
            ],
        )
    }

    #[test]
    fn test_telemetry_from_metric_catalog() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_from_metric_catalog");
        let _main_span_guard = main_span.enter();

        let ts = Utc.ymd(1988, 5, 30).and_hms(9, 1, 17).into();
        let corr_id = Id::direct(17, "AB");
        let metrics = MetricCatalog {
            correlation_id: corr_id.clone(),
            timestamp: ts,
            job_health: JobHealthMetrics {
                job_uptime_millis: 1_234_567,
                job_nr_restarts: 3,
                job_nr_completed_checkpoints: 12_345,
                job_nr_failed_checkpoints: 7,
            },
            flow: FlowMetrics {
                records_in_per_sec: 17.,
                input_consumer_lag: 3.14,
                records_out_per_sec: 0.0,
            },
            cluster: ClusterMetrics {
                nr_task_managers: 4,
                task_cpu_load: 0.65,
                task_heap_memory_used: 92_987_f64,
                task_heap_memory_committed: 103_929_920_f64,
                nr_threads: 8,
                task_network_input_queue_len: 12,
                task_network_input_pool_usage: 8,
                task_network_output_queue_len: 13,
                task_network_output_pool_usage: 5,
            },
            custom: maplit::hashmap! {
                "foo".to_string() => "David".to_telemetry(),
                "bar".to_string() => 33.to_telemetry(),
            },
        };

        let telemetry = Telemetry::try_from(&metrics)?;
        let (ts_secs, ts_nsecs) = ts.as_pair();

        assert_eq!(
            telemetry,
            TelemetryValue::Table(maplit::hashmap! {
                SUBSCRIPTION_CORRELATION.to_string() => corr_id.to_telemetry(),
                SUBSCRIPTION_TIMESTAMP.to_string() => TelemetryValue::Seq(vec![ts_secs.to_telemetry(), ts_nsecs.to_telemetry(),]),
                "job_uptime_millis".to_string() => (1_234_567).to_telemetry(),
                "job_nr_restarts".to_string() => (3).to_telemetry(),
                "job_nr_completed_checkpoints".to_string() => (12_345).to_telemetry(),
                "job_nr_failed_checkpoints".to_string() => (7).to_telemetry(),

                "records_in_per_sec".to_string() => (17.).to_telemetry(),
                "records_out_per_sec".to_string() => (0.).to_telemetry(),
                "input_consumer_lag".to_string() => 3.14.to_telemetry(),

                "nr_task_managers".to_string() => 4.to_telemetry(),
                "task_cpu_load".to_string() => (0.65).to_telemetry(),
                "task_heap_memory_used".to_string() => (92_987.).to_telemetry(),
                "task_heap_memory_committed".to_string() => (103_929_920.).to_telemetry(),
                "nr_threads".to_string() => (8).to_telemetry(),
                "task_network_input_queue_len".to_string() => (12).to_telemetry(),
                "task_network_input_pool_usage".to_string() => (8).to_telemetry(),
                "task_network_output_queue_len".to_string() => (13).to_telemetry(),
                "task_network_output_pool_usage".to_string() => (5).to_telemetry(),

                "foo".to_string() => "David".to_telemetry(),
                "bar".to_string() => 33.to_telemetry(),
            }.into())
            .into()
        );

        Ok(())
    }

    // #[test]
    // fn test_metric_to_f64() {
    //     let expected = 3.14159_f64;
    //     let m: Metric<f64> = Metric::new("pi", expected);
    //
    //     let actual: f64 = m.into();
    //     assert_eq!(actual, expected);
    // }
}
