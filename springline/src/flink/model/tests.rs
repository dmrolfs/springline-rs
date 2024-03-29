use crate::flink::*;
use chrono::{DateTime, TimeZone, Utc};
use claim::*;
use once_cell::sync::Lazy;
use proctor::elements::{Telemetry, TelemetryType, TelemetryValue, ToTelemetry};
use proctor::error::TelemetryError;
use proctor::phases::sense::{SUBSCRIPTION_CORRELATION, SUBSCRIPTION_TIMESTAMP};
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde_test::{assert_tokens, Token};
use std::collections::HashMap;
use std::convert::TryFrom;
use trim_margin::MarginTrimmable;

pub trait ModuloSignedExt {
    fn modulo(&self, n: Self) -> Self;
}

macro_rules! modulo_signed_ext_impl {
    ($($t:ty)*) => ($(
        impl ModuloSignedExt for $t {
            #[inline]
            fn modulo(&self, n: Self) -> Self {
                (self % n + n) % n
            }
        }
    )*)
}
modulo_signed_ext_impl! { i8 i16 i32 i64 u32 }

mod catalog {
    use crate::model::NrReplicas;
    use crate::Env;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::{Id, Label, Labeling};
    use proctor::elements::{telemetry, Timestamp};
    use proctor::MetaData;

    use super::*;

    fn metrics_for_test_with_datetime(
        ts: DateTime<Utc>, custom: telemetry::TableType,
    ) -> Env<MetricCatalog> {
        Env::from_parts(
            MetaData::default().with_recv_timestamp(ts.into()),
            MetricCatalog {
                custom,
                health: JobHealthMetrics::default(),
                flow: FlowMetrics::default(),
                cluster: ClusterMetrics::default(),
            },
        )
    }

    fn get_custom_metric<T>(mc: &MetricCatalog, key: &str) -> Result<Option<T>, TelemetryError>
    where
        T: TryFrom<TelemetryValue>,
        TelemetryError: From<<T as TryFrom<TelemetryValue>>::Error>,
    {
        mc.custom
            .get(key)
            .map(|telemetry| {
                let value = T::try_from(telemetry.clone())?;
                Ok(value)
            })
            .transpose()
    }

    #[derive(PartialEq, Debug)]
    struct Bar(String);

    impl TryFrom<TelemetryValue> for Bar {
        type Error = TelemetryError;

        fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
            match value {
                TelemetryValue::Text(rep) => Ok(Bar(rep)),
                v => Err(TelemetryError::TypeError {
                    expected: TelemetryType::Text,
                    actual: Some(format!("{:?}", v)),
                }),
            }
        }
    }

    #[test]
    fn test_flow_task_utilization() {
        let mut flow = FlowMetrics {
            idle_time_millis_per_sec: 500.0,
            ..FlowMetrics::default()
        };
        assert_eq!(flow.task_utilization(), 0.5);

        flow.idle_time_millis_per_sec = 1000.0;
        assert_eq!(flow.task_utilization(), 0.0);

        flow.idle_time_millis_per_sec = 0.0;
        assert_eq!(flow.task_utilization(), 1.0);

        // by flink definition can't happen but want to reasonably handle:

        flow.idle_time_millis_per_sec = 201_312.7;
        assert_eq!(flow.task_utilization(), 0.0);

        flow.idle_time_millis_per_sec = -2.1;
        assert_eq!(flow.task_utilization(), 1.0);
    }

    #[test]
    fn test_invalid_type_serde_issue() {
        let mut telemetry = Telemetry::new();
        telemetry.insert(
            "cluster.task_cpu_load".to_string(),
            TelemetryValue::Float(0.025),
        );
        telemetry.insert(
            "cluster.task_heap_memory_used".to_string(),
            TelemetryValue::Float(2511508464.0),
        );
        telemetry.insert(
            "cluster.task_network_input_queue_len".to_string(),
            TelemetryValue::Float(1.0),
        );
        telemetry.insert(
            "health.job_uptime_millis".to_string(),
            TelemetryValue::Integer(201402),
        );
        telemetry.insert(
            "flow.forecasted_timestamp".to_string(),
            TelemetryValue::Float(Timestamp::new(1647307440, 378969192).as_secs_f64()),
        );
        telemetry.insert(
            "cluster.task_network_output_queue_len".to_string(),
            TelemetryValue::Float(1.0),
        );
        telemetry.insert(
            "cluster.nr_active_jobs".to_string(),
            TelemetryValue::Integer(0),
        );
        telemetry.insert(
            "health.job_nr_restarts".to_string(),
            TelemetryValue::Integer(0),
        );
        telemetry.insert(
            "health.job_max_parallelism".to_string(),
            TelemetryValue::Integer(4),
        );
        telemetry.insert(
            "health.job_source_max_parallelism".to_string(),
            TelemetryValue::Integer(4),
        );
        telemetry.insert(
            "health.job_nonsource_max_parallelism".to_string(),
            TelemetryValue::Integer(3),
        );
        telemetry.insert(
            "flow.records_in_per_sec".to_string(),
            TelemetryValue::Float(20.0),
        );
        telemetry.insert(
            "health.job_nr_completed_checkpoints".to_string(),
            TelemetryValue::Integer(0),
        );
        telemetry.insert(
            "cluster.task_network_output_pool_usage".to_string(),
            TelemetryValue::Float(0.1),
        );
        telemetry.insert(
            "flow.records_out_per_sec".to_string(),
            TelemetryValue::Float(19.966666666666665),
        );
        telemetry.insert(
            "flow.idle_time_millis_per_sec".to_string(),
            TelemetryValue::Float(555.5),
        );
        telemetry.insert(
            "flow.source_back_pressured_time_millis_per_sec".to_string(),
            TelemetryValue::Float(257.0),
        );
        telemetry.insert(
            "cluster.task_heap_memory_committed".to_string(),
            TelemetryValue::Float(3623878656.0),
        );
        telemetry.insert(
            "cluster.task_nr_threads".to_string(),
            TelemetryValue::Integer(57),
        );
        telemetry.insert(
            "cluster.task_network_input_pool_usage".to_string(),
            TelemetryValue::Float(0.0),
        );
        telemetry.insert(
            MC_CLUSTER__NR_TASK_MANAGERS.to_string(),
            TelemetryValue::Integer(5),
        );
        telemetry.insert(
            MC_CLUSTER__FREE_TASK_SLOTS.to_string(),
            TelemetryValue::Integer(0),
        );
        telemetry.insert(
            "health.job_nr_failed_checkpoints".to_string(),
            TelemetryValue::Integer(0),
        );
        telemetry.insert(
            "flow.forecasted_records_in_per_sec".to_string(),
            TelemetryValue::Float(21.4504261933966),
        );

        let actual: MetricCatalog = assert_ok!(telemetry.try_into());
        assert_eq!(
            actual,
            MetricCatalog {
                health: JobHealthMetrics {
                    job_max_parallelism: Parallelism::new(4),
                    job_source_max_parallelism: Parallelism::new(4),
                    job_nonsource_max_parallelism: Parallelism::new(3),
                    job_uptime_millis: Some(201402),
                    job_nr_restarts: Some(0),
                    job_nr_completed_checkpoints: Some(0),
                    job_nr_failed_checkpoints: Some(0),
                },
                flow: FlowMetrics {
                    records_in_per_sec: 20.0,
                    records_out_per_sec: 19.966666666666665,
                    idle_time_millis_per_sec: 555.5,
                    source_back_pressured_time_millis_per_sec: 257.0,
                    forecasted_timestamp: Some(Timestamp::new(1647307440, 378969192)),
                    forecasted_records_in_per_sec: Some(21.4504261933966),
                    source_records_lag_max: None,
                    source_assigned_partitions: None,
                    source_total_lag: None,
                    source_records_consumed_rate: None,
                    source_millis_behind_latest: None,
                },
                cluster: ClusterMetrics {
                    nr_active_jobs: 0,
                    nr_task_managers: NrReplicas::new(5),
                    free_task_slots: 0,
                    task_cpu_load: 0.025,
                    task_heap_memory_used: 2511508464.0,
                    task_heap_memory_committed: 3623878656.0,
                    task_nr_threads: 57,
                    task_network_input_queue_len: 1.0,
                    task_network_input_pool_usage: 0.0,
                    task_network_output_queue_len: 1.,
                    task_network_output_pool_usage: 0.1,
                },
                custom: HashMap::new(),
            }
        );
    }

    #[test]
    fn test_custom_metric() {
        let cdata = maplit::hashmap! {
            "foo".to_string() => "17".to_telemetry(),
            "otis".to_string() => "Otis".to_telemetry(),
            "bar".to_string() => "Neo".to_telemetry(),
        };
        let data = metrics_for_test_with_datetime(Utc::now(), cdata);
        assert_eq!(
            assert_some!(assert_ok!(get_custom_metric::<i64>(&data, "foo"))),
            17_i64
        );
        assert_eq!(
            assert_some!(assert_ok!(get_custom_metric::<f64>(&data, "foo"))),
            17.0_f64
        );
        assert_eq!(
            assert_some!(assert_ok!(get_custom_metric::<String>(&data, "otis"))),
            "Otis".to_string()
        );
        assert_eq!(
            assert_some!(assert_ok!(get_custom_metric::<Bar>(&data, "bar"))),
            Bar("Neo".to_string())
        );
        assert_eq!(
            assert_some!(assert_ok!(get_custom_metric::<String>(&data, "bar"))),
            "Neo".to_string()
        );
        assert_none!(assert_ok!(get_custom_metric::<i64>(&data, "zed")));
    }

    #[test]
    fn test_metric_add() {
        let ts = Utc::now();
        let data = metrics_for_test_with_datetime(ts.clone(), std::collections::HashMap::default());
        let am1 = maplit::hashmap! {
            "foo.1".to_string() => "f-1".to_telemetry(),
            "bar.1".to_string() => "b-1".to_telemetry(),
        };
        let a1 = metrics_for_test_with_datetime(ts.clone(), am1.clone());
        let d1 = data.clone() + a1.clone();
        assert_eq!(d1.custom, am1);

        let am2 = maplit::hashmap! {
            "foo.2".to_string() => "f-2".to_telemetry(),
            "bar.2".to_string() => "b-2".to_telemetry(),
        };
        let a2 = metrics_for_test_with_datetime(ts.clone(), am2.clone());
        let d2 = d1.clone() + a2.clone();
        let mut exp2 = am1.clone();
        exp2.extend(am2.clone());
        assert_eq!(d2.custom, exp2);
    }

    const CORR_ID_REP: &str = "L";
    static CORR_ID: Lazy<Id<MetricCatalog>> =
        Lazy::new(|| Id::direct("MetricCatalog", 12, CORR_ID_REP));

    #[test]
    fn test_metric_catalog_serde() {
        let ts: Timestamp =
            assert_some!(Utc.with_ymd_and_hms(1988, 5, 30, 9, 1, 17).single()).into();
        let (ts_secs, ts_nsecs) = ts.as_pair();
        let metrics = Env::from_parts(
            MetaData::from_parts(CORR_ID.clone(), ts),
            MetricCatalog {
                health: JobHealthMetrics {
                    job_max_parallelism: Parallelism::new(0),
                    job_source_max_parallelism: Parallelism::new(0),
                    job_nonsource_max_parallelism: Parallelism::new(0),
                    job_uptime_millis: Some(1_234_567),
                    job_nr_restarts: Some(3),
                    job_nr_completed_checkpoints: Some(12_345),
                    job_nr_failed_checkpoints: Some(7),
                },
                flow: FlowMetrics {
                    records_in_per_sec: 17.,
                    forecasted_timestamp: Some(ts),
                    forecasted_records_in_per_sec: Some(23.),
                    idle_time_millis_per_sec: 777.7,
                    source_back_pressured_time_millis_per_sec: 111.0,
                    source_records_lag_max: Some(314),
                    source_assigned_partitions: Some(2),
                    source_total_lag: Some(628),
                    source_records_consumed_rate: Some(471.0),
                    source_millis_behind_latest: None,
                    records_out_per_sec: 0.0,
                },
                cluster: ClusterMetrics {
                    nr_active_jobs: 1,
                    nr_task_managers: NrReplicas::new(4),
                    free_task_slots: 7,
                    task_cpu_load: 0.65,
                    task_heap_memory_used: 92_987_f64,
                    task_heap_memory_committed: 103_929_920_f64,
                    task_nr_threads: 8,
                    task_network_input_queue_len: 12.,
                    task_network_input_pool_usage: 8.,
                    task_network_output_queue_len: 13.,
                    task_network_output_pool_usage: 5.,
                },
                custom: maplit::hashmap! {
                    "bar".to_string() => 33.to_telemetry(),
                },
            },
        );

        assert_tokens(
            &metrics,
            &vec![
                Token::Struct { name: "Envelope", len: 2 },
                Token::Str("metadata"),
                Token::Struct { name: "MetaData", len: 2 },
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
                Token::StructEnd,
                Token::Str("content"),
                Token::Map { len: None },
                Token::Str("health.job_max_parallelism"),
                Token::U32(0),
                Token::Str("health.job_source_max_parallelism"),
                Token::U32(0),
                Token::Str("health.job_nonsource_max_parallelism"),
                Token::U32(0),
                Token::Str("health.job_uptime_millis"),
                Token::Some,
                Token::U32(1_234_567),
                Token::Str("health.job_nr_restarts"),
                Token::Some,
                Token::U32(3),
                Token::Str("health.job_nr_completed_checkpoints"),
                Token::Some,
                Token::U32(12_345),
                Token::Str("health.job_nr_failed_checkpoints"),
                Token::Some,
                Token::U32(7),
                Token::Str(MC_FLOW__RECORDS_IN_PER_SEC),
                Token::F64(17.),
                Token::Str("flow.records_out_per_sec"),
                Token::F64(0.),
                Token::Str("flow.idle_time_millis_per_sec"),
                Token::F64(777.7),
                Token::Str("flow.source_back_pressured_time_millis_per_sec"),
                Token::F64(111.0),
                Token::Str("flow.forecasted_timestamp"),
                Token::Some,
                Token::TupleStruct { name: "Timestamp", len: 2 },
                Token::I64(ts.as_pair().0),
                Token::U32(ts.as_pair().1),
                Token::TupleStructEnd,
                Token::Str("flow.forecasted_records_in_per_sec"),
                Token::Some,
                Token::F64(23.),
                Token::Str("flow.source_records_lag_max"),
                Token::Some,
                Token::U32(314),
                Token::Str("flow.source_assigned_partitions"),
                Token::Some,
                Token::U32(2),
                Token::Str("flow.source_total_lag"),
                Token::Some,
                Token::U32(628),
                Token::Str("flow.source_records_consumed_rate"),
                Token::Some,
                Token::F64(471.0),
                Token::Str(MC_CLUSTER__NR_ACTIVE_JOBS),
                Token::U32(1),
                Token::Str(MC_CLUSTER__NR_TASK_MANAGERS),
                Token::U32(4),
                Token::Str(MC_CLUSTER__FREE_TASK_SLOTS),
                Token::U32(7),
                Token::Str("cluster.task_cpu_load"),
                Token::F64(0.65),
                Token::Str("cluster.task_heap_memory_used"),
                Token::F64(92_987.),
                Token::Str("cluster.task_heap_memory_committed"),
                Token::F64(103_929_920.),
                Token::Str("cluster.task_nr_threads"),
                Token::U32(8),
                Token::Str("cluster.task_network_input_queue_len"),
                Token::F64(12.0),
                Token::Str("cluster.task_network_input_pool_usage"),
                Token::F64(8.0),
                Token::Str("cluster.task_network_output_queue_len"),
                Token::F64(13.0),
                Token::Str("cluster.task_network_output_pool_usage"),
                Token::F64(5.0),
                Token::Str("bar"),
                Token::I64(33),
                Token::MapEnd,
                Token::StructEnd,
            ],
        )
    }

    #[test]
    fn test_telemetry_from_metric_catalog() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_from_metric_catalog");
        let _main_span_guard = main_span.enter();

        let ts = assert_some!(Utc.with_ymd_and_hms(1988, 5, 30, 9, 1, 17).single()).into();
        let corr_id = Id::direct("MetricCatalog", 17, "AB");
        let metrics = Env::from_parts(
            MetaData::from_parts(corr_id.clone(), ts),
            MetricCatalog {
                health: JobHealthMetrics {
                    job_max_parallelism: Parallelism::new(12),
                    job_source_max_parallelism: Parallelism::new(12),
                    job_nonsource_max_parallelism: Parallelism::new(9),
                    job_uptime_millis: Some(1_234_567),
                    job_nr_restarts: Some(3),
                    job_nr_completed_checkpoints: Some(12_345),
                    job_nr_failed_checkpoints: Some(7),
                },
                flow: FlowMetrics {
                    records_in_per_sec: 17.,
                    idle_time_millis_per_sec: 333.3,
                    source_back_pressured_time_millis_per_sec: 928.0,
                    forecasted_timestamp: None,
                    forecasted_records_in_per_sec: None,
                    source_records_lag_max: Some(314),
                    source_assigned_partitions: Some(3),
                    source_total_lag: Some(1_042),
                    source_records_consumed_rate: Some(521.0),
                    source_millis_behind_latest: None,
                    records_out_per_sec: 0.0,
                },
                cluster: ClusterMetrics {
                    nr_active_jobs: 1,
                    nr_task_managers: NrReplicas::new(4),
                    free_task_slots: 11,
                    task_cpu_load: 0.65,
                    task_heap_memory_used: 92_987_f64,
                    task_heap_memory_committed: 103_929_920_f64,
                    task_nr_threads: 8,
                    task_network_input_queue_len: 12.,
                    task_network_input_pool_usage: 8.,
                    task_network_output_queue_len: 13.,
                    task_network_output_pool_usage: 5.,
                },
                custom: maplit::hashmap! {
                    "foo".to_string() => "David".to_telemetry(),
                    "bar".to_string() => 33.to_telemetry(),
                },
            },
        );

        let telemetry = Telemetry::try_from(metrics.as_ref())?;

        assert_eq!(
            telemetry,
            TelemetryValue::Table(maplit::hashmap! {
                "health.job_max_parallelism".to_string() => 12.to_telemetry(),
                "health.job_source_max_parallelism".to_string() => 12.to_telemetry(),
                "health.job_nonsource_max_parallelism".to_string() => 9.to_telemetry(),
                "health.job_uptime_millis".to_string() => (1_234_567).to_telemetry(),
                "health.job_nr_restarts".to_string() => (3).to_telemetry(),
                "health.job_nr_completed_checkpoints".to_string() => (12_345).to_telemetry(),
                "health.job_nr_failed_checkpoints".to_string() => (7).to_telemetry(),

                MC_FLOW__RECORDS_IN_PER_SEC.to_string() => (17.).to_telemetry(),
                "flow.records_out_per_sec".to_string() => (0.).to_telemetry(),
                "flow.source_records_lag_max".to_string() => 314.to_telemetry(),
                "flow.source_assigned_partitions".to_string() => 3.to_telemetry(),
                "flow.source_total_lag".to_string() => 1_042.to_telemetry(),
                "flow.source_records_consumed_rate".to_string() => 521_f64.to_telemetry(),
                "flow.idle_time_millis_per_sec".to_string() => 333.3.to_telemetry(),
                "flow.source_back_pressured_time_millis_per_sec".to_string() => 928_f64.to_telemetry(),

                MC_CLUSTER__NR_ACTIVE_JOBS.to_string() => 1.to_telemetry(),
                MC_CLUSTER__NR_TASK_MANAGERS.to_string() => 4.to_telemetry(),
                MC_CLUSTER__FREE_TASK_SLOTS.to_string() => 11.to_telemetry(),
                "cluster.task_cpu_load".to_string() => (0.65).to_telemetry(),
                "cluster.task_heap_memory_used".to_string() => (92_987.).to_telemetry(),
                "cluster.task_heap_memory_committed".to_string() => (103_929_920.).to_telemetry(),
                "cluster.task_nr_threads".to_string() => (8).to_telemetry(),
                "cluster.task_network_input_queue_len".to_string() => (12.0).to_telemetry(),
                "cluster.task_network_input_pool_usage".to_string() => (8.0).to_telemetry(),
                "cluster.task_network_output_queue_len".to_string() => (13.0).to_telemetry(),
                "cluster.task_network_output_pool_usage".to_string() => (5.0).to_telemetry(),

                "foo".to_string() => "David".to_telemetry(),
                "bar".to_string() => 33.to_telemetry(),
            }.into())
                .into()
        );

        Ok(())
    }

    #[tracing::instrument(level = "info")]
    pub fn make_test_catalog(ts: Timestamp, value: u32) -> Env<MetricCatalog> {
        Env::from_parts(
            MetaData::from_parts(
                Id::direct(
                    <MetricCatalog as Label>::labeler().label(),
                    ts.as_secs(),
                    ts.as_secs_f64().to_string(),
                ),
                ts,
            ),
            MetricCatalog {
                health: JobHealthMetrics::default(),
                flow: FlowMetrics {
                    records_in_per_sec: f64::from(value),
                    idle_time_millis_per_sec: f64::from(value.modulo(1_000)),
                    ..FlowMetrics::default()
                },
                cluster: ClusterMetrics::default(),
                custom: HashMap::new(),
            },
        )
    }
}

mod window {
    use std::time::Duration;

    use crate::Env;
    use approx::assert_relative_eq;
    use frunk::{Monoid, Semigroup};
    use pretty_assertions::assert_eq;
    use pretty_snowflake::Label;
    use proctor::elements::Timestamp;
    use proctor::{Correlation, ReceivedAt};

    use super::catalog::make_test_catalog;
    use super::*;

    #[tracing::instrument(level = "info", skip(catalogs))]
    fn make_test_window(
        limit: usize, interval: Duration, catalogs: &[Env<MetricCatalog>],
    ) -> (AppDataWindow<Env<MetricCatalog>>, Vec<Env<MetricCatalog>>) {
        let mut window = AppDataWindow::builder()
            .with_size_and_interval(limit, interval)
            .with_quorum_percentile(0.5);
        let mut used = Vec::new();
        let mut remaining = Vec::new();
        for c in catalogs {
            if used.len() < limit {
                used.push(c.clone());
            } else {
                remaining.push(c.clone());
            }
        }

        for c in used {
            tracing::debug!(window_len=%(window.len()+1), catalog=%c.correlation(),"pushing catalog into window.");
            window.push_item(c);
        }

        (assert_ok!(window.build()), remaining)
    }

    fn extract_inner<T: Label + Send>(ds: Vec<Env<T>>) -> Vec<T> {
        ds.into_iter().map(|d| d.into_inner()).collect()
    }

    #[test]
    fn test_window_invariants() {
        let window = AppDataWindow::<MetricCatalog>::builder()
            .with_time_window(Duration::from_secs(1))
            .with_quorum_percentile(0.5)
            .build();
        assert_err!(window, "window is empty");

        let window = AppDataWindow::<MetricCatalog>::builder()
            .with_time_window(Duration::from_secs(1))
            .with_quorum_percentile(0.0)
            .with_item(MetricCatalog::empty())
            .build();
        assert_err!(window, "zero sufficient coverage");

        let window = AppDataWindow::<MetricCatalog>::builder()
            .with_time_window(Duration::from_secs(1))
            .with_quorum_percentile(1.0)
            .with_item(MetricCatalog::empty())
            .build();
        assert_ok!(window);

        let window = AppDataWindow::<MetricCatalog>::builder()
            .with_time_window(Duration::from_secs(1))
            .with_quorum_percentile(-0.07)
            .with_item(MetricCatalog::empty())
            .build();
        assert_err!(window, "negative coverage");

        let window = AppDataWindow::<MetricCatalog>::builder()
            .with_time_window(Duration::from_secs(1))
            .with_quorum_percentile(1.00003)
            .with_item(MetricCatalog::empty())
            .build();
        assert_err!(window, "impossible sufficient coverage required");
    }

    #[test]
    fn test_metric_window_head() {
        let m1 = make_test_catalog(Timestamp::new(1, 0), 1);
        let m2 = make_test_catalog(Timestamp::new(2, 0), 2);

        let mut window = AppDataWindow::from_size(m1.clone(), 3, Duration::from_secs(1));
        let (actual_data, actual_ts) = window.latest_entry();
        assert_eq!(actual_ts, &m1.recv_timestamp());
        assert_eq!(actual_data, m1.as_ref());

        window.push(m2.clone());
        let (actual_data, actual_ts) = window.latest_entry();
        assert_eq!(actual_ts, &m2.recv_timestamp());
        assert_eq!(actual_data, m2.as_ref());
    }

    #[test]
    fn test_metric_window_combine() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_metric_window_combine");
        let _main_span_guard = main_span.enter();

        let interval = Duration::from_secs(1);

        let m1 = make_test_catalog(Timestamp::new(1, 0), 1);
        let m2 = make_test_catalog(Timestamp::new(2, 0), 2);
        let m3 = make_test_catalog(Timestamp::new(3, 0), 3);
        let m4 = make_test_catalog(Timestamp::new(4, 0), 4);
        let m5 = make_test_catalog(Timestamp::new(5, 0), 5);
        let m6 = make_test_catalog(Timestamp::new(6, 0), 6);
        let m7 = make_test_catalog(Timestamp::new(7, 0), 7);
        let ms = [
            m1.clone(),
            m2.clone(),
            m3.clone(),
            m4.clone(),
            m5.clone(),
            m6.clone(),
            m7.clone(),
        ];

        let (port_1, remaining) = make_test_window(3, interval, &ms);
        assert_eq!(port_1.len(), 3);
        assert_eq!(
            port_1.clone().into_iter().collect::<Vec<_>>(),
            extract_inner(vec![m1.clone(), m2.clone(), m3.clone()])
        );
        let (port_2, _) = make_test_window(4, interval, &remaining);
        assert_eq!(port_2.len(), 4);
        assert_eq!(
            port_2.clone().into_iter().collect::<Vec<_>>(),
            extract_inner(vec![m4.clone(), m5.clone(), m6.clone(), m7.clone()])
        );
        let combined = port_1.clone().combine(&port_2);
        assert_eq!(combined.time_window, port_1.time_window);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(
            actual,
            extract_inner(vec![m4.clone(), m5.clone(), m6.clone(), m7.clone()])
        );
        let combined = port_2.combine(&port_1);
        assert_eq!(combined.time_window, port_2.time_window);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(
            actual,
            extract_inner(vec![
                m3.clone(),
                m4.clone(),
                m5.clone(),
                m6.clone(),
                m7.clone()
            ])
        );

        let (port_3, remaining) = make_test_window(4, interval, &ms);
        let (port_4, _) = make_test_window(2, interval, &remaining);
        let combined = port_3.clone().combine(&port_4);
        assert_eq!(combined.time_window, port_3.time_window);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(
            actual,
            extract_inner(vec![
                m2.clone(),
                m3.clone(),
                m4.clone(),
                m5.clone(),
                m6.clone()
            ])
        );
        let combined = port_4.clone().combine(&port_3);
        assert_eq!(combined.time_window, port_4.time_window);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(
            actual,
            extract_inner(vec![m4.clone(), m5.clone(), m6.clone()])
        );
    }

    #[test]
    fn test_metric_window_shuffled_combine() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_metric_window_shuffled_combine");
        let _main_span_guard = main_span.enter();

        let interval = Duration::from_secs(1);

        let m1 = make_test_catalog(Timestamp::new(1, 0), 1);
        let m2 = make_test_catalog(Timestamp::new(2, 0), 2);
        let m3 = make_test_catalog(Timestamp::new(3, 0), 3);
        let m4 = make_test_catalog(Timestamp::new(4, 0), 4);
        let m5 = make_test_catalog(Timestamp::new(5, 0), 5);
        let m6 = make_test_catalog(Timestamp::new(6, 0), 6);
        let m7 = make_test_catalog(Timestamp::new(7, 0), 7);
        let ms = [
            m1.clone(),
            m2.clone(),
            m3.clone(),
            m4.clone(),
            m5.clone(),
            m6.clone(),
            m7.clone(),
        ];
        let mut shuffled = ms.to_vec();
        shuffled.shuffle(&mut thread_rng());
        assert_ne!(shuffled.as_slice(), &ms);

        let (port_1, remaining) = make_test_window(3, interval, &shuffled);
        let (port_2, _) = make_test_window(4, interval, &remaining);
        let combined = port_1.clone().combine(&port_2);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(
            actual,
            extract_inner(vec![m4.clone(), m5.clone(), m6.clone(), m7.clone()])
        );
        let combined = port_2.clone().combine(&port_1);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(
            actual,
            extract_inner(vec![
                m3.clone(),
                m4.clone(),
                m5.clone(),
                m6.clone(),
                m7.clone()
            ])
        );

        let (port_3, remaining) = make_test_window(6, interval, &shuffled);
        assert_eq!(port_3.len(), 6);
        let (port_4, _) = make_test_window(1, interval, &remaining);
        assert_eq!(port_4.len(), 1);
        let combined = port_3.clone().combine(&port_4);
        tracing::debug!(
            lhs=?port_3.history().map(|p| p.recv_timestamp().to_string()).collect::<Vec<_>>(),
            rhs=?port_4.history().map(|p| p.recv_timestamp().to_string()).collect::<Vec<_>>(),
            combined=?combined.history().map(|p| p.recv_timestamp().to_string()).collect::<Vec<_>>(),
            "port_3.combine(port_4)"
        );
        assert_eq!(combined.len(), port_3.len() + 1);
        assert_eq!(
            assert_some!(combined.window_interval()).duration(),
            port_3.time_window
        );
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(
            actual,
            extract_inner(vec![
                m1.clone(),
                m2.clone(),
                m3.clone(),
                m4.clone(),
                m5.clone(),
                m6.clone(),
                m7.clone()
            ])
        );
        let combined = port_4.clone().combine(&port_3);
        assert_eq!(combined.len(), port_4.len() + 1);
        assert_eq!(
            assert_some!(combined.window_interval()).duration(),
            port_4.time_window
        );
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(actual, extract_inner(vec![m6.clone(), m7.clone()]));
    }

    #[test]
    fn test_metric_window_for_period() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_metric_window_for_period");
        let _main_span_guard = main_span.enter();

        let interval = Duration::from_secs(10);

        let now = Timestamp::now();
        // flow.records_in_per_sec *decline* over time
        let m1 = make_test_catalog(now - Duration::from_secs(1 * 10), 1);
        let m2 = make_test_catalog(now - Duration::from_secs(2 * 10), 2);
        let m3 = make_test_catalog(now - Duration::from_secs(3 * 10), 3);
        let m4 = make_test_catalog(now - Duration::from_secs(4 * 10), 4);
        let m5 = make_test_catalog(now - Duration::from_secs(5 * 10), 5);
        let m6 = make_test_catalog(now - Duration::from_secs(6 * 10), 6);
        let m7 = make_test_catalog(now - Duration::from_secs(7 * 10), 7);
        let ms = [
            m1.clone(),
            m2.clone(),
            m3.clone(),
            m4.clone(),
            m5.clone(),
            m6.clone(),
            m7.clone(),
        ];

        let f = |(c, ts): &(MetricCatalog, Timestamp)| {
            tracing::debug!(
                "[test] testing catalog[{}]: ({} <= {}) is {}",
                ts,
                c.flow.records_in_per_sec,
                5.0,
                c.flow.records_in_per_sec <= 5.0,
            );
            c.flow.records_in_per_sec <= 5.0
        };

        let (window, _) = make_test_window(10, interval, &ms);
        tracing::info!(
            window=?window.history().map(|m| (m.recv_timestamp().to_string(), m.0.flow.records_in_per_sec)).collect::<Vec<_>>(),
            "*** WINDOW CREATED"
        );

        tracing::info_span!("looking back for 5 seconds").in_scope(|| {
            assert_eq!(
                window.for_duration_from_head(Duration::from_secs(5), f),
                false
            );
        });

        tracing::info_span!("looking back for 11 seconds").in_scope(|| {
            assert_eq!(
                window.for_duration_from_head(Duration::from_secs(11), f),
                true
            );
        });

        tracing::info_span!("looking back for 21 seconds").in_scope(|| {
            assert_eq!(
                window.for_duration_from_head(Duration::from_secs(21), f),
                true
            );
        });

        tracing::info_span!("looking back for 31 seconds").in_scope(|| {
            assert_eq!(
                window.for_duration_from_head(Duration::from_secs(31), f),
                true
            );
        });

        tracing::info_span!("looking back for 41 seconds").in_scope(|| {
            assert_eq!(
                window.for_duration_from_head(Duration::from_secs(41), f),
                true
            );
        });

        tracing::info_span!("looking back for 51 seconds").in_scope(|| {
            assert_eq!(
                window.for_duration_from_head(Duration::from_secs(51), f),
                false
            );
        });

        tracing::info_span!("looking back for 61 seconds").in_scope(|| {
            assert_eq!(
                window.for_duration_from_head(Duration::from_secs(61), f),
                false
            );
        });

        tracing::info_span!("looking back for 71 seconds").in_scope(|| {
            assert_eq!(
                window.for_duration_from_head(Duration::from_secs(71), f),
                false
            );
        });

        tracing::info_span!("looking back for 2000 seconds").in_scope(|| {
            assert_eq!(
                window.for_duration_from_head(Duration::from_secs(2000), f),
                false
            );
        });
    }

    #[test]
    fn test_flow_task_utilization_rolling_average() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flow_task_utilization_rolling_average");
        let _main_span_guard = main_span.enter();

        let interval = Duration::from_secs(10);

        let now = Timestamp::now();
        // flow.records_in_per_sec *decline* over time
        let m1 = make_test_catalog(now - Duration::from_secs(1 * 10), 100);
        let m2 = make_test_catalog(now - Duration::from_secs(2 * 10), 200);
        let m3 = make_test_catalog(now - Duration::from_secs(3 * 10), 300);
        let m4 = make_test_catalog(now - Duration::from_secs(4 * 10), 400);
        let m5 = make_test_catalog(now - Duration::from_secs(5 * 10), 500);
        let m6 = make_test_catalog(now - Duration::from_secs(6 * 10), 600);
        let m7 = make_test_catalog(now - Duration::from_secs(7 * 10), 700);
        let ms = [
            m1.clone(),
            m2.clone(),
            m3.clone(),
            m4.clone(),
            m5.clone(),
            m6.clone(),
            m7.clone(),
        ];

        let (window, remaining) = make_test_window(10, interval, &ms);
        assert!(remaining.is_empty());

        tracing::info_span!("looking back for 5 seconds").in_scope(|| {
            assert_relative_eq!(
                window.flow_task_utilization_rolling_average(5),
                0.9,
                epsilon = 1.0e-10
            );
        });

        tracing::info_span!("looking back for 11 seconds").in_scope(|| {
            assert_relative_eq!(
                window.flow_task_utilization_rolling_average(11),
                0.85,
                epsilon = 1.0e-10
            );
        });

        tracing::info_span!("looking back for 21 seconds").in_scope(|| {
            assert_relative_eq!(
                window.flow_task_utilization_rolling_average(21),
                0.8,
                epsilon = 1.0e-10
            );
        });

        tracing::info_span!("looking back for 31 seconds").in_scope(|| {
            assert_relative_eq!(
                window.flow_task_utilization_rolling_average(31),
                0.75,
                epsilon = 1.0e-10
            );
        });

        tracing::info_span!("looking back for 41 seconds").in_scope(|| {
            assert_relative_eq!(
                window.flow_task_utilization_rolling_average(41),
                0.7,
                epsilon = 1.0e-10
            );
        });

        tracing::info_span!("looking back for 51 seconds").in_scope(|| {
            assert_relative_eq!(
                window.flow_task_utilization_rolling_average(51),
                0.65,
                epsilon = 1.0e-10
            );
        });

        tracing::info_span!("looking back for 61 seconds").in_scope(|| {
            assert_relative_eq!(
                window.flow_task_utilization_rolling_average(61),
                0.6,
                epsilon = 1.0e-10
            );
        });

        tracing::info_span!("looking back for 75 seconds").in_scope(|| {
            assert_relative_eq!(
                window.flow_task_utilization_rolling_average(71),
                0.6,
                epsilon = 1.0e-10
            );
        });
    }
}

mod vertex {
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use proctor::elements::Timestamp;

    use super::*;

    #[test]
    fn test_job_vertices_parsing() {
        let jobs_json = r##"|{
        |    "jobs": [
        |        { "id": "0771e8332dc401d254a140a707169a48", "status": "RUNNING" },
        |        { "id": "08734e8332dc401d254a140a707169c98", "status": "CANCELLING" }
        |    ]
        |}"##
            .trim_margin_with("|")
            .unwrap();

        let my_jobs: serde_json::Value = assert_ok!(serde_json::from_str(&jobs_json));
        let jobs: Vec<JobSummary> = match my_jobs["jobs"].as_array() {
            Some(js) => js.iter().map(|j| assert_ok!(serde_json::from_value(j.clone()))).collect(),
            None => Vec::default(),
        };

        assert_eq!(
            jobs,
            vec![
                JobSummary {
                    id: JobId::new("0771e8332dc401d254a140a707169a48"),
                    status: JobState::Running,
                },
                JobSummary {
                    id: JobId::new("08734e8332dc401d254a140a707169c98"),
                    status: JobState::Cancelling,
                },
            ]
        );
    }

    #[test]
    fn test_vertex_detail_deser() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_vertex_detail_deser");
        let _ = main_span.enter();

        let json_rep = r##"|{
        |    "id": "cbc357ccb763df2852fee8c4fc7d55f2",
        |    "name": "Source: Custom Source -> Timestamps/Watermarks",
        |    "maxParallelism": 128,
        |    "parallelism": 1,
        |    "status": "RUNNING",
        |    "start-time": 1638989054310,
        |    "end-time": -1,
        |    "duration": 35010565,
        |    "tasks": {
        |        "SCHEDULED": 0,
        |        "FINISHED": 0,
        |        "FAILED": 0,
        |        "CANCELING": 0,
        |        "CANCELED": 0,
        |        "DEPLOYING": 0,
        |        "RECONCILING": 0,
        |        "CREATED": 0,
        |        "RUNNING": 1,
        |        "INITIALIZING": 0
        |    },
        |    "metrics": {
        |        "read-bytes": 0,
        |        "read-bytes-complete": true,
        |        "write-bytes": 32768,
        |        "write-bytes-complete": true,
        |        "read-records": 0,
        |        "read-records-complete": true,
        |        "write-records": 1012,
        |        "write-records-complete": true
        |    }
        |}"##
            .trim_margin_with("|")
            .unwrap();

        let actual: VertexDetail = assert_ok!(serde_json::from_str(&json_rep));
        assert_eq!(
            actual,
            VertexDetail {
                id: VertexId::new("cbc357ccb763df2852fee8c4fc7d55f2"),
                name: "Source: Custom Source -> Timestamps/Watermarks".to_string(),
                max_parallelism: Some(128),
                parallelism: 1,
                status: TaskState::Running,
                start_time: Timestamp::new(1638989054, 310 * 1_000_000),
                end_time: None,
                duration: Some(Duration::from_millis(35010565)),
                tasks: maplit::hashmap! {
                    TaskState::Scheduled => 0,
                    TaskState::Finished => 0,
                    TaskState::Failed => 0,
                    TaskState::Canceling => 0,
                    TaskState::Canceled => 0,
                    TaskState::Deploying => 0,
                    TaskState::Reconciling => 0,
                    TaskState::Created => 0,
                    TaskState::Running => 1,
                    TaskState::Initializing => 0,
                },
                metrics: maplit::hashmap! {
                    "read-bytes".to_string() => 0_i64.into(),
                    "read-bytes-complete".to_string() => true.into(),
                    "write-bytes".to_string() => 32768_i64.into(),
                    "write-bytes-complete".to_string() => true.into(),
                    "read-records".to_string() => 0_i64.into(),
                    "read-records-complete".to_string() => true.into(),
                    "write-records".to_string() => 1012_i64.into(),
                    "write-records-complete".to_string() => true.into(),
                },
            }
        )
    }
}

mod job_detail {
    use std::time::Duration;

    use crate::flink::model::{JobPlan, JobPlanItem, PlanItemInput};
    use pretty_assertions::assert_eq;
    use proctor::elements::Timestamp;

    use super::*;

    #[test]
    fn test_job_detail_deser() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_job_detail_deser");
        let _ = main_span.enter();

        let json_rep = r##"|{
        |   "jid": "0771e8332dc401d254a140a707169a48",
        |    "name": "CarTopSpeedWindowingExample",
        |    "isStoppable": false,
        |    "state": "RUNNING",
        |    "start-time": 1638989050332,
        |    "end-time": -1,
        |    "duration": 35014543,
        |    "maxParallelism": -1,
        |    "now": 1639024064875,
        |    "timestamps": {
        |        "CREATED": 1638989050521,
        |        "FAILED": 0,
        |        "RUNNING": 1638989054173,
        |        "CANCELED": 0,
        |        "CANCELLING": 0,
        |        "SUSPENDED": 0,
        |        "FAILING": 0,
        |        "RESTARTING": 0,
        |        "FINISHED": 0,
        |        "INITIALIZING": 1638989050332,
        |        "RECONCILING": 0
        |    },
        |    "vertices": [
        |        {
        |            "id": "cbc357ccb763df2852fee8c4fc7d55f2",
        |            "name": "Source: Custom Source -> Timestamps/Watermarks",
        |            "maxParallelism": 128,
        |            "parallelism": 1,
        |            "status": "RUNNING",
        |            "start-time": 1638989054310,
        |            "end-time": -1,
        |            "duration": 35010565,
        |            "tasks": {
        |                "SCHEDULED": 0,
        |                "FINISHED": 0,
        |                "FAILED": 0,
        |                "CANCELING": 0,
        |                "CANCELED": 0,
        |                "DEPLOYING": 0,
        |                "RECONCILING": 0,
        |                "CREATED": 0,
        |                "RUNNING": 1,
        |                "INITIALIZING": 0
        |            },
        |            "metrics": {
        |                "read-bytes": 0,
        |                "read-bytes-complete": true,
        |                "write-bytes": 32768,
        |                "write-bytes-complete": true,
        |                "read-records": 0,
        |                "read-records-complete": true,
        |                "write-records": 1012,
        |                "write-records-complete": true
        |            }
        |        },
        |        {
        |            "id": "90bea66de1c231edf33913ecd54406c1",
        |            "name": "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, PassThroughWindowFunction) -> Sink: Print to Std. Out",
        |            "maxParallelism": 128,
        |            "parallelism": 1,
        |            "status": "RUNNING",
        |            "start-time": 1638989054320,
        |            "end-time": -1,
        |            "duration": 35010555,
        |            "tasks": {
        |                "SCHEDULED": 0,
        |                "FINISHED": 0,
        |                "FAILED": 0,
        |                "CANCELING": 0,
        |                "CANCELED": 0,
        |                "DEPLOYING": 0,
        |                "RECONCILING": 0,
        |                "CREATED": 0,
        |                "RUNNING": 1,
        |                "INITIALIZING": 0
        |            },
        |            "metrics": {
        |                "read-bytes": 40637,
        |                "read-bytes-complete": true,
        |                "write-bytes": 0,
        |                "write-bytes-complete": true,
        |                "read-records": 1010,
        |                "read-records-complete": true,
        |                "write-records": 0,
        |                "write-records-complete": true
        |            }
        |        }
        |    ],
        |    "status-counts": {
        |        "SCHEDULED": 0,
        |        "FINISHED": 0,
        |        "FAILED": 0,
        |        "CANCELING": 0,
        |        "CANCELED": 0,
        |        "DEPLOYING": 0,
        |        "RECONCILING": 0,
        |        "CREATED": 0,
        |        "RUNNING": 2,
        |        "INITIALIZING": 0
        |    },
        |    "plan": {
        |        "jid": "0771e8332dc401d254a140a707169a48",
        |        "name": "CarTopSpeedWindowingExample",
        |        "nodes": [
        |            {
        |                "id": "90bea66de1c231edf33913ecd54406c1",
        |                "parallelism": 1,
        |                "operator": "",
        |                "operator_strategy": "",
        |                "description": "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, PassThroughWindowFunction) -&gt; Sink: Print to Std. Out",
        |                "inputs": [
        |                    {
        |                        "num": 0,
        |                        "id": "cbc357ccb763df2852fee8c4fc7d55f2",
        |                        "ship_strategy": "HASH",
        |                        "exchange": "pipelined_bounded"
        |                    }
        |                ],
        |                "optimizer_properties": {}
        |            },
        |            {
        |                "id": "cbc357ccb763df2852fee8c4fc7d55f2",
        |                "parallelism": 1,
        |                "operator": "",
        |                "operator_strategy": "",
        |                "description": "Source: Custom Source -&gt; Timestamps/Watermarks",
        |                "optimizer_properties": {}
        |            }
        |        ]
        |    }
        |}"##.trim_margin_with("|").unwrap();

        let actual: JobDetail = assert_ok!(serde_json::from_str(&json_rep));
        assert_eq!(
            actual,
            JobDetail {
                jid: JobId::new("0771e8332dc401d254a140a707169a48"),
                name: "CarTopSpeedWindowingExample".to_string(),
                is_stoppable: false,
                state: JobState::Running,
                start_time: Timestamp::new(1638989050, 332_000_000),
                end_time: None,
                duration: Some(Duration::from_millis(35014543)),
                max_parallelism: None,
                now: Timestamp::new(1639024064, 875_000_000),
                timestamps: maplit::hashmap! {
                    JobState::Created => Timestamp::new(1638989050, 521_000_000),
                    JobState::Failed => Timestamp::ZERO,
                    JobState::Running => Timestamp::new(1638989054, 173_000_000),
                    JobState::Canceled => Timestamp::ZERO,
                    JobState::Cancelling => Timestamp::ZERO,
                    JobState::Suspended => Timestamp::ZERO,
                    JobState::Failing => Timestamp::ZERO,
                    JobState::Restarting => Timestamp::ZERO,
                    JobState::Finished => Timestamp::ZERO,
                    JobState::Initializing => Timestamp::new(1638989050, 332_000_000),
                    JobState::Reconciling => Timestamp::ZERO,
                },
                vertices: vec![
                    VertexDetail {
                        id: VertexId::new("cbc357ccb763df2852fee8c4fc7d55f2"),
                        name: "Source: Custom Source -> Timestamps/Watermarks".to_string(),
                        max_parallelism: Some(128),
                        parallelism: 1,
                        status: TaskState::Running,
                        start_time: Timestamp::new(1638989054, 310 * 1_000_000),
                        end_time: None,
                        duration: Some(Duration::from_millis(35010565)),
                        tasks: maplit::hashmap! {
                            TaskState::Scheduled => 0,
                            TaskState::Finished => 0,
                            TaskState::Failed => 0,
                            TaskState::Canceling => 0,
                            TaskState::Canceled => 0,
                            TaskState::Deploying => 0,
                            TaskState::Reconciling => 0,
                            TaskState::Created => 0,
                            TaskState::Running => 1,
                            TaskState::Initializing => 0,
                        },
                        metrics: maplit::hashmap! {
                            "read-bytes".to_string() => 0_i64.into(),
                            "read-bytes-complete".to_string() => true.into(),
                            "write-bytes".to_string() => 32768_i64.into(),
                            "write-bytes-complete".to_string() => true.into(),
                            "read-records".to_string() => 0_i64.into(),
                            "read-records-complete".to_string() => true.into(),
                            "write-records".to_string() => 1012_i64.into(),
                            "write-records-complete".to_string() => true.into(),
                        },
                    },
                    VertexDetail {
                        id: VertexId::new("90bea66de1c231edf33913ecd54406c1"),
                        name: "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, \
                               PassThroughWindowFunction) -> Sink: Print to Std. Out"
                            .to_string(),
                        max_parallelism: Some(128),
                        parallelism: 1,
                        status: TaskState::Running,
                        start_time: Timestamp::new(1638989054, 320 * 1_000_000),
                        end_time: None,
                        duration: Some(Duration::from_millis(35010555)),
                        tasks: maplit::hashmap! {
                            TaskState::Scheduled => 0,
                            TaskState::Finished => 0,
                            TaskState::Failed => 0,
                            TaskState::Canceling => 0,
                            TaskState::Canceled => 0,
                            TaskState::Deploying => 0,
                            TaskState::Reconciling => 0,
                            TaskState::Created => 0,
                            TaskState::Running => 1,
                            TaskState::Initializing => 0,
                        },
                        metrics: maplit::hashmap! {
                            "read-bytes".to_string() => 40637_i64.into(),
                            "read-bytes-complete".to_string() => true.into(),
                            "write-bytes".to_string() => 0_i64.into(),
                            "write-bytes-complete".to_string() => true.into(),
                            "read-records".to_string() => 1010_i64.into(),
                            "read-records-complete".to_string() => true.into(),
                            "write-records".to_string() => 0_i64.into(),
                            "write-records-complete".to_string() => true.into(),
                        },
                    },
                ],
                status_counts: maplit::hashmap! {
                    TaskState::Scheduled => 0,
                    TaskState::Finished => 0,
                    TaskState::Failed => 0,
                    TaskState::Canceling => 0,
                    TaskState::Canceled => 0,
                    TaskState::Deploying => 0,
                    TaskState::Reconciling => 0,
                    TaskState::Created => 0,
                    TaskState::Running => 2,
                    TaskState::Initializing => 0,
                },
                plan: JobPlan {
                    jid: JobId::new("0771e8332dc401d254a140a707169a48"),
                    name: "CarTopSpeedWindowingExample".to_string(),
                    nodes: vec![
                        JobPlanItem {
                            id: VertexId::new("90bea66de1c231edf33913ecd54406c1"),
                            parallelism: 1,
                            operator: "".to_string(),
                            operator_strategy: "".to_string(),
                            description: "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, PassThroughWindowFunction) -&gt; Sink: Print to Std. Out".to_string(),
                            inputs: vec![
                                PlanItemInput {
                                    id: VertexId::new("cbc357ccb763df2852fee8c4fc7d55f2"),
                                    num: 0,
                                    ship_strategy: "HASH".to_string(),
                                    exchange: "pipelined_bounded".to_string(),
                                },
                            ],
                            optimizer_properties: HashMap::default(),
                        },
                        JobPlanItem {
                            id: VertexId::new("cbc357ccb763df2852fee8c4fc7d55f2"),
                            parallelism: 1,
                            operator: "".to_string(),
                            operator_strategy: "".to_string(),
                            description: "Source: Custom Source -&gt; Timestamps/Watermarks".to_string(),
                            inputs: Vec::default(),
                            optimizer_properties: HashMap::default(),
                        },
                    ],
                },
            }
        )
    }
}
