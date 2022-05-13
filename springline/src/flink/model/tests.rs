use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Mutex;

use chrono::{DateTime, TimeZone, Utc};
use claim::*;
use once_cell::sync::Lazy;
use proctor::elements::telemetry::TableValue;
use proctor::elements::{Telemetry, TelemetryType, TelemetryValue, ToTelemetry};
use proctor::error::TelemetryError;
use proctor::phases::sense::{SUBSCRIPTION_CORRELATION, SUBSCRIPTION_TIMESTAMP};
use proctor::ProctorIdGenerator;
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde_test::{assert_tokens, Token};
use trim_margin::MarginTrimmable;

use crate::flink::*;

static ID_GENERATOR: Lazy<Mutex<ProctorIdGenerator<MetricCatalog>>> =
    Lazy::new(|| Mutex::new(ProctorIdGenerator::default()));

mod catalog {
    use pretty_assertions::assert_eq;
    use pretty_snowflake::{Id, Label, Labeling};
    use proctor::elements::{telemetry, Timestamp};

    use super::*;

    fn metrics_for_test_with_datetime(ts: DateTime<Utc>, custom: telemetry::TableType) -> MetricCatalog {
        let mut id_gen = assert_ok!(ID_GENERATOR.lock());
        MetricCatalog {
            correlation_id: id_gen.next_id(),
            recv_timestamp: ts.into(),
            custom,
            health: JobHealthMetrics::default(),
            flow: FlowMetrics::default(),
            cluster: ClusterMetrics::default(),
        }
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
    fn test_invalid_type_serde_issue() {
        let mut telemetry = Telemetry::new();
        telemetry.insert("cluster.task_cpu_load".to_string(), TelemetryValue::Float(0.025));
        telemetry.insert(
            "cluster.task_heap_memory_used".to_string(),
            TelemetryValue::Float(2511508464.0),
        );
        telemetry.insert(
            "cluster.task_network_input_queue_len".to_string(),
            TelemetryValue::Float(1.0),
        );
        telemetry.insert("health.job_uptime_millis".to_string(), TelemetryValue::Integer(201402));
        telemetry.insert(
            "flow.forecasted_timestamp".to_string(),
            TelemetryValue::Float(Timestamp::new(1647307440, 378969192).as_secs_f64()),
        );
        telemetry.insert(
            "cluster.task_network_output_queue_len".to_string(),
            TelemetryValue::Float(1.0),
        );
        telemetry.insert("cluster.nr_active_jobs".to_string(), TelemetryValue::Integer(0));
        telemetry.insert("health.job_nr_restarts".to_string(), TelemetryValue::Integer(0));
        telemetry.insert(
            "recv_timestamp".to_string(),
            TelemetryValue::Table(TableValue(Box::new(maplit::hashmap! {
                "secs".to_string() => TelemetryValue::Integer(1647307527),
                "nanos".to_string() => TelemetryValue::Integer(57406000)
            }))),
        );
        telemetry.insert("flow.records_in_per_sec".to_string(), TelemetryValue::Float(20.0));
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
            "cluster.task_heap_memory_committed".to_string(),
            TelemetryValue::Float(3623878656.0),
        );
        telemetry.insert("cluster.task_nr_threads".to_string(), TelemetryValue::Integer(57));
        telemetry.insert(
            "cluster.task_network_input_pool_usage".to_string(),
            TelemetryValue::Float(0.0),
        );
        telemetry.insert("cluster.nr_task_managers".to_string(), TelemetryValue::Integer(5));
        telemetry.insert(
            "correlation_id".to_string(),
            TelemetryValue::Table(TableValue(Box::new(maplit::hashmap! {
                "pretty".to_string() => TelemetryValue::Text("FRQB-08549-HYQY-31208".to_string()),
                "snowflake".to_string() => TelemetryValue::Integer(6909308549966213120)
            }))),
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
                correlation_id: CorrelationId::direct(
                    "MetricCatalog",
                    6909308549966213120_i64,
                    "FRQB-08549-HYQY-31208"
                ),
                recv_timestamp: Timestamp::new(1647307527, 57406000),
                health: JobHealthMetrics {
                    job_uptime_millis: 201402,
                    job_nr_restarts: 0,
                    job_nr_completed_checkpoints: 0,
                    job_nr_failed_checkpoints: 0,
                },
                flow: FlowMetrics {
                    records_in_per_sec: 20.0,
                    records_out_per_sec: 19.966666666666665,
                    forecasted_timestamp: Some(Timestamp::new(1647307440, 378969192)),
                    forecasted_records_in_per_sec: Some(21.4504261933966),
                    input_records_lag_max: None,
                    input_millis_behind_latest: None,
                },
                cluster: ClusterMetrics {
                    nr_active_jobs: 0,
                    nr_task_managers: 5,
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
        assert_eq!(assert_some!(assert_ok!(get_custom_metric::<i64>(&data, "foo"))), 17_i64);
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
    static CORR_ID: Lazy<Id<MetricCatalog>> = Lazy::new(|| Id::direct("MetricCatalog", 12, CORR_ID_REP));

    #[test]
    fn test_metric_catalog_serde() {
        let ts: Timestamp = Utc.ymd(1988, 5, 30).and_hms(9, 1, 17).into();
        let (ts_secs, ts_nsecs) = ts.as_pair();
        let metrics = MetricCatalog {
            correlation_id: CORR_ID.clone(),
            recv_timestamp: ts,
            health: JobHealthMetrics {
                job_uptime_millis: 1_234_567,
                job_nr_restarts: 3,
                job_nr_completed_checkpoints: 12_345,
                job_nr_failed_checkpoints: 7,
            },
            flow: FlowMetrics {
                records_in_per_sec: 17.,
                forecasted_timestamp: Some(ts),
                forecasted_records_in_per_sec: Some(23.),
                input_records_lag_max: Some(314),
                input_millis_behind_latest: None,
                records_out_per_sec: 0.0,
            },
            cluster: ClusterMetrics {
                nr_active_jobs: 1,
                nr_task_managers: 4,
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
                Token::Str("health.job_uptime_millis"),
                Token::I64(1_234_567),
                Token::Str("health.job_nr_restarts"),
                Token::I64(3),
                Token::Str("health.job_nr_completed_checkpoints"),
                Token::I64(12_345),
                Token::Str("health.job_nr_failed_checkpoints"),
                Token::I64(7),
                Token::Str(MC_FLOW__RECORDS_IN_PER_SEC),
                Token::F64(17.),
                Token::Str("flow.records_out_per_sec"),
                Token::F64(0.),
                Token::Str("flow.forecasted_timestamp"),
                Token::Some,
                Token::TupleStruct { name: "Timestamp", len: 2 },
                Token::I64(ts.as_pair().0),
                Token::U32(ts.as_pair().1),
                Token::TupleStructEnd,
                Token::Str("flow.forecasted_records_in_per_sec"),
                Token::Some,
                Token::F64(23.),
                Token::Str("flow.input_records_lag_max"),
                Token::Some,
                Token::I64(314),
                Token::Str(MC_CLUSTER__NR_ACTIVE_JOBS),
                Token::U32(1),
                Token::Str(MC_CLUSTER__NR_TASK_MANAGERS),
                Token::U32(4),
                Token::Str("cluster.task_cpu_load"),
                Token::F64(0.65),
                Token::Str("cluster.task_heap_memory_used"),
                Token::F64(92_987.),
                Token::Str("cluster.task_heap_memory_committed"),
                Token::F64(103_929_920.),
                Token::Str("cluster.task_nr_threads"),
                Token::I64(8),
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
            ],
        )
    }

    #[test]
    fn test_telemetry_from_metric_catalog() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_telemetry_from_metric_catalog");
        let _main_span_guard = main_span.enter();

        let ts = Utc.ymd(1988, 5, 30).and_hms(9, 1, 17).into();
        let corr_id = Id::direct("MetricCatalog", 17, "AB");
        let metrics = MetricCatalog {
            correlation_id: corr_id.clone(),
            recv_timestamp: ts,
            health: JobHealthMetrics {
                job_uptime_millis: 1_234_567,
                job_nr_restarts: 3,
                job_nr_completed_checkpoints: 12_345,
                job_nr_failed_checkpoints: 7,
            },
            flow: FlowMetrics {
                records_in_per_sec: 17.,
                forecasted_timestamp: None,
                forecasted_records_in_per_sec: None,
                input_records_lag_max: Some(314),
                input_millis_behind_latest: None,
                records_out_per_sec: 0.0,
            },
            cluster: ClusterMetrics {
                nr_active_jobs: 1,
                nr_task_managers: 4,
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
        };

        let telemetry = Telemetry::try_from(&metrics)?;
        let (ts_secs, ts_nsecs) = ts.as_pair();

        assert_eq!(
            telemetry,
            TelemetryValue::Table(maplit::hashmap! {
                SUBSCRIPTION_CORRELATION.to_string() => corr_id.to_telemetry(),
                SUBSCRIPTION_TIMESTAMP.to_string() => TelemetryValue::Seq(vec![ts_secs.to_telemetry(), ts_nsecs.to_telemetry(),]),
                "health.job_uptime_millis".to_string() => (1_234_567).to_telemetry(),
                "health.job_nr_restarts".to_string() => (3).to_telemetry(),
                "health.job_nr_completed_checkpoints".to_string() => (12_345).to_telemetry(),
                "health.job_nr_failed_checkpoints".to_string() => (7).to_telemetry(),

                MC_FLOW__RECORDS_IN_PER_SEC.to_string() => (17.).to_telemetry(),
                "flow.records_out_per_sec".to_string() => (0.).to_telemetry(),
                "flow.input_records_lag_max".to_string() => 314.to_telemetry(),

                MC_CLUSTER__NR_ACTIVE_JOBS.to_string() => 1.to_telemetry(),
                MC_CLUSTER__NR_TASK_MANAGERS.to_string() => 4.to_telemetry(),
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

    // #[test]
    // fn test_metric_to_f64() {
    //     let expected = 3.14159_f64;
    //     let m: Metric<f64> = Metric::new("pi", expected);
    //
    //     let actual: f64 = m.into();
    //     assert_eq!(actual, expected);
    // }

    #[tracing::instrument(level = "info")]
    pub fn make_test_catalog(ts: Timestamp, value: u32) -> MetricCatalog {
        MetricCatalog {
            correlation_id: Id::direct(
                <MetricCatalog as Label>::labeler().label(),
                ts.as_secs(),
                ts.as_secs_f64().to_string(),
            ),
            recv_timestamp: ts,
            health: JobHealthMetrics::default(),
            flow: FlowMetrics {
                records_in_per_sec: f64::from(value),
                ..FlowMetrics::default()
            },
            cluster: ClusterMetrics::default(),
            custom: HashMap::new(),
        }
    }
}

mod portfolio {
    use std::time::Duration;

    use frunk::Semigroup;
    use pretty_assertions::assert_eq;
    use proctor::elements::Timestamp;

    use super::catalog::make_test_catalog;
    use super::*;

    #[tracing::instrument(level = "info", skip(catalogs))]
    fn make_test_portfolio(
        limit: usize, interval: Duration, catalogs: &[MetricCatalog],
    ) -> (MetricPortfolio, Vec<MetricCatalog>) {
        let mut portfolio = MetricPortfolio::builder().with_size_and_interval(limit, interval);
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
            tracing::debug!(portfolio_len=%(portfolio.len()+1), catalog=%c.correlation_id,"pushing catalog into portfolio.");
            portfolio.push(c);
        }

        (portfolio.build(), remaining)
    }

    #[test]
    fn test_metric_portfolio_head() {
        let m1 = make_test_catalog(Timestamp::new(1, 0), 1);
        let m2 = make_test_catalog(Timestamp::new(2, 0), 2);

        let mut portfolio = MetricPortfolio::from_size(m1.clone(), 3, Duration::from_secs(1));
        assert_eq!(portfolio.correlation_id, m1.correlation_id);
        assert_eq!(portfolio.recv_timestamp, m1.recv_timestamp);

        portfolio.push(m2.clone());
        assert_eq!(portfolio.correlation_id, m2.correlation_id);
        assert_eq!(portfolio.recv_timestamp, m2.recv_timestamp);
    }

    #[test]
    fn test_metric_portfolio_combine() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_metric_portfolio_combine");
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

        let (port_1, remaining) = make_test_portfolio(3, interval, &ms);
        assert_eq!(port_1.len(), 3);
        assert_eq!(
            port_1.clone().into_iter().collect::<Vec<_>>(),
            vec![m1.clone(), m2.clone(), m3.clone()]
        );
        let (port_2, _) = make_test_portfolio(4, interval, &remaining);
        assert_eq!(port_2.len(), 4);
        assert_eq!(
            port_2.clone().into_iter().collect::<Vec<_>>(),
            vec![m4.clone(), m5.clone(), m6.clone(), m7.clone()]
        );
        let combined = port_1.clone().combine(&port_2);
        assert_eq!(combined.time_window, port_1.time_window);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(actual, vec![m4.clone(), m5.clone(), m6.clone(), m7.clone()]);
        let combined = port_2.combine(&port_1);
        assert_eq!(combined.time_window, port_2.time_window);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(actual, vec![m3.clone(), m4.clone(), m5.clone(), m6.clone(), m7.clone()]);

        let (port_3, remaining) = make_test_portfolio(4, interval, &ms);
        let (port_4, _) = make_test_portfolio(2, interval, &remaining);
        let combined = port_3.clone().combine(&port_4);
        assert_eq!(combined.time_window, port_3.time_window);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(actual, vec![m2.clone(), m3.clone(), m4.clone(), m5.clone(), m6.clone()]);
        let combined = port_4.clone().combine(&port_3);
        assert_eq!(combined.time_window, port_4.time_window);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(actual, vec![m4.clone(), m5.clone(), m6.clone()]);
    }

    #[test]
    fn test_metric_portfolio_shuffled_combine() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_metric_portfolio_shuffled_combine");
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

        let (port_1, remaining) = make_test_portfolio(3, interval, &shuffled);
        let (port_2, _) = make_test_portfolio(4, interval, &remaining);
        let combined = port_1.clone().combine(&port_2);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(actual, vec![m4.clone(), m5.clone(), m6.clone(), m7.clone()]);
        let combined = port_2.clone().combine(&port_1);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(actual, vec![m3.clone(), m4.clone(), m5.clone(), m6.clone(), m7.clone()]);

        let (port_3, remaining) = make_test_portfolio(6, interval, &shuffled);
        assert_eq!(port_3.len(), 6);
        let (port_4, _) = make_test_portfolio(1, interval, &remaining);
        assert_eq!(port_4.len(), 1);
        let combined = port_3.clone().combine(&port_4);
        tracing::debug!(
            lhs=?port_3.history().map(|p| p.correlation_id.to_string()).collect::<Vec<_>>(),
            rhs=?port_4.history().map(|p| p.correlation_id.to_string()).collect::<Vec<_>>(),
            combined=?combined.history().map(|p| p.correlation_id.to_string()).collect::<Vec<_>>(),
            "port_3.combine(port_4)"
        );
        assert_eq!(combined.len(), port_3.len() + 1);
        assert_eq!(assert_some!(combined.window_interval()).duration(), port_3.time_window);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(
            actual,
            vec![
                m1.clone(),
                m2.clone(),
                m3.clone(),
                m4.clone(),
                m5.clone(),
                m6.clone(),
                m7.clone()
            ]
        );
        let combined = port_4.clone().combine(&port_3);
        assert_eq!(combined.len(), port_4.len() + 1);
        assert_eq!(assert_some!(combined.window_interval()).duration(), port_4.time_window);
        let actual: Vec<MetricCatalog> = combined.into_iter().collect();
        assert_eq!(actual, vec![m6.clone(), m7.clone()]);
    }

    #[test]
    fn test_metric_portfolio_for_period() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_metric_portfolio_for_period");
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

        let f = |c: &MetricCatalog| {
            tracing::debug!(
                "[test] testing catalog[{}]: ({} <= {}) is {}",
                c.recv_timestamp.to_string(),
                c.flow.records_in_per_sec,
                5.0,
                c.flow.records_in_per_sec <= 5.0,
            );
            c.flow.records_in_per_sec <= 5.0
        };

        let (portfolio, _) = make_test_portfolio(10, interval, &ms);
        tracing::info!(
            portfolio=?portfolio.history().map(|m| (m.recv_timestamp.to_string(), m.flow.records_in_per_sec)).collect::<Vec<_>>(),
            "*** PORTFOLIO CREATED"
        );

        tracing::info_span!("looking back for 5 seconds").in_scope(|| {
            assert_eq!(portfolio.forall_from_head(Duration::from_secs(5), f), false);
        });

        tracing::info_span!("looking back for 11 seconds").in_scope(|| {
            assert_eq!(portfolio.forall_from_head(Duration::from_secs(11), f), true);
        });

        tracing::info_span!("looking back for 21 seconds").in_scope(|| {
            assert_eq!(portfolio.forall_from_head(Duration::from_secs(21), f), true);
        });

        tracing::info_span!("looking back for 31 seconds").in_scope(|| {
            assert_eq!(portfolio.forall_from_head(Duration::from_secs(31), f), true);
        });

        tracing::info_span!("looking back for 41 seconds").in_scope(|| {
            assert_eq!(portfolio.forall_from_head(Duration::from_secs(41), f), true);
        });

        tracing::info_span!("looking back for 51 seconds").in_scope(|| {
            assert_eq!(portfolio.forall_from_head(Duration::from_secs(51), f), false);
        });

        tracing::info_span!("looking back for 61 seconds").in_scope(|| {
            assert_eq!(portfolio.forall_from_head(Duration::from_secs(61), f), false);
        });

        tracing::info_span!("looking back for 71 seconds").in_scope(|| {
            assert_eq!(portfolio.forall_from_head(Duration::from_secs(71), f), false);
        });

        tracing::info_span!("looking back for 2000 seconds").in_scope(|| {
            assert_eq!(portfolio.forall_from_head(Duration::from_secs(2000), f), false);
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
            }
        )
    }
}
