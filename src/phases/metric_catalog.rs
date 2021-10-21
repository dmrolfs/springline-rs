use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::ops::Add;

// use ::serde_with::{serde_as, TimestampSeconds};
use chrono::{DateTime, Utc};
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
    pub flow: FlowMetrics,

    #[polar(attribute)]
    #[serde(flatten)] // current subscription mechanism only supports flatten keys
    pub cluster: ClusterMetrics,

    #[polar(attribute)]
    #[serde(flatten)] // flatten to collect extra properties.
    pub custom: telemetry::TableType,
}

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct FlowMetrics {
    // this will need to be in context:  historical_input_messages_per_sec: VecDeque<(f64, DateTime<Utc>)>,
    #[polar(attribute)]
    pub records_in_per_sec: f64,

    // #[polar(attribute)]
    // pub task_nr_records_in_per_sec: f64,

    // #[polar(attribute)]
    // pub task_nr_records_out_per_sec: f64,
    #[polar(attribute)]
    pub records_out_per_sec: f64,

    #[polar(attribute)]
    pub input_consumer_lag: f64,
    // #[polar(attribute)]
    // pub max_message_latency: f64,

    // #[polar(attribute)]
    // pub net_in_utilization: f64,

    // #[polar(attribute)]
    // pub net_out_utilization: f64,

    // #[polar(attribute)]
    // pub sink_health_metrics: f64,
}

#[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct ClusterMetrics {
    #[polar(attribute)]
    pub nr_task_managers: u16,

    #[polar(attribute)]
    pub task_cpu_load: f64,

    #[polar(attribute)]
    pub network_io_utilization: f64,
}

use pretty_snowflake::{AlphabetCodec, IdPrettifier};
use proctor::IdGenerator;
use std::sync::Mutex;

lazy_static! {
    static ref ID_GENERATOR: Mutex<IdGenerator> =
        Mutex::new(IdGenerator::single_node(IdPrettifier::<AlphabetCodec>::default()));
}

impl MetricCatalog {
    pub(crate) fn for_test_with_timestamp(timestamp: Timestamp, custom: telemetry::TableType) -> Self {
        let generator = &mut ID_GENERATOR.lock().unwrap();

        Self {
            correlation_id: generator.next_id(),
            timestamp,
            flow: FlowMetrics::default(),
            cluster: ClusterMetrics::default(),
            custom,
        }
    }

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
            // FlowMetrics
            "records_in_per_sec".into(),
            "records_out_per_sec".into(),
            "input_consumer_lag".into(),
            // "max_message_latency",
            // "net_in_utilization",
            // "net_out_utilization",
            // "sink_health_metrics",

            // ClusterMetrics
            "nr_task_managers".into(),
            "task_cpu_load".into(),
            "network_io_utilization".into(),
        }
    }
}

impl UpdateMetrics for MetricCatalog {
    fn update_metrics_for(name: SharedString) -> Box<dyn Fn(&str, &Telemetry) -> () + Send + Sync + 'static> {
        let update_fn =
            move |subscription_name: &str, telemetry: &Telemetry| match telemetry.clone().try_into::<MetricCatalog>() {
                Ok(catalog) => {
                    METRIC_CATALOG_TIMESTAMP.set(catalog.timestamp.as_secs());

                    METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC.set(catalog.flow.records_in_per_sec);
                    METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC.set(catalog.flow.records_out_per_sec);
                    METRIC_CATALOG_FLOW_INPUT_CONSUMER_LAG.set(catalog.flow.input_consumer_lag);

                    METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS.set(catalog.cluster.nr_task_managers as i64);
                    METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD.set(catalog.cluster.task_cpu_load);
                    METRIC_CATALOG_CLUSTER_NETWORK_IO_UTILIZATION.set(catalog.cluster.network_io_utilization);
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
        "Current CPU load experienced by the cluster task managers"
    )
    .expect("failed creating metric_catalog_records_out_per_sec metric");
    pub(crate) static ref METRIC_CATALOG_CLUSTER_NETWORK_IO_UTILIZATION: Gauge = Gauge::new(
        "metric_catalog_cluster_network_io_utilization",
        "Current network IO utilization by the cluster"
    )
    .expect("failed creating metric_catalog_cluster_network_io_utilization metric");
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
            flow: FlowMetrics {
                records_in_per_sec: 17.,
                input_consumer_lag: 3.14,
                records_out_per_sec: 0.0,
                // max_message_latency: 0.0,
                // net_in_utilization: 0.0,
                // net_out_utilization: 0.0,
                // sink_health_metrics: 0.0,
            },
            cluster: ClusterMetrics {
                nr_task_managers: 4,
                task_cpu_load: 0.0,
                network_io_utilization: 0.0,
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
                Token::Str("records_in_per_sec"),
                Token::F64(17.),
                Token::Str("records_out_per_sec"),
                Token::F64(0.),
                Token::Str("input_consumer_lag"),
                Token::F64(3.14),
                // Token::Str("max_message_latency"),
                // Token::F64(0.),
                // Token::Str("net_in_utilization"),
                // Token::F64(0.),
                // Token::Str("net_out_utilization"),
                // Token::F64(0.),
                // Token::Str("sink_health_metrics"),
                // Token::F64(0.),
                Token::Str("nr_task_managers"),
                Token::U16(4),
                Token::Str("task_cpu_load"),
                Token::F64(0.),
                Token::Str("network_io_utilization"),
                Token::F64(0.),
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
            flow: FlowMetrics {
                records_in_per_sec: 17.,
                input_consumer_lag: 3.14,
                records_out_per_sec: 0.0,
                // max_message_latency: 0.0,
                // net_in_utilization: 0.0,
                // net_out_utilization: 0.0,
                // sink_health_metrics: 0.0,
            },
            cluster: ClusterMetrics {
                nr_task_managers: 4,
                task_cpu_load: 0.0,
                network_io_utilization: 0.0,
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
                "records_in_per_sec".to_string() => (17.).to_telemetry(),
                "records_out_per_sec".to_string() => (0.).to_telemetry(),
                "input_consumer_lag".to_string() => 3.14.to_telemetry(),
                // "max_message_latency".to_string() => (0.).to_telemetry(),
                // "net_in_utilization".to_string() => (0.).to_telemetry(),
                // "net_out_utilization".to_string() => (0.).to_telemetry(),
                // "sink_health_metrics".to_string() => (0.).to_telemetry(),
                "nr_task_managers".to_string() => 4.to_telemetry(),
                "task_cpu_load".to_string() => (0.).to_telemetry(),
                "network_io_utilization".to_string() => (0.).to_telemetry(),
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
