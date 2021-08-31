use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::ops::Add;

// use ::serde_with::{serde_as, TimestampSeconds};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use oso::PolarClass;
use serde::{Deserialize, Serialize};

use proctor::elements::{telemetry, TelemetryValue, Timestamp};
use proctor::error::{CollectionError, TelemetryError};
use proctor::phases::collection::{
    ClearinghouseApi, ClearinghouseCmd, SubscriptionChannel, TelemetrySubscription,
};

// todo: replace this with reflection approach if one identified.
// tried serde-reflection, which failed since serde identifiers (flatten) et.al., are not supported.
// tried Oso PolarClass, but Class::attributes is private.
// could write a rpcedural macro, but want to list effective serde names, such as for flattened.
lazy_static! {
    pub static ref METRIC_CATALOG_REQ_SUBSCRIPTION_FIELDS: HashSet<&'static str> = maplit::hashset! {
        "timestamp",

        // FlowMetrics
        "records_in_per_sec",
        "records_out_per_sec",
        "input_consumer_lag",
        // "max_message_latency",
        // "net_in_utilization",
        // "net_out_utilization",
        // "sink_health_metrics",

        // ClusterMetrics
        "nr_task_managers",
        "task_cpu_load",
        "network_io_utilization",
    };//.iter().map(|rep| rep.to_string()).collect();
}

// #[serde_as]
#[derive(PolarClass, Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct MetricCatalog {
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
    pub custom: telemetry::Table,
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

impl MetricCatalog {
    pub fn for_timestamp(timestamp: Timestamp, custom: telemetry::Table) -> Self {
        Self {
            timestamp,
            flow: FlowMetrics::default(),
            cluster: ClusterMetrics::default(),
            custom,
        }
    }

    pub fn for_datetime(timestamp: DateTime<Utc>, custom: telemetry::Table) -> Self {
        Self::for_timestamp(timestamp.into(), custom)
    }

    // todo limited usefulness by itself; keys? iter support for custom and for entire catalog?
    pub fn custom<T: TryFrom<TelemetryValue>>(
        &self,
        metric: &str,
    ) -> Option<Result<T, TelemetryError>>
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

impl MetricCatalog {
    #[tracing::instrument(
        level="info",
        skip(channel_name, tx_clearinghouse_api),
        fields(channel_name=%channel_name.as_ref()),
    )]
    pub async fn connect_channel(
        channel_name: impl AsRef<str>,
        tx_clearinghouse_api: &ClearinghouseApi,
    ) -> Result<SubscriptionChannel<MetricCatalog>, CollectionError> {
        let channel = SubscriptionChannel::new(channel_name.as_ref()).await?;
        let subscription = TelemetrySubscription::new(channel_name.as_ref())
            .with_required_fields(METRIC_CATALOG_REQ_SUBSCRIPTION_FIELDS.clone());

        let (subscribe_cmd, rx_subscribe_ack) =
            ClearinghouseCmd::subscribe(subscription, channel.subscription_receiver.clone());
        tx_clearinghouse_api
            .send(subscribe_cmd)
            .map_err(|err| CollectionError::StageError(err.into()))?;
        rx_subscribe_ack
            .await
            .map_err(|err| CollectionError::StageError(err.into()))?;
        Ok(channel)
    }
}

impl Add for MetricCatalog {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let mut lhs = self.custom;
        lhs.extend(rhs.custom);
        MetricCatalog {
            custom: lhs,
            ..self
        }
    }
}

impl Add<&Self> for MetricCatalog {
    type Output = Self;

    fn add(self, rhs: &Self) -> Self::Output {
        let mut lhs = self.custom;
        lhs.extend(rhs.custom.clone());
        Self {
            custom: lhs,
            ..self
        }
    }
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
        let data = MetricCatalog::for_datetime(Utc::now(), cdata);
        assert_eq!(data.custom::<i64>("foo").unwrap().unwrap(), 17_i64);
        assert_eq!(data.custom::<f64>("foo").unwrap().unwrap(), 17.0_f64);
        assert_eq!(
            data.custom::<String>("otis").unwrap().unwrap(),
            "Otis".to_string()
        );
        assert_eq!(
            data.custom::<Bar>("bar").unwrap().unwrap(),
            Bar("Neo".to_string())
        );
        assert_eq!(
            data.custom::<String>("bar").unwrap().unwrap(),
            "Neo".to_string()
        );
        assert!(data.custom::<i64>("zed").is_none());
    }

    #[test]
    fn test_metric_add() {
        let ts = Utc::now();
        let data = MetricCatalog::for_datetime(ts.clone(), std::collections::HashMap::default());
        let am1 = maplit::hashmap! {
            "foo.1".to_string() => "f-1".to_telemetry(),
            "bar.1".to_string() => "b-1".to_telemetry(),
        };
        let a1 = MetricCatalog::for_datetime(ts.clone(), am1.clone());
        let d1 = data.clone() + a1.clone();
        assert_eq!(d1.custom, am1);

        let am2 = maplit::hashmap! {
            "foo.2".to_string() => "f-2".to_telemetry(),
            "bar.2".to_string() => "b-2".to_telemetry(),
        };
        let a2 = MetricCatalog::for_datetime(ts.clone(), am2.clone());
        let d2 = d1.clone() + a2.clone();
        let mut exp2 = am1.clone();
        exp2.extend(am2.clone());
        assert_eq!(d2.custom, exp2);
    }

    #[test]
    fn test_metric_catalog_serde() {
        let ts: Timestamp = Utc.ymd(1988, 5, 30).and_hms(9, 1, 17).into();
        let (ts_secs, ts_nsecs) = ts.as_pair();
        let metrics = MetricCatalog {
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
                Token::Str("timestamp"),
                Token::TupleStruct {
                    name: "Timestamp",
                    len: 2,
                },
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
        let metrics = MetricCatalog {
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
                "timestamp".to_string() => TelemetryValue::Seq(vec![ts_secs.to_telemetry(), ts_nsecs.to_telemetry(),]),
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
            })
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
