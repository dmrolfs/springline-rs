use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use proctor::elements::{Telemetry, Timestamp};

lazy_static! {
    pub static ref DT_1: DateTime<Utc> = DateTime::parse_from_str("2021-05-05T17:11:07.246310806Z", "%+")
        .unwrap()
        .with_timezone(&Utc);
    pub static ref DT_1_STR: String = format!("{}", DT_1.format("%+"));
    pub static ref DT_1_TS: i64 = DT_1.timestamp();
}

pub fn make_test_item(_timestamp: &DateTime<Utc>, records_in_per_sec: f64, inbox_lag: f64) -> Telemetry {
    let item = maplit::hashmap! {
        // "timestamp".to_string() => Timestamp::from_datetime(&timestamp).into(),
        "records_in_per_sec".to_string() => records_in_per_sec.into(),
        "input_consumer_lag".to_string() => inbox_lag.into(),
    }
    .into_iter()
    .collect();

    item
}

pub fn make_test_item_padding() -> Telemetry {
    let padding = maplit::hashmap! {
            "records_out_per_sec".to_string() => (0.).into(),
            "max_message_latency".to_string() => (0.).into(),
            "net_in_utilization".to_string() => (0.).into(),
            "net_out_utilization".to_string() => (0.).into(),
            "sink_health_metrics".to_string() => (0.).into(),
            "task_cpu_load".to_string() => (0.).into(),
            "network_io_utilization".to_string() => (0.).into(),
    }
    .into_iter()
    .collect();

    padding
}
