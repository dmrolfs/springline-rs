use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use proctor::elements::Telemetry;

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
        "flow.records_in_per_sec".to_string() => records_in_per_sec.into(),
        "flow.input_records_lag_max".to_string() => inbox_lag.into(),
    }
    .into_iter()
    .collect();

    item
}

pub fn make_test_item_padding() -> Telemetry {
    let padding = maplit::hashmap! {
            "health.job_uptime_millis".to_string() => (0).into(),
            "health.job_nr_restarts".to_string() => (0).into(),
            "health.job_nr_completed_checkpoints".to_string() => (0).into(),
            "health.job_nr_failed_checkpoints".to_string() => (0).into(),

            "flow.records_out_per_sec".to_string() => (0.).into(),

            "cluster.task_cpu_load".to_string() => (0.).into(),
            "cluster.task_heap_memory_used".to_string() => (0.).into(),
            "cluster.task_heap_memory_committed".to_string() => (0.).into(),
            "cluster.task_nr_threads".to_string() => (0).into(),
            "cluster.task_network_input_queue_len".to_string() => (0).into(),
            "cluster.task_network_input_pool_usage".to_string() => (0).into(),
            "cluster.task_network_output_queue_len".to_string() => (0).into(),
            "cluster.task_network_output_pool_usage".to_string() => (0).into(),
    }
    .into_iter()
    .collect();

    padding
}
