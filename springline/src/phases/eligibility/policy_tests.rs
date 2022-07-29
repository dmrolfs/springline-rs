mod policy_scenario;
mod resources_policy_tests;
mod resources_regression_tests;

pub use policy_scenario::*;

use crate::flink::{ClusterMetrics, FlowMetrics, JobHealthMetrics};
pub use chrono::{DateTime, Utc, TimeZone};
pub use claim::*;
use fake::{Fake, Faker};
use pretty_snowflake::{Id, Label, Labeling};
use proctor::error::PolicyError;
use proptest::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

pub use super::{ClusterStatus, EligibilityContext, EligibilityPolicy, EligibilityTemplateData, JobStatus};
pub use crate::flink::{AppDataWindow, AppDataWindowBuilder, MetricCatalog};
pub use crate::phases::policy_test_fixtures::{arb_date_time, prepare_policy_engine};
pub use crate::settings::EligibilitySettings;
pub use proctor::elements::{PolicySource, QueryPolicy, QueryResult, Timestamp};
pub use proctor::elements::telemetry::TelemetryValue;

pub use crate::phases::REASON;
pub const NO_ACTIVE_JOBS: &str = "no_active_jobs";
pub const RESCALING: &str = "rescaling";
pub const DEPLOYING: &str = "deploying";
pub const COOLING_PERIOD: &str = "cooling_period";
pub const RECENT_FAILURE: &str = "recent_failure";

fn arb_policy_template_data() -> impl Strategy<Value = EligibilityTemplateData> {
    (any::<Option<u32>>(), any::<Option<u32>>()).prop_map(|(cooling_secs, stable_secs)| EligibilityTemplateData {
        cooling_secs,
        stable_secs,
        ..EligibilityTemplateData::default()
    })
}

fn make_metric_catalog(nr_active_jobs: u32) -> MetricCatalog {
    MetricCatalog {
        correlation_id: Id::direct(<MetricCatalog as Label>::labeler().label(), 0, "test_metric_catalog"),
        recv_timestamp: Timestamp::now(),
        health: JobHealthMetrics {
            job_max_parallelism: Faker.fake(),
            job_uptime_millis: Faker.fake(),
            job_nr_restarts: Faker.fake(),
            job_nr_completed_checkpoints: Faker.fake(),
            job_nr_failed_checkpoints: Faker.fake(),
        },
        flow: FlowMetrics {
            records_in_per_sec: Faker.fake(),
            records_out_per_sec: Faker.fake(),
            idle_time_millis_per_sec: Faker.fake(),
            source_back_pressured_time_millis_per_sec: Faker.fake(),
            forecasted_timestamp: None,
            forecasted_records_in_per_sec: None,
            source_records_lag_max: None,
            source_assigned_partitions: None,
            source_records_consumed_rate: None,
            source_total_lag: None,
            source_millis_behind_latest: None,
        },
        cluster: ClusterMetrics {
            nr_active_jobs,
            nr_task_managers: Faker.fake(),
            task_cpu_load: Faker.fake(),
            task_heap_memory_used: Faker.fake(),
            task_heap_memory_committed: Faker.fake(),
            task_nr_threads: Faker.fake(),
            task_network_input_queue_len: Faker.fake(),
            task_network_input_pool_usage: Faker.fake(),
            task_network_output_queue_len: Faker.fake(),
            task_network_output_pool_usage: Faker.fake(),
        },
        custom: HashMap::default(),
    }
}
