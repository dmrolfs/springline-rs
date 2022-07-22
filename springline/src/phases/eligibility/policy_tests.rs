use oso::Oso;
use proctor::error::PolicyError;
use proptest::prelude::*;
use chrono::{DateTime, Utc, TimeZone};
use std::collections::HashMap;
use std::time::Duration;
use pretty_snowflake::{Id, Label, Labeling};
pub use crate::flink::{MetricCatalog, AppDataWindow, AppDataWindowBuilder};
pub use super::{EligibilityTemplateData, EligibilityPolicy, EligibilityContext, JobStatus, ClusterStatus};
pub use crate::settings::EligibilitySettings;
pub use proctor::elements::{QueryPolicy, PolicySource, Timestamp};
use crate::flink::{ClusterMetrics, FlowMetrics, JobHealthMetrics};
use fake::{Fake, Faker};

pub fn prepare_policy_engine<P: QueryPolicy>(policy: &P) -> Result<Oso, PolicyError> {
    let mut oso = Oso::new();
    policy.load_policy_engine(&mut oso)?;
    policy.initialize_policy_engine(&mut oso)?;
    Ok(oso)
}

fn policy_template_data()  -> impl Strategy<Value = Option<EligibilityTemplateData>> {
    (any::<bool>(), any::<Option<u32>>(), any::<Option<u32>>()).prop_map(|(is_some, cooling_secs, stable_secs)| {
        if is_some {
            Some(EligibilityTemplateData { cooling_secs, stable_secs, ..EligibilityTemplateData::default() })
        } else {
            None
        }
    })
}

fn date_time() -> impl Strategy<Value = DateTime<Utc>> {
    (0_i32..10_000, 1_u32..=12, 1_u32..=31, 0_u32..24, 0_u32..60, 0_u32..60, 0_u32..1_000_000_000)
        .prop_map(|(yr, mnth, day, h, m, s, n): (i32, u32, u32, u32, u32, u32, u32)| {
        Utc.ymd(yr, mnth, day).and_hms_nano(h, m, s, n)
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

mod resources_policy {
    use super::*;
    use claim::*;
    use proctor::elements::PolicySource;
    use pretty_snowflake::{Id, Label};

    proptest! {
        #[test]
        fn doesnt_crash(
            template_data in policy_template_data(),
            nr_active_jobs: u32,
            is_deploying: bool,
            is_rescaling: bool,
            last_deployment in date_time(),
            // last_failure in OptionStrategy of date_time(),
        ) {
            let context = EligibilityContext {
                correlation_id: Id::direct(<EligibilityContext as Label>::labeler().label(), 0, "test_doesnt_crash"),
                recv_timestamp: Timestamp::now(),
                job: JobStatus {
                    last_failure: None,
                },
                cluster: ClusterStatus { is_deploying, is_rescaling, last_deployment, },
                all_sinks_healthy: true,
                custom: HashMap::default(),
            };

            let item = AppDataWindow::from_time_window(make_metric_catalog(nr_active_jobs), Duration::from_secs(600));

            let policy = EligibilityPolicy::new(&EligibilitySettings {
                policies: vec![
                    PolicySource::File { path: "../resources/eligibility.polar".into(), is_template: true, },
                    PolicySource::File { path: "../resources/eligibility_basis.polar".into(), is_template: true, },
                ],
                template_data,
                ..EligibilitySettings::default()
            });
            let policy_engine = assert_ok!(prepare_policy_engine(&policy));
            let args = policy.make_query_args(&item, &context);
            let _result = assert_ok!(policy.query_policy(&policy_engine, args));

        }
    }
}