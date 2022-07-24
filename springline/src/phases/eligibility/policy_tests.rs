use oso::Oso;
use proctor::error::PolicyError;
use proptest::prelude::*;
use chrono::{DateTime, Utc, TimeZone};
use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;
use pretty_snowflake::{Id, Label, Labeling};
pub use crate::flink::{MetricCatalog, AppDataWindow, AppDataWindowBuilder};
pub use super::{EligibilityTemplateData, EligibilityPolicy, EligibilityContext, JobStatus, ClusterStatus};
pub use crate::settings::EligibilitySettings;
pub use proctor::elements::{QueryPolicy, PolicySource, Timestamp};
use crate::flink::{ClusterMetrics, FlowMetrics, JobHealthMetrics};
use fake::{Fake, Faker};
use claim::*;

#[tracing::instrument(level="info")]
pub fn prepare_policy_engine<P: QueryPolicy>(policy: &P) -> Result<Oso, PolicyError> {
    for source in policy.sources() {
        match source {
            PolicySource::File { path, .. } => {
                let mut f = std::fs::File::open(path);
                match &f {
                    Ok(f0) => tracing::info!(file=?f0, ?path, "opened policy source file."),
                    Err(err) => tracing::error!(error=?err, ?path, "failed to open policy source file."),
                };

                let mut contents = String::new();
                let size = assert_ok!(assert_ok!(f).read_to_string(&mut contents));
                tracing::info!(%contents, %size, "reading policy FILE {path:?}.");
            },
            PolicySource::String { name, polar, .. } => {
                tracing::info!(contents=%polar, %name, "reading policy STRING {name}.");
            },
        }
    }

    for rendered in assert_ok!(policy.render_policy_sources()) {
        match rendered {
            proctor::elements::policy_filter::PolicySourcePath::File(path) => {
                let mut f = assert_ok!(std::fs::File::open(path.clone()));
                let mut contents = String::new();
                let size = assert_ok!(f.read_to_string(&mut contents));
                tracing::info!(%contents, %size, "reading RENDERED policy FILE {path:?}.");
            },
            proctor::elements::policy_filter::PolicySourcePath::String(tf) => {
                let mut f = assert_ok!(tf.reopen());
                let mut contents = String::new();
                let size = assert_ok!(f.read_to_string(&mut contents));
                tracing::info!(%contents, %size, "reading RENDERED policy STRING {tf:?}.");
            },
        }
    }

    let mut oso = Oso::new();
    policy.load_policy_engine(&mut oso)?;
    policy.initialize_policy_engine(&mut oso)?;
    Ok(oso)
}

fn policy_template_data()  -> impl Strategy<Value = EligibilityTemplateData> {
    (any::<Option<u32>>(), any::<Option<u32>>()).prop_map(|(cooling_secs, stable_secs)| {
        EligibilityTemplateData { cooling_secs, stable_secs, ..EligibilityTemplateData::default() }
    })
}

fn date_time() -> impl Strategy<Value = DateTime<Utc>> {
    let MIN_YEAR: i32 = i32::MIN >> 13;
    let MAX_YEAR: i32 = i32::MAX >> 13;

    fn is_leap_year(year: i32) -> bool {
        return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
    }

    fn last_day_of_month(year: i32, month: u32) -> u32 {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => if is_leap_year(year) { 29 } else { 28 },
            _ => panic!("invalid month: {}" , month),
        }
    }

    (MIN_YEAR..=MAX_YEAR, 1_u32..=12, 0_u32..24, 0_u32..60, 0_u32..60, 0_u32..1_000_000_000)
        .prop_flat_map(|(yr, mnth, h, m, s, n): (i32, u32, u32, u32, u32, u32)| {
            let day = 1_u32..=last_day_of_month(yr, mnth);
            (Just(yr), Just(mnth), day, Just(h), Just(m), Just(s), Just(n))
        })
        .prop_map(|(yr, mnth, day, h, m, s, n): (i32, u32, u32, u32, u32, u32, u32)| {
            tracing::info!("DMR: yr:{yr} mnth:{mnth} day:{day} h:{h} m:{m} s:{s} n:{n}");
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

pub mod resources_policy {
    use super::*;
    use claim::*;
    use proctor::elements::PolicySource;
    use pretty_snowflake::{Id, Label};
    use proptest::{option as prop_option};

    #[tracing::instrument(level="info",)]
    pub fn doesnt_crash_test(
        template_data: Option<EligibilityTemplateData>,
        nr_active_jobs: u32,
        is_deploying:bool,
        is_rescaling: bool,
        last_deployment: DateTime<Utc>,
        last_failure: Option<DateTime<Utc>>,
    ) {
        let context = EligibilityContext {
            correlation_id: Id::direct(<EligibilityContext as Label>::labeler().label(), 0, "test_doesnt_crash"),
            recv_timestamp: Timestamp::now(),
            job: JobStatus { last_failure, },
            cluster: ClusterStatus { is_deploying, is_rescaling, last_deployment, },
            all_sinks_healthy: true,
            custom: HashMap::default(),
        };

        let item = AppDataWindow::from_time_window(make_metric_catalog(nr_active_jobs), Duration::from_secs(600));

        let policy = EligibilityPolicy::new(&EligibilitySettings {
            policies: vec![
                PolicySource::File { path: "../resources/eligibility.polar".into(), is_template: true, },
                PolicySource::File { path: "../resources/eligibility_ext.polar".into(), is_template: true, },
            ],
            template_data,
            ..EligibilitySettings::default()
        });
        let policy_engine = assert_ok!(prepare_policy_engine(&policy));
        let args = policy.make_query_args(&item, &context);
        assert_ok!(policy.query_policy(&policy_engine, args));
    }

    proptest! {
        #[test]
        fn doesnt_crash(
            template_data in prop_option::of(policy_template_data()),
            nr_active_jobs: u32,
            is_deploying: bool,
            is_rescaling: bool,
            last_deployment in date_time(),
            last_failure in prop_option::of(date_time()),
        ) {
            doesnt_crash_test(template_data, nr_active_jobs, is_deploying, is_rescaling, last_deployment, last_failure)
        }
    }
}