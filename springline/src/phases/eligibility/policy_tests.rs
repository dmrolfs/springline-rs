use crate::flink::{ClusterMetrics, FlowMetrics, JobHealthMetrics};
use chrono::{DateTime, TimeZone, Utc};
use claim::*;
use fake::{Fake, Faker};
use oso::Oso;
use pretty_snowflake::{Id, Label, Labeling};
use proctor::error::PolicyError;
use proptest::option as prop_option;
use proptest::prelude::*;
use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;

pub use super::{ClusterStatus, EligibilityContext, EligibilityPolicy, EligibilityTemplateData, JobStatus};
pub use crate::flink::{AppDataWindow, AppDataWindowBuilder, MetricCatalog};
pub use crate::settings::EligibilitySettings;
pub use proctor::elements::{PolicySource, QueryPolicy, Timestamp};

pub use crate::phases::REASON;
pub const NO_ACTIVE_JOBS: &str = "no_active_jobs";
pub const RESCALING: &str = "rescaling";
pub const DEPLOYING: &str = "deploying";
pub const COOLING_PERIOD: &str = "cooling_period";
pub const RECENT_FAILURE: &str = "recent_failure";

#[tracing::instrument(level = "info")]
pub fn prepare_policy_engine<P: QueryPolicy>(policy: &P) -> Result<Oso, PolicyError> {
    for source in policy.sources() {
        match source {
            PolicySource::File { path, .. } => {
                let f = std::fs::File::open(path);
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyScenario {
    pub template_data: Option<EligibilityTemplateData>,
    pub nr_active_jobs: u32,
    pub is_deploying: bool,
    pub is_rescaling: bool,
    pub last_deployment: DateTime<Utc>,
    pub last_failure: Option<DateTime<Utc>>,
}

impl PolicyScenario {
    pub fn strategy() -> impl Strategy<Value = Self> {
        Self::builder().strategy()
    }

    pub fn builder() -> PolicyScenarioBuilder {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("policy_scenario_builder");
        let _main_span_guard = main_span.enter();

        PolicyScenarioBuilder::default()
    }
}

#[derive(Debug, Default, Clone)]
pub struct PolicyScenarioBuilder {
    pub template_data: Option<BoxedStrategy<Option<EligibilityTemplateData>>>,
    pub nr_active_jobs: Option<BoxedStrategy<u32>>,
    pub is_deploying: Option<BoxedStrategy<bool>>,
    pub is_rescaling: Option<BoxedStrategy<bool>>,
    pub last_deployment: Option<BoxedStrategy<DateTime<Utc>>>,
    pub last_failure: Option<BoxedStrategy<Option<DateTime<Utc>>>>,
}

#[allow(dead_code)]
impl PolicyScenarioBuilder {
    pub fn template_data(self, template_data: impl Into<BoxedStrategy<Option<EligibilityTemplateData>>>) -> Self {
        let mut new = self;
        new.template_data = Some(template_data.into());
        new
    }

    pub fn just_template_data(self, template_data: impl Into<Option<EligibilityTemplateData>>) -> Self {
        self.template_data(Just(template_data.into()).boxed())
    }

    #[tracing::instrument(level = "info", skip(nr_active_jobs))]
    pub fn nr_active_jobs(self, nr_active_jobs: impl Into<BoxedStrategy<u32>>) -> Self {
        let mut new = self;
        let nr_active_jobs = nr_active_jobs.into();
        tracing::info!(is_rescaling=?new.is_rescaling, "DMR: nr_active_jobs={nr_active_jobs:?}");
        new.nr_active_jobs = Some(nr_active_jobs);
        new
    }

    pub fn just_nr_active_jobs(self, nr_active_jobs: impl Into<u32>) -> Self {
        self.nr_active_jobs(Just(nr_active_jobs.into()).boxed())
    }

    pub fn is_deploying(self, is_deploying: impl Into<BoxedStrategy<bool>>) -> Self {
        let mut new = self;
        new.is_deploying = Some(is_deploying.into());
        new
    }

    pub fn just_is_deploying(self, is_deploying: impl Into<bool>) -> Self {
        self.is_deploying(Just(is_deploying.into()).boxed())
    }

    #[tracing::instrument(level = "info", skip(is_rescaling))]
    pub fn is_rescaling(self, is_rescaling: impl Into<BoxedStrategy<bool>>) -> Self {
        let mut new = self;
        let is_rescaling = is_rescaling.into();
        tracing::info!(nr_active_jobs=?new.nr_active_jobs, "DMR: is_rescaling={is_rescaling:?}");
        new.is_rescaling = Some(is_rescaling);
        new
    }

    pub fn just_is_rescaling(self, is_rescaling: impl Into<bool>) -> Self {
        self.is_rescaling(Just(is_rescaling.into()).boxed())
    }

    pub fn last_deployment(self, last_deployment: impl Into<BoxedStrategy<DateTime<Utc>>>) -> Self {
        let mut new = self;
        new.last_deployment = Some(last_deployment.into());
        new
    }

    pub fn just_last_deployment(self, last_deployment: impl Into<DateTime<Utc>>) -> Self {
        self.last_deployment(Just(last_deployment.into()).boxed())
    }

    pub fn last_failure(self, last_failure: impl Into<BoxedStrategy<Option<DateTime<Utc>>>>) -> Self {
        let mut new = self;
        new.last_failure = Some(last_failure.into());
        new
    }

    pub fn just_last_failure(self, last_failure: impl Into<Option<DateTime<Utc>>>) -> Self {
        self.last_failure(Just(last_failure.into()).boxed())
    }

    pub fn strategy(self) -> impl Strategy<Value = PolicyScenario> {
        tracing::info!(?self, "DMR: building strategy");
        let template_data = self
            .template_data
            .unwrap_or(prop_option::of(policy_template_data()).boxed());
        let nr_active_jobs = self.nr_active_jobs.unwrap_or(any::<u32>().boxed());
        let is_rescaling = self.is_rescaling.unwrap_or(any::<bool>().boxed());
        let is_deploying = self.is_deploying.unwrap_or(any::<bool>().boxed());
        let last_deployment = self.last_deployment.unwrap_or(date_time().boxed());
        let last_failure = self.last_failure.unwrap_or(prop_option::of(date_time()).boxed());

        (
            template_data,
            nr_active_jobs,
            is_deploying,
            is_rescaling,
            last_deployment,
            last_failure,
        )
            .prop_map(
                |(template_data, nr_active_jobs, is_deploying, is_rescaling, last_deployment, last_failure)| {
                    tracing::info!(?is_rescaling, ?nr_active_jobs, "DMR: making scenario..");
                    PolicyScenario {
                        template_data,
                        nr_active_jobs,
                        is_deploying,
                        is_rescaling,
                        last_deployment,
                        last_failure,
                    }
                },
            )
    }
}

fn policy_template_data() -> impl Strategy<Value = EligibilityTemplateData> {
    (any::<Option<u32>>(), any::<Option<u32>>()).prop_map(|(cooling_secs, stable_secs)| EligibilityTemplateData {
        cooling_secs,
        stable_secs,
        ..EligibilityTemplateData::default()
    })
}

fn date_time() -> impl Strategy<Value = DateTime<Utc>> {
    let min_year = i32::MIN >> 13;
    let max_year = i32::MAX >> 13;

    fn is_leap_year(year: i32) -> bool {
        return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    }

    fn last_day_of_month(year: i32, month: u32) -> u32 {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if is_leap_year(year) {
                    29
                } else {
                    28
                }
            },
            _ => panic!("invalid month: {}", month),
        }
    }

    (
        min_year..=max_year,
        1_u32..=12,
        0_u32..24,
        0_u32..60,
        0_u32..60,
        0_u32..1_000_000_000,
    )
        .prop_flat_map(|(yr, mnth, h, m, s, n): (i32, u32, u32, u32, u32, u32)| {
            let day = 1_u32..=last_day_of_month(yr, mnth);
            (Just(yr), Just(mnth), day, Just(h), Just(m), Just(s), Just(n))
        })
        .prop_map(|(yr, mnth, day, h, m, s, n): (i32, u32, u32, u32, u32, u32, u32)| {
            // tracing::info!("DMR: yr:{yr} mnth:{mnth} day:{day} h:{h} m:{m} s:{s} n:{n}");
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
    use itertools::Itertools;
    use pretty_snowflake::{Id, Label};
    use proctor::elements::{PolicySource, QueryResult, TelemetryValue};

    #[tracing::instrument(level = "info")]
    pub fn run_policy_scenario(scenario: &PolicyScenario) -> Result<QueryResult, PolicyError> {
        let context = EligibilityContext {
            correlation_id: Id::direct(<EligibilityContext as Label>::labeler().label(), 0, "test_doesnt_crash"),
            recv_timestamp: Timestamp::now(),
            job: JobStatus { last_failure: scenario.last_failure },
            cluster: ClusterStatus {
                is_deploying: scenario.is_deploying,
                is_rescaling: scenario.is_rescaling,
                last_deployment: scenario.last_deployment,
            },
            all_sinks_healthy: true,
            custom: HashMap::default(),
        };

        let item =
            AppDataWindow::from_time_window(make_metric_catalog(scenario.nr_active_jobs), Duration::from_secs(600));

        let policy = EligibilityPolicy::new(&EligibilitySettings {
            policies: vec![
                PolicySource::File {
                    path: "../resources/eligibility.polar".into(),
                    is_template: true,
                },
                PolicySource::File {
                    path: "../resources/eligibility_ext.polar".into(),
                    is_template: true,
                },
            ],
            template_data: scenario.template_data.clone(),
            ..EligibilitySettings::default()
        });
        let policy_engine = assert_ok!(prepare_policy_engine(&policy));
        let args = policy.make_query_args(&item, &context);
        policy.query_policy(&policy_engine, args)
    }

    proptest! {
        #[test]
        fn doesnt_crash(scenario in PolicyScenario::strategy()) {
            prop_assert!(run_policy_scenario(&scenario).is_ok())
        }

        #[test]
        fn test_is_rescaling(scenario in PolicyScenario::strategy()) {
            let result = assert_ok!(run_policy_scenario(&scenario));
            if scenario.is_rescaling {
                prop_assert_eq!(
                    result,
                    QueryResult { passed: false, bindings: maplit::hashmap! { REASON.to_string() => vec![RESCALING.into()] } }
                );
            }
        }

        #[test]
        fn test_no_active_jobs(scenario in (PolicyScenario::builder().just_is_rescaling(false).just_nr_active_jobs(0_u32).strategy())) {
            let result = assert_ok!(run_policy_scenario(&scenario));

            if scenario.nr_active_jobs == 0 {
                prop_assert!(result.passed == false);
                prop_assert!(result.bindings.contains_key(REASON));
                let reasons = assert_some!(result.bindings.get(REASON));
                prop_assert!(reasons.into_iter().contains(&TelemetryValue::from(NO_ACTIVE_JOBS)));
            } else if result.passed {
                prop_assert!(result.bindings.is_empty());
            } else {
                prop_assert!(result.bindings.contains_key(REASON));
                let reasons = assert_some!(result.bindings.get(REASON));
                prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(NO_ACTIVE_JOBS)));
            }
        }

        #[test]
        fn test_is_deploying(scenario in (PolicyScenario::builder().just_is_rescaling(false).strategy())) {
            let result = assert_ok!(run_policy_scenario(&scenario));

            if scenario.is_deploying {
                prop_assert!(result.passed == false);
                prop_assert!(result.bindings.contains_key(REASON));
                let reasons = assert_some!(result.bindings.get(REASON));
                prop_assert!(reasons.into_iter().contains(&TelemetryValue::from(DEPLOYING)));
            } else if result.passed {
                prop_assert!(result.bindings.is_empty());
            } else {
                prop_assert!(result.bindings.contains_key(REASON));
                let reasons = assert_some!(result.bindings.get(REASON));
                prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(DEPLOYING)));
            }
        }

        #[test]
        fn test_cooling_period(scenario in (PolicyScenario::builder().just_is_rescaling(false).strategy())) {
            let result = assert_ok!(run_policy_scenario(&scenario));

            let cooling_boundary = scenario.template_data
                .and_then(|data| data.cooling_secs)
                .map(|cooling_secs| Utc::now() - chrono::Duration::seconds(i64::from(cooling_secs)));

            match cooling_boundary {
                Some(boundary) if boundary < scenario.last_deployment => {
                    prop_assert!(result.passed == false);
                    prop_assert!(result.bindings.contains_key(REASON));
                    let reasons = assert_some!(result.bindings.get(REASON));
                    prop_assert!(reasons.into_iter().contains(&TelemetryValue::from(COOLING_PERIOD)));
                },
                _ if result.passed => prop_assert!(result.bindings.is_empty()),
                _ => {
                    prop_assert!(result.bindings.contains_key(REASON));
                    let reasons = assert_some!(result.bindings.get(REASON));
                    prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(COOLING_PERIOD)));
                },
            }
        }

        #[test]
        fn test_stable_period(scenario in (PolicyScenario::builder().just_is_rescaling(false).strategy())) {
            let result = assert_ok!(run_policy_scenario(&scenario));

            let stability_boundary = scenario.template_data
                .and_then(|data| data.stable_secs)
                .map(|stable_secs| Utc::now() - chrono::Duration::seconds(i64::from(stable_secs)));

            match stability_boundary.zip(scenario.last_failure) {
                Some((boundary, last_failure)) if boundary < last_failure => {
                    prop_assert!(result.passed == false);
                    prop_assert!(result.bindings.contains_key(REASON));
                    let reasons = assert_some!(result.bindings.get(REASON));
                    prop_assert!(reasons.into_iter().contains(&TelemetryValue::from(RECENT_FAILURE)));
                },
                _ if result.passed => prop_assert!(result.bindings.is_empty()),
                _ => {
                    prop_assert!(result.bindings.contains_key(REASON));
                    let reasons = assert_some!(result.bindings.get(REASON));
                    prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(RECENT_FAILURE)));
                },
            }
        }
    }
}
