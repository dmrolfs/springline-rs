use super::*;
use proctor::MetaData;

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

    #[tracing::instrument(level = "info")]
    pub fn run(&self) -> Result<QueryResult, PolicyError> {
        let context = Env::from_parts(
            MetaData::from_parts(
                Id::direct(
                    <EligibilityContext as Label>::labeler().label(),
                    0,
                    "test_doesnt_crash",
                ),
                Timestamp::now(),
            ),
            EligibilityContext {
                job: JobStatus { last_failure: self.last_failure },
                cluster: ClusterStatus {
                    is_deploying: self.is_deploying,
                    is_rescaling: self.is_rescaling,
                    last_deployment: self.last_deployment,
                },
                all_sinks_healthy: true,
                custom: HashMap::default(),
            },
        );

        let item = make_metric_catalog(self.nr_active_jobs)
            .flat_map(|mc| AppDataWindow::from_time_window(mc, Duration::from_secs(600)));

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
            template_data: self.template_data.clone(),
            ..EligibilitySettings::default()
        });
        let policy_engine = assert_ok!(prepare_policy_engine(&policy));
        let args = policy.make_query_args(&item, &context);
        policy.query_policy(&policy_engine, args)
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
    pub fn template_data(
        mut self, template_data: impl Strategy<Value = Option<EligibilityTemplateData>> + 'static,
    ) -> Self {
        self.template_data = Some(template_data.boxed());
        self
    }

    pub fn just_template_data(
        self, template_data: impl Into<Option<EligibilityTemplateData>>,
    ) -> Self {
        self.template_data(Just(template_data.into()))
    }

    #[tracing::instrument(level = "info", skip(nr_active_jobs))]
    pub fn nr_active_jobs(self, nr_active_jobs: impl Strategy<Value = u32> + 'static) -> Self {
        self.nr_active_jobs = Some(nr_active_jobs.boxed());
        self
    }

    pub fn just_nr_active_jobs(self, nr_active_jobs: impl Into<u32>) -> Self {
        self.nr_active_jobs(Just(nr_active_jobs.into()))
    }

    pub fn is_deploying(mut self, is_deploying: impl Strategy<Value = bool> + 'static) -> Self {
        self.is_deploying = Some(is_deploying.boxed());
        self
    }

    pub fn just_is_deploying(self, is_deploying: impl Into<bool>) -> Self {
        self.is_deploying(Just(is_deploying.into()))
    }

    #[tracing::instrument(level = "info", skip(is_rescaling))]
    pub fn is_rescaling(mut self, is_rescaling: impl Strategy<Value = bool> + 'static) -> Self {
        self.is_rescaling = Some(is_rescaling.boxed());
        self
    }

    pub fn just_is_rescaling(self, is_rescaling: impl Into<bool>) -> Self {
        self.is_rescaling(Just(is_rescaling.into()))
    }

    pub fn last_deployment(
        mut self, last_deployment: impl Strategy<Value = DateTime<Utc>> + 'static,
    ) -> Self {
        self.last_deployment = Some(last_deployment.boxed());
        self
    }

    pub fn just_last_deployment(self, last_deployment: impl Into<DateTime<Utc>>) -> Self {
        self.last_deployment(Just(last_deployment.into()))
    }

    pub fn last_failure(
        mut self, last_failure: impl Strategy<Value = Option<DateTime<Utc>>> + 'static,
    ) -> Self {
        self.last_failure = Some(last_failure.boxed());
        self
    }

    pub fn just_last_failure(self, last_failure: impl Into<Option<DateTime<Utc>>>) -> Self {
        self.last_failure(Just(last_failure.into()))
    }

    pub fn strategy(self) -> impl Strategy<Value = PolicyScenario> {
        let template_data = self
            .template_data
            .unwrap_or(prop::option::of(arb_policy_template_data()).boxed());
        let nr_active_jobs = self.nr_active_jobs.unwrap_or(any::<u32>().boxed());
        let is_rescaling = self.is_rescaling.unwrap_or(any::<bool>().boxed());
        let is_deploying = self.is_deploying.unwrap_or(any::<bool>().boxed());
        let last_deployment = self.last_deployment.unwrap_or(arb_date_time().boxed());
        let last_failure = self.last_failure.unwrap_or(prop::option::of(arb_date_time()).boxed());

        (
            template_data,
            nr_active_jobs,
            is_deploying,
            is_rescaling,
            last_deployment,
            last_failure,
        )
            .prop_map(
                |(
                    template_data,
                    nr_active_jobs,
                    is_deploying,
                    is_rescaling,
                    last_deployment,
                    last_failure,
                )| {
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
