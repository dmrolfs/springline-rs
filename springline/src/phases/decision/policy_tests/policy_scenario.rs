use super::*;
use proctor::elements::QueryResult;
use proctor::error::PolicyError;

#[derive(Debug, Clone, PartialEq)]
pub struct PolicyScenario {
    pub template_data: Option<DecisionTemplateData>,
    pub item: AppDataWindow<MetricCatalog>,
}

impl PolicyScenario {
    pub fn strategy(decision_basis: impl Into<String>) -> impl Strategy<Value = Self> {
        Self::builder().strategy(decision_basis)
    }

    pub fn builder() -> PolicyScenarioBuilder {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("decision_policy_scenario_builder");
        let _main_span_guard = main_span.enter();

        PolicyScenarioBuilder::default()
    }

    #[tracing::instrument(level = "info")]
    pub fn run(&self) -> Result<QueryResult, PolicyError> {
        todo!()
    }
}

#[derive(Debug, Default, Clone)]
pub struct PolicyScenarioBuilder {
    pub template_data: Option<BoxedStrategy<Option<DecisionTemplateData>>>,
    pub item: Option<BoxedStrategy<AppDataWindow<MetricCatalog>>>,
}

#[allow(dead_code)]
impl PolicyScenarioBuilder {
    #[tracing::instrument(level = "info", skip(template_data))]
    pub fn template_data(self, template_data: impl Strategy<Value = Option<DecisionTemplateData>> + 'static) -> Self {
        let template_data = template_data.boxed();
        tracing::info!("DMR: template_data={template_data:?}");

        let mut new = self;
        new.template_data = Some(template_data);
        new
    }

    pub fn just_template_data(self, template_data: impl Into<Option<DecisionTemplateData>>) -> Self {
        self.template_data(Just(template_data.into()))
    }

    #[tracing::instrument(level = "info", skip(items))]
    pub fn items(self, items: impl Strategy<Value = AppDataWindow<MetricCatalog>> + 'static) -> Self {
        let items = items.boxed();
        tracing::info!("DMR: items={items:?}");

        let mut new = self;
        new.item = Some(items);
        new
    }

    pub fn just_items(self, items: impl Into<AppDataWindow<MetricCatalog>>) -> Self {
        self.items(Just(items.into()))
    }

    #[tracing::instrument(level = "info", skip(item, window))]
    pub fn one_item(
        self, item: impl Strategy<Value = MetricCatalog> + 'static, window: impl Strategy<Value = Duration> + 'static,
    ) -> Self {
        let items = (item, window)
            .prop_map(|(item, window)| AppDataWindow::from_time_window(item, window))
            .boxed();

        let mut new = self;
        new.item = Some(items);
        new
    }

    pub fn just_one_item(self, item: impl Into<MetricCatalog>, window: impl Into<Duration>) -> Self {
        self.one_item(Just(item.into()), Just(window.into()))
    }

    #[tracing::instrument(level = "info", skip(decision_basis))]
    pub fn strategy(self, decision_basis: impl Into<String>) -> impl Strategy<Value = PolicyScenario> {
        tracing::info!(?self, "DMR: building decision policy strategy");
        let template_data = self
            .template_data
            .unwrap_or(prop::option::of(DecisionTemplateDataStrategyBuilder::strategy(decision_basis.into())).boxed());

        let data = arb_metric_catalog_window(
            Timestamp::now(),
            (10_u64..=600).prop_map(Duration::from_secs),
            arb_perturbed_duration(Duration::from_secs(15), 0.2),
            |recv_ts| {
                MetricCatalogStrategyBuilder::new()
                    .just_recv_timestamp(recv_ts)
                    .finish()
                    .boxed()
            },
        );

        let item = self.item.unwrap_or(data.boxed());
        (template_data, item).prop_map(|(template_data, item)| {
            tracing::info!(?template_data, ?item, "DMR: making scenario...");
            PolicyScenario { template_data, item }
        })
    }
}
