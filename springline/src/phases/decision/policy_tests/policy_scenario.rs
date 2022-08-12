use super::*;
use crate::phases::decision::{DecisionContext, DecisionPolicy};
use crate::phases::policy_test_fixtures::prepare_policy_engine;
use crate::settings::DecisionSettings;
use claim::*;
use pretty_snowflake::{Id, Label, Labeling};
use proctor::elements::{PolicySource, QueryPolicy, QueryResult};
use proctor::error::PolicyError;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyScenario {
    #[serde(default)]
    pub template_data: Option<DecisionTemplateData>,
    pub item: AppDataWindow<MetricCatalog>,
}

impl PolicyScenario {
    pub fn strategy(decision_basis: impl Into<String>) -> impl Strategy<Value = Self> {
        Self::builder().basis(decision_basis).strategy()
    }

    pub fn builder() -> PolicyScenarioBuilder {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::debug_span!("decision_policy_scenario_builder");
        let _main_span_guard = main_span.enter();

        PolicyScenarioBuilder::default()
    }

    #[tracing::instrument(level = "debug")]
    pub fn run(&self) -> Result<QueryResult, PolicyError> {
        let context = DecisionContext {
            correlation_id: Id::direct(
                <DecisionContext as Label>::labeler().label(),
                0,
                "test_doesnt_crash",
            ),
            recv_timestamp: Timestamp::now(),
            custom: Default::default(),
        };

        let policy = DecisionPolicy::new(&DecisionSettings {
            policies: vec![
                PolicySource::File {
                    path: "../resources/decision.polar".into(),
                    is_template: true,
                },
                PolicySource::File {
                    path: "../resources/decision_basis.polar".into(),
                    is_template: true,
                },
            ],
            template_data: self.template_data.clone(),
            ..DecisionSettings::default()
        })?;
        let policy_engine = assert_ok!(prepare_policy_engine(&policy));
        let args = policy.make_query_args(&self.item, &context);
        policy.query_policy(&policy_engine, args)
    }
}

impl fmt::Debug for PolicyScenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let json_rep = serde_json::to_string(self).expect("failed to create policy scenario json");
        f.write_str(json_rep.as_str())
    }
}

#[derive(Debug, Default, Clone)]
pub struct PolicyScenarioBuilder {
    basis: Option<String>,
    template_data: Option<BoxedStrategy<Option<DecisionTemplateData>>>,
    item: Option<BoxedStrategy<AppDataWindow<MetricCatalog>>>,
}

#[allow(dead_code)]
impl PolicyScenarioBuilder {
    pub fn basis(self, basis: impl Into<String>) -> Self {
        let mut new = self;
        new.basis = Some(basis.into());
        new
    }

    #[tracing::instrument(level = "debug", skip(template_data))]
    pub fn template_data(
        self, template_data: impl Strategy<Value = Option<DecisionTemplateData>> + 'static,
    ) -> Self {
        let template_data = template_data.boxed();
        tracing::info!("template_data={template_data:?}");

        let mut new = self;
        new.template_data = Some(template_data);
        new
    }

    pub fn just_template_data(
        self, template_data: impl Into<Option<DecisionTemplateData>>,
    ) -> Self {
        self.template_data(Just(template_data.into()))
    }

    #[tracing::instrument(level = "debug", skip(items))]
    pub fn items(
        self, items: impl Strategy<Value = AppDataWindow<MetricCatalog>> + 'static,
    ) -> Self {
        let items = items.boxed();
        tracing::info!("items={items:?}");

        let mut new = self;
        new.item = Some(items);
        new
    }

    pub fn just_items(self, items: impl Into<AppDataWindow<MetricCatalog>>) -> Self {
        self.items(Just(items.into()))
    }

    #[tracing::instrument(level = "debug", skip(item, window))]
    pub fn one_item(
        self, item: impl Strategy<Value = MetricCatalog> + 'static,
        window: impl Strategy<Value = Duration> + 'static,
    ) -> Self {
        let items = (item, window)
            .prop_map(|(item, window)| AppDataWindow::from_time_window(item, window))
            .boxed();

        let mut new = self;
        new.item = Some(items);
        new
    }

    pub fn just_one_item(
        self, item: impl Into<MetricCatalog>, window: impl Into<Duration>,
    ) -> Self {
        self.one_item(Just(item.into()), Just(window.into()))
    }

    #[tracing::instrument(level = "debug")]
    pub fn strategy(self) -> impl Strategy<Value = PolicyScenario> {
        tracing::info!(?self, "building decision policy strategy");
        let template_data = self.template_data.unwrap_or_else(|| {
            let mut builder = DecisionTemplateDataStrategyBuilder::default();
            if let Some(basis) = self.basis {
                builder = builder.basis(basis);
            }
            prop::option::of(builder.finish()).boxed()
        });

        let data = arb_metric_catalog_window_from_timestamp_window(
            arb_timestamp_window(
                Timestamp::now(),
                (10_u64..=600).prop_map(Duration::from_secs),
                arb_perturbed_duration(Duration::from_secs(15), 0.2),
            ),
            |recv_ts| {
                MetricCatalogStrategyBuilder::new()
                    .just_recv_timestamp(recv_ts)
                    .finish()
                    .boxed()
            },
        );

        let item = self.item.unwrap_or(data.boxed());
        (template_data, item).prop_map(|(template_data, item)| {
            tracing::info!(?template_data, ?item, "making scenario...");
            PolicyScenario { template_data, item }
        })
    }
}
