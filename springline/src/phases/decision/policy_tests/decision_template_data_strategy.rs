use super::*;
use validator::Validate;

#[derive(Debug, Default, Clone)]
pub struct DecisionTemplateDataStrategyBuilder {
    basis: Option<String>,
    max_healthy_relative_lag_velocity: Option<BoxedStrategy<Option<f64>>>,
    max_healthy_lag: Option<BoxedStrategy<Option<f64>>>,
    min_task_utilization: Option<BoxedStrategy<Option<f64>>>,
    max_healthy_cpu_load: Option<BoxedStrategy<Option<f64>>>,
    max_healthy_heap_memory_load: Option<BoxedStrategy<Option<f64>>>,
    max_healthy_network_io_utilization: Option<BoxedStrategy<Option<f64>>>,
    evaluate_duration_secs: Option<BoxedStrategy<Option<u32>>>,
}

#[allow(dead_code)]
impl DecisionTemplateDataStrategyBuilder {
    pub fn strategy() -> impl Strategy<Value = DecisionTemplateData> {
        Self::default().finish()
    }

    pub fn basis(self, basis: impl Into<String>) -> Self {
        let mut new = self;
        new.basis = Some(basis.into());
        new
    }

    pub fn max_healthy_relative_lag_velocity(
        self, max_healthy_relative_lag_velocity: impl Strategy<Value = Option<f64>> + 'static,
    ) -> Self {
        let mut new = self;
        new.max_healthy_relative_lag_velocity = Some(max_healthy_relative_lag_velocity.boxed());
        new
    }

    pub fn just_max_healthy_relative_lag_velocity(
        self, max_healthy_relative_lag_velocity: impl Into<Option<f64>>,
    ) -> Self {
        self.max_healthy_relative_lag_velocity(Just(max_healthy_relative_lag_velocity.into()))
    }

    pub fn max_healthy_lag(
        self, max_healthy_lag: impl Strategy<Value = Option<f64>> + 'static,
    ) -> Self {
        let mut new = self;
        new.max_healthy_lag = Some(max_healthy_lag.boxed());
        new
    }

    pub fn just_max_healthy_lag(self, max_healthy_lag: impl Into<Option<f64>>) -> Self {
        self.max_healthy_lag(Just(max_healthy_lag.into()))
    }

    pub fn min_task_utilization(
        self, min_task_utilization: impl Strategy<Value = Option<f64>> + 'static,
    ) -> Self {
        let mut new = self;
        new.min_task_utilization = Some(min_task_utilization.boxed());
        new
    }

    pub fn just_min_task_utilization(self, min_task_utilization: impl Into<Option<f64>>) -> Self {
        self.min_task_utilization(Just(min_task_utilization.into()))
    }

    pub fn max_healthy_cpu_load(
        self, max_healthy_cpu_load: impl Strategy<Value = Option<f64>> + 'static,
    ) -> Self {
        let mut new = self;
        new.max_healthy_cpu_load = Some(max_healthy_cpu_load.boxed());
        new
    }

    pub fn just_max_healthy_cpu_load(self, max_healthy_cpu_load: impl Into<Option<f64>>) -> Self {
        self.max_healthy_cpu_load(Just(max_healthy_cpu_load.into()))
    }

    pub fn max_healthy_heap_memory_load(
        self, max_healthy_heap_memory_load: impl Strategy<Value = Option<f64>> + 'static,
    ) -> Self {
        let mut new = self;
        new.max_healthy_heap_memory_load = Some(max_healthy_heap_memory_load.boxed());
        new
    }

    pub fn just_max_healthy_heap_memory_load(
        self, max_healthy_heap_memory_load: impl Into<Option<f64>>,
    ) -> Self {
        self.max_healthy_heap_memory_load(Just(max_healthy_heap_memory_load.into()))
    }

    pub fn max_healthy_network_io_utilization(
        self, max_healthy_network_io_utilization: impl Strategy<Value = Option<f64>> + 'static,
    ) -> Self {
        let mut new = self;
        new.max_healthy_network_io_utilization = Some(max_healthy_network_io_utilization.boxed());
        new
    }

    pub fn just_max_healthy_network_io_utilization(
        self, max_healthy_network_io_utilization: impl Into<Option<f64>>,
    ) -> Self {
        self.max_healthy_network_io_utilization(Just(max_healthy_network_io_utilization.into()))
    }

    pub fn evaluate_duration_secs(
        self, evaluate_duration_secs: impl Strategy<Value = Option<u32>> + 'static,
    ) -> Self {
        let mut new = self;
        new.evaluate_duration_secs = Some(evaluate_duration_secs.boxed());
        new
    }

    pub fn just_evaluate_duration_secs(
        self, evaluate_duration_secs: impl Into<Option<u32>>,
    ) -> Self {
        self.evaluate_duration_secs(Just(evaluate_duration_secs.into()))
    }

    pub fn finish(self) -> impl Strategy<Value = DecisionTemplateData> {
        tracing::info!(?self, "DMR: building DecisionTemplateData strategy");
        let basis = Just(self.basis.unwrap_or("decision_basis".to_string()));
        let max_healthy_relative_lag_velocity = self
            .max_healthy_relative_lag_velocity
            .unwrap_or(prop::option::of(-1e10_f64..=1e10).boxed());
        let max_healthy_lag =
            self.max_healthy_lag.unwrap_or(prop::option::of(-10_f64..=1e10).boxed());
        let min_task_utilization = self
            .min_task_utilization
            .unwrap_or(prop::option::of(-10_f64..=1e10).boxed());
        let max_healthy_cpu_load = self
            .max_healthy_cpu_load
            .unwrap_or(prop::option::of(-1e10_f64..=1e10).boxed());
        let max_healthy_heap_memory_load = self
            .max_healthy_heap_memory_load
            .unwrap_or(prop::option::of(-10_f64..=1e10).boxed());
        let max_healthy_network_io_utilization = self
            .max_healthy_network_io_utilization
            .unwrap_or(prop::option::of(-10_f64..=1e10).boxed());
        let evaluate_duration_secs = self
            .evaluate_duration_secs
            .unwrap_or(prop::option::of(any::<u32>()).boxed());

        (
            basis,
            max_healthy_relative_lag_velocity,
            max_healthy_lag,
            min_task_utilization,
            max_healthy_cpu_load,
            max_healthy_heap_memory_load,
            max_healthy_network_io_utilization,
            evaluate_duration_secs,
        )
            .prop_filter_map(
                "invalid DecisionTemplateData",
                move |(
                    basis,
                    max_healthy_relative_lag_velocity,
                    max_healthy_lag,
                    min_task_utilization,
                    max_healthy_cpu_load,
                    max_healthy_heap_memory_load,
                    max_healthy_network_io_utilization,
                    evaluate_duration_secs,
                )| {
                    tracing::info!(
                        ?basis,
                        ?max_healthy_relative_lag_velocity,
                        ?max_healthy_lag,
                        ?min_task_utilization,
                        ?max_healthy_cpu_load,
                        ?max_healthy_heap_memory_load,
                        ?max_healthy_network_io_utilization,
                        ?evaluate_duration_secs,
                        "making DecisionTemplateData..."
                    );

                    let template_data = DecisionTemplateData {
                        basis,
                        max_healthy_relative_lag_velocity,
                        max_healthy_lag,
                        min_task_utilization,
                        max_healthy_cpu_load,
                        max_healthy_heap_memory_load,
                        max_healthy_network_io_utilization,
                        evaluate_duration_secs,
                        custom: Default::default(),
                    };

                    template_data.validate().ok().map(|_| template_data)
                },
            )
    }
}
