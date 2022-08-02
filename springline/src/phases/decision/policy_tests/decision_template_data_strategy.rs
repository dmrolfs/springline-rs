use super::*;

#[derive(Debug, Clone)]
pub struct DecisionTemplateDataStrategyBuilder {
    pub basis: String,
    pub max_healthy_relative_lag_velocity: Option<BoxedStrategy<Option<f64>>>,
    pub max_healthy_lag: Option<BoxedStrategy<Option<f64>>>,
    pub min_task_utilization: Option<BoxedStrategy<Option<f64>>>,
    pub max_healthy_cpu_load: Option<BoxedStrategy<Option<f64>>>,
    pub max_healthy_heap_memory_load: Option<BoxedStrategy<Option<f64>>>,
    pub max_healthy_network_io_utilization: Option<BoxedStrategy<Option<f64>>>,
    pub evaluate_duration_secs: Option<BoxedStrategy<Option<u32>>>,
}

impl Default for DecisionTemplateDataStrategyBuilder {
    fn default() -> Self {
        Self {
            basis: "decision_basis".to_string(),
            max_healthy_relative_lag_velocity: None,
            max_healthy_lag: None,
            min_task_utilization: None,
            max_healthy_cpu_load: None,
            max_healthy_heap_memory_load: None,
            max_healthy_network_io_utilization: None,
            evaluate_duration_secs: None,
        }
    }
}

#[allow(dead_code)]
impl DecisionTemplateDataStrategyBuilder {
    pub fn strategy(basis: impl Into<String>) -> impl Strategy<Value = DecisionTemplateData> {
        Self::new(basis).finish()
    }

    pub fn new(basis: impl Into<String>) -> Self {
        Self { basis: basis.into(), ..Self::default() }
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

    pub fn max_healthy_lag(self, max_healthy_lag: impl Strategy<Value = Option<f64>> + 'static) -> Self {
        let mut new = self;
        new.max_healthy_lag = Some(max_healthy_lag.boxed());
        new
    }

    pub fn just_max_healthy_lag(self, max_healthy_lag: impl Into<Option<f64>>) -> Self {
        self.max_healthy_lag(Just(max_healthy_lag.into()))
    }

    pub fn min_task_utilization(self, min_task_utilization: impl Strategy<Value = Option<f64>> + 'static) -> Self {
        let mut new = self;
        new.min_task_utilization = Some(min_task_utilization.boxed());
        new
    }

    pub fn just_min_task_utilization(self, min_task_utilization: impl Into<Option<f64>>) -> Self {
        self.min_task_utilization(Just(min_task_utilization.into()))
    }

    pub fn max_healthy_cpu_load(self, max_healthy_cpu_load: impl Strategy<Value = Option<f64>> + 'static) -> Self {
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

    pub fn just_max_healthy_heap_memory_load(self, max_healthy_heap_memory_load: impl Into<Option<f64>>) -> Self {
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

    pub fn evaluate_duration_secs(self, evaluate_duration_secs: impl Strategy<Value = Option<u32>> + 'static) -> Self {
        let mut new = self;
        new.evaluate_duration_secs = Some(evaluate_duration_secs.boxed());
        new
    }

    pub fn just_evaluate_duration_secs(self, evaluate_duration_secs: impl Into<Option<u32>>) -> Self {
        self.evaluate_duration_secs(Just(evaluate_duration_secs.into()))
    }

    pub fn finish(self) -> impl Strategy<Value = DecisionTemplateData> {
        tracing::info!(?self, "DMR: building DecisionTemplateData strategy");
        let basis = self.basis.clone();
        let max_healthy_relative_lag_velocity = self
            .max_healthy_relative_lag_velocity
            .unwrap_or(prop::option::of(-1e10_f64..=1e10).boxed());
        let max_healthy_lag = self.max_healthy_lag.unwrap_or(prop::option::of(-1e10_f64..=1e10).boxed());
        let min_task_utilization = self
            .min_task_utilization
            .unwrap_or(prop::option::of(-1e10_f64..=1e10).boxed());
        let max_healthy_cpu_load = self
            .max_healthy_cpu_load
            .unwrap_or(prop::option::of(-1e10_f64..=1e10).boxed());
        let max_healthy_heap_memory_load = self
            .max_healthy_heap_memory_load
            .unwrap_or(prop::option::of(-1e10..=1e10).boxed());
        let max_healthy_network_io_utilization = self
            .max_healthy_network_io_utilization
            .unwrap_or(prop::option::of(-1e10..=1e10).boxed());
        let evaluate_duration_secs = self
            .evaluate_duration_secs
            .unwrap_or(prop::option::of(any::<u32>()).boxed());

        (
            max_healthy_relative_lag_velocity,
            max_healthy_lag,
            min_task_utilization,
            max_healthy_cpu_load,
            max_healthy_heap_memory_load,
            max_healthy_network_io_utilization,
            evaluate_duration_secs,
        )
            .prop_map(
                move |(
                    max_healthy_relative_lag_velocity,
                    max_healthy_lag,
                    min_task_utilization,
                    max_healthy_cpu_load,
                    max_healthy_heap_memory_load,
                    max_healthy_network_io_utilization,
                    evaluate_duration_secs,
                )| {
                    tracing::info!(
                        ?max_healthy_relative_lag_velocity,
                        ?max_healthy_lag,
                        ?min_task_utilization,
                        ?max_healthy_cpu_load,
                        ?max_healthy_heap_memory_load,
                        ?max_healthy_network_io_utilization,
                        ?evaluate_duration_secs,
                        "making DecisionTemplateData..."
                    );

                    DecisionTemplateData {
                        basis: basis.clone(),
                        max_healthy_relative_lag_velocity,
                        max_healthy_lag,
                        min_task_utilization,
                        max_healthy_cpu_load,
                        max_healthy_heap_memory_load,
                        max_healthy_network_io_utilization,
                        evaluate_duration_secs,
                        custom: Default::default(),
                    }
                },
            )
    }
}
