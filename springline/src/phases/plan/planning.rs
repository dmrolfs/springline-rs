use std::fmt::Debug;

use async_trait::async_trait;
use proctor::elements::RecordsPerSecond;
use proctor::error::PlanError;
use proctor::graph::{Inlet, Outlet};
use proctor::phases::plan::Planning;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::phases::decision::DecisionResult;
use crate::phases::plan::context::PlanningContext;
use crate::phases::plan::forecast::{ForecastCalculator, ForecastInputs, Forecaster};
use crate::phases::plan::model::ScalePlan;
use crate::phases::plan::performance_history::PerformanceHistory;
use crate::phases::plan::performance_repository::PerformanceRepository;
use crate::phases::plan::{PlanningMeasurement, PLANNING_FORECASTED_WORKLOAD};
use crate::phases::MetricCatalog;

// todo: this needs to be worked into Plan stage...  Need to determine best design
// todo: pub type FlinkPlanningApi = mpsc::UnboundedSender<FlinkPlanningCmd>;

// todo: this needs to be worked into Plan stage...  Need to determine best design
// todo: #[derive(Debug)]
// todo: pub enum FlinkPlanningCmd {
// todo:     GetHistory(oneshot::Sender<PerformanceHistory>),
// todo: }

pub type FlinkPlanningMonitor = broadcast::Receiver<Arc<FlinkPlanningEvent>>;

#[derive(Debug, Clone, PartialEq)]
pub enum FlinkPlanningEvent {
    ContextChanged(Option<PlanningContext>),
    ObservationAdded {
        observation: PlanningMeasurement,
        next_forecast: Option<RecordsPerSecond>,
    },
}

#[derive(Debug)]
pub struct FlinkPlanning<F: Forecaster> {
    name: String,
    min_scaling_step: usize,
    forecast_calculator: ForecastCalculator<F>,
    performance_history: PerformanceHistory,
    performance_repository: Box<dyn PerformanceRepository>,
    context_inlet: Inlet<PlanningContext>,
    outlet: Option<Outlet<ScalePlan>>,
    tx_monitor: broadcast::Sender<Arc<FlinkPlanningEvent>>,
    /* todo: tx_api: mpsc::UnboundedSender<FlinkPlanningCmd>,
     * todo: rx_api: mpsc::UnboundedReceiver<FlinkPlanningCmd>, */
}

impl<F: Forecaster> FlinkPlanning<F> {
    pub async fn new(
        planning_name: &str, min_scaling_step: usize, inputs: ForecastInputs, forecaster: F,
        performance_repository: Box<dyn PerformanceRepository>, context_inlet: Inlet<PlanningContext>,
    ) -> Result<Self, PlanError> {
        performance_repository.check().await?;

        let forecast_calculator = ForecastCalculator::new(forecaster, inputs)?;

        let name = planning_name.to_string();
        let performance_history = performance_repository.load(name.as_str()).await?.unwrap_or_default();

        // todo: this needs to be worked into Plan stage...  Need to determine best design
        // todo: let (tx_api, rx_api) = mpsc::unbounded_channel();
        let (tx_monitor, _) = broadcast::channel(num_cpus::get() * 2);

        Ok(Self {
            name,
            min_scaling_step,
            forecast_calculator,
            performance_history,
            performance_repository,
            context_inlet,
            outlet: None,
            tx_monitor,
            /* todo: tx_api,
             * todo: rx_api, */
        })
    }

    #[inline]
    pub fn rx_monitor(&self) -> FlinkPlanningMonitor {
        self.tx_monitor.subscribe()
    }

    #[tracing::instrument(level = "info", skip(self, decision), fields(%decision))]
    pub async fn update_performance_history(
        &mut self, decision: &DecisionResult<MetricCatalog>,
    ) -> Result<(), PlanError> {
        use crate::phases::decision::DecisionResult as DR;

        let update_repository = match decision {
            DR::NoAction(_) => false,
            DR::ScaleUp(metrics) => {
                self.performance_history.add_upper_benchmark(metrics.into());
                true
            },
            DR::ScaleDown(metrics) => {
                self.performance_history.add_lower_benchmark(metrics.into());
                true
            },
        };

        if update_repository {
            if let Err(err) = self
                .performance_repository
                .save(self.name.as_str(), &self.performance_history)
                .await
            {
                tracing::error!(error=?err, job=%self.name, "failed to save planning history.");
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, _metrics))]
    fn handle_do_not_scale_decision(&mut self, _metrics: &MetricCatalog) -> Result<Option<ScalePlan>, PlanError> {
        tracing::debug!("Decision made not scale cluster up or down.");
        Ok(None)
    }

    #[tracing::instrument(
        level = "info",
        skip(self, decision),
        fields(planning_name=%self.name, %decision),
    )]
    async fn handle_scale_decision(
        &mut self, decision: DecisionResult<MetricCatalog>,
    ) -> Result<Option<ScalePlan>, PlanError> {
        let plan = if self.forecast_calculator.have_enough_data() {
            let history = &self.performance_history;
            let current_nr_task_managers = decision.item().cluster.nr_task_managers;
            let buffered_records = Self::buffered_lag_score(decision.item());

            let forecasted_workload = self
                .forecast_calculator
                .calculate_target_rate(decision.item().recv_timestamp, buffered_records)?;
            PLANNING_FORECASTED_WORKLOAD.set(*forecasted_workload.as_ref());

            let required_nr_task_managers = history.cluster_size_for_workload(forecasted_workload);

            if let Some(plan) = ScalePlan::new(decision, required_nr_task_managers, self.min_scaling_step) {
                if let Some(ref mut outlet) = self.outlet {
                    tracing::info!(?plan, "pushing scale plan.");
                    outlet.send(plan.clone()).await?;
                } else {
                    tracing::warn!(outlet=?self.outlet, ?plan, "wanted to push plan but could not since planning outlet is not set.");
                }
                Some(plan)
            } else {
                tracing::warn!(
                    ?required_nr_task_managers,
                    %current_nr_task_managers,
                    "performance history suggests no change in cluster size needed."
                );
                // todo: should we clear some of the history????
                None
            }
        } else {
            tracing::info!(
                needed=%self.forecast_calculator.observations_needed().0,
                required=%self.forecast_calculator.observations_needed().1,
                "passing on planning decision since more observations are required to forecast workflow."
            );
            None
        };

        Ok(plan)
    }

    /// An attempt at cross-platform (wrt FlinkKafkaConsumer and FlinkKinesisConsumer at least)
    /// scoring of input lag. Precedence is given to Kafka. If neither are set, returns 0.0.
    /// The assumption is made that Kafka or Kinesis will be used ongoing. They obviously represent
    /// different units; however the planning calculation, if the lag score is of a consistent type,
    /// remains the same.
    fn buffered_lag_score(item: &MetricCatalog) -> f64 {
        // todo: how to support other options?
        match (item.flow.input_records_lag_max, item.flow.input_millis_behind_latest) {
            (Some(lag), _) => lag as f64,
            (None, Some(lag)) => lag as f64,
            (None, None) => 0_f64,
        }
    }
}

#[async_trait]
impl<F: Forecaster> Planning for FlinkPlanning<F> {
    type Observation = super::PlanningMeasurement;
    type Decision = DecisionResult<MetricCatalog>;
    type Out = ScalePlan;

    fn set_outlet(&mut self, outlet: Outlet<Self::Out>) {
        self.outlet = Some(outlet);
    }

    fn add_observation(&mut self, observation: Self::Observation) {
        self.forecast_calculator.add_observation(observation.clone());
        let next_forecast = self
            .forecast_calculator
            .calculate_next_workload(observation.recv_timestamp)
            .map(|forecast| {
                tracing::info!(?forecast, "next observation forecasted.");
                forecast
            })
            .map_err(|err| {
                let (needed, window) = self.forecast_calculator.observations_needed();
                tracing::warn!(
                    error=?err,
                    "failed to forecast next workload -- needed: {} of {}",
                    needed, window
                );
                err
            })
            .ok();

        let event = Arc::new(FlinkPlanningEvent::ObservationAdded { observation, next_forecast });
        if let Err(err) = self.tx_monitor.send(event.clone()) {
            tracing::warn!(error=?err, ?event, "failed to publish ObservationAdded event")
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn handle_decision(&mut self, decision: Self::Decision) -> Result<Option<ScalePlan>, PlanError> {
        self.update_performance_history(&decision).await?;

        let plan = if let DecisionResult::NoAction(ref catalog) = decision {
            self.handle_do_not_scale_decision(catalog)?
        } else {
            self.handle_scale_decision(decision).await?
        };

        Ok(plan)
    }

    async fn close(self) -> Result<(), PlanError> {
        tracing::trace!("closing flink planning.");
        self.performance_repository.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::{DateTime, TimeZone, Utc};
    use claim::*;
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::Id;
    use proctor::elements::telemetry;
    use proctor::graph;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tokio::sync::Mutex;
    use tokio_test::block_on;

    use super::*;
    use crate::phases::plan::benchmark::*;
    use crate::phases::plan::forecast::*;
    use crate::phases::plan::performance_repository::*;
    use crate::phases::plan::{PerformanceRepositorySettings, MINIMAL_CLUSTER_SIZE};
    use crate::phases::{ClusterMetrics, FlowMetrics, JobHealthMetrics};

    type TestPlanning = FlinkPlanning<LeastSquaresWorkloadForecaster>;
    #[allow(dead_code)]
    type Calculator = ForecastCalculator<LeastSquaresWorkloadForecaster>;

    const STEP: i64 = 15;
    const NOW: i64 = 1624061766 + (30 * STEP);

    static CORRELATION: Lazy<Id<MetricCatalog>> = Lazy::new(|| Id::direct("MetricCatalog", 13, "ABC"));
    static METRICS: Lazy<MetricCatalog> = Lazy::new(|| MetricCatalog {
        correlation_id: CORRELATION.clone(),
        recv_timestamp: Utc.timestamp(NOW, 0).into(),
        health: JobHealthMetrics::default(),
        flow: FlowMetrics {
            // input_records_lag_max: 314.15926535897932384264,
            input_records_lag_max: Some(314),
            ..FlowMetrics::default()
        },
        cluster: ClusterMetrics {
            nr_active_jobs: 1,
            nr_task_managers: 4,
            ..ClusterMetrics::default()
        },
        custom: telemetry::TableType::default(),
    });
    static SCALE_UP: Lazy<DecisionResult<MetricCatalog>> = Lazy::new(|| DecisionResult::ScaleUp(METRICS.clone()));
    static SCALE_DOWN: Lazy<DecisionResult<MetricCatalog>> = Lazy::new(|| DecisionResult::ScaleDown(METRICS.clone()));
    static NO_SCALE: Lazy<DecisionResult<MetricCatalog>> = Lazy::new(|| DecisionResult::NoAction(METRICS.clone()));

    enum SignalType {
        Sine,
        Linear,
    }

    async fn setup_planning(
        planning_name: &str, context_inlet: Inlet<PlanningContext>, outlet: Outlet<ScalePlan>, signal_type: SignalType,
    ) -> anyhow::Result<Arc<Mutex<TestPlanning>>> {
        let mut calc = ForecastCalculator::new(
            LeastSquaresWorkloadForecaster::new(20, SpikeSettings { influence: 0.25, ..SpikeSettings::default() }),
            ForecastInputs {
                restart: Duration::from_secs(2 * 60),       // restart
                max_catch_up: Duration::from_secs(13 * 60), // max_catch_up
                valid_offset: Duration::from_secs(5 * 60),  // valid_offset
            },
        )
        .unwrap();

        let points: Vec<(DateTime<Utc>, f64)> = match signal_type {
            SignalType::Linear => {
                let total = 30;
                (1..=total)
                    .into_iter()
                    .map(|tick| {
                        let x = Utc.timestamp(NOW - (total - tick) * STEP, 0);
                        let y = tick as f64;
                        (x, y)
                    })
                    .collect()
            },

            SignalType::Sine => {
                let total = 30;
                (1..=total)
                    .into_iter()
                    .map(|tick| {
                        let x = Utc.timestamp(NOW - (total - tick) * STEP, 0);
                        let y = 1000. * ((tick as f64) / 15.).sin();
                        (x, y)
                    })
                    .collect()
            },
        };

        points.into_iter().for_each(|(ts, value)| {
            calc.add_observation(WorkloadMeasurement {
                timestamp_secs: ts.timestamp(),
                workload: value.into(),
            })
        });

        let (tx_monitor, _) = broadcast::channel(num_cpus::get() * 2);
        let planning = FlinkPlanning {
            name: planning_name.to_string(),
            min_scaling_step: 2,
            forecast_calculator: calc,
            performance_history: PerformanceHistory::default(),
            performance_repository: make_performance_repository(&PerformanceRepositorySettings {
                storage: PerformanceRepositoryType::Memory,
                storage_path: None,
            })?,
            context_inlet,
            outlet: Some(outlet),
            tx_monitor,
        };

        Ok(Arc::new(Mutex::new(planning)))
    }

    #[tracing::instrument(level = "info", skip(planning, probe_rx))]
    async fn assert_scale_decision_scenario(
        label: &str,
        decision: &DecisionResult<MetricCatalog>,
        planning: Arc<Mutex<TestPlanning>>,

        // calculator: Arc<Mutex<ForecastCalculator<LeastSquaresWorkloadForecastBuilder>>>,
        // history: PerformanceHistory,
        // outlet: Outlet<FlinkScalePlan>,
        probe_rx: &mut Receiver<ScalePlan>,
        // min_step: usize,
        expected: ScalePlan,
    ) -> anyhow::Result<()> {
        tracing::warn!("DMR - testing {}...", label);

        let decision = decision.clone();
        let planning_2 = Arc::clone(&planning);
        // let calculator_2 = Arc::clone(&calculator);
        // let history = Arc::new(Mutex::new(history));
        // let history_2 = Arc::clone(&history);

        let handle = tokio::spawn(async move {
            // let c = &mut *calculator_2.lock().await;
            // let h = &mut *history_2.lock().await;
            // assert_ok!(FlinkScalePlanning::handle_scale_decision(decision, c, h, &outlet, min_step).await);
            let p = &mut *planning_2.lock().await;
            assert_ok!(p.handle_scale_decision(decision).await);
        });
        assert_ok!(handle.await);

        let actual = probe_rx.recv().await;
        let p = planning.lock().await;
        tracing::info!(?expected, ?actual, history=?p.performance_history, calculator=?p.forecast_calculator, "results for {}", label);
        assert_eq!(actual, Some(expected));

        Ok(())
    }

    #[test]
    fn test_flink_planning_handle_empty_scale_decision() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_planning_handle_empty_scale_decision");
        let _main_span_guard = main_span.enter();

        let (tx_context, rx_context) = mpsc::channel(8);
        let context_inlet = Inlet::new("plan context", graph::PORT_CONTEXT);
        let mut context_inlet_2 = context_inlet.clone();
        block_on(async move { context_inlet_2.attach("plan_context_inlet".into(), rx_context).await });

        let (probe_tx, mut probe_rx) = mpsc::channel(8);
        let outlet = Outlet::new("plan outlet", graph::PORT_DATA);
        let mut outlet_2 = outlet.clone();
        block_on(async move { outlet_2.attach("plan_outlet".into(), probe_tx).await });

        let recv_timestamp = METRICS.recv_timestamp;
        let block: anyhow::Result<()> = block_on(async move {
            let planning = setup_planning("planning_1", context_inlet, outlet, SignalType::Linear).await?;
            let min_step = planning.lock().await.min_scaling_step;

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-up with empty history",
                    &SCALE_UP,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    ScalePlan {
                        recv_timestamp,
                        correlation_id: CORRELATION.clone(),
                        target_nr_task_managers: min_step as u32 + METRICS.cluster.nr_task_managers,
                        current_nr_task_managers: METRICS.cluster.nr_task_managers,
                    },
                )
                .await
            );

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-down with empty history",
                    &SCALE_DOWN,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    ScalePlan {
                        recv_timestamp,
                        correlation_id: CORRELATION.clone(),
                        target_nr_task_managers: METRICS.cluster.nr_task_managers - min_step as u32,
                        current_nr_task_managers: METRICS.cluster.nr_task_managers,
                    },
                )
                .await
            );

            planning.lock().await.min_scaling_step = METRICS.cluster.nr_task_managers as usize + 1_000;

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-down with empty history and way bigger step down than nr task managers",
                    &SCALE_DOWN,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    ScalePlan {
                        recv_timestamp,
                        correlation_id: CORRELATION.clone(),
                        target_nr_task_managers: MINIMAL_CLUSTER_SIZE as u32,
                        current_nr_task_managers: METRICS.cluster.nr_task_managers,
                    },
                )
                .await
            );

            Ok(())
        });

        block?;

        Ok(())
    }

    #[test]
    fn test_flink_planning_handle_scale_decision() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_planning_handle_scale_decision");
        let _main_span_guard = main_span.enter();

        let (tx_context, rx_context) = mpsc::channel(8);
        let context_inlet = Inlet::new("plan context", graph::PORT_CONTEXT);
        let mut context_inlet_2 = context_inlet.clone();
        block_on(async move { context_inlet_2.attach("plan_context_inlet".into(), rx_context).await });

        let (probe_tx, mut probe_rx) = mpsc::channel(8);
        let outlet = Outlet::new("plan outlet", graph::PORT_DATA);
        let mut outlet_2 = outlet.clone();
        block_on(async move { outlet_2.attach("plan_outlet".into(), probe_tx).await });

        let recv_timestamp = METRICS.recv_timestamp;

        let block: anyhow::Result<()> = block_on(async move {
            let planning = setup_planning("planning_2", context_inlet, outlet, SignalType::Sine).await?;

            let mut performance_history = PerformanceHistory::default();
            performance_history.add_upper_benchmark(Benchmark::new(1, 55.0.into()));
            performance_history.add_upper_benchmark(Benchmark::new(10, 1500.0.into()));
            performance_history.add_upper_benchmark(Benchmark::new(15, 2000.0.into()));
            performance_history.add_upper_benchmark(Benchmark::new(20, 5000.0.into()));
            planning.lock().await.performance_history = performance_history;

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-up with some history",
                    &SCALE_UP,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    ScalePlan {
                        recv_timestamp,
                        correlation_id: CORRELATION.clone(),
                        target_nr_task_managers: 16,
                        current_nr_task_managers: METRICS.cluster.nr_task_managers,
                    },
                )
                .await
            );

            Ok(())
        });

        block?;

        Ok(())
    }
}
