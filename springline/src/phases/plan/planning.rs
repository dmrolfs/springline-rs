use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use crate::flink::Parallelism;
use crate::phases::decision::{DecisionDataT, DecisionOutcome, DecisionResult};
use crate::phases::plan::benchmark::Benchmark;
use crate::phases::plan::clipping::ClippingHandling;
use crate::phases::plan::context::PlanningContext;
use crate::phases::plan::forecast::{ForecastCalculator, ForecastInputs, Forecaster};
use crate::phases::plan::model::{ScaleParameters, ScalePlan};
use crate::phases::plan::performance_history::PerformanceHistory;
use crate::phases::plan::performance_repository::PerformanceRepository;
use crate::phases::plan::{
    BenchmarkRange, ClippingHandlingSettings, PlanningMeasurement, PlanningOutcome,
    PLANNING_CTX_FORECASTING_MAX_CATCH_UP_SECS, PLANNING_CTX_FORECASTING_RECOVERY_VALID_SECS,
    PLANNING_CTX_FORECASTING_RESTART_SECS, PLANNING_CTX_MIN_SCALING_STEP,
    PLANNING_FORECASTED_WORKLOAD,
};
use crate::phases::WindowDataT;
use crate::settings::Settings;
use crate::{math, Env};
use async_trait::async_trait;
use once_cell::sync::Lazy;
use pretty_snowflake::Label;
use proctor::elements::{RecordsPerSecond, Timestamp};
use proctor::error::PlanError;
use proctor::graph::{Outlet, Port};
use proctor::phases::plan::{PlanEvent, Planning};
use proctor::ReceivedAt;
use prometheus::core::{AtomicU64, GenericGauge};
use prometheus::Opts;
use tokio::sync::{broadcast, Mutex};

pub type FlinkPlanningMonitor = broadcast::Receiver<Arc<FlinkPlanningEvent>>;

#[derive(Debug, Clone, Label, PartialEq)]
pub enum FlinkPlanningEvent {
    ObservationAdded {
        observation: Env<PlanningMeasurement>,
        next_forecast: Option<(Timestamp, RecordsPerSecond)>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanningParameters {
    pub min_scaling_step: u32,
    pub evaluation_window: Option<Duration>,
    pub forecast_inputs: ForecastInputs,
    pub total_task_slots: Option<u32>,
    pub free_task_slots: Option<u32>,
    pub benchmarks: Vec<BenchmarkRange>,
    pub clipping_handling: ClippingHandlingSettings,
}

impl PlanningParameters {
    pub fn from_settings(settings: &Settings) -> Result<Self, PlanError> {
        let evaluation_window = settings
            .decision
            .template_data
            .as_ref()
            .and_then(|td| td.evaluate_duration_secs)
            .map(|s| Duration::from_secs(u64::from(s)));

        let forecast_inputs = ForecastInputs::from_settings(&settings.plan)?;

        Ok(Self {
            min_scaling_step: settings.plan.min_scaling_step,
            evaluation_window,
            forecast_inputs,
            total_task_slots: None,
            free_task_slots: None,
            benchmarks: settings.plan.benchmarks.clone(),
            clipping_handling: settings.plan.clipping_handling,
        })
    }
}

#[derive(Debug)]
pub struct FlinkPlanning<F: Forecaster> {
    name: String,
    min_scaling_step: u32,
    evaluation_window: Option<Duration>,
    total_task_slots: Option<u32>,
    free_task_slots: Option<u32>,
    clipping_handling: Mutex<ClippingHandling>,
    pub forecast_calculator: ForecastCalculator<F>,
    performance_history: PerformanceHistory,
    performance_repository: Box<dyn PerformanceRepository>,
    outlet: Option<Outlet<<Self as Planning>::Out>>,
    tx_monitor: broadcast::Sender<Arc<FlinkPlanningEvent>>,
}

impl<F: Forecaster> PartialEq for FlinkPlanning<F> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.total_task_slots == other.total_task_slots
            && self.free_task_slots == other.free_task_slots
            && self.min_scaling_step == other.min_scaling_step
            && self.forecast_calculator.inputs == other.forecast_calculator.inputs
            && self.performance_history == other.performance_history
    }
}

impl<F: Forecaster> FlinkPlanning<F> {
    pub async fn new(
        planning_name: &str, forecaster: F, performance_repository: Box<dyn PerformanceRepository>,
        params: PlanningParameters,
    ) -> Result<Self, PlanError> {
        performance_repository.check().await?;

        let forecast_calculator =
            ForecastCalculator::new(forecaster, params.forecast_inputs.clone())?;

        let name = planning_name.to_string();
        let performance_history =
            Self::initialize_performance_history(&name, performance_repository.as_ref(), &params)
                .await?;

        let (tx_monitor, _) = broadcast::channel(num_cpus::get() * 2);

        Ok(Self {
            name,
            min_scaling_step: params.min_scaling_step,
            evaluation_window: params.evaluation_window,
            total_task_slots: params.total_task_slots,
            free_task_slots: params.free_task_slots,
            clipping_handling: Mutex::new(ClippingHandling::new(&params.clipping_handling)),
            forecast_calculator,
            performance_history,
            performance_repository,
            outlet: None,
            tx_monitor,
        })
    }

    #[inline]
    pub fn rx_monitor(&self) -> FlinkPlanningMonitor {
        self.tx_monitor.subscribe()
    }

    #[tracing::instrument(level = "trace", skip(self, decision), fields(%decision))]
    pub async fn update_performance_history(
        &mut self, decision: &DecisionOutcome,
    ) -> Result<(), PlanError> {
        use crate::phases::decision::DecisionResult as DR;

        let update_repository = match decision.as_ref() {
            DR::NoAction(_) => false,
            DR::ScaleUp(data) => {
                let b = self.benchmark_from_data(data);
                self.performance_history.add_upper_benchmark(b);
                true
            },
            DR::ScaleDown(data) => {
                let b = self.benchmark_from_data(data);
                self.performance_history.add_lower_benchmark(b);
                true
            },
        };

        if update_repository {
            Self::update_performance_history_metrics(&self.performance_history);

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
}

#[async_trait]
impl<F: Forecaster> Planning for FlinkPlanning<F> {
    type Context = Env<PlanningContext>;
    type Decision = Env<DecisionResult<DecisionDataT>>;
    type Observation = Env<PlanningMeasurement>;
    type Out = PlanningOutcome;

    fn set_outlet(&mut self, outlet: Outlet<Self::Out>) {
        self.outlet = Some(outlet);
    }

    fn add_observation(&mut self, observation: Self::Observation) {
        self.forecast_calculator.add_observation(observation.clone());
        let next_forecast = self
            .forecast_calculator
            .calculate_next_workload(observation.recv_timestamp())
            .map(|forecast| {
                tracing::debug!(forecast_ts=%forecast.0, forecast_val=%forecast.1, "next observation forecasted.");
                forecast
            })
            .map_err(|err| {
                let (needed, window) = self.forecast_calculator.observations_needed();
                match &err {
                    PlanError::NotEnoughData { supplied, need } => {
                        tracing::info!(
                            supplied=%supplied, need=%need,
                            "not enough data to forecast next observation - need: {needed} of {window}."
                        );
                    },
                    e => {
                        tracing::warn!(error = ?e, "error while forecasting next observation.");
                    },
                };

                err
            })
            .ok();

        let event = Arc::new(FlinkPlanningEvent::ObservationAdded { observation, next_forecast });
        if let Err(err) = self.tx_monitor.send(event.clone()) {
            tracing::warn!(error=?err, ?event, "failed to publish ObservationAdded event")
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn patch_context(
        &mut self, context: Self::Context,
    ) -> Result<Option<PlanEvent<Self>>, PlanError> {
        context.patch_inputs(&mut self.forecast_calculator.inputs);
        self.total_task_slots = Some(context.total_task_slots);
        self.free_task_slots = Some(context.free_task_slots);
        tracing::debug!(
            ?context, forecast_inputs=?self.forecast_calculator.inputs,
            total_task_slots=?self.total_task_slots, free_task_slots=?self.free_task_slots,
            "patched planning context inputs."
        );

        self.update_metrics();

        Ok(Some(PlanEvent::ContextChanged(context)))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_decision(
        &mut self, decision: Self::Decision,
    ) -> Result<PlanEvent<Self>, PlanError> {
        self.update_performance_history(&decision).await?;

        if let DecisionResult::NoAction(_) = decision.as_ref() {
            self.handle_do_not_scale_decision(decision)
        } else {
            self.handle_plan_for_scale_decision(decision).await
        }
    }

    async fn close(self) -> Result<(), PlanError> {
        tracing::trace!("closing flink planning.");
        self.performance_repository.close().await?;
        if let Some(mut o) = self.outlet {
            o.close().await;
        }
        Ok(())
    }
}

impl<F: Forecaster> FlinkPlanning<F> {
    #[tracing::instrument(level = "info", skip(repository, parameters))]
    async fn initialize_performance_history(
        name: &str, repository: &dyn PerformanceRepository, parameters: &PlanningParameters,
    ) -> Result<PerformanceHistory, PlanError> {
        let history = match repository.load(name).await? {
            Some(history) => {
                tracing::info!(?history, "performance history loaded from repository.");
                Ok(history)
            },
            None if parameters.benchmarks.is_empty() => {
                let history = PerformanceHistory::default();
                tracing::info!(
                    ?history,
                    "no performance history loaded nor are there configured default benchmarks - history initialized as empty."
                );
                Ok(history)
            },
            None => {
                let mut history = PerformanceHistory::default();
                for b in parameters.benchmarks.iter() {
                    if let Some(hi_mark) = b.hi_mark() {
                        history.add_upper_benchmark(hi_mark);
                    }

                    if let Some(lo_mark) = b.lo_mark() {
                        history.add_lower_benchmark(lo_mark);
                    }
                }
                tracing::info!(
                    ?history,
                    "initialized performance history to configured default benchmarks."
                );
                Ok(history)
            },
        };

        if let Ok(ref h) = history {
            let nr_entries = u64::try_from(h.len()).unwrap_or(u64::MAX);
            PLANNING_PERFORMANCE_HISTORY_ENTRY_COUNT.set(nr_entries);
        }

        history
    }

    #[inline]
    fn update_performance_history_metrics(history: &PerformanceHistory) {
        let nr_entries: u64 = u64::try_from(history.len()).unwrap_or(u64::MAX);
        tracing::info!(?history, %nr_entries, "updating performance history entry count metric");
        PLANNING_PERFORMANCE_HISTORY_ENTRY_COUNT.set(nr_entries);
    }

    fn update_metrics(&self) {
        PLANNING_CTX_MIN_SCALING_STEP.set(u64::from(self.min_scaling_step));

        PLANNING_CTX_FORECASTING_MAX_CATCH_UP_SECS
            .set(self.forecast_calculator.inputs.max_catch_up.as_secs());

        PLANNING_CTX_FORECASTING_RECOVERY_VALID_SECS
            .set(self.forecast_calculator.inputs.valid_offset.as_secs());

        for (d, r) in self.forecast_calculator.inputs.direction_restart.iter() {
            PLANNING_CTX_FORECASTING_RESTART_SECS
                .with_label_values(&[(*d).into()])
                .set(r.as_secs());
        }
    }

    fn benchmark_from_data(&self, data: &DecisionDataT) -> Benchmark {
        self.evaluation_window
            .map(|eval_window| Benchmark::from_window(data, eval_window))
            .unwrap_or_else(|| data.latest().into())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn handle_do_not_scale_decision(
        &mut self, decision: <Self as Planning>::Decision,
    ) -> Result<PlanEvent<Self>, PlanError> {
        tracing::debug!("Decision made not scale cluster up or down.");
        Ok(PlanEvent::DecisionIgnored(decision))
    }

    #[tracing::instrument(
    level = "debug",
    skip(self, decision),
    fields(planning_name=%self.name, ?decision),
    )]
    async fn handle_plan_for_scale_decision(
        &mut self, decision: <Self as Planning>::Decision,
    ) -> Result<PlanEvent<Self>, PlanError> {
        let event = if self.forecast_calculator.have_enough_data() {
            let history = &self.performance_history;
            let decision_item = decision.item();
            let buffered_records = Self::buffered_lag_score(decision_item);
            let current_job_parallelism = decision_item.health.job_nonsource_max_parallelism;

            let forecasted_workload = self.forecast_calculator.calculate_target_rate(
                decision.recv_timestamp(),
                &decision.direction(),
                buffered_records,
            )?;
            PLANNING_FORECASTED_WORKLOAD.set(*forecasted_workload.as_ref());

            let required_job_parallelism =
                history.job_parallelism_for_workload(forecasted_workload);

            let _dmr_prior_clipping_point = self.clipping_handling.lock().await.clipping_point();
            let clipping_point = self.assess_for_job_clipping(&decision).await;
            let _dmr_set_clipping_point = self.clipping_handling.lock().await.clipping_point();

            let params = ScaleParameters {
                calculated_job_parallelism: required_job_parallelism,
                clipping_point,
                nr_task_managers: decision_item.cluster.nr_task_managers,
                total_task_slots: self.total_task_slots,
                free_task_slots: self.free_task_slots,
                min_scaling_step: self.min_scaling_step,
            };

            let decision_plan = decision.clone().map(|d| ScalePlan::new(d, params)).transpose();
            if let Some(plan) = decision_plan {
                if let Some(ref mut outlet) = self.outlet {
                    tracing::info!(?plan, "pushing scale plan.");
                    outlet.send(plan.clone()).await?;
                } else {
                    tracing::warn!(outlet=?self.outlet, ?plan, "wanted to push plan but could not since planning outlet is not set.");
                }

                PlanEvent::DecisionPlanned(decision, plan)
            } else {
                tracing::warn!(
                    ?required_job_parallelism, %current_job_parallelism,
                    "planning performance history suggests no change in job parallelism needed in spite of decision."
                );
                // todo: should we clear some of the history????
                PlanEvent::DecisionIgnored(decision)
            }
        } else {
            tracing::debug!(
                needed=%self.forecast_calculator.observations_needed().0,
                required=%self.forecast_calculator.observations_needed().1,
                "passing on planning decision since more observations are required to forecast workflow."
            );
            PlanEvent::DecisionIgnored(decision)
        };

        Ok(event)
    }

    /// Assess and tune the job's clipping level, and return the current effective clipping point to
    /// contribute to planning.
    ///
    /// Tests decision telemetry for prolonged signs of job instability, aka. "clipping", during which rescaling
    /// is not receiving necessary telemetry to make proper decisions. Although it requires a rescale decision,
    /// this step is here in order to facilitate converging on max, non-clipping parallelism level *and* to
    /// set a dynamic max parallelism accordingly. The clipping level could be maintained and identified in decision
    /// phase, but the extra data would need to be conveyed in the decision result.
    #[tracing::instrument(level = "info", skip(self, decision))]
    async fn assess_for_job_clipping(&mut self, decision: &DecisionOutcome) -> Option<Parallelism> {
        let item = decision.item();
        let is_clipping = match self.evaluation_window {
            Some(window) => {
                let looking_back_secs = math::saturating_u64_to_u32(window.as_secs());
                item.flow_is_source_consumer_telemetry_empty_over_window(looking_back_secs)
            },
            None => !item.flow.is_source_consumer_telemetry_populated(),
        };

        let mut handling = self.clipping_handling.lock().await;
        if is_clipping {
            handling.note_clipping(item.health.job_max_parallelism);
        }
        handling.clipping_point()
    }

    /// An attempt at cross-platform (wrt FlinkKafkaConsumer and FlinkKinesisConsumer at least)
    /// scoring of input lag. Precedence is given to Kafka. If neither are set, returns 0.0.
    /// The assumption is made that Kafka or Kinesis will be used ongoing. They obviously represent
    /// different units; however the planning calculation, if the lag score is of a consistent type,
    /// remains the same.
    #[allow(clippy::missing_const_for_fn)]
    fn buffered_lag_score(item: &WindowDataT) -> f64 {
        // todo: how to support other options?
        match (
            item.flow.source_records_lag_max,
            item.flow.source_millis_behind_latest,
        ) {
            (Some(lag), _) => f64::from(lag),
            (None, Some(lag)) => f64::from(lag),
            (None, None) => 0_f64,
        }
    }
}

pub static PLANNING_PERFORMANCE_HISTORY_ENTRY_COUNT: Lazy<GenericGauge<AtomicU64>> =
    Lazy::new(|| {
        GenericGauge::with_opts(
            Opts::new(
                "planning_performance_history_entry_count",
                "count of job parallelism benchmarks recorded",
            )
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
        )
        .expect("failed creating planning_performance_history_entry_count metric")
    });

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::phases::plan::tests::{setup_signal_history, SignalType, NOW};
    use chrono::{DateTime, Utc};
    use claim::*;
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::Id;
    use proctor::elements::telemetry;
    use proctor::{graph, MetaData};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tokio::sync::Mutex;
    use tokio_test::block_on;

    use super::*;
    use crate::flink::{
        AppDataWindow, ClusterMetrics, FlowMetrics, JobHealthMetrics, MetricCatalog,
    };
    use crate::model::NrReplicas;
    use crate::phases::plan::benchmark::*;
    use crate::phases::plan::forecast::*;
    use crate::phases::plan::performance_repository::*;
    use crate::phases::plan::{PerformanceRepositorySettings, ScaleDirection};

    type TestPlanning = FlinkPlanning<LeastSquaresWorkloadForecaster>;
    #[allow(dead_code)]
    type Calculator = ForecastCalculator<LeastSquaresWorkloadForecaster>;

    static CORRELATION: Lazy<Id<MetricCatalog>> =
        Lazy::new(|| Id::direct("MetricCatalog", 13, "ABC"));

    static META_DATA: Lazy<MetaData<MetricCatalog>> =
        Lazy::new(|| MetaData::from_parts(CORRELATION.clone(), NOW.into()));

    fn envelope_for<T: Label + Send>(content: T) -> Env<T> {
        Env::from_parts(META_DATA.clone().relabel(), content)
    }

    static METRICS: Lazy<Env<MetricCatalog>> = Lazy::new(|| {
        Env::from_parts(
            META_DATA.clone(),
            MetricCatalog {
                // correlation_id: CORRELATION.clone(),
                // recv_timestamp: Utc.timestamp(NOW, 0).into(),
                health: JobHealthMetrics {
                    job_nonsource_max_parallelism: Parallelism::new(4),
                    ..JobHealthMetrics::default()
                },
                flow: FlowMetrics {
                    // input_records_lag_max: 314.15926535897932384264,
                    source_records_lag_max: Some(314),
                    ..FlowMetrics::default()
                },
                cluster: ClusterMetrics {
                    nr_active_jobs: 1,
                    nr_task_managers: NrReplicas::new(4),
                    ..ClusterMetrics::default()
                },
                custom: telemetry::TableType::default(),
            },
        )
    });
    static SCALE_UP: Lazy<DecisionOutcome> = Lazy::new(|| {
        envelope_for(DecisionResult::ScaleUp(AppDataWindow::from_size(
            METRICS.clone(),
            1,
            Duration::from_secs(120),
        )))
    });
    static SCALE_DOWN: Lazy<DecisionOutcome> = Lazy::new(|| {
        envelope_for(DecisionResult::ScaleDown(AppDataWindow::from_size(
            METRICS.clone(),
            1,
            Duration::from_secs(120),
        )))
    });

    #[tracing::instrument(level = "info")]
    async fn setup_planning(
        planning_name: &str, outlet: Outlet<Env<ScalePlan>>, signal_type: SignalType,
        total_task_slots: u32,
    ) -> anyhow::Result<TestPlanning> {
        let mut calc = ForecastCalculator::new(
            LeastSquaresWorkloadForecaster::new(
                20,
                SpikeSettings { influence: 0.25, ..SpikeSettings::default() },
            ),
            ForecastInputs {
                direction_restart: maplit::hashmap! {
                    ScaleDirection::Up => Duration::from_secs(2 * 60),
                    ScaleDirection::Down => Duration::from_secs(2 * 60),
                },
                max_catch_up: Duration::from_secs(13 * 60), // max_catch_up
                valid_offset: Duration::from_secs(5 * 60),  // valid_offset
            },
        )
        .unwrap();

        let points: Vec<(DateTime<Utc>, f64)> = setup_signal_history(signal_type);

        tracing::info!(
            points=?points.iter().map(|(x, y)| (x.timestamp(), y)).collect::<Vec<_>>(),
            "observations prepared"
        );

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
            evaluation_window: Some(Duration::from_secs(120)),
            total_task_slots: Some(total_task_slots),
            free_task_slots: None,
            clipping_handling: Mutex::new(ClippingHandling::Ignore),
            forecast_calculator: calc,
            performance_history: PerformanceHistory::default(),
            performance_repository: make_performance_repository(&PerformanceRepositorySettings {
                storage: PerformanceRepositoryType::Memory,
                storage_path: None,
            })?,
            outlet: Some(outlet),
            tx_monitor,
        };

        Ok(planning)
    }

    #[tracing::instrument(level = "info", skip(planning, probe_rx))]
    async fn assert_scale_decision_scenario(
        label: &str, decision: &DecisionOutcome, planning: Arc<Mutex<TestPlanning>>,
        probe_rx: &mut Receiver<Env<ScalePlan>>, expected: Env<ScalePlan>,
    ) -> anyhow::Result<()> {
        tracing::warn!("testing {}...", label);

        let decision = decision.clone();
        let planning_2 = Arc::clone(&planning);

        let handle = tokio::spawn(async move {
            let p = &mut *planning_2.lock().await;
            assert_ok!(p.handle_plan_for_scale_decision(decision).await);
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

        let (probe_tx, mut probe_rx) = mpsc::channel(8);
        let outlet = Outlet::new("plan outlet", graph::PORT_DATA);
        let mut outlet_2 = outlet.clone();
        block_on(async move { outlet_2.attach("plan_outlet".into(), probe_tx).await });

        let recv_timestamp = METRICS.recv_timestamp();
        let block: anyhow::Result<()> = block_on(async move {
            let planning = Arc::new(Mutex::new(assert_ok!(
                setup_planning(
                    "planning_1",
                    outlet,
                    SignalType::Linear { slope: 1.0 },
                    METRICS.cluster.nr_task_managers.as_u32()
                )
                .await
            )));
            let min_step = planning.lock().await.min_scaling_step;

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-up with empty history",
                    &SCALE_UP,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    Env::from_parts(
                        MetaData::from_parts(CORRELATION.relabel(), recv_timestamp),
                        ScalePlan {
                            current_job_parallelism: METRICS.health.job_nonsource_max_parallelism,
                            target_job_parallelism: min_step
                                + METRICS.health.job_nonsource_max_parallelism,
                            target_nr_taskmanagers: min_step + METRICS.cluster.nr_task_managers,
                            current_nr_taskmanagers: METRICS.cluster.nr_task_managers,
                            task_slots_per_taskmanager: 1.0,
                        }
                    ),
                )
                .await
            );

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-down with empty history",
                    &SCALE_DOWN,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    Env::from_parts(
                        MetaData::from_parts(CORRELATION.relabel(), recv_timestamp),
                        ScalePlan {
                            // recv_timestamp,
                            // correlation_id: CORRELATION.clone(),
                            current_job_parallelism: METRICS.health.job_nonsource_max_parallelism,
                            target_job_parallelism: METRICS.health.job_nonsource_max_parallelism
                                - min_step,
                            target_nr_taskmanagers: METRICS.cluster.nr_task_managers - min_step,
                            current_nr_taskmanagers: METRICS.cluster.nr_task_managers,
                            task_slots_per_taskmanager: 1.0
                        }
                    ),
                )
                .await
            );

            planning.lock().await.min_scaling_step =
                METRICS.health.job_nonsource_max_parallelism.as_u32() + 1_000;

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-down with empty history and way bigger step down than nr task managers",
                    &SCALE_DOWN,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    Env::from_parts(
                        MetaData::from_parts(CORRELATION.relabel(), recv_timestamp),
                        ScalePlan {
                            current_job_parallelism: METRICS.health.job_nonsource_max_parallelism,
                            target_job_parallelism: Parallelism::MIN,
                            target_nr_taskmanagers: NrReplicas::new(Parallelism::MIN.as_u32()),
                            current_nr_taskmanagers: METRICS.cluster.nr_task_managers,
                            task_slots_per_taskmanager: 1.0,
                        }
                    ),
                )
                .await
            );

            Ok(())
        });

        block?;

        Ok(())
    }

    #[test]
    fn test_flink_linear_planning_handle_scale_decision() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_linear_planning_handle_scale_decision");
        let _main_span_guard = main_span.enter();

        let (probe_tx, mut probe_rx) = mpsc::channel(8);
        let outlet = Outlet::new("plan outlet", graph::PORT_DATA);
        let mut outlet_2 = outlet.clone();
        block_on(async move { outlet_2.attach("plan_outlet".into(), probe_tx).await });

        let recv_timestamp = METRICS.recv_timestamp();

        let block: anyhow::Result<()> = block_on(async move {
            let planning = Arc::new(Mutex::new(assert_ok!(
                setup_planning(
                    "planning_2",
                    outlet,
                    SignalType::Linear { slope: 1.0 },
                    METRICS.cluster.nr_task_managers.as_u32()
                )
                .await
            )));

            let mut performance_history = PerformanceHistory::default();
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(1), 55.0.into()));
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(10), 1500.0.into()));
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(15), 2000.0.into()));
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(20), 5000.0.into()));
            planning.lock().await.performance_history = performance_history;

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-up with some history",
                    &SCALE_UP,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    Env::from_parts(
                        MetaData::from_parts(CORRELATION.relabel(), recv_timestamp),
                        ScalePlan {
                            current_job_parallelism: METRICS.health.job_nonsource_max_parallelism,
                            target_job_parallelism: Parallelism::new(12),
                            target_nr_taskmanagers: NrReplicas::new(12),
                            current_nr_taskmanagers: METRICS.cluster.nr_task_managers,
                            task_slots_per_taskmanager: 1.0,
                        },
                    ),
                )
                .await
            );

            Ok(())
        });

        block?;

        Ok(())
    }

    #[test]
    fn test_flink_constant_planning_handle_scale_decision() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_constant_planning_handle_scale_decision");
        let _main_span_guard = main_span.enter();

        let (probe_tx, mut probe_rx) = mpsc::channel(8);
        let outlet = Outlet::new("plan outlet", graph::PORT_DATA);
        let mut outlet_2 = outlet.clone();
        block_on(async move { outlet_2.attach("plan_outlet".into(), probe_tx).await });

        let recv_timestamp = METRICS.recv_timestamp();

        let block: anyhow::Result<()> = block_on(async move {
            let planning = Arc::new(Mutex::new(assert_ok!(
                setup_planning(
                    "planning_2",
                    outlet,
                    SignalType::Constant { rate: 1472.0 },
                    METRICS.cluster.nr_task_managers.as_u32()
                )
                .await
            )));

            let mut performance_history = PerformanceHistory::default();
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(1), 55.0.into()));
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(10), 1500.0.into()));
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(15), 2000.0.into()));
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(20), 5000.0.into()));
            planning.lock().await.performance_history = performance_history;

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-up with some history",
                    &SCALE_UP,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    Env::from_parts(
                        MetaData::from_parts(CORRELATION.relabel(), recv_timestamp),
                        ScalePlan {
                            current_job_parallelism: METRICS.health.job_nonsource_max_parallelism,
                            target_job_parallelism: Parallelism::new(12),
                            target_nr_taskmanagers: NrReplicas::new(12),
                            current_nr_taskmanagers: METRICS.cluster.nr_task_managers,
                            task_slots_per_taskmanager: 1.0,
                        },
                    ),
                )
                .await
            );

            Ok(())
        });

        block?;

        Ok(())
    }

    #[test]
    fn test_flink_sine_planning_handle_scale_decision() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_sine_planning_handle_scale_decision");
        let _main_span_guard = main_span.enter();

        let (probe_tx, mut probe_rx) = mpsc::channel(8);
        let outlet = Outlet::new("plan outlet", graph::PORT_DATA);
        let mut outlet_2 = outlet.clone();
        block_on(async move { outlet_2.attach("plan_outlet".into(), probe_tx).await });

        let recv_timestamp = METRICS.recv_timestamp();

        let block: anyhow::Result<()> = block_on(async move {
            let planning = Arc::new(Mutex::new(assert_ok!(
                setup_planning(
                    "planning_2",
                    outlet,
                    SignalType::Sine,
                    METRICS.cluster.nr_task_managers.as_u32()
                )
                .await
            )));

            let mut performance_history = PerformanceHistory::default();
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(1), 55.0.into()));
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(10), 1500.0.into()));
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(15), 2000.0.into()));
            performance_history
                .add_upper_benchmark(Benchmark::new(Parallelism::new(20), 5000.0.into()));
            planning.lock().await.performance_history = performance_history;

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-up with some history",
                    &SCALE_UP,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    Env::from_parts(
                        MetaData::from_parts(CORRELATION.relabel(), recv_timestamp),
                        ScalePlan {
                            current_job_parallelism: METRICS.health.job_nonsource_max_parallelism,
                            target_job_parallelism: Parallelism::new(16),
                            target_nr_taskmanagers: NrReplicas::new(16),
                            current_nr_taskmanagers: METRICS.cluster.nr_task_managers,
                            task_slots_per_taskmanager: 1.0,
                        },
                    ),
                )
                .await
            );

            Ok(())
        });

        block?;

        Ok(())
    }

    #[test]
    fn test_patch_forecast_inputs_from_context() {
        block_on(async {
            let mut planning = assert_ok!(
                setup_planning(
                    "patch",
                    Outlet::new("patch", graph::PORT_DATA),
                    SignalType::Sine,
                    METRICS.cluster.nr_task_managers.as_u32()
                )
                .await
            );

            let expected = ForecastInputs {
                direction_restart: maplit::hashmap! {
                    ScaleDirection::Up => Duration::from_secs(2 * 60),
                    ScaleDirection::Down => Duration::from_secs(2 * 60),
                },
                max_catch_up: Duration::from_secs(13 * 60),
                valid_offset: Duration::from_secs(5 * 60),
            };
            assert_eq!(planning.forecast_calculator.inputs, expected);

            let expected = ForecastInputs {
                direction_restart: maplit::hashmap! {
                    ScaleDirection::Up => Duration::from_secs(33),
                    ScaleDirection::Down => Duration::from_secs(33),
                },
                valid_offset: Duration::from_secs(44),
                ..expected
            };

            let context = Env::new(PlanningContext {
                total_task_slots: 11,
                free_task_slots: 3,
                min_scaling_step: None,
                rescale_restart: expected.direction_restart.clone(),
                max_catch_up: None,
                recovery_valid: Some(expected.valid_offset),
            });

            let event = assert_some!(assert_ok!(planning.patch_context(context.clone())));
            assert_eq!(event, PlanEvent::ContextChanged(context));
            assert_eq!(planning.forecast_calculator.inputs, expected);
            assert_eq!(assert_some!(planning.total_task_slots), 11);
            assert_eq!(assert_some!(planning.free_task_slots), 3);
        })
    }
}
