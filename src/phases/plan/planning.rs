use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;

use crate::phases::decision::result::DecisionResult;
use crate::phases::plan::{
    FlinkScalePlan, ForecastCalculator, PerformanceHistory, PerformanceRepository,
    WorkloadForecastBuilder,
};
use crate::phases::MetricCatalog;
use proctor::error::PlanError;
use proctor::graph::Outlet;
use proctor::phases::plan::Planning;

// todo: this needs to be worked into Plan stage...  Need to determine best design
// todo: pub type FlinkPlanningApi = mpsc::UnboundedSender<FlinkPlanningCmd>;

// todo: this needs to be worked into Plan stage...  Need to determine best design
// todo: #[derive(Debug)]
// todo: pub enum FlinkPlanningCmd {
// todo:     GetHistory(oneshot::Sender<PerformanceHistory>),
// todo: }

#[derive(Debug)]
pub struct FlinkPlanning<F: WorkloadForecastBuilder> {
    name: String,
    min_scaling_step: u16,
    forecast_calculator: ForecastCalculator<F>,
    performance_history: PerformanceHistory,
    performance_repository: Box<dyn PerformanceRepository>,
    outlet: Option<Outlet<FlinkScalePlan>>,
    /* todo: tx_api: mpsc::UnboundedSender<FlinkPlanningCmd>,
     * todo: rx_api: mpsc::UnboundedReceiver<FlinkPlanningCmd>, */
}

impl<F: WorkloadForecastBuilder> FlinkPlanning<F> {
    pub async fn new(
        planning_name: impl AsRef<str>,
        min_scaling_step: u16,
        restart: Duration,
        max_catch_up: Duration,
        recovery_valid: Duration,
        forecast_builder: F,
        performance_repository: Box<dyn PerformanceRepository>,
    ) -> Result<Self, PlanError> {
        let forecast_calculator =
            ForecastCalculator::new(forecast_builder, restart, max_catch_up, recovery_valid)?;

        let performance_history = performance_repository
            .load(planning_name.as_ref())
            .await?
            .unwrap_or(PerformanceHistory::default());

        // todo: this needs to be worked into Plan stage...  Need to determine best design
        // todo: let (tx_api, rx_api) = mpsc::unbounded_channel();

        Ok(Self {
            name: planning_name.as_ref().to_string(),
            min_scaling_step,
            forecast_calculator,
            performance_history,
            performance_repository,
            outlet: None,
            /* todo: tx_api,
             * todo: rx_api, */
        })
    }

    #[tracing::instrument(level = "info", skip(self, decision), fields(%decision))]
    pub async fn update_performance_history(
        &mut self,
        decision: &DecisionResult<MetricCatalog>,
    ) -> Result<(), PlanError> {
        use crate::phases::decision::result::DecisionResult as DR;

        let update_repository = match decision {
            DR::NoAction(_) => false,
            DR::ScaleUp(metrics) => {
                self.performance_history.add_upper_benchmark(metrics.into());
                true
            }
            DR::ScaleDown(metrics) => {
                self.performance_history.add_lower_benchmark(metrics.into());
                true
            }
        };

        if update_repository {
            self.performance_repository
                .save(self.name.as_str(), &self.performance_history)
                .await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, _metrics))]
    fn handle_do_not_scale_decision(
        &mut self,
        _metrics: &MetricCatalog,
    ) -> Result<Option<FlinkScalePlan>, PlanError> {
        tracing::debug!("Decision made not scale cluster up or down.");
        Ok(None)
    }

    #[tracing::instrument(
        level = "info",
        skip(self, decision),
        fields(planning_name=%self.name, %decision),
    )]
    async fn handle_scale_decision(
        &mut self,
        decision: DecisionResult<MetricCatalog>,
    ) -> Result<Option<FlinkScalePlan>, PlanError> {
        let plan = if self.forecast_calculator.have_enough_data() {
            let history = &self.performance_history;
            let current_nr_task_managers = decision.item().cluster.nr_task_managers;
            let buffered_records = decision.item().flow.input_consumer_lag; // todo: how to support other options?
            let anticipated_workload = self
                .forecast_calculator
                .calculate_target_rate(decision.item().timestamp, buffered_records)?;
            let required_nr_task_managers = history.cluster_size_for_workload(anticipated_workload);

            if let Some(plan) =
                FlinkScalePlan::new(decision, required_nr_task_managers, self.min_scaling_step)
            {
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
}

#[async_trait]
impl<F: WorkloadForecastBuilder> Planning for FlinkPlanning<F> {
    type Observation = MetricCatalog;
    type Decision = DecisionResult<MetricCatalog>;
    type Out = FlinkScalePlan;

    fn set_outlet(&mut self, outlet: Outlet<Self::Out>) {
        self.outlet = Some(outlet);
    }

    fn add_observation(&mut self, observation: Self::Observation) {
        self.forecast_calculator.add_observation(observation.into());
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn handle_decision(
        &mut self,
        decision: Self::Decision,
    ) -> Result<Option<FlinkScalePlan>, PlanError> {
        self.update_performance_history(&decision).await?;

        let plan = if let DecisionResult::NoAction(ref metrics) = decision {
            self.handle_do_not_scale_decision(metrics)?
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

// pub struct FlinkScalePlanning<F: WorkloadForecastBuilder> {
//     name: String,
//     inlet: Inlet<MetricCatalog>,
//     decision_inlet: Inlet<DecisionResult<MetricCatalog>>,
//     outlet: Outlet<FlinkScalePlan>,
//     forecast_calculator: ForecastCalculator<F>,
//     performance_repository: Box<dyn PerformanceRepository>,
//     min_scaling_step: u16,
// }
//
// impl<F: WorkloadForecastBuilder> FlinkScalePlanning<F> {
//     #[tracing::instrument(level = "info", skip(name))]
//     pub fn new(
//         name: &str, restart_duration: Duration, max_catch_up_duration: Duration,
// recovery_valid_offset: Duration,         forecast_builder: F, performance_repository: Box<dyn
// PerformanceRepository>, min_scaling_step: u16,     ) -> Result<Self, PlanError> {
//         let name = name.to_string();
//         let inlet = Inlet::new(name.clone());
//         let decision_inlet = Inlet::new(format!("decision_{}", name.clone()));
//         let outlet = Outlet::new(name.clone());
//         let forecast_calculator = ForecastCalculator::new(
//             forecast_builder,
//             restart_duration,
//             max_catch_up_duration,
//             recovery_valid_offset,
//         )?;
//
//         Ok(Self {
//             name,
//             inlet,
//             decision_inlet,
//             outlet,
//             forecast_calculator,
//             performance_repository,
//             min_scaling_step,
//         })
//     }
// }
//
// impl<F: WorkloadForecastBuilder> Debug for FlinkScalePlanning<F> {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("FlinkScalePlanning")
//             .field("name", &self.name)
//             .field("inlet", &self.inlet)
//             .field("decision_inlet", &self.decision_inlet)
//             .field("outlet", &self.outlet)
//             .field("forecast_calculator", &self.forecast_calculator)
//             .field("performance_repository", &self.performance_repository)
//             .field("min_scaling_step", &self.min_scaling_step)
//             .finish()
//     }
// }
//
// impl<F: WorkloadForecastBuilder> SourceShape for FlinkScalePlanning<F> {
//     type Out = FlinkScalePlan;
//
//     fn outlet(&self) -> Outlet<Self::Out> {
//         self.outlet.clone()
//     }
// }
//
// impl<F: WorkloadForecastBuilder> SinkShape for FlinkScalePlanning<F> {
//     type In = MetricCatalog;
//
//     fn inlet(&self) -> Inlet<Self::In> {
//         self.inlet.clone()
//     }
// }
//
// impl<F: 'static + WorkloadForecastBuilder> DataDecisionStage for FlinkScalePlanning<F> {
//     type Decision = DecisionResult<MetricCatalog>;
//
//     fn decision_inlet(&self) -> Inlet<Self::Decision> {
//         self.decision_inlet.clone()
//     }
// }
//
// #[dyn_upcast]
// #[async_trait]
// impl<F: 'static + WorkloadForecastBuilder> Stage for FlinkScalePlanning<F> {
//     fn name(&self) -> &str {
//         self.name.as_str()
//     }
//
//     #[tracing::instrument(level = "info", skip(self))]
//     async fn check(&self) -> ProctorResult<()> {
//         self.do_check().await?;
//         Ok(())
//     }
//
//     #[tracing::instrument(level = "info", name = "run Flink planning phase", skip(self))]
//     async fn run(&mut self) -> ProctorResult<()> {
//         self.do_run().await?;
//         Ok(())
//     }
//
//     #[tracing::instrument(level = "info", skip(self))]
//     async fn close(mut self: Box<Self>) -> ProctorResult<()> {
//         self.do_close().await?;
//         Ok(())
//     }
// }
//
// impl<F: 'static + WorkloadForecastBuilder> FlinkScalePlanning<F> {
//     async fn do_check(&self) -> Result<(), PlanError> {
//         self.inlet.check_attachment().await?;
//         self.decision_inlet.check_attachment().await?;
//         self.outlet.check_attachment().await?;
//         Ok(())
//     }
//
//     async fn do_run(&mut self) -> Result<(), PlanError> {
//         use crate::flink::decision::result::DecisionResult as DR;
//
//         let name = self.name.clone();
//         let outlet = &self.outlet;
//         let rx_data = &mut self.inlet;
//         let rx_decision = &mut self.decision_inlet;
//         let calculator = &mut self.forecast_calculator;
//         let performance_repository = &mut self.performance_repository;
//
//         let mut performance_history = performance_repository
//             .load(name.as_str())
//             .await?
//             .unwrap_or(PerformanceHistory::default());
//
//         loop {
//             tokio::select! {
//                 Some(data) = rx_data.recv() => {
//                     calculator.add_observation(data.into());
//                 },
//
//                 Some(decision) = rx_decision.recv() => {
//                     Self::update_performance_history(&decision, name.as_str(), &mut
// performance_history, performance_repository).await?;
//
//                     if let DR::NoAction(ref metrics) = decision {
//                         Self::handle_do_not_scale_decision(metrics)?
//                     } else {
//                         Self::handle_scale_decision(decision, calculator, &mut
// performance_history, outlet, self.min_scaling_step).await?                     };
//                 },
//
//                 else => {
//                     tracing::info!("Flink scale planning done - breaking...");
//                     break;
//                 },
//             }
//         }
//
//         Ok(())
//     }
//
//     #[tracing::instrument(level = "info", skip())]
//     async fn update_performance_history(
//         decision: &DecisionResult<MetricCatalog>, name: &str, performance_history: &mut
// PerformanceHistory,         repository: &mut Box<dyn PerformanceRepository>,
//     ) -> Result<(), PlanError> {
//         use crate::flink::decision::result::DecisionResult as DR;
//
//         let update_repository = match decision {
//             DR::NoAction(_) => false,
//             DR::ScaleUp(metrics) => {
//                 performance_history.add_upper_benchmark(metrics.into());
//                 true
//             },
//             DR::ScaleDown(metrics) => {
//                 performance_history.add_lower_benchmark(metrics.into());
//                 true
//             },
//         };
//
//         if update_repository {
//             repository.save(name, performance_history).await?;
//         }
//
//         Ok(())
//     }
//
//     #[tracing::instrument(level = "debug", skip(_metrics))]
//     fn handle_do_not_scale_decision(_metrics: &MetricCatalog) -> Result<(), PlanError> {
//         tracing::debug!("Decision made not scale cluster up or down.");
//         Ok(())
//     }
//
//     #[tracing::instrument(level = "info", skip(outlet))]
//     async fn handle_scale_decision(
//         decision: DecisionResult<MetricCatalog>, calculator: &mut ForecastCalculator<F>,
//         performance_history: &mut PerformanceHistory, outlet: &Outlet<FlinkScalePlan>,
// min_scaling_step: u16,     ) -> Result<(), PlanError> {
//         if calculator.have_enough_data() {
//             let current_nr_task_managers = decision.item().cluster.nr_task_managers;
//             let buffered_records = decision.item().flow.input_consumer_lag; // todo: how to
// support other options             let anticipated_workload =
// calculator.calculate_target_rate(decision.item().timestamp, buffered_records)?;             let
// required_nr_task_managers = performance_history.cluster_size_for_workload(anticipated_workload);
//             if let Some(plan) = FlinkScalePlan::new(decision, required_nr_task_managers,
// min_scaling_step) {                 tracing::info!(?plan, "pushing scale plan.");
//                 outlet.send(plan).await?;
//             } else {
//                 tracing::warn!(
//                     ?required_nr_task_managers,
//                     %current_nr_task_managers,
//                     "performance history suggests no change in cluster size needed."
//                 );
//                 // todo: should we clear some of the history????
//             }
//         } else {
//             tracing::info!(
//                 needed=%calculator.observations_needed().0,
//                 required=%calculator.observations_needed().1,
//                 "passing on planning decision since more observations are required to forecast
// workflow."             )
//         }
//
//         Ok(())
//     }
//
//     async fn do_close(mut self: Box<Self>) -> Result<(), PlanError> {
//         tracing::trace!("closing flink scale planning ports.");
//         self.inlet.close().await;
//         self.decision_inlet.close().await;
//         self.outlet.close().await;
//         self.performance_repository.close().await?;
//         Ok(())
//     }
// }

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, TimeZone, Utc};
    use claim::*;
    use lazy_static::lazy_static;
    use pretty_assertions::assert_eq;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tokio::sync::Mutex;
    use tokio_test::block_on;

    use super::*;
    use crate::phases::plan::forecast::*;
    use crate::phases::plan::{
        make_performance_repository, Benchmark, PerformanceRepositorySettings,
        PerformanceRepositoryType, MINIMAL_CLUSTER_SIZE,
    };
    use crate::phases::{ClusterMetrics, FlowMetrics};
    use proctor::elements::telemetry;

    type TestPlanning = FlinkPlanning<LeastSquaresWorkloadForecastBuilder>;
    #[allow(dead_code)]
    type Calculator = ForecastCalculator<LeastSquaresWorkloadForecastBuilder>;

    const STEP: i64 = 15;
    const NOW: i64 = 1624061766 + (30 * STEP);

    lazy_static! {
        static ref METRICS: MetricCatalog = MetricCatalog {
            timestamp: Utc.timestamp(NOW, 0).into(),
            flow: FlowMetrics {
                input_consumer_lag: 314.15926535897932384264,
                ..FlowMetrics::default()
            },
            cluster: ClusterMetrics {
                nr_task_managers: 4,
                ..ClusterMetrics::default()
            },
            custom: telemetry::Table::default(),
        };
        static ref SCALE_UP: DecisionResult<MetricCatalog> =
            DecisionResult::ScaleUp(METRICS.clone());
        static ref SCALE_DOWN: DecisionResult<MetricCatalog> =
            DecisionResult::ScaleDown(METRICS.clone());
        static ref NO_SCALE: DecisionResult<MetricCatalog> =
            DecisionResult::NoAction(METRICS.clone());
    }

    enum SignalType {
        Sine,
        Linear,
    }

    async fn setup_planning(
        planning_name: &str,
        outlet: Outlet<FlinkScalePlan>,
        signal_type: SignalType,
    ) -> anyhow::Result<Arc<Mutex<TestPlanning>>> {
        let mut calc = ForecastCalculator::new(
            LeastSquaresWorkloadForecastBuilder::new(
                20,
                SpikeSettings {
                    influence: 0.25,
                    ..SpikeSettings::default()
                },
            ),
            Duration::from_secs(2 * 60),  // restart
            Duration::from_secs(13 * 60), // max_catch_up
            Duration::from_secs(5 * 60),  // valid_offset
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
            }

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
            }
        };

        points.into_iter().for_each(|(ts, value)| {
            calc.add_observation(WorkloadMeasurement {
                timestamp_secs: ts.timestamp(),
                workload: value.into(),
            })
        });

        let planning = FlinkPlanning {
            name: planning_name.to_string(),
            min_scaling_step: 2,
            forecast_calculator: calc,
            performance_history: PerformanceHistory::default(),
            performance_repository: make_performance_repository(&PerformanceRepositorySettings {
                storage: PerformanceRepositoryType::Memory,
                storage_path: None,
            })?,
            outlet: Some(outlet),
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
        probe_rx: &mut Receiver<FlinkScalePlan>,
        // min_step: u16,
        expected: FlinkScalePlan,
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
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_planning_handle_empty_scale_decision");
        let _main_span_guard = main_span.enter();

        let (probe_tx, mut probe_rx) = mpsc::channel(8);
        let outlet = Outlet::new("plan outlet");
        let mut outlet_2 = outlet.clone();
        block_on(async move { outlet_2.attach("plan_outlet", probe_tx).await });

        let timestamp = METRICS.timestamp;
        let block: anyhow::Result<()> = block_on(async move {
            let planning = setup_planning("planning_1", outlet, SignalType::Linear).await?;
            let min_step = planning.lock().await.min_scaling_step;

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-up with empty history",
                    &SCALE_UP,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    FlinkScalePlan {
                        timestamp,
                        target_nr_task_managers: min_step + METRICS.cluster.nr_task_managers,
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
                    FlinkScalePlan {
                        timestamp,
                        target_nr_task_managers: METRICS.cluster.nr_task_managers - min_step,
                        current_nr_task_managers: METRICS.cluster.nr_task_managers,
                    },
                )
                .await
            );

            planning.lock().await.min_scaling_step = METRICS.cluster.nr_task_managers + 1_000;

            assert_ok!(
                assert_scale_decision_scenario(
                    "scale-down with empty history and way bigger step down than nr task managers",
                    &SCALE_DOWN,
                    Arc::clone(&planning),
                    &mut probe_rx,
                    FlinkScalePlan {
                        timestamp,
                        target_nr_task_managers: MINIMAL_CLUSTER_SIZE,
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
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_planning_handle_scale_decision");
        let _main_span_guard = main_span.enter();

        let (probe_tx, mut probe_rx) = mpsc::channel(8);
        let outlet = Outlet::new("plan outlet");
        let mut outlet_2 = outlet.clone();
        block_on(async move { outlet_2.attach("plan_outlet", probe_tx).await });

        let timestamp = METRICS.timestamp;

        let block: anyhow::Result<()> = block_on(async move {
            let planning = setup_planning("planning_2", outlet, SignalType::Sine).await?;

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
                    FlinkScalePlan {
                        timestamp,
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
