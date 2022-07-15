use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use claim::*;
use fake::locales::EN;
use fake::Fake;
use pretty_assertions::assert_eq;
use proctor::elements::telemetry;
use proctor::elements::Timestamp;
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::phases::plan::{Plan, PlanEvent};
use proctor::ProctorResult;
use springline::flink::{ClusterMetrics, FlowMetrics, JobHealthMetrics, MetricCatalog};
use springline::phases::decision::DecisionResult;
use springline::phases::plan::{
    make_performance_repository, FlinkPlanningMonitor, ForecastInputs, PlanningContext, PlanningMeasurement,
};
use springline::phases::plan::{
    FlinkPlanning, LeastSquaresWorkloadForecaster, PerformanceRepositorySettings, PerformanceRepositoryType, ScalePlan,
    SpikeSettings,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::CORRELATION_ID;

type InData = PlanningMeasurement;
type InDecision = DecisionResult<MetricCatalog>;
type Out = ScalePlan;
#[allow(dead_code)]
type ForecastBuilder = LeastSquaresWorkloadForecaster;
type TestPlanning = FlinkPlanning<LeastSquaresWorkloadForecaster>;
type TestStage = Plan<TestPlanning>;

#[allow(dead_code)]
struct TestFlow {
    pub graph_handle: JoinHandle<ProctorResult<()>>,
    pub tx_data_sensor_api: stage::ActorSourceApi<InData>,
    pub tx_decision_sensor_api: stage::ActorSourceApi<InDecision>,
    pub tx_context_sensor_api: stage::ActorSourceApi<PlanningContext>,
    pub rx_planning_monitor: FlinkPlanningMonitor,
    pub tx_sink_api: stage::FoldApi<Vec<Out>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<Out>>>,
}

impl TestFlow {
    pub async fn new(planning_stage: TestStage, rx_planning_monitor: FlinkPlanningMonitor) -> anyhow::Result<Self> {
        let data_sensor: stage::ActorSource<InData> = stage::ActorSource::new("data_sensor");
        let tx_data_sensor_api = data_sensor.tx_api();

        let decision_sensor: stage::ActorSource<InDecision> = stage::ActorSource::new("decision_sensor");
        let tx_decision_sensor_api = decision_sensor.tx_api();

        let context_sensor: stage::ActorSource<PlanningContext> = stage::ActorSource::new("context_sensor");
        let tx_context_sensor_api = context_sensor.tx_api();

        let mut sink = stage::Fold::<_, Out, _>::new("sink", Vec::new(), |mut acc, item| {
            acc.push(item);
            acc
        });
        let tx_sink_api = sink.tx_api();
        let rx_sink = sink.take_final_rx();

        (data_sensor.outlet(), planning_stage.inlet()).connect().await;
        (decision_sensor.outlet(), planning_stage.decision_inlet()).connect().await;
        (context_sensor.outlet(), planning_stage.context_inlet()).connect().await;
        (planning_stage.outlet(), sink.inlet()).connect().await;

        let mut graph = Graph::default();
        graph.push_back(Box::new(data_sensor)).await;
        graph.push_back(Box::new(decision_sensor)).await;
        graph.push_back(Box::new(context_sensor)).await;
        graph.push_back(Box::new(planning_stage)).await;
        graph.push_back(Box::new(sink)).await;

        let graph_handle = tokio::spawn(async move { graph.run().await });

        Ok(Self {
            graph_handle,
            tx_data_sensor_api,
            tx_decision_sensor_api,
            tx_context_sensor_api,
            rx_planning_monitor,
            tx_sink_api,
            rx_sink,
        })
    }

    pub async fn push_data(&self, metrics: MetricCatalog) -> anyhow::Result<()> {
        let measurement: PlanningMeasurement = metrics.into();
        stage::ActorSourceCmd::push(&self.tx_data_sensor_api, measurement)
            .await
            .map_err(|err| err.into())
    }

    pub async fn push_decision(&self, decision: InDecision) -> anyhow::Result<()> {
        stage::ActorSourceCmd::push(&self.tx_decision_sensor_api, decision)
            .await
            .map_err(|err| err.into())
    }

    pub async fn push_context(&self, context: PlanningContext) -> anyhow::Result<()> {
        stage::ActorSourceCmd::push(&self.tx_context_sensor_api, context)
            .await
            .map_err(|err| err.into())
    }

    pub async fn inspect_sink(&self) -> anyhow::Result<Vec<Out>> {
        stage::FoldCmd::get_accumulation(&self.tx_sink_api)
            .await
            .map(|a| {
                tracing::info!(accumulation=?a, "inspected sink accumulation");
                a
            })
            .map_err(|err| err.into())
    }

    #[tracing::instrument(level = "info", skip(self, check_size))]
    pub async fn check_sink_accumulation(
        &self, _label: &str, timeout: Duration, mut check_size: impl FnMut(Vec<Out>) -> bool,
    ) -> anyhow::Result<bool> {
        use std::time::Instant;
        let deadline = Instant::now() + timeout;
        let step = Duration::from_millis(50);
        let mut result = false;

        loop {
            if Instant::now() < deadline {
                let acc = self.inspect_sink().await;
                if acc.is_ok() {
                    let acc = acc?;
                    tracing::info!(?acc, len=?acc.len(), "inspecting sink");
                    result = check_size(acc);
                    if !result {
                        tracing::warn!(
                            ?result,
                            "sink length failed check predicate - retrying after {:?}.",
                            step
                        );
                        tokio::time::sleep(step).await;
                    } else {
                        tracing::info!(?result, "sink length passed check predicate.");
                        break;
                    }
                } else {
                    tracing::error!(?acc, "failed to inspect sink");
                    break;
                }
            } else {
                tracing::error!(?timeout, "check timeout exceeded - stopping check.");
                break;
            }
        }

        Ok(result)
    }

    #[tracing::instrument(level = "warn", skip(self))]
    pub async fn close(mut self) -> anyhow::Result<Vec<Out>> {
        tracing::info!("DMR: closing data sensor...");
        stage::ActorSourceCmd::stop(&self.tx_data_sensor_api).await?;
        tracing::info!("DMR: closing decision sensor...");
        stage::ActorSourceCmd::stop(&self.tx_decision_sensor_api).await?;
        tracing::info!("DMR: closing context sensor...");
        stage::ActorSourceCmd::stop(&self.tx_context_sensor_api).await?;
        tracing::info!("DMR: closing graph...");
        self.graph_handle.await??;

        tracing::info!("DMR: final take from sink");
        let result = self.rx_sink.take().unwrap().await?;
        tracing::info!(?result, "DMR: took from sink... close done");
        Ok(result)
    }
}

const STEP: i64 = 15;

#[tracing::instrument(level = "info")]
fn make_test_data(
    start: Timestamp, tick: i64, nr_task_managers: u32, source_records_lag_max: i64, records_per_sec: f64,
) -> MetricCatalog {
    let timestamp = Utc.timestamp(start.as_secs() + tick * STEP, 0).into();
    let corr_id = CORRELATION_ID.clone();
    let forecasted_timestamp = timestamp + Duration::from_secs(STEP as u64);
    MetricCatalog {
        correlation_id: corr_id,
        recv_timestamp: timestamp,
        health: JobHealthMetrics {
            job_uptime_millis: 1_234_567,
            job_nr_restarts: 3,
            job_nr_completed_checkpoints: 12_345,
            job_nr_failed_checkpoints: 7,
        },
        flow: FlowMetrics {
            records_in_per_sec: records_per_sec,
            records_out_per_sec: records_per_sec,
            idle_time_millis_per_sec: 222.2,
            source_back_pressured_time_millis_per_sec: 127.0,
            forecasted_timestamp: Some(forecasted_timestamp),
            forecasted_records_in_per_sec: Some(records_per_sec),
            source_records_lag_max: Some(source_records_lag_max),
            source_assigned_partitions: Some(nr_task_managers as i64),
            source_total_lag: Some(source_records_lag_max * nr_task_managers as i64),
            source_records_consumed_rate: Some((source_records_lag_max * nr_task_managers as i64 * 2) as f64),
            source_millis_behind_latest: None,
        },
        cluster: ClusterMetrics {
            nr_active_jobs: 1,
            nr_task_managers,
            task_cpu_load: 0.65,
            task_heap_memory_used: 92_987_f64,
            task_heap_memory_committed: 103_929_920_f64,
            task_nr_threads: 8,
            task_network_input_queue_len: 12.,
            task_network_input_pool_usage: 8.,
            task_network_output_queue_len: 12.,
            task_network_output_pool_usage: 5.,
        },
        custom: telemetry::TableType::default(),
    }
}

fn make_test_data_series(
    start: Timestamp, nr_task_managers: u32, source_records_lag_max: i64, mut gen: impl FnMut(i64) -> f64,
) -> Vec<MetricCatalog> {
    let total = 30;
    (0..total)
        .into_iter()
        .map(move |tick| make_test_data(start, tick, nr_task_managers, source_records_lag_max, gen(tick)))
        .collect()
}

#[allow(dead_code)]
#[derive(Debug)]
enum DecisionType {
    Up,
    Down,
    NoAction,
}

#[tracing::instrument(level = "info")]
fn make_decision(
    decision: DecisionType, start: Timestamp, tick: i64, nr_task_managers: u32, source_records_lag_max: i64,
    records_per_sec: f64,
) -> InDecision {
    let data = make_test_data(start, tick, nr_task_managers, source_records_lag_max, records_per_sec);
    match decision {
        DecisionType::Up => DecisionResult::ScaleUp(data),
        DecisionType::Down => DecisionResult::ScaleDown(data),
        DecisionType::NoAction => DecisionResult::NoAction(data),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_planning_linear() {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_planning_linear");
    let _ = main_span.enter();

    let restart_duration = Duration::from_secs(2 * 60);
    let max_catch_up_duration = Duration::from_secs(13 * 60);
    let recovery_valid_offset = Duration::from_secs(5 * 60);
    let inputs = assert_ok!(ForecastInputs::new(
        restart_duration,
        max_catch_up_duration,
        recovery_valid_offset
    ));

    let forecast_builder = LeastSquaresWorkloadForecaster::new(20, SpikeSettings::default());
    let performance_repository = assert_ok!(make_performance_repository(&PerformanceRepositorySettings {
        storage: PerformanceRepositoryType::Memory,
        storage_path: None,
    }));

    let mut planning =
        assert_ok!(TestPlanning::new("test_planning_1", 2, inputs, forecast_builder, performance_repository,).await);

    let start: DateTime<Utc> = fake::faker::chrono::raw::DateTimeBefore(EN, Utc::now()).fake();
    let data = make_test_data_series(start.into(), 2, 1000, |tick| tick as f64);
    let data_len = data.len();
    let last_data = assert_some!(data.last()).clone();

    assert_ok!(
        planning
            .update_performance_history(&make_decision(DecisionType::Up, last_data.recv_timestamp, 0, 2, 0, 25.))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    assert_ok!(
        planning
            .update_performance_history(&make_decision(DecisionType::Up, last_data.recv_timestamp, 0, 4, 0, 75.))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    assert_ok!(
        planning
            .update_performance_history(&make_decision(
                DecisionType::Up,
                last_data.recv_timestamp,
                0,
                10,
                0,
                250.
            ))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);

    let rx_flink_planning_monitor = planning.rx_monitor();
    let stage = TestStage::new("planning_1", planning);
    let mut _plan_monitor = stage.rx_monitor();

    let mut flow = assert_ok!(TestFlow::new(stage, rx_flink_planning_monitor).await);

    tracing::info!("pushing test data into graph...");
    for d in data {
        assert_ok!(flow.push_data(d).await);
    }

    tracing::info!("DMR: watching for all observations to be pushed...");
    {
        let span = tracing::info_span!("watching for all observations to be pushed");
        let _ = span.enter();

        for i in 0..data_len {
            let evt = assert_ok!(flow.rx_planning_monitor.recv().await);
            tracing::info!(?evt, "Observation[{}] made", i);
        }
    }

    tracing::info!("pushing decision...");
    let decision = InDecision::ScaleUp(last_data);
    let timestamp = decision.item().recv_timestamp;
    let correlation_id = decision.item().correlation_id.clone();
    assert_ok!(flow.push_decision(decision).await);

    tracing::info!("DMR-waiting for plan to reach sink...");
    assert!(assert_ok!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 1)
            .await
    ));

    tracing::info!("DMR: Verify final accumulation...");
    let actual = assert_ok!(flow.close().await);
    tracing::info!(?actual, "checking final results...");
    let result = std::panic::catch_unwind(|| {
        assert_eq!(
            actual,
            vec![ScalePlan {
                recv_timestamp: timestamp,
                correlation_id: correlation_id.clone(),
                target_nr_task_managers: 6, // todo: also allow 5???
                current_nr_task_managers: 2,
            }]
        )
    });

    if result.is_err() {
        assert_eq!(
            actual,
            vec![ScalePlan {
                recv_timestamp: timestamp,
                correlation_id,
                target_nr_task_managers: 5,
                current_nr_task_managers: 2,
            }]
        )
    }

    // todo: assert performance history updated for 2 => 29. once extensible api design is worked
    // out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_planning_sine() {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_planning_sine");
    let _ = main_span.enter();

    let restart_duration = Duration::from_secs(2 * 60);
    let max_catch_up_duration = Duration::from_secs(13 * 60);
    let recovery_valid_offset = Duration::from_secs(5 * 60);
    let inputs = assert_ok!(ForecastInputs::new(
        restart_duration,
        max_catch_up_duration,
        recovery_valid_offset
    ));

    let forecaster = LeastSquaresWorkloadForecaster::new(20, SpikeSettings::default());
    let performance_repository = assert_ok!(make_performance_repository(&PerformanceRepositorySettings {
        storage: PerformanceRepositoryType::Memory,
        storage_path: None,
    }));

    let mut planning =
        assert_ok!(TestPlanning::new("test_planning_2", 2, inputs, forecaster, performance_repository,).await);

    let start: DateTime<Utc> = fake::faker::chrono::raw::DateTimeBefore(EN, Utc::now()).fake();
    let data = make_test_data_series(start.into(), 2, 1000, |tick| 75. * ((tick as f64) / 15.).sin());
    let data_len = data.len();
    let last_data = assert_some!(data.last()).clone();

    assert_ok!(
        planning
            .update_performance_history(&make_decision(DecisionType::Up, last_data.recv_timestamp, 1, 2, 0, 25.))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    assert_ok!(
        planning
            .update_performance_history(&make_decision(DecisionType::Up, last_data.recv_timestamp, 2, 4, 0, 75.))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    assert_ok!(
        planning
            .update_performance_history(&make_decision(
                DecisionType::Up,
                last_data.recv_timestamp,
                3,
                10,
                0,
                250.
            ))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    let rx_flink_planning_monitor = planning.rx_monitor();

    let stage = TestStage::new("planning_1", planning);

    let mut flow = assert_ok!(TestFlow::new(stage, rx_flink_planning_monitor).await);

    tracing::info!("pushing test data into graph...");
    for d in data {
        assert_ok!(flow.push_data(d).await);
    }

    tracing::info!("DMR: watching for all observations to be pushed...");
    {
        let span = tracing::info_span!("watching for all observations to be pushed");
        let _ = span.enter();

        for i in 0..data_len {
            let evt = assert_ok!(flow.rx_planning_monitor.recv().await);
            tracing::info!(?evt, "Observation[{}] made", i);
        }
    }

    tracing::info!("pushing decision...");
    let decision = InDecision::ScaleUp(last_data);
    let timestamp = decision.item().recv_timestamp;
    assert_ok!(flow.push_decision(decision).await);

    tracing::info!("DMR-waiting for plan to reach sink...");
    assert!(assert_ok!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 1)
            .await
    ));

    tracing::info!("DMR: Verify final accumulation...");
    let actual = assert_ok!(flow.close().await);

    if let Err(err) = std::panic::catch_unwind(|| {
        assert_eq!(
            actual,
            vec![ScalePlan {
                recv_timestamp: timestamp,
                correlation_id: CORRELATION_ID.clone(),
                target_nr_task_managers: 8,
                current_nr_task_managers: 2,
            }]
        )
    }) {
        tracing::error!(error=?err, "common lower boundary failed - trying higher..");
        assert_eq!(
            actual,
            vec![ScalePlan {
                recv_timestamp: timestamp,
                correlation_id: CORRELATION_ID.clone(),
                target_nr_task_managers: 9,
                current_nr_task_managers: 2,
            }]
        )
    }

    // todo: assert performance history updated for 2 => 29. once extensible api design is worked
    // out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_planning_context_change() {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_planning_sine");
    let _ = main_span.enter();

    let inputs = assert_ok!(ForecastInputs::new(
        Duration::from_secs(2 * 60),
        Duration::from_secs(13 * 60),
        Duration::from_secs(5 * 60),
    ));

    let forecaster = LeastSquaresWorkloadForecaster::new(20, SpikeSettings::default());
    let performance_repository = assert_ok!(make_performance_repository(&PerformanceRepositorySettings {
        storage: PerformanceRepositoryType::Memory,
        storage_path: None,
    }));

    let mut planning =
        assert_ok!(TestPlanning::new("test_planning_3", 2, inputs, forecaster, performance_repository,).await);

    let start: DateTime<Utc> = fake::faker::chrono::raw::DateTimeBefore(EN, Utc::now()).fake();
    let data = make_test_data_series(start.into(), 2, 1000, |tick| 75. * ((tick as f64) / 15.).sin());
    let data_len = data.len();
    let penultimate_data = data[data.len() - 2].clone();
    let last_data = data[data.len() - 1].clone();

    assert_ok!(
        planning
            .update_performance_history(&make_decision(
                DecisionType::Up,
                penultimate_data.recv_timestamp,
                1,
                2,
                0,
                25.
            ))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    assert_ok!(
        planning
            .update_performance_history(&make_decision(DecisionType::Up, last_data.recv_timestamp, 2, 4, 0, 75.))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    assert_ok!(
        planning
            .update_performance_history(&make_decision(
                DecisionType::Up,
                last_data.recv_timestamp,
                3,
                10,
                0,
                250.
            ))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    let rx_flink_planning_monitor = planning.rx_monitor();
    let stage = TestStage::new("planning_1", planning);
    let mut rx_plan_monitor = stage.rx_monitor();
    let mut flow = assert_ok!(TestFlow::new(stage, rx_flink_planning_monitor).await);

    tracing::info!("pushing test data into graph...");
    for d in data {
        assert_ok!(flow.push_data(d).await);
    }

    tracing::info!("DMR: watching for all observations to be pushed...");
    {
        let span = tracing::info_span!("watching for all observations to be pushed");
        let _ = span.enter();

        for i in 0..data_len {
            let evt = assert_ok!(flow.rx_planning_monitor.recv().await);
            tracing::info!(?evt, "Observation[{}] made", i);
        }
    }

    tracing::info!("pushing penultimate decision...");
    let decision = InDecision::ScaleUp(penultimate_data);
    let penultimate_timestamp = decision.item().recv_timestamp;
    assert_ok!(flow.push_decision(decision).await);

    assert_matches!(
        assert_ok!(rx_plan_monitor.recv().await).as_ref(),
        PlanEvent::<TestPlanning>::DecisionPlanned(_, _)
    );

    tracing::info!("DMR: pushing new context");
    assert_ok!(
        flow.push_context(PlanningContext {
            correlation_id: CORRELATION_ID.relabel::<PlanningContext>(),
            recv_timestamp: Timestamp::now(),
            min_scaling_step: Some(100),
            rescale_restart: Some(Duration::from_millis(1)),
            max_catch_up: Some(Duration::from_millis(2)),
            recovery_valid: Some(Duration::from_millis(3)),
        })
        .await
    );

    let context_event = assert_ok!(rx_plan_monitor.recv().await);
    match context_event.as_ref() {
        PlanEvent::ContextChanged(ctx) => {
            assert_eq!(assert_some!(ctx.min_scaling_step), 100);
            assert_eq!(assert_some!(ctx.rescale_restart), Duration::from_millis(1));
            assert_eq!(assert_some!(ctx.max_catch_up), Duration::from_millis(2));
            assert_eq!(assert_some!(ctx.recovery_valid), Duration::from_millis(3));
        },
        _ => panic!("unexpected event: {:?}", context_event),
    };

    tracing::info!("pushing last decision...");
    let decision = InDecision::ScaleUp(last_data);
    let _last_timestamp = decision.item().recv_timestamp;
    assert_ok!(flow.push_decision(decision).await);

    tracing::info!("DMR-waiting for plan to reach sink...");
    assert!(assert_ok!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 2)
            .await
    ));

    tracing::info!("DMR: Verify final accumulation...");
    let actual = assert_ok!(flow.close().await);

    if let Err(err) = std::panic::catch_unwind(|| {
        assert_eq!(
            actual[0],
            ScalePlan {
                recv_timestamp: penultimate_timestamp,
                correlation_id: CORRELATION_ID.clone(),
                target_nr_task_managers: 8,
                current_nr_task_managers: 2,
            },
        )
    }) {
        tracing::error!(error=?err, "common boundary failed - trying higher..");
        if let Err(err) = std::panic::catch_unwind(|| {
            assert_eq!(
                actual[0],
                ScalePlan {
                    recv_timestamp: penultimate_timestamp,
                    correlation_id: CORRELATION_ID.clone(),
                    target_nr_task_managers: 9,
                    current_nr_task_managers: 2,
                },
            )
        }) {
            tracing::error!(error=?err, "common and high boundaries failed - trying lower..");
            assert_eq!(
                actual[0],
                ScalePlan {
                    recv_timestamp: penultimate_timestamp,
                    correlation_id: CORRELATION_ID.clone(),
                    target_nr_task_managers: 7,
                    current_nr_task_managers: 2,
                },
            )
        }
    }

    assert_gt!(
        actual[1].target_nr_task_managers,
        1000 * actual[0].target_nr_task_managers
    );
}
