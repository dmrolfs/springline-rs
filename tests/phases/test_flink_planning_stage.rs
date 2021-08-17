use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use claim::*;
use fake::locales::EN;
use fake::Fake;
use lazy_static::lazy_static;
use pretty_assertions::assert_eq;
use proctor::elements::telemetry;
use proctor::elements::TimestampSeconds;
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::phases::plan::Plan;
use proctor::ProctorResult;
use springline::phases::decision::result::DecisionResult;
use springline::phases::plan::{
    make_performance_repository, FlinkPlanning, FlinkScalePlan,
    LeastSquaresWorkloadForecastBuilder, PerformanceRepositorySettings, PerformanceRepositoryType,
    SpikeSettings,
};
use springline::phases::{ClusterMetrics, FlowMetrics, MetricCatalog};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type InData = MetricCatalog;
type InDecision = DecisionResult<MetricCatalog>;
type Out = FlinkScalePlan;
#[allow(dead_code)]
type ForecastBuilder = LeastSquaresWorkloadForecastBuilder;
type TestPlanning = FlinkPlanning<LeastSquaresWorkloadForecastBuilder>;
type TestStage = Plan<TestPlanning>;

lazy_static! {
    static ref DT_1: DateTime<Utc> =
        DateTime::parse_from_str("2021-05-05T17:11:07.246310806Z", "%+")
            .unwrap()
            .with_timezone(&Utc);
    static ref DT_1_STR: String = format!("{}", DT_1.format("%+"));
    static ref DT_1_TS: i64 = DT_1.timestamp();
}

#[allow(dead_code)]
struct TestFlow {
    pub graph_handle: JoinHandle<ProctorResult<()>>,
    pub tx_data_source_api: stage::ActorSourceApi<InData>,
    pub tx_decision_source_api: stage::ActorSourceApi<InDecision>,
    pub tx_sink_api: stage::FoldApi<Vec<Out>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<Out>>>,
}

impl TestFlow {
    pub async fn new(planning_stage: TestStage) -> anyhow::Result<Self> {
        let data_source: stage::ActorSource<InData> = stage::ActorSource::new("data_source");
        let tx_data_source_api = data_source.tx_api();

        let decision_source: stage::ActorSource<InDecision> =
            stage::ActorSource::new("decision_source");
        let tx_decision_source_api = decision_source.tx_api();

        let mut sink = stage::Fold::<_, Out, _>::new("sink", Vec::new(), |mut acc, item| {
            acc.push(item);
            acc
        });
        let tx_sink_api = sink.tx_api();
        let rx_sink = sink.take_final_rx();

        (data_source.outlet(), planning_stage.inlet())
            .connect()
            .await;
        (decision_source.outlet(), planning_stage.decision_inlet())
            .connect()
            .await;
        (planning_stage.outlet(), sink.inlet()).connect().await;

        let mut graph = Graph::default();
        graph.push_back(Box::new(data_source)).await;
        graph.push_back(Box::new(decision_source)).await;
        graph.push_back(Box::new(planning_stage)).await;
        graph.push_back(Box::new(sink)).await;

        let graph_handle = tokio::spawn(async move { graph.run().await });

        Ok(Self {
            graph_handle,
            tx_data_source_api,
            tx_decision_source_api,
            tx_sink_api,
            rx_sink,
        })
    }

    pub async fn push_data(&self, metrics: MetricCatalog) -> anyhow::Result<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(metrics);
        self.tx_data_source_api.send(cmd)?;
        let _ack = ack.await?;
        Ok(())
    }

    pub async fn push_decision(&self, decision: InDecision) -> anyhow::Result<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(decision);
        self.tx_decision_source_api.send(cmd)?;
        let _ack = ack.await?;
        Ok(())
    }

    pub async fn inspect_sink(&self) -> anyhow::Result<Vec<Out>> {
        let (cmd, acc) = stage::FoldCmd::get_accumulation();
        self.tx_sink_api.send(cmd)?;
        let result = acc.await.map(|a| {
            tracing::info!(accumulation=?a, "inspected sink accumulation");
            a
        })?;

        Ok(result)
    }

    #[tracing::instrument(level = "info", skip(self, check_size))]
    pub async fn check_sink_accumulation(
        &self,
        _label: &str,
        timeout: Duration,
        mut check_size: impl FnMut(Vec<Out>) -> bool,
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
        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_data_source_api.send(stop)?;

        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_decision_source_api.send(stop)?;

        self.graph_handle.await??;

        let result = self.rx_sink.take().unwrap().await?;
        Ok(result)
    }
}

const STEP: i64 = 15;

#[tracing::instrument(level = "info")]
fn make_test_data(
    start: TimestampSeconds,
    tick: i64,
    nr_task_managers: u16,
    input_consumer_lag: f64,
    records_per_sec: f64,
) -> InData {
    let timestamp = Utc.timestamp(start.as_i64() + tick * STEP, 0).into();

    MetricCatalog {
        timestamp,
        flow: FlowMetrics {
            records_in_per_sec: records_per_sec,
            records_out_per_sec: records_per_sec,
            input_consumer_lag,
            max_message_latency: 0.,
            net_in_utilization: 0.,
            net_out_utilization: 0.,
            sink_health_metrics: 0.,
        },
        cluster: ClusterMetrics {
            nr_task_managers,
            task_cpu_load: 0.,
            network_io_utilization: 0.,
        },
        custom: telemetry::Table::default(),
    }
}

fn make_test_data_series(
    start: TimestampSeconds,
    nr_task_managers: u16,
    input_consumer_lag: f64,
    mut gen: impl FnMut(i64) -> f64,
) -> Vec<InData> {
    let total = 30;
    (0..total)
        .into_iter()
        .map(move |tick| {
            make_test_data(start, tick, nr_task_managers, input_consumer_lag, gen(tick))
        })
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
    decision: DecisionType,
    start: TimestampSeconds,
    tick: i64,
    nr_task_managers: u16,
    input_consumer_lag: f64,
    records_per_sec: f64,
) -> InDecision {
    let data = make_test_data(
        start,
        tick,
        nr_task_managers,
        input_consumer_lag,
        records_per_sec,
    );
    match decision {
        DecisionType::Up => DecisionResult::ScaleUp(data),
        DecisionType::Down => DecisionResult::ScaleDown(data),
        DecisionType::NoAction => DecisionResult::NoAction(data),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_planning_linear() {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_planning_linear");
    let _ = main_span.enter();

    let restart_duration = Duration::from_secs(2 * 60);
    let max_catch_up_duration = Duration::from_secs(13 * 60);
    let recovery_valid_offset = Duration::from_secs(5 * 60);

    let forecast_builder = LeastSquaresWorkloadForecastBuilder::new(20, SpikeSettings::default());
    let performance_repository =
        assert_ok!(make_performance_repository(PerformanceRepositorySettings {
            storage: PerformanceRepositoryType::Memory,
            storage_path: None,
        }));

    let mut planning = assert_ok!(
        TestPlanning::new(
            "test_planning_1",
            2,
            restart_duration,
            max_catch_up_duration,
            recovery_valid_offset,
            forecast_builder,
            performance_repository,
        )
        .await
    );

    let start: DateTime<Utc> = fake::faker::chrono::raw::DateTimeBefore(EN, Utc::now()).fake();
    let data = make_test_data_series(start.into(), 2, 1000., |tick| tick as f64);
    let data_len = data.len();
    let last_data = assert_some!(data.last()).clone();

    assert_ok!(
        planning
            .update_performance_history(&make_decision(
                DecisionType::Up,
                last_data.timestamp,
                0,
                2,
                0.,
                25.
            ))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    assert_ok!(
        planning
            .update_performance_history(&make_decision(
                DecisionType::Up,
                last_data.timestamp,
                0,
                4,
                0.,
                75.
            ))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    assert_ok!(
        planning
            .update_performance_history(&make_decision(
                DecisionType::Up,
                last_data.timestamp,
                0,
                10,
                0.,
                250.
            ))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);

    let stage = TestStage::new("planning_1", planning);
    let mut plan_monitor = stage.rx_monitor();

    let flow = assert_ok!(TestFlow::new(stage).await);

    tracing::info!("pushing test data into graph...");
    for d in data {
        assert_ok!(flow.push_data(d).await);
    }

    tracing::info!("DMR: watching for all observations to be pushed...");
    {
        let span = tracing::info_span!("watching for all observations to be pushed");
        let _ = span.enter();

        for i in 0..data_len {
            let evt = assert_ok!(plan_monitor.recv().await);
            tracing::info!(?evt, "Observation[{}] made", i);
        }
    }

    tracing::info!("pushing decision...");
    let decision = InDecision::ScaleUp(last_data);
    let timestamp = decision.item().timestamp;
    assert_ok!(flow.push_decision(decision).await);

    tracing::info!("DMR-waiting for plan to reach sink...");
    assert!(assert_ok!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 1)
            .await
    ));

    tracing::info!("DMR: Verify final accumulation...");
    let actual = assert_ok!(flow.close().await);
    assert_eq!(
        actual,
        vec![FlinkScalePlan {
            timestamp,
            target_nr_task_managers: 6,
            current_nr_task_managers: 2,
        }]
    )

    // todo: assert performance history updated for 2 => 29. once extensible api design is worked
    // out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_planning_sine() {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_planning_sine");
    let _ = main_span.enter();

    let restart_duration = Duration::from_secs(2 * 60);
    let max_catch_up_duration = Duration::from_secs(13 * 60);
    let recovery_valid_offset = Duration::from_secs(5 * 60);

    let forecast_builder = LeastSquaresWorkloadForecastBuilder::new(20, SpikeSettings::default());
    let performance_repository =
        assert_ok!(make_performance_repository(PerformanceRepositorySettings {
            storage: PerformanceRepositoryType::Memory,
            storage_path: None,
        }));

    let mut planning = assert_ok!(
        TestPlanning::new(
            "test_planning_2",
            2,
            restart_duration,
            max_catch_up_duration,
            recovery_valid_offset,
            forecast_builder,
            performance_repository,
        )
        .await
    );

    let start: DateTime<Utc> = fake::faker::chrono::raw::DateTimeBefore(EN, Utc::now()).fake();
    let data = make_test_data_series(start.into(), 2, 1000., |tick| {
        75. * ((tick as f64) / 15.).sin()
    });
    let data_len = data.len();
    let last_data = assert_some!(data.last()).clone();

    assert_ok!(
        planning
            .update_performance_history(&make_decision(
                DecisionType::Up,
                last_data.timestamp,
                0,
                2,
                0.,
                25.
            ))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    assert_ok!(
        planning
            .update_performance_history(&make_decision(
                DecisionType::Up,
                last_data.timestamp,
                0,
                4,
                0.,
                75.
            ))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);
    assert_ok!(
        planning
            .update_performance_history(&make_decision(
                DecisionType::Up,
                last_data.timestamp,
                0,
                10,
                0.,
                250.
            ))
            .await
    );
    tracing::warn!("DMR: planning history = {:?}", planning);

    let stage = TestStage::new("planning_1", planning);
    let mut plan_monitor = stage.rx_monitor();

    let flow = assert_ok!(TestFlow::new(stage).await);

    tracing::info!("pushing test data into graph...");
    for d in data {
        assert_ok!(flow.push_data(d).await);
    }

    tracing::info!("DMR: watching for all observations to be pushed...");
    {
        let span = tracing::info_span!("watching for all observations to be pushed");
        let _ = span.enter();

        for i in 0..data_len {
            let evt = assert_ok!(plan_monitor.recv().await);
            tracing::info!(?evt, "Observation[{}] made", i);
        }
    }

    tracing::info!("pushing decision...");
    let decision = InDecision::ScaleUp(last_data);
    let timestamp = decision.item().timestamp;
    assert_ok!(flow.push_decision(decision).await);

    tracing::info!("DMR-waiting for plan to reach sink...");
    assert!(assert_ok!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 1)
            .await
    ));

    tracing::info!("DMR: Verify final accumulation...");
    let actual = assert_ok!(flow.close().await);
    assert_eq!(
        actual,
        vec![FlinkScalePlan {
            timestamp,
            target_nr_task_managers: 8,
            current_nr_task_managers: 2,
        }]
    )

    // todo: assert performance history updated for 2 => 29. once extensible api design is worked
    // out
}
