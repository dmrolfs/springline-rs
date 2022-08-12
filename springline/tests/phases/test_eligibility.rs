use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use claim::*;
use lazy_static::lazy_static;
use once_cell::sync::Lazy;
use pretty_assertions::assert_eq;
use proctor::elements::{self, telemetry, PolicyFilterEvent, PolicySource, Timestamp, ToTelemetry};
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::ProctorIdGenerator;
use springline::flink::{
    AppDataWindow, ClusterMetrics, FlowMetrics, JobHealthMetrics, MetricCatalog,
};
use springline::phases::eligibility::{ClusterStatus, JobStatus};
use springline::phases::eligibility::{
    EligibilityContext, EligibilityPolicy, EligibilityTemplateData,
};
use springline::settings::EligibilitySettings;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type Data = AppDataWindow<MetricCatalog>;
type Context = EligibilityContext;

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
    pub graph_handle: JoinHandle<()>,
    pub tx_data_sensor_api: stage::ActorSourceApi<Data>,
    pub tx_context_sensor_api: stage::ActorSourceApi<Context>,
    pub tx_stage_api: elements::PolicyFilterApi<Context, EligibilityTemplateData>,
    pub rx_stage_monitor: elements::PolicyFilterMonitor<Data, Context>,
    pub tx_sink_api: stage::FoldApi<Vec<Data>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<Data>>>,
}

impl TestFlow {
    pub async fn new(
        stage: PolicyPhase<Data, Data, Context, EligibilityTemplateData>,
    ) -> anyhow::Result<Self> {
        let data_sensor: stage::ActorSource<Data> = stage::ActorSource::new("plan_sensor");
        let tx_data_sensor_api = data_sensor.tx_api();

        let context_sensor: stage::ActorSource<Context> = stage::ActorSource::new("context_sensor");
        let tx_context_sensor_api = context_sensor.tx_api();

        let tx_stage_api = stage.tx_api();
        let rx_stage_monitor = stage.rx_monitor();

        let mut sink = stage::Fold::<_, Data, _>::new("sink", Vec::new(), |mut acc, item| {
            acc.push(item);
            acc
        });
        let tx_sink_api = sink.tx_api();
        let rx_sink = sink.take_final_rx();

        (data_sensor.outlet(), stage.inlet()).connect().await;
        (context_sensor.outlet(), stage.context_inlet()).connect().await;
        (stage.outlet(), sink.inlet()).connect().await;
        assert!(stage.inlet().is_attached().await);
        assert!(stage.context_inlet().is_attached().await);
        assert!(stage.outlet().is_attached().await);

        let mut graph = Graph::default();
        graph.push_back(Box::new(data_sensor)).await;
        graph.push_back(Box::new(context_sensor)).await;
        graph.push_back(Box::new(stage)).await;
        graph.push_back(Box::new(sink)).await;

        let graph_handle = tokio::spawn(async move {
            graph
                .run()
                .await
                .map_err(|err| {
                    tracing::error!(error=?err, "graph run failed!");
                    err
                })
                .expect("graph run failed!")
        });

        Ok(Self {
            graph_handle,
            tx_data_sensor_api,
            tx_context_sensor_api,
            tx_stage_api,
            rx_stage_monitor,
            tx_sink_api,
            rx_sink,
        })
    }

    pub async fn push_data(&self, data: Data) -> anyhow::Result<()> {
        stage::ActorSourceCmd::push(&self.tx_data_sensor_api, data)
            .await
            .map_err(|err| err.into())
    }

    pub async fn push_context(&self, context: Context) -> anyhow::Result<()> {
        stage::ActorSourceCmd::push(&self.tx_context_sensor_api, context)
            .await
            .map_err(|err| err.into())
    }

    #[allow(dead_code)]
    pub async fn tell_policy(
        &self,
        command_rx: (
            elements::PolicyFilterCmd<Context, EligibilityTemplateData>,
            oneshot::Receiver<proctor::Ack>,
        ),
    ) -> anyhow::Result<proctor::Ack> {
        self.tx_stage_api.send(command_rx.0)?;
        Ok(command_rx.1.await?)
    }

    pub async fn recv_policy_event(
        &mut self,
    ) -> anyhow::Result<Arc<elements::PolicyFilterEvent<Data, Context>>> {
        Ok(self.rx_stage_monitor.recv().await?)
    }

    #[allow(dead_code)]
    pub async fn inspect_policy_context(
        &self,
    ) -> anyhow::Result<elements::PolicyFilterDetail<Context, EligibilityTemplateData>> {
        elements::PolicyFilterCmd::inspect(&self.tx_stage_api)
            .await
            .map(|d| {
                tracing::info!(detail=?d, "inspected policy.");
                d
            })
            .map_err(|err| err.into())
    }

    pub async fn inspect_sink(&self) -> anyhow::Result<Vec<Data>> {
        stage::FoldCmd::get_accumulation(&self.tx_sink_api)
            .await
            .map(|a| {
                tracing::info!(accumulation=?a, "inspected sink accumulation");
                a
            })
            .map_err(|err| err.into())
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn check_scenario(
        &mut self, label: &str, data: Data, expectation: Vec<Data>,
    ) -> anyhow::Result<()> {
        let scenario_span =
            tracing::info_span!("DMR check scenario", %label, ?data, ?expectation, );
        let _ = scenario_span.enter();

        let timeout = Duration::from_millis(250);

        assert_ok!(self.push_data(data).await);
        claim::assert_matches!(
            &*assert_ok!(self.rx_stage_monitor.recv().await),
            &PolicyFilterEvent::ItemPassed(_, _)
        );

        let result = assert_ok!(
            self.check_sink_accumulation(label, timeout, |acc| {
                let check_span =
                    tracing::info_span!("DMR check sensesensor accumulation", %label, ?expectation, ?timeout);
                let _ = check_span.enter();

                tracing::warn!(
                    ?acc,
                    "checking accumulation against expected. lengths:[{}=={} - {}]",
                    acc.len(),
                    expectation.len(),
                    acc.len() == expectation.len()
                );

                let result = std::panic::catch_unwind(|| {
                    assert_eq!(acc.len(), expectation.len());
                    assert_eq!(acc, expectation);
                    true
                });

                match result {
                    Ok(check) => check,
                    Err(err) => {
                        tracing::info!(error=?err, "check accumulation failed.");
                        false
                    },
                }
            })
            .await
        );

        if !result {
            anyhow::bail!("failed accumulation check.")
        }

        let acc = assert_ok!(stage::FoldCmd::get_and_reset_accumulation(&self.tx_sink_api).await);
        tracing::info!("sink accumulation before reset = {acc:?}");

        Ok(())
    }

    #[allow(unused)]
    #[tracing::instrument(level = "info", skip(self, check_accumulation))]
    pub async fn check_sink_accumulation<F>(
        &self, label: &str, timeout: Duration, mut check_accumulation: F,
    ) -> anyhow::Result<bool>
    where
        F: FnMut(Vec<Data>) -> bool,
    {
        use std::time::Instant;
        let deadline = Instant::now() + timeout;
        let step = Duration::from_millis(50);

        let mut result = false;

        loop {
            let check_span = tracing::info_span!("DMR check sink accumulation", %label);
            let _ = check_span.enter();

            if Instant::now() < deadline {
                let acc = self.inspect_sink().await;
                if acc.is_ok() {
                    let acc = acc?;
                    tracing::info!(?acc, len=?acc.len(), "inspecting sink");

                    result = check_accumulation(acc);

                    if !result {
                        tracing::warn!(
                            ?result,
                            "sink accumulation check predicate - retrying after {:?}.",
                            step
                        );
                        tokio::time::sleep(step).await;
                    } else {
                        tracing::info!(?result, "sink accumulation passed check predicate.");
                        break;
                    }
                } else {
                    tracing::error!(?acc, "failed to inspect sink");
                    anyhow::bail!("failed to inspect sink");
                }
            } else {
                tracing::error!(?timeout, "check timeout exceeded - stopping check.");
                anyhow::bail!(format!(
                    "check {:?} timeout exceeded - stopping check.",
                    timeout
                ));
            }
        }

        Ok(result)
    }

    #[tracing::instrument(level = "warn", skip(self))]
    pub async fn close(mut self) -> anyhow::Result<Vec<Data>> {
        stage::ActorSourceCmd::stop(&self.tx_data_sensor_api).await?;
        stage::ActorSourceCmd::stop(&self.tx_context_sensor_api).await?;
        self.graph_handle.await?;
        let result = self.rx_sink.take().unwrap().await?;
        Ok(result)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_eligibility_happy_flow() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_eligibility_happy_flow");
    let _ = main_span.enter();

    let settings = EligibilitySettings::default()
        .with_source(
            PolicySource::from_template_file("../resources/eligibility.polar")
                .expect("failed to create eligibility policy source"),
        )
        .with_source(
            PolicySource::from_template_file("../resources/eligibility_ext.polar")
                .expect("failed to create eligibility_ext policy source"),
        )
        .with_template_data(EligibilityTemplateData {
            cooling_secs: Some(4 * 60 * 60), // 4 hours
            stable_secs: Some(2 * 60 * 60),  // 2 hours
            ..EligibilityTemplateData::default()
        });

    let policy = EligibilityPolicy::new(&settings);

    let stage = PolicyPhase::strip_policy_outcome("test_eligibility", policy).await?;

    let mut flow = TestFlow::new(stage).await?;

    let now = Utc::now();
    let t1 = now - chrono::Duration::days(1);

    let ctx_1 = make_context(
        None,
        false,
        false,
        t1,
        maplit::hashmap! { "location_code".to_string() => 3_i32.to_telemetry() },
    );

    tracing::info!(?ctx_1, "pushing test context...");
    assert_ok!(flow.push_context(ctx_1).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let data_1 = make_test_item(maplit::hashmap! {"foo".to_string() => "bar".into()});
    flow.push_data(data_1.clone()).await?;

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ItemPassed(_, _));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_eligibility_block_on_active_deployment() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_eligibility_block_on_active_deployment");
    let _ = main_span.enter();

    let settings = EligibilitySettings::default()
        .with_source(
            PolicySource::from_template_file("../resources/eligibility.polar")
                .expect("failed to create eligibility policy source"),
        )
        .with_source(
            PolicySource::from_template_file("../resources/eligibility_ext.polar")
                .expect("failed to create eligibility_ext policy source"),
        )
        .with_template_data(EligibilityTemplateData { ..EligibilityTemplateData::default() });

    let policy = EligibilityPolicy::new(&settings);

    let stage = PolicyPhase::strip_policy_outcome("test_eligibility", policy).await?;

    let mut flow = TestFlow::new(stage).await?;

    let now = Utc::now();
    let t1 = now - chrono::Duration::days(1);

    let ctx_1 = make_context(
        None,
        true,
        false,
        t1,
        maplit::hashmap! { "location_code".to_string() => 3_i32.to_telemetry() },
    );

    tracing::info!(?ctx_1, "pushing test context...");
    assert_ok!(flow.push_context(ctx_1).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let data_1 = make_test_item(maplit::hashmap! {"foo".to_string() => "bar".into()});
    flow.push_data(data_1.clone()).await?;

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ItemBlocked(_, _));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_eligibility_block_on_recent_deployment() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_eligibility_block_on_recent_deployment");
    let _ = main_span.enter();

    let cooling_duration = chrono::Duration::minutes(15);
    let settings = EligibilitySettings::default()
        .with_source(
            PolicySource::from_template_file("../resources/eligibility.polar")
                .expect("failed to create eligibility policy source"),
        )
        .with_source(
            PolicySource::from_template_file("../resources/eligibility_ext.polar")
                .expect("failed to create eligibility_ext policy source"),
        )
        .with_template_data(EligibilityTemplateData {
            cooling_secs: Some(cooling_duration.num_seconds() as u32),
            ..EligibilityTemplateData::default()
        });

    let policy = EligibilityPolicy::new(&settings);

    let stage = PolicyPhase::strip_policy_outcome("test_eligibility", policy).await?;

    let mut flow = TestFlow::new(stage).await?;

    let now = Utc::now();
    let ts_cold = now - cooling_duration + chrono::Duration::minutes(1);

    let ctx_cold = make_context(
        None,
        false,
        true,
        ts_cold,
        maplit::hashmap! { "location_code".to_string() => 3_i32.to_telemetry() },
    );

    tracing::info!(?ctx_cold, "pushing test context...");
    assert_ok!(flow.push_context(ctx_cold).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let data_1 = make_test_item(maplit::hashmap! {"foo".to_string() => "bar".into()});
    flow.push_data(data_1.clone()).await?;

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ItemBlocked(_, _));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_eligibility_block_on_recent_failure() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_eligibility_block_on_recent_failure");
    let _ = main_span.enter();

    let stability_window = chrono::Duration::minutes(15);
    let settings = EligibilitySettings::default()
        .with_source(
            PolicySource::from_template_file("../resources/eligibility.polar")
                .expect("failed to create eligibility policy source"),
        )
        .with_source(
            PolicySource::from_template_file("../resources/eligibility_ext.polar")
                .expect("failed to create eligibility_ext policy source"),
        )
        .with_template_data(EligibilityTemplateData {
            stable_secs: Some(stability_window.num_seconds() as u32),
            ..EligibilityTemplateData::default()
        });

    let policy = EligibilityPolicy::new(&settings);

    let stage = PolicyPhase::strip_policy_outcome("test_eligibility", policy).await?;

    let mut flow = TestFlow::new(stage).await?;

    let now = Utc::now();
    let ts = now - stability_window + chrono::Duration::minutes(1);

    let ctx = make_context(
        Some(ts),
        false,
        false,
        ts,
        maplit::hashmap! { "location_code".to_string() => 3_i32.to_telemetry() },
    );

    tracing::info!(?ctx, "pushing test context...");
    assert_ok!(flow.push_context(ctx).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let data_1 = make_test_item(maplit::hashmap! {"foo".to_string() => "bar".into()});
    flow.push_data(data_1.clone()).await?;

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ItemBlocked(_, _));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_eligibility_block_on_rescaling() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_eligibility_block_on_rescaling");
    let _ = main_span.enter();

    let settings = EligibilitySettings::default()
        .with_source(
            PolicySource::from_template_file("../resources/eligibility.polar")
                .expect("failed to create eligibility policy source"),
        )
        .with_source(
            PolicySource::from_template_file("../resources/eligibility_ext.polar")
                .expect("failed to create eligibility_ext policy source"),
        )
        .with_template_data(EligibilityTemplateData { ..EligibilityTemplateData::default() });

    let policy = EligibilityPolicy::new(&settings);

    let stage = PolicyPhase::strip_policy_outcome("test_eligibility", policy).await?;

    let mut flow = TestFlow::new(stage).await?;

    let now = Utc::now();
    let t1 = now - chrono::Duration::days(1);

    let ctx_1 = make_context(
        None,
        false,
        true,
        t1,
        maplit::hashmap! { "location_code".to_string() => 3_i32.to_telemetry() },
    );

    tracing::info!(?ctx_1, "pushing test context...");
    assert_ok!(flow.push_context(ctx_1).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let data_1 = make_test_item(maplit::hashmap! {"foo".to_string() => "bar".into()});
    flow.push_data(data_1.clone()).await?;

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ItemBlocked(_, _));
    Ok(())
}

static ID_GENERATOR: Lazy<Mutex<ProctorIdGenerator<()>>> =
    Lazy::new(|| Mutex::new(ProctorIdGenerator::default()));

pub fn make_context(
    last_failure: Option<DateTime<Utc>>, is_deploying: bool, is_rescaling: bool,
    last_deployment: DateTime<Utc>, custom: telemetry::TableType,
) -> Context {
    let mut gen = ID_GENERATOR.lock().unwrap();

    EligibilityContext {
        recv_timestamp: Timestamp::now(),
        correlation_id: gen.next_id().relabel(),
        all_sinks_healthy: true,
        job: JobStatus { last_failure },
        cluster: ClusterStatus { is_deploying, is_rescaling, last_deployment },
        custom,
    }
}

pub fn make_test_item(custom: telemetry::TableType) -> Data {
    let mut gen = ID_GENERATOR.lock().unwrap();

    let catalog = MetricCatalog {
        recv_timestamp: Timestamp::now(),
        correlation_id: gen.next_id().relabel(),
        health: JobHealthMetrics::default(),
        flow: FlowMetrics::default(),
        cluster: ClusterMetrics { nr_active_jobs: 1, ..ClusterMetrics::default() },
        custom,
    };

    AppDataWindow::from_size(catalog, 60, Duration::from_secs(10))
}
