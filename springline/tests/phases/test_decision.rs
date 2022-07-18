use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use pretty_snowflake::MachineNode;
use proctor::elements::{
    self, PolicyOutcome, PolicySource, PolicySubscription, Telemetry, TelemetryValue, ToTelemetry,
};
use proctor::elements::{PolicySettings, QueryPolicy};
use proctor::graph::stage::{self, ThroughStage, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, Inlet, SinkShape, SourceShape};
use proctor::phases::policy_phase::PolicyPhase;
use proctor::phases::sense::clearinghouse::TelemetryCacheSettings;
use proctor::phases::sense::{self, Sense, SubscriptionRequirements, TelemetrySubscription};
use proctor::{AppData, ProctorContext};
use springline::flink::{AppDataWindow, MetricCatalog, MC_CLUSTER__NR_ACTIVE_JOBS, MC_CLUSTER__NR_TASK_MANAGERS};
use springline::phases::decision::{make_decision_transform, DecisionResult, DECISION_DIRECTION};
use springline::phases::decision::{DecisionContext, DecisionPolicy, DecisionTemplateData};
use springline::settings::{DecisionSettings, EngineSettings};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::fixtures::*;

type Data = AppDataWindow<MetricCatalog>;

lazy_static::lazy_static! {
    static ref DECISION_PREAMBLE: PolicySource = PolicySource::from_template_file("../resources/decision.polar").expect("failed to create decision policy source");
    static ref POLICY_SETTINGS: DecisionSettings = DecisionSettings::default()
        .with_source(DECISION_PREAMBLE.clone())
        .with_template_data(DecisionTemplateData {
            custom: maplit::hashmap! {
                "max_records_in_per_sec".to_string() => 3_f64.to_string(),
                "min_records_in_per_sec".to_string() => 1_f64.to_string(),
            },
            ..DecisionTemplateData::default()
        });
}

#[allow(dead_code)]
struct TestFlow<Out, C> {
    pub graph_handle: JoinHandle<()>,
    pub tx_data_sensor_api: stage::ActorSourceApi<Telemetry>,
    pub tx_context_sensor_api: stage::ActorSourceApi<Telemetry>,
    pub tx_clearinghouse_api: sense::ClearinghouseApi,
    pub tx_decision_api: elements::PolicyFilterApi<C, DecisionTemplateData>,
    pub rx_decision_monitor: elements::PolicyFilterMonitor<Data, C>,
    pub tx_sink_api: stage::FoldApi<Vec<Out>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<Out>>>,
}

impl<Out, C> TestFlow<Out, C>
where
    // In: AppData + Clone + DeserializeOwned + ToPolar,
    Out: AppData + Clone,
    C: ProctorContext,
{
    pub async fn new(
        sensor_out_subscription: TelemetrySubscription, context_subscription: TelemetrySubscription,
        decision_stage: impl ThroughStage<Data, Out>, decision_context_inlet: Inlet<C>,
        tx_decision_api: elements::PolicyFilterApi<C, DecisionTemplateData>,
        rx_decision_monitor: elements::PolicyFilterMonitor<Data, C>,
    ) -> anyhow::Result<Self> {
        let telemetry_sensor = stage::ActorSource::<Telemetry>::new("telemetry_sensor");
        let tx_data_sensor_api = telemetry_sensor.tx_api();

        let ctx_sensor = stage::ActorSource::<Telemetry>::new("context_sensor");
        let tx_context_sensor_api = ctx_sensor.tx_api();

        let cache_settings = TelemetryCacheSettings::default();
        let mut builder = Sense::builder(
            "sensor",
            vec![Box::new(telemetry_sensor), Box::new(ctx_sensor)],
            &cache_settings,
            MachineNode::default(),
        );
        let tx_clearinghouse_api = builder.clearinghouse.tx_api();

        tracing::info!("CONNECT CONTEXT SUBSCRIPTION...");
        let context_channel =
            sense::SubscriptionChannel::<C>::connect_subscription(context_subscription, (&mut builder).into()).await?;
        let sense = builder.build_for_out_subscription(sensor_out_subscription).await?;

        let engine_settings = EngineSettings::default();
        let collect = springline::phases::CollectMetricWindow::new("collect_window", &engine_settings);
        let mut sink = stage::Fold::<_, Out, _>::new("sink", Vec::new(), |mut acc, item| {
            acc.push(item);
            acc
        });
        let tx_sink_api = sink.tx_api();
        let rx_sink = sink.take_final_rx();

        (sense.outlet(), collect.inlet()).connect().await;
        (collect.outlet(), decision_stage.inlet()).connect().await;
        (context_channel.outlet(), decision_context_inlet).connect().await;
        (decision_stage.outlet(), sink.inlet()).connect().await;

        assert!(sense.outlet().is_attached().await);
        assert!(decision_stage.inlet().is_attached().await);
        assert!(decision_stage.outlet().is_attached().await);

        let mut graph = Graph::default();
        graph.push_back(Box::new(sense)).await;
        graph.push_back(Box::new(collect)).await;
        graph.push_back(Box::new(context_channel)).await;
        graph.push_back(Box::new(decision_stage)).await;
        graph.push_back(Box::new(sink)).await;

        let graph_handle = tokio::spawn(async move {
            graph
                .run()
                .await
                .map_err(|err| {
                    tracing::error!(error=?err, "graph run failed!!");
                    err
                })
                .expect("graph run failed")
        });

        Ok(Self {
            graph_handle,
            tx_data_sensor_api,
            tx_context_sensor_api,
            tx_clearinghouse_api,
            tx_decision_api,
            rx_decision_monitor,
            tx_sink_api,
            rx_sink,
        })
    }

    pub async fn push_telemetry(&self, telemetry: Telemetry) -> anyhow::Result<()> {
        stage::ActorSourceCmd::push(&self.tx_data_sensor_api, telemetry)
            .await
            .map_err(|err| err.into())
    }

    pub async fn push_context<'a, I>(&self, context_data: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = (&'a str, TelemetryValue)>,
    {
        let telemetry = context_data.into_iter().collect();
        stage::ActorSourceCmd::push(&self.tx_context_sensor_api, telemetry)
            .await
            .map_err(|err| err.into())
    }

    #[allow(dead_code)]
    pub async fn tell_policy(
        &self,
        command_rx: (
            elements::PolicyFilterCmd<C, DecisionTemplateData>,
            oneshot::Receiver<proctor::Ack>,
        ),
    ) -> anyhow::Result<proctor::Ack> {
        self.tx_decision_api.send(command_rx.0)?;
        command_rx.1.await.map_err(|err| err.into())
    }

    pub async fn recv_policy_event(&mut self) -> anyhow::Result<Arc<elements::PolicyFilterEvent<Data, C>>> {
        self.rx_decision_monitor.recv().await.map_err(|err| err.into())
    }

    #[allow(dead_code)]
    pub async fn inspect_policy_context(
        &self,
    ) -> anyhow::Result<elements::PolicyFilterDetail<C, DecisionTemplateData>> {
        elements::PolicyFilterCmd::inspect(&self.tx_decision_api)
            .await
            .map(|d| {
                tracing::info!(detail=?d, "inspected policy.");
                d
            })
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
        &self, label: &str, timeout: Duration, mut check_size: impl FnMut(Vec<Out>) -> bool,
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
        stage::ActorSourceCmd::stop(&self.tx_data_sensor_api).await?;
        stage::ActorSourceCmd::stop(&self.tx_context_sensor_api).await?;
        self.graph_handle.await?;
        self.rx_sink.take().unwrap().await.map_err(|err| err.into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_decision_carry_policy_result() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_decision_carry_policy_result");
    let _ = main_span.enter();

    let telemetry_subscription = TelemetrySubscription::new("measurements")
        .with_required_fields(<MetricCatalog as SubscriptionRequirements>::required_fields())
        .with_optional_fields(maplit::hashset! {
            "all_sinks_healthy",
            MC_CLUSTER__NR_TASK_MANAGERS,
        });

    let policy = DecisionPolicy::new(&POLICY_SETTINGS.clone().with_source(PolicySource::from_template_string(
        format!("{}_basis", DecisionPolicy::base_template_name()),
        r###"
            | {{> preamble}}
            | scale_up(item, _context, reason) if {{max_records_in_per_sec}} < item.flow.records_in_per_sec and reason = "lagging_behind";
            | scale_down(item, _context, reason) if item.flow.records_in_per_sec < {{min_records_in_per_sec}} and reason = "too_comfortable";
            "###,
    )?));

    let settings = DecisionSettings {
        required_subscription_fields: maplit::hashset! { "all_sinks_healthy".to_string(), },
        ..PolicySettings::default()
    };

    let context_subscription = policy.subscription("decision_context", &settings);

    let decision_stage = PolicyPhase::carry_policy_outcome("carry_policy_decision", policy).await?;
    let decision_context_inlet = decision_stage.context_inlet();
    let tx_decision_api = decision_stage.tx_api();
    let rx_decision_monitor = decision_stage.rx_monitor();

    let mut flow = TestFlow::new(
        telemetry_subscription,
        context_subscription,
        decision_stage,
        decision_context_inlet,
        tx_decision_api,
        rx_decision_monitor,
    )
    .await?;

    tracing::info!("PUSHING CONTEXT...");
    flow.push_context(maplit::hashmap! {
        "all_sinks_healthy" => true.to_telemetry(),
        MC_CLUSTER__NR_ACTIVE_JOBS => 1.to_telemetry(),
        MC_CLUSTER__NR_TASK_MANAGERS => 4.to_telemetry(),
    })
    .await?;

    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    tracing::info!("pushing test item padding - the extra parts req in a metrics subscription...");
    let padding = make_test_item_padding();
    flow.push_telemetry(padding).await?;

    let ts = *DT_1 + chrono::Duration::hours(1);
    let item = make_test_item(&ts, std::f64::consts::PI, 1);
    tracing::warn!(?item, "DMR-A.1: created item to push.");
    flow.push_telemetry(item).await?;
    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ItemPassed(_, _));
    tracing::info!("DMR-waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 1)
            .await?
    );

    let item = make_test_item(&ts, std::f64::consts::E, 2);
    // let telemetry = Telemetry::try_from(&item);
    flow.push_telemetry(item).await?;
    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ItemBlocked(_, _));
    tracing::warn!(?event, "DMR-C: item dropped confirmed");

    let item = make_test_item(&ts, std::f64::consts::LN_2, 1);
    tracing::warn!(?item, "DMR-D.1: created item to push.");
    flow.push_telemetry(item).await?;
    tracing::info!("waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 2)
            .await?
    );

    let actual: Vec<PolicyOutcome<Data, DecisionContext>> = flow.close().await?;
    tracing::warn!(?actual, "DMR: 08. Verify final accumulation...");
    let actual_vals: Vec<(f64, Option<String>)> = actual
        .into_iter()
        .map(|a| {
            let direction: Option<String> = a
                .binding(DECISION_DIRECTION)
                .expect("failed to pull string from direction binding.")
                .first()
                .cloned();

            (a.item.flow.records_in_per_sec, direction)
        })
        .collect();

    assert_eq!(
        actual_vals,
        vec![
            (std::f64::consts::PI, Some("up".to_string())),
            (std::f64::consts::LN_2, Some("down".to_string())),
        ]
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_decision_common() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_decision_common");
    let _ = main_span.enter();

    let mut optional_fields = <MetricCatalog as SubscriptionRequirements>::optional_fields();
    let lag_fields = maplit::hashset! {
            "flow.source_records_lag_max".into(),
            "flow.source_assigned_partitions".into(),
            "flow.source_total_lag".into(),
            "flow.source_records_consumed_rate".into(),
    };
    optional_fields.extend(lag_fields);

    let telemetry_subscription = TelemetrySubscription::new("measurements")
        .with_required_fields(<MetricCatalog as SubscriptionRequirements>::required_fields())
        .with_optional_fields(optional_fields);

    let policy = DecisionPolicy::new(&POLICY_SETTINGS.clone().with_source(PolicySource::from_template_string(
        format!("{}_basis", DecisionPolicy::base_template_name()),
        r###"
            | {{> preamble}}
            | scale_up(item, _context, reason) if
            |   not item.flow.source_records_lag_max == nil
            |   and {{max_records_in_per_sec}} < item.flow.records_in_per_sec
            |   and reason = "lagging_behind";
            | scale_down(item, _context, reason) if item.flow.records_in_per_sec < {{min_records_in_per_sec}} and reason = "too comfortable";
            "###,
    )?));

    let settings = DecisionSettings { ..PolicySettings::default() };

    let context_subscription = policy.subscription("decision_context", &settings);

    let decision_stage = PolicyPhase::with_transform(
        "common_decision".into(),
        policy,
        make_decision_transform("common_decision_transform"),
    )
    .await?;
    let decision_context_inlet = decision_stage.context_inlet();
    let tx_decision_api: elements::PolicyFilterApi<DecisionContext, DecisionTemplateData> = decision_stage.tx_api();
    let rx_decision_monitor: elements::PolicyFilterMonitor<Data, DecisionContext> = decision_stage.rx_monitor();

    let mut flow = TestFlow::new(
        telemetry_subscription,
        context_subscription,
        decision_stage,
        decision_context_inlet,
        tx_decision_api,
        rx_decision_monitor,
    )
    .await?;

    flow.push_context(maplit::hashmap! {
        "all_sinks_healthy" => true.to_telemetry(),
        MC_CLUSTER__NR_ACTIVE_JOBS => 1.to_telemetry(),
        MC_CLUSTER__NR_TASK_MANAGERS => 4.to_telemetry(),
    })
    .await?;

    // decision context remains a zero definition for test
    // let event = flow.recv_policy_event().await?;
    // tracing::info!(?event, "DMR: TESTING policy event for context change");
    // claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    tracing::info!("DMR: pushing metrics padding - req metrics subscriptions fields not used in test.");
    flow.push_telemetry(make_test_item_padding()).await?;

    let ts = *DT_1 + chrono::Duration::hours(1);
    let item = make_test_item(&ts, std::f64::consts::PI, 1);
    tracing::warn!(?item, "DMR-A.1: created item to push.");

    flow.push_telemetry(item).await?;
    let event = &*flow.recv_policy_event().await?;
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ItemPassed(_, _));
    tracing::info!("DMR-waiting for *first* item to reach sink...");
    assert!(
        flow.check_sink_accumulation("first", Duration::from_millis(500), |acc| acc.len() == 1)
            .await?
    );

    let item = make_test_item(&ts, std::f64::consts::E, 2);
    flow.push_telemetry(item).await?;
    let event = &*flow.recv_policy_event().await?;
    tracing::info!(?event, "DMR-2: TESTING policy event for blockage");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ItemBlocked(_, _));
    tracing::warn!(?event, "DMR-C: item dropped confirmed");

    let item = make_test_item(&ts, std::f64::consts::LN_2, 1);
    tracing::warn!(?item, "DMR-D.1: created item to push.");
    flow.push_telemetry(item).await?;
    tracing::info!("waiting for item to reach sink...");
    assert!(
        flow.check_sink_accumulation("first", Duration::from_millis(250), |acc| acc.len() == 2)
            .await?
    );

    let actual: Vec<DecisionResult<Data>> = flow.close().await?;
    tracing::warn!(?actual, "DMR: 08. Verify final accumulation...");
    let actual_vals: Vec<(f64, &'static str)> = actual
        .into_iter()
        .map(|a| match a {
            DecisionResult::ScaleUp(item) => (item.flow.records_in_per_sec, "up"),
            DecisionResult::ScaleDown(item) => (item.flow.records_in_per_sec, "down"),
            DecisionResult::NoAction(item) => (item.flow.records_in_per_sec, "no action"),
        })
        .collect();

    assert_eq!(
        actual_vals,
        vec![(std::f64::consts::PI, "up"), (std::f64::consts::LN_2, "down"),]
    );

    Ok(())
}
