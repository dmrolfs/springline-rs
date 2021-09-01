use std::time::Duration;

use claim::*;
use pretty_assertions::assert_eq;
use proctor::elements::{self, PolicyFilterEvent, PolicySettings, PolicySource, Timestamp};
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::phases::policy_phase::PolicyPhase;
use springline::phases::governance::{
    make_governance_transform, GovernanceContext, GovernancePolicy,
};
use springline::phases::plan::ScalePlan;
use std::path::PathBuf;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type Data = ScalePlan;
type Context = GovernanceContext;

lazy_static::lazy_static! {
    static ref GOVERNANCE_PREAMBLE: PolicySource = PolicySource::File(PathBuf::from("./resources/governance_preamble.polar"));
    static ref POLICY_SETTINGS: PolicySettings = PolicySettings::default().with_source(GOVERNANCE_PREAMBLE.clone());
}

#[allow(dead_code)]
struct TestFlow {
    pub graph_handle: JoinHandle<()>,
    pub tx_data_source_api: stage::ActorSourceApi<Data>,
    pub tx_context_source_api: stage::ActorSourceApi<Context>,
    pub tx_governance_api: elements::PolicyFilterApi<Context>,
    pub rx_governance_monitor: elements::PolicyFilterMonitor<Data, Context>,
    pub tx_sink_api: stage::FoldApi<Vec<Data>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<Data>>>,
}

impl TestFlow {
    pub async fn new(governance_stage: PolicyPhase<Data, Data, Context>) -> anyhow::Result<Self> {
        let data_source: stage::ActorSource<Data> = stage::ActorSource::new("plan_source");
        let tx_data_source_api = data_source.tx_api();

        let context_source: stage::ActorSource<Context> = stage::ActorSource::new("context_source");
        let tx_context_source_api = context_source.tx_api();

        let tx_governance_api = governance_stage.tx_api();
        let rx_governance_monitor = governance_stage.rx_monitor();

        let mut sink = stage::Fold::<_, Data, _>::new("sink", Vec::new(), |mut acc, item| {
            acc.push(item);
            acc
        });
        let tx_sink_api = sink.tx_api();
        let rx_sink = sink.take_final_rx();

        (data_source.outlet(), governance_stage.inlet())
            .connect()
            .await;
        (context_source.outlet(), governance_stage.context_inlet())
            .connect()
            .await;
        (governance_stage.outlet(), sink.inlet()).connect().await;
        assert!(governance_stage.inlet().is_attached().await);
        assert!(governance_stage.context_inlet().is_attached().await);
        assert!(governance_stage.outlet().is_attached().await);

        let mut graph = Graph::default();
        graph.push_back(Box::new(data_source)).await;
        graph.push_back(Box::new(context_source)).await;
        graph.push_back(Box::new(governance_stage)).await;
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
            tx_data_source_api,
            tx_context_source_api,
            tx_governance_api,
            rx_governance_monitor,
            tx_sink_api,
            rx_sink,
        })
    }

    pub async fn push_data(&self, data: Data) -> anyhow::Result<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(data);
        self.tx_data_source_api.send(cmd)?;
        Ok(ack.await?)
    }

    pub async fn push_context(&self, context: Context) -> anyhow::Result<()> {
        let (cmd, ack) = stage::ActorSourceCmd::push(context);
        self.tx_context_source_api.send(cmd)?;
        Ok(ack.await?)
    }

    #[allow(dead_code)]
    pub async fn tell_policy(
        &self,
        command_rx: (
            elements::PolicyFilterCmd<Context>,
            oneshot::Receiver<proctor::Ack>,
        ),
    ) -> anyhow::Result<proctor::Ack> {
        self.tx_governance_api.send(command_rx.0)?;
        Ok(command_rx.1.await?)
    }

    pub async fn recv_policy_event(
        &mut self,
    ) -> anyhow::Result<elements::PolicyFilterEvent<Data, Context>> {
        Ok(self.rx_governance_monitor.recv().await?)
    }

    #[allow(dead_code)]
    pub async fn inspect_policy_context(
        &self,
    ) -> anyhow::Result<elements::PolicyFilterDetail<Context>> {
        let (cmd, detail) = elements::PolicyFilterCmd::inspect();
        self.tx_governance_api.send(cmd)?;

        let result = detail.await.map(|d| {
            tracing::info!(detail=?d, "inspected policy.");
            d
        })?;

        Ok(result)
    }

    pub async fn inspect_sink(&self) -> anyhow::Result<Vec<Data>> {
        let (cmd, acc) = stage::FoldCmd::get_accumulation();
        self.tx_sink_api.send(cmd)?;
        acc.await
            .map(|a| {
                tracing::info!(accumulation=?a, "inspected sink accumulation");
                a
            })
            .map_err(|err| err.into())
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn check_scenario(
        &mut self,
        label: &str,
        data: ScalePlan,
        expectation: Vec<ScalePlan>,
    ) -> anyhow::Result<()> {
        let scenario_span =
            tracing::info_span!("DMR check scenario", %label, ?data, ?expectation, );
        let _ = scenario_span.enter();

        let timeout = Duration::from_millis(250);

        assert_ok!(self.push_data(data).await);
        claim::assert_matches!(
            assert_ok!(self.rx_governance_monitor.recv().await),
            PolicyFilterEvent::ItemPassed
        );

        let result = assert_ok!(
            self.check_sink_accumulation(label, timeout, |acc| {
                let check_span =
                    tracing::info_span!("DMR check collection accumulation", %label, ?expectation, ?timeout);
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

        let (reset_cmd, reset_rx) = stage::FoldCmd::get_and_reset_accumulation();
        assert_ok!(self.tx_sink_api.send(reset_cmd));
        let ack = assert_ok!(reset_rx.await);
        tracing::info!("sink reset ack = {:?}", ack);

        Ok(())
    }

    #[allow(unused)]
    #[tracing::instrument(level = "info", skip(self, check_accumulation))]
    pub async fn check_sink_accumulation<F>(
        &self,
        label: &str,
        timeout: Duration,
        mut check_accumulation: F,
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
        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_data_source_api.send(stop)?;

        let (stop, _) = stage::ActorSourceCmd::stop();
        self.tx_context_source_api.send(stop)?;

        self.graph_handle.await?;

        let result = self.rx_sink.take().unwrap().await?;
        Ok(result)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_and_happy() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_and_happy");
    let _ = main_span.enter();

    let policy = GovernancePolicy::new(&POLICY_SETTINGS);

    let governance_stage = PolicyPhase::with_transform(
        "test_governance",
        policy,
        make_governance_transform("common_governance_transform"),
    )
    .await;

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_cluster_size = 2;
    let max_cluster_size = 10;
    let max_scaling_step = 5;
    let context = GovernanceContext {
        min_cluster_size,
        max_cluster_size,
        max_scaling_step,
        custom: Default::default(),
    };
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::new_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "happy_1",
            ScalePlan {
                timestamp,
                target_nr_task_managers: 8,
                current_nr_task_managers: 4
            },
            vec![ScalePlan {
                timestamp,
                target_nr_task_managers: 8,
                current_nr_task_managers: 4
            }]
        )
        .await
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_below_min_cluster_size() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_below_min_cluster_size");
    let _ = main_span.enter();

    let policy = GovernancePolicy::new(&POLICY_SETTINGS);

    let governance_stage = PolicyPhase::with_transform(
        "test_governance",
        policy,
        make_governance_transform("common_governance_transform"),
    )
    .await;

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_cluster_size = 2;
    let max_cluster_size = 10;
    let max_scaling_step = 5;
    let context = GovernanceContext {
        min_cluster_size,
        max_cluster_size,
        max_scaling_step,
        custom: Default::default(),
    };
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::new_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "below min cluster size",
            ScalePlan {
                timestamp,
                target_nr_task_managers: 0,
                current_nr_task_managers: 4
            },
            vec![ScalePlan {
                timestamp,
                target_nr_task_managers: min_cluster_size,
                current_nr_task_managers: 4
            }]
        )
        .await
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_above_max_cluster_size() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_above_max_cluster_size");
    let _ = main_span.enter();

    let policy = GovernancePolicy::new(&POLICY_SETTINGS);

    let governance_stage = PolicyPhase::with_transform(
        "test_governance",
        policy,
        make_governance_transform("common_governance_transform"),
    )
    .await;

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_cluster_size = 2;
    let max_cluster_size = 10;
    let max_scaling_step = 5;
    let context = GovernanceContext {
        min_cluster_size,
        max_cluster_size,
        max_scaling_step,
        custom: Default::default(),
    };
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::new_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "above max cluster size",
            ScalePlan {
                timestamp,
                target_nr_task_managers: 999,
                current_nr_task_managers: 6
            },
            vec![ScalePlan {
                timestamp,
                target_nr_task_managers: max_cluster_size,
                current_nr_task_managers: 6
            }]
        )
        .await
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_step_up_too_big() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_step_up_too_big");
    let _ = main_span.enter();

    let policy = GovernancePolicy::new(&POLICY_SETTINGS);

    let governance_stage = PolicyPhase::with_transform(
        "test_governance",
        policy,
        make_governance_transform("common_governance_transform"),
    )
    .await;

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_cluster_size = 2;
    let max_cluster_size = 10;
    let max_scaling_step = 5;
    let context = GovernanceContext {
        min_cluster_size,
        max_cluster_size,
        max_scaling_step,
        custom: Default::default(),
    };
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::new_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "too big a scale up step",
            ScalePlan {
                timestamp,
                target_nr_task_managers: 9,
                current_nr_task_managers: 0
            },
            vec![ScalePlan {
                timestamp,
                target_nr_task_managers: max_scaling_step,
                current_nr_task_managers: 0
            }]
        )
        .await
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_step_down_too_big() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_step_down_too_big");
    let _ = main_span.enter();

    let policy = GovernancePolicy::new(&POLICY_SETTINGS);

    let governance_stage = PolicyPhase::with_transform(
        "test_governance",
        policy,
        make_governance_transform("common_governance_transform"),
    )
    .await;

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_cluster_size = 2;
    let max_cluster_size = 10;
    let max_scaling_step = 5;
    let context = GovernanceContext {
        min_cluster_size,
        max_cluster_size,
        max_scaling_step,
        custom: Default::default(),
    };
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::new_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "too big a scale down step",
            ScalePlan {
                timestamp,
                target_nr_task_managers: min_cluster_size,
                current_nr_task_managers: max_cluster_size
            },
            vec![ScalePlan {
                timestamp,
                target_nr_task_managers: max_cluster_size - max_scaling_step,
                current_nr_task_managers: max_cluster_size
            }]
        )
        .await
    );

    // todo step vs min/max precedent

    // todo test veto in subsequent test

    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_step_up_before_max() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_step_up_before_max");
    let _ = main_span.enter();

    let policy = GovernancePolicy::new(&POLICY_SETTINGS);

    let governance_stage = PolicyPhase::with_transform(
        "test_governance",
        policy,
        make_governance_transform("common_governance_transform"),
    )
    .await;

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_cluster_size = 2;
    let max_cluster_size = 10;
    let max_scaling_step = 5;
    let context = GovernanceContext {
        min_cluster_size,
        max_cluster_size,
        max_scaling_step,
        custom: Default::default(),
    };
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::new_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "too big a scale up step before max",
            ScalePlan {
                timestamp,
                target_nr_task_managers: 999,
                current_nr_task_managers: 0
            },
            vec![ScalePlan {
                timestamp,
                target_nr_task_managers: max_scaling_step,
                current_nr_task_managers: 0
            }]
        )
        .await
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_step_down_before_min() -> anyhow::Result<()> {
    lazy_static::initialize(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_step_down_before_min");
    let _ = main_span.enter();

    let policy = GovernancePolicy::new(&POLICY_SETTINGS);

    let governance_stage = PolicyPhase::with_transform(
        "test_governance",
        policy,
        make_governance_transform("common_governance_transform"),
    )
    .await;

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_cluster_size = 2;
    let max_cluster_size = 10;
    let max_scaling_step = 5;
    let context = GovernanceContext {
        min_cluster_size,
        max_cluster_size,
        max_scaling_step,
        custom: Default::default(),
    };
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::new_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "too big a scale down step before min",
            ScalePlan {
                timestamp,
                target_nr_task_managers: 0,
                current_nr_task_managers: max_cluster_size
            },
            vec![ScalePlan {
                timestamp,
                target_nr_task_managers: max_cluster_size - max_scaling_step,
                current_nr_task_managers: max_cluster_size
            }]
        )
        .await
    );

    // todo step vs min/max precedent

    // todo test veto in subsequent test

    Ok(())
}
