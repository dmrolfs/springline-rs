use std::sync::Arc;
use std::time::Duration;

use claim::*;
use pretty_assertions::assert_eq;
use proctor::elements::{self, PolicyFilterEvent, Timestamp};
use proctor::graph::stage::{self, WithApi, WithMonitor};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::MetaData;
use springline::flink::Parallelism;
use springline::model::NrReplicas;
use springline::phases::governance::{GovernanceContext, GovernanceStage};
use springline::phases::plan::ScalePlan;
use springline::Env;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::CORRELATION_ID;

type DataT = ScalePlan;
type Data = Env<DataT>;
type Context = Env<GovernanceContext>;

#[allow(dead_code)]
struct TestFlow {
    pub graph_handle: JoinHandle<()>,
    pub tx_data_sensor_api: stage::ActorSourceApi<Data>,
    pub tx_context_sensor_api: stage::ActorSourceApi<Context>,
    pub rx_governance_monitor: elements::PolicyFilterMonitor<Data, Context>,
    pub tx_sink_api: stage::FoldApi<Vec<Data>>,
    pub rx_sink: Option<oneshot::Receiver<Vec<Data>>>,
}

#[allow(dead_code)]
impl TestFlow {
    pub async fn new(governance_stage: GovernanceStage<DataT>) -> anyhow::Result<Self> {
        let data_sensor: stage::ActorSource<Data> = stage::ActorSource::new("plan_sensor");
        let tx_data_sensor_api = data_sensor.tx_api();

        let context_sensor: stage::ActorSource<Context> = stage::ActorSource::new("context_sensor");
        let tx_context_sensor_api = context_sensor.tx_api();

        let rx_governance_monitor = governance_stage.rx_monitor();

        let mut sink = stage::Fold::<_, Data, _>::new("sink", Vec::new(), |mut acc, item| {
            acc.push(item);
            acc
        });
        let tx_sink_api = sink.tx_api();
        let rx_sink = sink.take_final_rx();

        (data_sensor.outlet(), governance_stage.inlet()).connect().await;
        (context_sensor.outlet(), governance_stage.context_inlet()).connect().await;
        (governance_stage.outlet(), sink.inlet()).connect().await;
        assert!(governance_stage.inlet().is_attached().await);
        assert!(governance_stage.context_inlet().is_attached().await);
        assert!(governance_stage.outlet().is_attached().await);

        let mut graph = Graph::default();
        graph.push_back(Box::new(data_sensor)).await;
        graph.push_back(Box::new(context_sensor)).await;
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
            tx_data_sensor_api,
            tx_context_sensor_api,
            rx_governance_monitor,
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

    pub async fn recv_policy_event(
        &mut self,
    ) -> anyhow::Result<Arc<elements::PolicyFilterEvent<Data, Context>>> {
        Ok(self.rx_governance_monitor.recv().await?)
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

        let timeout = Duration::from_millis(500);

        assert_ok!(self.push_data(data).await);
        claim::assert_matches!(
            &*assert_ok!(self.rx_governance_monitor.recv().await),
            &PolicyFilterEvent::ItemPassed(_, _)
        );

        let result = assert_ok!(
            self.check_sink_accumulation(label, timeout, |acc| {
                let check_span =
                    tracing::info_span!("DMR check sensor accumulation", %label, ?expectation, ?timeout);
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
        tracing::info!("sink acc before reset = {acc:?}");

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
async fn test_flink_governance_flow_simple_and_happy() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_and_happy");
    let _ = main_span.enter();

    let governance_stage = GovernanceStage::new("test_governance");

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_parallelism = Parallelism::new(1);
    let max_parallelism = Parallelism::new(20);
    let min_cluster_size = NrReplicas::new(2);
    let max_cluster_size = NrReplicas::new(10);
    let min_scaling_step = 1;
    let max_scaling_step = 5;
    let context = Env::from_parts(
        MetaData::default(),
        GovernanceContext {
            min_parallelism,
            max_parallelism,
            min_cluster_size,
            max_cluster_size,
            min_scaling_step,
            max_scaling_step,
            custom: Default::default(),
        },
    );
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::from_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "happy_1",
            Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(4),
                    target_job_parallelism: Parallelism::new(8),
                    current_nr_taskmanagers: NrReplicas::new(4),
                    target_nr_taskmanagers: NrReplicas::new(8),
                    task_slots_per_taskmanager: 1_f64,
                }
            ),
            vec![Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(4),
                    target_job_parallelism: Parallelism::new(8),
                    current_nr_taskmanagers: NrReplicas::new(4),
                    target_nr_taskmanagers: NrReplicas::new(8),
                    task_slots_per_taskmanager: 1_f64,
                }
            )]
        )
        .await
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_below_min_cluster_size() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_below_min_cluster_size");
    let _ = main_span.enter();

    let governance_stage = GovernanceStage::new("common_governance_transform");

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_parallelism = Parallelism::new(1);
    let max_parallelism = Parallelism::new(20);
    let min_cluster_size = NrReplicas::new(2);
    let max_cluster_size = NrReplicas::new(10);
    let min_scaling_step = 1;
    let max_scaling_step = 5;
    let context = Env::from_parts(
        MetaData::default(),
        GovernanceContext {
            min_parallelism,
            max_parallelism,
            min_cluster_size,
            max_cluster_size,
            min_scaling_step,
            max_scaling_step,
            custom: Default::default(),
        },
    );
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::from_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "below min cluster size",
            Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(4),
                    target_job_parallelism: Parallelism::new(0),
                    current_nr_taskmanagers: NrReplicas::new(4),
                    target_nr_taskmanagers: NrReplicas::new(0),
                    task_slots_per_taskmanager: 1_f64,
                }
            ),
            vec![Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(4),
                    target_job_parallelism: min_parallelism,
                    current_nr_taskmanagers: NrReplicas::new(4),
                    target_nr_taskmanagers: min_cluster_size,
                    task_slots_per_taskmanager: 1_f64,
                }
            )]
        )
        .await
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_above_max_cluster_size() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_above_max_cluster_size");
    let _ = main_span.enter();

    let governance_stage = GovernanceStage::new("governance_flow_simple_above_max_cluster_size");

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_parallelism = Parallelism::new(1);
    let max_parallelism = Parallelism::new(20);
    let min_cluster_size = NrReplicas::new(2);
    let max_cluster_size = NrReplicas::new(10);
    let min_scaling_step = 1;
    let max_scaling_step = 5;
    let context = Env::from_parts(
        MetaData::default(),
        GovernanceContext {
            min_parallelism,
            max_parallelism,
            min_cluster_size,
            max_cluster_size,
            min_scaling_step,
            max_scaling_step,
            custom: Default::default(),
        },
    );
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::from_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "above max cluster size",
            Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(6),
                    target_job_parallelism: Parallelism::new(999),
                    current_nr_taskmanagers: NrReplicas::new(6),
                    target_nr_taskmanagers: NrReplicas::new(999),
                    task_slots_per_taskmanager: 1_f64,
                }
            ),
            vec![Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(6),
                    target_job_parallelism: Parallelism::new(6 + max_scaling_step),
                    current_nr_taskmanagers: NrReplicas::new(6),
                    target_nr_taskmanagers: max_cluster_size,
                    task_slots_per_taskmanager: 1_f64,
                }
            )]
        )
        .await
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_step_up_too_big() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_step_up_too_big");
    let _ = main_span.enter();

    let governance_stage = GovernanceStage::new("governance_flow_simple_step_up_too_big");

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_parallelism = Parallelism::new(1);
    let max_parallelism = Parallelism::new(20);
    let min_cluster_size = NrReplicas::new(2);
    let max_cluster_size = NrReplicas::new(10);
    let min_scaling_step = 1;
    let max_scaling_step = 5;
    let context = Env::from_parts(
        MetaData::default(),
        GovernanceContext {
            min_parallelism,
            max_parallelism,
            min_cluster_size,
            max_cluster_size,
            min_scaling_step,
            max_scaling_step,
            custom: Default::default(),
        },
    );
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::from_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "too big a scale up step",
            Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(0),
                    target_job_parallelism: Parallelism::new(9),
                    current_nr_taskmanagers: NrReplicas::new(0),
                    target_nr_taskmanagers: NrReplicas::new(9),
                    task_slots_per_taskmanager: 1_f64,
                }
            ),
            vec![Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(0),
                    target_job_parallelism: Parallelism::new(max_scaling_step),
                    current_nr_taskmanagers: NrReplicas::new(0),
                    target_nr_taskmanagers: NrReplicas::new(max_scaling_step),
                    task_slots_per_taskmanager: 1_f64,
                }
            )]
        )
        .await
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_step_down_too_big() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_step_down_too_big");
    let _ = main_span.enter();

    let governance_stage = GovernanceStage::new("flow_simple_step_down_too_big");

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_parallelism = Parallelism::new(1);
    let max_parallelism = Parallelism::new(20);
    let min_cluster_size = NrReplicas::new(2);
    let max_cluster_size = NrReplicas::new(10);
    let min_scaling_step = 1;
    let max_scaling_step = 5;
    let context = Env::from_parts(
        MetaData::default(),
        GovernanceContext {
            min_parallelism,
            max_parallelism,
            min_cluster_size,
            max_cluster_size,
            min_scaling_step,
            max_scaling_step,
            custom: Default::default(),
        },
    );
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::from_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "too big a scale down step",
            Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(max_cluster_size.as_u32()),
                    target_job_parallelism: Parallelism::new(min_cluster_size.as_u32()),
                    current_nr_taskmanagers: max_cluster_size,
                    target_nr_taskmanagers: min_cluster_size,
                    task_slots_per_taskmanager: 1_f64,
                }
            ),
            vec![Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(max_cluster_size.as_u32()),
                    target_job_parallelism: Parallelism::new(
                        (max_cluster_size - max_scaling_step).as_u32()
                    ),
                    current_nr_taskmanagers: max_cluster_size,
                    target_nr_taskmanagers: max_cluster_size - max_scaling_step,
                    task_slots_per_taskmanager: 1_f64,
                }
            )]
        )
        .await
    );

    // todo step vs min/max precedent

    // todo test veto in subsequent test

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_step_up_before_max() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_step_up_before_max");
    let _ = main_span.enter();

    let governance_stage = GovernanceStage::new("flow_simple_step_up_before_max");

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_parallelism = Parallelism::new(1);
    let max_parallelism = Parallelism::new(20);
    let min_cluster_size = NrReplicas::new(2);
    let max_cluster_size = NrReplicas::new(10);
    let min_scaling_step = 1;
    let max_scaling_step = 5;
    let context = Env::from_parts(
        MetaData::default(),
        GovernanceContext {
            min_parallelism,
            max_parallelism,
            min_cluster_size,
            max_cluster_size,
            min_scaling_step,
            max_scaling_step,
            custom: Default::default(),
        },
    );
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::from_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "too big a scale up step before max",
            Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(0),
                    target_job_parallelism: Parallelism::new(999),
                    current_nr_taskmanagers: NrReplicas::new(0),
                    target_nr_taskmanagers: NrReplicas::new(999),
                    task_slots_per_taskmanager: 1_f64,
                }
            ),
            vec![Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(0),
                    target_job_parallelism: Parallelism::new(max_scaling_step),
                    current_nr_taskmanagers: NrReplicas::new(0),
                    target_nr_taskmanagers: NrReplicas::new(max_scaling_step),
                    task_slots_per_taskmanager: 1_f64,
                }
            )]
        )
        .await
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_governance_flow_simple_step_down_before_min() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_governance_flow_simple_step_down_before_min");
    let _ = main_span.enter();

    let governance_stage = GovernanceStage::new("flow_simple_step_down_before_min");

    let mut flow = TestFlow::new(governance_stage).await?;

    let min_parallelism = Parallelism::new(1);
    let max_parallelism = Parallelism::new(20);
    let min_cluster_size = NrReplicas::new(2);
    let max_cluster_size = NrReplicas::new(10);
    let min_scaling_step = 1;
    let max_scaling_step = 5;
    let context = Env::from_parts(
        MetaData::default(),
        GovernanceContext {
            min_parallelism,
            max_parallelism,
            min_cluster_size,
            max_cluster_size,
            min_scaling_step,
            max_scaling_step,
            custom: Default::default(),
        },
    );
    tracing::info!(?context, "pushing test context...");
    assert_ok!(flow.push_context(context).await);

    let event = &*assert_ok!(flow.recv_policy_event().await);
    tracing::info!(?event, "received policy event.");
    claim::assert_matches!(event, &elements::PolicyFilterEvent::ContextChanged(_));

    let timestamp = Timestamp::from_secs(*super::fixtures::DT_1_TS);
    assert_ok!(
        flow.check_scenario(
            "too big a scale down step before min",
            Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(max_cluster_size.as_u32()),
                    target_job_parallelism: Parallelism::new(0),
                    current_nr_taskmanagers: max_cluster_size,
                    target_nr_taskmanagers: NrReplicas::NONE,
                    task_slots_per_taskmanager: 1_f64,
                }
            ),
            vec![Env::from_parts(
                MetaData::from_parts(CORRELATION_ID.relabel(), timestamp),
                ScalePlan {
                    current_job_parallelism: Parallelism::new(max_cluster_size.as_u32()),
                    target_job_parallelism: Parallelism::new(
                        (max_cluster_size - max_scaling_step).as_u32()
                    ),
                    current_nr_taskmanagers: max_cluster_size,
                    target_nr_taskmanagers: max_cluster_size - max_scaling_step,
                    task_slots_per_taskmanager: 1_f64,
                }
            )]
        )
        .await
    );

    // todo step vs min/max precedent

    // todo test veto in subsequent test

    Ok(())
}
