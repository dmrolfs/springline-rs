use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures_util::TryFutureExt;
use proctor::elements::telemetry;
use proctor::error::SenseError;
use proctor::graph::stage::{self, Stage};
use proctor::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use proctor::{AppData, ProctorResult};
use tracing::Instrument;

use super::{FlinkScope, Unpack};
use crate::flink::CorrelationGenerator;
use crate::flink::{MC_CLUSTER__FREE_TASK_SLOTS, MC_CLUSTER__NR_TASK_MANAGERS};
use crate::phases::plan::PLANNING__TOTAL_TASK_SLOTS;
use crate::phases::sense::flink::FlinkContext;

/// Load telemetry for a specify scope from the Flink Job Manager REST API; e.g., Job or
/// Taskmanager. Note: cast_trait_object issues a conflicting impl error if no generic is specified
/// (at least for my use cases), so a simple Telemetry doesn't work and I need to parameterize even
/// though I'll only use wrt Telemetry.
#[derive(Debug)]
pub struct TaskmanagerAdminSensor<Out> {
    context: FlinkContext,
    correlation_gen: CorrelationGenerator,
    trigger: Inlet<()>,
    outlet: Outlet<Out>,
}

const NAME: &str = "taskmanager_admin_sensor";

impl<Out> TaskmanagerAdminSensor<Out> {
    pub fn new(context: FlinkContext, correlation_gen: CorrelationGenerator) -> Self {
        let trigger = Inlet::new(NAME, "trigger");
        let outlet = Outlet::new(NAME, "outlet");
        Self { context, correlation_gen, trigger, outlet }
    }
}

impl<Out> SourceShape for TaskmanagerAdminSensor<Out> {
    type Out = Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<Out> SinkShape for TaskmanagerAdminSensor<Out> {
    type In = ();

    fn inlet(&self) -> Inlet<Self::In> {
        self.trigger.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<Out> Stage for TaskmanagerAdminSensor<Out>
where
    Out: AppData + Unpack,
{
    fn name(&self) -> &str {
        NAME
    }

    #[tracing::instrument(Level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        name = "run Flink taskmanager admin sensor stage",
        skip(self)
    )]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

const SCOPE: FlinkScope = FlinkScope::TaskManager;

impl<Out> TaskmanagerAdminSensor<Out>
where
    Out: AppData + Unpack,
{
    async fn do_check(&self) -> Result<(), SenseError> {
        self.trigger.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), SenseError> {
        let mut url = self.context.base_url();
        url.path_segments_mut().unwrap().push("taskmanagers");
        tracing::debug!("url = {:?}", url);

        while self.trigger.recv().await.is_some() {
            let _stage_timer = stage::start_stage_eval_time(self.name());

            let correlation = self.correlation_gen.next_id();
            let send_telemetry = self
                .outlet
                .reserve_send::<_, SenseError>(async {
                    // timer spans all retries
                    let _flink_timer = super::start_flink_sensor_timer(&SCOPE);
                    let result = self
                        .context
                        .query_taskmanagers(&correlation)
                        .map_err(|err| {
                            SenseError::Api("query_taskmanagers".to_string(), err.into())
                        })
                        .instrument(tracing::debug_span!(
                            "Flink REST API - taskmanager admin::taskmanagers"
                        ))
                        .await
                        .and_then(|tm_detail| {
                            let mut telemetry: telemetry::TableType = HashMap::default();

                            //todo: only publish task slots once?
                            telemetry.insert(
                                PLANNING__TOTAL_TASK_SLOTS.into(),
                                tm_detail.total_task_slots.into(),
                            );

                            telemetry.insert(
                                MC_CLUSTER__NR_TASK_MANAGERS.to_string(),
                                tm_detail.nr_taskmanagers.into(),
                            );

                            telemetry.insert(
                                MC_CLUSTER__FREE_TASK_SLOTS.into(),
                                tm_detail.free_task_slots.into(),
                            );

                            Out::unpack(telemetry.into()).map_err(|err| err.into())
                        });

                    super::identity_or_track_error(SCOPE, result).or_else(|_err| Ok(Out::default()))
                })
                .instrument(tracing::debug_span!(
                    "collect Flink taskmanager admin telemetry",
                    ?correlation
                ))
                .await;

            let _ = super::identity_or_track_error(SCOPE, send_telemetry);
        }

        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), SenseError> {
        self.trigger.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // use crate::phases::sense::flink::STD_METRIC_ORDERS;
    // use crate::phases::sense::flink::{FLINK_COLLECTION_ERRORS, FLINK_COLLECTION_TIME};
    use claim::*;
    // use fake::{Fake, Faker};
    // use inspect_prometheus::{self, Metric, MetricFamily, MetricLabel};
    use pretty_assertions::assert_eq;
    // use proctor::graph::stage::STAGE_EVAL_TIME;
    // use prometheus::Registry;
    use proctor::elements::Telemetry;
    use reqwest::header::HeaderMap;
    use reqwest_middleware::ClientBuilder;
    use reqwest_retry::policies::ExponentialBackoff;
    use reqwest_retry::RetryTransientMiddleware;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_test::block_on;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn context_for(mock_server: &MockServer) -> anyhow::Result<FlinkContext> {
        let client = reqwest::Client::builder().default_headers(HeaderMap::default()).build()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(2);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        let url = format!("{}/", &mock_server.uri());
        FlinkContext::new("test_flink", client, Url::parse(url.as_str())?).map_err(|err| err.into())
    }

    async fn test_stage_for(
        context: FlinkContext,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::Sender<()>,
        mpsc::Receiver<Telemetry>,
    ) {
        let mut stage = TaskmanagerAdminSensor::new(context, CorrelationGenerator::default());
        let (tx_trigger, rx_trigger) = mpsc::channel(1);
        let (tx_out, rx_out) = mpsc::channel(8);
        stage.trigger.attach("trigger".into(), rx_trigger).await;
        stage.outlet.attach("out".into(), tx_out).await;
        let handle = tokio::spawn(async move {
            assert_ok!(stage.run().await);
        });
        (handle, tx_trigger, rx_out)
    }

    fn make_taskmanager_admin_data() -> serde_json::Value {
        json!({
            "taskmanagers": [
                {
                    "id": "100.97.247.74:43435-69a783",
                    "path": "akka.tcp://flink@100.97.247.74:43435/user/rpc/taskmanager_0",
                    "dataPort": 42381,
                    "jmxPort": -1,
                    "timeSinceLastHeartbeat": 1638856220901_i64,
                    "slotsNumber": 1,
                    "freeSlots": 0,
                    "totalResource": {
                        "cpuCores": 1.0,
                        "taskHeapMemory": 3327,
                        "taskOffHeapMemory": 0,
                        "managedMemory": 2867,
                        "networkMemory": 716,
                        "extendedResources": {}
                    },
                    "freeResource": {
                        "cpuCores": 0.0,
                        "taskHeapMemory": 0,
                        "taskOffHeapMemory": 0,
                        "managedMemory": 0,
                        "networkMemory": 0,
                        "extendedResources": {}
                    },
                    "hardware": {
                        "cpuCores": 1,
                        "physicalMemory": 267929460736_i64,
                        "freeMemory": 3623878656_i64,
                        "managedMemory": 3006477152_i64
                    },
                    "memoryConfiguration": {
                        "frameworkHeap": 134217728_i64,
                        "taskHeap": 3489660872_i64,
                        "frameworkOffHeap": 134217728_i64,
                        "taskOffHeap": 0,
                        "networkMemory": 751619288_i64,
                        "managedMemory": 3006477152_i64,
                        "jvmMetaspace": 268435456_i64,
                        "jvmOverhead": 864958705_i64,
                        "totalFlinkMemory": 7516192768_i64,
                        "totalProcessMemory": 8649586929_i64
                    }
                },
                {
                    "id": "100.97.247.74:43435-69a798",
                    "path": "akka.tcp://flink@100.97.247.74:43435/user/rpc/taskmanager_1",
                    "dataPort": 42381,
                    "jmxPort": -1,
                    "timeSinceLastHeartbeat": 1638856220901_i64,
                    "slotsNumber": 1,
                    "freeSlots": 0,
                    "totalResource": {
                        "cpuCores": 1.0,
                        "taskHeapMemory": 3327,
                        "taskOffHeapMemory": 0,
                        "managedMemory": 2867,
                        "networkMemory": 716,
                        "extendedResources": {}
                    },
                    "freeResource": {
                        "cpuCores": 0.0,
                        "taskHeapMemory": 0,
                        "taskOffHeapMemory": 0,
                        "managedMemory": 0,
                        "networkMemory": 0,
                        "extendedResources": {}
                    },
                    "hardware": {
                        "cpuCores": 1,
                        "physicalMemory": 267929460736_i64,
                        "freeMemory": 3623878656_i64,
                        "managedMemory": 3006477152_i64
                    },
                    "memoryConfiguration": {
                        "frameworkHeap": 134217728_i64,
                        "taskHeap": 3489660872_i64,
                        "frameworkOffHeap": 134217728_i64,
                        "taskOffHeap": 0,
                        "networkMemory": 751619288_i64,
                        "managedMemory": 3006477152_i64,
                        "jvmMetaspace": 268435456_i64,
                        "jvmOverhead": 864958705_i64,
                        "totalFlinkMemory": 7516192768_i64,
                        "totalProcessMemory": 8649586929_i64
                    }
                }
            ]
        })
    }

    #[test]
    fn test_flink_taskmanager_admin_sensor() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_taskmanager_admin_sensor");
        let _ = main_span.enter();

        // let registry_name = "test_metrics";
        // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
        // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
        // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));
        // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));

        block_on(async {
            let mock_server = MockServer::start().await;

            let b = make_taskmanager_admin_data();
            let metric_response = ResponseTemplate::new(200).set_body_json(b);
            Mock::given(method("GET"))
                .and(path("/taskmanagers"))
                .respond_with(metric_response)
                .expect(2)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));
            let (handle, tx_trigger, mut rx_out) = test_stage_for(context).await;

            let sensor_handle = tokio::spawn(async move {
                assert_ok!(tx_trigger.send(()).await);
                assert_ok!(tx_trigger.send(()).await);
            });

            assert_ok!(sensor_handle.await);
            assert_ok!(handle.await);

            for _ in 0..2 {
                let actual: Telemetry = assert_some!(rx_out.recv().await);
                assert_eq!(
                    actual,
                    maplit::hashmap! {
                        PLANNING__TOTAL_TASK_SLOTS.to_string() => 2.into(),
                        MC_CLUSTER__NR_TASK_MANAGERS.to_string() => 2.into(),
                        MC_CLUSTER__FREE_TASK_SLOTS.to_string() => 0.into(),
                    }
                    .into()
                );
            }

            assert_none!(rx_out.recv().await);
        });

        // MetricFamily::distill_from(registry.gather())
        //     .into_iter()
        //     .for_each(|family| match family.name.as_str() {
        //         "test_metrics_flink_collection_errors" => {
        //             let actual = family
        //                 .metrics
        //                 .iter()
        //                 .flat_map(|m| m.labels())
        //                 .collect::<Vec<MetricLabel>>();
        //             assert_eq!(
        //                 actual,
        //                 vec![
        //                     "error_type|sense::http_integration".into(),
        //                     "flink_scope|Jobs".into(),
        //                 ]
        //             )
        //         },
        //         "test_metrics_flink_collection_time" => {
        //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
        //         },
        //         "test_metrics_stage_eval_time" => {
        //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
        //         },
        //         rep => assert_eq!(&format!("{:?}", family), rep),
        //     });
    }
}
