use super::{FlinkScope, Unpack};
use crate::phases::collection::flink::TaskContext;
use crate::phases::MC_CLUSTER__NR_TASK_MANAGERS;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures_util::TryFutureExt;
use proctor::elements::telemetry;
use proctor::error::{CollectionError, ProctorError};
use proctor::graph::stage::{self, Stage};
use proctor::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use proctor::{AppData, ProctorResult, SharedString};
use reqwest::Method;
use std::collections::HashMap;
use std::fmt::Debug;
use tracing::Instrument;

/// Load telemetry for a specify scope from the Flink Job Manager REST API; e.g., Job or Taskmanager.
/// Note: cast_trait_object issues a conflicting impl error if no generic is specified (at least for
/// my use cases), so a simple Telemetry doesn't work and I need to parameterize even though
/// I'll only use wrt Telemetry.
#[derive(Debug)]
pub struct CollectTaskmanagerAdmin<Out>
where
    Out: Unpack,
{
    context: TaskContext,
    trigger: Inlet<()>,
    outlet: Outlet<Out>,
}

const NAME: &str = "collect_taskmanager_admin";

impl<Out> CollectTaskmanagerAdmin<Out>
where
    Out: Unpack,
{
    pub fn new(context: TaskContext) -> Self {
        let trigger = Inlet::new(NAME, "trigger");
        let outlet = Outlet::new(NAME, "outlet");
        Self { context, trigger, outlet }
    }
}

impl<Out> SourceShape for CollectTaskmanagerAdmin<Out>
where
    Out: Unpack,
{
    type Out = Out;
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<Out> SinkShape for CollectTaskmanagerAdmin<Out>
where
    Out: Unpack,
{
    type In = ();
    fn inlet(&self) -> Inlet<Self::In> {
        self.trigger.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<Out> Stage for CollectTaskmanagerAdmin<Out>
where
    Out: AppData + Unpack,
{
    fn name(&self) -> SharedString {
        NAME.into()
    }

    #[tracing::instrument(Level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await.map_err(|err| ProctorError::PhaseError(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run collect taskmanager admin stage", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

const SCOPE: FlinkScope = FlinkScope::TaskManagers;

impl<Out> CollectTaskmanagerAdmin<Out>
where
    Out: AppData + Unpack,
{
    async fn do_check(&self) -> Result<(), CollectionError> {
        self.trigger.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), CollectionError> {
        let mut url = self.context.base_url.clone();
        url.path_segments_mut().unwrap().push("taskmanagers");
        tracing::info!("url = {:?}", url);

        let url = url.clone();
        let name = self.name();
        // let outlet = self.outlet.clone();
        // let client = &self.context.client.clone();

        while self.trigger.recv().await.is_some() {
            let _stage_timer = stage::start_stage_eval_time(name.as_ref());

            let span = tracing::info_span!("collect Flink taskmanager admin telemetry");
            let collection_and_send = self
                .outlet
                .reserve_send::<_, CollectionError>(async {
                    // timer spans all retries
                    let _flink_timer = super::start_flink_collection_timer(&SCOPE);

                    let result: Result<Out, CollectionError> = self
                        .context
                        .client
                        .request(Method::GET, url.clone())
                        .send()
                        .and_then(|response| {
                            super::log_response("taskmanager admin response", &response);
                            response.json::<serde_json::Value>().map_err(|err| err.into())
                        })
                        .instrument(tracing::info_span!("Flink taskmanager REST API", scope=%SCOPE))
                        .await
                        .map_err(|err| err.into())
                        .map(|resp: serde_json::Value| {
                            resp["taskmanagers"].as_array().map(|tms| tms.len()).unwrap_or(0)
                        })
                        .and_then(|taskmanagers| {
                            let mut telemetry: telemetry::TableType = HashMap::default();
                            telemetry.insert(MC_CLUSTER__NR_TASK_MANAGERS.to_string(), taskmanagers.into());
                            Out::unpack(telemetry.into()).map_err(|err| err.into())
                        });

                    super::identity_and_track_errors(SCOPE, result)
                })
                .instrument(span)
                .await;

            let _ = super::identity_and_track_errors(SCOPE, collection_and_send);
        }

        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), CollectionError> {
        self.trigger.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::phases::collection::flink::STD_METRIC_ORDERS;
    // use crate::phases::collection::flink::{FLINK_COLLECTION_ERRORS, FLINK_COLLECTION_TIME};
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

    fn context_for(mock_server: &MockServer) -> anyhow::Result<TaskContext> {
        let client = reqwest::Client::builder().default_headers(HeaderMap::default()).build()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(2);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        let url = format!("{}/", &mock_server.uri());
        Ok(TaskContext { client, base_url: Url::parse(url.as_str())? })
    }

    async fn test_stage_for(
        context: TaskContext,
    ) -> (tokio::task::JoinHandle<()>, mpsc::Sender<()>, mpsc::Receiver<Telemetry>) {
        let mut stage = CollectTaskmanagerAdmin::new(context);
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
    fn test_flink_collect_taskmanager_admin() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_collect_taskmanager_admin");
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

            let source_handle = tokio::spawn(async move {
                assert_ok!(tx_trigger.send(()).await);
                assert_ok!(tx_trigger.send(()).await);
            });

            assert_ok!(source_handle.await);
            assert_ok!(handle.await);

            for _ in 0..2 {
                let actual: Telemetry = assert_some!(rx_out.recv().await);
                assert_eq!(
                    actual,
                    maplit::hashmap! { MC_CLUSTER__NR_TASK_MANAGERS.to_string() => 2.into(), }.into()
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
        //                     "error_type|collection::http_integration".into(),
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
