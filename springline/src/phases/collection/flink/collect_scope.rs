use super::FlinkScope;
use super::{api_model, Aggregation, MetricOrder, Unpack};
use crate::phases::collection::flink::TaskContext;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures_util::TryFutureExt;
use heck::ToSnakeCase;
use itertools::Itertools;
use proctor::error::{CollectionError, ProctorError};
use proctor::graph::stage::{self, Stage};
use proctor::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use proctor::{AppData, ProctorResult, SharedString};
use reqwest::Method;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::Instrument;
use url::Url;

/// Load telemetry for a specify scope from the Flink Job Manager REST API; e.g., Job or Taskmanager.
/// Note: cast_trait_object issues a conflicting impl error if no generic is specified (at least for
/// my use cases), so a simple Telemetry doesn't work and I need to parameterize even though
/// I'll only use wrt Telemetry.
#[derive(Debug)]
pub struct CollectScope<Out>
where
    Out: Unpack,
{
    scope: FlinkScope,
    context: TaskContext,
    orders: Arc<Vec<MetricOrder>>,
    trigger: Inlet<()>,
    outlet: Outlet<Out>,
}

impl<Out> CollectScope<Out>
where
    Out: Unpack,
{
    pub fn new(scope: FlinkScope, orders: Arc<Vec<MetricOrder>>, context: TaskContext) -> Self {
        let name: SharedString = format!("Collect{}", scope).to_snake_case().into();
        let trigger = Inlet::new(name.clone(), "trigger");
        let outlet = Outlet::new(name, "outlet");
        Self { scope, context, orders, trigger, outlet }
    }
}

impl<Out> SourceShape for CollectScope<Out>
where
    Out: Unpack,
{
    type Out = Out;
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<Out> SinkShape for CollectScope<Out>
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
impl<Out> Stage for CollectScope<Out>
where
    Out: AppData + Unpack,
{
    fn name(&self) -> SharedString {
        self.scope.to_string().into()
    }

    #[tracing::instrument(Level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await.map_err(|err| ProctorError::PhaseError(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run collect flink scope stage", skip(self))]
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

impl<Out> CollectScope<Out>
where
    Out: AppData + Unpack,
{
    async fn do_check(&self) -> Result<(), CollectionError> {
        self.trigger.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    fn extend_url_for<'a>(
        &self, metrics: impl Iterator<Item = &'a String>, agg: impl Iterator<Item = &'a Aggregation>,
    ) -> Url {
        let scope_rep = self.scope.to_string().to_lowercase();
        let mut url = self.context.base_url.clone();
        url.path_segments_mut().unwrap().push(&scope_rep).push("metrics");
        url.query_pairs_mut()
            .clear()
            .append_pair("get", metrics.cloned().join(",").as_str())
            .append_pair("agg", agg.copied().join(",").as_str());

        url
    }

    async fn do_run(&mut self) -> Result<(), CollectionError> {
        let scopes = maplit::hashset! { self.scope };
        let (metric_orders, agg_span) = super::distill_metric_orders_and_agg(&scopes, &self.orders);
        if metric_orders.is_empty() {
            //todo: best to end this useless stage or do nothing in loop? I hope end is best.
            tracing::warn!(
                stage=%self.name(), scope=%self.scope,
                "no flink metric orders to collect for scope - stopping scope collection stage."
            );
            return Ok(());
        }

        let scope_rep = SharedString::Owned(self.scope.to_string().to_lowercase());
        let metrics = metric_orders.keys();
        let url = self.extend_url_for(metrics, agg_span.iter());
        tracing::info!("url = {:?}", url);
        let name = self.name();

        while self.trigger.recv().await.is_some() {
            let _stage_timer = stage::start_stage_eval_time(name.as_ref());

            let span = tracing::info_span!("collect Flink scope telemetry", scope=%self.scope);
            let collection_and_send: Result<(), CollectionError> = self
                .outlet
                .reserve_send::<_, CollectionError>(async {
                    // timer spans all retries
                    let _flink_timer = super::start_flink_collection_timer(&self.scope);

                    let response: Result<Out, CollectionError> = self
                        .context
                        .client
                        .request(Method::GET, url.clone())
                        .send()
                        .and_then(|response| {
                            super::log_response(format!("{} scope response", scope_rep.clone()).as_str(), &response);
                            response.json::<api_model::FlinkMetricResponse>().map_err(|err| err.into())
                        })
                        .instrument(tracing::info_span!("Flink scope metrics REST API", scope=%self.scope))
                        .await
                        .map_err(|err| err.into())
                        .and_then(|metric_response: api_model::FlinkMetricResponse| {
                            api_model::build_telemetry(metric_response, &metric_orders)
                                // this is only needed because async_trait forcing me to parameterize this stage
                                .and_then(|telemetry| Out::unpack(telemetry))
                                .map_err(|err| err.into())
                        });

                    super::identity_and_track_errors(self.scope, response)
                })
                .instrument(span)
                .await;

            let _ = super::identity_and_track_errors(self.scope, collection_and_send);
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
    use crate::phases::collection::flink::STD_METRIC_ORDERS;
    // use crate::phases::collection::flink::{FLINK_COLLECTION_ERRORS, FLINK_COLLECTION_TIME};
    // use proctor::graph::stage::STAGE_EVAL_TIME;
    // use inspect_prometheus::{self, Metric, MetricFamily, MetricLabel};
    // use prometheus::Registry;
    use claim::*;
    use fake::{Fake, Faker};
    use pretty_assertions::assert_eq;
    use proctor::elements::Telemetry;
    use reqwest::header::HeaderMap;
    use reqwest_middleware::ClientBuilder;
    use reqwest_retry::policies::ExponentialBackoff;
    use reqwest_retry::RetryTransientMiddleware;
    use serde_json::json;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::sync::mpsc;
    use tokio_test::block_on;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    pub struct RetryResponder(Arc<AtomicU32>, u32, ResponseTemplate, u16);

    impl RetryResponder {
        fn new(retries: u32, fail_status_code: u16, success_template: ResponseTemplate) -> Self {
            Self(Arc::new(AtomicU32::new(0)), retries, success_template, fail_status_code)
        }
    }

    impl Respond for RetryResponder {
        #[tracing::instrument(level = "info", skip(self))]
        fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
            let mut attempts = self.0.load(Ordering::SeqCst);
            attempts += 1;
            self.0.store(attempts, Ordering::SeqCst);

            if self.1 < attempts {
                let result = self.2.clone();
                tracing::info!(?result, %attempts, retries=%(self.1), "enough attempts returning response");
                result
            } else {
                tracing::info!(%attempts, retries=%(self.1), "not enough attempts");
                ResponseTemplate::new(self.3)
            }
        }
    }

    fn context_for(mock_server: &MockServer) -> anyhow::Result<TaskContext> {
        let client = reqwest::Client::builder().default_headers(HeaderMap::default()).build()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(2);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        let url = format!("{}/", &mock_server.uri());
        // let url = "http://localhost:8081/".to_string();
        Ok(TaskContext { client, base_url: Url::parse(url.as_str())? })
    }

    async fn test_stage_for(
        scope: FlinkScope, orders: &Vec<MetricOrder>, context: TaskContext,
    ) -> (tokio::task::JoinHandle<()>, mpsc::Sender<()>, mpsc::Receiver<Telemetry>) {
        let mut stage = CollectScope::new(scope, Arc::new(orders.clone()), context);
        let (tx_trigger, rx_trigger) = mpsc::channel(1);
        let (tx_out, rx_out) = mpsc::channel(8);
        stage.trigger.attach("trigger".into(), rx_trigger).await;
        stage.outlet.attach("out".into(), tx_out).await;
        let handle = tokio::spawn(async move {
            assert_ok!(stage.run().await);
        });
        (handle, tx_trigger, rx_out)
    }

    #[test]
    fn test_flink_collect_scope_failure() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_collect_scope_failure");
        let _ = main_span.enter();

        // let registry_name = "test_metrics";
        // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
        // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
        // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));
        // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));

        block_on(async {
            let mock_server = MockServer::start().await;

            let metric_response = ResponseTemplate::new(500);
            Mock::given(method("GET"))
                .and(path("/jobs/metrics"))
                .respond_with(metric_response)
                .expect(6)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));
            let (handle, tx_trigger, mut rx_out) = test_stage_for(FlinkScope::Jobs, &STD_METRIC_ORDERS, context).await;

            let source_handle = tokio::spawn(async move {
                assert_ok!(tx_trigger.send(()).await);
                assert_ok!(tx_trigger.send(()).await);
            });

            assert_ok!(source_handle.await);
            assert_ok!(handle.await);
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

    fn make_jobs_data() -> (i64, i64, i64, serde_json::Value) {
        let uptime: i64 = Faker.fake::<chrono::Duration>().num_milliseconds();
        let restarts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64, _>(&mut positive.clone())as f64;
        let failed_checkpts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64(&mut positive) as f64;

        let body = json!([
            {
                "id": "uptime",
                "max": uptime as f64,
            },
            {
                "id": "numRestarts",
                "max": restarts as f64,
            },
            {
                "id": "numberOfFailedCheckpoints",
                "max": failed_checkpts,
            }
        ]);

        (uptime, restarts, failed_checkpts, body)
    }

    #[test]
    fn test_flink_collect_scope_simple_jobs() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_collect_scope_simple_jobs");
        let _ = main_span.enter();

        // let registry_name = "test_metrics";
        // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
        // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
        // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));
        // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));

        block_on(async {
            let mock_server = MockServer::start().await;

            let (uptime, restarts, failed_checkpts, b) = make_jobs_data();
            let metric_response = ResponseTemplate::new(200).set_body_json(b);
            Mock::given(method("GET"))
                .and(path("/jobs/metrics"))
                .respond_with(metric_response)
                .expect(2)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));
            let (handle, tx_trigger, mut rx_out) = test_stage_for(FlinkScope::Jobs, &STD_METRIC_ORDERS, context).await;

            let source_handle = tokio::spawn(async move {
                assert_ok!(tx_trigger.send(()).await);
                assert_ok!(tx_trigger.send(()).await);
            });

            assert_ok!(source_handle.await);
            assert_ok!(handle.await);

            for _ in 0..2 {
                let actual = assert_some!(rx_out.recv().await);
                assert_eq!(
                    actual,
                    maplit::hashmap! {
                        "health.job_uptime_millis".to_string() => uptime.into(),
                        "health.job_nr_restarts".to_string() => restarts.into(),
                        "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
                    }
                    .into()
                );
            }

            assert_none!(rx_out.recv().await);
        });

        // MetricFamily::distill_from(registry.gather())
        //     .into_iter()
        //     .for_each(|family| match family.name.as_str() {
        //         "test_metrics_flink_collection_time" => {
        //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
        //         },
        //         "test_metrics_stage_eval_time" => {
        //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
        //         },
        //         rep => assert_eq!(&format!("{:?}", family), rep),
        //     });
    }

    #[test]
    fn test_flink_collect_scope_jobs_retry() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_collect_scope_jobs_retry");
        let _ = main_span.enter();

        // let registry_name = "test_metrics";
        // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
        // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
        // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));
        // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));

        block_on(async {
            let mock_server = MockServer::start().await;

            let (uptime, restarts, failed_checkpts, b) = make_jobs_data();
            let metric_response = RetryResponder::new(1, 500, ResponseTemplate::new(200).set_body_json(b));
            Mock::given(method("GET"))
                .and(path("/jobs/metrics"))
                .respond_with(metric_response)
                .expect(3)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));
            let (handle, tx_trigger, mut rx_out) = test_stage_for(FlinkScope::Jobs, &STD_METRIC_ORDERS, context).await;

            let source_handle = tokio::spawn(async move {
                assert_ok!(tx_trigger.send(()).await);
                assert_ok!(tx_trigger.send(()).await);
            });

            assert_ok!(source_handle.await);
            assert_ok!(handle.await);

            for _ in 0..2 {
                let actual = assert_some!(rx_out.recv().await);
                assert_eq!(
                    actual,
                    maplit::hashmap! {
                        "health.job_uptime_millis".to_string() => uptime.into(),
                        "health.job_nr_restarts".to_string() => restarts.into(),
                        "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
                    }
                    .into()
                );
            }

            assert_none!(rx_out.recv().await);
        });

        // MetricFamily::distill_from(registry.gather())
        //     .into_iter()
        //     .for_each(|family| match family.name.as_str() {
        //         "test_metrics_flink_collection_time" => {
        //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
        //         },
        //         "test_metrics_stage_eval_time" => {
        //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
        //         },
        //         rep => assert_eq!(&format!("{:?}", family), rep),
        //     });
    }

    #[test]
    fn test_flink_collect_scope_taskmanagers() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_collect_scope_taskmanagers");
        let _ = main_span.enter();

        // let registry_name = "test_metrics";
        // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
        // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
        // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));
        // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));

        block_on(async {
            let mock_server = MockServer::start().await;

            let cpu_load: f64 = 16. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
            let heap_used: f64 = 1_000_000_000. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
            let heap_committed: f64 = 1_000_000_000. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
            let nr_threads: i32 = (1..150).fake();
            let b = json!([
                { "id": "Status.JVM.CPU.Load", "max": cpu_load, },
                { "id": "Status.JVM.Memory.Heap.Used", "max": heap_used, },
                { "id": "Status.JVM.Memory.Heap.Committed", "max": heap_committed, },
                { "id": "Status.JVM.Threads.Count", "max": nr_threads as f64, },
            ]);

            let expected: Telemetry = maplit::hashmap! {
                "cluster.task_cpu_load".to_string() => cpu_load.into(),
                "cluster.task_heap_memory_used".to_string() => heap_used.into(),
                "cluster.task_heap_memory_committed".to_string() => heap_committed.into(),
                "cluster.task_nr_threads".to_string() => nr_threads.into(),
            }
            .into();

            let metric_response = ResponseTemplate::new(200).set_body_json(b);
            Mock::given(method("GET"))
                .and(path("/taskmanagers/metrics"))
                .respond_with(metric_response)
                .expect(2)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));
            let (handle, tx_trigger, mut rx_out) =
                test_stage_for(FlinkScope::TaskManagers, &STD_METRIC_ORDERS, context).await;

            let source_handle = tokio::spawn(async move {
                assert_ok!(tx_trigger.send(()).await);
                assert_ok!(tx_trigger.send(()).await);
            });

            assert_ok!(source_handle.await);
            assert_ok!(handle.await);

            for _ in 0..2 {
                let actual = assert_some!(rx_out.recv().await);
                assert_eq!(actual, expected);
            }

            assert_none!(rx_out.recv().await);
        });
    }
}
