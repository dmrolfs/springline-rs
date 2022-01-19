use super::FlinkScope;
use super::{api_model, Aggregation, MetricOrder, Unpack};
use crate::phases::collection::flink::api_model::{FlinkMetricResponse, JobDetail, JobId, JobSummary, VertexId};
use crate::phases::collection::flink::{OrdersByMetric, TaskContext, JOB_SCOPE, TASK_SCOPE};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures_util::TryFutureExt;
use itertools::Itertools;
use once_cell::sync::Lazy;
use proctor::elements::Telemetry;
use proctor::error::{CollectionError, ProctorError};
use proctor::graph::stage::{self, Stage};
use proctor::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use proctor::{AppData, ProctorResult, SharedString};
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};
use reqwest::Method;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::Instrument;
use url::Url;

/// Load telemetry for a specify scope from the Flink Job Manager REST API; e.g., Job or Taskmanager.
/// Note: cast_trait_object issues a conflicting impl error if no generic is specified (at least for
/// my use cases), so a simple Telemetry doesn't work and I need to parameterize even though
/// I'll only use wrt Telemetry.
#[derive(Debug)]
pub struct CollectVertex<Out>
where
    Out: Unpack,
{
    scopes: Vec<FlinkScope>,
    context: TaskContext,
    orders: Arc<Vec<MetricOrder>>,
    trigger: Inlet<()>,
    outlet: Outlet<Out>,
    jobs_endpoint: Url,
}

const NAME: &str = "collect_vertex";

impl<Out> CollectVertex<Out>
where
    Out: Unpack,
{
    pub fn new(orders: Arc<Vec<MetricOrder>>, context: TaskContext) -> Result<Self, CollectionError> {
        let scopes = vec![FlinkScope::Task, FlinkScope::Kafka, FlinkScope::Kinesis];
        let trigger = Inlet::new(NAME, "trigger");
        let outlet = Outlet::new(NAME, "outlet");

        let mut jobs_endpoint = context.base_url.clone();
        jobs_endpoint
            .path_segments_mut()
            .map_err(|_| CollectionError::NotABaseUrl(context.base_url.clone()))?
            .push("jobs");

        Ok(Self {
            scopes,
            context,
            orders,
            trigger,
            outlet,
            jobs_endpoint,
        })
    }
}

impl<Out> SourceShape for CollectVertex<Out>
where
    Out: Unpack,
{
    type Out = Out;
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<Out> SinkShape for CollectVertex<Out>
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
impl<Out> Stage for CollectVertex<Out>
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

    #[tracing::instrument(level = "info", name = "run collect flink vertex stage", skip(self))]
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

impl<Out> CollectVertex<Out>
where
    Out: AppData + Unpack,
{
    async fn do_check(&self) -> Result<(), CollectionError> {
        self.trigger.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), CollectionError> {
        let scopes = self.scopes.iter().copied().collect();
        let (metric_orders, _agg_span) = super::distill_metric_orders_and_agg(&scopes, &self.orders);
        if metric_orders.is_empty() {
            //todo: best to end this useless stage or do nothing in loop? I hope end is best.
            tracing::warn!(
                stage=%self.name(), scopes=?self.scopes,
                "no flink metric orders to collect for vertex - stopping vertex collection stage."
            );
            return Ok(());
        }

        while self.trigger.recv().await.is_some() {
            let _stage_timer = stage::start_stage_eval_time(self.name().as_ref());

            let span = tracing::info_span!("collect Flink vertex telemetry");
            let collection_and_send: Result<(), CollectionError> = self
                .outlet
                .reserve_send(async {
                    let flink_span = tracing::info_span!("query Flink REST APIs");

                    self.query_active_jobs()
                        .and_then(|active_jobs| async {
                            let mut metric_points = HashMap::with_capacity(active_jobs.len());

                            for job in active_jobs {
                                let detail = match self.query_job_details(&job.id).await {
                                    Ok(job) => job,
                                    Err(_) => continue,
                                };

                                for vertex in detail.vertices.into_iter().filter(|v| v.status.is_active()) {
                                    let vertex_telemetry =
                                        self.query_vertex_telemetry(&job.id, &vertex.id, &metric_orders).await;
                                    let vertex_telemetry = match vertex_telemetry {
                                        Ok(telemetry) => telemetry,
                                        Err(_) => continue,
                                    };

                                    super::collect_metric_points(&mut metric_points, vertex_telemetry);
                                }
                            }

                            super::merge_telemetry_per_order(metric_points, &self.orders)
                                .and_then(Out::unpack)
                                .map_err(|err| err.into())
                        })
                        .instrument(flink_span)
                        .await
                })
                .instrument(span)
                .await;

            let _ = super::identity_and_track_errors(FlinkScope::Task, collection_and_send);
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn query_active_jobs(&self) -> Result<Vec<JobSummary>, CollectionError> {
        let _timer = start_flink_query_active_jobs_timer();
        let span = tracing::info_span!("query Flink active jobs");

        let result: Result<Vec<JobSummary>, CollectionError> = self
            .context
            .client
            .request(Method::GET, self.jobs_endpoint.clone())
            .send()
            .and_then(|response| {
                super::log_response("active jobs", &response);
                response.json().map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .map_err(|err: reqwest_middleware::Error| err.into())
            .and_then(|jobs_json_value: serde_json::Value| {
                jobs_json_value
                    .get("jobs")
                    .cloned()
                    .map(|json| serde_json::from_value::<Vec<JobSummary>>(json).map_err(|err| err.into()))
                    .unwrap_or_else(|| Ok(Vec::default()))
            })
            .map(|jobs: Vec<JobSummary>| jobs.into_iter().filter(|j| j.status.is_active()).collect());

        super::identity_and_track_errors(FlinkScope::Jobs, result)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn query_job_details(&self, job_id: &JobId) -> Result<JobDetail, CollectionError> {
        let mut url = self.jobs_endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| CollectionError::NotABaseUrl(self.jobs_endpoint.clone()))?
            .push(job_id.as_ref());

        let _timer = start_flink_query_job_detail_timer();
        let span = tracing::info_span!("query FLink job detail");

        let result: Result<JobDetail, CollectionError> = self
            .context
            .client
            .request(Method::GET, url)
            .send()
            .and_then(|response| {
                super::log_response("job detail", &response);
                response.json().map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .map_err(|err: reqwest_middleware::Error| err.into());

        super::identity_and_track_errors(FlinkScope::Jobs, result)
    }

    #[tracing::instrument(level = "info", skip(self, metric_orders))]
    async fn query_vertex_telemetry(
        &self, job_id: &JobId, vertex_id: &VertexId, metric_orders: &OrdersByMetric,
    ) -> Result<Telemetry, CollectionError> {
        let mut url = self.jobs_endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| CollectionError::NotABaseUrl(self.jobs_endpoint.clone()))?
            .push(job_id.as_ref())
            .push("vertices")
            .push(vertex_id.as_ref())
            .push("subtasks")
            .push("metrics");

        let _timer = start_flink_query_vertex_telemetry_timer();
        let span = tracing::info_span!("query Flink vertex telemetry");

        self.do_query_vertex_metric_picklist(url.clone(), metric_orders)
            .and_then(|picklist| {
                //todo used???: let vertex_agg_span = Self::agg_span_for(&picklist, metric_orders);
                //todo used???: tracing::info!(?picklist, &vertex_agg_span, "available vertex metrics identified for order");
                self.do_query_vertex_available_telemetry(picklist, metric_orders, url)
            })
            .instrument(span)
            .await
    }

    #[tracing::instrument(level = "info", skip(self, vertex_metrics_url, metric_orders))]
    async fn do_query_vertex_metric_picklist(
        &self, vertex_metrics_url: Url, metric_orders: &OrdersByMetric,
    ) -> Result<Vec<String>, CollectionError> {
        let _timer = start_flink_query_vertex_metric_picklist_time();
        let span = tracing::info_span!("query Flink vertex metric picklist");

        let picklist: Result<Vec<String>, CollectionError> = self
            .context
            .client
            .request(Method::GET, vertex_metrics_url)
            .send()
            .map_err(|err| err.into())
            .and_then(|response| {
                super::log_response("vertex metric picklet", &response);
                response.json::<FlinkMetricResponse>().map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .map(|picklist_response: FlinkMetricResponse| {
                picklist_response
                    .into_iter()
                    .filter_map(|metric| metric_orders.get(&metric.id).and(Some(metric.id)))
                    .collect()
            });

        super::identity_and_track_errors(FlinkScope::Task, picklist)
    }

    #[tracing::instrument(level = "info", skip(self, picklist, metric_orders, vertex_metrics_url))]
    async fn do_query_vertex_available_telemetry(
        &self, picklist: Vec<String>, metric_orders: &OrdersByMetric, mut vertex_metrics_url: Url,
    ) -> Result<Telemetry, CollectionError> {
        let agg_span = Self::agg_span_for(&picklist, metric_orders);
        tracing::info!(
            ?picklist,
            ?agg_span,
            "vertex metric picklist and aggregation span for metric order"
        );

        let telemetry: Result<Telemetry, CollectionError> = if !metric_orders.is_empty() {
            vertex_metrics_url
                .query_pairs_mut()
                .clear()
                .append_pair("get", picklist.into_iter().join(",").as_str());

            if !agg_span.is_empty() {
                vertex_metrics_url
                    .query_pairs_mut()
                    .append_pair("agg", agg_span.into_iter().join(",").as_str());
            }

            let _timer = start_flink_query_vertex_avail_telemetry_timer();
            let span = tracing::info_span!("query Flink vertex available telemetry");

            self.context
                .client
                .request(Method::GET, vertex_metrics_url)
                .send()
                .map_err(|err| err.into())
                .and_then(|vertex_metrics_response| {
                    super::log_response("job vertex available telemetry", &vertex_metrics_response);
                    vertex_metrics_response.json().map_err(|err| err.into())
                })
                .instrument(span)
                .await
                .and_then(|metric_response: FlinkMetricResponse| {
                    api_model::build_telemetry(metric_response, metric_orders).map_err(|err| err.into())
                })
        } else {
            Ok(Telemetry::default())
        };

        super::identity_and_track_errors(FlinkScope::Task, telemetry)
    }

    #[tracing::instrument(level = "info", skip(picklist, metric_orders))]
    fn agg_span_for(picklist: &[String], metric_orders: &OrdersByMetric) -> Vec<Aggregation> {
        picklist
            .iter()
            .flat_map(|metric| {
                metric_orders
                    .get(metric)
                    .cloned()
                    .unwrap_or_else(Vec::new)
                    .into_iter()
                    .map(|order| order.agg)
                    .filter(|agg| *agg != Aggregation::Value)
            })
            .collect()
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), CollectionError> {
        self.trigger.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

pub static FLINK_QUERY_ACTIVE_JOBS_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_query_active_jobs_time",
            "Time spent collecting active jobs from Flink in seconds",
        )
        .buckets(vec![0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_query_active_jobs_time metric")
});

#[inline]
fn start_flink_query_active_jobs_timer() -> HistogramTimer {
    FLINK_QUERY_ACTIVE_JOBS_TIME.with_label_values(&[JOB_SCOPE]).start_timer()
}

pub static FLINK_QUERY_JOB_DETAIL_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_query_job_detail_time",
            "Time spent collecting job detail from Flink in seconds",
        )
        .buckets(vec![0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_query_job_detail_time metric")
});

#[inline]
fn start_flink_query_job_detail_timer() -> HistogramTimer {
    FLINK_QUERY_JOB_DETAIL_TIME.with_label_values(&[JOB_SCOPE]).start_timer()
}

pub static FLINK_QUERY_VERTEX_TELEMETRY_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_query_vertex_telemetry_time",
            "Time spent collecting vertex telemetry from Flink in seconds",
        )
        .buckets(vec![0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_query_vertex_telemetry_time metric")
});

#[inline]
fn start_flink_query_vertex_telemetry_timer() -> HistogramTimer {
    FLINK_QUERY_VERTEX_TELEMETRY_TIME
        .with_label_values(&[TASK_SCOPE])
        .start_timer()
}

pub static FLINK_QUERY_VERTEX_METRIC_PICKLIST_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_query_vertex_metric_picklist_time",
            "Time spent getting the vertex metric picklist Flink in seconds",
        )
        .buckets(vec![0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_query_vertex_metric_picklist_time metric")
});

#[inline]
fn start_flink_query_vertex_metric_picklist_time() -> HistogramTimer {
    FLINK_QUERY_VERTEX_METRIC_PICKLIST_TIME
        .with_label_values(&[TASK_SCOPE])
        .start_timer()
}

pub static FLINK_QUERY_VERTEX_AVAIL_TELEMETRY_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_query_vertex_avail_telemetry_time",
            "Time spent collecting available vertex metrics from Flink in seconds",
        )
        .buckets(vec![0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_query_vertex_avail_telemetry_time metric")
});

#[inline]
fn start_flink_query_vertex_avail_telemetry_timer() -> HistogramTimer {
    FLINK_QUERY_VERTEX_AVAIL_TELEMETRY_TIME
        .with_label_values(&[JOB_SCOPE])
        .start_timer()
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
        let mut stage = assert_ok!(CollectVertex::new(Arc::new(orders.clone()), context));
        let (tx_trigger, rx_trigger) = mpsc::channel(1);
        let (tx_out, rx_out) = mpsc::channel(8);
        stage.trigger.attach("trigger".into(), rx_trigger).await;
        stage.outlet.attach("out".into(), tx_out).await;
        let handle = tokio::spawn(async move {
            assert_ok!(stage.run().await);
        });
        (handle, tx_trigger, rx_out)
    }

    // #[test]
    // fn test_flink_collect_scope_failure() {
    //     once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    //     let main_span = tracing::info_span!("test_flink_collect_scope_failure");
    //     let _ = main_span.enter();
    //
    //     // let registry_name = "test_metrics";
    //     // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
    //     // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));
    //
    //     block_on(async {
    //         let mock_server = MockServer::start().await;
    //
    //         let metric_response = ResponseTemplate::new(500);
    //         Mock::given(method("GET"))
    //             .and(path("/jobs/metrics"))
    //             .respond_with(metric_response)
    //             .expect(6)
    //             .mount(&mock_server)
    //             .await;
    //
    //         let context = assert_ok!(context_for(&mock_server));
    //         let (handle, tx_trigger, mut rx_out) = test_stage_for(FlinkScope::Jobs, &STD_METRIC_ORDERS, context).await;
    //
    //         let source_handle = tokio::spawn(async move {
    //             assert_ok!(tx_trigger.send(()).await);
    //             assert_ok!(tx_trigger.send(()).await);
    //         });
    //
    //         assert_ok!(source_handle.await);
    //         assert_ok!(handle.await);
    //         assert_none!(rx_out.recv().await);
    //     });
    //
    //     // MetricFamily::distill_from(registry.gather())
    //     //     .into_iter()
    //     //     .for_each(|family| match family.name.as_str() {
    //     //         "test_metrics_flink_collection_errors" => {
    //     //             let actual = family
    //     //                 .metrics
    //     //                 .iter()
    //     //                 .flat_map(|m| m.labels())
    //     //                 .collect::<Vec<MetricLabel>>();
    //     //             assert_eq!(
    //     //                 actual,
    //     //                 vec![
    //     //                     "error_type|collection::http_integration".into(),
    //     //                     "flink_scope|Jobs".into(),
    //     //                 ]
    //     //             )
    //     //         },
    //     //         "test_metrics_flink_collection_time" => {
    //     //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
    //     //         },
    //     //         "test_metrics_stage_eval_time" => {
    //     //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
    //     //         },
    //     //         rep => assert_eq!(&format!("{:?}", family), rep),
    //     //     });
    // }
    //
    // fn make_jobs_data() -> (i64, i64, i64, serde_json::Value) {
    //     let uptime: i64 = Faker.fake::<chrono::Duration>().num_milliseconds();
    //     let restarts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64, _>(&mut positive.clone())as f64;
    //     let failed_checkpts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64(&mut positive) as f64;
    //
    //     let body = json!([
    //         {
    //             "id": "uptime",
    //             "max": uptime as f64,
    //         },
    //         {
    //             "id": "numRestarts",
    //             "max": restarts as f64,
    //         },
    //         {
    //             "id": "numberOfFailedCheckpoints",
    //             "max": failed_checkpts,
    //         }
    //     ]);
    //
    //     (uptime, restarts, failed_checkpts, body)
    // }
    //
    // #[test]
    // fn test_flink_collect_scope_simple_jobs() {
    //     once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    //     let main_span = tracing::info_span!("test_flink_collect_scope_simple_jobs");
    //     let _ = main_span.enter();
    //
    //     // let registry_name = "test_metrics";
    //     // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
    //     // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));
    //
    //     block_on(async {
    //         let mock_server = MockServer::start().await;
    //
    //         let (uptime, restarts, failed_checkpts, b) = make_jobs_data();
    //         let metric_response = ResponseTemplate::new(200).set_body_json(b);
    //         Mock::given(method("GET"))
    //             .and(path("/jobs/metrics"))
    //             .respond_with(metric_response)
    //             .expect(2)
    //             .mount(&mock_server)
    //             .await;
    //
    //         let context = assert_ok!(context_for(&mock_server));
    //         let (handle, tx_trigger, mut rx_out) = test_stage_for(FlinkScope::Jobs, &STD_METRIC_ORDERS, context).await;
    //
    //         let source_handle = tokio::spawn(async move {
    //             assert_ok!(tx_trigger.send(()).await);
    //             assert_ok!(tx_trigger.send(()).await);
    //         });
    //
    //         assert_ok!(source_handle.await);
    //         assert_ok!(handle.await);
    //
    //         for _ in 0..2 {
    //             let actual = assert_some!(rx_out.recv().await);
    //             assert_eq!(
    //                 actual,
    //                 maplit::hashmap! {
    //                     "health.job_uptime_millis".to_string() => uptime.into(),
    //                     "health.job_nr_restarts".to_string() => restarts.into(),
    //                     "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
    //                 }
    //                 .into()
    //             );
    //         }
    //
    //         assert_none!(rx_out.recv().await);
    //     });
    //
    //     // MetricFamily::distill_from(registry.gather())
    //     //     .into_iter()
    //     //     .for_each(|family| match family.name.as_str() {
    //     //         "test_metrics_flink_collection_time" => {
    //     //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
    //     //         },
    //     //         "test_metrics_stage_eval_time" => {
    //     //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
    //     //         },
    //     //         rep => assert_eq!(&format!("{:?}", family), rep),
    //     //     });
    // }
    //
    // #[test]
    // fn test_flink_collect_scope_jobs_retry() {
    //     once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    //     let main_span = tracing::info_span!("test_flink_collect_scope_jobs_retry");
    //     let _ = main_span.enter();
    //
    //     // let registry_name = "test_metrics";
    //     // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
    //     // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));
    //
    //     block_on(async {
    //         let mock_server = MockServer::start().await;
    //
    //         let (uptime, restarts, failed_checkpts, b) = make_jobs_data();
    //         let metric_response = RetryResponder::new(1, 500, ResponseTemplate::new(200).set_body_json(b));
    //         Mock::given(method("GET"))
    //             .and(path("/jobs/metrics"))
    //             .respond_with(metric_response)
    //             .expect(3)
    //             .mount(&mock_server)
    //             .await;
    //
    //         let context = assert_ok!(context_for(&mock_server));
    //         let (handle, tx_trigger, mut rx_out) = test_stage_for(FlinkScope::Jobs, &STD_METRIC_ORDERS, context).await;
    //
    //         let source_handle = tokio::spawn(async move {
    //             assert_ok!(tx_trigger.send(()).await);
    //             assert_ok!(tx_trigger.send(()).await);
    //         });
    //
    //         assert_ok!(source_handle.await);
    //         assert_ok!(handle.await);
    //
    //         for _ in 0..2 {
    //             let actual = assert_some!(rx_out.recv().await);
    //             assert_eq!(
    //                 actual,
    //                 maplit::hashmap! {
    //                     "health.job_uptime_millis".to_string() => uptime.into(),
    //                     "health.job_nr_restarts".to_string() => restarts.into(),
    //                     "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
    //                 }
    //                 .into()
    //             );
    //         }
    //
    //         assert_none!(rx_out.recv().await);
    //     });
    //
    //     // MetricFamily::distill_from(registry.gather())
    //     //     .into_iter()
    //     //     .for_each(|family| match family.name.as_str() {
    //     //         "test_metrics_flink_collection_time" => {
    //     //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
    //     //         },
    //     //         "test_metrics_stage_eval_time" => {
    //     //             assert_eq!(family.metrics.iter().map(|m| m.count()).sum::<u64>(), 2);
    //     //         },
    //     //         rep => assert_eq!(&format!("{:?}", family), rep),
    //     //     });
    // }
    //
    // #[test]
    // fn test_flink_collect_scope_taskmanagers() {
    //     once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    //     let main_span = tracing::info_span!("test_flink_collect_scope_taskmanagers");
    //     let _ = main_span.enter();
    //
    //     // let registry_name = "test_metrics";
    //     // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()), None));
    //     // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));
    //
    //     block_on(async {
    //         let mock_server = MockServer::start().await;
    //
    //         let cpu_load: f64 = 16. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
    //         let heap_used: f64 = 1_000_000_000. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
    //         let heap_committed: f64 = 1_000_000_000. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
    //         let nr_threads: i32 = (1..150).fake();
    //         let b = json!([
    //             { "id": "Status.JVM.CPU.Load", "max": cpu_load, },
    //             { "id": "Status.JVM.Memory.Heap.Used", "max": heap_used, },
    //             { "id": "Status.JVM.Memory.Heap.Committed", "max": heap_committed, },
    //             { "id": "Status.JVM.Threads.Count", "max": nr_threads as f64, },
    //         ]);
    //
    //         let expected: Telemetry = maplit::hashmap! {
    //             "cluster.task_cpu_load".to_string() => cpu_load.into(),
    //             "cluster.task_heap_memory_used".to_string() => heap_used.into(),
    //             "cluster.task_heap_memory_committed".to_string() => heap_committed.into(),
    //             "cluster.task_nr_threads".to_string() => nr_threads.into(),
    //         }
    //         .into();
    //
    //         let metric_response = ResponseTemplate::new(200).set_body_json(b);
    //         Mock::given(method("GET"))
    //             .and(path("/taskmanagers/metrics"))
    //             .respond_with(metric_response)
    //             .expect(2)
    //             .mount(&mock_server)
    //             .await;
    //
    //         let context = assert_ok!(context_for(&mock_server));
    //         let (handle, tx_trigger, mut rx_out) =
    //             test_stage_for(FlinkScope::TaskManagers, &STD_METRIC_ORDERS, context).await;
    //
    //         let source_handle = tokio::spawn(async move {
    //             assert_ok!(tx_trigger.send(()).await);
    //             assert_ok!(tx_trigger.send(()).await);
    //         });
    //
    //         assert_ok!(source_handle.await);
    //         assert_ok!(handle.await);
    //
    //         for _ in 0..2 {
    //             let actual = assert_some!(rx_out.recv().await);
    //             assert_eq!(actual, expected);
    //         }
    //
    //         assert_none!(rx_out.recv().await);
    //     });
    // }
}
