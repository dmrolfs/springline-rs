use super::FlinkScope;
use super::{api_model, Aggregation, MetricOrder, Unpack};
use crate::flink::{self, JobDetail, JobId, JobSummary, VertexId};
use crate::phases::sense::flink::api_model::FlinkMetricResponse;
use crate::phases::sense::flink::{CorrelationGenerator, FlinkContext, OrdersByMetric, JOB_SCOPE, TASK_SCOPE};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures_util::TryFutureExt;
use itertools::Itertools;
use once_cell::sync::Lazy;
use proctor::elements::{Telemetry, TelemetryValue};
use proctor::error::SenseError;
use proctor::graph::stage::{self, Stage};
use proctor::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use proctor::{AppData, ProctorResult, SharedString};
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};
use reqwest::Method;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::Instrument;
use url::Url;
use crate::model::{CorrelationId, MC_CLUSTER__NR_ACTIVE_JOBS};

/// Load telemetry for a specify scope from the Flink Job Manager REST API; e.g., Job or Taskmanager.
/// Note: cast_trait_object issues a conflicting impl error if no generic is specified (at least for
/// my use cases), so a simple Telemetry doesn't work and I need to parameterize even though
/// I'll only use wrt Telemetry.
#[derive(Debug)]
pub struct VertexSensor<Out> {
    scopes: Vec<FlinkScope>,
    context: FlinkContext,
    orders: Arc<Vec<MetricOrder>>,
    correlation_gen: CorrelationGenerator,
    trigger: Inlet<()>,
    outlet: Outlet<Out>,
    // jobs_endpoint: Url,
}

const NAME: &str = "vertex_sensor";

impl<Out> VertexSensor<Out> {
    pub fn new(
        orders: Arc<Vec<MetricOrder>>, context: FlinkContext, correlation_gen: CorrelationGenerator,
    ) -> Result<Self, SenseError> {
        let scopes = vec![FlinkScope::Task, FlinkScope::Kafka, FlinkScope::Kinesis];
        let trigger = Inlet::new(NAME, "trigger");
        let outlet = Outlet::new(NAME, "outlet");

        // let mut jobs_endpoint = context.base_url();
        // jobs_endpoint
        //     .path_segments_mut()
        //     .map_err(|_| SenseError::NotABaseUrl(context.base_url()))?
        //     .push("jobs");

        Ok(Self {
            scopes,
            context,
            orders,
            correlation_gen,
            trigger,
            outlet,
            // jobs_endpoint,
        })
    }
}

impl<Out> SourceShape for VertexSensor<Out> {
    type Out = Out;
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<Out> SinkShape for VertexSensor<Out> {
    type In = ();
    fn inlet(&self) -> Inlet<Self::In> {
        self.trigger.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<Out> Stage for VertexSensor<Out>
where
    Out: AppData + Unpack,
{
    fn name(&self) -> SharedString {
        NAME.into()
    }

    #[tracing::instrument(Level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run flink vertex sensor stage", skip(self))]
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

impl<Out> VertexSensor<Out>
where
    Out: AppData + Unpack,
{
    async fn do_check(&self) -> Result<(), SenseError> {
        self.trigger.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    async fn do_run(&mut self) -> Result<(), SenseError> {
        let scopes = self.scopes.iter().copied().collect();
        let (metric_orders, _agg_span) = super::distill_metric_orders_and_agg(&scopes, &self.orders);
        if metric_orders.is_empty() {
            //todo: best to end this useless stage or do nothing in loop? I hope end is best.
            tracing::warn!(
                stage=%self.name(), scopes=?self.scopes,
                "no flink metric orders for vertex - stopping vertex sensesensor stage."
            );
            return Ok(());
        }

        while self.trigger.recv().await.is_some() {
            let _stage_timer = stage::start_stage_eval_time(self.name().as_ref());

            let correlation = self.correlation_gen.next_id();
            let span = tracing::info_span!("collect Flink vertex telemetry", %correlation);
            let send_telemetry: Result<(), SenseError> = self
                .outlet
                .reserve_send(async {
                    let flink_span = tracing::info_span!("query Flink REST APIs");

                    let out: Result<Out, SenseError> = self
                        .context
                        .query_active_jobs(&correlation)
                        .map_err(|err| SenseError::Api("query_active_jobs".to_string(), err.into()))
                        .and_then(|active_jobs| async {
                            let nr_active_jobs = active_jobs.len();
                            let metric_telemetry = Arc::new(Mutex::new(HashMap::with_capacity(nr_active_jobs + 1)));

                            let _vertex_gather_tasks: Vec<()> =
                                futures::future::join_all(active_jobs.into_iter().map(|job| async {
                                    self.gather_vertex_telemetry(
                                        job,
                                        metric_telemetry.clone(),
                                        &metric_orders,
                                        &correlation,
                                    )
                                    .await
                                }))
                                .await;
                            //todo: clean up once vertex_gather_tasks proves ok in real env
                            // for job in active_jobs {
                            // let detail = match self.query_job_details(&job.id).await {
                            //     Ok(job) => job,
                            //     Err(_) => continue,
                            // };
                            //
                            // for vertex in detail.vertices.into_iter().filter(|v| v.status.is_active()) {
                            //     let vertex_telemetry =
                            //         self.query_vertex_telemetry(&job.id, &vertex.id, &metric_orders).await;
                            //     let vertex_telemetry = match vertex_telemetry {
                            //         Ok(telemetry) => telemetry,
                            //         Err(_) => continue,
                            //     };
                            //
                            //     super::merge_into_metric_groups(&mut metric_telemetry, vertex_telemetry);
                            // }
                            // }

                            let groups = metric_telemetry.lock().await.drain().collect();
                            super::consolidate_active_job_telemetry_for_order(groups, &self.orders)
                                .map(|mut telemetry| {
                                    telemetry.insert(MC_CLUSTER__NR_ACTIVE_JOBS.to_string(), nr_active_jobs.into());
                                    telemetry
                                })
                                .and_then(Out::unpack)
                                .map_err(SenseError::Telemetry)
                        })
                        .instrument(flink_span)
                        .await;

                    super::identity_or_track_error(FlinkScope::Task, out).or_else(|_err| Ok(Out::default()))
                })
                .instrument(span)
                .await;

            let _ = super::identity_or_track_error(FlinkScope::Task, send_telemetry);
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, metric_telemetry, metric_orders))]
    async fn gather_vertex_telemetry(
        &self, job: JobSummary, metric_telemetry: Arc<Mutex<HashMap<String, Vec<TelemetryValue>>>>,
        metric_orders: &OrdersByMetric, correlation: &CorrelationId,
    ) {
        if let Ok(detail) = self.query_job_details(&job.id).await {
            for vertex in detail.vertices.into_iter().filter(|v| v.status.is_active()) {
                if let Ok(vertex_telemetry) = self
                    .query_vertex_telemetry(&job.id, &vertex.id, metric_orders, correlation)
                    .await
                {
                    let mut groups = metric_telemetry.lock().await;
                    super::merge_into_metric_groups(&mut *groups, vertex_telemetry);
                }
            }
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn query_job_details(&self, job_id: &JobId) -> Result<JobDetail, SenseError> {
        let mut url = self.context.jobs_endpoint();
        url.path_segments_mut()
            .map_err(|_| SenseError::NotABaseUrl(self.context.jobs_endpoint()))?
            .push(job_id.as_ref());

        let _timer = start_flink_query_job_detail_timer();
        let span = tracing::info_span!("query FLink job detail");

        let result: Result<JobDetail, SenseError> = self
            .context
            .client()
            .request(Method::GET, url)
            .send()
            .map_err(|error| {
                tracing::error!(?error, "failed Flink API job_detail response");
                error.into()
            })
            .and_then(|response| {
                flink::log_response("job detail", &response);
                response.text().map_err(|err| err.into())
            })
            .instrument(span)
            .await
            // .map_err(|err| err.into())
            .and_then(|body| {
                let result = serde_json::from_str(body.as_str()).map_err(|err| err.into());
                tracing::info!(%body, ?result, "Flink job detail response body");
                result
            });

        super::identity_or_track_error(FlinkScope::Jobs, result)
    }

    #[tracing::instrument(level = "info", skip(self, metric_orders))]
    async fn query_vertex_telemetry(
        &self, job_id: &JobId, vertex_id: &VertexId, metric_orders: &OrdersByMetric, correlation: &CorrelationId,
    ) -> Result<Telemetry, SenseError> {
        let mut url = self.context.jobs_endpoint();
        url.path_segments_mut()
            .map_err(|_| SenseError::NotABaseUrl(self.context.jobs_endpoint()))?
            .push(job_id.as_ref())
            .push("vertices")
            .push(vertex_id.as_ref())
            .push("subtasks")
            .push("metrics");

        let _timer = start_flink_vertex_sensor_timer();
        let span = tracing::info_span!("query Flink vertex telemetry", %correlation);

        self.do_query_vertex_metric_picklist(url.clone(), metric_orders, correlation)
            .and_then(|picklist| {
                //todo used???: let vertex_agg_span = Self::agg_span_for(&picklist, metric_orders);
                //todo used???: tracing::info!(?picklist, &vertex_agg_span, "available vertex metrics identified for order");
                self.do_query_vertex_available_telemetry(picklist, metric_orders, url, correlation)
            })
            .instrument(span)
            .await
    }

    #[tracing::instrument(level = "info", skip(self, vertex_metrics_url, metric_orders))]
    async fn do_query_vertex_metric_picklist(
        &self, vertex_metrics_url: Url, metric_orders: &OrdersByMetric, correlation: &CorrelationId,
    ) -> Result<Vec<String>, SenseError> {
        let _timer = start_flink_vertex_sensor_metric_picklist_time();
        let span = tracing::info_span!("query Flink vertex metric picklist", %correlation);

        let picklist: Result<Vec<String>, SenseError> = self
            .context
            .client()
            .request(Method::GET, vertex_metrics_url)
            .send()
            .map_err(|error| {
                tracing::error!(?error, "failed Flink API vertex_metrics response");
                error.into()
            })
            .and_then(|response| {
                flink::log_response("vertex metric picklet", &response);
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

        super::identity_or_track_error(FlinkScope::Task, picklist)
    }

    #[tracing::instrument(level = "info", skip(self, picklist, metric_orders, vertex_metrics_url))]
    async fn do_query_vertex_available_telemetry(
        &self, picklist: Vec<String>, metric_orders: &OrdersByMetric, mut vertex_metrics_url: Url,
        correlation: &CorrelationId,
    ) -> Result<Telemetry, SenseError> {
        let agg_span = Self::agg_span_for(&picklist, metric_orders);
        tracing::info!(
            ?picklist,
            ?agg_span,
            %correlation,
            "vertex metric picklist and aggregation span for metric order"
        );

        let telemetry: Result<Telemetry, SenseError> = if !metric_orders.is_empty() {
            vertex_metrics_url
                .query_pairs_mut()
                .clear()
                .append_pair("get", picklist.into_iter().join(",").as_str());

            if !agg_span.is_empty() {
                vertex_metrics_url
                    .query_pairs_mut()
                    .append_pair("agg", agg_span.into_iter().join(",").as_str());
            }

            let _timer = start_flink_vertex_sensor_avail_telemetry_timer();
            let span = tracing::info_span!("query Flink vertex available telemetry", %correlation);

            self.context
                .client()
                .request(Method::GET, vertex_metrics_url)
                .send()
                .map_err(|error| {
                    tracing::error!(?error, "failed Flink API job_vertex available_telemetry response");
                    error.into()
                })
                .and_then(|response| {
                    flink::log_response("job_vertex available telemetry", &response);
                    response.text().map_err(|err| err.into())
                })
                .instrument(span)
                .await
                // .map_err(|err| err.into())
                .and_then(|body| {
                    let result = serde_json::from_str(body.as_str()).map_err(|err| err.into());
                    tracing::info!(%body, ?result, "Flink vertex metrics response body");
                    result
                })
                .and_then(|metric_response: FlinkMetricResponse| {
                    api_model::build_telemetry(metric_response, metric_orders).map_err(|err| err.into())
                })
        } else {
            Ok(Telemetry::default())
        };

        super::identity_or_track_error(FlinkScope::Task, telemetry)
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

    async fn do_close(mut self: Box<Self>) -> Result<(), SenseError> {
        self.trigger.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

pub static FLINK_QUERY_JOB_DETAIL_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_query_job_detail_time",
            "Time spent collecting job detail from Flink in seconds",
        )
        .buckets(vec![0.2, 0.225, 0.25, 0.275, 0.3, 0.35, 0.4, 0.45, 0.5, 0.75, 1.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_query_job_detail_time metric")
});

#[inline]
fn start_flink_query_job_detail_timer() -> HistogramTimer {
    FLINK_QUERY_JOB_DETAIL_TIME.with_label_values(&[JOB_SCOPE]).start_timer()
}

pub static FLINK_VERTEX_SENSOR_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_vertex_sensor_time",
            "Time spent collecting vertex telemetry from Flink in seconds",
        )
        .buckets(vec![0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_vertex_sensor_time metric")
});

#[inline]
fn start_flink_vertex_sensor_timer() -> HistogramTimer {
    FLINK_VERTEX_SENSOR_TIME.with_label_values(&[TASK_SCOPE]).start_timer()
}

pub static FLINK_VERTEX_SENSOR_METRIC_PICKLIST_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_vertex_sensor_metric_picklist_time",
            "Time spent getting the vertex metric picklist Flink in seconds",
        )
        .buckets(vec![0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.75, 1.0, 2.5, 5.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_vertex_sensor_metric_picklist_time metric")
});

#[inline]
fn start_flink_vertex_sensor_metric_picklist_time() -> HistogramTimer {
    FLINK_VERTEX_SENSOR_METRIC_PICKLIST_TIME
        .with_label_values(&[TASK_SCOPE])
        .start_timer()
}

pub static FLINK_VERTEX_SENSOR_AVAIL_TELEMETRY_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_vertex_sensor_avail_telemetry_time",
            "Time spent collecting available vertex metrics from Flink in seconds",
        )
        .buckets(vec![0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.75, 1.0, 2.5, 5.0]),
        &["flink_scope"],
    )
    .expect("failed creating flink_vertex_sensor_avail_telemetry_time metric")
});

#[inline]
fn start_flink_vertex_sensor_avail_telemetry_timer() -> HistogramTimer {
    FLINK_VERTEX_SENSOR_AVAIL_TELEMETRY_TIME
        .with_label_values(&[JOB_SCOPE])
        .start_timer()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flink::{JobState, TaskState, VertexDetail};
    use crate::phases::sense::flink;
    use crate::phases::sense::flink::STD_METRIC_ORDERS;
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::elements::{Telemetry, TelemetryType, Timestamp};
    use reqwest::header::HeaderMap;
    use reqwest_middleware::ClientBuilder;
    use reqwest_retry::policies::ExponentialBackoff;
    use reqwest_retry::RetryTransientMiddleware;
    use serde_json::json;
    use std::borrow::Cow;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use pretty_snowflake::Id;
    use tokio::sync::mpsc;
    use tokio_test::block_on;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Match, Mock, MockServer, Request, Respond, ResponseTemplate};

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

    fn context_for(mock_server: &MockServer) -> anyhow::Result<FlinkContext> {
        let client = reqwest::Client::builder().default_headers(HeaderMap::default()).build()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(2);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        let url = format!("{}/", &mock_server.uri());
        // let url = "http://localhost:8081/".to_string();
        FlinkContext::new("test_flink", client, Url::parse(url.as_str())?).map_err(|err| err.into())
    }

    async fn test_stage_for(
        orders: &Vec<MetricOrder>, context: FlinkContext,
    ) -> (VertexSensor<Telemetry>, mpsc::Sender<()>, mpsc::Receiver<Telemetry>) {
        let mut stage = assert_ok!(VertexSensor::new(
            Arc::new(orders.clone()),
            context,
            CorrelationGenerator::default()
        ));
        let (tx_trigger, rx_trigger) = mpsc::channel(1);
        let (tx_out, rx_out) = mpsc::channel(8);
        stage.trigger.attach("trigger".into(), rx_trigger).await;
        stage.outlet.attach("out".into(), tx_out).await;
        (stage, tx_trigger, rx_out)
    }

    fn make_job_detail_body_and_expectation() -> (serde_json::Value, JobDetail) {
        let body = json!({
            "jid": "a97b6344d775aafe03e55a8e812d2713",
            "name": "CarTopSpeedWindowingExample",
            "isStoppable": false,
            "state": "RUNNING",
            "start-time": 1639156793312_i64,
            "end-time": -1,
            "duration": 23079,
            "maxParallelism": -1,
            "now": 1639156816391_i64,
            "timestamps": {
                "CREATED": 1639156793320_i64,
                "FAILED": 0,
                "RUNNING": 1639156794159_i64,
                "CANCELED": 0,
                "CANCELLING": 0,
                "SUSPENDED": 0,
                "FAILING": 0,
                "RESTARTING": 0,
                "FINISHED": 0,
                "INITIALIZING": 1639156793312_i64,
                "RECONCILING": 0
            },
            "vertices": [
                {
                    "id": "cbc357ccb763df2852fee8c4fc7d55f2",
                    "name": "Source: Custom Source -> Timestamps/Watermarks",
                    "maxParallelism": 128,
                    "parallelism": 1,
                    "status": "RUNNING",
                    "start-time": 1639156794188_i64,
                    "end-time": -1,
                    "duration": 22203,
                    "tasks": {
                        "SCHEDULED": 0,
                        "FINISHED": 0,
                        "FAILED": 0,
                        "CANCELING": 0,
                        "CANCELED": 0,
                        "DEPLOYING": 0,
                        "RECONCILING": 0,
                        "CREATED": 0,
                        "RUNNING": 1,
                        "INITIALIZING": 0
                    },
                    "metrics": {
                        "read-bytes": 0,
                        "read-bytes-complete": false,
                        "write-bytes": 0,
                        "write-bytes-complete": false,
                        "read-records": 0,
                        "read-records-complete": false,
                        "write-records": 0,
                        "write-records-complete": false
                    }
                },
                {
                    "id": "90bea66de1c231edf33913ecd54406c1",
                    "name": "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, PassThroughWindowFunction) -> Sink: Print to Std. Out",
                    "maxParallelism": 128,
                    "parallelism": 1,
                    "status": "RUNNING",
                    "start-time": 1639156794193_i64,
                    "end-time": -1,
                    "duration": 22198,
                    "tasks": {
                        "SCHEDULED": 0,
                        "FINISHED": 0,
                        "FAILED": 0,
                        "CANCELING": 0,
                        "CANCELED": 0,
                        "DEPLOYING": 0,
                        "RECONCILING": 0,
                        "CREATED": 0,
                        "RUNNING": 1,
                        "INITIALIZING": 0
                    },
                    "metrics": {
                        "read-bytes": 0,
                        "read-bytes-complete": false,
                        "write-bytes": 0,
                        "write-bytes-complete": false,
                        "read-records": 0,
                        "read-records-complete": false,
                        "write-records": 0,
                        "write-records-complete": false
                    }
                }
            ],
            "status-counts": {
                "SCHEDULED": 0,
                "FINISHED": 0,
                "FAILED": 0,
                "CANCELING": 0,
                "CANCELED": 0,
                "DEPLOYING": 0,
                "RECONCILING": 0,
                "CREATED": 0,
                "RUNNING": 2,
                "INITIALIZING": 0
            },
            "plan": {
                "jid": "a97b6344d775aafe03e55a8e812d2713",
                "name": "CarTopSpeedWindowingExample",
                "nodes": [
                    {
                        "id": "90bea66de1c231edf33913ecd54406c1",
                        "parallelism": 1,
                        "operator": "",
                        "operator_strategy": "",
                        "description": "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, PassThroughWindowFunction) -&gt; Sink: Print to Std. Out",
                        "inputs": [
                            {
                                "num": 0,
                                "id": "cbc357ccb763df2852fee8c4fc7d55f2",
                                "ship_strategy": "HASH",
                                "exchange": "pipelined_bounded"
                            }
                        ],
                        "optimizer_properties": {}
                    },
                    {
                        "id": "cbc357ccb763df2852fee8c4fc7d55f2",
                        "parallelism": 1,
                        "operator": "",
                        "operator_strategy": "",
                        "description": "Source: Custom Source -&gt; Timestamps/Watermarks",
                        "optimizer_properties": {}
                    }
                ]
            }
        });

        let expected = JobDetail {
            jid: JobId::new("a97b6344d775aafe03e55a8e812d2713"),
            name: "CarTopSpeedWindowingExample".to_string(),
            is_stoppable: false,
            state: JobState::Running,
            start_time: Timestamp::new(1639156793, 312_000_000),
            end_time: None,
            duration: Some(Duration::from_millis(23079)),
            max_parallelism: None,
            now: Timestamp::new(1639156816, 391_000_000),
            timestamps: maplit::hashmap! {
                JobState::Created => Timestamp::new(1639156793, 320_000_000),
                JobState::Failed => Timestamp::ZERO,
                JobState::Running => Timestamp::new(1639156794, 159_000_000),
                JobState::Canceled => Timestamp::ZERO,
                JobState::Cancelling => Timestamp::ZERO,
                JobState::Suspended => Timestamp::ZERO,
                JobState::Failing => Timestamp::ZERO,
                JobState::Restarting => Timestamp::ZERO,
                JobState::Finished => Timestamp::ZERO,
                JobState::Initializing => Timestamp::new(1639156793, 312_000_000),
                JobState::Reconciling => Timestamp::ZERO,
            },
            vertices: vec![
                VertexDetail {
                    id: VertexId::new("cbc357ccb763df2852fee8c4fc7d55f2"),
                    name: "Source: Custom Source -> Timestamps/Watermarks".to_string(),
                    max_parallelism: Some(128),
                    parallelism: 1,
                    status: TaskState::Running,
                    start_time: Timestamp::new(1639156794, 188_000_000),
                    end_time: None,
                    duration: Some(Duration::from_millis(22203)),
                    tasks: maplit::hashmap! {
                        TaskState::Scheduled => 0,
                        TaskState::Finished => 0,
                        TaskState::Failed => 0,
                        TaskState::Canceling => 0,
                        TaskState::Canceled => 0,
                        TaskState::Deploying => 0,
                        TaskState::Reconciling => 0,
                        TaskState::Created => 0,
                        TaskState::Running => 1,
                        TaskState::Initializing => 0,
                    },
                    metrics: maplit::hashmap! {
                        "read-bytes".to_string() => 0_i64.into(),
                        "read-bytes-complete".to_string() => false.into(),
                        "write-bytes".to_string() => 0_i64.into(),
                        "write-bytes-complete".to_string() => false.into(),
                        "read-records".to_string() => 0_i64.into(),
                        "read-records-complete".to_string() => false.into(),
                        "write-records".to_string() => 0_i64.into(),
                        "write-records-complete".to_string() => false.into(),
                    },
                },
                VertexDetail {
                    id: VertexId::new("90bea66de1c231edf33913ecd54406c1"),
                    name: "Window(GlobalWindows(), DeltaTrigger, TimeEvictor, ComparableAggregator, \
                                   PassThroughWindowFunction) -> Sink: Print to Std. Out"
                        .to_string(),
                    max_parallelism: Some(128),
                    parallelism: 1,
                    status: TaskState::Running,
                    start_time: Timestamp::new(1639156794, 193_000_000),
                    end_time: None,
                    duration: Some(Duration::from_millis(22198)),
                    tasks: maplit::hashmap! {
                        TaskState::Scheduled => 0,
                        TaskState::Finished => 0,
                        TaskState::Failed => 0,
                        TaskState::Canceling => 0,
                        TaskState::Canceled => 0,
                        TaskState::Deploying => 0,
                        TaskState::Reconciling => 0,
                        TaskState::Created => 0,
                        TaskState::Running => 1,
                        TaskState::Initializing => 0,
                    },
                    metrics: maplit::hashmap! {
                        "read-bytes".to_string() => 0_i64.into(),
                        "read-bytes-complete".to_string() => false.into(),
                        "write-bytes".to_string() => 0_i64.into(),
                        "write-bytes-complete".to_string() => false.into(),
                        "read-records".to_string() => 0_i64.into(),
                        "read-records-complete".to_string() => false.into(),
                        "write-records".to_string() => 0_i64.into(),
                        "write-records-complete".to_string() => false.into(),
                    },
                },
            ],
            status_counts: maplit::hashmap! {
                TaskState::Scheduled => 0,
                TaskState::Finished => 0,
                TaskState::Failed => 0,
                TaskState::Canceling => 0,
                TaskState::Canceled => 0,
                TaskState::Deploying => 0,
                TaskState::Reconciling => 0,
                TaskState::Created => 0,
                TaskState::Running => 2,
                TaskState::Initializing => 0,
            },
        };

        (body, expected)
    }

    #[test]
    fn test_query_job_details() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_query_job_details");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            let (b, expected) = make_job_detail_body_and_expectation();

            let response = ResponseTemplate::new(200).set_body_json(b);

            let job_id = "a97b6344d775aafe03e55a8e812d2713".into();

            Mock::given(method("GET"))
                .and(path(format!("/jobs/{job_id}")))
                .respond_with(response)
                .expect(1)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));
            let (stage, _, _) = test_stage_for(&STD_METRIC_ORDERS, context).await;

            let actual = assert_ok!(stage.query_job_details(&job_id).await);
            assert_eq!(actual, expected);
        })
    }

    struct EmptyQueryParamMatcher;
    impl Match for EmptyQueryParamMatcher {
        fn matches(&self, request: &Request) -> bool {
            request.url.query().is_none()
        }
    }

    struct QueryParamKeyMatcher(String);

    impl QueryParamKeyMatcher {
        pub fn new(key: impl Into<String>) -> Self {
            Self(key.into())
        }
    }

    impl Match for QueryParamKeyMatcher {
        fn matches(&self, request: &Request) -> bool {
            let query_keys: HashSet<Cow<'_, str>> = request.url.query_pairs().map(|(k, _)| k).collect();
            query_keys.contains(self.0.as_str())
        }
    }

    const METRIC_SUMMARY: Lazy<serde_json::Value> = Lazy::new(|| {
        json!([
                { "id": "Shuffle.Netty.Output.Buffers.outPoolUsage" },
                { "id": "checkpointStartDelayNanos" },
                { "id": "numBytesInLocal" },
                { "id": "numBytesInRemotePerSecond" },
                { "id": "Shuffle.Netty.Input.numBytesInRemotePerSecond" },
                { "id": "Source__Custom_Source.numRecordsInPerSecond" },
                { "id": "numBytesOut" },
                { "id": "Timestamps/Watermarks.currentInputWatermark" },
                { "id": "numBytesIn" },
                { "id": "Timestamps/Watermarks.numRecordsOutPerSecond" },
                { "id": "numBuffersOut" },
                { "id": "Shuffle.Netty.Input.numBuffersInLocal" },
                { "id": "numBuffersInRemotePerSecond" },
                { "id": "numBytesOutPerSecond" },
                { "id": "Timestamps/Watermarks.numRecordsOut" },
                { "id": "buffers.outputQueueLength" },
                { "id": "Timestamps/Watermarks.numRecordsIn" },
                { "id": "numBuffersOutPerSecond" },
                { "id": "Shuffle.Netty.Input.Buffers.inputExclusiveBuffersUsage" },
                { "id": "isBackPressured" },
                { "id": "numBytesInLocalPerSecond" },
                { "id": "buffers.inPoolUsage" },
                { "id": "idleTimeMsPerSecond" },
                { "id": "Shuffle.Netty.Input.numBytesInLocalPerSecond" },
                { "id": "numBytesInRemote" },
                { "id": "Source__Custom_Source.numRecordsOut" },
                { "id": "Shuffle.Netty.Input.numBytesInLocal" },
                { "id": "Shuffle.Netty.Input.numBytesInRemote" },
                { "id": "busyTimeMsPerSecond" },
                { "id": "Shuffle.Netty.Output.Buffers.outputQueueLength" },
                { "id": "buffers.inputFloatingBuffersUsage" },
                { "id": "Shuffle.Netty.Input.Buffers.inPoolUsage" },
                { "id": "numBuffersInLocalPerSecond" },
                { "id": "numRecordsOut" },
                { "id": "numBuffersInLocal" },
                { "id": "Timestamps/Watermarks.currentOutputWatermark" },
                { "id": "Source__Custom_Source.currentOutputWatermark" },
                { "id": "numBuffersInRemote" },
                { "id": "buffers.inputQueueLength" },
                { "id": "Source__Custom_Source.numRecordsOutPerSecond" },
                { "id": "Timestamps/Watermarks.numRecordsInPerSecond" },
                { "id": "numRecordsIn" },
                { "id": "Shuffle.Netty.Input.numBuffersInRemote" },
                { "id": "numBytesInPerSecond" },
                { "id": "backPressuredTimeMsPerSecond" },
                { "id": "Shuffle.Netty.Input.Buffers.inputQueueLength" },
                { "id": "Source__Custom_Source.numRecordsIn" },
                { "id": "buffers.inputExclusiveBuffersUsage" },
                { "id": "Shuffle.Netty.Input.numBuffersInRemotePerSecond" },
                { "id": "numRecordsOutPerSecond" },
                { "id": "buffers.outPoolUsage" },
                { "id": "Shuffle.Netty.Input.numBuffersInLocalPerSecond" },
                { "id": "numRecordsInPerSecond" },
                { "id": "Shuffle.Netty.Input.Buffers.inputFloatingBuffersUsage" },
            ] )
    });

    #[test]
    fn test_query_vertex_telemetry() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_query_vertex_telemetry");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            let summary_response = ResponseTemplate::new(200).set_body_json(METRIC_SUMMARY.clone());

            let metrics = json!([
                { "id": "numRecordsInPerSecond", "max": 0.0 },
                { "id": "numRecordsOutPerSecond", "max": 20.0 },
                { "id": "buffers.inputQueueLength", "max": 0.0 },
                { "id": "buffers.inPoolUsage", "max": 0.0 },
                { "id": "buffers.outputQueueLength", "max": 1.0 },
                { "id": "buffers.outPoolUsage", "max": 0.1 },
            ] );

            let metrics_response = ResponseTemplate::new(200).set_body_json(metrics);

            let job_id = "f3f10c679805d35fbed73a08c37d03cc".into();
            let vertex_id = "cbc357ccb763df2852fee8c4fc7d55f2".into();

            let query_path = format!("/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics");

            Mock::given(method("GET"))
                .and(path(query_path.clone()))
                .and(EmptyQueryParamMatcher)
                .respond_with(summary_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            Mock::given(method("GET"))
                .and(path(query_path))
                .and(QueryParamKeyMatcher::new("get"))
                .respond_with(metrics_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));
            let (stage, _, _) = test_stage_for(&STD_METRIC_ORDERS, context).await;

            let scopes = maplit::hashset! {
                FlinkScope::Task, FlinkScope::Kafka, FlinkScope::Kinesis,
            };

            let mut orders = STD_METRIC_ORDERS.clone();
            let kafka_order = MetricOrder {
                scope: FlinkScope::Kafka,
                metric: "records-lag-max".to_string(),
                agg: Aggregation::Value,
                telemetry_path: "flow.input_records_lag_max".to_string(),
                telemetry_type: TelemetryType::Integer,
            };
            orders.extend(vec![kafka_order.clone()]);

            let (metric_orders, agg_span) = flink::distill_metric_orders_and_agg(&scopes, &orders);
            tracing::info!(?metric_orders, ?agg_span, "orders distilled");

            let actual = assert_ok!(
                stage
                    .query_vertex_telemetry(
                        &job_id,
                        &vertex_id,
                        &metric_orders,
                        &Id::direct("test_query_vertex_telemetry", 23, "CBA")
                    )
                    .await
            );

            assert_eq!(
                actual,
                maplit::hashmap! {
                    crate::phases::metric_catalog::MC_FLOW__RECORDS_IN_PER_SEC.to_string() => 0_f64.into(),
                    "flow.records_out_per_sec".to_string() => 20_f64.into(),
                    "cluster.task_network_input_queue_len".to_string() => 0_f64.into(),
                    "cluster.task_network_input_pool_usage".to_string() => 0_f64.into(),
                    "cluster.task_network_output_queue_len".to_string() => 1_f64.into(),
                    "cluster.task_network_output_pool_usage".to_string() => 0.1_f64.into(),
                }
                .into()
            )
        })
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
    //     //                     "error_type|sense::http_integration".into(),
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
