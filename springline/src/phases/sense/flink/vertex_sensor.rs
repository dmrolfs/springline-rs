use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures_util::{FutureExt, TryFutureExt};
use itertools::Itertools;
use once_cell::sync::Lazy;
use proctor::elements::{Telemetry, TelemetryValue};
use proctor::error::SenseError;
use proctor::graph::stage::{self, Stage};
use proctor::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use proctor::{AppData, ProctorResult};
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec};
use reqwest::Method;
use tokio::sync::Mutex;
use tracing::Instrument;
use url::Url;

use super::FlinkScope;
use super::{api_model, Aggregation, MetricOrder, Unpack};
use crate::flink::{self, JobId, JobSummary, VertexId, MC_CLUSTER__NR_ACTIVE_JOBS};
use crate::phases::sense::flink::api_model::FlinkMetricResponse;
use crate::phases::sense::flink::metric_order::MetricOrderMatcher;
use crate::phases::sense::flink::{CorrelationGenerator, FlinkContext, JOB_SCOPE, TASK_SCOPE};
use crate::CorrelationId;

/// Load telemetry for a specify scope from the Flink Job Manager REST API; e.g., Job or
/// Taskmanager. Note: cast_trait_object issues a conflicting impl error if no generic is specified
/// (at least for my use cases), so a simple Telemetry doesn't work and I need to parameterize even
/// though I'll only use wrt Telemetry.
pub struct VertexSensor<Out> {
    context: FlinkContext,
    scopes: Vec<FlinkScope>,
    orders: Vec<MetricOrder>,
    order_matchers: HashMap<MetricOrder, MetricOrderMatcher>,
    correlation_gen: CorrelationGenerator,
    trigger: Inlet<()>,
    outlet: Outlet<Out>,
}

impl<Out> fmt::Debug for VertexSensor<Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VertexSensor")
            .field("context", &self.context)
            .field("scopes", &self.scopes)
            .field("orders", &self.orders)
            .field("trigger", &self.trigger)
            .field("outlet", &self.outlet)
            .finish()
    }
}

const NAME: &str = "vertex_sensor";

impl<Out> VertexSensor<Out> {
    pub fn new(
        orders: &[MetricOrder], context: FlinkContext, correlation_gen: CorrelationGenerator,
    ) -> Result<Self, SenseError> {
        let scopes = vec![FlinkScope::Task, FlinkScope::Operator];
        let trigger = Inlet::new(NAME, "trigger");
        let outlet = Outlet::new(NAME, "outlet");

        let mut my_orders = Vec::new();
        let mut order_matchers = HashMap::new();
        for order in orders {
            let found = scopes.iter().any(|s| *s == order.scope());
            if found {
                my_orders.push(order.clone());
                let matcher = order.matcher()?;
                order_matchers.insert(order.clone(), matcher);
            }
        }

        // orders.iter().filter(|o| scopes.iter().find(|s| *s == &o.scope()).is_some()).cloned().collect();
        Ok(Self {
            context,
            scopes,
            orders: my_orders,
            order_matchers,
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
    fn name(&self) -> &str {
        NAME
    }

    #[tracing::instrument(Level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run flink vertex sensor stage", skip(self))]
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
        if self.orders.is_empty() {
            tracing::info!(
                stage=%self.name(), scopes=?self.scopes,
                "no flink metric orders for vertex - stopping vertex sensor stage."
            );
            return Ok(());
        }

        while self.trigger.recv().await.is_some() {
            let _stage_timer = stage::start_stage_eval_time(self.name());

            let correlation = self.correlation_gen.next_id();
            let send_telemetry: Result<(), SenseError> = self
                .outlet
                .reserve_send(async {
                    let out: Result<Out, SenseError> = self
                        .context
                        .query_active_jobs(&correlation)
                        .map_err(|err| SenseError::Api("query_active_jobs".to_string(), err.into()))
                        .and_then(|active_jobs| async {
                            let nr_active_jobs = active_jobs.len();
                            let metric_telemetry = Arc::new(Mutex::new(HashMap::with_capacity(nr_active_jobs + 1)));

                            let _vertex_gather_tasks: Vec<()> =
                                futures::future::join_all(active_jobs.into_iter().map(|job| async {
                                    self.gather_vertex_telemetry(job, metric_telemetry.clone(), &correlation)
                                        .await
                                }))
                                .await;

                            let groups = metric_telemetry.lock().await.drain().collect();
                            super::consolidate_active_job_telemetry_for_order(groups, &self.orders)
                                .map(|mut telemetry| {
                                    telemetry.insert(MC_CLUSTER__NR_ACTIVE_JOBS.to_string(), nr_active_jobs.into());
                                    telemetry
                                })
                                .and_then(Out::unpack)
                                .map_err(SenseError::Telemetry)
                        })
                        .instrument(tracing::debug_span!("Flink REST API - vertex metrics"))
                        .await;

                    super::identity_or_track_error(FlinkScope::Task, out).or_else(|_err| Ok(Out::default()))
                })
                .instrument(tracing::debug_span!("collect Flink vertex telemetry", ?correlation))
                .await;

            let _ = super::identity_or_track_error(FlinkScope::Task, send_telemetry);
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, metric_telemetry))]
    async fn gather_vertex_telemetry(
        &self, job: JobSummary, metric_telemetry: Arc<Mutex<HashMap<String, Vec<TelemetryValue>>>>,
        correlation: &CorrelationId,
    ) {
        match self.context.query_job_details(&job.id, correlation).await {
            Ok(detail) => {
                for vertex in detail.vertices.into_iter().filter(|v| v.status.is_active()) {
                    if let Ok(vertex_telemetry) = self.query_vertex_telemetry(&job.id, &vertex.id, correlation).await {
                        let mut groups = metric_telemetry.lock().await;
                        super::merge_into_metric_groups(&mut *groups, vertex_telemetry);
                    }
                }
            },
            Err(err) => {
                tracing::warn!(error=?err, ?correlation, "failed to query Flink job details");
            },
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn query_vertex_telemetry(
        &self, job_id: &JobId, vertex_id: &VertexId, correlation: &CorrelationId,
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
        let span = tracing::debug_span!("query Flink vertex telemetry", ?correlation, ?job_id, ?vertex_id);

        self.do_query_vertex_metric_picklist(url.clone(), correlation)
            .and_then(|picklist| {
                // todo used???: let vertex_agg_span = Self::agg_span_for(&picklist, metric_orders);
                // todo used???: tracing::info!(?picklist, &vertex_agg_span, "available vertex metrics identified
                // for order");
                self.do_query_vertex_available_telemetry(picklist, url, correlation)
            })
            .instrument(span)
            .await
    }

    #[tracing::instrument(level = "trace", skip(self, vertex_metrics_url))]
    async fn do_query_vertex_metric_picklist(
        &self, vertex_metrics_url: Url, correlation: &CorrelationId,
    ) -> Result<Vec<String>, SenseError> {
        let _timer = start_flink_vertex_sensor_metric_picklist_time();
        let span = tracing::debug_span!("query Flink vertex metric picklist", ?correlation, ?vertex_metrics_url);

        let picklist: Result<Vec<String>, SenseError> = self
            .context
            .client()
            .request(Method::GET, vertex_metrics_url.clone())
            .send()
            .map_err(|error| {
                tracing::error!(?error, "failed Flink API vertex_metrics response");
                error.into()
            })
            .and_then(|response| {
                flink::log_response("vertex metric picklist", &vertex_metrics_url, &response);
                response.json::<FlinkMetricResponse>().map_err(|err| err.into())
            })
            .instrument(span)
            .await
            .map(|picklist_response: FlinkMetricResponse| {
                picklist_response
                    .into_iter()
                    .filter_map(|metric| {
                        self.order_matchers.iter().find_map(|(o, matches)| {
                            if matches(&metric.id) {
                                Some(metric.id.clone())
                            } else {
                                None
                            }
                        })
                    })
                    .collect()
            });

        tracing::debug!("job_id+vertex_id available metric order picklist are: {picklist:?}");
        super::identity_or_track_error(FlinkScope::Task, picklist)
    }

    #[tracing::instrument(level = "trace", skip(self, picklist, vertex_metrics_url))]
    async fn do_query_vertex_available_telemetry(
        &self, picklist: Vec<String>, mut vertex_metrics_url: Url, correlation: &CorrelationId,
    ) -> Result<Telemetry, SenseError> {
        let agg_span = Self::agg_span_for(&picklist, &self.orders);

        tracing::debug!(
            ?picklist,
            ?agg_span,
            ?correlation,
            "vertex metric picklist and aggregation span for metric order"
        );

        let telemetry: Result<Telemetry, SenseError> = if !self.orders.is_empty() {
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

            self.context
                .client()
                .request(Method::GET, vertex_metrics_url.clone())
                .send()
                .map_err(|error| error.into())
                .and_then(|response| {
                    flink::log_response("job_vertex available telemetry", &vertex_metrics_url, &response);
                    response.text().map(|body| {
                        body.map_err(|err| err.into()).and_then(|b| {
                            let result = serde_json::from_str(&b).map_err(|err| err.into());
                            tracing::debug!(body=%b, response=?result, "Flink vertex metrics response body");
                            result
                        })
                    })
                })
                .instrument(tracing::trace_span!(
                    "query Flink REST API - vertex available telemetry",
                    ?correlation
                ))
                .await
                .and_then(|metric_response: FlinkMetricResponse| {
                    api_model::build_telemetry(metric_response, &self.order_matchers).map_err(|err| err.into())
                })
        } else {
            Ok(Telemetry::default())
        };

        super::identity_or_track_error(FlinkScope::Task, telemetry)
    }

    #[tracing::instrument(level = "trace", skip(picklist, metric_orders))]
    fn agg_span_for(picklist: &[String], metric_orders: &[MetricOrder]) -> Vec<Aggregation> {
        picklist
            .iter()
            .flat_map(|metric| {
                metric_orders.iter().filter_map(|o| {
                    if o.agg() != Aggregation::Value && o.metric() == metric.as_str() {
                        Some(o.agg())
                    } else {
                        None
                    }
                })

                // metric_orders
                //     .get(metric)
                //     .cloned()
                //     .unwrap_or_default()
                //     .into_iter()
                //     .map(|order| order.agg())
                //     .filter(|agg| *agg != Aggregation::Value)
            })
            .collect()
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), SenseError> {
        self.trigger.close().await;
        self.outlet.close().await;
        Ok(())
    }
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
    use std::borrow::Cow;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU32, Ordering};

    use claim::*;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::Id;
    use proctor::elements::{Telemetry, TelemetryType};
    use reqwest::header::HeaderMap;
    use reqwest_middleware::ClientBuilder;
    use reqwest_retry::policies::ExponentialBackoff;
    use reqwest_retry::RetryTransientMiddleware;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_test::block_on;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Match, Mock, MockServer, Request, Respond, ResponseTemplate};

    use super::*;
    use crate::flink::MC_FLOW__RECORDS_IN_PER_SEC;
    use crate::phases::sense::flink::metric_order::{MetricSpec, ScopeSpec};
    use crate::phases::sense::flink::STD_METRIC_ORDERS;

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
        orders: &[MetricOrder], context: FlinkContext,
    ) -> (VertexSensor<Telemetry>, mpsc::Sender<()>, mpsc::Receiver<Telemetry>) {
        let correlation_gen = CorrelationGenerator::default();
        let mut stage = assert_ok!(VertexSensor::new(orders, context, correlation_gen));
        let (tx_trigger, rx_trigger) = mpsc::channel(1);
        let (tx_out, rx_out) = mpsc::channel(8);
        stage.trigger.attach("trigger".into(), rx_trigger).await;
        stage.outlet.attach("out".into(), tx_out).await;
        (stage, tx_trigger, rx_out)
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
                { "id": "Source__Foo_Data_stream.KafkaConsumer.client-id.fce7d7241a2648da85a99be91e4f2b77.catalog.ingestion-service.layer.observations.consumer-fetch-manager-metrics_records-lag-max" },
                { "id": "Shuffle.Netty.Output.Buffers.outPoolUsage" },
                { "id": "checkpointStartDelayNanos" },
                { "id": "numBytesInLocal" },
                { "id": "numBytesInRemotePerSecond" },
                { "id": "Shuffle.Netty.Input.numBytesInRemotePerSecond" },
                { "id": "Source__Foo_Data_stream.numRecordsInPerSecond" },
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
                { "id": "Source__Foo_Data_stream.numRecordsOutPerSecond" },
                { "id": "Timestamps/Watermarks.numRecordsInPerSecond" },
                { "id": "numRecordsIn" },
                { "id": "Shuffle.Netty.Input.numBuffersInRemote" },
                { "id": "numBytesInPerSecond" },
                { "id": "backPressuredTimeMsPerSecond" },
                { "id": "Shuffle.Netty.Input.Buffers.inputQueueLength" },
                { "id": "Source__Foo_Data_stream.numRecordsIn" },
                { "id": "buffers.inputExclusiveBuffersUsage" },
                { "id": "Shuffle.Netty.Input.numBuffersInRemotePerSecond" },
                { "id": "numRecordsOutPerSecond" },
                { "id": "buffers.outPoolUsage" },
                { "id": "Shuffle.Netty.Input.numBuffersInLocalPerSecond" },
                { "id": "numRecordsInPerSecond" },
                { "id": "Shuffle.Netty.Input.Buffers.inputFloatingBuffersUsage" },
            ] )
    });

    static TEST_ORDERS: Lazy<Vec<MetricOrder>> = Lazy::new(|| {
        let mut orders = STD_METRIC_ORDERS.clone();
        orders.push(MetricOrder::Operator {
            name: "Source: Foo Data stream".into(),
            metric: "records-lag-max".into(),
            agg: Aggregation::Sum,
            telemetry_path: "flow.input_records_lag_max".into(),
            telemetry_type: TelemetryType::Integer,
        });
        orders
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
                {
                    "id": "Source__Foo_Data_stream.KafkaConsumer.client-id.fce7d7241a2648da85a99be91e4f2b77.catalog.ingestion-service.layer.observations.consumer-fetch-manager-metrics_records-lag-max",
                    "sum": 123456_i64,
                },
                { "id": "numRecordsInPerSecond", "max": 0_f64 },
                { "id": "numRecordsOutPerSecond", "max": 20_f64 },
                { "id": "buffers.inputQueueLength", "max": 0_f64 },
                { "id": "buffers.inPoolUsage", "max": 0_f64 },
                { "id": "buffers.outputQueueLength", "max": 1_f64 },
                { "id": "buffers.outPoolUsage", "max": 0.1_f64 },
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
            let (stage, ..) = test_stage_for(&TEST_ORDERS, context).await;

            // let scopes = maplit::hashset! { FlinkScope::Task, FlinkScope::Operator };

            let mut orders = STD_METRIC_ORDERS.clone();
            let kafka_order = MetricOrder::new(
                ScopeSpec::new(FlinkScope::Operator, "Source: Foo Data stream"),
                MetricSpec::new(
                    "records-lag-max",
                    Aggregation::Value,
                    "flow.input_records_lag_max",
                    TelemetryType::Integer,
                ),
            )
            .unwrap();
            orders.extend(vec![kafka_order.clone()]);

            // let metric_orders: Vec<MetricOrder> = orders.iter().filter(|o| scopes.iter().find(|s| *s == &o.scope()).is_some()).cloned().collect();
            // let agg_span: HashSet<Aggregation> = metric_orders.iter().map(|o| o.agg()).collect();
            // tracing::info!(?metric_orders, ?agg_span, "orders distilled");

            let actual = assert_ok!(
                stage
                    .query_vertex_telemetry(
                        &job_id,
                        &vertex_id,
                        &Id::direct("test_query_vertex_telemetry", 23, "CBA")
                    )
                    .await
            );

            assert_eq!(
                actual,
                maplit::hashmap! {
                    "flow.input_records_lag_max".to_string() => 123456_i64.into(),
                    MC_FLOW__RECORDS_IN_PER_SEC.to_string() => 0_f64.into(),
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
    //     // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()),
    // None));     // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
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
    //         let (handle, tx_trigger, mut rx_out) = test_stage_for(FlinkScope::Jobs,
    // &STD_METRIC_ORDERS, context).await;
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
    //     let restarts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64, _>(&mut
    // positive.clone())as f64;     let failed_checkpts: i64 = (1..99_999).fake(); //
    // Faker.fake_with_rng::<i64(&mut positive) as f64;
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
    //     // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()),
    // None));     // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
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
    //         let (handle, tx_trigger, mut rx_out) = test_stage_for(FlinkScope::Jobs,
    // &STD_METRIC_ORDERS, context).await;
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
    //     // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()),
    // None));     // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));
    //
    //     block_on(async {
    //         let mock_server = MockServer::start().await;
    //
    //         let (uptime, restarts, failed_checkpts, b) = make_jobs_data();
    //         let metric_response = RetryResponder::new(1, 500,
    // ResponseTemplate::new(200).set_body_json(b));         Mock::given(method("GET"))
    //             .and(path("/jobs/metrics"))
    //             .respond_with(metric_response)
    //             .expect(3)
    //             .mount(&mock_server)
    //             .await;
    //
    //         let context = assert_ok!(context_for(&mock_server));
    //         let (handle, tx_trigger, mut rx_out) = test_stage_for(FlinkScope::Jobs,
    // &STD_METRIC_ORDERS, context).await;
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
    //     // let registry = assert_ok!(Registry::new_custom(Some(registry_name.to_string()),
    // None));     // assert_ok!(registry.register(Box::new(STAGE_EVAL_TIME.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));
    //     // assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));
    //
    //     block_on(async {
    //         let mock_server = MockServer::start().await;
    //
    //         let cpu_load: f64 = 16. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
    //         let heap_used: f64 = 1_000_000_000. * ((0..100_000_000).fake::<i64>() as f64 /
    // 100_000_000.);         let heap_committed: f64 = 1_000_000_000. *
    // ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);         let nr_threads: i32 =
    // (1..150).fake();         let b = json!([
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
