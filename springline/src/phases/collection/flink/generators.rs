use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;

use futures::future::Future;
use itertools::Itertools;
use once_cell::sync::Lazy;
use proctor::elements::{telemetry, Telemetry, TelemetryValue};
use proctor::error::{CollectionError, MetricLabel, TelemetryError};
use proctor::graph::stage::{self, SourceStage, WithApi};
use proctor::graph::{Connect, Graph, SinkShape, SourceShape};
use proctor::phases::collection::TelemetrySource;
use proctor::SharedString;
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec, IntCounterVec, Opts};
use reqwest::Method;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use tokio::task::JoinHandle;
use tracing_futures::Instrument;
use url::Url;

use super::api_model::{build_telemetry, FlinkMetricResponse, JobSummary};
use super::{Aggregation, FlinkScope, MetricOrder, STD_METRIC_ORDERS};
use crate::phases::collection::flink::api_model::{JobDetail, JobId, VertexId};
use crate::settings::FlinkSettings;

pub(crate) static FLINK_COLLECTION_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "flink_collection_time",
            "Time spent collecting telemetry from Flink in seconds",
        ),
        &["flink_scope"],
    )
    .expect("failed creating flink_collection_time metric")
});

pub(crate) static FLINK_COLLECTION_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new("flink_collection_errors", "Number of errors collecting Flink telemetry"),
        &["flink_scope", "error_type"],
    )
    .expect("failed creating flink_collection_errors metric")
});

#[inline]
pub(crate) fn start_flink_collection_timer(scope: &FlinkScope) -> HistogramTimer {
    FLINK_COLLECTION_TIME
        .with_label_values(&[scope.to_string().as_str()])
        .start_timer()
}

#[inline]
pub(crate) fn track_flink_errors(scope: &FlinkScope, error: &CollectionError) {
    FLINK_COLLECTION_ERRORS
        .with_label_values(&[scope.to_string().as_str(), error.label().as_ref()])
        .inc()
}

#[inline]
fn track_generator_errors<T>(scope: FlinkScope, result: &Result<T, CollectionError>) {
    if let Err(err) = result {
        tracing::error!(%scope, error=?err, "failed to collect Flink {} telemetry part", scope);
        track_flink_errors(&scope, err);
    }
}

pub type Generator<T> =
    Box<(dyn Fn() -> Pin<Box<(dyn Future<Output = Result<T, CollectionError>> + Send)>> + Send + Sync)>;

#[derive(Clone)]
pub struct TaskContext {
    pub client: ClientWithMiddleware,
    pub base_url: Url,
}

impl fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskContext").field("base_url", &self.base_url).finish()
    }
}

#[tracing::instrument(level = "info", skip(name, settings), fields(source_name=%name.as_ref()))]
pub async fn make_flink_metrics_source(
    name: impl AsRef<str>, settings: &FlinkSettings,
) -> Result<TelemetrySource, CollectionError> {
    let name = SharedString::Owned(format!("{}_flink_source", name.as_ref()));

    let scheduler = make_scheduler_source(name.as_ref(), settings);
    let tx_scheduler_api = scheduler.tx_api();

    let gen = make_flink_generator(settings)?;

    let collect_flink_telemetry = stage::TriggeredGenerator::new(name.clone(), gen);

    let filter = stage::FilterMap::<_, Option<Telemetry>, Telemetry>::new(
        format!("{}_filter_errors", name),
        std::convert::identity,
    )
    .with_block_logging();

    (scheduler.outlet(), collect_flink_telemetry.inlet()).connect().await;
    (collect_flink_telemetry.outlet(), filter.inlet()).connect().await;
    let composite_outlet = filter.outlet();

    let mut cg = Graph::default();
    cg.push_back(Box::new(scheduler)).await;
    cg.push_back(Box::new(collect_flink_telemetry)).await;
    cg.push_back(Box::new(filter)).await;

    let composite: stage::CompositeSource<Telemetry> =
        stage::CompositeSource::new(name.clone(), cg, composite_outlet).await;
    let stage: Option<Box<dyn SourceStage<Telemetry>>> = Some(Box::new(composite));
    Ok(TelemetrySource {
        name: name.into_owned(),
        stage,
        tx_stop: Some(tx_scheduler_api),
    })
}

fn make_scheduler_source(name: &str, settings: &FlinkSettings) -> stage::Tick<()> {
    stage::Tick::new(
        format!("flink_source_{}_tick", name),
        settings.metrics_initial_delay,
        settings.metrics_interval,
        (),
    )
}

fn make_flink_generator(
    settings: &FlinkSettings,
) -> Result<impl FnMut(()) -> Pin<Box<(dyn Future<Output = Option<Telemetry>> + Send)>>, CollectionError> {
    let headers = settings.header_map()?;
    let client = reqwest::Client::builder().default_headers(headers).build()?;
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(settings.max_retries);
    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
    let context = TaskContext { client, base_url: settings.base_url()? };

    let mut orders = STD_METRIC_ORDERS.clone();
    orders.extend(settings.metric_orders.clone());

    let gen = move |_: ()| {
        let jobs_gen =
            make_root_scope_collection_generator(FlinkScope::Jobs, &orders, &context).map(|g| (FlinkScope::Jobs, g));
        let tm_gen = make_root_scope_collection_generator(FlinkScope::TaskManagers, &orders, &context)
            .map(|g| (FlinkScope::TaskManagers, g));
        let tm_admin_gen = make_taskmanagers_admin_generator(&orders, &context).map(|g| (FlinkScope::TaskManagers, g));
        let vertex_gen = make_vertex_collection_generator(&orders, &context).map(|g| (FlinkScope::Task, g));

        let task: Pin<Box<dyn Future<Output = Option<Telemetry>> + Send>> = Box::pin(async move {
            let flink_span = tracing::info_span!("collect flink telemetry");
            let _flink_span_guardian = flink_span.enter();

            let generators = vec![jobs_gen, tm_gen, tm_admin_gen, vertex_gen];

            // let tasks = generators
            let tasks: Vec<JoinHandle<Result<Telemetry, CollectionError>>> = generators
                .into_iter()
                .flatten()
                .map(|(scope, gen)| {
                    tokio::spawn(async move {
                        let gen_result = gen().await;
                        track_generator_errors(scope, &gen_result);
                        gen_result
                    })
                })
                .collect::<Vec<_>>();

            // let results : Result<Vec<Result<Telemetry, CollectionError>>, tokio::task::JoinError> =
            let results = futures::future::join_all(tasks)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>();

            let span = tracing::info_span!("consolidate telemetry parts");
            let _ = span.enter();
            match results {
                Err(err) => {
                    tracing::error!(error=?err, "failed to join Flink telemetry collection tasks");
                    track_flink_errors(&FlinkScope::Other, &err.into());
                    None
                }
                Ok(results) => {
                    let mut telemetry = Telemetry::default();
                    for (idx, result) in results.into_iter().enumerate() {
                        match result {
                            Ok(t) => {
                                tracing::info!(acc_telemetry=?telemetry, new_part=?t, "adding flink telemetry part");
                                telemetry.extend(t);
                            }
                            Err(err) => {
                                // error metric (and dup log) recorded in decorating `with_error_tracking()`
                                tracing::error!(error=?err, "failed to collect Flink telemetry portion:{}", idx);
                            }
                        }
                    }

                    Some(telemetry)
                }
            }
        });

        task
    };

    Ok(gen)
}

#[inline]
fn log_response(label: &str, response: &reqwest::Response) {
    const PREAMBLE: &str = "flink telemetry response received";
    let status = response.status();
    if status.is_success() || status.is_informational() {
        tracing::info!(?response, "{}:{}", PREAMBLE, label);
    } else if status.is_client_error() {
        tracing::error!(?response, "{}:{}", PREAMBLE, label);
    } else {
        tracing::warn!(?response, "{}:{}", PREAMBLE, label);
    }
}

pub fn make_root_scope_collection_generator(
    scope: FlinkScope, orders: &[MetricOrder], context: &TaskContext,
) -> Option<Generator<Telemetry>> {
    let scope_rep = SharedString::Owned(scope.to_string().to_lowercase());
    let scopes = maplit::hashset! { scope };
    let (metric_orders, agg_span) = distill_metric_orders_and_agg(&scopes, orders);
    if metric_orders.is_empty() {
        return None;
    }

    let mut url = context.base_url.clone();
    url.path_segments_mut().unwrap().push(scope_rep.as_ref()).push("metrics");
    url.query_pairs_mut()
        .clear()
        .append_pair("get", metric_orders.keys().join(",").as_str())
        .append_pair("agg", agg_span.iter().join(",").as_str());

    let client = context.client.clone();

    let gen: Generator<Telemetry> = Box::new(move || {
        let client = client.clone();
        let orders = metric_orders.clone();
        let url = url.clone();
        let scope_rep = scope_rep.clone();
        let scope_rep2 = scope_rep.clone();

        Box::pin(
            async move {
                let _timer = start_flink_collection_timer(&scope);
                let scope_resp = client.request(Method::GET, url).send().await.map_err(|err| {
                    let collection_err: CollectionError = err.into();
                    track_flink_errors(&scope, &collection_err);
                    collection_err
                })?;

                log_response(format!("{} scope response", scope_rep2).as_str(), &scope_resp);
                let scope_resp: FlinkMetricResponse = scope_resp.json().await?;

                build_telemetry(scope_resp, &orders).map_err(|err| {
                    let collection_err: CollectionError = err.into();
                    track_flink_errors(&scope, &collection_err);
                    collection_err
                })
            }
            .instrument(tracing::info_span!("collect Flink scope telemetry", %scope_rep)),
        )
    });

    Some(gen)
}

// note: `cluster.nr_task_managers` is a standard metric pulled from Flink's admin API. The order
// mechanism may need to be expanded to consider further meta information outside of Flink Metrics
// API.
pub fn make_taskmanagers_admin_generator(
    _orders: &[MetricOrder], context: &TaskContext,
) -> Option<Generator<Telemetry>> {
    let client = context.client.clone();

    let mut url = context.base_url.clone();
    url.path_segments_mut().unwrap().push("taskmanagers");

    let gen: Generator<Telemetry> = Box::new(move || {
        let client = client.clone();
        let url = url.clone();

        Box::pin(
            async move {
                let _timer = start_flink_collection_timer(&FlinkScope::TaskManagers);

                let taskmanagers_admin_resp = client.request(Method::GET, url).send().await.map_err(|err| {
                    let collection_err: CollectionError = err.into();
                    track_flink_errors(&FlinkScope::TaskManagers, &collection_err);
                    collection_err
                })?;

                log_response("taskmanagers admin", &taskmanagers_admin_resp);

                let taskmanagers_admin_resp: serde_json::Value =
                    taskmanagers_admin_resp.json().await.map_err(|err| {
                        let collection_err: CollectionError = err.into();
                        track_flink_errors(&FlinkScope::TaskManagers, &collection_err);
                        collection_err
                    })?;

                let taskmanagers = taskmanagers_admin_resp["taskmanagers"]
                    .as_array()
                    .map(|tms| tms.len())
                    .unwrap_or(0);
                let mut telemetry: telemetry::TableType = HashMap::default();
                telemetry.insert("cluster.nr_task_managers".to_string(), taskmanagers.into());
                Ok(telemetry.into())
            }
            .instrument(tracing::info_span!("collect Flink taskmanagers admin telemetry",)),
        )
    });
    Some(gen)
}

pub fn make_vertex_collection_generator(orders: &[MetricOrder], context: &TaskContext) -> Option<Generator<Telemetry>> {
    let scopes = maplit::hashset! {
        FlinkScope::Task, FlinkScope::Kafka, FlinkScope::Kinesis,
    };

    let orders = orders.to_vec();
    let (metric_orders, _agg_span) = distill_metric_orders_and_agg(&scopes, &orders);
    if metric_orders.is_empty() {
        return None;
    }

    let context = context.clone();

    let gen: Generator<Telemetry> = Box::new(move || {
        let context = context.clone();
        let orders = orders.clone();
        let metric_orders = metric_orders.clone();

        Box::pin(
            async move {
                let _timer = start_flink_collection_timer(&FlinkScope::Task);
                let active_jobs = query_active_jobs(&context).await?;

                let mut job_details = Vec::with_capacity(active_jobs.len());
                for job in active_jobs {
                    let detail = query_job_details(job.id.as_ref(), &context).await?;
                    job_details.push(detail);
                }

                let mut metric_points: HashMap<String, Vec<TelemetryValue>> = HashMap::new();
                for job in job_details {
                    for vertex in job.vertices.into_iter().filter(|v| v.status.is_active()) {
                        let vertex_telemetry =
                            query_vertex_telemetry(&job.jid, &vertex.id, &metric_orders, &context).await?;

                        collect_metric_points(&mut metric_points, vertex_telemetry);
                    }
                }

                merge_telemetry_per_order(metric_points, &orders).map_err(|err| err.into())
            }
            .instrument(tracing::info_span!("collect Flink vertices telemetry")),
        )
    });

    Some(gen)
}

fn collect_metric_points(metric_points: &mut HashMap<String, Vec<TelemetryValue>>, vertex_telemetry: Telemetry) {
    for (metric, vertex_val) in vertex_telemetry.into_iter() {
        metric_points.entry(metric).or_insert_with(Vec::default).push(vertex_val);
    }
}

#[tracing::instrument(level = "info", skip(orders))]
fn merge_telemetry_per_order(
    metric_points: HashMap<String, Vec<TelemetryValue>>, orders: &[MetricOrder],
) -> Result<Telemetry, TelemetryError> {
    // to avoid repeated linear searches, reorg strategy data based on metrics
    let mut telemetry_agg = HashMap::with_capacity(orders.len());
    for o in orders {
        telemetry_agg.insert(o.telemetry_path.as_str(), o.agg);
    }

    // merge via order aggregation
    let telemetry: Telemetry = metric_points
        .into_iter()
        .map(
            |(metric, values)| match telemetry_agg.get(metric.as_str()).map(|agg| (agg, agg.combinator())) {
                None => Ok(Some((metric, TelemetryValue::Seq(values)))),
                Some((agg, combo)) => {
                    let merger = combo.combine(values.clone()).map(|combined| combined.map(|c| (metric, c)));
                    tracing::info!(?merger, ?values, %agg, "merging metric values per order aggregator");
                    merger
                }
            },
        )
        .collect::<Result<Vec<_>, TelemetryError>>()?
        .into_iter()
        .flatten()
        .collect::<telemetry::TableType>()
        .into();

    Ok(telemetry)
}

#[tracing::instrument(level = "info", skip(context))]
async fn query_active_jobs(context: &TaskContext) -> Result<Vec<JobSummary>, CollectionError> {
    let mut url = context.base_url.clone();
    url.path_segments_mut().unwrap().push("jobs");

    let active_jobs_resp = context.client.request(Method::GET, url).send().await?;
    log_response("active jobs", &active_jobs_resp);
    let active_jobs_resp: serde_json::Value = active_jobs_resp.json().await?;

    let jobs: Vec<JobSummary> = match active_jobs_resp.get("jobs") {
        None => Vec::default(),
        Some(js) => serde_json::from_value(js.clone())?,
    };

    Ok(jobs.into_iter().filter(|j| j.status.is_active()).collect())
}

#[tracing::instrument(level = "info", skip(context))]
async fn query_job_details(job_id: &str, context: &TaskContext) -> Result<JobDetail, CollectionError> {
    let mut url = context.base_url.clone();
    url.path_segments_mut().unwrap().push("jobs").push(job_id);
    let job_details_resp = context.client.request(Method::GET, url).send().await?;
    log_response("job details", &job_details_resp);
    let job_details_resp = job_details_resp.json().await?;
    Ok(job_details_resp)
}

#[tracing::instrument(level = "info", skip(metric_orders, context))]
async fn query_vertex_telemetry(
    job_id: &JobId, vertex_id: &VertexId, metric_orders: &HashMap<String, Vec<MetricOrder>>, context: &TaskContext,
) -> Result<Telemetry, CollectionError> {
    let mut url = context.base_url.clone();
    url.path_segments_mut()
        .unwrap()
        .push("jobs")
        .push(job_id.as_ref())
        .push("vertices")
        .push(vertex_id.as_ref())
        .push("subtasks")
        .push("metrics");

    let mut url2 = url.clone();

    let available_metrics_resp = context.client.request(Method::GET, url).send().await?;
    log_response("available metrics", &available_metrics_resp);
    let available_metrics_resp: FlinkMetricResponse = available_metrics_resp.json().await?;

    let available_vertex_metrics: HashSet<String> = available_metrics_resp
        .into_iter()
        .filter_map(|m| metric_orders.get(&m.id).and(Some(m.id)))
        .collect();
    let vertex_agg_span = agg_span_for(&available_vertex_metrics, metric_orders);
    tracing::info!(
        ?available_vertex_metrics,
        ?vertex_agg_span,
        "available vertex metrics identified per order"
    );

    let telemetry = if !available_vertex_metrics.is_empty() {
        url2.query_pairs_mut()
            .clear()
            .append_pair("get", available_vertex_metrics.into_iter().join(",").as_str());

        if !vertex_agg_span.is_empty() {
            url2.query_pairs_mut()
                .append_pair("agg", vertex_agg_span.iter().join(",").as_str());
        }

        let vertex_telemetry_resp = context.client.request(Method::GET, url2).send().await?;
        log_response("job vertex telemetry", &vertex_telemetry_resp);
        let vertex_telemetry_resp: FlinkMetricResponse = vertex_telemetry_resp.json().await?;
        build_telemetry(vertex_telemetry_resp, metric_orders)?
    } else {
        Telemetry::default()
    };

    Ok(telemetry)
}

fn agg_span_for(target: &HashSet<String>, metric_orders: &HashMap<String, Vec<MetricOrder>>) -> HashSet<Aggregation> {
    let result: HashSet<Aggregation> = target
        .iter()
        .flat_map(|metric| {
            metric_orders
                .get(metric)
                .cloned()
                .unwrap_or_else(Vec::new)
                .into_iter()
                .map(|order| order.agg)
                .filter(|agg| *agg != Aggregation::Value)
                .collect::<HashSet<_>>()
        })
        .collect();

    result
}

/// Distills orders to requested scope and reorganizes them (order has metric+agg) to metric and
/// consolidates aggregation span.
fn distill_metric_orders_and_agg(
    scopes: &HashSet<FlinkScope>, orders: &[MetricOrder],
) -> (HashMap<String, Vec<MetricOrder>>, HashSet<Aggregation>) {
    let mut order_domain = HashMap::default();
    let mut agg_span = HashSet::default();

    for o in orders.iter().filter(|o| scopes.contains(&o.scope)) {
        agg_span.insert(o.agg);
        let entry = order_domain.entry(o.metric.clone()).or_insert_with(Vec::new);
        entry.push(o.clone());
    }

    (order_domain, agg_span)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use cast_trait_object::DynCastExt;
    use claim::*;
    use fake::{Fake, Faker};
    use pretty_assertions::assert_eq;
    use proctor::elements::{TelemetryType, Timestamp};
    use proctor::graph::stage::tick::TickMsg;
    use proctor::graph::Graph;
    use reqwest::header::HeaderMap;
    use serde_json::json;
    use tokio_test::block_on;
    use wiremock::matchers::{method, path};
    use wiremock::{Match, Mock, MockServer, Request, Respond, ResponseTemplate};

    use super::*;
    use crate::phases::collection::flink::api_model::{JobId, JobState, TaskState, VertexDetail};

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

    #[test]
    fn test_flink_collect_failure() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_simple_jobs_collect");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            let metric_response = ResponseTemplate::new(500);
            Mock::given(method("GET"))
                .and(path("/jobs/metrics"))
                .respond_with(metric_response)
                .expect(3)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));

            let gen = assert_some!(make_root_scope_collection_generator(
                FlinkScope::Jobs,
                &STD_METRIC_ORDERS,
                &context
            ));

            assert_err!(gen().await, "Checking for error passed back after retries.");
        });
    }

    #[test]
    fn test_flink_simple_jobs_collect() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_simple_jobs_collect");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            let uptime: i64 = Faker.fake::<chrono::Duration>().num_milliseconds();
            let restarts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64, _>(&mut positive.clone())as f64;
            let failed_checkpts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64(&mut positive) as f64;

            let b = json!([
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

            let metric_response = ResponseTemplate::new(200).set_body_json(b);

            Mock::given(method("GET"))
                .and(path("/jobs/metrics"))
                .respond_with(metric_response)
                .expect(2)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));

            let gen = assert_some!(make_root_scope_collection_generator(
                FlinkScope::Jobs,
                &STD_METRIC_ORDERS,
                &context
            ));

            let actual: Telemetry = assert_ok!(gen().await);
            assert_eq!(
                actual,
                maplit::hashmap! {
                    "health.job_uptime_millis".to_string() => uptime.into(),
                    "health.job_nr_restarts".to_string() => restarts.into(),
                    "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
                }
                .into()
            );

            let actual: Telemetry = assert_ok!(gen().await);
            assert_eq!(
                actual,
                maplit::hashmap! {
                    "health.job_uptime_millis".to_string() => uptime.into(),
                    "health.job_nr_restarts".to_string() => restarts.into(),
                    "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
                }
                .into()
            );

            let status = assert_ok!(reqwest::get(format!("{}/missing", &mock_server.uri())).await).status();
            assert_eq!(status, 404);
        });
    }

    #[test]
    fn test_flink_retry_jobs_collect() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_retry_jobs_collect");
        let _ = main_span.enter();

        let registry_name = "test_flink";
        let registry = assert_ok!(prometheus::Registry::new_custom(Some(registry_name.to_string()), None));
        assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));
        assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));

        block_on(async {
            let mock_server = MockServer::start().await;

            let uptime: i64 = Faker.fake::<chrono::Duration>().num_milliseconds();
            let restarts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64, _>(&mut positive.clone())as f64;
            let failed_checkpts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64(&mut positive) as f64;

            let b = json!([
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

            let metric_response = RetryResponder::new(1, 500, ResponseTemplate::new(200).set_body_json(b));

            Mock::given(method("GET"))
                .and(path("/jobs/metrics"))
                .respond_with(metric_response)
                .expect(3)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));

            let gen = assert_some!(make_root_scope_collection_generator(
                FlinkScope::Jobs,
                &STD_METRIC_ORDERS,
                &context
            ));

            let actual: Telemetry = assert_ok!(gen().await);
            assert_eq!(
                actual,
                maplit::hashmap! {
                    "health.job_uptime_millis".to_string() => uptime.into(),
                    "health.job_nr_restarts".to_string() => restarts.into(),
                    "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
                }
                .into()
            );

            let actual: Telemetry = assert_ok!(gen().await);
            assert_eq!(
                actual,
                maplit::hashmap! {
                    "health.job_uptime_millis".to_string() => uptime.into(),
                    "health.job_nr_restarts".to_string() => restarts.into(),
                    "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
                }
                .into()
            );

            let status = assert_ok!(reqwest::get(format!("{}/missing", &mock_server.uri())).await).status();
            assert_eq!(status, 404);
        });

        let metric_family = registry.gather();
        assert_eq!(metric_family.len(), 1);
        assert_eq!(
            metric_family[0].get_name(),
            &format!("{}_{}", registry_name, "flink_collection_time")
        );
        let metrics = metric_family[0].get_metric();
        assert_eq!(metrics[0].get_label().len(), 1);
        assert_eq!(metrics[0].get_label()[0].get_name(), "flink_scope");
        assert_eq!(metrics[0].get_label()[0].get_value(), "Jobs");
    }

    #[test]
    fn test_flink_jobs_error_metrics() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_jobs_error_metrics");
        let _ = main_span.enter();

        let registry_name = "test_flink";
        let registry = assert_ok!(prometheus::Registry::new_custom(Some(registry_name.to_string()), None));
        assert_ok!(registry.register(Box::new(FLINK_COLLECTION_TIME.clone())));
        assert_ok!(registry.register(Box::new(FLINK_COLLECTION_ERRORS.clone())));

        block_on(async {
            let mock_server = MockServer::start().await;

            let uptime: i64 = Faker.fake::<chrono::Duration>().num_milliseconds();
            let restarts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64, _>(&mut positive.clone())as f64;
            let failed_checkpts: i64 = (1..99_999).fake(); // Faker.fake_with_rng::<i64(&mut positive) as f64;

            let b = json!([
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

            let metric_response = RetryResponder::new(55, 500, ResponseTemplate::new(200).set_body_json(b));

            Mock::given(method("GET"))
                .and(path("/jobs/metrics"))
                .respond_with(metric_response)
                .expect(3)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));

            let gen = assert_some!(make_root_scope_collection_generator(
                FlinkScope::Jobs,
                &STD_METRIC_ORDERS,
                &context
            ));

            let result = assert_err!(gen().await);
            track_flink_errors(&FlinkScope::Jobs, &result);
        });

        let metric_family = registry.gather();
        assert_eq!(metric_family.len(), 2);
        assert_eq!(
            metric_family[0].get_name(),
            &format!("{}_{}", registry_name, "flink_collection_errors")
        );
        let metrics = metric_family[0].get_metric();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].get_label().len(), 2);
        assert_eq!(metrics[0].get_label()[0].get_name(), "error_type");
        assert_eq!(metrics[0].get_label()[0].get_value(), "collection::http_integration");
        assert_eq!(metrics[0].get_label()[1].get_name(), "flink_scope");
        assert_eq!(metrics[0].get_label()[1].get_value(), "Jobs");
        let error_types: Vec<&str> = metrics
            .iter()
            .flat_map(|m| {
                m.get_label()
                    .iter()
                    .filter(|l| l.get_name() == "error_type")
                    .map(|l| l.get_value())
            })
            .collect();
        assert_eq!(error_types, vec!["collection::http_integration",]);
    }

    #[test]
    fn test_flink_simple_taskmanagers_scope_collect() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_simple_taskmanagers_scope_collect");
        let _ = main_span.enter();

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

            let metric_response = ResponseTemplate::new(200).set_body_json(b);

            Mock::given(method("GET"))
                .and(path("/taskmanagers/metrics"))
                .respond_with(metric_response)
                .expect(2)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));

            let gen = assert_some!(make_root_scope_collection_generator(
                FlinkScope::TaskManagers,
                &STD_METRIC_ORDERS,
                &context
            ));

            let actual: Telemetry = assert_ok!(gen().await);
            assert_eq!(
                actual,
                maplit::hashmap! {
                    "cluster.task_cpu_load".to_string() => cpu_load.into(),
                    "cluster.task_heap_memory_used".to_string() => heap_used.into(),
                    "cluster.task_heap_memory_committed".to_string() => heap_committed.into(),
                    "cluster.task_nr_threads".to_string() => nr_threads.into(),
                }
                .into()
            );

            let actual: Telemetry = assert_ok!(gen().await);
            assert_eq!(
                actual,
                maplit::hashmap! {
                    "cluster.task_cpu_load".to_string() => cpu_load.into(),
                    "cluster.task_heap_memory_used".to_string() => heap_used.into(),
                    "cluster.task_heap_memory_committed".to_string() => heap_committed.into(),
                    "cluster.task_nr_threads".to_string() => nr_threads.into(),
                }
                .into()
            );

            let status = assert_ok!(reqwest::get(format!("{}/missing", &mock_server.uri())).await).status();
            assert_eq!(status, 404);
        });
    }

    #[test]
    fn test_flink_taskmanagers_admin_collect() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_taskmanagers_admin_collect");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            let b = json!({
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
            });

            let metric_response = ResponseTemplate::new(200).set_body_json(b);

            Mock::given(method("GET"))
                .and(path("/taskmanagers"))
                .respond_with(metric_response)
                .expect(2)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));

            let gen = assert_some!(make_taskmanagers_admin_generator(&STD_METRIC_ORDERS, &context,));

            let actual: Telemetry = assert_ok!(gen().await);
            assert_eq!(
                actual,
                maplit::hashmap! {
                    "cluster.nr_task_managers".to_string() => 2.into(),
                }
                .into()
            );

            let actual: Telemetry = assert_ok!(gen().await);
            assert_eq!(
                actual,
                maplit::hashmap! {
                    "cluster.nr_task_managers".to_string() => 2.into(),
                }
                .into()
            );
        });
    }

    #[test]
    fn test_query_active_jobs() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_query_active_jobs");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            let b = json!({
                "jobs": [
                    {
                        "id": "0771e8332dc401d254a140a707169a48",
                        "status": "RUNNING"
                    },
                    {
                        "id": "5226h8332dc401d254a140a707114f93",
                        "status": "CREATED"
                    }
                ]
            });

            let response = ResponseTemplate::new(200).set_body_json(b);

            Mock::given(method("GET"))
                .and(path("/jobs"))
                .respond_with(response)
                .expect(1)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));

            let actual = assert_ok!(query_active_jobs(&context).await);
            assert_eq!(
                actual,
                vec![
                    JobSummary {
                        id: JobId::new("0771e8332dc401d254a140a707169a48"),
                        status: JobState::Running,
                    },
                    JobSummary {
                        id: JobId::new("5226h8332dc401d254a140a707114f93"),
                        status: JobState::Created,
                    },
                ]
            )
        })
    }

    #[test]
    fn test_query_job_details() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_query_job_details");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            let b = json!({
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

            let response = ResponseTemplate::new(200).set_body_json(b);

            Mock::given(method("GET"))
                .and(path("/jobs/a97b6344d775aafe03e55a8e812d2713"))
                .respond_with(response)
                .expect(1)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));

            let actual = assert_ok!(query_job_details("a97b6344d775aafe03e55a8e812d2713", &context).await);
            assert_eq!(
                actual,
                JobDetail {
                    jid: JobId::new("a97b6344d775aafe03e55a8e812d2713"),
                    name: "CarTopSpeedWindowingExample".to_string(),
                    is_stoppable: false,
                    state: JobState::Running,
                    start_time: Timestamp::new(1639156793, 312_000_000),
                    end_time: None,
                    duration: Duration::from_millis(23079),
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
                            duration: Duration::from_millis(22203),
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
                            }
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
                            duration: Duration::from_millis(22198),
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
                            }
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
                    }
                }
            )
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
            let query_keys: HashSet<Cow<str>> = request.url.query_pairs().map(|(k, _)| k).collect();
            query_keys.contains(self.0.as_str())
        }
    }

    #[test]
    fn test_query_vertex_telemetry() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_query_vertex_telemetry");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            let summary = json!([
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
            ] );

            let summary_response = ResponseTemplate::new(200).set_body_json(summary);

            let metrics = json!([
                { "id": "numRecordsInPerSecond", "max": 0.0 },
                { "id": "numRecordsOutPerSecond", "max": 20.0 },
                { "id": "buffers.inputQueueLength", "max": 0.0 },
                { "id": "buffers.inPoolUsage", "max": 0.0 },
                { "id": "buffers.outputQueueLength", "max": 1.0 },
                { "id": "buffers.outPoolUsage", "max": 0.1 },
            ] );

            let metrics_response = ResponseTemplate::new(200).set_body_json(metrics);

            let job_id = JobId::new("f3f10c679805d35fbed73a08c37d03cc");
            let vertex_id = VertexId::new("cbc357ccb763df2852fee8c4fc7d55f2");

            let query_path = format!(
                "/jobs/{jid}/vertices/{vid}/subtasks/metrics",
                jid = job_id,
                vid = vertex_id
            );

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

            let (metric_orders, agg_span) = distill_metric_orders_and_agg(&scopes, &orders);
            tracing::info!(?metric_orders, ?agg_span, "orders distilled");

            let actual = assert_ok!(query_vertex_telemetry(&job_id, &vertex_id, &metric_orders, &context).await);

            assert_eq!(
                actual,
                maplit::hashmap! {
                    "flow.records_in_per_sec".to_string() => 0_f64.into(),
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

    #[test]
    fn test_flink_metrics_generator() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_flink_metrics_generator");
        let _ = main_span.enter();

        block_on(async {
            let mock_server = MockServer::start().await;

            // -- jobs telemetry --
            let uptime: i64 = Faker.fake::<chrono::Duration>().num_milliseconds();
            let restarts: i64 = (0..99).fake();
            let failed_checkpts: i64 = (0..99).fake();
            let jobs_json = json!([
                { "id": "uptime", "max":  uptime as f64,},
                { "id": "numRestarts", "max":  restarts as f64,},
                { "id": "numberOfFailedCheckpoints", "max":  failed_checkpts as f64,},
            ]);
            let jobs_response = ResponseTemplate::new(200).set_body_json(jobs_json);
            Mock::given(method("GET"))
                .and(path("/jobs/metrics"))
                .respond_with(jobs_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            // -- taskmanagers telemetry --
            let cpu_load: f64 = 16. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
            let heap_used: f64 = 1_000_000_000. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
            let heap_committed: f64 = 1_000_000_000. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
            let nr_threads: i32 = (1..150).fake();
            let tm_json = json!([
                { "id": "Status.JVM.CPU.Load", "max":  cpu_load,},
                { "id": "Status.JVM.Memory.Heap.Used", "max":  heap_used,},
                { "id": "Status.JVM.Memory.Heap.Committed", "max":  heap_committed,},
                { "id": "Status.JVM.Threads.Count", "max":  nr_threads as f64,},
            ]);
            let tm_response = ResponseTemplate::new(200).set_body_json(tm_json);
            Mock::given(method("GET"))
                .and(path("/taskmanagers/metrics"))
                .respond_with(tm_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            // -- taskmanager admin telemetry --
            let tm_admin_json = json!({
                "taskmanagers": [
                    {
                        "id": "100.97.247.74:43435-69a783",
                        "path": "akka.tcp://flink@100.97.247.74:43435/user/rpc/taskmanager_0",
                        "dataPort": Faker.fake::<i32>(),
                        "jmxPort": Faker.fake::<i32>(),
                        "timeSinceLastHeartbeat": Faker.fake::<i64>(), // 1638856220901_i64,
                        "slotsNumber": Faker.fake::<i32>(), // 1,
                        "freeSlots": Faker.fake::<i32>(), // 0,
                        "totalResource": {
                            "cpuCores": Faker.fake::<f64>(), // 1.0,
                            "taskHeapMemory": Faker.fake::<i32>(), // 3327,
                            "taskOffHeapMemory": Faker.fake::<i32>(), //0,
                            "managedMemory": Faker.fake::<i32>(), //2867,
                            "networkMemory": Faker.fake::<i32>(), //716,
                            "extendedResources": {}
                        },
                        "freeResource": {
                            "cpuCores": Faker.fake::<f64>(), //0.0,
                            "taskHeapMemory": Faker.fake::<i32>(), //0,
                            "taskOffHeapMemory": Faker.fake::<i32>(), //0,
                            "managedMemory": Faker.fake::<i32>(), //0,
                            "networkMemory": Faker.fake::<i32>(), //0,
                            "extendedResources": {}
                        },
                        "hardware": {
                            "cpuCores": Faker.fake::<i32>(), //1,
                            "physicalMemory": Faker.fake::<i64>(), //267929460736_i64,
                            "freeMemory": Faker.fake::<i64>(), //3623878656_i64,
                            "managedMemory": Faker.fake::<i64>(), //3006477152_i64
                        },
                        "memoryConfiguration": {
                            "frameworkHeap": Faker.fake::<i64>(), //134217728_i64,
                            "taskHeap": Faker.fake::<i64>(), //3489660872_i64,
                            "frameworkOffHeap": Faker.fake::<i64>(), //134217728_i64,
                            "taskOffHeap": Faker.fake::<i64>(), //0,
                            "networkMemory": Faker.fake::<i64>(), //751619288_i64,
                            "managedMemory": Faker.fake::<i64>(), //3006477152_i64,
                            "jvmMetaspace": Faker.fake::<i64>(), //268435456_i64,
                            "jvmOverhead": Faker.fake::<i64>(), //864958705_i64,
                            "totalFlinkMemory": Faker.fake::<i64>(), //7516192768_i64,
                            "totalProcessMemory": Faker.fake::<i64>(), //8649586929_i64
                        }
                    },
                    {
                        "id": "100.97.247.74:43435-69a798",
                        "path": "akka.tcp://flink@100.97.247.74:43435/user/rpc/taskmanager_1",
                        "dataPort": Faker.fake::<i32>(), //42381,
                        "jmxPort": Faker.fake::<i32>(), //-1,
                        "timeSinceLastHeartbeat": 1638856220901_i64,
                        "slotsNumber": Faker.fake::<i32>(), //1,
                        "freeSlots": Faker.fake::<i32>(), //0,
                        "totalResource": {
                            "cpuCores": Faker.fake::<f64>(), //1.0,
                            "taskHeapMemory": Faker.fake::<i32>(), //3327,
                            "taskOffHeapMemory": Faker.fake::<i32>(), //0,
                            "managedMemory": Faker.fake::<i32>(), //2867,
                            "networkMemory": Faker.fake::<i32>(), //716,
                            "extendedResources": {}
                        },
                        "freeResource": {
                            "cpuCores": Faker.fake::<f64>(), //0.0,
                            "taskHeapMemory": Faker.fake::<i32>(), //0,
                            "taskOffHeapMemory": Faker.fake::<i32>(), //0,
                            "managedMemory": Faker.fake::<i32>(), //0,
                            "networkMemory": Faker.fake::<i32>(), //0,
                            "extendedResources": {}
                        },
                        "hardware": {
                            "cpuCores": Faker.fake::<i32>(), //1,
                            "physicalMemory": Faker.fake::<i64>(), //267929460736_i64,
                            "freeMemory": Faker.fake::<i64>(), //3623878656_i64,
                            "managedMemory": Faker.fake::<i64>(), //3006477152_i64
                        },
                        "memoryConfiguration": {
                            "frameworkHeap": Faker.fake::<i64>(), //134217728_i64,
                            "taskHeap": Faker.fake::<i64>(), //3489660872_i64,
                            "frameworkOffHeap": Faker.fake::<i64>(), //134217728_i64,
                            "taskOffHeap": Faker.fake::<i32>(), //0,
                            "networkMemory": Faker.fake::<i64>(), //751619288_i64,
                            "managedMemory": Faker.fake::<i64>(), //3006477152_i64,
                            "jvmMetaspace": Faker.fake::<i64>(), //268435456_i64,
                            "jvmOverhead": Faker.fake::<i64>(), //864958705_i64,
                            "totalFlinkMemory": Faker.fake::<i64>(), //7516192768_i64,
                            "totalProcessMemory": Faker.fake::<i64>(), //8649586929_i64
                        }
                    }
                ]
            });
            let tm_admin_response = ResponseTemplate::new(200).set_body_json(tm_admin_json);
            Mock::given(method("GET"))
                .and(path("/taskmanagers"))
                .respond_with(tm_admin_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            // -- query active jobs
            let job_1 = "0771e8332dc401d254a140a707169a48";
            let job_2 = "5226h8332dc401d254a140a707114f93";
            let active_jobs_json = json!({
                "jobs": [ { "id": job_1, "status": "RUNNING" }, { "id": job_2, "status": "CREATED" } ]
            });
            let active_jobs_response = ResponseTemplate::new(200).set_body_json(active_jobs_json);
            Mock::given(method("GET"))
                .and(path("/jobs"))
                .respond_with(active_jobs_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            // -- query active job running --
            let vertex_1_1 = "cbc357ccb763df2852fee8c4fc7d55f2";
            let vertex_1_2 = "90bea66de1c231edf33913ecd54406c1";

            let active_job_1_json = json!({
                "jid": job_1,
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
                        "id": vertex_1_1,
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
                        "id": vertex_1_2,
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
                            "id": vertex_1_2,
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
                            "id": vertex_1_1,
                            "parallelism": 1,
                            "operator": "",
                            "operator_strategy": "",
                            "description": "Source: Custom Source -&gt; Timestamps/Watermarks",
                            "optimizer_properties": {}
                        }
                    ]
                }
            });

            let active_job_1_response = ResponseTemplate::new(200).set_body_json(active_job_1_json);
            Mock::given(method("GET"))
                .and(path(format!("/jobs/{}", job_1)))
                .respond_with(active_job_1_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            // -- active job 2 --
            let active_job_2_json = json!({
                "jid": job_2,
                "name": "Stubby Job",
                "isStoppable": false,
                "state": "CREATED",
                "start-time": 1639156793312_i64,
                "end-time": -1,
                "duration": 23079,
                "maxParallelism": -1,
                "now": 1639156816391_i64,
                "timestamps": {
                    "CREATED": 1639156793320_i64,
                    "FAILED": 0,
                    "RUNNING": 0,
                    "CANCELED": 0,
                    "CANCELLING": 0,
                    "SUSPENDED": 0,
                    "FAILING": 0,
                    "RESTARTING": 0,
                    "FINISHED": 0,
                    "INITIALIZING": 1639156793312_i64,
                    "RECONCILING": 0
                },
                "vertices": [ ],
                "status-counts": {
                    "SCHEDULED": 0,
                    "FINISHED": 0,
                    "FAILED": 0,
                    "CANCELING": 0,
                    "CANCELED": 0,
                    "DEPLOYING": 0,
                    "RECONCILING": 0,
                    "CREATED": 0,
                    "RUNNING": 0,
                    "INITIALIZING": 0
                },
                "plan": {
                    "jid": "a97b6344d775aafe03e55a8e812d2713",
                    "name": "Stubby_1",
                    "nodes": []
                }
            });
            let active_job_2_response = ResponseTemplate::new(200).set_body_json(active_job_2_json);
            Mock::given(method("GET"))
                .and(path(format!("/jobs/{}", job_2)))
                .respond_with(active_job_2_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            let num_records_in_per_second_1 = Faker.fake::<f64>();
            let num_records_in_per_second_2 = Faker.fake::<f64>();
            let max_num_records_in_per_second = num_records_in_per_second_1.max(num_records_in_per_second_2);
            tracing::info!(%num_records_in_per_second_1, %num_records_in_per_second_2, %max_num_records_in_per_second, "flow.records_in_per_sec");

            let num_records_out_per_second_1 = Faker.fake::<f64>();
            let num_records_out_per_second_2 = Faker.fake::<f64>();
            let max_num_records_out_per_second = num_records_out_per_second_1.max(num_records_out_per_second_2);
            tracing::info!(%num_records_out_per_second_1, %num_records_out_per_second_2, %max_num_records_out_per_second, "flow.records_out_per_sec");

            let buf_input_queue_len_1 = Faker.fake::<f64>();
            let buf_input_queue_len_2 = Faker.fake::<f64>();
            let max_buf_input_queue_len = buf_input_queue_len_1.max(buf_input_queue_len_2);
            tracing::info!(%buf_input_queue_len_1, %buf_input_queue_len_2, %max_buf_input_queue_len, "cluster.task_network_input_queue_len");

            let buf_input_pool_usage_1 = Faker.fake::<f64>();
            let buf_input_pool_usage_2 = Faker.fake::<f64>();
            let max_buf_input_pool_usage = buf_input_pool_usage_1.max(buf_input_pool_usage_2);
            tracing::info!(%buf_input_pool_usage_1, %buf_input_pool_usage_2, %max_buf_input_pool_usage, "cluster.task_network_input_pool_usage");

            let buf_output_queue_len_1 = Faker.fake::<f64>();
            let buf_output_queue_len_2 = Faker.fake::<f64>();
            let max_buf_output_queue_len = buf_output_queue_len_1.max(buf_output_queue_len_2);
            tracing::info!(%buf_output_queue_len_1, %buf_output_queue_len_2, %max_buf_output_queue_len, "cluster.task_network_output_queue_len");

            let buf_output_pool_usage_1 = Faker.fake::<f64>();
            let buf_output_pool_usage_2 = Faker.fake::<f64>();
            let max_buf_output_pool_usage = buf_output_pool_usage_1.max(buf_output_pool_usage_2);
            tracing::info!(%buf_output_pool_usage_1, %buf_output_pool_usage_2, %max_buf_output_pool_usage, "cluster.task_network_output_pool_usage");

            // -- job 1
            let summary_1_json = json!([
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
            ]);

            let summary_1_response = ResponseTemplate::new(200).set_body_json(summary_1_json);

            let metrics_1_json = json!([
                { "id": "numRecordsInPerSecond", "max": num_records_in_per_second_1 },
                { "id": "numRecordsOutPerSecond", "max": num_records_out_per_second_1 },
                { "id": "buffers.inputQueueLength", "max": buf_input_queue_len_1 },
                { "id": "buffers.inPoolUsage", "max": buf_input_pool_usage_1 },
                { "id": "buffers.outputQueueLength", "max": buf_output_queue_len_1 },
                { "id": "buffers.outPoolUsage", "max": buf_output_pool_usage_1 },
            ] );

            let metrics_1_response = ResponseTemplate::new(200).set_body_json(metrics_1_json);

            let job_id = JobId::new(job_1.clone());
            let vertex_id = VertexId::new(vertex_1_1.clone());

            let query_path = format!(
                "/jobs/{jid}/vertices/{vid}/subtasks/metrics",
                jid = job_id,
                vid = vertex_id
            );

            Mock::given(method("GET"))
                .and(path(query_path.clone()))
                .and(EmptyQueryParamMatcher)
                .respond_with(summary_1_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            Mock::given(method("GET"))
                .and(path(query_path))
                .and(QueryParamKeyMatcher::new("get"))
                .respond_with(metrics_1_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            // -- job 2
            let summary_2_json = json!([
                { "id": "Shuffle.Netty.Output.Buffers.outPoolUsage" },
                { "id": "checkpointStartDelayNanos" },
                { "id": "numBytesInLocal" },
                { "id": "checkpointAlignmentTime" },
                { "id": "numBytesInRemotePerSecond" },
                { "id": "Shuffle.Netty.Input.numBytesInRemotePerSecond" },
                { "id": "numBytesOut" },
                { "id": "Window(GlobalWindows()__DeltaTrigger__TimeEvictor__ComparableAggregator__PassThr.numRecordsInPerSecond" },
                { "id": "Window(GlobalWindows()__DeltaTrigger__TimeEvictor__ComparableAggregator__PassThr.currentOutputWatermark" },
                { "id": "numBytesIn" },
                { "id": "Window(GlobalWindows()__DeltaTrigger__TimeEvictor__ComparableAggregator__PassThr.currentInputWatermark" },
                { "id": "Sink__Print_to_Std__Out.numRecordsOutPerSecond" },
                { "id": "numBuffersOut" },
                { "id": "Shuffle.Netty.Input.numBuffersInLocal" },
                { "id": "numBuffersInRemotePerSecond" },
                { "id": "numBytesOutPerSecond" },
                { "id": "Window(GlobalWindows()__DeltaTrigger__TimeEvictor__ComparableAggregator__PassThr.numLateRecordsDropped" },
                { "id": "buffers.outputQueueLength" },
                { "id": "numBuffersOutPerSecond" },
                { "id": "Shuffle.Netty.Input.Buffers.inputExclusiveBuffersUsage" },
                { "id": "isBackPressured" },
                { "id": "Window(GlobalWindows()__DeltaTrigger__TimeEvictor__ComparableAggregator__PassThr.numRecordsOut" },
                { "id": "Sink__Print_to_Std__Out.currentInputWatermark" },
                { "id": "numBytesInLocalPerSecond" },
                { "id": "buffers.inPoolUsage" },
                { "id": "idleTimeMsPerSecond" },
                { "id": "Shuffle.Netty.Input.numBytesInLocalPerSecond" },
                { "id": "Sink__Print_to_Std__Out.numRecordsInPerSecond" },
                { "id": "numBytesInRemote" },
                { "id": "Shuffle.Netty.Input.numBytesInLocal" },
                { "id": "Sink__Print_to_Std__Out.currentOutputWatermark" },
                { "id": "Shuffle.Netty.Input.numBytesInRemote" },
                { "id": "busyTimeMsPerSecond" },
                { "id": "Shuffle.Netty.Output.Buffers.outputQueueLength" },
                { "id": "buffers.inputFloatingBuffersUsage" },
                { "id": "currentInputWatermark" },
                { "id": "Window(GlobalWindows()__DeltaTrigger__TimeEvictor__ComparableAggregator__PassThr.numRecordsIn" },
                { "id": "Shuffle.Netty.Input.Buffers.inPoolUsage" },
                { "id": "numBuffersInLocalPerSecond" },
                { "id": "numRecordsOut" },
                { "id": "numBuffersInLocal" },
                { "id": "Sink__Print_to_Std__Out.numRecordsOut" },
                { "id": "numBuffersInRemote" },
                { "id": "buffers.inputQueueLength" },
                { "id": "Sink__Print_to_Std__Out.numRecordsIn" },
                { "id": "Window(GlobalWindows()__DeltaTrigger__TimeEvictor__ComparableAggregator__PassThr.numRecordsOutPerSecond" },
                { "id": "numRecordsIn" },
                { "id": "Shuffle.Netty.Input.numBuffersInRemote" },
                { "id": "numBytesInPerSecond" },
                { "id": "Shuffle.Netty.Input.Buffers.inputQueueLength" },
                { "id": "backPressuredTimeMsPerSecond" },
                { "id": "buffers.inputExclusiveBuffersUsage" },
                { "id": "Shuffle.Netty.Input.numBuffersInRemotePerSecond" },
                { "id": "numRecordsOutPerSecond" },
                { "id": "buffers.outPoolUsage" },
                { "id": "Shuffle.Netty.Input.numBuffersInLocalPerSecond" },
                { "id": "numRecordsInPerSecond" },
                { "id": "Shuffle.Netty.Input.Buffers.inputFloatingBuffersUsage" }
            ]);

            let summary_2_response = ResponseTemplate::new(200).set_body_json(summary_2_json);

            let metrics_2_json = json!([
                { "id": "numRecordsInPerSecond", "max": num_records_in_per_second_2 },
                { "id": "numRecordsOutPerSecond", "max": num_records_out_per_second_2 },
                { "id": "buffers.inputQueueLength", "max": buf_input_queue_len_2 },
                { "id": "buffers.inPoolUsage", "max": buf_input_pool_usage_2 },
                { "id": "buffers.outputQueueLength", "max": buf_output_queue_len_2 },
                { "id": "buffers.outPoolUsage", "max": buf_output_pool_usage_2 },
            ]);

            let metrics_2_response = ResponseTemplate::new(200).set_body_json(metrics_2_json);

            let job_id = JobId::new(job_1.clone());
            let vertex_id = VertexId::new(vertex_1_2.clone());

            let query_path = format!(
                "/jobs/{jid}/vertices/{vid}/subtasks/metrics",
                jid = job_id,
                vid = vertex_id
            );

            Mock::given(method("GET"))
                .and(path(query_path.clone()))
                .and(EmptyQueryParamMatcher)
                .respond_with(summary_2_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            Mock::given(method("GET"))
                .and(path(query_path))
                .and(QueryParamKeyMatcher::new("get"))
                .respond_with(metrics_2_response)
                .expect(1)
                .mount(&mock_server)
                .await;

            let mock_uri = assert_ok!(Url::parse(mock_server.uri().as_str()));
            let job_manager_host = assert_some!(mock_uri.host());
            let job_manager_port = assert_some!(mock_uri.port());

            let settings = FlinkSettings {
                job_manager_uri_scheme: "http".to_string(),
                job_manager_host: job_manager_host.to_string(), //"localhost".to_string(),
                job_manager_port,                               //: 8081,
                metrics_initial_delay: Duration::ZERO,
                metrics_interval: Duration::from_secs(120),
                metric_orders: STD_METRIC_ORDERS.clone(),
                ..FlinkSettings::default()
            };

            let mut gen = assert_ok!(make_flink_metrics_source("test_flink", &settings).await);
            let gen_source = assert_some!(gen.stage.take());
            let tick_api = assert_some!(gen.tx_stop.take());

            let mut sink = proctor::graph::stage::Fold::<_, Telemetry, Vec<Telemetry>>::new(
                "sink",
                Vec::default(),
                |mut acc, item| {
                    acc.push(item);
                    acc
                },
            );
            let rx_acc = assert_some!(sink.take_final_rx());

            (gen_source.outlet(), sink.inlet()).connect().await;
            let mut g = Graph::default();
            g.push_back(gen_source.dyn_upcast()).await;
            g.push_back(Box::new(sink)).await;
            tracing::warn!("RUNNING GRAPH...");
            tokio::spawn(async move {
                assert_ok!(g.run().await);
            });
            tracing::warn!("SLEEP BEFORE STOPPING...");

            tokio::time::sleep(Duration::from_millis(10)).await;
            tracing::warn!("STOPPING...");
            let (stop_cmd, stop_rx) = TickMsg::stop();
            assert_ok!(tick_api.send(stop_cmd));
            assert_ok!(assert_ok!(stop_rx.await));
            tracing::warn!("ASSESSING...");

            let actual = assert_some!(assert_ok!(rx_acc.await).pop());

            let expected: HashMap<String, TelemetryValue> = maplit::hashmap! {
                "health.job_uptime_millis".to_string() => uptime.into(),
                "health.job_nr_restarts".to_string() => restarts.into(),
                "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
                "cluster.task_cpu_load".to_string() => cpu_load.into(),
                "cluster.task_heap_memory_used".to_string() => heap_used.into(),
                "cluster.task_heap_memory_committed".to_string() => heap_committed.into(),
                "cluster.task_nr_threads".to_string() => nr_threads.into(),
                "cluster.nr_task_managers".to_string() => 2.into(),
                "flow.records_in_per_sec".to_string() => max_num_records_in_per_second.into(),
                "flow.records_out_per_sec".to_string() => max_num_records_out_per_second.into(),
                "cluster.task_network_input_queue_len".to_string() => max_buf_input_queue_len.into(),
                "cluster.task_network_input_pool_usage".to_string() => max_buf_input_pool_usage.into(),
                "cluster.task_network_output_queue_len".to_string() => max_buf_output_queue_len.into(),
                "cluster.task_network_output_pool_usage".to_string() => max_buf_output_pool_usage.into(),
            };

            for (key, expected_v) in expected.iter() {
                match assert_some!(actual.get(key)) {
                    TelemetryValue::Float(actual) => {
                        let ev: f64 = assert_ok!(expected_v.try_into());
                        if !approx::relative_eq!(*actual, ev) {
                            assert_eq!(*actual, ev, "metric: {}", key);
                        }
                    }
                    actual => {
                        assert_eq!((key, actual), (key, expected_v));
                    }
                };
                // assert_eq!((key, actual_v), (key, expected_v));
            }

            use std::collections::BTreeSet;
            let expected_keys: BTreeSet<&String> = expected.keys().collect();
            let actual_keys: BTreeSet<&String> = actual.keys().collect();
            assert_eq!(actual_keys, expected_keys);
        })
    }
}
