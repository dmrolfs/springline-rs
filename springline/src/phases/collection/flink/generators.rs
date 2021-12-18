use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;

use futures::future::Future;
use itertools::Itertools;
use proctor::elements::{telemetry, Telemetry, TelemetryValue};
use proctor::error::{CollectionError, TelemetryError};
use proctor::graph::stage::WithApi;
use proctor::phases::collection::TelemetrySource;
use reqwest::Method;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use tracing_futures::Instrument;
use url::Url;

use super::api_model::{build_telemetry, FlinkMetricResponse, JobSummary};
use super::{Aggregation, FlinkScope, MetricOrder, STD_METRIC_ORDERS};
use crate::phases::collection::flink::api_model::{JobDetail, JobId, VertexId};
use crate::settings::FlinkSettings;

pub type Generator<T> = Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<T, CollectionError>>>>>;

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
    // scheduler
    let tick = proctor::graph::stage::Tick::new(
        format!("flink_source_{}_tick", name.as_ref()),
        settings.metrics_initial_delay,
        settings.metrics_interval,
        (),
    );
    let tx_tick_api = tick.tx_api();

    // flink metric rest generator
    let headers = settings.header_map()?;
    let client = reqwest::Client::builder().default_headers(headers).build()?;
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(settings.max_retries);
    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
    let context = TaskContext { client, base_url: settings.base_url()? };

    let mut orders = STD_METRIC_ORDERS.clone();
    orders.extend(settings.metric_orders.clone());

    // let scope_orders = MetricOrder::organize_by_scope(&orders);
    // let mut foo = Vec::default();
    // for (scope, scope_orders) in MetricOrder::organize_by_scope(&orders).into_iter() {
    //     match scope {
    //         FlinkScope::Jobs => {
    //             let task = make_root_scope_collection_generator(FlinkScope::Jobs, &scope_orders,
    // context.clone());             foo.push(Box::new(task));
    //         },
    //         _ => unimplemented!(),
    //     }
    // }

    todo!()
}

pub fn make_root_scope_collection_generator(
    scope: FlinkScope, orders: &[MetricOrder], context: &TaskContext,
) -> Result<Option<Generator<Telemetry>>, CollectionError> {
    let scope_rep = scope.to_string().to_lowercase();
    let scopes = maplit::hashset! { scope };
    let (metric_orders, agg_span) = distill_metric_orders_and_agg(&scopes, orders);
    if metric_orders.is_empty() {
        return Ok(None);
    }

    let mut url = context.base_url.clone();
    url.path_segments_mut().unwrap().push(scope_rep.as_str()).push("metrics");
    url.query_pairs_mut()
        .clear()
        .append_pair("get", metric_orders.keys().join(",").as_str())
        .append_pair("agg", agg_span.iter().join(",").as_str());


    let client = context.client.clone();

    let gen: Generator<Telemetry> = Box::new(move || {
        let client = client.clone();
        let orders = metric_orders.clone();
        let url = url.clone();

        Box::pin(
            async move {
                let resp: FlinkMetricResponse = client.request(Method::GET, url).send().await?.json().await?;
                let telemetry = build_telemetry(resp, &orders)?;
                std::result::Result::<Telemetry, CollectionError>::Ok(telemetry)
            }
            .instrument(tracing::info_span!("collect Flink scope telemetry", %scope_rep)),
        )
    });

    Ok(Some(gen))
}


pub fn make_taskmanagers_admin_generator(
    context: &TaskContext,
) -> Result<Option<Generator<Telemetry>>, CollectionError> {
    let client = context.client.clone();

    let mut url = context.base_url.clone();
    url.path_segments_mut().unwrap().push("taskmanagers");

    let gen: Generator<Telemetry> = Box::new(move || {
        let client = client.clone();
        let url = url.clone();

        Box::pin(
            async move {
                let resp: serde_json::Value = client.request(Method::GET, url).send().await?.json().await?;
                let taskmanagers = resp["taskmanagers"].as_array().map(|tms| tms.len()).unwrap_or(0);
                let mut telemetry: telemetry::TableType = HashMap::default();
                telemetry.insert("cluster.nr_task_managers".to_string(), taskmanagers.into());
                Ok(telemetry.into())
            }
            .instrument(tracing::info_span!("collect Flink taskmanagers telemetry",)),
        )
    });
    Ok(Some(gen))
}

pub fn make_vertex_collection_generator(
    orders: &[MetricOrder], context: &TaskContext,
) -> Result<Option<Generator<Telemetry>>, CollectionError> {
    let scopes = maplit::hashset! {
        FlinkScope::Task, FlinkScope::Kafka, FlinkScope::Kinesis,
    };

    let orders = orders.to_vec();
    let (metric_orders, agg_span) = distill_metric_orders_and_agg(&scopes, &orders);
    if metric_orders.is_empty() {
        return Ok(None);
    }

    let context = context.clone();

    let gen: Generator<Telemetry> = Box::new(move || {
        let context = context.clone();
        let orders = orders.clone();
        let metric_orders = metric_orders.clone();
        let agg_span = agg_span.clone();

        Box::pin(
            async move {
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
                            query_vertex_telemetry(&job.jid, &vertex.id, &metric_orders, &agg_span, &context).await?;

                        collect_metric_points(&mut metric_points, vertex_telemetry);
                    }
                }

                let telemetry = merge_telemetry_per_order(metric_points, &orders)?;
                std::result::Result::<Telemetry, CollectionError>::Ok(telemetry)
            }
            .instrument(tracing::info_span!("collect Flink vertices telemetry")),
        )
    });

    Ok(Some(gen))
}

fn collect_metric_points(metric_points: &mut HashMap<String, Vec<TelemetryValue>>, vertex_telemetry: Telemetry) {
    for (metric, vertex_val) in vertex_telemetry.into_iter() {
        metric_points.entry(metric).or_insert_with(Vec::default).push(vertex_val);
    }
}

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
            |(metric, values)| match telemetry_agg.get(metric.as_str()).map(|agg| agg.combinator()) {
                None => Ok(Some((metric, TelemetryValue::Seq(values)))),
                Some(combo) => combo.combine(values).map(|combined| combined.map(|c| (metric, c))),
            },
        )
        .collect::<Result<Vec<_>, TelemetryError>>()?
        .into_iter()
        .flatten()
        .collect::<telemetry::TableType>()
        .into();

    Ok(telemetry)

    // for (metric, values) in metric_points.into_iter() {
    //
    //     acc
    //         .entry(vertex_key)
    //         .and_modify(|acc_val| {
    //             match telemetry_agg.get(&vertex_key) {
    //                 None => (),
    //                 Some(o) => {
    //                     let combine = |lhs: &TelemetryValue, rhs: &TelemetryValue| {
    // o.combine(vec![lhs, rhs])}                     o.combine(vec![acc_val, vertex_val])
    //                 },
    //             }
    //         })
    // }
    // todo!()
}

#[tracing::instrument(level = "info", skip(context))]
async fn query_active_jobs(context: &TaskContext) -> Result<Vec<JobSummary>, CollectionError> {
    let mut url = context.base_url.clone();
    url.path_segments_mut().unwrap().push("jobs");

    let resp: serde_json::Value = context.client.request(Method::GET, url).send().await?.json().await?;

    let jobs: Vec<JobSummary> = match resp.get("jobs") {
        None => Vec::default(),
        Some(js) => serde_json::from_value(js.clone())?,
    };

    let active_jobs = jobs.into_iter().filter(|j| j.status.is_active()).collect();

    Ok(active_jobs)
}

#[tracing::instrument(level = "info", skip(context))]
async fn query_job_details(job_id: &str, context: &TaskContext) -> Result<JobDetail, CollectionError> {
    let mut url = context.base_url.clone();
    url.path_segments_mut().unwrap().push("jobs").push(job_id);

    let detail: JobDetail = context.client.request(Method::GET, url).send().await?.json().await?;

    Ok(detail)
}

#[tracing::instrument(level = "info", skip(metric_orders, agg_span, context))]
async fn query_vertex_telemetry(
    job_id: &JobId, vertex_id: &VertexId, metric_orders: &HashMap<String, Vec<MetricOrder>>,
    agg_span: &HashSet<Aggregation>, context: &TaskContext,
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

    let avail_resp: FlinkMetricResponse = context.client.request(Method::GET, url).send().await?.json().await?;

    let target_metrics: HashSet<String> = avail_resp
        .into_iter()
        .filter_map(|m| metric_orders.get(&m.id).and(Some(m.id)))
        .collect();

    url2.query_pairs_mut()
        .clear()
        .append_pair("get", target_metrics.into_iter().join(",").as_str())
        .append_pair("agg", agg_span.iter().join(",").as_str());

    let resp: FlinkMetricResponse = context.client.request(Method::GET, url2).send().await?.json().await?;

    let telemetry = build_telemetry(resp, metric_orders)?;
    Ok(telemetry)
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

    use claim::*;
    use fake::{Fake, Faker};
    use pretty_assertions::assert_eq;
    use proctor::elements::{TelemetryType, Timestamp};
    use reqwest::header::HeaderMap;
    use serde_json::json;
    use tokio_test::block_on;
    use trim_margin::MarginTrimmable;
    use wiremock::matchers::{method, path, query_param};
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

            let gen = assert_some!(assert_ok!(make_root_scope_collection_generator(
                FlinkScope::Jobs,
                &STD_METRIC_ORDERS,
                &context
            )));

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

            let gen = assert_some!(assert_ok!(make_root_scope_collection_generator(
                FlinkScope::Jobs,
                &STD_METRIC_ORDERS,
                &context
            )));

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

            let metric_response = RetryResponder::new(1, 500, ResponseTemplate::new(200).set_body_json(b));

            Mock::given(method("GET"))
                .and(path("/jobs/metrics"))
                .respond_with(metric_response)
                .expect(3)
                .mount(&mock_server)
                .await;

            let context = assert_ok!(context_for(&mock_server));

            let gen = assert_some!(assert_ok!(make_root_scope_collection_generator(
                FlinkScope::Jobs,
                &STD_METRIC_ORDERS,
                &context
            )));

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

            let gen = assert_some!(assert_ok!(make_root_scope_collection_generator(
                FlinkScope::TaskManagers,
                &STD_METRIC_ORDERS,
                &context
            )));

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

            let gen = assert_some!(assert_ok!(make_taskmanagers_admin_generator(&context)));

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
                { "id": "Shuffle.Netty.Input.Buffers.inputFloatingBuffersUsage" }
            ] );

            let summary_response = ResponseTemplate::new(200).set_body_json(summary);

            let metrics = json!([
                { "id": "numRecordsInPerSecond", "max": 0.0 },
                { "id": "numRecordsOutPerSecond", "max": 20.0 },
                { "id": "buffers.inputQueueLength", "max": 0.0 },
                { "id": "buffers.inPoolUsage", "max": 0.0 },
                { "id": "buffers.outputQueueLength", "max": 1.0 },
                { "id": "buffers.outPoolUsage", "max": 0.1 }
            ] );

            let metrics_response = ResponseTemplate::new(200).set_body_json(metrics);

            let job_id = JobId::new("a97b6344d775aafe03e55a8e812d2713");
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

            let actual =
                assert_ok!(query_vertex_telemetry(&job_id, &vertex_id, &metric_orders, &agg_span, &context).await);

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
}
