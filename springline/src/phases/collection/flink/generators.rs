use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;

use futures::future::Future;
use itertools::Itertools;
use proctor::elements::{telemetry, Telemetry};
use proctor::error::CollectionError;
use proctor::graph::stage::WithApi;
use proctor::phases::collection::TelemetrySource;
use reqwest::Method;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use tokio::sync::Mutex;
use tracing_futures::Instrument;
use url::Url;
use crate::phases::collection::flink::model::{JobDetail, JobId, VertexId};

use super::model::{build_telemetry, FlinkMetricResponse, JobSummary};
use super::STD_METRIC_ORDERS;
use crate::settings::{Aggregation, FlinkScope, FlinkSettings, MetricOrder};

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
    let client = context.client.clone();
    let scope_rep = scope.to_string().to_lowercase();
    let scopes = maplit::hashset! { scope };
    let (metric_orders, agg_span) = distill_metric_orders_and_agg(&scopes, orders);
    if metric_orders.is_empty() {
        return Ok(None);
    }

    let mut url = context.base_url.clone();
    url.path_segments_mut()
        .unwrap()
        .push(scope_rep.as_str())
        .push("metrics");
    url.query_pairs_mut()
        .clear()
        .append_pair("get", metric_orders.keys().join(",").as_str())
        .append_pair("agg", agg_span.iter().join(",").as_str());


    let gen: Generator<Telemetry> = Box::new(move || {
        let client = client.clone();
        let orders = metric_orders.clone();
        let url = url.clone();

        Box::pin(
            async move {
                let resp: FlinkMetricResponse = client.request(Method::GET, url).send().await?.json().await?;
                let telemetry = build_telemetry(resp, orders)?;
                std::result::Result::<Telemetry, CollectionError>::Ok(telemetry)
            }
            .instrument(tracing::info_span!("collect Flink scope telemetry", %scope_rep)),
        )
    });

    Ok(Some(gen))
}


pub fn make_taskmanagers_admin_generator(context: &TaskContext) -> Result<Option<Generator<Telemetry>>, CollectionError> {
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

// pub fn make_vertex_collection_generator(
//     orders: &[MetricOrder], context: &TaskContext,
// ) -> Result<Generator<Telemetry>, CollectionError> {
//     let (metric_orders, agg_span) = distill_metric_orders_and_agg(orders);
//
//     let gen = Box::new(move || {
//         let context = context.clone();
//
//         Box::pin(
//             async move {
//                 let active_jobs = query_active_jobs(&context).await?;
//                 let job_details: Vec<JobDetail> = active_jobs.into_iter()
//                     .map(|j| {
//                         let detail = query_job_details(j.id, &context).await?;
//                         Ok(detail)
//                     })
//                     .collect();
//
//                 let criteria: Vec<(JobId, VertexId)> = job_details
//                     .into_iter()
//                     .flat_map(|job| {
//                         job.vertices
//                             .into_iter()
//                             .filter_map(|vertex| {
//                                 if vertex.status.is_active() {
//                                    Some((job.jid, vertex.id))
//                                 } else {
//                                     None
//                                 }
//                             })
//                     })
//                     .collect();
//
//                 let vertices_telemetry= Mutex::new(Telemetry::new());
//
//                 let tasks = criteria
//                     .into_iter()
//                     .map(|(jid, vid)| {
//                         let mut url = context.base_url.clone();
//                         url.path_segments_mut()?
//                             .push("jobs")
//                             .push(jid.into())
//                             .push("vertices")
//                             .push(vid.into())
//                             .push("subtasks")
//                             .push("metrics");
//
//                         context.client
//                             .request(Method::GET, url)
//                             .send()
//                             .await?
//
//                     })
//
//                     for job in job_details {
//                     for vertex in job.vertices.into_iter().filter(|v| v.status.is_active()) {
//                         ()
//                     }
//                 }
//
//                     = job_details
//                     .into_iter()
//                     .fold(
//                         Telemetry::new(),
//                         |acc, job| {
//
//                         });
//             }
//                 .instrument(tracing::info_span!("collect Flink vertices telemetry"))
//         )
//     });
//     // for j in job {
//     //     for v in j.vertices {
//     //         metrics
//     //     }
//     // }
//     todo!()
// }

#[tracing::instrument(level="info", skip(context))]
async fn query_active_jobs(context: &TaskContext) -> Result<Vec<JobSummary>, CollectionError> {
    let mut url = context.base_url.clone();
    url.path_segments_mut().unwrap().push("jobs");

    let jobs: Vec<JobSummary> = context.client
        .request(Method::GET, url)
        .send()
        .await?
        .json()
        .await?;

    let active_jobs: Vec<JobSummary> = jobs
        .into_iter()
        .filter(|j| j.status.is_active())
        .collect();

    Ok(active_jobs)
}

#[tracing::instrument(level="info", skip(context))]
async fn query_job_details(job_id: &str, context: &TaskContext) -> Result<JobDetail, CollectionError> {
    let mut url = context.base_url.clone();
    url.path_segments_mut().unwrap().push("jobs").push(job_id);

    let detail: JobDetail = context.client
        .request(Method::GET, url)
        .send()
        .await?
        .json()
        .await?;

    Ok(detail)
}

#[tracing::instrument(level="info", skip(orders))]
async fn query_vertex_telemetry(
    scope: FlinkScope,
    orders: &[MetricOrder],
    job_id: &str,
    vertex_id: &str
) -> Result<Telemetry, CollectionError> {
    todo!()
}

// fn make_active_jobs_generator(context: &TaskContext) -> Result<Generator<Vec<JobSummary>>, CollectionError> {
//     let client = context.client.clone();
//     let mut jobs_url = context.base_url.clone();
//     jobs_url.path_segments_mut()?.push("jobs");
//
//     let gen: Generator<Vec<JobSummary>> = Box::new(move || {
//         let client = client.clone();
//         let url = url.clone();
//
//         Box::pin(
//             async move {
//                 let jobs: Vec<JobSummary> = client
//                     .request(Method::GET, url)
//                     .send()
//                     .await?
//                     .json()
//                     .await?;
//
//                 let active_jobs: Vec<JobSummary> = jobs
//                     .into_iter()
//                     .filter(|j| j.status.is_active())
//                     .collect();
//
//                 Ok(active_jobs)
//             }
//                 .instrument(tracing::info_span!("collect Flink active jobs"))
//         )
//     });
//
//     Ok(gen)
// }


// fn make_jobs_generator(context: &TaskContext) -> impl Fn() -> serde_json::Value {
// let foo = move || {
//     let client = context.client.clone();
//     let mut url = context.base_url.clone();
//     url.path_segments_mut().unwrap().push("jobs");
//
//     async move {
//         let resp: serde_json::Value = client.request(Method::GET,
// url).send().await?.json().await?;         let job_ids = if let Some(jobs) =
// resp["jobs"].as_array() {           // let foo = jobs.iter().map(|j| )
//         } else {
//             // Vec::default()
//         };
//     }
// };
// todo!()
// }

/// Distills orders to requested scope and reorganizes them (order has metric+agg) to metric and
/// consolidates aggregation span.
fn distill_metric_orders_and_agg(scopes: &HashSet<FlinkScope>, orders: &[MetricOrder]) -> (HashMap<String, Vec<MetricOrder>>, HashSet<Aggregation>) {
    let mut order_domain = HashMap::default();
    let mut agg_span = HashSet::default();

    for o in orders.into_iter().filter(|o| scopes.contains(&o.scope)) {
        agg_span.insert(o.agg);
        let entry = order_domain.entry(o.metric.clone()).or_insert(Vec::new());
        entry.push(o.clone());
    }

    (order_domain, agg_span)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use claim::*;
    use fake::{Fake, Faker};
    use pretty_assertions::assert_eq;
    use reqwest::header::HeaderMap;
    use serde_json::json;
    use tokio_test::block_on;
    use trim_margin::MarginTrimmable;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    use super::*;


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

            let client = assert_ok!(reqwest::Client::builder().default_headers(HeaderMap::default()).build());
            let retry_policy = ExponentialBackoff::builder().build_with_total_retry_duration(Duration::from_secs(2));
            let client = ClientBuilder::new(client)
                .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                .build();
            let url = format!("{}/", &mock_server.uri());
            let context = TaskContext {
                client,
                base_url: assert_ok!(Url::parse(url.as_str())),
            };

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

            let client = assert_ok!(reqwest::Client::builder().default_headers(HeaderMap::default()).build());
            let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
            let client = ClientBuilder::new(client)
                .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                .build();
            let url = format!("{}/", &mock_server.uri());
            let context = TaskContext {
                client,
                base_url: assert_ok!(Url::parse(url.as_str())),
            };

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

            let client = assert_ok!(reqwest::Client::builder().default_headers(HeaderMap::default()).build());
            let retry_policy = ExponentialBackoff::builder().build_with_total_retry_duration(Duration::from_secs(2));
            let client = ClientBuilder::new(client)
                .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                .build();
            let url = format!("{}/", &mock_server.uri());
            let context = TaskContext {
                client,
                base_url: assert_ok!(Url::parse(url.as_str())),
            };

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

            let client = assert_ok!(reqwest::Client::builder().default_headers(HeaderMap::default()).build());
            let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
            let client = ClientBuilder::new(client)
                .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                .build();
            let url = format!("{}/", &mock_server.uri());
            let context = TaskContext {
                client,
                base_url: assert_ok!(Url::parse(url.as_str())),
            };

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

            let client = assert_ok!(reqwest::Client::builder().default_headers(HeaderMap::default()).build());
            let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
            let client = ClientBuilder::new(client)
                .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                .build();
            let url = format!("{}/", &mock_server.uri());
            let context = TaskContext {
                client,
                // base_url: assert_ok!(Url::parse("http://localhost:8081/")),
                base_url: assert_ok!(Url::parse(url.as_str())),
            };

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
}
