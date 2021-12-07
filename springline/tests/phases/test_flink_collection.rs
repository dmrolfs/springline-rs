use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use claim::*;
use fake::{Fake, Faker};
use pretty_assertions::assert_eq;
use proctor::elements::Telemetry;
use reqwest::header::HeaderMap;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use serde_json::json;
use url::Url;
use springline::settings::FlinkScope;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};
use springline::phases::collection::flink_metrics::TaskContext;

pub struct RetryResponder(Arc<AtomicU32>, u32, ResponseTemplate, u16);

impl RetryResponder {
    fn new(retries: u32, fail_status_code: u16, success_template: ResponseTemplate) -> Self {
        Self(Arc::new(AtomicU32::new(0)), retries, success_template, fail_status_code)
    }
}

impl Respond for RetryResponder {
    #[tracing::instrument(level="info", skip(self),)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_flink_collect_failure() {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_simple_jobs_collect");
    let _ = main_span.enter();

    let mock_server = MockServer::start().await;
    let metric_response = ResponseTemplate::new(500);
    Mock::given(method("GET"))
        .and(path("/jobs/metrics"))
        .respond_with(metric_response)
        .expect(3)
        .mount(&mock_server)
        .await;

    let orders = &springline::phases::collection::flink_metrics::STD_METRIC_ORDERS;
    let client = assert_ok!(reqwest::Client::builder().default_headers(HeaderMap::default()).build());
    let retry_policy = ExponentialBackoff::builder()
        .build_with_total_retry_duration(Duration::from_secs(2));
    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
    let url = format!("{}/", &mock_server.uri());
    let context = TaskContext { client, base_url: assert_ok!(Url::parse(url.as_str())), };

    let gen = assert_some!(assert_ok!(springline::phases::collection::flink_metrics::make_root_scope_collection_task(
        FlinkScope::Jobs,
        &orders,
        context
    )));

    assert_err!(gen().await, "Checking for error passed back after retries.");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_flink_simple_jobs_collect() {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_simple_jobs_collect");
    let _ = main_span.enter();

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

    // let orders = [
    //     MetricOrder {
    //         scope: FlinkScope::Jobs,
    //         metric: "uptime".to_string(),
    //         agg: Aggregation::Max,
    //         telemetry_path: "health.job_uptime_millis".to_string(),
    //         telemetry_type: TelemetryType::Integer
    //     },
    //     MetricOrder {
    //         scope: FlinkScope::Jobs,
    //         metric: "numRestarts".to_string(),
    //         agg: Aggregation::Max,
    //         telemetry_path: "health.job_nr_restarts".to_string(),
    //         telemetry_type: TelemetryType::Integer
    //     },
    //     MetricOrder {
    //         scope: FlinkScope::Jobs,
    //         metric: "numberOfFailedCheckpoints".to_string(),
    //         agg: Aggregation::Max,
    //         telemetry_path: "health.job_nr_failed_checkpoints".to_string(),
    //         telemetry_type: TelemetryType::Integer
    //     },
    // ];

    let orders = &springline::phases::collection::flink_metrics::STD_METRIC_ORDERS;
    let client = assert_ok!(reqwest::Client::builder().default_headers(HeaderMap::default()).build());
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
    let url = format!("{}/", &mock_server.uri());
    let context = TaskContext { client, base_url: assert_ok!(Url::parse(url.as_str())), };

    let gen = assert_some!(assert_ok!(springline::phases::collection::flink_metrics::make_root_scope_collection_task(
        FlinkScope::Jobs,
        &orders,
        context
    )));

    let actual: Telemetry = assert_ok!(gen().await);
    assert_eq!(
        actual,
        maplit::hashmap! {
            "health.job_uptime_millis".to_string() => uptime.into(),
            "health.job_nr_restarts".to_string() => restarts.into(),
            "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
        }.into()
    );

    let actual: Telemetry = assert_ok!(gen().await);
    assert_eq!(
        actual,
        maplit::hashmap! {
            "health.job_uptime_millis".to_string() => uptime.into(),
            "health.job_nr_restarts".to_string() => restarts.into(),
            "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
        }.into()
    );

    let status = assert_ok!(reqwest::get(format!("{}/missing", &mock_server.uri())).await).status();
    assert_eq!(status, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_flink_retry_jobs_collect() {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_simple_jobs_collect");
    let _ = main_span.enter();

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

    let metric_response = RetryResponder::new(
        1,
        500,
        ResponseTemplate::new(200).set_body_json(b)
    );

    Mock::given(method("GET"))
        .and(path("/jobs/metrics"))
        .respond_with(metric_response)
        .expect(3)
        .mount(&mock_server)
        .await;

    let orders = &springline::phases::collection::flink_metrics::STD_METRIC_ORDERS;
    let client = assert_ok!(reqwest::Client::builder().default_headers(HeaderMap::default()).build());
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
    let url = format!("{}/", &mock_server.uri());
    let context = TaskContext { client, base_url: assert_ok!(Url::parse(url.as_str())), };

    let gen = assert_some!(assert_ok!(springline::phases::collection::flink_metrics::make_root_scope_collection_task(
        FlinkScope::Jobs,
        &orders,
        context
    )));

    let actual: Telemetry = assert_ok!(gen().await);
    assert_eq!(
        actual,
        maplit::hashmap! {
            "health.job_uptime_millis".to_string() => uptime.into(),
            "health.job_nr_restarts".to_string() => restarts.into(),
            "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
        }.into()
    );

    let actual: Telemetry = assert_ok!(gen().await);
    assert_eq!(
        actual,
        maplit::hashmap! {
            "health.job_uptime_millis".to_string() => uptime.into(),
            "health.job_nr_restarts".to_string() => restarts.into(),
            "health.job_nr_failed_checkpoints".to_string() => failed_checkpts.into(),
        }.into()
    );

    let status = assert_ok!(reqwest::get(format!("{}/missing", &mock_server.uri())).await).status();
    assert_eq!(status, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_flink_simple_taskmanagers_collect() {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_simple_taskmanagers_collect");
    let _ = main_span.enter();

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

    let orders = &springline::phases::collection::flink_metrics::STD_METRIC_ORDERS;
    let client = assert_ok!(reqwest::Client::builder().default_headers(HeaderMap::default()).build());
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
    let url = format!("{}/", &mock_server.uri());
    let context = TaskContext { client, base_url: assert_ok!(Url::parse(url.as_str())), };

    let gen = assert_some!(assert_ok!(springline::phases::collection::flink_metrics::make_root_scope_collection_task(
        FlinkScope::TaskManagers,
        &orders,
        context
    )));

    let actual: Telemetry = assert_ok!(gen().await);
    assert_eq!(
        actual,
        maplit::hashmap! {
            "cluster.task_cpu_load".to_string() => cpu_load.into(),
            "cluster.task_heap_memory_used".to_string() => heap_used.into(),
            "cluster.task_heap_memory_committed".to_string() => heap_committed.into(),
            "cluster.task_nr_threads".to_string() => nr_threads.into(),
        }.into()
    );

    let actual: Telemetry = assert_ok!(gen().await);
    assert_eq!(
        actual,
        maplit::hashmap! {
            "cluster.task_cpu_load".to_string() => cpu_load.into(),
            "cluster.task_heap_memory_used".to_string() => heap_used.into(),
            "cluster.task_heap_memory_committed".to_string() => heap_committed.into(),
            "cluster.task_nr_threads".to_string() => nr_threads.into(),
        }.into()
    );

    let status = assert_ok!(reqwest::get(format!("{}/missing", &mock_server.uri())).await).status();
    assert_eq!(status, 404);
}

