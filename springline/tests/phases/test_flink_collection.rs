use claim::*;
use fake::{Fake, Faker};
use pretty_assertions::assert_eq;
use proctor::elements::{Telemetry, TelemetryType};
use reqwest::header::HeaderMap;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use serde_json::json;
use url::Url;
use springline::settings::{Aggregation, MetricOrder, FlinkScope};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use springline::phases::collection::flink_metrics::TaskContext;

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

    let orders = [
        MetricOrder {
            scope: FlinkScope::Jobs,
            metric: "uptime".to_string(),
            agg: Aggregation::Max,
            telemetry_path: "health.job_uptime_millis".to_string(),
            telemetry_type: TelemetryType::Integer
        },
        MetricOrder {
            scope: FlinkScope::Jobs,
            metric: "numRestarts".to_string(),
            agg: Aggregation::Max,
            telemetry_path: "health.job_nr_restarts".to_string(),
            telemetry_type: TelemetryType::Integer
        },
        MetricOrder {
            scope: FlinkScope::Jobs,
            metric: "numberOfFailedCheckpoints".to_string(),
            agg: Aggregation::Max,
            telemetry_path: "health.job_nr_failed_checkpoints".to_string(),
            telemetry_type: TelemetryType::Integer
        },
    ];

    let orders = &springline::phases::collection::flink_metrics::STD_METRIC_ORDERS;
    let client = assert_ok!(reqwest::Client::builder().default_headers(HeaderMap::default()).build());
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
    let url = format!("{}/", &mock_server.uri());
    let context = TaskContext { client, base_url: assert_ok!(Url::parse(url.as_str())), };

    let gen = assert_some!(assert_ok!(springline::phases::collection::flink_metrics::make_jobs_collection_task(
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

    // let actual = assert_ok!(reqwest::get(format!("{}/jobs/metrics", &mock_server.uri())).await);
    // let status = actual.status();
    // assert_eq!(status, 200);
    // let body: FlinkMetricResponse = assert_ok!(actual.json().await);
    // assert_eq!(
    //     body,
    //     FlinkMetricResponse(vec![
    //         FlinkMetric {
    //             id: "uptime".to_string(),
    //             values: maplit::hashmap! {Aggregation::Max => uptime.into()},
    //         },
    //         FlinkMetric {
    //             id: "numRestarts".to_string(),
    //             values: maplit::hashmap! {Aggregation::Max => restarts.into()},
    //         },
    //         FlinkMetric {
    //             id: "numberOfFailedCheckpoints".to_string(),
    //             values: maplit::hashmap! {Aggregation::Max => failed_checkpts.into()},
    //         },
    //     ]) /* format!(
    //         *     r##"[{{"id":"uptime","max":{uptime}}},{{"id":"records-lag-max","value":{lag}}},{{"id":"Status.JVM.
    //         * Memory.Heap.Committed","max":{cpu}}}]"##,     uptime= uptime, lag= lag, cpu= cpu
    //         * ) */
    // );

    let status = assert_ok!(reqwest::get(format!("{}/missing", &mock_server.uri())).await).status();
    assert_eq!(status, 404);
}
