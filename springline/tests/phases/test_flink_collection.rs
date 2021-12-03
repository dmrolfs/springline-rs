use claim::*;
use fake::{Fake, Faker};
use pretty_assertions::assert_eq;
use serde_json::json;
use springline::phases::collection::flink_metrics::{FlinkMetric, FlinkMetricResponse};
use springline::settings::Aggregation;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_flink_simple_jobs_collect() {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_simple_jobs_collect");
    let _ = main_span.enter();

    let mock_server = MockServer::start().await;

    let uptime = Faker.fake::<chrono::Duration>().num_milliseconds();
    let lag: i64 = Faker.fake();
    let cpu: f64 = Faker.fake();

    let b = json!([
        {
            "id": "uptime",
            "max": uptime,
        },
        {
            "id": "records-lag-max",
            "value": lag,
        },
        {
            "id": "Status.JVM.Memory.Heap.Committed",
            "max": cpu,
        }
    ]);

    let metric_response = ResponseTemplate::new(200).set_body_json(b);

    Mock::given(method("GET"))
        .and(path("/jobs/metrics"))
        .respond_with(metric_response)
        .mount(&mock_server)
        .await;

    let actual = assert_ok!(reqwest::get(format!("{}/jobs/metrics", &mock_server.uri())).await);
    let status = actual.status();
    assert_eq!(status, 200);
    let body: FlinkMetricResponse = assert_ok!(actual.json().await);
    assert_eq!(
        body,
        FlinkMetricResponse(vec![
            FlinkMetric {
                id: "uptime".to_string(),
                values: maplit::hashmap! {Aggregation::Max => uptime.into()},
            },
            FlinkMetric {
                id: "records-lag-max".to_string(),
                values: maplit::hashmap! {Aggregation::None => lag.into()},
            },
            FlinkMetric {
                id: "Status.JVM.Memory.Heap.Committed".to_string(),
                values: maplit::hashmap! {Aggregation::Max => cpu.into()},
            },
        ]) /* format!(
            *     r##"[{{"id":"uptime","max":{uptime}}},{{"id":"records-lag-max","value":{lag}}},{{"id":"Status.JVM.
            * Memory.Heap.Committed","max":{cpu}}}]"##,     uptime= uptime, lag= lag, cpu= cpu
            * ) */
    );

    let status = assert_ok!(reqwest::get(format!("{}/missing", &mock_server.uri())).await).status();
    assert_eq!(status, 404);
}
