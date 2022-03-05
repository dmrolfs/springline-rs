use cast_trait_object::DynCastExt;
use claim::*;
use fake::{Fake, Faker};
use pretty_assertions::assert_eq;
use proctor::elements::{RecordsPerSecond, Telemetry, TelemetryValue};
use proctor::graph::stage::{self, ActorSourceApi, ActorSourceCmd, WithApi};
use proctor::graph::{Connect, Graph, SinkShape};
use proctor::Ack;
use serde_json::json;
use springline::flink::{FlinkContext, JobId, JobState, TaskState, VertexId, JOB_STATES, TASK_STATES};
use springline::phases::sense::flink::{make_sensor, FlinkSensorSpecification, STD_METRIC_ORDERS};
use springline::phases::{MC_CLUSTER__NR_ACTIVE_JOBS, MC_CLUSTER__NR_TASK_MANAGERS, MC_FLOW__RECORDS_IN_PER_SEC};
use springline::settings::{FlinkSensorSettings, FlinkSettings};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Match, Mock, MockServer, Request, ResponseTemplate, Times};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flink_sensor_merge_combine_stage() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_sensor_merge_combine_stage");
    let _ = main_span.enter();

    let mock_server = MockServer::start().await;
    let expected_jobs = MockFlinkJobsMetrics::mount_mock(&mock_server, 1).await;
    tracing::info!("expected_jobs: {:?}", expected_jobs);
    let expected_tm = MockFlinkTaskmanagersMetrics::mount_mock(&mock_server, 1).await;
    tracing::info!("expected_tm: {:?}", expected_tm);
    let expected_tm_admin = MockFlinkTaskmanagerAdminMetrics::mount_mock(&mock_server, 1).await;
    tracing::info!("expected_tm_admin: {:?}", expected_tm_admin);
    let expected_job_states = MockFlinkJobStates::mount_mock(&mock_server, 1).await;
    tracing::info!("expected_job_states: {:?}", expected_job_states);

    let mut expected_job_details = Vec::with_capacity(expected_job_states.job_states.len());
    for (job_id, job_state) in expected_job_states.job_states.iter() {
        expected_job_details
            .push(MockFlinkJobDetail::mount_mock(Some(job_id.clone()), Some(*job_state), &mock_server, 1).await);
    }
    tracing::info!("expected_job_details: {:?}", expected_job_details);

    let mut expected_vertex_metric_summaries = Vec::with_capacity(expected_job_details.len());
    let mut expected_vertex_metrics = Vec::with_capacity(expected_job_details.len());
    for expected in expected_job_details.iter() {
        let job_id = &expected.job_id;
        for (vertex_id, _) in expected.vertex_states.iter().filter(|(_, s)| s.is_active()) {
            let expected_vertex_summary =
                MockFlinkVertexMetricSummary::mount_mock(job_id, vertex_id, &mock_server, 1).await;
            expected_vertex_metric_summaries.push(expected_vertex_summary);
            expected_vertex_metrics.push(MockFlinkVertexMetrics::mount_mock(job_id, vertex_id, &mock_server, 1).await);
        }
    }
    tracing::info!(
        "expected_vertex_metric_summaries: {:?}",
        expected_vertex_metric_summaries
    );
    tracing::info!("expected_vertex_metrics: {:?}", expected_vertex_metrics);

    let mock_uri = assert_ok!(Url::parse(mock_server.uri().as_str()));
    let job_manager_host = assert_some!(mock_uri.host());
    let job_manager_port = assert_some!(mock_uri.port());

    let context = FlinkContext::from_settings(&FlinkSettings {
        job_manager_uri_scheme: "http".to_string(),
        job_manager_host: job_manager_host.to_string(), //"localhost".to_string(),
        job_manager_port,                               //: 8081,
        ..FlinkSettings::default()
    })?;

    let settings = FlinkSensorSettings {
        metrics_initial_delay: Duration::ZERO,
        metrics_interval: Duration::from_secs(120),
        metric_orders: STD_METRIC_ORDERS.clone(),
        ..FlinkSensorSettings::default()
    };

    let scheduler = stage::ActorSource::new("trigger");
    let tx_scheduler_api = scheduler.tx_api();

    let spec = FlinkSensorSpecification {
        name: "test_flink",
        context,
        scheduler: Box::new(scheduler),
        settings: &settings,
        machine_node: pretty_snowflake::MachineNode::default(),
    };

    let flink_sensor = make_sensor(spec).await?;

    let mut sink = stage::Fold::new("sink", Telemetry::default(), |mut acc, item| {
        tracing::info!(?item, ?acc, "PUSHING ITEM INTO ACC...");
        acc.extend(item);
        acc
    });

    let rx_acc = assert_some!(sink.take_final_rx());

    (flink_sensor.outlet(), sink.inlet()).connect().await;
    let mut g = Graph::default();
    g.push_back(flink_sensor.dyn_upcast()).await;
    g.push_back(Box::new(sink)).await;
    let handler = tokio::spawn(async move {
        assert_ok!(g.run().await);
    });

    assert_ok!(trigger(&tx_scheduler_api).await);
    assert_ok!(stop(&tx_scheduler_api).await);
    assert_ok!(handler.await);

    let actual = assert_ok!(rx_acc.await);
    tracing::info!(?actual, "FINAL RESULT FROM SINK");

    let expected = make_expected_telemetry(
        expected_jobs,
        expected_tm,
        expected_tm_admin,
        expected_job_states,
        &expected_job_details,
        &expected_vertex_metric_summaries,
        &expected_vertex_metrics,
    );

    for (key, expected_v) in expected.iter() {
        tracing::info!("ASSERTING TELEMETRY[{key}]: {:?} == {:?}", expected_v, actual.get(key));
        match actual.get(key) {
            Some(TelemetryValue::Float(actual)) => {
                let ev: f64 = assert_ok!(expected_v.try_into());
                if !approx::relative_eq!(*actual, ev) {
                    assert_eq!(*actual, ev, "metric: {}", key);
                }
            },
            Some(actual) => {
                assert_eq!((key, actual), (key, expected_v));
            },
            None => {
                tracing::warn!(?actual, "actual does not have value for key:{}", key);
            },
        };
        // assert_eq!((key, actual_v), (key, expected_v));
    }

    use std::collections::BTreeSet;
    let expected_keys: BTreeSet<&String> = expected.keys().collect();
    let actual_keys: BTreeSet<&String> = actual.keys().collect();
    assert_eq!(actual_keys, expected_keys);

    tracing::info!("mock received requests: {:?}", mock_server.received_requests().await);

    Ok(())
}

async fn trigger(tx: &ActorSourceApi<()>) -> anyhow::Result<Ack> {
    ActorSourceCmd::push(tx, ()).await.map_err(|err| err.into())
}

async fn stop(tx: &ActorSourceApi<()>) -> anyhow::Result<Ack> {
    ActorSourceCmd::stop(tx).await.map_err(|err| err.into())
}

fn make_expected_telemetry(
    expected_jobs: MockFlinkJobsMetrics, expected_tm: MockFlinkTaskmanagersMetrics,
    expected_tm_admin: MockFlinkTaskmanagerAdminMetrics, expected_job_states: MockFlinkJobStates,
    _expected_job_details: &[MockFlinkJobDetail], _expected_vertex_metric_summaries: &[MockFlinkVertexMetricSummary],
    expected_vertex_metrics: &[MockFlinkVertexMetrics],
) -> Telemetry {
    let mut expected: HashMap<String, TelemetryValue> = maplit::hashmap! {
        "health.job_uptime_millis".to_string() => expected_jobs.uptime.into(),
        "health.job_nr_restarts".to_string() => expected_jobs.restarts.into(),
        "health.job_nr_failed_checkpoints".to_string() => expected_jobs.failed_checkpoints.into(),
        "cluster.task_cpu_load".to_string() => expected_tm.cpu_load.into(),
        "cluster.task_heap_memory_used".to_string() => expected_tm.heap_used.into(),
        "cluster.task_heap_memory_committed".to_string() => expected_tm.heap_committed.into(),
        "cluster.task_nr_threads".to_string() => expected_tm.nr_threads.into(),
        MC_CLUSTER__NR_ACTIVE_JOBS.to_string() => expected_job_states.nr_active_jobs().into(),
        MC_CLUSTER__NR_TASK_MANAGERS.to_string() => expected_tm_admin.nr_task_managers.into(),
    };

    let expected_max_records_in_per_sec = expected_vertex_metrics
        .iter()
        .map(|m| m.nr_records_in_per_second)
        .max_by(|lhs, rhs| assert_some!(lhs.partial_cmp(&rhs)));
    if let Some(expected_value) = expected_max_records_in_per_sec {
        expected.insert(MC_FLOW__RECORDS_IN_PER_SEC.to_string(), expected_value.into());
    }

    let expected_max_records_out_per_sec = expected_vertex_metrics
        .iter()
        .map(|m| m.nr_records_out_per_second)
        .max_by(|lhs, rhs| assert_some!(lhs.partial_cmp(&rhs)));
    if let Some(expected_value) = expected_max_records_out_per_sec {
        expected.insert("flow.records_out_per_sec".to_string(), expected_value.into());
    }

    let expected_max_input_queue_len =
        expected_vertex_metrics
            .iter()
            .map(|m| m.input_buffer_queue_len)
            .max_by(|lhs, rhs| {
                tracing::info!(
                    ?lhs,
                    ?rhs,
                    "MAX[task_network_input_queue_len]: lhs max rhs = {:?}",
                    lhs.partial_cmp(&rhs)
                );
                assert_some!(lhs.partial_cmp(&rhs))
            });
    if let Some(expected_value) = expected_max_input_queue_len {
        tracing::info!(%expected_value, "MAX:cluster.task_network_input_queue_len");
        expected.insert(
            "cluster.task_network_input_queue_len".to_string(),
            expected_value.into(),
        );
    }

    let expected_max_in_pool_usage =
        expected_vertex_metrics
            .iter()
            .map(|m| m.in_buffer_pool_usage)
            .max_by(|lhs, rhs| {
                tracing::info!(
                    ?lhs,
                    ?rhs,
                    "MAX[task_network_input_pool_usage]: lhs max rhs = {:?}",
                    lhs.partial_cmp(&rhs)
                );
                assert_some!(lhs.partial_cmp(&rhs))
            });
    if let Some(expected_value) = expected_max_in_pool_usage {
        tracing::info!(%expected_value, "MAX:cluster.task_network_input_pool_usage");
        expected.insert(
            "cluster.task_network_input_pool_usage".to_string(),
            expected_value.into(),
        );
    }

    let expected_max_output_queue_len =
        expected_vertex_metrics
            .iter()
            .map(|m| m.output_buffer_queue_len)
            .max_by(|lhs, rhs| {
                tracing::info!(
                    ?lhs,
                    ?rhs,
                    "MAX[task_network_output_queue_len]: lhs max rhs = {:?}",
                    lhs.partial_cmp(&rhs)
                );
                assert_some!(lhs.partial_cmp(&rhs))
            });
    if let Some(expected_value) = expected_max_output_queue_len {
        tracing::info!(%expected_value, "MAX:cluster.task_network_output_queue_len");
        expected.insert(
            "cluster.task_network_output_queue_len".to_string(),
            expected_value.into(),
        );
    }

    let expected_max_out_pool_usage =
        expected_vertex_metrics
            .iter()
            .map(|m| m.out_buffer_pool_usage)
            .max_by(|lhs, rhs| {
                tracing::info!(
                    ?lhs,
                    ?rhs,
                    "MAX[task_network_output_pool_usage]: lhs max rhs = {:?}",
                    lhs.partial_cmp(&rhs)
                );
                assert_some!(lhs.partial_cmp(&rhs))
            });
    if let Some(expected_value) = expected_max_out_pool_usage {
        tracing::info!(%expected_value, "MAX:cluster.task_network_output_pool_usage");
        expected.insert(
            "cluster.task_network_output_pool_usage".to_string(),
            expected_value.into(),
        );
    }

    expected.into()
}

#[derive(Debug)]
struct MockFlinkJobsMetrics {
    pub uptime: i64,
    pub restarts: i64,
    pub failed_checkpoints: i64,
}

impl MockFlinkJobsMetrics {
    async fn mount_mock(server: &MockServer, expect: impl Into<Times>) -> Self {
        let uptime = i64::abs(Faker.fake::<chrono::Duration>().num_milliseconds());
        let restarts = (0..99).fake();
        let failed_checkpoints = (0..99).fake();
        let body = json!([
            { "id": "uptime", "max": uptime as f64, },
            { "id": "numRestarts", "max": restarts as f64, },
            { "id": "numberOfFailedCheckpoints", "max": failed_checkpoints as f64, },
        ]);
        let response = ResponseTemplate::new(200).set_body_json(body);
        Mock::given(method("GET"))
            .and(path("/jobs/metrics"))
            .respond_with(response)
            .expect(expect)
            .mount(&server)
            .await;

        Self { uptime, restarts, failed_checkpoints }
    }
}

#[derive(Debug)]
struct MockFlinkTaskmanagersMetrics {
    pub cpu_load: f64,
    pub heap_used: f64,
    pub heap_committed: f64,
    pub nr_threads: i32,
}

impl MockFlinkTaskmanagersMetrics {
    async fn mount_mock(server: &MockServer, expect: impl Into<Times>) -> Self {
        let cpu_load = 16. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
        let heap_used = 1_000_000_000. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
        let heap_committed = 1_000_000_000. * ((0..100_000_000).fake::<i64>() as f64 / 100_000_000.);
        let nr_threads = (1..150).fake();
        let body = json!([
            { "id": "Status.JVM.CPU.Load", "max":  cpu_load,},
            { "id": "Status.JVM.Memory.Heap.Used", "max":  heap_used,},
            { "id": "Status.JVM.Memory.Heap.Committed", "max":  heap_committed,},
            { "id": "Status.JVM.Threads.Count", "max":  nr_threads as f64,},
        ]);
        let response = ResponseTemplate::new(200).set_body_json(body);
        Mock::given(method("GET"))
            .and(path("/taskmanagers/metrics"))
            .respond_with(response)
            .expect(expect)
            .mount(&server)
            .await;

        Self { cpu_load, heap_used, heap_committed, nr_threads }
    }
}

#[derive(Debug)]
struct MockFlinkTaskmanagerAdminMetrics {
    pub nr_task_managers: usize,
}

impl MockFlinkTaskmanagerAdminMetrics {
    async fn mount_mock(server: &MockServer, expect: impl Into<Times>) -> Self {
        let nr_task_managers = (0..9).fake();
        tracing::info!("nr_task_managers: {nr_task_managers}");
        let tms_bodies = (0..nr_task_managers)
            .into_iter()
            .map(Self::generate_task_manager_body)
            .collect();
        let body = json!({ "taskmanagers": serde_json::Value::Array(tms_bodies) });
        let response = ResponseTemplate::new(200).set_body_json(body);
        Mock::given(method("GET"))
            .and(path("/taskmanagers"))
            .respond_with(response)
            .expect(expect)
            .mount(&server)
            .await;

        Self { nr_task_managers }
    }

    fn generate_task_manager_body(idx: usize) -> serde_json::Value {
        json!({
            "id": format!("100.97.247.74:43435-69a{}", 700 + idx),
            "path": format!("akka.tcp://flink@100.97.247.74:43435/user/rpc/taskmanager_{}", idx),
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
        })
    }
}

#[derive(Debug)]
struct MockFlinkJobStates {
    pub job_states: HashMap<JobId, JobState>,
}

impl MockFlinkJobStates {
    pub async fn mount_mock(server: &MockServer, expect: impl Into<Times>) -> Self {
        let nr_jobs = (0..9).fake();
        let mut job_states = HashMap::with_capacity(nr_jobs);
        let mut job_state_bodies = Vec::with_capacity(nr_jobs);
        for _ in 0..nr_jobs {
            let (jid, state, body) = Self::generate_active_job_body();
            job_states.insert(jid, state);
            job_state_bodies.push(body);
        }
        tracing::info!(%nr_jobs, "job_states: {:?}", job_states);

        let body = json!({ "jobs": serde_json::Value::Array(job_state_bodies) });
        let response = ResponseTemplate::new(200).set_body_json(body);
        Mock::given(method("GET"))
            .and(path("/jobs"))
            .respond_with(response)
            .expect(expect)
            .mount(&server)
            .await;

        Self { job_states }
    }

    pub fn active_jobs(&self) -> Vec<JobId> {
        self.job_states
            .iter()
            .filter_map(|(id, s)| if s.is_active() { Some(id.clone()) } else { None })
            .collect()
    }

    pub fn nr_active_jobs(&self) -> usize {
        self.active_jobs().iter().count()
    }

    fn generate_active_job_body() -> (JobId, JobState, serde_json::Value) {
        let jid = Faker.fake::<String>().into();
        let state_sel: usize = (0..JOB_STATES.len()).fake();
        let state = JOB_STATES[state_sel];
        let body = json!({ "id": jid, "status": state });
        (jid, state, body)
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct MockFlinkJobDetail {
    pub job_id: JobId,
    pub job_state: JobState,
    pub vertex_states: HashMap<VertexId, TaskState>,
}

impl MockFlinkJobDetail {
    pub async fn mount_mock(
        job_id: Option<JobId>, job_state: Option<JobState>, server: &MockServer, expect: impl Into<Times>,
    ) -> Self {
        let job_id = job_id.unwrap_or_else(|| Faker.fake::<String>().into());
        let job_state = job_state.unwrap_or_else(|| {
            let sel: usize = (0..JOB_STATES.len()).fake();
            JOB_STATES[sel]
        });

        let vertex_states = if job_state.is_active() {
            let nr_vertices = (0..9).fake();
            Self::do_register_mock(&job_id, job_state, nr_vertices, server, expect).await
        } else {
            HashMap::default()
        };
        tracing::info!(nr_vertices=%vertex_states.len(), "vertices: {:?}", vertex_states);

        Self { job_id, job_state, vertex_states }
    }

    async fn do_register_mock(
        job_id: &JobId, job_state: JobState, nr_vertices: usize, server: &MockServer, expect: impl Into<Times>,
    ) -> HashMap<VertexId, TaskState> {
        let mut vertex_states = HashMap::with_capacity(nr_vertices);
        let mut vertex_summaries = Vec::with_capacity(nr_vertices);
        let mut vertex_plans = Vec::with_capacity(nr_vertices);
        for _ in 0..nr_vertices {
            let (id, task_state, summary, plan) = Self::generate_vertex_summary();
            vertex_states.insert(id, task_state);
            vertex_summaries.push(summary);
            vertex_plans.push(plan);
        }

        let duration: u64 = (0..1_000_000).fake();
        let now = assert_ok!(std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)).as_secs();
        let start_time = now - duration;

        let body = json!({
            "jid": job_id,
            "name": Faker.fake::<String>(),
            "state": job_state,
            "isStoppable": Faker.fake::<bool>(),
            "start-time": start_time,
            "end-time": -1,
            "duration": duration,
            "maxParallelism": (-1..99).fake::<isize>(),
            "now": now,
            "timestamps": {
                "CREATED": start_time,
                "FAILED": 0,
                "RUNNING": duration,
                "CANCELED": 0,
                "CANCELLING": 0,
                "SUSPENDED": 0,
                "FAILING": 0,
                "RESTARTING": 0,
                "FINISHED": 0,
                "INITIALIZING": start_time,
                "RECONCILING": 0
            },
            "vertices": serde_json::Value::Array(vertex_summaries),
            "status-counts": {
                    "SCHEDULED": (0..10).fake::<usize>(),
                    "FINISHED": (0..10).fake::<usize>(),
                    "FAILED": (0..10).fake::<usize>(),
                    "CANCELING": (0..10).fake::<usize>(),
                    "CANCELED": (0..10).fake::<usize>(),
                    "DEPLOYING": (0..10).fake::<usize>(),
                    "RECONCILING": (0..10).fake::<usize>(),
                    "CREATED": (0..10).fake::<usize>(),
                    "RUNNING": (0..10).fake::<usize>(),
                    "INITIALIZING": (0..10).fake::<usize>()
            },
            "plan": {
                "jid": job_id,
                "name": Faker.fake::<String>(),
                "nodes": serde_json::Value::Array(vertex_plans)
            }
        });

        let response = ResponseTemplate::new(200).set_body_json(body);

        Mock::given(method("GET"))
            .and(path(format!("/jobs/{job_id}")))
            .respond_with(response)
            .expect(expect)
            .mount(&server)
            .await;

        vertex_states
    }

    fn generate_vertex_summary() -> (VertexId, TaskState, serde_json::Value, serde_json::Value) {
        let id = Faker.fake::<String>().into();

        let max_parallelism: usize = (1..1000).fake();
        let parallelism: usize = max_parallelism.min((1..1000).fake());
        let duration: u64 = (0..1_000_000).fake();
        let now = assert_ok!(std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)).as_secs();
        let start_time = now - duration;

        let task_state_sel: usize = (0..TASK_STATES.len()).fake();
        let task_state = TASK_STATES[task_state_sel];

        let summary = json!({
            "id": id,
            "name": Faker.fake::<String>(),
            "maxParallelism": max_parallelism,
            "parallelism": parallelism,
            "status": task_state,
            "start-time": start_time,
            "end-time": -1,
            "duration": duration,
            "tasks": {
                "SCHEDULED": (0..10).fake::<usize>(),
                "FINISHED": (0..10).fake::<usize>(),
                "FAILED": (0..10).fake::<usize>(),
                "CANCELING": (0..10).fake::<usize>(),
                "CANCELED": (0..10).fake::<usize>(),
                "DEPLOYING": (0..10).fake::<usize>(),
                "RECONCILING": (0..10).fake::<usize>(),
                "CREATED": (0..10).fake::<usize>(),
                "RUNNING": (0..10).fake::<usize>(),
                "INITIALIZING": (0..10).fake::<usize>()
            },
            "metrics": {
                "read-bytes": Faker.fake::<i64>(),
                "read-bytes-complete": Faker.fake::<bool>(),
                "write-bytes": Faker.fake::<i64>(),
                "write-bytes-complete": Faker.fake::<bool>(),
                "read-records": Faker.fake::<i64>(),
                "read-records-complete": Faker.fake::<bool>(),
                "write-records": Faker.fake::<i64>(),
                "write-records-complete": Faker.fake::<bool>()
            }
        });

        let plan = json!({
                "id": id,
                "parallelism": (1..1000).fake::<usize>(),
                "operator": "",
                "operator_strategy": "",
                "description": Faker.fake::<String>(),
                // no inputs for now
                "optimizer_properties": {}
        });

        (id, task_state, summary, plan)
    }
}

#[derive(Debug)]
struct MockFlinkVertexMetricSummary;

impl MockFlinkVertexMetricSummary {
    async fn mount_mock(job_id: &JobId, vertex_id: &VertexId, server: &MockServer, expect: impl Into<Times>) -> Self {
        let body = json!([
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

        let response = ResponseTemplate::new(200).set_body_json(body);

        let query_path = format!("/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics");

        Mock::given(method("GET"))
            .and(path(query_path))
            .and(EmptyQueryParamMatcher)
            .respond_with(response)
            .expect(expect)
            .mount(&server)
            .await;

        Self
    }
}

#[derive(Debug)]
struct MockFlinkVertexMetrics {
    #[allow(dead_code)]
    pub job_id: JobId,
    #[allow(dead_code)]
    pub vertex_id: VertexId,
    pub nr_records_in_per_second: RecordsPerSecond,
    pub nr_records_out_per_second: RecordsPerSecond,
    pub input_buffer_queue_len: f64,
    pub in_buffer_pool_usage: f64,
    pub output_buffer_queue_len: f64,
    pub out_buffer_pool_usage: f64,
}

impl MockFlinkVertexMetrics {
    pub async fn mount_mock(
        job_id: &JobId, vertex_id: &VertexId, server: &MockServer, expect: impl Into<Times>,
    ) -> Self {
        let nr_records_in_per_second = Faker.fake::<f64>().into();
        let nr_records_out_per_second = Faker.fake::<f64>().into();
        let input_buffer_queue_len = Faker.fake::<f64>();
        let in_buffer_pool_usage = Faker.fake::<f64>();
        let output_buffer_queue_len = Faker.fake::<f64>();
        let out_buffer_pool_usage = Faker.fake::<f64>();

        let body = json!([
            { "id": "numRecordsInPerSecond", "max": nr_records_in_per_second },
            { "id": "numRecordsOutPerSecond", "max": nr_records_out_per_second },
            { "id": "buffers.inputQueueLength", "max": input_buffer_queue_len },
            { "id": "buffers.inPoolUsage", "max": in_buffer_pool_usage },
            { "id": "buffers.outputQueueLength", "max": output_buffer_queue_len },
            { "id": "buffers.outPoolUsage", "max": out_buffer_pool_usage },
        ]);

        let response = ResponseTemplate::new(200).set_body_json(body);

        let query_path = format!("/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics");

        Mock::given(method("GET"))
            .and(path(query_path))
            .and(QueryParamKeyMatcher::new("get"))
            .respond_with(response)
            .expect(expect)
            .mount(&server)
            .await;

        Self {
            job_id: job_id.clone(),
            vertex_id: vertex_id.clone(),
            nr_records_in_per_second,
            nr_records_out_per_second,
            input_buffer_queue_len,
            in_buffer_pool_usage,
            output_buffer_queue_len,
            out_buffer_pool_usage,
        }
    }
}
