use std::collections::{HashMap, HashSet};
use std::fmt;

use futures::future::Future;
use itertools::Itertools;
use once_cell::sync::Lazy;
use proctor::elements::{Telemetry, TelemetryValue};
use proctor::error::CollectionError;
use proctor::graph::stage::WithApi;
use proctor::phases::collection::TelemetrySource;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::settings::{Aggregation, FlinkScope, FlinkSettings, MetricOrder};

static STD_METRIC_ORDERS: Lazy<Vec<MetricOrder>> = Lazy::new(|| {
    use proctor::elements::TelemetryType::*;

    use crate::settings::{Aggregation::*, FlinkScope::*};

    [
        (Jobs, "uptime", Max, "health.job_uptime_millis", Integer), // does not work w reactive mode
        (Jobs, "numRestarts", Max, "health.job_nr_restarts", Integer),
        (
            Jobs,
            "numberOfCompletedCheckpoints",
            Max,
            "health.job_nr_completed_checkpoints",
            Integer,
        ), // does not work w reactive mode
        (
            Jobs,
            "numberOfFailedCheckpoints",
            Max,
            "health.job_nr_failed_checkpoints",
            Integer,
        ), // does not work w reactive mode
        (Task, "numRecordsInPerSecond", Max, "flow.records_in_per_sec", Float),
        (Task, "numRecordsOutPerSecond", Max, "flow.records_out_per_sec", Float),
        (TaskManagers, "Status.JVM.CPU.LOAD", Max, "cluster.task_cpu_load", Float),
        (
            TaskManagers,
            "Status.JVM.Memory.Heap.Used",
            Max,
            "cluster.task_heap_memory_used",
            Float,
        ),
        (
            TaskManagers,
            "Status.JVM.Memory.Heap.Committed",
            Max,
            "cluster.task_heap_memory_committed",
            Float,
        ),
        (
            TaskManagers,
            "Status.JVM.Threads.Count",
            Max,
            "cluster.task_nr_threads",
            Integer,
        ),
        (
            Task,
            "buffers.inputQueueLength",
            Max,
            "cluster.task_network_input_queue_len",
            Integer,
        ), // verify,
        (
            Task,
            "buffers.inPoolUsage",
            Max,
            "cluster.task_network_input_pool_usage",
            Integer,
        ), // verify,
        (
            Task,
            "buffers.outputQueueLength",
            Max,
            "cluster.task_network_output_queue_len",
            Integer,
        ), // verify,
        (
            Task,
            "buffers.outPoolUsage",
            Max,
            "cluster.task_network_output_pool_usage",
            Integer,
        ), // verify,
    ]
    .into_iter()
    .map(|(scope, m, agg, tp, telemetry_type)| MetricOrder {
        scope,
        metric: m.to_string(),
        agg,
        telemetry_path: tp.to_string(),
        telemetry_type,
    })
    .collect()
});

#[derive(Clone)]
struct TaskContext {
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
    let mut foo = Vec::default();
    for (scope, scope_orders) in MetricOrder::organize_by_scope(&orders).into_iter() {
        match scope {
            FlinkScope::Jobs => {
                let task = make_jobs_collection_task(&scope_orders, context.clone());
                foo.push(Box::new(task));
            },
            _ => unimplemented!(),
        }
    }

    todo!()
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlinkMetricResponse(pub Vec<FlinkMetric>);

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlinkMetric {
    pub id: String,
    #[serde(flatten)]
    pub values: HashMap<Aggregation, TelemetryValue>,
}


pub type TelemetryGenerator = Box<dyn Fn() -> Box<dyn Future<Output = Result<Telemetry, CollectionError>>>>;

fn make_jobs_collection_task(
    orders: &[MetricOrder], context: TaskContext,
) -> Result<Option<TelemetryGenerator>, CollectionError> {
    let mut metric_orders: HashMap<&str, &MetricOrder> = HashMap::default();
    let mut agg_span: HashSet<Aggregation> = HashSet::default();
    for o in orders.iter().filter(|o| o.scope == FlinkScope::Jobs) {
        agg_span.insert(o.agg);
        metric_orders.insert(o.metric.as_str(), &o);
    }

    if metric_orders.is_empty() {
        return Ok(None);
    }

    let mut url = context.base_url.join("jobs/metrics")?;
    url.query_pairs_mut()
        .clear()
        .append_pair("get", metric_orders.keys().join(",").as_str())
        .append_pair("agg", agg_span.iter().join(",").as_str());


    let gen: TelemetryGenerator = Box::new(move || {
        let client = context.client.clone();
        let url = url.clone();

        Box::new(
            async move {
                let resp: FlinkMetricResponse = client.request(reqwest::Method::GET, url).send().await?.json().await?;

                let telemetry = Telemetry::try_from(&resp)?;
                std::result::Result::<Telemetry, CollectionError>::Ok(telemetry)
            }, // .map(|rec| rec.unwrap())
        )
    });

    Ok(Some(gen))
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;
    use trim_margin::MarginTrimmable;

    use super::*;

    #[test]
    fn test_flink_metric_response_json_deser() {
        let json = r##"|[
                |  {
                |    "id": "metric1",
                |    "min": 1,
                |    "max": 34,
                |    "avg": 15,
                |    "sum": 45
                |  },
                |  {
                |    "id": "metric2",
                |    "min": 2,
                |    "max": 14,
                |    "avg": 7,
                |    "sum": 16
                |  }
                |]"##
            .trim_margin_with("|")
            .unwrap();

        let actual: FlinkMetricResponse = assert_ok!(serde_json::from_str(&json));
        assert_eq!(
            actual,
            FlinkMetricResponse(vec![
                FlinkMetric {
                    id: "metric1".to_string(),
                    values: maplit::hashmap! {
                        Aggregation::Min => 1_i64.into(),
                        Aggregation::Max => 34_i64.into(),
                        Aggregation::Avg => 15_i64.into(),
                        Aggregation::Sum => 45_i64.into(),
                    },
                },
                FlinkMetric {
                    id: "metric2".to_string(),
                    values: maplit::hashmap! {
                        Aggregation::Min => 2_i64.into(),
                        Aggregation::Max => 14_i64.into(),
                        Aggregation::Avg => 7_i64.into(),
                        Aggregation::Sum => 16_i64.into(),
                    },
                },
            ])
        );
    }
}
