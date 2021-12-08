use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;

use futures::future::Future;
use itertools::Itertools;
use once_cell::sync::Lazy;
use proctor::elements::{telemetry, Telemetry, TelemetryValue};
use proctor::error::{CollectionError, TelemetryError};
use proctor::graph::stage::WithApi;
use proctor::phases::collection::TelemetrySource;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use serde::{Deserialize, Serialize};
use tracing_futures::Instrument;
use url::Url;

use crate::settings::{Aggregation, FlinkScope, FlinkSettings, MetricOrder};

pub static STD_METRIC_ORDERS: Lazy<Vec<MetricOrder>> = Lazy::new(|| {
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
        (TaskManagers, "Status.JVM.CPU.Load", Max, "cluster.task_cpu_load", Float),
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
pub struct TaskContext {
    pub client: ClientWithMiddleware,
    pub base_url: Url,
}

impl fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskContext").field("base_url", &self.base_url).finish()
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlinkMetricResponse(pub Vec<FlinkMetric>);

impl IntoIterator for FlinkMetricResponse {
    type IntoIter = std::vec::IntoIter<Self::Item>;
    type Item = FlinkMetric;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}


#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlinkMetric {
    pub id: String,
    #[serde(flatten)]
    pub values: HashMap<Aggregation, TelemetryValue>,
}

impl FlinkMetric {
    fn populate_telemetry<'m, O>(&self, telemetry: &mut Telemetry, orders: O)
    where
        O: IntoIterator<Item = &'m MetricOrder>,
    {
        for o in orders.into_iter() {
            let agg = o.agg;
            match self.values.get(&agg) {
                None => tracing::warn!(metric=%o.metric, %agg, "metric order not found in flink response."),
                Some(metric_value) => match metric_value.clone().try_cast(o.telemetry_type) {
                    Err(err) => tracing::error!(
                        error=?err, metric=%o.metric, ?metric_value, order=?o,
                        "Unable to read ordered type in flink metric response - skipping."
                    ),
                    Ok(value) => {
                        let _ = telemetry.insert(o.telemetry_path.clone(), value);
                    },
                },
            }
        }
    }
}

/// sigh -- each flink scope follows it's own metric format convention. This function attempts to
/// fashion a corresponding aggregation suffix.
fn suffix_for(id: &str, agg: Aggregation) -> String {
    let forms: Lazy<regex::RegexSet> = Lazy::new(|| {
        regex::RegexSet::new(&[
            r##"^[a-z]+[a-zA-Z]+$"##,                     // camelCase: Jobs, Kinesis
            r##"^[a-z]+[a-zA-Z]*(\.[a-z]+[a-zA-Z]*)+$"##, // .camelCase: Task vertix
            r##"^[a-z]+[-a-z]+$"##,                       // kabab-case: Kafka
            r##"^[A-Z]+[a-zA-Z]*(\.[A-Z]+[a-zA-Z]*)*$"##, // .PascalCase: TaskManagers
        ])
        .unwrap()
    });

    match forms.matches(id).into_iter().take(1).next() {
        Some(0) => format!("{}", agg),                             // camelCase - Jobs and Kinesis
        Some(1) => format!(".{}", agg.to_string().to_lowercase()), // .camelCase - Task vertix
        Some(2) => format!("-{}", agg.to_string().to_lowercase()), // kabab-case - Kafka
        Some(3) => format!(".{}", agg),                            // .PascalCase - TaskManagers
        _ => {
            tracing::warn!(%id, %agg, "failed to correlate metric form to known Flink scopes - defaulting to camelCase");
            format!("{}", agg)
        },
    }
}

#[tracing::instrument(level = "debug", skip(metrics, orders))]
pub fn build_telemetry<M>(metrics: M, orders: HashMap<String, Vec<MetricOrder>>) -> Result<Telemetry, TelemetryError>
where
    M: IntoIterator<Item = FlinkMetric>,
{
    let mut telemetry = Telemetry::default();

    let mut satisfied = HashSet::new();

    for m in metrics.into_iter() {
        match orders.get(m.id.as_str()) {
            Some(os) => {
                satisfied.insert(m.id.clone());
                m.populate_telemetry(&mut telemetry, os);
            },
            None => {
                tracing::warn!(unexpected_metric=?m, "unexpected metric in response not ordered - adding with minimal translation");
                m.values.into_iter().for_each(|(agg, val)| {
                    let key = format!("{}{}", m.id, suffix_for(m.id.as_str(), agg));
                    let _ = telemetry.insert(key, val);
                });
            },
        }
    }

    let all: HashSet<String> = orders.keys().cloned().collect();
    let unfulfilled = all.difference(&satisfied).collect::<HashSet<_>>();
    if !unfulfilled.is_empty() {
        tracing::warn!(?unfulfilled, "some metrics orders were not fulfilled.");
    }

    Ok(telemetry)
}

pub type TelemetryGenerator = Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<Telemetry, CollectionError>>>>>;

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
                let task = make_root_scope_collection_generator(FlinkScope::Jobs, &scope_orders, context.clone());
                foo.push(Box::new(task));
            },
            _ => unimplemented!(),
        }
    }

    todo!()
}

pub fn make_root_scope_collection_generator(
    scope: FlinkScope, orders: &[MetricOrder], context: TaskContext,
) -> Result<Option<TelemetryGenerator>, CollectionError> {
    let scope = scope.to_string().to_lowercase();
    let (metric_orders, agg_span) = distill_metric_orders_and_agg(orders);
    if metric_orders.is_empty() {
        return Ok(None);
    }

    let mut url = context.base_url.join(format!("{}/metrics", scope).as_str())?;
    url.query_pairs_mut()
        .clear()
        .append_pair("get", metric_orders.keys().join(",").as_str())
        .append_pair("agg", agg_span.iter().join(",").as_str());


    let gen: TelemetryGenerator = Box::new(move || {
        let client = context.client.clone();
        let url = url.clone();
        let orders = metric_orders.clone();

        Box::pin(
            async move {
                let resp: FlinkMetricResponse = client.request(reqwest::Method::GET, url).send().await?.json().await?;
                let telemetry = build_telemetry(resp, orders)?;
                std::result::Result::<Telemetry, CollectionError>::Ok(telemetry)
            }
            .instrument(tracing::info_span!("Flink collection", %scope)),
        )
    });

    Ok(Some(gen))
}


pub fn make_taskmanagers_collection_generator(
    context: TaskContext,
) -> Result<Option<TelemetryGenerator>, CollectionError> {
    let mut url = context.base_url.join("taskmanagers/")?;
    let gen: TelemetryGenerator = Box::new(move || {
        let client = context.client.clone();
        let url = url.clone();

        Box::pin(
            async move {
                let resp: serde_json::Value = client.request(reqwest::Method::GET, url).send().await?.json().await?;
                let taskmanagers = resp["taskmanagers"].as_array().map(|tms| tms.len()).unwrap_or(0);
                let mut telemetry: telemetry::TableType = HashMap::default();
                telemetry.insert("cluster.nr_task_managers".to_string(), taskmanagers.into());
                Ok(telemetry.into())
            }
            .instrument(tracing::info_span!("Flink taskmanager collection",)),
        )
    });
    Ok(Some(gen))
}

/// Distills the simple list for a given Flink collection scope to target specific metrics and
/// aggregation span.
fn distill_metric_orders_and_agg(orders: &[MetricOrder]) -> (HashMap<String, Vec<MetricOrder>>, HashSet<Aggregation>) {
    let mut order_domain = HashMap::default();
    let mut agg_span = HashSet::default();

    for o in orders {
        agg_span.insert(o.agg);
        let entry = order_domain.entry(o.metric.clone()).or_insert(Vec::new());
        entry.push(o.clone());
    }

    (order_domain, agg_span)
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

    #[test]
    fn test_suffix_for() {
        use self::Aggregation::*;

        assert_eq!(&suffix_for("", Min), "Min");
        assert_eq!(&suffix_for("*&^@(*#(*", Value), "Value");
        assert_eq!(&suffix_for("uptime", Max), "Max");
        assert_eq!(&suffix_for("numRestarts", Max), "Max");
        assert_eq!(&suffix_for("numberOfCompletedCheckpoints", Max), "Max");
        assert_eq!(&suffix_for("Status.JVM.CPU.Load", Max), ".Max");
        assert_eq!(&suffix_for("buffers.inputQueueLength", Max), ".max");
        assert_eq!(&suffix_for("records-lag-max", Value), "-value");
    }
}
