// use std::collections::{HashSet, VecDeque};
// use std::fmt::{self, Debug};
// use std::ops::Add;
// use std::time::Duration;
//
// use frunk::{Monoid, Semigroup};
// use itertools::Itertools;
// use once_cell::sync::Lazy;
// use oso::{Oso, PolarClass};
// use pretty_snowflake::{Id, Label, Labeling};
// use proctor::elements::telemetry::UpdateMetricsFn;
// use proctor::elements::{telemetry, Interval, PolicyContributor, Telemetry, Timestamp};
// use proctor::error::{PolicyError, ProctorError};
// use proctor::phases::sense::SubscriptionRequirements;
// use proctor::{AppData, Correlation, ProctorIdGenerator};
// use prometheus::{Gauge, IntGauge};
// use serde::{Deserialize, Serialize};
//
// use crate::metrics::UpdateMetrics;

// pub type CorrelationId = Id<MetricCatalog>;
// pub type CorrelationGenerator = ProctorIdGenerator<MetricCatalog>;

// pub trait Portfolio: AppData + Monoid {
//     type Item: AppData;
//     fn set_time_window(&mut self, time_window: Duration);
//     fn window_interval(&self) -> Option<Interval>;
//     fn push(&mut self, item: Self::Item);
// }
//
// #[derive(PolarClass, Clone, PartialEq, Serialize, Deserialize)]
// pub struct MetricPortfolio {
//     portfolio: VecDeque<MetricCatalog>,
//     time_window: Duration,
// }
//
// impl fmt::Debug for MetricPortfolio {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("MetricPortfolio")
//             .field("interval", &self.window_interval())
//             .field("time_window", &self.time_window)
//             .field("portfolio_size", &self.portfolio.len())
//             .field("head", &self.head().map(|c| c.recv_timestamp.to_string()))
//             .field("portfolio", &self.portfolio.iter().map(|m| m.recv_timestamp.to_string()).collect::<Vec<_>>())
//             // .field("head", self.deref())
//             .finish()
//     }
// }
//
// impl MetricPortfolio {
//     pub fn from_size(metrics: MetricCatalog, window: usize, interval: Duration) -> Self {
//         let mut portfolio = VecDeque::with_capacity(window);
//         portfolio.push_back(metrics);
//         let time_window = interval * window as u32;
//         Self { portfolio, time_window }
//     }
//
//     pub fn from_time_window(metrics: MetricCatalog, time_window: Duration) -> Self {
//         let mut portfolio = VecDeque::new();
//         portfolio.push_back(metrics);
//         Self { portfolio, time_window }
//     }
//
//     pub fn builder() -> MetricPortfolioBuilder {
//         MetricPortfolioBuilder::default()
//     }
// }
//
// impl PolicyContributor for MetricPortfolio {
//     #[tracing::instrument(level = "trace", skip(engine))]
//     fn register_with_policy_engine(engine: &mut Oso) -> Result<(), PolicyError> {
//         MetricCatalog::register_with_policy_engine(engine)?;
//
//         engine.register_class(
//             Self::get_polar_class_builder()
//                 .add_attribute_getter("recv_timestamp", |p| p.recv_timestamp)
//                 .add_attribute_getter("health", |p| p.health.clone())
//                 .add_attribute_getter("flow", |p| p.flow.clone())
//                 .add_attribute_getter("cluster", |p| p.cluster.clone())
//                 .add_attribute_getter("custom", |p| p.custom.clone())
//                 .add_method(
//                     "has_sufficient_coverage_looking_back_secs",
//                     Self::has_sufficient_coverage_looking_back_secs,
//                 )
//                 .add_method(
//                     "flow_input_records_lag_max_below_mark",
//                     Self::flow_input_records_lag_max_below_mark,
//                 )
//                 .add_method(
//                     "flow_input_records_lag_max_above_mark",
//                     Self::flow_input_records_lag_max_above_mark,
//                 )
//                 .add_method(
//                     "flow_input_millis_behind_latest_below_mark",
//                     Self::flow_input_millis_behind_latest_below_mark,
//                 )
//                 .add_method(
//                     "flow_input_millis_behind_latest_above_mark",
//                     Self::flow_input_millis_behind_latest_above_mark,
//                 )
//                 .add_method(
//                     "cluster_task_cpu_load_below_mark",
//                     Self::cluster_task_cpu_load_below_mark,
//                 )
//                 .add_method(
//                     "cluster_task_cpu_load_above_mark",
//                     Self::cluster_task_cpu_load_above_mark,
//                 )
//                 .add_method(
//                     "cluster_task_heap_memory_used_below_mark",
//                     Self::cluster_task_heap_memory_used_below_mark,
//                 )
//                 .add_method(
//                     "cluster_task_heap_memory_used_above_mark",
//                     Self::cluster_task_heap_memory_used_above_mark,
//                 )
//                 .add_method(
//                     "cluster_task_memory_load_below_mark",
//                     Self::cluster_task_heap_memory_load_below_mark,
//                 )
//                 .add_method(
//                     "cluster_task_memory_load_above_mark",
//                     Self::cluster_task_heap_memory_load_above_mark,
//                 )
//                 .build(),
//         )?;
//
//         Ok(())
//     }
// }
//
// impl Correlation for MetricPortfolio {
//     type Correlated = MetricCatalog;
//
//     fn correlation(&self) -> &Id<Self::Correlated> {
//         &self.correlation_id
//     }
// }
//
// impl MetricPortfolio {
//     pub fn head(&self) -> Option<&MetricCatalog> {
//         self.portfolio.back()
//     }
//
//     pub fn is_empty(&self) -> bool {
//         self.portfolio.is_empty()
//     }
//
//     pub fn len(&self) -> usize {
//         self.portfolio.len()
//     }
//
//     pub fn has_sufficient_coverage_looking_back_secs(&self, looking_back_secs: u32) -> bool {
//         let head_ts = self.recv_timestamp;
//         let looking_back = Duration::from_secs(looking_back_secs as u64);
//         self.has_sufficient_coverage(Interval::new(head_ts - looking_back, head_ts).unwrap())
//     }
//
//     pub fn has_sufficient_coverage(&self, interval: Interval) -> bool {
//         let coverage = self.coverage_for(interval).0;
//         0.5 <= coverage
//     }
//
//     pub fn coverage_for(&self, interval: Interval) -> (f64, impl Iterator<Item = &MetricCatalog>) {
//         let mut coverage_start: Option<Timestamp> = None;
//         let mut coverage_end: Option<Timestamp> = None;
//         let mut range = Vec::new();
//         for m in self.portfolio.iter().rev() {
//             if interval.contains_timestamp(m.recv_timestamp) {
//                 coverage_start = Some(m.recv_timestamp);
//                 if coverage_end.is_none() {
//                     coverage_end = coverage_start;
//                 }
//                 tracing::trace!("portfolio catalog[{}] lies in {interval:?}", m.recv_timestamp);
//                 range.push(m);
//             }
//         }
//
//         let coverage_interval = coverage_start
//             .zip(coverage_end)
//             .map(|(start, end)| Interval::new(start, end))
//             .transpose()
//             .unwrap_or_else(|err| {
//                 tracing::warn!(error=?err, "portfolio represents an invalid interval - using None");
//                 None
//             });
//
//         let coverage = coverage_interval
//             .map(|coverage| coverage.duration().as_secs_f64() / interval.duration().as_secs_f64())
//             .unwrap_or(0.0);
//
//         (coverage, range.into_iter())
//     }
//
//     /// Extracts metric properties from the head of the portfolio (i.e., the current catalog) toward
//     /// the past.
//     #[tracing::instrument(level = "trace", skip(self, extractor))]
//     pub fn extract_from_head<F, T>(&self, looking_back: Duration, extractor: F) -> Vec<T>
//     where
//         F: FnMut(&MetricCatalog) -> T,
//     {
//         self.head().map_or_else(Vec::new, |h| {
//             let head_ts = h.recv_timestamp;
//             self.extract_in_interval(Interval::new(head_ts - looking_back, head_ts).unwrap(), extractor)
//                 .collect()
//         })
//     }
//
//     /// Extracts metric properties from the end of the interval (i.e., the youngest catalog
//     /// contained in the interval) toward the past.
//     #[tracing::instrument(level = "trace", skip(self, extractor))]
//     pub fn extract_in_interval<F, T>(&self, interval: Interval, extractor: F) -> impl Iterator<Item = T>
//     where
//         F: FnMut(&MetricCatalog) -> T,
//     {
//         self.portfolio
//             .iter()
//             .rev()
//             .take_while(|m| interval.contains_timestamp(m.recv_timestamp))
//             .map(extractor)
//             .collect::<Vec<_>>()
//             .into_iter()
//     }
//
//     #[tracing::instrument(level = "trace", skip(self, f))]
//     pub fn forall_from_head<F>(&self, looking_back: Duration, f: F) -> bool
//     where
//         F: FnMut(&MetricCatalog) -> bool,
//     {
//         self.head().map_or(false, |h| {
//             let head_ts = h.recv_timestamp;
//             self.forall_in_interval(Interval::new(head_ts - looking_back, head_ts).unwrap(), f)
//         })
//     }
//
//     #[tracing::instrument(level = "trace", skip(self, f))]
//     pub fn forall_in_interval<F>(&self, interval: Interval, mut f: F) -> bool
//     where
//         F: FnMut(&MetricCatalog) -> bool,
//     {
//         if self.is_empty() {
//             tracing::debug!("empty portfolio");
//             false
//         } else if self.len() == 1 && interval.duration() == Duration::ZERO {
//             tracing::debug!("single metric portfolio");
//             interval.contains_timestamp(self.recv_timestamp) && f(self)
//         } else {
//             tracing::debug!(portfolio_window=?self.window_interval(), ?interval, "Checking for interval");
//
//             let (coverage, range_iter) = self.coverage_for(interval);
//             if coverage < 0.5 {
//                 tracing::debug!(
//                     ?interval,
//                     included_range=?range_iter.map(|m| m.recv_timestamp.to_string()).collect::<Vec<_>>(),
//                     %coverage, "not enough coverage for meaningful evaluation."
//                 );
//                 return false;
//             }
//
//             let range: Vec<&MetricCatalog> = range_iter.collect();
//             tracing::debug!(
//                 range=?range.iter().map(|m| (m.recv_timestamp.to_string(), m.flow.records_in_per_sec)).collect::<Vec<_>>(),
//                 %coverage,
//                 "evaluating for interval: {interval:?}",
//             );
//
//             range.into_iter().all(f)
//         }
//     }
// }
//
// impl Portfolio for MetricPortfolio {
//     type Item = MetricCatalog;
//
//     fn window_interval(&self) -> Option<Interval> {
//         // portfolio runs oldest to youngest
//         let start = self.portfolio.front().map(|m| m.recv_timestamp);
//         let end = self.portfolio.back().map(|m| m.recv_timestamp);
//         start.zip(end).map(|i| {
//             i.try_into()
//                 .expect("portfolio represents invalid interval (end before start): {i:?}")
//         })
//     }
//
//     fn set_time_window(&mut self, time_window: Duration) {
//         self.time_window = time_window;
//     }
//
//     fn push(&mut self, metrics: MetricCatalog) {
//         // todo: assumes monotonically increasing recv_timestamp -- check if not true and insert accordingly
//         // or sort after push and before pop?
//         let oldest_allowed = metrics.recv_timestamp - self.time_window;
//
//         self.portfolio.push_back(metrics);
//
//         while let Some(metric) = self.portfolio.front() {
//             if metric.recv_timestamp < oldest_allowed {
//                 let too_old = self.portfolio.pop_front();
//                 tracing::debug!(?too_old, %oldest_allowed, "popping metric outside of time window");
//             } else {
//                 break;
//             }
//         }
//     }
// }
//
// impl MetricPortfolio {
//     pub fn flow_input_records_lag_max_below_mark(&self, looking_back_secs: u32, max_value: i64) -> bool {
//         self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
//             m.flow
//                 .input_records_lag_max
//                 .map(|lag| {
//                     tracing::debug!(
//                         "eval lag in catalog[{}]: {lag} <= {max_value} = {}",
//                         m.recv_timestamp,
//                         lag <= max_value
//                     );
//                     lag <= max_value
//                 })
//                 .unwrap_or(false)
//         })
//     }
//
//     pub fn flow_input_records_lag_max_above_mark(&self, looking_back_secs: u32, min_value: i64) -> bool {
//         self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
//             m.flow
//                 .input_records_lag_max
//                 .map(|lag| {
//                     tracing::debug!(
//                         "eval lag in catalog[{}]: {min_value} <= {lag} = {}",
//                         m.recv_timestamp,
//                         min_value <= lag
//                     );
//                     min_value <= lag
//                 })
//                 .unwrap_or(false)
//         })
//     }
//
//     pub fn flow_input_millis_behind_latest_below_mark(&self, looking_back_secs: u32, max_value: i64) -> bool {
//         self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
//             m.flow
//                 .input_millis_behind_latest
//                 .map(|millis_behind_latest| {
//                     tracing::debug!(
//                         "eval millis_behind_latest in catalog[{}]: {millis_behind_latest} <= {max_value} = {}",
//                         m.recv_timestamp,
//                         millis_behind_latest <= max_value
//                     );
//                     millis_behind_latest <= max_value
//                 })
//                 .unwrap_or(false)
//         })
//     }
//
//     pub fn flow_input_millis_behind_latest_above_mark(&self, looking_back_secs: u32, min_value: i64) -> bool {
//         self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
//             m.flow
//                 .input_millis_behind_latest
//                 .map(|millis_behind_latest| {
//                     tracing::debug!(
//                         "eval millis_behind_latest in catalog[{}]: {min_value} <= {millis_behind_latest} = {}",
//                         m.recv_timestamp,
//                         min_value <= millis_behind_latest
//                     );
//                     min_value <= millis_behind_latest
//                 })
//                 .unwrap_or(false)
//         })
//     }
//
//     pub fn cluster_task_cpu_load_below_mark(&self, looking_back_secs: u32, max_value: f64) -> bool {
//         self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
//             tracing::debug!(
//                 "eval task_cpu_load in catalog[{}]: {} <= {max_value} = {}",
//                 m.recv_timestamp,
//                 m.cluster.task_cpu_load,
//                 m.cluster.task_cpu_load <= max_value
//             );
//             m.cluster.task_cpu_load <= max_value
//         })
//     }
//
//     pub fn cluster_task_cpu_load_above_mark(&self, looking_back_secs: u32, min_value: f64) -> bool {
//         self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
//             tracing::debug!(
//                 "eval task_cpu_load in catalog[{}]: {min_value} <= {} = {}",
//                 m.recv_timestamp,
//                 m.cluster.task_cpu_load,
//                 min_value <= m.cluster.task_cpu_load
//             );
//             min_value <= m.cluster.task_cpu_load
//         })
//     }
//
//     pub fn cluster_task_heap_memory_used_below_mark(&self, looking_back_secs: u32, max_value: f64) -> bool {
//         self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
//             tracing::debug!(
//                 "eval task_heap_memory_used in catalog[{}]: {} <= {max_value} = {}",
//                 m.recv_timestamp,
//                 m.cluster.task_heap_memory_used,
//                 m.cluster.task_heap_memory_used <= max_value
//             );
//             m.cluster.task_heap_memory_used <= max_value
//         })
//     }
//
//     pub fn cluster_task_heap_memory_used_above_mark(&self, looking_back_secs: u32, min_value: f64) -> bool {
//         self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
//             tracing::debug!(
//                 "eval task_heap_memory_used in catalog[{}]: {min_value} <= {} = {}",
//                 m.recv_timestamp,
//                 m.cluster.task_heap_memory_used,
//                 min_value <= m.cluster.task_heap_memory_used
//             );
//             min_value <= m.cluster.task_heap_memory_used
//         })
//     }
//
//     pub fn cluster_task_heap_memory_load_below_mark(&self, looking_back_secs: u32, max_value: f64) -> bool {
//         self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
//             tracing::debug!(
//                 "eval task_heap_memory_load in catalog[{}]: {} <= {max_value} = {}",
//                 m.recv_timestamp,
//                 m.cluster.task_cpu_load,
//                 m.cluster.task_cpu_load <= max_value
//             );
//             m.cluster.task_heap_memory_load() <= max_value
//         })
//     }
//
//     pub fn cluster_task_heap_memory_load_above_mark(&self, looking_back_secs: u32, min_value: f64) -> bool {
//         self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
//             tracing::debug!(
//                 "eval task_heap_memory_load in catalog[{}]: {min_value} <= {} = {}",
//                 m.recv_timestamp,
//                 m.cluster.task_cpu_load,
//                 min_value <= m.cluster.task_cpu_load
//             );
//             min_value <= m.cluster.task_heap_memory_load()
//         })
//     }
// }
//
// impl std::ops::Deref for MetricPortfolio {
//     type Target = MetricCatalog;
//
//     fn deref(&self) -> &Self::Target {
//         self.head().unwrap_or(&EMPTY_METRIC_CATALOG)
//     }
// }
//
// impl std::ops::Add<MetricCatalog> for MetricPortfolio {
//     type Output = Self;
//
//     fn add(mut self, rhs: MetricCatalog) -> Self::Output {
//         self.push(rhs);
//         self
//     }
// }
//
// impl std::ops::Add for MetricPortfolio {
//     type Output = Self;
//
//     fn add(self, rhs: Self) -> Self::Output {
//         self.combine(&rhs)
//     }
// }
//
// impl Monoid for MetricPortfolio {
//     fn empty() -> Self {
//         Self {
//             portfolio: VecDeque::new(),
//             time_window: Duration::ZERO,
//         }
//     }
// }
//
// impl Semigroup for MetricPortfolio {
//     #[tracing::instrument(level = "trace")]
//     fn combine(&self, other: &Self) -> Self {
//         let book = Self::do_ordered_combine(self, other);
//         Self { portfolio: book, time_window: self.time_window }
//     }
// }
//
// impl MetricPortfolio {
//     #[tracing::instrument(level = "trace", skip(older, younger))]
//     fn block_combine(
//         time_window: Duration, older: &VecDeque<MetricCatalog>, younger: &VecDeque<MetricCatalog>,
//     ) -> VecDeque<MetricCatalog> {
//         younger.back().map_or_else(
//             || older.clone(), // if younger has no back, then it must be empty.
//             |youngest| {
//                 let cutoff = youngest.recv_timestamp - time_window;
//                 let older_iter = older.iter().cloned();
//                 let younger_iter = younger.iter().cloned();
//                 let result: VecDeque<MetricCatalog> = older_iter
//                     .chain(younger_iter)
//                     .filter(|m| cutoff <= m.recv_timestamp)
//                     .collect();
//
//                 tracing::error!(
//                     ?time_window,
//                     older=?older.iter().map(|m| m.recv_timestamp).collect::<Vec<_>>(),
//                     younger=?younger.iter().map(|m| m.recv_timestamp).collect::<Vec<_>>(),
//                     ?youngest, ?cutoff,
//                     "filtering catalogs by time window: {:?}",
//                     result.iter().map(|m| m.recv_timestamp).collect::<Vec<_>>(),
//                 );
//
//                 result
//             },
//         )
//     }
//
//     #[tracing::instrument(level = "trace", skip(lhs_portfolio, rhs_portfolio))]
//     fn do_ordered_combine(lhs_portfolio: &Self, rhs_portfolio: &Self) -> VecDeque<MetricCatalog> {
//         let (lhs_interval, rhs_interval) = match (lhs_portfolio.window_interval(), rhs_portfolio.window_interval()) {
//             (None, None) => return VecDeque::new(),
//             (Some(_), None) => return lhs_portfolio.portfolio.clone(),
//             (None, Some(_)) => return rhs_portfolio.portfolio.clone(),
//             (Some(lhs), Some(rhs)) => (lhs, rhs),
//         };
//
//         let time_window = lhs_portfolio.time_window;
//         if lhs_interval.is_before(rhs_interval) {
//             Self::block_combine(time_window, &lhs_portfolio.portfolio, &rhs_portfolio.portfolio)
//         } else if rhs_interval.is_before(lhs_interval) {
//             Self::block_combine(time_window, &rhs_portfolio.portfolio, &lhs_portfolio.portfolio)
//         } else {
//             tracing::trace_span!("interspersed combination", ?time_window).in_scope(|| {
//                 let mut combined = lhs_portfolio
//                     .portfolio
//                     .iter()
//                     .chain(rhs_portfolio.portfolio.iter())
//                     .sorted_by(|lhs, rhs| lhs.recv_timestamp.cmp(&rhs.recv_timestamp))
//                     .cloned()
//                     .collect::<VecDeque<_>>();
//
//                 tracing::trace!(
//                     "combined portfolio: {:?}",
//                     combined.iter().map(|m| m.recv_timestamp).collect::<Vec<_>>()
//                 );
//                 if let Some(cutoff) = combined.back().map(|m| m.recv_timestamp - time_window) {
//                     tracing::debug!("cutoff: {cutoff:?}");
//                     combined.retain(|m| cutoff <= m.recv_timestamp);
//                 }
//
//                 tracing::debug!(
//                     "final combined: {:?}",
//                     combined.iter().map(|m| m.recv_timestamp).collect::<Vec<_>>()
//                 );
//                 combined
//             })
//         }
//     }
// }
//
// #[derive(Debug, Default, Clone, PartialEq)]
// pub struct MetricPortfolioBuilder {
//     metrics: VecDeque<MetricCatalog>,
//     time_window: Option<Duration>,
// }
//
// impl MetricPortfolioBuilder {
//     pub const fn with_time_window(mut self, time_window: Duration) -> Self {
//         self.time_window = Some(time_window);
//         self
//     }
//
//     pub fn with_size_and_interval(mut self, size: usize, interval: Duration) -> Self {
//         self.time_window = Some(interval * size as u32);
//         self
//     }
//
//     pub fn push(&mut self, catalog: MetricCatalog) -> &mut Self {
//         self.metrics.push_back(catalog);
//         self
//     }
//
//     pub fn is_empty(&self) -> bool {
//         self.metrics.is_empty()
//     }
//
//     pub fn len(&self) -> usize {
//         self.metrics.len()
//     }
//
//     pub fn build(self) -> MetricPortfolio {
//         let mut portfolio: Vec<MetricCatalog> = self.metrics.into_iter().collect();
//         portfolio.sort_by(|lhs, rhs| lhs.recv_timestamp.cmp(&rhs.recv_timestamp)); // sort to reverse
//
//         MetricPortfolio {
//             portfolio: portfolio.into_iter().collect(),
//             time_window: self.time_window.expect("must supply time window before final build"),
//         }
//     }
// }

// static EMPTY_METRIC_CATALOG: Lazy<MetricCatalog> = Lazy::new(MetricCatalog::empty);
//
// // #[serde_as]
// #[derive(PolarClass, Label, PartialEq, Clone, Serialize, Deserialize)]
// pub struct MetricCatalog {
//     pub correlation_id: Id<Self>,
//
//     #[polar(attribute)]
//     pub recv_timestamp: Timestamp,
//
//     #[polar(attribute)]
//     #[serde(flatten)] // current subscription mechanism only supports flatten keys
//     pub health: JobHealthMetrics,
//
//     #[polar(attribute)]
//     #[serde(flatten)] // current subscription mechanism only supports flatten keys
//     pub flow: FlowMetrics,
//
//     #[polar(attribute)]
//     #[serde(flatten)] // current subscription mechanism only supports flatten keys
//     pub cluster: ClusterMetrics,
//
//     #[polar(attribute)]
//     #[serde(flatten)] // flatten to collect extra properties.
//     pub custom: telemetry::TableType,
// }
//
// impl fmt::Debug for MetricCatalog {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("MetricCatalog")
//             .field("correlation", &self.correlation_id)
//             .field("recv_timestamp", &format!("{}", self.recv_timestamp))
//             .field("health", &self.health)
//             .field("flow", &self.flow)
//             .field("cluster", &self.cluster)
//             .field("custom", &self.custom)
//             .finish()
//     }
// }
//
// impl Correlation for MetricCatalog {
//     type Correlated = Self;
//
//     fn correlation(&self) -> &Id<Self::Correlated> {
//         &self.correlation_id
//     }
// }
//
// impl PolicyContributor for MetricCatalog {
//     #[tracing::instrument(level = "trace", skip(engine))]
//     fn register_with_policy_engine(engine: &mut Oso) -> Result<(), PolicyError> {
//         engine.register_class(Self::get_polar_class())?;
//         engine.register_class(JobHealthMetrics::get_polar_class())?;
//         engine.register_class(FlowMetrics::get_polar_class())?;
//         engine.register_class(
//             ClusterMetrics::get_polar_class_builder()
//                 .name("ClusterMetrics")
//                 .add_method("task_heap_memory_load", ClusterMetrics::task_heap_memory_load)
//                 .add_method(
//                     "task_network_input_utilization",
//                     ClusterMetrics::task_network_input_utilization,
//                 )
//                 .add_method(
//                     "task_network_output_utilization",
//                     ClusterMetrics::task_network_output_utilization,
//                 )
//                 .build(),
//         )?;
//         Ok(())
//     }
// }
//
// impl Monoid for MetricCatalog {
//     fn empty() -> Self {
//         Self {
//             correlation_id: Id::direct(<Self as Label>::labeler().label(), 0, "<undefined>"),
//             recv_timestamp: Timestamp::ZERO,
//             health: JobHealthMetrics::empty(),
//             flow: FlowMetrics::empty(),
//             cluster: ClusterMetrics::empty(),
//             custom: telemetry::TableType::new(),
//         }
//     }
// }
//
// impl Semigroup for MetricCatalog {
//     fn combine(&self, other: &Self) -> Self {
//         let mut custom = self.custom.clone();
//         custom.extend(other.custom.clone());
//
//         Self {
//             correlation_id: other.correlation_id.clone(),
//             recv_timestamp: other.recv_timestamp,
//             health: self.health.combine(&other.health),
//             flow: self.flow.combine(&other.flow),
//             cluster: self.cluster.combine(&other.cluster),
//             custom,
//         }
//     }
// }
//
// #[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
// pub struct JobHealthMetrics {
//     // todo per Flink doc's this metric does not work properly under Reactive mode. remove in favor of eligibility's
//     // last_failure?
//     /// The time that the job has been running without interruption.
//     /// Flink REST API: /jobs/metrics?get=uptime&agg=max
//     /// Returns -1 for completed jobs (in milliseconds).
//     #[polar(attribute)]
//     #[serde(rename = "health.job_uptime_millis")]
//     pub job_uptime_millis: i64,
//
//     /// The total number of restarts since this job was submitted, including full restarts and
//     /// fine-grained restarts.
//     /// Flink REST API: /jobs/metrics?get=numRestarts&agg=max
//     #[polar(attribute)]
//     #[serde(rename = "health.job_nr_restarts")]
//     pub job_nr_restarts: i64,
//
//     /// The number of successfully completed checkpoints.
//     /// Note: this metrics does not work properly when Reactive Mode is enabled.
//     /// Flink REST API: /jobs/metrics?get=numberOfCompletedCheckpoints&agg=max
//     #[polar(attribute)]
//     #[serde(rename = "health.job_nr_completed_checkpoints")]
//     pub job_nr_completed_checkpoints: i64,
//
//     /// The number of failed checkpoints.
//     /// Note: this metrics does not work properly when Reactive Mode is enabled.
//     /// Flink REST API: /jobs/metrics?get=numberOfFailedCheckpoints&agg=max
//     #[polar(attribute)]
//     #[serde(rename = "health.job_nr_failed_checkpoints")]
//     pub job_nr_failed_checkpoints: i64,
// }
//
// impl Monoid for JobHealthMetrics {
//     fn empty() -> Self {
//         Self {
//             job_uptime_millis: -1,
//             job_nr_restarts: -1,
//             job_nr_completed_checkpoints: -1,
//             job_nr_failed_checkpoints: -1,
//         }
//     }
// }
//
// impl Semigroup for JobHealthMetrics {
//     fn combine(&self, other: &Self) -> Self {
//         other.clone()
//     }
// }
//
// pub const MC_FLOW__RECORDS_IN_PER_SEC: &str = "flow.records_in_per_sec";
// pub const MC_FLOW__FORECASTED_TIMESTAMP: &str = "flow.forecasted_timestamp";
// pub const MC_FLOW__FORECASTED_RECORDS_IN_PER_SEC: &str = "flow.forecasted_records_in_per_sec";
//
// #[derive(PolarClass, Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
// pub struct FlowMetrics {
//     /// max rate of records flow into kafka/kinesis related subtask
//     /// Flink REST API:
//     /// /jobs/<job-id>/vertices/<vertex-id>/subtasks/metrics?get=numRecordsInPerSecond&subtask=0&
//     /// agg=max Flink REST API:
//     /// /jobs/<job-id>/vertices/<vertex-id>?get=numRecordsInPerSecond&agg=max
//     /// and regex for all subtask.metric fields
//     // todo: determine which vertices pertains to kafka/kinesis by:
//     #[polar(attribute)]
//     #[serde(rename = "flow.records_in_per_sec")]
//     pub records_in_per_sec: f64,
//
//     /// max rate of records flow out of job kafka/kinesis related subtask
//     /// Flink REST API:
//     /// /jobs/<job-id>/vertices/<vertex-id>/subtasks/metrics?get=numRecordsOutPerSecond&subtask=0&
//     /// agg=max
//     // todo: determine which vertices pertains to kafka/kinesis by:
//     #[polar(attribute)]
//     #[serde(rename = "flow.records_out_per_sec")]
//     pub records_out_per_sec: f64,
//
//     /// Timestamp (in fractional secs) for the forecasted_records_in_per_sec value.
//     #[polar(attribute)]
//     #[serde(
//         default,
//         rename = "flow.forecasted_timestamp",
//         skip_serializing_if = "Option::is_none"
//     )]
//     pub forecasted_timestamp: Option<f64>,
//
//     /// Forecasted rate of records flow predicted by springline.
//     #[polar(attribute)]
//     #[serde(
//         default,
//         rename = "flow.forecasted_records_in_per_sec",
//         skip_serializing_if = "Option::is_none"
//     )]
//     pub forecasted_records_in_per_sec: Option<f64>,
//
//     /// Applies to Kafka input connections. Pulled from the FlinkKafkaConsumer records-lag-max
//     /// metric.
//     #[polar(attribute)]
//     #[serde(
//         default,
//         rename = "flow.input_records_lag_max",
//         skip_serializing_if = "Option::is_none"
//     )]
//     pub input_records_lag_max: Option<i64>,
//
//     /// Applies to Kinesis input connections. Pulled from the FlinkKinesisConsumer
//     /// millisBehindLatest metric.
//     #[polar(attribute)]
//     #[serde(
//         default,
//         rename = "flow.millis_behind_latest",
//         skip_serializing_if = "Option::is_none"
//     )]
//     pub input_millis_behind_latest: Option<i64>,
// }
//
// impl Monoid for FlowMetrics {
//     fn empty() -> Self {
//         Self {
//             records_in_per_sec: -1.0,
//             records_out_per_sec: -1.0,
//             forecasted_timestamp: None,
//             forecasted_records_in_per_sec: None,
//             input_records_lag_max: None,
//             input_millis_behind_latest: None,
//         }
//     }
// }
//
// impl Semigroup for FlowMetrics {
//     fn combine(&self, other: &Self) -> Self {
//         other.clone()
//     }
// }
//
// pub const MC_CLUSTER__NR_ACTIVE_JOBS: &str = "cluster.nr_active_jobs";
// pub const MC_CLUSTER__NR_TASK_MANAGERS: &str = "cluster.nr_task_managers";
//
// #[derive(PolarClass, Default, PartialEq, Clone, Serialize, Deserialize)]
// pub struct ClusterMetrics {
//     /// Count of active jobs returned from Flink REST /jobs endpoint
//     #[polar(attribute)]
//     #[serde(rename = "cluster.nr_active_jobs")]
//     pub nr_active_jobs: u32,
//
//     /// Count of entries returned from Flink REST API /taskmanagers
//     #[polar(attribute)]
//     #[serde(rename = "cluster.nr_task_managers")]
//     pub nr_task_managers: u32,
//
//     /// The recent CPU usage of the JVM for all taskmanagers.
//     /// - Flink REST API /taskmanagers/metrics?get=Status.JVM.CPU.LOAD&agg=max
//     #[polar(attribute)]
//     #[serde(rename = "cluster.task_cpu_load")]
//     pub task_cpu_load: f64,
//
//     /// The amount of heap memory currently used (in bytes).
//     /// - Flink REST API /taskmanagers/metrics?get=Status.JVM.Memory.Heap.Used&agg=max
//     #[polar(attribute)]
//     #[serde(rename = "cluster.task_heap_memory_used")]
//     pub task_heap_memory_used: f64,
//
//     /// The amount of heap memory guaranteed to be available to the JVM (in bytes).
//     /// - Flink REST API /taskmanagers/metrics?get=Status.JVM.Memory.Heap.Committed&agg=max
//     #[polar(attribute)]
//     #[serde(rename = "cluster.task_heap_memory_committed")]
//     pub task_heap_memory_committed: f64,
//
//     /// The total number of live threads.
//     /// - Flink REST API /taskmanagers/metrics?get=Status.JVM.Threads.Count&agg=max
//     #[polar(attribute)]
//     #[serde(rename = "cluster.task_nr_threads")]
//     pub task_nr_threads: i64,
//
//     /// The number of queued input buffers.
//     #[polar(attribute)]
//     #[serde(rename = "cluster.task_network_input_queue_len")]
//     pub task_network_input_queue_len: f64,
//
//     /// An estimate of the input buffers usage.
//     #[polar(attribute)]
//     #[serde(rename = "cluster.task_network_input_pool_usage")]
//     pub task_network_input_pool_usage: f64,
//
//     /// The number of queued input buffers.
//     #[polar(attribute)]
//     #[serde(rename = "cluster.task_network_output_queue_len")]
//     pub task_network_output_queue_len: f64,
//
//     /// An estimate of the output buffers usage.
//     #[polar(attribute)]
//     #[serde(rename = "cluster.task_network_output_pool_usage")]
//     pub task_network_output_pool_usage: f64,
// }
//
// #[allow(unused_must_use)]
// impl ClusterMetrics {
//     pub fn task_heap_memory_load(&self) -> f64 {
//         self.task_heap_memory_used / self.task_heap_memory_committed
//     }
//
//     pub fn task_network_input_utilization(&self) -> f64 {
//         self.task_network_input_pool_usage / self.task_network_input_queue_len
//     }
//
//     pub fn task_network_output_utilization(&self) -> f64 {
//         self.task_network_output_pool_usage / self.task_network_output_queue_len
//     }
// }
//
// impl fmt::Debug for ClusterMetrics {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("ClusterMetrics")
//             .field("nr_active_jobs", &self.nr_active_jobs)
//             .field("nr_task_managers", &self.nr_task_managers)
//             .field("task_cpu_load", &self.task_cpu_load)
//             .field("task_heap_memory_used", &self.task_heap_memory_used)
//             .field("task_heap_memory_committed", &self.task_heap_memory_committed)
//             .field("task_heap_memory_load", &self.task_heap_memory_load())
//             .field("task_nr_threads", &self.task_nr_threads)
//             .field("task_network_input_queue_len", &self.task_network_input_queue_len)
//             .field("task_network_input_pool_usage", &self.task_network_input_pool_usage)
//             .field("task_network_input_utilization", &self.task_network_input_utilization())
//             .field("task_network_output_queue_len", &self.task_network_output_queue_len)
//             .field("task_network_output_pool_usage", &self.task_network_output_pool_usage)
//             .field(
//                 "task_network_output_utilization",
//                 &self.task_network_output_utilization(),
//             )
//             .finish()
//     }
// }
//
// impl Monoid for ClusterMetrics {
//     fn empty() -> Self {
//         Self {
//             nr_active_jobs: 0,
//             nr_task_managers: 0,
//             task_cpu_load: -1.0,
//             task_heap_memory_used: -1.0,
//             task_heap_memory_committed: -1.0,
//             task_nr_threads: -1,
//             task_network_input_queue_len: -1.0,
//             task_network_input_pool_usage: -1.0,
//             task_network_output_queue_len: -1.0,
//             task_network_output_pool_usage: -1.0,
//         }
//     }
// }
//
// impl Semigroup for ClusterMetrics {
//     fn combine(&self, other: &Self) -> Self {
//         other.clone()
//     }
// }
//
// impl Add for MetricCatalog {
//     type Output = Self;
//
//     fn add(self, rhs: Self) -> Self::Output {
//         let mut lhs = self.custom;
//         lhs.extend(rhs.custom);
//         Self { custom: lhs, ..self }
//     }
// }
//
// impl Add<&Self> for MetricCatalog {
//     type Output = Self;
//
//     fn add(self, rhs: &Self) -> Self::Output {
//         let mut lhs = self.custom;
//         lhs.extend(rhs.custom.clone());
//         Self { custom: lhs, ..self }
//     }
// }
//
// impl SubscriptionRequirements for MetricCatalog {
//     fn required_fields() -> HashSet<String> {
//         maplit::hashset! {
//             // JobHealthMetrics
//             "health.job_uptime_millis".into(),
//             "health.job_nr_restarts".into(),
//             "health.job_nr_completed_checkpoints".into(),
//             "health.job_nr_failed_checkpoints".into(),
//
//             // FlowMetrics
//             MC_FLOW__RECORDS_IN_PER_SEC.into(),
//             "flow.records_out_per_sec".into(),
//
//             // ClusterMetrics
//             MC_CLUSTER__NR_ACTIVE_JOBS.into(),
//             MC_CLUSTER__NR_TASK_MANAGERS.into(),
//             "cluster.task_cpu_load".into(),
//             "cluster.task_heap_memory_used".into(),
//             "cluster.task_heap_memory_committed".into(),
//             "cluster.task_nr_threads".into(),
//             "cluster.task_network_input_queue_len".into(),
//             "cluster.task_network_input_pool_usage".into(),
//             "cluster.task_network_output_queue_len".into(),
//             "cluster.task_network_output_pool_usage".into(),
//         }
//     }
//
//     fn optional_fields() -> HashSet<String> {
//         maplit::hashset! {
//             // FlowMetrics
//             "flow.forecasted_timestamp".into(),
//             "flow.forecasted_records_in_per_sec".into(),
//             "flow.input_records_lag_max".into(),
//             "flow.input_millis_behind_latest".into(),
//         }
//     }
// }
//
// impl UpdateMetrics for MetricCatalog {
//     fn update_metrics_for(name: &str) -> UpdateMetricsFn {
//         let phase_name = name.to_string();
//         let update_fn = move |subscription_name: &str, telemetry: &Telemetry| match telemetry.clone().try_into::<Self>()
//         {
//             Ok(catalog) => {
//                 METRIC_CATALOG_TIMESTAMP.set(catalog.recv_timestamp.as_secs());
//
//                 METRIC_CATALOG_JOB_HEALTH_UPTIME.set(catalog.health.job_uptime_millis);
//                 METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS.set(catalog.health.job_nr_restarts);
//                 METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS.set(catalog.health.job_nr_completed_checkpoints);
//                 METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS.set(catalog.health.job_nr_failed_checkpoints);
//
//                 METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC.set(catalog.flow.records_in_per_sec);
//                 METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC.set(catalog.flow.records_out_per_sec);
//
//                 if let Some(lag) = catalog.flow.input_records_lag_max {
//                     METRIC_CATALOG_FLOW_INPUT_RECORDS_LAG_MAX.set(lag);
//                 }
//
//                 if let Some(lag) = catalog.flow.input_millis_behind_latest {
//                     METRIC_CATALOG_FLOW_INPUT_MILLIS_BEHIND_LATEST.set(lag);
//                 }
//
//                 METRIC_CATALOG_CLUSTER_NR_ACTIVE_JOBS.set(catalog.cluster.nr_active_jobs as i64);
//                 METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS.set(catalog.cluster.nr_task_managers as i64);
//                 METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD.set(catalog.cluster.task_cpu_load);
//                 METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED.set(catalog.cluster.task_heap_memory_used);
//                 METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED.set(catalog.cluster.task_heap_memory_committed);
//                 METRIC_CATALOG_CLUSTER_TASK_NR_THREADS.set(catalog.cluster.task_nr_threads);
//                 METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_QUEUE_LEN.set(catalog.cluster.task_network_input_queue_len);
//                 METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_POOL_USAGE.set(catalog.cluster.task_network_input_pool_usage);
//                 METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_QUEUE_LEN.set(catalog.cluster.task_network_output_queue_len);
//                 METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_POOL_USAGE
//                     .set(catalog.cluster.task_network_output_pool_usage);
//             },
//
//             Err(err) => {
//                 tracing::warn!(
//                     error=?err, %phase_name,
//                     "failed to update sensor metrics for subscription: {}", subscription_name
//                 );
//                 proctor::track_errors(&phase_name, &ProctorError::SensePhase(err.into()));
//             },
//         };
//
//         Box::new(update_fn)
//     }
// }
//
// pub(crate) static METRIC_CATALOG_TIMESTAMP: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "metric_catalog_timestamp",
//         "UNIX timestamp in seconds of last operational reading",
//     )
//     .expect("failed creating metric_catalog_timestamp metric")
// });
//
// pub(crate) static METRIC_CATALOG_JOB_HEALTH_UPTIME: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "metric_catalog_job_health_uptime",
//         "The time that the job has been running without interruption.",
//     )
//     .expect("failed creating metric_catalog_job_health_uptime metric")
// });
//
// pub(crate) static METRIC_CATALOG_JOB_HEALTH_NR_RESTARTS: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "metric_catalog_job_health_nr_restarts",
//         "The total number of restarts since this job was submitted, including full restarts and fine-grained restarts.",
//     )
//     .expect("failed creating metric_catalog_job_health_nr_restarts metric")
// });
//
// pub(crate) static METRIC_CATALOG_JOB_HEALTH_NR_COMPLETED_CHECKPOINTS: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "metric_catalog_job_health_nr_completed_checkpoints",
//         "The number of successfully completed checkpoints.",
//     )
//     .expect("failed creating metric_catalog_job_health_nr_completed_checkpoints metric")
// });
//
// pub(crate) static METRIC_CATALOG_JOB_HEALTH_NR_FAILED_CHECKPOINTS: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "metric_catalog_job_health_nr_failed_checkpoints",
//         "The number of failed checkpoints.",
//     )
//     .expect("failed creating metric_catalog_job_health_nr_failed_checkpoints metric")
// });
//
// pub(crate) static METRIC_CATALOG_FLOW_RECORDS_IN_PER_SEC: Lazy<Gauge> = Lazy::new(|| {
//     Gauge::new(
//         "metric_catalog_flow_records_in_per_sec",
//         "Current records ingress per second",
//     )
//     .expect("failed creating metric_catalog_flow_records_in_per_sec metric")
// });
//
// pub(crate) static METRIC_CATALOG_FLOW_RECORDS_OUT_PER_SEC: Lazy<Gauge> = Lazy::new(|| {
//     Gauge::new(
//         "metric_catalog_flow_records_out_per_sec",
//         "Current records egress per second",
//     )
//     .expect("failed creating metric_catalog_flow_records_out_per_sec metric")
// });
//
// pub(crate) static METRIC_CATALOG_FLOW_INPUT_RECORDS_LAG_MAX: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "metric_catalog_flow_input_records_lag_max",
//         "Current lag in handling messages from the Kafka ingress topic",
//     )
//     .expect("failed creating metric_catalog_flow_input_records_lag_max metric")
// });
//
// pub(crate) static METRIC_CATALOG_FLOW_INPUT_MILLIS_BEHIND_LATEST: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "metric_catalog_flow_input_millis_behind_latest",
//         "Current lag in handling messages from the Kinesis ingress topic",
//     )
//     .expect("failed creating metric_catalog_flow_input_records_lag_max metric")
// });
//
// pub(crate) static METRIC_CATALOG_CLUSTER_NR_ACTIVE_JOBS: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "metric_catalog_cluster_nr_active_jobs",
//         "Number of active jobs in the cluster",
//     )
//     .expect("failed creating metric_catalog_cluster_nr_active_jobs metric")
// });
//
// pub(crate) static METRIC_CATALOG_CLUSTER_NR_TASK_MANAGERS: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "metric_catalog_cluster_nr_task_managers",
//         "Number of active task managers in the cluster",
//     )
//     .expect("failed creating metric_catalog_cluster_nr_task_managers metric")
// });
//
// pub(crate) static METRIC_CATALOG_CLUSTER_TASK_CPU_LOAD: Lazy<Gauge> = Lazy::new(|| {
//     Gauge::new(
//         "metric_catalog_cluster_task_cpu_load",
//         "The recent CPU usage of the JVM.",
//     )
//     .expect("failed creating metric_catalog_cluster_task_cpu_load metric")
// });
//
// pub(crate) static METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_USED: Lazy<Gauge> = Lazy::new(|| {
//     Gauge::new(
//         "metric_catalog_cluster_task_heap_memory_used",
//         "The amount of heap memory currently used (in bytes).",
//     )
//     .expect("failed creating metric_catalog_cluster_task_heap_memory_used metric")
// });
//
// pub(crate) static METRIC_CATALOG_CLUSTER_TASK_HEAP_MEMORY_COMMITTED: Lazy<Gauge> = Lazy::new(|| {
//     Gauge::new(
//         "metric_catalog_cluster_task_heap_memory_committed",
//         "The amount of heap memory guaranteed to be available to the JVM (in bytes).",
//     )
//     .expect("failed creating metric_catalog_cluster_task_heap_memory_committed metric")
// });
//
// pub(crate) static METRIC_CATALOG_CLUSTER_TASK_NR_THREADS: Lazy<IntGauge> = Lazy::new(|| {
//     IntGauge::new(
//         "metric_catalog_cluster_task_nr_threads",
//         "The total number of live threads.",
//     )
//     .expect("failed creating metric_catalog_cluster_task_nr_threads metric")
// });
//
// pub(crate) static METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_QUEUE_LEN: Lazy<Gauge> = Lazy::new(|| {
//     Gauge::new(
//         "metric_catalog_cluster_task_network_input_queue_len",
//         "The number of queued input buffers.",
//     )
//     .expect("failed creating metric_catalog_cluster_task_network_input_queue_len metric")
// });
//
// pub(crate) static METRIC_CATALOG_CLUSTER_TASK_NETWORK_INPUT_POOL_USAGE: Lazy<Gauge> = Lazy::new(|| {
//     Gauge::new(
//         "metric_catalog_cluster_task_network_input_pool_usage",
//         "An estimate of the input buffers usage. ",
//     )
//     .expect("failed creating metric_catalog_cluster_task_network_input_pool_usage metric")
// });
//
// pub(crate) static METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_QUEUE_LEN: Lazy<Gauge> = Lazy::new(|| {
//     Gauge::new(
//         "metric_catalog_cluster_task_network_output_queue_len",
//         "The number of queued output buffers.",
//     )
//     .expect("failed creating metric_catalog_cluster_task_network_output_queue_len metric")
// });
//
// pub(crate) static METRIC_CATALOG_CLUSTER_TASK_NETWORK_OUTPUT_POOL_USAGE: Lazy<Gauge> = Lazy::new(|| {
//     Gauge::new(
//         "metric_catalog_cluster_task_network_output_pool_usage",
//         "An estimate of the output buffers usage. ",
//     )
//     .expect("failed creating metric_catalog_cluster_task_network_output_pool_usage metric")
// });

// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////

#[cfg(test)]
mod tests {
    // use std::collections::HashMap;
    // use std::convert::TryFrom;
    // use std::sync::Mutex;
    //
    // use chrono::{DateTime, TimeZone, Utc};
    // use claim::*;
    // use once_cell::sync::Lazy;
    // use pretty_assertions::assert_eq;
    // use proctor::elements::telemetry::TableValue;
    // use proctor::elements::{Telemetry, TelemetryType, TelemetryValue, ToTelemetry};
    // use proctor::error::TelemetryError;
    // use proctor::phases::sense::{SUBSCRIPTION_CORRELATION, SUBSCRIPTION_TIMESTAMP};
    // use proctor::ProctorIdGenerator;
    // use rand::seq::SliceRandom;
    // use rand::thread_rng;
    // use serde_test::{assert_tokens, Token};
    //
    // use super::*;

    // static ID_GENERATOR: Lazy<Mutex<ProctorIdGenerator<MetricCatalog>>> =
    //     Lazy::new(|| Mutex::new(ProctorIdGenerator::default()));
    //
    // fn metrics_for_test_with_datetime(ts: DateTime<Utc>, custom: telemetry::TableType) -> MetricCatalog {
    //     let mut id_gen = assert_ok!(ID_GENERATOR.lock());
    //     MetricCatalog {
    //         correlation_id: id_gen.next_id(),
    //         recv_timestamp: ts.into(),
    //         custom,
    //         health: JobHealthMetrics::default(),
    //         flow: FlowMetrics::default(),
    //         cluster: ClusterMetrics::default(),
    //     }
    // }
    //
    // fn get_custom_metric<T>(mc: &MetricCatalog, key: &str) -> Result<Option<T>, TelemetryError>
    // where
    //     T: TryFrom<TelemetryValue>,
    //     TelemetryError: From<<T as TryFrom<TelemetryValue>>::Error>,
    // {
    //     mc.custom
    //         .get(key)
    //         .map(|telemetry| {
    //             let value = T::try_from(telemetry.clone())?;
    //             Ok(value)
    //         })
    //         .transpose()
    // }
    //
    // #[derive(PartialEq, Debug)]
    // struct Bar(String);
    //
    // impl TryFrom<TelemetryValue> for Bar {
    //     type Error = TelemetryError;
    //
    //     fn try_from(value: TelemetryValue) -> Result<Self, Self::Error> {
    //         match value {
    //             TelemetryValue::Text(rep) => Ok(Bar(rep)),
    //             v => Err(TelemetryError::TypeError {
    //                 expected: TelemetryType::Text,
    //                 actual: Some(format!("{:?}", v)),
    //             }),
    //         }
    //     }
    // }
    //
    // #[test]
    // fn test_invalid_type_serde_issue() {
    //     let mut telemetry = Telemetry::new();
    //     telemetry.insert("cluster.task_cpu_load".to_string(), TelemetryValue::Float(0.025));
    //     telemetry.insert(
    //         "cluster.task_heap_memory_used".to_string(),
    //         TelemetryValue::Float(2511508464.0),
    //     );
    //     telemetry.insert(
    //         "cluster.task_network_input_queue_len".to_string(),
    //         TelemetryValue::Float(1.0),
    //     );
    //     telemetry.insert("health.job_uptime_millis".to_string(), TelemetryValue::Integer(201402));
    //     telemetry.insert(
    //         "flow.forecasted_timestamp".to_string(),
    //         TelemetryValue::Float(Timestamp::new(1647307440, 378969192).as_secs_f64()),
    //     );
    //     telemetry.insert(
    //         "cluster.task_network_output_queue_len".to_string(),
    //         TelemetryValue::Float(1.0),
    //     );
    //     telemetry.insert("cluster.nr_active_jobs".to_string(), TelemetryValue::Integer(0));
    //     telemetry.insert("health.job_nr_restarts".to_string(), TelemetryValue::Integer(0));
    //     telemetry.insert(
    //         "recv_timestamp".to_string(),
    //         TelemetryValue::Table(TableValue(Box::new(maplit::hashmap! {
    //             "secs".to_string() => TelemetryValue::Integer(1647307527),
    //             "nanos".to_string() => TelemetryValue::Integer(57406000)
    //         }))),
    //     );
    //     telemetry.insert("flow.records_in_per_sec".to_string(), TelemetryValue::Float(20.0));
    //     telemetry.insert(
    //         "health.job_nr_completed_checkpoints".to_string(),
    //         TelemetryValue::Integer(0),
    //     );
    //     telemetry.insert(
    //         "cluster.task_network_output_pool_usage".to_string(),
    //         TelemetryValue::Float(0.1),
    //     );
    //     telemetry.insert(
    //         "flow.records_out_per_sec".to_string(),
    //         TelemetryValue::Float(19.966666666666665),
    //     );
    //     telemetry.insert(
    //         "cluster.task_heap_memory_committed".to_string(),
    //         TelemetryValue::Float(3623878656.0),
    //     );
    //     telemetry.insert("cluster.task_nr_threads".to_string(), TelemetryValue::Integer(57));
    //     telemetry.insert(
    //         "cluster.task_network_input_pool_usage".to_string(),
    //         TelemetryValue::Float(0.0),
    //     );
    //     telemetry.insert("cluster.nr_task_managers".to_string(), TelemetryValue::Integer(5));
    //     telemetry.insert(
    //         "correlation_id".to_string(),
    //         TelemetryValue::Table(TableValue(Box::new(maplit::hashmap! {
    //             "pretty".to_string() => TelemetryValue::Text("FRQB-08549-HYQY-31208".to_string()),
    //             "snowflake".to_string() => TelemetryValue::Integer(6909308549966213120)
    //         }))),
    //     );
    //     telemetry.insert(
    //         "health.job_nr_failed_checkpoints".to_string(),
    //         TelemetryValue::Integer(0),
    //     );
    //     telemetry.insert(
    //         "flow.forecasted_records_in_per_sec".to_string(),
    //         TelemetryValue::Float(21.4504261933966),
    //     );
    //
    //     let actual: MetricCatalog = assert_ok!(telemetry.try_into());
    //     assert_eq!(
    //         actual,
    //         MetricCatalog {
    //             correlation_id: CorrelationId::direct(
    //                 "MetricCatalog",
    //                 6909308549966213120_i64,
    //                 "FRQB-08549-HYQY-31208"
    //             ),
    //             recv_timestamp: Timestamp::new(1647307527, 57406000),
    //             health: JobHealthMetrics {
    //                 job_uptime_millis: 201402,
    //                 job_nr_restarts: 0,
    //                 job_nr_completed_checkpoints: 0,
    //                 job_nr_failed_checkpoints: 0,
    //             },
    //             flow: FlowMetrics {
    //                 records_in_per_sec: 20.0,
    //                 records_out_per_sec: 19.966666666666665,
    //                 forecasted_timestamp: Some(Timestamp::new(1647307440, 378969192).as_secs_f64()),
    //                 forecasted_records_in_per_sec: Some(21.4504261933966),
    //                 input_records_lag_max: None,
    //                 input_millis_behind_latest: None,
    //             },
    //             cluster: ClusterMetrics {
    //                 nr_active_jobs: 0,
    //                 nr_task_managers: 5,
    //                 task_cpu_load: 0.025,
    //                 task_heap_memory_used: 2511508464.0,
    //                 task_heap_memory_committed: 3623878656.0,
    //                 task_nr_threads: 57,
    //                 task_network_input_queue_len: 1.0,
    //                 task_network_input_pool_usage: 0.0,
    //                 task_network_output_queue_len: 1.,
    //                 task_network_output_pool_usage: 0.1,
    //             },
    //             custom: HashMap::new(),
    //         }
    //     );
    // }
    //
    // #[test]
    // fn test_custom_metric() {
    //     let cdata = maplit::hashmap! {
    //         "foo".to_string() => "17".to_telemetry(),
    //         "otis".to_string() => "Otis".to_telemetry(),
    //         "bar".to_string() => "Neo".to_telemetry(),
    //     };
    //     let data = metrics_for_test_with_datetime(Utc::now(), cdata);
    //     assert_eq!(assert_some!(assert_ok!(get_custom_metric::<i64>(&data, "foo"))), 17_i64);
    //     assert_eq!(
    //         assert_some!(assert_ok!(get_custom_metric::<f64>(&data, "foo"))),
    //         17.0_f64
    //     );
    //     assert_eq!(
    //         assert_some!(assert_ok!(get_custom_metric::<String>(&data, "otis"))),
    //         "Otis".to_string()
    //     );
    //     assert_eq!(
    //         assert_some!(assert_ok!(get_custom_metric::<Bar>(&data, "bar"))),
    //         Bar("Neo".to_string())
    //     );
    //     assert_eq!(
    //         assert_some!(assert_ok!(get_custom_metric::<String>(&data, "bar"))),
    //         "Neo".to_string()
    //     );
    //     assert_none!(assert_ok!(get_custom_metric::<i64>(&data, "zed")));
    // }
    //
    // #[test]
    // fn test_metric_add() {
    //     let ts = Utc::now();
    //     let data = metrics_for_test_with_datetime(ts.clone(), std::collections::HashMap::default());
    //     let am1 = maplit::hashmap! {
    //         "foo.1".to_string() => "f-1".to_telemetry(),
    //         "bar.1".to_string() => "b-1".to_telemetry(),
    //     };
    //     let a1 = metrics_for_test_with_datetime(ts.clone(), am1.clone());
    //     let d1 = data.clone() + a1.clone();
    //     assert_eq!(d1.custom, am1);
    //
    //     let am2 = maplit::hashmap! {
    //         "foo.2".to_string() => "f-2".to_telemetry(),
    //         "bar.2".to_string() => "b-2".to_telemetry(),
    //     };
    //     let a2 = metrics_for_test_with_datetime(ts.clone(), am2.clone());
    //     let d2 = d1.clone() + a2.clone();
    //     let mut exp2 = am1.clone();
    //     exp2.extend(am2.clone());
    //     assert_eq!(d2.custom, exp2);
    // }
    //
    // const CORR_ID_REP: &str = "L";
    // static CORR_ID: Lazy<Id<MetricCatalog>> = Lazy::new(|| Id::direct("MetricCatalog", 12, CORR_ID_REP));
    //
    // #[test]
    // fn test_metric_catalog_serde() {
    //     let ts: Timestamp = Utc.ymd(1988, 5, 30).and_hms(9, 1, 17).into();
    //     let (ts_secs, ts_nsecs) = ts.as_pair();
    //     let metrics = MetricCatalog {
    //         correlation_id: CORR_ID.clone(),
    //         recv_timestamp: ts,
    //         health: JobHealthMetrics {
    //             job_uptime_millis: 1_234_567,
    //             job_nr_restarts: 3,
    //             job_nr_completed_checkpoints: 12_345,
    //             job_nr_failed_checkpoints: 7,
    //         },
    //         flow: FlowMetrics {
    //             records_in_per_sec: 17.,
    //             forecasted_timestamp: Some(ts.as_secs_f64()),
    //             forecasted_records_in_per_sec: Some(23.),
    //             input_records_lag_max: Some(314),
    //             input_millis_behind_latest: None,
    //             records_out_per_sec: 0.0,
    //         },
    //         cluster: ClusterMetrics {
    //             nr_active_jobs: 1,
    //             nr_task_managers: 4,
    //             task_cpu_load: 0.65,
    //             task_heap_memory_used: 92_987_f64,
    //             task_heap_memory_committed: 103_929_920_f64,
    //             task_nr_threads: 8,
    //             task_network_input_queue_len: 12.,
    //             task_network_input_pool_usage: 8.,
    //             task_network_output_queue_len: 13.,
    //             task_network_output_pool_usage: 5.,
    //         },
    //         custom: maplit::hashmap! {
    //             "bar".to_string() => 33.to_telemetry(),
    //         },
    //     };
    //
    //     assert_tokens(
    //         &metrics,
    //         &vec![
    //             Token::Map { len: None },
    //             Token::Str(SUBSCRIPTION_CORRELATION),
    //             Token::Struct { name: "Id", len: 2 },
    //             Token::Str("snowflake"),
    //             Token::I64(CORR_ID.clone().into()),
    //             Token::Str("pretty"),
    //             Token::Str(&CORR_ID_REP),
    //             Token::StructEnd,
    //             Token::Str(SUBSCRIPTION_TIMESTAMP),
    //             Token::TupleStruct { name: "Timestamp", len: 2 },
    //             Token::I64(ts_secs),
    //             Token::U32(ts_nsecs),
    //             Token::TupleStructEnd,
    //             Token::Str("health.job_uptime_millis"),
    //             Token::I64(1_234_567),
    //             Token::Str("health.job_nr_restarts"),
    //             Token::I64(3),
    //             Token::Str("health.job_nr_completed_checkpoints"),
    //             Token::I64(12_345),
    //             Token::Str("health.job_nr_failed_checkpoints"),
    //             Token::I64(7),
    //             Token::Str(MC_FLOW__RECORDS_IN_PER_SEC),
    //             Token::F64(17.),
    //             Token::Str("flow.records_out_per_sec"),
    //             Token::F64(0.),
    //             Token::Str("flow.forecasted_timestamp"),
    //             Token::Some,
    //             Token::F64(ts.as_secs_f64()),
    //             Token::Str("flow.forecasted_records_in_per_sec"),
    //             Token::Some,
    //             Token::F64(23.),
    //             Token::Str("flow.input_records_lag_max"),
    //             Token::Some,
    //             Token::I64(314),
    //             Token::Str(MC_CLUSTER__NR_ACTIVE_JOBS),
    //             Token::U32(1),
    //             Token::Str(MC_CLUSTER__NR_TASK_MANAGERS),
    //             Token::U32(4),
    //             Token::Str("cluster.task_cpu_load"),
    //             Token::F64(0.65),
    //             Token::Str("cluster.task_heap_memory_used"),
    //             Token::F64(92_987.),
    //             Token::Str("cluster.task_heap_memory_committed"),
    //             Token::F64(103_929_920.),
    //             Token::Str("cluster.task_nr_threads"),
    //             Token::I64(8),
    //             Token::Str("cluster.task_network_input_queue_len"),
    //             Token::F64(12.0),
    //             Token::Str("cluster.task_network_input_pool_usage"),
    //             Token::F64(8.0),
    //             Token::Str("cluster.task_network_output_queue_len"),
    //             Token::F64(13.0),
    //             Token::Str("cluster.task_network_output_pool_usage"),
    //             Token::F64(5.0),
    //             Token::Str("bar"),
    //             Token::I64(33),
    //             Token::MapEnd,
    //         ],
    //     )
    // }
    //
    // #[test]
    // fn test_telemetry_from_metric_catalog() -> anyhow::Result<()> {
    //     once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    //     let main_span = tracing::info_span!("test_telemetry_from_metric_catalog");
    //     let _main_span_guard = main_span.enter();
    //
    //     let ts = Utc.ymd(1988, 5, 30).and_hms(9, 1, 17).into();
    //     let corr_id = Id::direct("MetricCatalog", 17, "AB");
    //     let metrics = MetricCatalog {
    //         correlation_id: corr_id.clone(),
    //         recv_timestamp: ts,
    //         health: JobHealthMetrics {
    //             job_uptime_millis: 1_234_567,
    //             job_nr_restarts: 3,
    //             job_nr_completed_checkpoints: 12_345,
    //             job_nr_failed_checkpoints: 7,
    //         },
    //         flow: FlowMetrics {
    //             records_in_per_sec: 17.,
    //             forecasted_timestamp: None,
    //             forecasted_records_in_per_sec: None,
    //             input_records_lag_max: Some(314),
    //             input_millis_behind_latest: None,
    //             records_out_per_sec: 0.0,
    //         },
    //         cluster: ClusterMetrics {
    //             nr_active_jobs: 1,
    //             nr_task_managers: 4,
    //             task_cpu_load: 0.65,
    //             task_heap_memory_used: 92_987_f64,
    //             task_heap_memory_committed: 103_929_920_f64,
    //             task_nr_threads: 8,
    //             task_network_input_queue_len: 12.,
    //             task_network_input_pool_usage: 8.,
    //             task_network_output_queue_len: 13.,
    //             task_network_output_pool_usage: 5.,
    //         },
    //         custom: maplit::hashmap! {
    //             "foo".to_string() => "David".to_telemetry(),
    //             "bar".to_string() => 33.to_telemetry(),
    //         },
    //     };
    //
    //     let telemetry = Telemetry::try_from(&metrics)?;
    //     let (ts_secs, ts_nsecs) = ts.as_pair();
    //
    //     assert_eq!(
    //         telemetry,
    //         TelemetryValue::Table(maplit::hashmap! {
    //             SUBSCRIPTION_CORRELATION.to_string() => corr_id.to_telemetry(),
    //             SUBSCRIPTION_TIMESTAMP.to_string() => TelemetryValue::Seq(vec![ts_secs.to_telemetry(), ts_nsecs.to_telemetry(),]),
    //             "health.job_uptime_millis".to_string() => (1_234_567).to_telemetry(),
    //             "health.job_nr_restarts".to_string() => (3).to_telemetry(),
    //             "health.job_nr_completed_checkpoints".to_string() => (12_345).to_telemetry(),
    //             "health.job_nr_failed_checkpoints".to_string() => (7).to_telemetry(),
    //
    //             MC_FLOW__RECORDS_IN_PER_SEC.to_string() => (17.).to_telemetry(),
    //             "flow.records_out_per_sec".to_string() => (0.).to_telemetry(),
    //             "flow.input_records_lag_max".to_string() => 314.to_telemetry(),
    //
    //             MC_CLUSTER__NR_ACTIVE_JOBS.to_string() => 1.to_telemetry(),
    //             MC_CLUSTER__NR_TASK_MANAGERS.to_string() => 4.to_telemetry(),
    //             "cluster.task_cpu_load".to_string() => (0.65).to_telemetry(),
    //             "cluster.task_heap_memory_used".to_string() => (92_987.).to_telemetry(),
    //             "cluster.task_heap_memory_committed".to_string() => (103_929_920.).to_telemetry(),
    //             "cluster.task_nr_threads".to_string() => (8).to_telemetry(),
    //             "cluster.task_network_input_queue_len".to_string() => (12.0).to_telemetry(),
    //             "cluster.task_network_input_pool_usage".to_string() => (8.0).to_telemetry(),
    //             "cluster.task_network_output_queue_len".to_string() => (13.0).to_telemetry(),
    //             "cluster.task_network_output_pool_usage".to_string() => (5.0).to_telemetry(),
    //
    //             "foo".to_string() => "David".to_telemetry(),
    //             "bar".to_string() => 33.to_telemetry(),
    //         }.into())
    //         .into()
    //     );
    //
    //     Ok(())
    // }
    //
    // // #[test]
    // // fn test_metric_to_f64() {
    // //     let expected = 3.14159_f64;
    // //     let m: Metric<f64> = Metric::new("pi", expected);
    // //
    // //     let actual: f64 = m.into();
    // //     assert_eq!(actual, expected);
    // // }
    //
    // #[tracing::instrument(level = "info")]
    // fn make_test_catalog(ts: Timestamp, value: u32) -> MetricCatalog {
    //     MetricCatalog {
    //         correlation_id: Id::direct(
    //             <MetricCatalog as Label>::labeler().label(),
    //             ts.as_secs(),
    //             ts.as_secs_f64().to_string(),
    //         ),
    //         recv_timestamp: ts,
    //         health: JobHealthMetrics::default(),
    //         flow: FlowMetrics {
    //             records_in_per_sec: f64::from(value),
    //             ..FlowMetrics::default()
    //         },
    //         cluster: ClusterMetrics::default(),
    //         custom: HashMap::new(),
    //     }
    // }

    // #[tracing::instrument(level = "info", skip(catalogs))]
    // fn make_test_portfolio(
    //     limit: usize, interval: Duration, catalogs: &[MetricCatalog],
    // ) -> (MetricPortfolio, Vec<MetricCatalog>) {
    //     let mut portfolio = MetricPortfolio::builder().with_size_and_interval(limit, interval);
    //     let mut used = Vec::new();
    //     let mut remaining = Vec::new();
    //     for c in catalogs {
    //         if used.len() < limit {
    //             used.push(c.clone());
    //         } else {
    //             remaining.push(c.clone());
    //         }
    //     }
    //
    //     for c in used {
    //         tracing::debug!(portfolio_len=%(portfolio.len()+1), catalog=%c.correlation_id,"pushing catalog into portfolio.");
    //         portfolio.push(c);
    //     }
    //
    //     (portfolio.build(), remaining)
    // }
    //
    // #[test]
    // fn test_metric_portfolio_head() {
    //     let m1 = make_test_catalog(Timestamp::new(1, 0), 1);
    //     let m2 = make_test_catalog(Timestamp::new(2, 0), 2);
    //
    //     let mut portfolio = MetricPortfolio::from_size(m1.clone(), 3, Duration::from_secs(1));
    //     assert_eq!(portfolio.correlation_id, m1.correlation_id);
    //     assert_eq!(portfolio.recv_timestamp, m1.recv_timestamp);
    //
    //     portfolio.push(m2.clone());
    //     assert_eq!(portfolio.correlation_id, m2.correlation_id);
    //     assert_eq!(portfolio.recv_timestamp, m2.recv_timestamp);
    // }
    //
    // #[test]
    // fn test_metric_portfolio_combine() {
    //     once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    //     let main_span = tracing::info_span!("test_metric_portfolio_combine");
    //     let _main_span_guard = main_span.enter();
    //
    //     let interval = Duration::from_secs(1);
    //
    //     let m1 = make_test_catalog(Timestamp::new(1, 0), 1);
    //     let m2 = make_test_catalog(Timestamp::new(2, 0), 2);
    //     let m3 = make_test_catalog(Timestamp::new(3, 0), 3);
    //     let m4 = make_test_catalog(Timestamp::new(4, 0), 4);
    //     let m5 = make_test_catalog(Timestamp::new(5, 0), 5);
    //     let m6 = make_test_catalog(Timestamp::new(6, 0), 6);
    //     let m7 = make_test_catalog(Timestamp::new(7, 0), 7);
    //     let ms = [
    //         m1.clone(),
    //         m2.clone(),
    //         m3.clone(),
    //         m4.clone(),
    //         m5.clone(),
    //         m6.clone(),
    //         m7.clone(),
    //     ];
    //
    //     let (port_1, remaining) = make_test_portfolio(3, interval, &ms);
    //     assert_eq!(port_1.len(), 3);
    //     assert_eq!(
    //         port_1.portfolio.clone().into_iter().collect::<Vec<_>>(),
    //         vec![m1.clone(), m2.clone(), m3.clone()]
    //     );
    //     let (port_2, _) = make_test_portfolio(4, interval, &remaining);
    //     assert_eq!(port_2.len(), 4);
    //     assert_eq!(
    //         port_2.portfolio.clone().into_iter().collect::<Vec<_>>(),
    //         vec![m4.clone(), m5.clone(), m6.clone(), m7.clone()]
    //     );
    //     let combined = port_1.clone().combine(&port_2);
    //     assert_eq!(combined.time_window, port_1.time_window);
    //     let actual: Vec<MetricCatalog> = combined.portfolio.into();
    //     assert_eq!(actual, vec![m4.clone(), m5.clone(), m6.clone(), m7.clone()]);
    //     let combined = port_2.combine(&port_1);
    //     assert_eq!(combined.time_window, port_2.time_window);
    //     let actual: Vec<MetricCatalog> = combined.portfolio.into();
    //     assert_eq!(actual, vec![m3.clone(), m4.clone(), m5.clone(), m6.clone(), m7.clone()]);
    //
    //     let (port_3, remaining) = make_test_portfolio(4, interval, &ms);
    //     let (port_4, _) = make_test_portfolio(2, interval, &remaining);
    //     let combined = port_3.clone().combine(&port_4);
    //     assert_eq!(combined.time_window, port_3.time_window);
    //     let actual: Vec<MetricCatalog> = combined.portfolio.into();
    //     assert_eq!(actual, vec![m2.clone(), m3.clone(), m4.clone(), m5.clone(), m6.clone()]);
    //     let combined = port_4.clone().combine(&port_3);
    //     assert_eq!(combined.time_window, port_4.time_window);
    //     let actual: Vec<MetricCatalog> = combined.portfolio.into();
    //     assert_eq!(actual, vec![m4.clone(), m5.clone(), m6.clone()]);
    // }
    //
    // #[test]
    // fn test_metric_portfolio_shuffled_combine() {
    //     once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    //     let main_span = tracing::info_span!("test_metric_portfolio_shuffled_combine");
    //     let _main_span_guard = main_span.enter();
    //
    //     let interval = Duration::from_secs(1);
    //
    //     let m1 = make_test_catalog(Timestamp::new(1, 0), 1);
    //     let m2 = make_test_catalog(Timestamp::new(2, 0), 2);
    //     let m3 = make_test_catalog(Timestamp::new(3, 0), 3);
    //     let m4 = make_test_catalog(Timestamp::new(4, 0), 4);
    //     let m5 = make_test_catalog(Timestamp::new(5, 0), 5);
    //     let m6 = make_test_catalog(Timestamp::new(6, 0), 6);
    //     let m7 = make_test_catalog(Timestamp::new(7, 0), 7);
    //     let ms = [
    //         m1.clone(),
    //         m2.clone(),
    //         m3.clone(),
    //         m4.clone(),
    //         m5.clone(),
    //         m6.clone(),
    //         m7.clone(),
    //     ];
    //     let mut shuffled = ms.to_vec();
    //     shuffled.shuffle(&mut thread_rng());
    //     assert_ne!(shuffled.as_slice(), &ms);
    //
    //     let (port_1, remaining) = make_test_portfolio(3, interval, &shuffled);
    //     let (port_2, _) = make_test_portfolio(4, interval, &remaining);
    //     let combined = port_1.clone().combine(&port_2);
    //     let actual: Vec<MetricCatalog> = combined.portfolio.into();
    //     assert_eq!(actual, vec![m4.clone(), m5.clone(), m6.clone(), m7.clone()]);
    //     let combined = port_2.clone().combine(&port_1);
    //     let actual: Vec<MetricCatalog> = combined.portfolio.into();
    //     assert_eq!(actual, vec![m3.clone(), m4.clone(), m5.clone(), m6.clone(), m7.clone()]);
    //
    //     let (port_3, remaining) = make_test_portfolio(6, interval, &shuffled);
    //     assert_eq!(port_3.len(), 6);
    //     let (port_4, _) = make_test_portfolio(1, interval, &remaining);
    //     assert_eq!(port_4.len(), 1);
    //     let combined = port_3.clone().combine(&port_4);
    //     tracing::debug!(
    //         lhs=?port_3.portfolio.iter().map(|p| p.correlation_id.to_string()).collect::<Vec<_>>(),
    //         rhs=?port_4.portfolio.iter().map(|p| p.correlation_id.to_string()).collect::<Vec<_>>(),
    //         combined=?combined.portfolio.iter().map(|p| p.correlation_id.to_string()).collect::<Vec<_>>(),
    //         "port_3.combine(port_4)"
    //     );
    //     assert_eq!(combined.len(), port_3.len() + 1);
    //     assert_eq!(assert_some!(combined.window_interval()).duration(), port_3.time_window);
    //     let actual: Vec<MetricCatalog> = combined.portfolio.into();
    //     assert_eq!(
    //         actual,
    //         vec![
    //             m1.clone(),
    //             m2.clone(),
    //             m3.clone(),
    //             m4.clone(),
    //             m5.clone(),
    //             m6.clone(),
    //             m7.clone()
    //         ]
    //     );
    //     let combined = port_4.clone().combine(&port_3);
    //     assert_eq!(combined.len(), port_4.len() + 1);
    //     assert_eq!(assert_some!(combined.window_interval()).duration(), port_4.time_window);
    //     let actual: Vec<MetricCatalog> = combined.portfolio.into();
    //     assert_eq!(actual, vec![m6.clone(), m7.clone()]);
    // }
    //
    // #[test]
    // fn test_metric_portfolio_for_period() {
    //     once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    //     let main_span = tracing::info_span!("test_metric_portfolio_for_period");
    //     let _main_span_guard = main_span.enter();
    //
    //     let interval = Duration::from_secs(10);
    //
    //     let now = Timestamp::now();
    //     // flow.records_in_per_sec *decline* over time
    //     let m1 = make_test_catalog(now - Duration::from_secs(1 * 10), 1);
    //     let m2 = make_test_catalog(now - Duration::from_secs(2 * 10), 2);
    //     let m3 = make_test_catalog(now - Duration::from_secs(3 * 10), 3);
    //     let m4 = make_test_catalog(now - Duration::from_secs(4 * 10), 4);
    //     let m5 = make_test_catalog(now - Duration::from_secs(5 * 10), 5);
    //     let m6 = make_test_catalog(now - Duration::from_secs(6 * 10), 6);
    //     let m7 = make_test_catalog(now - Duration::from_secs(7 * 10), 7);
    //     let ms = [
    //         m1.clone(),
    //         m2.clone(),
    //         m3.clone(),
    //         m4.clone(),
    //         m5.clone(),
    //         m6.clone(),
    //         m7.clone(),
    //     ];
    //
    //     let f = |c: &MetricCatalog| {
    //         tracing::debug!(
    //             "[test] testing catalog[{}]: ({} <= {}) is {}",
    //             c.recv_timestamp.to_string(),
    //             c.flow.records_in_per_sec,
    //             5.0,
    //             c.flow.records_in_per_sec <= 5.0,
    //         );
    //         c.flow.records_in_per_sec <= 5.0
    //     };
    //
    //     let (portfolio, _) = make_test_portfolio(10, interval, &ms);
    //     tracing::info!(
    //         portfolio=?portfolio.portfolio.iter().map(|m| (m.recv_timestamp.to_string(), m.flow.records_in_per_sec)).collect::<Vec<_>>(),
    //         "*** PORTFOLIO CREATED"
    //     );
    //
    //     tracing::info_span!("looking back for 5 seconds").in_scope(|| {
    //         assert_eq!(portfolio.forall_from_head(Duration::from_secs(5), f), false);
    //     });
    //
    //     tracing::info_span!("looking back for 11 seconds").in_scope(|| {
    //         assert_eq!(portfolio.forall_from_head(Duration::from_secs(11), f), true);
    //     });
    //
    //     tracing::info_span!("looking back for 21 seconds").in_scope(|| {
    //         assert_eq!(portfolio.forall_from_head(Duration::from_secs(21), f), true);
    //     });
    //
    //     tracing::info_span!("looking back for 31 seconds").in_scope(|| {
    //         assert_eq!(portfolio.forall_from_head(Duration::from_secs(31), f), true);
    //     });
    //
    //     tracing::info_span!("looking back for 41 seconds").in_scope(|| {
    //         assert_eq!(portfolio.forall_from_head(Duration::from_secs(41), f), true);
    //     });
    //
    //     tracing::info_span!("looking back for 51 seconds").in_scope(|| {
    //         assert_eq!(portfolio.forall_from_head(Duration::from_secs(51), f), false);
    //     });
    //
    //     tracing::info_span!("looking back for 61 seconds").in_scope(|| {
    //         assert_eq!(portfolio.forall_from_head(Duration::from_secs(61), f), false);
    //     });
    //
    //     tracing::info_span!("looking back for 71 seconds").in_scope(|| {
    //         assert_eq!(portfolio.forall_from_head(Duration::from_secs(71), f), false);
    //     });
    //
    //     tracing::info_span!("looking back for 2000 seconds").in_scope(|| {
    //         assert_eq!(portfolio.forall_from_head(Duration::from_secs(2000), f), false);
    //     });
    // }
}
