use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::time::Duration;

use frunk::{Monoid, Semigroup};
use itertools::Itertools;
use oso::{Oso, PolarClass};
use pretty_snowflake::Id;
use proctor::elements::{Interval, PolicyContributor, Timestamp};
use proctor::error::PolicyError;
use proctor::{AppData, Correlation};
use serde::{Deserialize, Serialize};

use super::MetricCatalog;


pub trait Portfolio: AppData + Monoid {
    type Item: AppData;
    fn time_window(&self) -> Duration;
    fn set_time_window(&mut self, time_window: Duration);
    fn window_interval(&self) -> Option<Interval>;
    fn push(&mut self, item: Self::Item);
}

#[derive(PolarClass, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricPortfolio {
    portfolio: VecDeque<MetricCatalog>,
    pub time_window: Duration,
}

impl fmt::Debug for MetricPortfolio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricPortfolio")
            .field("interval", &self.window_interval())
            .field("interval_duration", &self.window_interval().map(|w| w.duration()))
            .field("time_window", &self.time_window)
            .field("portfolio_size", &self.portfolio.len())
            .field("head", &self.head().map(|c| c.recv_timestamp.to_string()))
            // .field("portfolio", &self.portfolio.iter().rev().map(|m| m.recv_timestamp.to_string()).collect::<Vec<_>>())
            // .field("head", self.deref())
            .finish()
    }
}

impl MetricPortfolio {
    pub fn from_size(metrics: MetricCatalog, window: usize, interval: Duration) -> Self {
        let mut portfolio = VecDeque::with_capacity(window);
        portfolio.push_back(metrics);
        let time_window = interval * window as u32;
        Self { portfolio, time_window }
    }

    pub fn from_time_window(metrics: MetricCatalog, time_window: Duration) -> Self {
        let mut portfolio = VecDeque::new();
        portfolio.push_back(metrics);
        Self { portfolio, time_window }
    }

    pub fn builder() -> MetricPortfolioBuilder {
        MetricPortfolioBuilder::default()
    }
}

impl PolicyContributor for MetricPortfolio {
    #[tracing::instrument(level = "trace", skip(engine))]
    fn register_with_policy_engine(engine: &mut Oso) -> Result<(), PolicyError> {
        MetricCatalog::register_with_policy_engine(engine)?;

        engine.register_class(
            Self::get_polar_class_builder()
                .add_attribute_getter("recv_timestamp", |p| p.recv_timestamp)
                .add_attribute_getter("health", |p| p.health.clone())
                .add_attribute_getter("flow", |p| p.flow.clone())
                .add_attribute_getter("cluster", |p| p.cluster.clone())
                .add_attribute_getter("custom", |p| p.custom.clone())
                .add_method(
                    "has_sufficient_coverage_looking_back_secs",
                    Self::has_sufficient_coverage_looking_back_secs,
                )
                .add_method(
                    "flow_input_records_lag_max_below_mark",
                    Self::flow_input_records_lag_max_below_mark,
                )
                .add_method(
                    "flow_input_records_lag_max_above_mark",
                    Self::flow_input_records_lag_max_above_mark,
                )
                .add_method(
                    "flow_input_millis_behind_latest_below_mark",
                    Self::flow_input_millis_behind_latest_below_mark,
                )
                .add_method(
                    "flow_input_millis_behind_latest_above_mark",
                    Self::flow_input_millis_behind_latest_above_mark,
                )
                .add_method(
                    "cluster_task_cpu_load_below_mark",
                    Self::cluster_task_cpu_load_below_mark,
                )
                .add_method(
                    "cluster_task_cpu_load_above_mark",
                    Self::cluster_task_cpu_load_above_mark,
                )
                .add_method(
                    "cluster_task_heap_memory_used_below_mark",
                    Self::cluster_task_heap_memory_used_below_mark,
                )
                .add_method(
                    "cluster_task_heap_memory_used_above_mark",
                    Self::cluster_task_heap_memory_used_above_mark,
                )
                .add_method(
                    "cluster_task_memory_load_below_mark",
                    Self::cluster_task_heap_memory_load_below_mark,
                )
                .add_method(
                    "cluster_task_memory_load_above_mark",
                    Self::cluster_task_heap_memory_load_above_mark,
                )
                .build(),
        )?;

        Ok(())
    }
}

impl Correlation for MetricPortfolio {
    type Correlated = MetricCatalog;

    fn correlation(&self) -> &Id<Self::Correlated> {
        &self.correlation_id
    }
}

impl IntoIterator for MetricPortfolio {
    type IntoIter = std::collections::vec_deque::IntoIter<Self::Item>;
    type Item = MetricCatalog;

    fn into_iter(self) -> Self::IntoIter {
        self.portfolio.into_iter()
    }
}

impl MetricPortfolio {
    pub fn head(&self) -> Option<&MetricCatalog> {
        self.portfolio.back()
    }

    pub fn history(&self) -> impl Iterator<Item = &MetricCatalog> {
        self.portfolio.iter().rev()
    }

    pub fn is_empty(&self) -> bool {
        self.portfolio.is_empty()
    }

    pub fn len(&self) -> usize {
        self.portfolio.len()
    }

    pub fn has_sufficient_coverage_looking_back_secs(&self, looking_back_secs: u32) -> bool {
        let head_ts = self.recv_timestamp;
        let looking_back = Duration::from_secs(looking_back_secs as u64);
        self.has_sufficient_coverage(Interval::new(head_ts - looking_back, head_ts).unwrap())
    }

    pub fn has_sufficient_coverage(&self, interval: Interval) -> bool {
        let coverage = self.coverage_for(interval).0;
        0.5 <= coverage
    }

    pub fn coverage_for(&self, interval: Interval) -> (f64, impl Iterator<Item = &MetricCatalog>) {
        let mut coverage_start: Option<Timestamp> = None;
        let mut coverage_end: Option<Timestamp> = None;
        let mut range = Vec::new();
        for m in self.history() {
            if interval.contains_timestamp(m.recv_timestamp) {
                coverage_start = Some(m.recv_timestamp);
                if coverage_end.is_none() {
                    coverage_end = coverage_start;
                }
                tracing::trace!("portfolio catalog[{}] lies in {interval:?}", m.recv_timestamp);
                range.push(m);
            }
        }

        let coverage_interval = coverage_start
            .zip(coverage_end)
            .map(|(start, end)| Interval::new(start, end))
            .transpose()
            .unwrap_or_else(|err| {
                tracing::warn!(error=?err, "portfolio represents an invalid interval - using None");
                None
            });

        let coverage = coverage_interval
            .map(|coverage| coverage.duration().as_secs_f64() / interval.duration().as_secs_f64())
            .unwrap_or(0.0);

        (coverage, range.into_iter())
    }

    /// Extracts metric properties from the head of the portfolio (i.e., the current catalog) toward
    /// the past.
    #[tracing::instrument(level = "trace", skip(self, extractor))]
    pub fn extract_from_head<F, T>(&self, looking_back: Duration, extractor: F) -> Vec<T>
    where
        F: FnMut(&MetricCatalog) -> T,
    {
        self.head().map_or_else(Vec::new, |h| {
            let head_ts = h.recv_timestamp;
            self.extract_in_interval(Interval::new(head_ts - looking_back, head_ts).unwrap(), extractor)
                .collect()
        })
    }

    /// Extracts metric properties from the end of the interval (i.e., the youngest catalog
    /// contained in the interval) toward the past.
    #[tracing::instrument(level = "trace", skip(self, extractor))]
    pub fn extract_in_interval<F, T>(&self, interval: Interval, extractor: F) -> impl Iterator<Item = T>
    where
        F: FnMut(&MetricCatalog) -> T,
    {
        self.history()
            .take_while(|m| interval.contains_timestamp(m.recv_timestamp))
            .map(extractor)
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    pub fn forall_from_head<F>(&self, looking_back: Duration, f: F) -> bool
    where
        F: FnMut(&MetricCatalog) -> bool,
    {
        self.head().map_or(false, |h| {
            let head_ts = h.recv_timestamp;
            self.forall_in_interval(Interval::new(head_ts - looking_back, head_ts).unwrap(), f)
        })
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    pub fn forall_in_interval<F>(&self, interval: Interval, mut f: F) -> bool
    where
        F: FnMut(&MetricCatalog) -> bool,
    {
        if self.is_empty() {
            tracing::debug!("empty portfolio");
            false
        } else if self.len() == 1 && interval.duration() == Duration::ZERO {
            tracing::debug!("single metric portfolio");
            interval.contains_timestamp(self.recv_timestamp) && f(self)
        } else {
            tracing::debug!(portfolio_window=?self.window_interval(), ?interval, "Checking for interval");

            let (coverage, range_iter) = self.coverage_for(interval);
            if coverage < 0.5 {
                tracing::debug!(
                    ?interval,
                    interval_duration=?interval.duration(),
                    included_range=?range_iter.map(|m| m.recv_timestamp.to_string()).collect::<Vec<_>>(),
                    %coverage, coverage_duration=?(interval.duration().mul_f64(coverage)),
                    "not enough coverage for meaningful evaluation."

                );
                return false;
            }

            let range: Vec<&MetricCatalog> = range_iter.collect();
            tracing::debug!(
                range=?range.iter().map(|m| (m.recv_timestamp.to_string(), m.flow.records_in_per_sec)).collect::<Vec<_>>(),
                ?interval,
                interval_duration=?interval.duration(),
                %coverage, coverage_duration=?(interval.duration().mul_f64(coverage)),
                "evaluating for interval: {interval:?}",
            );

            range.into_iter().all(f)
        }
    }
}

impl Portfolio for MetricPortfolio {
    type Item = MetricCatalog;

    fn window_interval(&self) -> Option<Interval> {
        // portfolio runs oldest to youngest
        let start = self.portfolio.front().map(|m| m.recv_timestamp);
        let end = self.portfolio.back().map(|m| m.recv_timestamp);
        start.zip(end).map(|i| {
            i.try_into()
                .expect("portfolio represents invalid interval (end before start): {i:?}")
        })
    }

    fn time_window(&self) -> Duration {
        self.time_window
    }

    fn set_time_window(&mut self, time_window: Duration) {
        self.time_window = time_window;
    }

    fn push(&mut self, metrics: MetricCatalog) {
        // todo: assumes monotonically increasing recv_timestamp -- check if not true and insert accordingly
        // or sort after push and before pop?
        let oldest_allowed = metrics.recv_timestamp - self.time_window;

        self.portfolio.push_back(metrics);

        while let Some(metric) = self.portfolio.front() {
            if metric.recv_timestamp < oldest_allowed {
                let too_old = self.portfolio.pop_front();
                tracing::debug!(?too_old, %oldest_allowed, "popping metric outside of time window");
            } else {
                break;
            }
        }
    }
}

impl MetricPortfolio {
    pub fn flow_input_records_lag_max_below_mark(&self, looking_back_secs: u32, max_value: i64) -> bool {
        self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
            m.flow
                .input_records_lag_max
                .map(|lag| {
                    tracing::debug!(
                        "eval lag in catalog[{}]: {lag} <= {max_value} = {}",
                        m.recv_timestamp,
                        lag <= max_value
                    );
                    lag <= max_value
                })
                .unwrap_or(false)
        })
    }

    pub fn flow_input_records_lag_max_above_mark(&self, looking_back_secs: u32, min_value: i64) -> bool {
        self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
            m.flow
                .input_records_lag_max
                .map(|lag| {
                    tracing::debug!(
                        "eval lag in catalog[{}]: {min_value} <= {lag} = {}",
                        m.recv_timestamp,
                        min_value <= lag
                    );
                    min_value <= lag
                })
                .unwrap_or(false)
        })
    }

    pub fn flow_input_millis_behind_latest_below_mark(&self, looking_back_secs: u32, max_value: i64) -> bool {
        self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
            m.flow
                .input_millis_behind_latest
                .map(|millis_behind_latest| {
                    tracing::debug!(
                        "eval millis_behind_latest in catalog[{}]: {millis_behind_latest} <= {max_value} = {}",
                        m.recv_timestamp,
                        millis_behind_latest <= max_value
                    );
                    millis_behind_latest <= max_value
                })
                .unwrap_or(false)
        })
    }

    pub fn flow_input_millis_behind_latest_above_mark(&self, looking_back_secs: u32, min_value: i64) -> bool {
        self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
            m.flow
                .input_millis_behind_latest
                .map(|millis_behind_latest| {
                    tracing::debug!(
                        "eval millis_behind_latest in catalog[{}]: {min_value} <= {millis_behind_latest} = {}",
                        m.recv_timestamp,
                        min_value <= millis_behind_latest
                    );
                    min_value <= millis_behind_latest
                })
                .unwrap_or(false)
        })
    }

    pub fn cluster_task_cpu_load_below_mark(&self, looking_back_secs: u32, max_value: f64) -> bool {
        self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
            tracing::debug!(
                "eval task_cpu_load in catalog[{}]: {} <= {max_value} = {}",
                m.recv_timestamp,
                m.cluster.task_cpu_load,
                m.cluster.task_cpu_load <= max_value
            );
            m.cluster.task_cpu_load <= max_value
        })
    }

    pub fn cluster_task_cpu_load_above_mark(&self, looking_back_secs: u32, min_value: f64) -> bool {
        self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
            tracing::debug!(
                "eval task_cpu_load in catalog[{}]: {min_value} <= {} = {}",
                m.recv_timestamp,
                m.cluster.task_cpu_load,
                min_value <= m.cluster.task_cpu_load
            );
            min_value <= m.cluster.task_cpu_load
        })
    }

    pub fn cluster_task_heap_memory_used_below_mark(&self, looking_back_secs: u32, max_value: f64) -> bool {
        self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
            tracing::debug!(
                "eval task_heap_memory_used in catalog[{}]: {} <= {max_value} = {}",
                m.recv_timestamp,
                m.cluster.task_heap_memory_used,
                m.cluster.task_heap_memory_used <= max_value
            );
            m.cluster.task_heap_memory_used <= max_value
        })
    }

    pub fn cluster_task_heap_memory_used_above_mark(&self, looking_back_secs: u32, min_value: f64) -> bool {
        self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
            tracing::debug!(
                "eval task_heap_memory_used in catalog[{}]: {min_value} <= {} = {}",
                m.recv_timestamp,
                m.cluster.task_heap_memory_used,
                min_value <= m.cluster.task_heap_memory_used
            );
            min_value <= m.cluster.task_heap_memory_used
        })
    }

    pub fn cluster_task_heap_memory_load_below_mark(&self, looking_back_secs: u32, max_value: f64) -> bool {
        self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
            tracing::debug!(
                "eval task_heap_memory_load in catalog[{}]: {} <= {max_value} = {}",
                m.recv_timestamp,
                m.cluster.task_cpu_load,
                m.cluster.task_cpu_load <= max_value
            );
            m.cluster.task_heap_memory_load() <= max_value
        })
    }

    pub fn cluster_task_heap_memory_load_above_mark(&self, looking_back_secs: u32, min_value: f64) -> bool {
        self.forall_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
            tracing::debug!(
                "eval task_heap_memory_load in catalog[{}]: {min_value} <= {} = {}",
                m.recv_timestamp,
                m.cluster.task_cpu_load,
                min_value <= m.cluster.task_cpu_load
            );
            min_value <= m.cluster.task_heap_memory_load()
        })
    }
}

impl std::ops::Deref for MetricPortfolio {
    type Target = MetricCatalog;

    fn deref(&self) -> &Self::Target {
        self.head().unwrap_or(&super::catalog::EMPTY_METRIC_CATALOG)
    }
}

impl std::ops::Add<MetricCatalog> for MetricPortfolio {
    type Output = Self;

    fn add(mut self, rhs: MetricCatalog) -> Self::Output {
        self.push(rhs);
        self
    }
}

impl std::ops::Add for MetricPortfolio {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        self.combine(&rhs)
    }
}

impl Monoid for MetricPortfolio {
    fn empty() -> Self {
        Self {
            portfolio: VecDeque::new(),
            time_window: Duration::ZERO,
        }
    }
}

impl Semigroup for MetricPortfolio {
    #[tracing::instrument(level = "trace")]
    fn combine(&self, other: &Self) -> Self {
        let book = Self::do_ordered_combine(self, other);
        Self { portfolio: book, time_window: self.time_window }
    }
}

impl MetricPortfolio {
    #[tracing::instrument(level = "trace", skip(older, younger))]
    fn block_combine(
        time_window: Duration, older: &VecDeque<MetricCatalog>, younger: &VecDeque<MetricCatalog>,
    ) -> VecDeque<MetricCatalog> {
        younger.back().map_or_else(
            || older.clone(), // if younger has no back, then it must be empty.
            |youngest| {
                let cutoff = youngest.recv_timestamp - time_window;
                let older_iter = older.iter().cloned();
                let younger_iter = younger.iter().cloned();
                let result: VecDeque<MetricCatalog> = older_iter
                    .chain(younger_iter)
                    .filter(|m| cutoff <= m.recv_timestamp)
                    .collect();

                tracing::error!(
                    ?time_window,
                    older=?older.iter().map(|m| m.recv_timestamp).collect::<Vec<_>>(),
                    younger=?younger.iter().map(|m| m.recv_timestamp).collect::<Vec<_>>(),
                    ?youngest, ?cutoff,
                    "filtering catalogs by time window: {:?}",
                    result.iter().map(|m| m.recv_timestamp).collect::<Vec<_>>(),
                );

                result
            },
        )
    }

    #[tracing::instrument(level = "trace", skip(lhs_portfolio, rhs_portfolio))]
    fn do_ordered_combine(lhs_portfolio: &Self, rhs_portfolio: &Self) -> VecDeque<MetricCatalog> {
        let (lhs_interval, rhs_interval) = match (lhs_portfolio.window_interval(), rhs_portfolio.window_interval()) {
            (None, None) => return VecDeque::new(),
            (Some(_), None) => return lhs_portfolio.portfolio.clone(),
            (None, Some(_)) => return rhs_portfolio.portfolio.clone(),
            (Some(lhs), Some(rhs)) => (lhs, rhs),
        };

        let time_window = lhs_portfolio.time_window;
        if lhs_interval.is_before(rhs_interval) {
            Self::block_combine(time_window, &lhs_portfolio.portfolio, &rhs_portfolio.portfolio)
        } else if rhs_interval.is_before(lhs_interval) {
            Self::block_combine(time_window, &rhs_portfolio.portfolio, &lhs_portfolio.portfolio)
        } else {
            tracing::trace_span!("interspersed combination", ?time_window).in_scope(|| {
                let mut combined = lhs_portfolio
                    .portfolio
                    .iter()
                    .chain(rhs_portfolio.portfolio.iter())
                    .sorted_by(|lhs, rhs| lhs.recv_timestamp.cmp(&rhs.recv_timestamp))
                    .cloned()
                    .collect::<VecDeque<_>>();

                tracing::trace!(
                    "combined portfolio: {:?}",
                    combined.iter().map(|m| m.recv_timestamp).collect::<Vec<_>>()
                );
                if let Some(cutoff) = combined.back().map(|m| m.recv_timestamp - time_window) {
                    tracing::debug!("cutoff: {cutoff:?}");
                    combined.retain(|m| cutoff <= m.recv_timestamp);
                }

                tracing::debug!(
                    "final combined: {:?}",
                    combined.iter().map(|m| m.recv_timestamp).collect::<Vec<_>>()
                );
                combined
            })
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct MetricPortfolioBuilder {
    metrics: VecDeque<MetricCatalog>,
    time_window: Option<Duration>,
}

impl MetricPortfolioBuilder {
    pub const fn with_time_window(mut self, time_window: Duration) -> Self {
        self.time_window = Some(time_window);
        self
    }

    pub fn with_size_and_interval(mut self, size: usize, interval: Duration) -> Self {
        self.time_window = Some(interval * size as u32);
        self
    }

    pub fn push(&mut self, catalog: MetricCatalog) -> &mut Self {
        self.metrics.push_back(catalog);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }

    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    pub fn build(self) -> MetricPortfolio {
        let mut portfolio: Vec<MetricCatalog> = self.metrics.into_iter().collect();
        portfolio.sort_by(|lhs, rhs| lhs.recv_timestamp.cmp(&rhs.recv_timestamp)); // sort to reverse

        MetricPortfolio {
            portfolio: portfolio.into_iter().collect(),
            time_window: self.time_window.expect("must supply time window before final build"),
        }
    }
}
