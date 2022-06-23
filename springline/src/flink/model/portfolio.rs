use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::ops::Deref;
use std::time::Duration;

use frunk::Semigroup;
use itertools::Itertools;
use oso::{Oso, PolarClass};
use pretty_snowflake::Id;
use proctor::elements::{Interval, PolicyContributor, Timestamp};
use proctor::error::PolicyError;
use proctor::{AppData, Correlation, ReceivedAt};
use validator::{Validate, ValidationError, ValidationErrors};

use super::MetricCatalog;

pub trait Portfolio: AppData {
    type Item: AppData;
    //todo: this needs to be rethought as builder would be appropriate; however this trait on the whole is a bit contrived.
    fn from_item(item: Self::Item, time_window: Duration, sufficient_coverage: f64) -> Result<Self, ValidationErrors>;
    fn time_window(&self) -> Duration;
    fn window_interval(&self) -> Option<Interval>;
    fn required_coverage(&self) -> f64;
    fn push(&mut self, item: Self::Item);
}

/// Portfolio of data objects used for range queries; e.g., has CPU utilization exceeded a threshold
/// for 5 minutes.
/// AppDataPortfolio has an invariant that there is always at least one item in the portfolio.
#[derive(Clone, PartialEq)]
pub struct AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    data: VecDeque<T>,
    pub time_window: Duration,
    pub sufficient_coverage: f64,
}

impl<T> Validate for AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    fn validate(&self) -> Result<(), ValidationErrors> {
        let checks = vec![
            Self::check_sufficient_coverage(self.sufficient_coverage).map_err(|err| ("insufficient coverage", err)),
            Self::check_nonempty(&self.data).map_err(|err| ("empty portfolio", err)),
        ];

        checks.into_iter().fold(Ok(()), |acc, check| match acc {
            Ok(_) => match check {
                Ok(_) => Ok(()),
                Err((label, err)) => {
                    let mut errors = ValidationErrors::new();
                    errors.add(label, err);
                    Err(errors)
                },
            },
            Err(mut errors) => match check {
                Ok(_) => Err(errors),
                Err((label, err)) => {
                    errors.add(label, err);
                    Err(errors)
                },
            },
        })
    }
}

impl<T> fmt::Debug for AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppDataPortfolio")
            .field("interval", &self.window_interval())
            .field("interval_duration", &self.window_interval().map(|w| w.duration()))
            .field("time_window", &self.time_window)
            .field("sufficient_coverage", &self.sufficient_coverage)
            .field("portfolio_size", &self.data.len())
            .field("head", &self.head().map(|c| c.recv_timestamp().to_string()))
            .finish()
    }
}

pub const DEFAULT_SUFFICIENT_COVERAGE: f64 = 0.8;

pub const fn default_sufficient_coverage() -> f64 {
    DEFAULT_SUFFICIENT_COVERAGE
}

impl<T> AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    fn check_sufficient_coverage(value: f64) -> Result<(), ValidationError> {
        if value <= 0.0 {
            Err(ValidationError::new("not enough window coverage to be meaningful."))
        } else if 1.0 < value {
            Err(ValidationError::new(
                "available telemetry window cannot meet sufficient coverage.",
            ))
        } else {
            Ok(())
        }
    }

    fn check_nonempty(data: &VecDeque<T>) -> Result<(), ValidationError> {
        if data.is_empty() {
            Err(ValidationError::new("portfolio data cannot be empty."))
        } else {
            Ok(())
        }
    }

    pub fn from_size(data: T, window: usize, interval: Duration) -> Self {
        let mut portfolio = VecDeque::with_capacity(window);
        portfolio.push_back(data);
        let time_window = interval * window as u32;
        let result = Self {
            data: portfolio,
            time_window,
            sufficient_coverage: DEFAULT_SUFFICIENT_COVERAGE,
        };
        result.validate().expect("portfolio parameters are not valid");
        result
    }

    pub fn from_time_window(data: T, time_window: Duration) -> Self {
        let mut portfolio = VecDeque::new();
        portfolio.push_back(data);
        let result = Self {
            data: portfolio,
            time_window,
            sufficient_coverage: DEFAULT_SUFFICIENT_COVERAGE,
        };
        result.validate().expect("portfolio parameters are not valid");
        result
    }

    pub fn builder() -> AppDataPortfolioBuilder<T> {
        AppDataPortfolioBuilder::default()
    }
}

impl<T: AppData + ReceivedAt> PolarClass for AppDataPortfolio<T> {}

impl<T: AppData + ReceivedAt> ReceivedAt for AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    fn recv_timestamp(&self) -> Timestamp {
        self.deref().recv_timestamp()
    }
}

impl PolicyContributor for AppDataPortfolio<MetricCatalog> {
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

impl<T> Correlation for AppDataPortfolio<T>
where
    T: AppData + ReceivedAt + Correlation<Correlated = T>,
{
    type Correlated = T;

    fn correlation(&self) -> &Id<Self::Correlated> {
        self.deref().correlation()
    }
}

impl<T> IntoIterator for AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    type IntoIter = std::collections::vec_deque::IntoIter<Self::Item>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl<T> AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    pub fn head(&self) -> Option<&T> {
        self.data.back()
    }

    pub fn history(&self) -> impl Iterator<Item = &T> {
        self.data.iter().rev()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn has_sufficient_coverage_looking_back_secs(&self, looking_back_secs: u32) -> bool {
        let head_ts = self.recv_timestamp();
        let looking_back = Duration::from_secs(looking_back_secs as u64);
        self.has_sufficient_coverage(Interval::new(head_ts - looking_back, head_ts).unwrap())
    }

    pub fn has_sufficient_coverage(&self, interval: Interval) -> bool {
        self.sufficient_coverage <= self.coverage_for(interval).0
    }

    pub fn coverage_for(&self, interval: Interval) -> (f64, impl Iterator<Item = &T>) {
        let mut coverage_start: Option<Timestamp> = None;
        let mut coverage_end: Option<Timestamp> = None;
        let mut range = Vec::new();
        for m in self.history() {
            let m_ts = m.recv_timestamp();
            if interval.contains_timestamp(m_ts) {
                coverage_start = Some(m_ts);
                if coverage_end.is_none() {
                    coverage_end = coverage_start;
                }
                tracing::trace!("portfolio catalog[{}] lies in {interval:?}", m_ts);
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
    pub fn extract_from_head<F, D>(&self, looking_back: Duration, extractor: F) -> Vec<D>
    where
        F: FnMut(&T) -> D,
    {
        self.head().map_or_else(Vec::new, |h| {
            let head_ts = h.recv_timestamp();
            self.extract_in_interval(Interval::new(head_ts - looking_back, head_ts).unwrap(), extractor)
                .collect()
        })
    }

    /// Extracts metric properties from the end of the interval (i.e., the youngest catalog
    /// contained in the interval) toward the past.
    #[tracing::instrument(level = "trace", skip(self, extractor))]
    pub fn extract_in_interval<F, D>(&self, interval: Interval, extractor: F) -> impl Iterator<Item = D>
    where
        F: FnMut(&T) -> D,
    {
        self.history()
            .take_while(|m| interval.contains_timestamp(m.recv_timestamp()))
            .map(extractor)
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    pub fn forall_from_head<F>(&self, looking_back: Duration, f: F) -> bool
    where
        F: FnMut(&T) -> bool,
    {
        self.head().map_or(false, |h| {
            let head_ts = h.recv_timestamp();
            self.forall_in_interval(Interval::new(head_ts - looking_back, head_ts).unwrap(), f)
        })
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    pub fn forall_in_interval<F>(&self, interval: Interval, mut f: F) -> bool
    where
        F: FnMut(&T) -> bool,
    {
        if self.is_empty() {
            tracing::debug!("empty portfolio");
            false
        } else if self.len() == 1 && interval.duration() == Duration::ZERO {
            tracing::debug!("single metric portfolio");
            interval.contains_timestamp(self.recv_timestamp()) && f(self)
        } else {
            tracing::debug!(portfolio_window=?self.window_interval(), ?interval, "Checking for interval");

            let (coverage, range_iter) = self.coverage_for(interval);
            if coverage < self.sufficient_coverage {
                tracing::debug!(
                    ?interval,
                    interval_duration=?interval.duration(),
                    included_range=?range_iter.map(|m| m.recv_timestamp().to_string()).collect::<Vec<_>>(),
                    %coverage, coverage_duration=?(interval.duration().mul_f64(coverage)),
                    "not enough coverage for meaningful evaluation."

                );
                return false;
            }

            let range: Vec<&T> = range_iter.collect();
            tracing::debug!(
                range=?range.iter().map(|m| m.recv_timestamp().to_string()).collect::<Vec<_>>(),
                ?interval,
                interval_duration=?interval.duration(),
                %coverage, coverage_duration=?(interval.duration().mul_f64(coverage)),
                "evaluating for interval: {interval:?}",
            );

            range.into_iter().all(f)
        }
    }
}

impl<T> Portfolio for AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    type Item = T;

    fn window_interval(&self) -> Option<Interval> {
        // portfolio vecdeque runs oldest to youngest
        let start = self.data.front().map(|m| m.recv_timestamp());
        let end = self.data.back().map(|m| m.recv_timestamp());
        start.zip(end).map(|i| {
            i.try_into()
                .expect("portfolio represents invalid interval (end before start): {i:?}")
        })
    }

    fn time_window(&self) -> Duration {
        self.time_window
    }

    fn push(&mut self, data: T) {
        // todo: assumes monotonically increasing recv_timestamp -- check if not true and insert accordingly
        // or sort after push and before pop?
        let oldest_allowed = data.recv_timestamp() - self.time_window;

        self.data.push_back(data);

        while let Some(metric) = self.data.front() {
            if metric.recv_timestamp() < oldest_allowed {
                let too_old = self.data.pop_front();
                tracing::debug!(?too_old, %oldest_allowed, "popping metric outside of time window");
            } else {
                break;
            }
        }
    }

    fn required_coverage(&self) -> f64 {
        self.sufficient_coverage
    }

    fn from_item(item: Self::Item, time_window: Duration, sufficient_coverage: f64) -> Result<Self, ValidationErrors> {
        Self::builder()
            .with_item(item)
            .with_time_window(time_window)
            .with_sufficient_coverage(sufficient_coverage)
            .build()
    }
}

impl AppDataPortfolio<MetricCatalog> {
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

impl<T> std::ops::Deref for AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.head().expect("failed nonempty AppDataPortfolio invariant")
    }
}

impl<T> std::ops::Add<T> for AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    type Output = Self;

    fn add(mut self, rhs: T) -> Self::Output {
        self.push(rhs);
        self
    }
}

impl<T> std::ops::Add for AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        self.combine(&rhs)
    }
}

impl<T> Semigroup for AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    #[tracing::instrument(level = "trace")]
    fn combine(&self, other: &Self) -> Self {
        let book = Self::do_ordered_combine(self, other);
        let required_coverage = self.sufficient_coverage.max(other.sufficient_coverage);
        Self {
            data: book,
            time_window: self.time_window,
            sufficient_coverage: required_coverage,
        }
    }
}

impl<T> AppDataPortfolio<T>
where
    T: AppData + ReceivedAt,
{
    #[tracing::instrument(level = "trace", skip(older, younger))]
    fn block_combine(time_window: Duration, older: &VecDeque<T>, younger: &VecDeque<T>) -> VecDeque<T> {
        younger.back().map_or_else(
            || older.clone(), // if younger has no back, then it must be empty.
            |youngest| {
                let cutoff = youngest.recv_timestamp() - time_window;
                let older_iter = older.iter().cloned();
                let younger_iter = younger.iter().cloned();
                let result: VecDeque<T> = older_iter
                    .chain(younger_iter)
                    .filter(|m| cutoff <= m.recv_timestamp())
                    .collect();

                tracing::error!(
                    ?time_window,
                    older=?older.iter().map(|m| m.recv_timestamp().to_string()).collect::<Vec<_>>(),
                    younger=?younger.iter().map(|m| m.recv_timestamp().to_string()).collect::<Vec<_>>(),
                    ?youngest, ?cutoff,
                    "filtering catalogs by time window: {:?}",
                    result.iter().map(|m| m.recv_timestamp().to_string()).collect::<Vec<_>>(),
                );

                result
            },
        )
    }

    #[tracing::instrument(level = "trace", skip(lhs_portfolio, rhs_portfolio))]
    fn do_ordered_combine(lhs_portfolio: &Self, rhs_portfolio: &Self) -> VecDeque<T> {
        let (lhs_interval, rhs_interval) = match (lhs_portfolio.window_interval(), rhs_portfolio.window_interval()) {
            (None, None) => return VecDeque::new(),
            (Some(_), None) => return lhs_portfolio.data.clone(),
            (None, Some(_)) => return rhs_portfolio.data.clone(),
            (Some(lhs), Some(rhs)) => (lhs, rhs),
        };

        let time_window = lhs_portfolio.time_window;
        if lhs_interval.is_before(rhs_interval) {
            Self::block_combine(time_window, &lhs_portfolio.data, &rhs_portfolio.data)
        } else if rhs_interval.is_before(lhs_interval) {
            Self::block_combine(time_window, &rhs_portfolio.data, &lhs_portfolio.data)
        } else {
            tracing::trace_span!("interspersed combination", ?time_window).in_scope(|| {
                let mut combined = lhs_portfolio
                    .data
                    .iter()
                    .chain(rhs_portfolio.data.iter())
                    .sorted_by(|lhs, rhs| lhs.recv_timestamp().cmp(&rhs.recv_timestamp()))
                    .cloned()
                    .collect::<VecDeque<_>>();

                tracing::trace!(
                    "combined portfolio: {:?}",
                    combined.iter().map(|m| m.recv_timestamp().to_string()).collect::<Vec<_>>()
                );
                if let Some(cutoff) = combined.back().map(|m| m.recv_timestamp() - time_window) {
                    tracing::debug!("cutoff: {cutoff:?}");
                    combined.retain(|m| cutoff <= m.recv_timestamp());
                }

                tracing::debug!(
                    "final combined: {:?}",
                    combined.iter().map(|m| m.recv_timestamp().to_string()).collect::<Vec<_>>()
                );
                combined
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Validate)]
pub struct AppDataPortfolioBuilder<T>
where
    T: AppData + ReceivedAt,
{
    data: Option<VecDeque<T>>,
    time_window: Option<Duration>,
    #[validate(custom = "AppDataPortfolio::<T>::check_sufficient_coverage")]
    sufficient_coverage: Option<f64>,
}

impl<T> Default for AppDataPortfolioBuilder<T>
where
    T: AppData + ReceivedAt,
{
    fn default() -> Self {
        Self {
            data: None,
            time_window: None,
            sufficient_coverage: None,
        }
    }
}

impl<T> AppDataPortfolioBuilder<T>
where
    T: AppData + ReceivedAt,
{
    pub fn with_item(mut self, item: T) -> Self {
        self.push(item);
        self
    }

    pub fn with_items(mut self, items: impl IntoIterator<Item = T>) -> Self {
        for item in items {
            self.push(item);
        }
        self
    }

    pub const fn with_time_window(mut self, time_window: Duration) -> Self {
        self.time_window = Some(time_window);
        self
    }

    pub fn with_size_and_interval(mut self, size: usize, interval: Duration) -> Self {
        self.time_window = Some(interval * size as u32);
        self
    }

    pub const fn with_sufficient_coverage(mut self, sufficient_coverage: f64) -> Self {
        self.sufficient_coverage = Some(sufficient_coverage);
        self
    }

    pub fn push(&mut self, data: T) -> &mut Self {
        if let Some(items) = &mut self.data {
            items.push_back(data);
        } else {
            let mut items = VecDeque::new();
            items.push_back(data);
            self.data = Some(items);
        }

        self
    }

    pub fn is_empty(&self) -> bool {
        self.data.as_ref().map_or(true, |d| d.is_empty())
    }

    pub fn len(&self) -> usize {
        self.data.as_ref().map_or(0, |d| d.len())
    }

    pub fn build(self) -> Result<AppDataPortfolio<T>, ValidationErrors> {
        let mut portfolio: Vec<T> = self.data.map_or(Vec::default(), |d| d.into_iter().collect());
        portfolio.sort_by_key(|item| item.recv_timestamp());
        let result = AppDataPortfolio {
            data: portfolio.into_iter().collect(),
            time_window: self.time_window.expect("must supply time window before final build"),
            sufficient_coverage: self
                .sufficient_coverage
                .expect("must supply required time window coverage"),
        };
        result.validate()?;
        Ok(result)
    }
}
