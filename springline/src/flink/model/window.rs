use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fmt::{self, Debug};
use std::ops::Deref;
use std::time::Duration;

use frunk::{Monoid, Semigroup};
use itertools::Itertools;
use once_cell::sync::Lazy;
use oso::{Oso, PolarClass};
use pretty_snowflake::Id;
use proctor::elements::{Interval, PolicyContributor, Timestamp};
use proctor::error::PolicyError;
use proctor::{AppData, Correlation, ReceivedAt};
use prometheus::{Gauge, Opts};
use validator::{Validate, ValidationError, ValidationErrors};

use super::MetricCatalog;

pub trait Window: Sized {
    type Item: AppData;
    //todo: this needs to be rethought as builder would be appropriate; however this trait on the whole is a bit contrived.
    fn from_item(item: Self::Item, time_window: Duration, quorum_percentage: f64) -> Result<Self, ValidationErrors>;
    fn time_window(&self) -> Duration;
    fn window_interval(&self) -> Option<Interval>;
    fn push(&mut self, item: Self::Item);
}

pub trait UpdateWindowMetrics: Window {
    fn update_metrics(&self);
}

/// Window of data objects used for range queries; e.g., has CPU utilization exceeded a threshold
/// for 5 minutes.
/// `AppDataWindow` has an invariant that there is always at least one item in the window..
#[derive(Clone, PartialEq)]
pub struct AppDataWindow<T>
where
    T: AppData + ReceivedAt,
{
    data: VecDeque<T>,
    pub time_window: Duration,
    pub quorum_percentage: f64,
}

impl<T> Validate for AppDataWindow<T>
where
    T: AppData + ReceivedAt,
{
    fn validate(&self) -> Result<(), ValidationErrors> {
        let checks = vec![
            Self::check_quorum_percentage(self.quorum_percentage).map_err(|err| ("insufficient quorum", err)),
            Self::check_nonempty(&self.data).map_err(|err| ("empty window", err)),
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

impl Debug for AppDataWindow<MetricCatalog> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppDataWindow")
            .field("interval", &self.window_interval())
            .field("interval_duration", &self.window_interval().map(|w| w.duration()))
            .field("time_window", &self.time_window)
            .field("quorum_percentage", &self.quorum_percentage)
            .field("window_size", &self.data.len())
            .field("head", &self.head().map(|c| c.recv_timestamp().to_string()))
            .field(
                "1_min_flow_source_relative_lag_change_rate",
                &self.flow_source_relative_lag_change_rate(60),
            )
            .finish()
    }
}

pub const DEFAULT_QUORUM_PERCENTAGE: f64 = 0.8;

pub const fn default_quorum_percentage() -> f64 {
    DEFAULT_QUORUM_PERCENTAGE
}

impl<T> AppDataWindow<T>
where
    T: AppData + ReceivedAt,
{
    fn check_quorum_percentage(percentage: f64) -> Result<(), ValidationError> {
        if percentage <= 0.0 {
            Err(ValidationError::new(
                "not enough window quorum coverage to be meaningful.",
            ))
        } else if 1.0 < percentage {
            Err(ValidationError::new(
                "impossible for telemetry window to meet quorum coverage requirement.",
            ))
        } else {
            Ok(())
        }
    }

    fn check_nonempty(data: &VecDeque<T>) -> Result<(), ValidationError> {
        if data.is_empty() {
            Err(ValidationError::new("window data cannot be empty."))
        } else {
            Ok(())
        }
    }

    pub fn from_size(data: T, window_size: usize, interval: Duration) -> Self {
        let mut window = VecDeque::with_capacity(window_size);
        window.push_back(data);
        let time_window = interval * window_size as u32;
        let result = Self {
            data: window,
            time_window,
            quorum_percentage: DEFAULT_QUORUM_PERCENTAGE,
        };
        result.validate().expect("window parameters are not valid");
        result
    }

    pub fn from_time_window(data: T, time_window: Duration) -> Self {
        let mut window = VecDeque::new();
        window.push_back(data);
        let result = Self {
            data: window,
            time_window,
            quorum_percentage: DEFAULT_QUORUM_PERCENTAGE,
        };
        result.validate().expect("window parameters are not valid");
        result
    }

    pub fn builder() -> AppDataWindowBuilder<T> {
        AppDataWindowBuilder::default()
    }
}

impl<T: AppData + ReceivedAt> PolarClass for AppDataWindow<T> {}

impl<T: AppData + ReceivedAt> ReceivedAt for AppDataWindow<T>
where
    T: AppData + ReceivedAt,
{
    fn recv_timestamp(&self) -> Timestamp {
        self.deref().recv_timestamp()
    }
}

// macro_rules! add_methods {
//     ($polar_class:expr, $($name:ident)*) => {
//         $polar_class
//         $(
//             .add_method(
//                 stringify!(::paste::paste! { [<$name _rolling_average>] }),
//                 ::paste::paste! { [<Self::$name _rolling_average>] },
//             )
//             .add_method(
//                 stringify!(::paste::paste! { [<$name _rolling_change_per_sec>] }),
//                 ::paste::paste! { [<Self::$name _rolling_change_per_sec>] },
//             )
//             .add_method(
//                 stringify!(::paste::paste! { [<$name _below_threshold>] }),
//                 ::paste::paste! { [<Self::$name _below_threshold>] },
//             )
//             .add_method(
//                 stringify!(::paste::paste! { [<$name _above_threshold>] }),
//                 ::paste::paste! { [<Self::$name _above_threshold>] },
//             )
//         )
//         .build()
//     }
// }

impl PolicyContributor for AppDataWindow<MetricCatalog> {
    #[tracing::instrument(level = "trace", skip(engine))]
    fn register_with_policy_engine(engine: &mut Oso) -> Result<(), PolicyError> {
        MetricCatalog::register_with_policy_engine(engine)?;

        let builder = Self::get_polar_class_builder()
            .add_attribute_getter("recv_timestamp", |p| p.recv_timestamp)
            .add_attribute_getter("health", |p| p.health.clone())
            .add_attribute_getter("flow", |p| p.flow.clone())
            .add_attribute_getter("cluster", |p| p.cluster.clone())
            .add_attribute_getter("custom", |p| p.custom.clone())
            .add_method("has_quorum_looking_back_secs", Self::has_quorum_looking_back_secs);

        // engine.register_class(
        //     add_methods!(builder,
        //         flow_idle_time_millis
        //         // flow_task_utilization
        //         // flow_source_back_pressured_time_millis_per_sec
        //         // flow_source_back_pressure_percentage
        //         // flow_source_records_lag_max
        //         // flow_source_total_lag
        //         // flow_source_records_consumed_rate
        //         // flow_source_relative_lag
        //         // flow_source_millis_behind_latest
        //         // flow_records_out_per_sec
        //         // cluster_task_cpu_load
        //         // cluster_task_heap_memory_used
        //         // cluster_task_heap_memory_load
        //     )
        // )?;

        engine.register_class(
            builder
                .add_method(
                    "flow_idle_time_millis_per_sec_rolling_average",
                    Self::flow_idle_time_millis_per_sec_rolling_average,
                )
                .add_method(
                    "flow_idle_time_millis_per_sec_rolling_change_per_sec",
                    Self::flow_idle_time_millis_per_sec_rolling_change_per_sec,
                )
                .add_method(
                    "flow_idle_time_millis_per_sec_below_threshold",
                    Self::flow_idle_time_millis_per_sec_below_threshold,
                )
                .add_method(
                    "flow_idle_time_millis_per_sec_above_threshold",
                    Self::flow_idle_time_millis_per_sec_above_threshold,
                )
                .add_method(
                    "flow_task_utilization_rolling_average",
                    Self::flow_task_utilization_rolling_average,
                )
                .add_method(
                    "flow_task_utilization_rolling_change_per_sec",
                    Self::flow_task_utilization_rolling_change_per_sec,
                )
                .add_method(
                    "flow_task_utilization_below_threshold",
                    Self::flow_task_utilization_below_threshold,
                )
                .add_method(
                    "flow_task_utilization_above_threshold",
                    Self::flow_task_utilization_above_threshold,
                )
                .add_method(
                    "flow_source_back_pressured_time_millis_per_sec_rolling_average",
                    Self::flow_source_back_pressured_time_millis_per_sec_rolling_average,
                )
                .add_method(
                    "flow_source_back_pressured_time_millis_per_sec_rolling_change_per_sec",
                    Self::flow_source_back_pressured_time_millis_per_sec_rolling_change_per_sec,
                )
                .add_method(
                    "flow_source_back_pressured_time_millis_per_sec_below_threshold",
                    Self::flow_source_back_pressured_time_millis_per_sec_below_threshold,
                )
                .add_method(
                    "flow_source_back_pressured_time_millis_per_sec_above_threshold",
                    Self::flow_source_back_pressured_time_millis_per_sec_above_threshold,
                )
                .add_method(
                    "flow_source_back_pressure_percentage_rolling_average",
                    Self::flow_source_back_pressure_percentage_rolling_average,
                )
                .add_method(
                    "flow_source_back_pressure_percentage_rolling_change_per_sec",
                    Self::flow_source_back_pressure_percentage_rolling_change_per_sec,
                )
                .add_method(
                    "flow_source_back_pressure_percentage_below_threshold",
                    Self::flow_source_back_pressure_percentage_below_threshold,
                )
                .add_method(
                    "flow_source_back_pressure_percentage_above_threshold",
                    Self::flow_source_back_pressure_percentage_above_threshold,
                )
                .add_method(
                    "flow_source_records_lag_max_rolling_average",
                    Self::flow_source_records_lag_max_rolling_average,
                )
                .add_method(
                    "flow_source_records_lag_max_rolling_change_per_sec",
                    Self::flow_source_records_lag_max_rolling_change_per_sec,
                )
                .add_method(
                    "flow_source_records_lag_max_below_threshold",
                    Self::flow_source_records_lag_max_below_threshold,
                )
                .add_method(
                    "flow_source_records_lag_max_above_threshold",
                    Self::flow_source_records_lag_max_above_threshold,
                )
                .add_method(
                    "flow_source_total_lag_rolling_average",
                    Self::flow_source_total_lag_rolling_average,
                )
                .add_method(
                    "flow_source_total_lag_rolling_change_per_sec",
                    Self::flow_source_total_lag_rolling_change_per_sec,
                )
                .add_method(
                    "flow_source_total_lag_below_threshold",
                    Self::flow_source_total_lag_below_threshold,
                )
                .add_method(
                    "flow_source_total_lag_above_threshold",
                    Self::flow_source_total_lag_above_threshold,
                )
                .add_method(
                    "flow_source_records_consumed_rate_rolling_average",
                    Self::flow_source_records_consumed_rate_rolling_average,
                )
                .add_method(
                    "flow_source_records_consumed_rate_rolling_change_per_sec",
                    Self::flow_source_records_consumed_rate_rolling_change_per_sec,
                )
                .add_method(
                    "flow_source_records_consumed_rate_below_threshold",
                    Self::flow_source_records_consumed_rate_below_threshold,
                )
                .add_method(
                    "flow_source_records_consumed_rate_above_threshold",
                    Self::flow_source_records_consumed_rate_above_threshold,
                )
                .add_method(
                    "flow_source_relative_lag_change_rate",
                    Self::flow_source_relative_lag_change_rate,
                )
                .add_method(
                    "flow_source_millis_behind_latest_rolling_average",
                    Self::flow_source_millis_behind_latest_rolling_average,
                )
                .add_method(
                    "flow_source_millis_behind_latest_rolling_change_per_sec",
                    Self::flow_source_millis_behind_latest_rolling_change_per_sec,
                )
                .add_method(
                    "flow_source_millis_behind_latest_below_threshold",
                    Self::flow_source_millis_behind_latest_below_threshold,
                )
                .add_method(
                    "flow_source_millis_behind_latest_above_threshold",
                    Self::flow_source_millis_behind_latest_above_threshold,
                )
                .add_method(
                    "flow_records_out_per_sec_rolling_average",
                    Self::flow_records_out_per_sec_rolling_average,
                )
                .add_method(
                    "flow_records_out_per_sec_rolling_change_per_sec",
                    Self::flow_records_out_per_sec_rolling_change_per_sec,
                )
                .add_method(
                    "cluster_task_cpu_load_rolling_average",
                    Self::cluster_task_cpu_load_rolling_average,
                )
                .add_method(
                    "cluster_task_cpu_load_rolling_change_per_sec",
                    Self::cluster_task_cpu_load_rolling_change_per_sec,
                )
                .add_method(
                    "cluster_task_cpu_load_below_threshold",
                    Self::cluster_task_cpu_load_below_threshold,
                )
                .add_method(
                    "cluster_task_cpu_load_above_threshold",
                    Self::cluster_task_cpu_load_above_threshold,
                )
                .add_method(
                    "cluster_task_heap_memory_used_rolling_average",
                    Self::cluster_task_heap_memory_used_rolling_average,
                )
                .add_method(
                    "cluster_task_heap_memory_used_rolling_change_per_sec",
                    Self::cluster_task_heap_memory_used_rolling_change_per_sec,
                )
                .add_method(
                    "cluster_task_heap_memory_used_below_threshold",
                    Self::cluster_task_heap_memory_used_below_threshold,
                )
                .add_method(
                    "cluster_task_heap_memory_used_above_threshold",
                    Self::cluster_task_heap_memory_used_above_threshold,
                )
                .add_method(
                    "cluster_task_heap_memory_load_rolling_average",
                    Self::cluster_task_heap_memory_load_rolling_average,
                )
                .add_method(
                    "cluster_task_heap_memory_load_rolling_change_per_sec",
                    Self::cluster_task_heap_memory_load_rolling_change_per_sec,
                )
                .add_method(
                    "cluster_task_heap_memory_load_below_threshold",
                    Self::cluster_task_heap_memory_load_below_threshold,
                )
                .add_method(
                    "cluster_task_heap_memory_load_above_threshold",
                    Self::cluster_task_heap_memory_load_above_threshold,
                )
                .build(),
        )?;

        Ok(())
    }
}

impl<T> Correlation for AppDataWindow<T>
where
    T: AppData + ReceivedAt + Correlation<Correlated = T>,
{
    type Correlated = T;

    fn correlation(&self) -> &Id<Self::Correlated> {
        self.deref().correlation()
    }
}

impl<T> IntoIterator for AppDataWindow<T>
where
    T: AppData + ReceivedAt,
{
    type IntoIter = std::collections::vec_deque::IntoIter<Self::Item>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

impl<T> AppDataWindow<T>
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

    pub fn has_quorum_looking_back_secs(&self, looking_back_secs: u32) -> bool {
        let head_ts = self.recv_timestamp();
        let looking_back = Duration::from_secs(looking_back_secs as u64);
        self.has_quorum(Interval::new(head_ts - looking_back, head_ts).unwrap())
    }

    pub fn has_quorum(&self, interval: Interval) -> bool {
        self.quorum_percentage <= self.assess_coverage_of(interval).0
    }

    pub fn assess_coverage_of(&self, interval: Interval) -> (f64, impl Iterator<Item = &T>) {
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
                tracing::trace!("window catalog[{}] lies in {interval:?}", m_ts);
                range.push(m);
            }
        }

        let coverage_interval = coverage_start
            .zip(coverage_end)
            .map(|(start, end)| Interval::new(start, end))
            .transpose()
            .unwrap_or_else(|err| {
                tracing::warn!(error=?err, "window represents an invalid interval - using None");
                None
            });

        let coverage = coverage_interval
            .map(|quorum| quorum.duration().as_secs_f64() / interval.duration().as_secs_f64())
            .unwrap_or(0.0);

        (coverage, range.into_iter())
    }

    /// Extracts metric properties from the head of the window (i.e., the current catalog) toward
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
    pub fn fold_duration_from_head<F, R>(&self, looking_back: Duration, f: F) -> R
    where
        F: FnMut(R, &T) -> R,
        R: Monoid,
    {
        self.head().map_or_else(
            || R::empty(),
            |h| {
                let head_ts = h.recv_timestamp();
                let interval = Interval::new(head_ts - looking_back, head_ts).unwrap();
                let (quorum_percentage, range_iter) = self.assess_coverage_of(interval);
                tracing::debug!(
                    ?looking_back, ?interval, %quorum_percentage, quorum_duration=?(looking_back.mul_f64(quorum_percentage)),
                    "folding looking back from head"
                );
                range_iter.fold(R::empty(), f)
            },
        )
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    pub fn sum_from_head<F, R>(&self, looking_back: Duration, mut f: F) -> (R, usize)
    where
        F: FnMut(&T) -> R,
        R: Monoid,
    {
        self.fold_duration_from_head(looking_back, |(acc_sum, acc_size): (R, usize), item| {
            let rhs = f(item);
            (acc_sum.combine(&rhs), acc_size + 1)
        })
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    pub fn for_duration_from_head<F>(&self, looking_back: Duration, f: F) -> bool
    where
        F: FnMut(&T) -> bool,
    {
        self.head().map_or(false, |h| {
            let head_ts = h.recv_timestamp();
            self.for_coverage_in_interval(Interval::new(head_ts - looking_back, head_ts).unwrap(), f)
        })
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    pub fn for_coverage_in_interval<F>(&self, interval: Interval, mut f: F) -> bool
    where
        F: FnMut(&T) -> bool,
    {
        if self.is_empty() {
            tracing::debug!("empty window");
            false
        } else if self.len() == 1 && interval.duration() == Duration::ZERO {
            tracing::debug!("single metric window");
            interval.contains_timestamp(self.recv_timestamp()) && f(self)
        } else {
            tracing::debug!(window=?self.window_interval(), ?interval, "Checking for interval");

            let (coverage, range_iter) = self.assess_coverage_of(interval);
            if coverage < self.quorum_percentage {
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

macro_rules! window_opt_integer_ops_for {
    ($($name:ident = $property:expr)*) => {
        $(
            ::paste::paste! {
                pub fn [<$name _rolling_average>](&self, looking_back_secs: u32) -> f64 {
                    let (sum, size) = self.sum_from_head(
                        Duration::from_secs(looking_back_secs as u64),
                        $property
                    );
                    if size == 0 {
                        0.0
                    } else {
                        sum.and_then(|value| {
                            i32::try_from(value).ok().map(|val| f64::from(val) / size as f64)
                        })
                            .unwrap_or(0.0)
                    }
                }

                pub fn [<$name _rolling_change_per_sec>](&self, looking_back_secs: u32) -> f64 {
                    let values = self.extract_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        |m: &MetricCatalog| { $property(m).map(|val| (m.recv_timestamp, val)) }
                    )
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();

                    values
                        .last()
                        .zip(values.first())
                        .and_then(|(first, last)| {
                            if first.0 == last.0 {
                                None
                            } else {
                                let duration_secs = (last.0 - first.0).as_secs_f64();
                                i32::try_from(last.1 - first.1)
                                    .ok()
                                    .map(|diff| f64::from(diff) / duration_secs)
                            }
                        })
                        .unwrap_or(0.0)
                }

                pub fn [<$name _below_threshold>](&self, looking_back_secs: u32, threshold: i64) -> bool {
                    self.for_duration_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        |m: &MetricCatalog| {
                            $property(m)
                                .map(|value| value < threshold)
                                .unwrap_or(false)
                        }
                    )
                }

                pub fn [<$name _above_threshold>](&self, looking_back_secs: u32, threshold: i64) -> bool {
                    self.for_duration_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        |m: &MetricCatalog| {
                            $property(m)
                                .map(|value| threshold < value)
                                .unwrap_or(false)
                        }
                    )
                }
            }
        )*
    }
}

macro_rules! window_opt_float_ops_for {
    ($($name:ident = $property:expr)*) => {
        $(
            ::paste::paste! {
                pub fn [<$name _rolling_average>](&self, looking_back_secs: u32) -> f64 {
                    let (sum, size) = self.sum_from_head(
                        Duration::from_secs(looking_back_secs as u64),
                        $property
                    );
                    tracing::debug!("DMR: rolling average[{size}]: {sum:?}");
                    if size == 0 { 0.0 } else { sum.map(|value| { value / size as f64 }).unwrap_or(0.0) }
                }

                pub fn [<$name _rolling_change_per_sec>](&self, looking_back_secs: u32) -> f64 {
                    let values = self.extract_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        |m: &MetricCatalog| { $property(m).map(|val| (m.recv_timestamp, val)) }
                    )
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();

                    values
                        .last()
                        .zip(values.first())
                        .and_then(|(first, last)| {
                            if first.0 == last.0 {
                                None
                            } else {
                                let duration_secs = (last.0 - first.0).as_secs_f64();
                                Some((last.1 - first.1) / duration_secs)
                            }
                        })
                        .unwrap_or(0.0)
                }

                pub fn [<$name _below_threshold>](&self, looking_back_secs: u32, threshold: f64) -> bool {
                    self.for_duration_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        |m: &MetricCatalog| {
                            $property(m)
                                .map(|value| value < threshold)
                                .unwrap_or(false)
                        }
                    )
                }

                pub fn [<$name _above_threshold>](&self, looking_back_secs: u32, threshold: f64) -> bool {
                    self.for_duration_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        |m: &MetricCatalog| {
                            $property(m)
                                .map(|value| threshold < value)
                                .unwrap_or(false)
                        }
                    )
                }
            }
        )*
    }
}

impl AppDataWindow<MetricCatalog> {
    window_opt_float_ops_for!(
        flow_idle_time_millis_per_sec = |m: &MetricCatalog| Some(m.flow.idle_time_millis_per_sec)
        flow_task_utilization = |m: &MetricCatalog| Some(m.flow.task_utilization())
        flow_source_back_pressured_time_millis_per_sec = |m: &MetricCatalog| Some(m.flow.source_back_pressured_time_millis_per_sec)
        flow_source_back_pressure_percentage = |m: &MetricCatalog| Some(m.flow.source_back_pressure_percentage())
        flow_source_records_consumed_rate = |m: &MetricCatalog| m.flow.source_records_consumed_rate
        flow_records_out_per_sec = |m: &MetricCatalog| Some(m.flow.records_out_per_sec)
        cluster_task_cpu_load = |m: &MetricCatalog| Some(m.cluster.task_cpu_load)
        cluster_task_heap_memory_used = |m: &MetricCatalog| Some(m.cluster.task_heap_memory_used)
        cluster_task_heap_memory_load = |m: &MetricCatalog| Some(m.cluster.task_heap_memory_load())
    );

    window_opt_integer_ops_for!(
        flow_source_records_lag_max = |m: &MetricCatalog| m.flow.source_records_lag_max
        flow_source_total_lag = |m: &MetricCatalog| m.flow.source_total_lag
        flow_source_millis_behind_latest = |m: &MetricCatalog| m.flow.source_millis_behind_latest
    );

    // pub fn flow_idle_time_millis_per_sec_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(looking_back_secs as u64), |m| {
    //         m.flow.idle_time_millis_per_sec
    //     });
    //
    //     if size == 0 {
    //         0.0
    //     } else {
    //         sum / size as f64
    //     }
    // }
    //
    // pub fn flow_idle_time_millis_per_sec_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //             (m.recv_timestamp, m.flow.idle_time_millis_per_sec)
    //         })
    //         .into_iter()
    //         .collect::<Vec<_>>();
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             if first.0 == last.0 {
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 Some((last.1 - first.1) / duration_secs)
    //             }
    //         })
    //         .unwrap_or(0.0)
    // }
    //
    // pub fn flow_idle_time_millis_per_sec_below_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow.idle_time_millis_per_sec < threshold
    //     })
    // }
    //
    // pub fn flow_idle_time_millis_per_sec_above_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         threshold < m.flow.idle_time_millis_per_sec
    //     })
    // }

    // pub fn flow_task_utilization_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(looking_back_secs as u64), |m| {
    //         m.flow.task_utilization()
    //     });
    //
    //     if size == 0 {
    //         0.0
    //     } else {
    //         sum / size as f64
    //     }
    // }
    //
    // pub fn flow_task_utilization_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //             (m.recv_timestamp, m.flow.task_utilization())
    //         })
    //         .into_iter()
    //         .collect::<Vec<_>>();
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             if first.0 == last.0 {
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 Some((last.1 - first.1) / duration_secs)
    //             }
    //         })
    //         .unwrap_or(0.0)
    // }
    //
    // pub fn flow_task_utilization_below_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow.task_utilization() < threshold
    //     })
    // }
    //
    // pub fn flow_task_utilization_above_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         threshold < m.flow.task_utilization()
    //     })
    // }

    // pub fn flow_source_back_pressured_time_millis_per_sec_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(looking_back_secs as u64), |m| {
    //         m.flow.source_back_pressured_time_millis_per_sec
    //     });
    //
    //     if size == 0 {
    //         0.0
    //     } else {
    //         sum / size as f64
    //     }
    // }
    //
    // pub fn flow_source_back_pressured_time_millis_per_sec_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //             (m.recv_timestamp, m.flow.source_back_pressured_time_millis_per_sec)
    //         })
    //         .into_iter()
    //         .collect::<Vec<_>>();
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             if first.0 == last.0 {
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 Some((last.1 - first.1) / duration_secs)
    //             }
    //         })
    //         .unwrap_or(0.0)
    // }
    //
    // pub fn flow_source_back_pressured_time_millis_per_sec_below_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow.source_back_pressured_time_millis_per_sec < threshold
    //     })
    // }
    //
    // pub fn flow_source_back_pressured_time_millis_per_sec_above_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         threshold < m.flow.source_back_pressured_time_millis_per_sec
    //     })
    // }

    // pub fn flow_source_back_pressure_percentage_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(looking_back_secs as u64), |m| {
    //         m.flow.source_back_pressure_percentage()
    //     });
    //
    //     if size == 0 {
    //         0.0
    //     } else {
    //         sum / size as f64
    //     }
    // }
    //
    // pub fn flow_source_back_pressure_percentage_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //             (m.recv_timestamp, m.flow.source_back_pressure_percentage())
    //         })
    //         .into_iter()
    //         .collect::<Vec<_>>();
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             if first.0 == last.0 {
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 Some((last.1 - first.1) / duration_secs)
    //             }
    //         })
    //         .unwrap_or(0.0)
    // }
    //
    // pub fn flow_source_back_pressure_percentage_below_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow.source_back_pressure_percentage() < threshold
    //     })
    // }
    //
    // pub fn flow_source_back_pressure_percentage_above_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         threshold < m.flow.source_back_pressure_percentage()
    //     })
    // }

    // pub fn flow_source_records_lag_max_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow.source_records_lag_max
    //     });
    //
    //     if size == 0 {
    //         0.0
    //     } else {
    //         sum.and_then(|lag| i32::try_from(lag).ok().map(|l| f64::from(l) / size as f64))
    //             .unwrap_or(0.0)
    //     }
    // }
    //
    // pub fn flow_source_records_lag_max_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values: Vec<(Timestamp, i64)> = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |metric| {
    //             (metric.recv_timestamp, metric.flow.source_records_lag_max)
    //         })
    //         .into_iter()
    //         .flat_map(|(ts, lag)| lag.map(|l| (ts, l)))
    //         .collect();
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             if first.0 == last.0 {
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 i32::try_from(last.1 - first.1)
    //                     .ok()
    //                     .map(|diff| f64::from(diff) / duration_secs)
    //             }
    //         })
    //         .unwrap_or(0.0)
    // }
    //
    // pub fn flow_source_records_lag_max_below_threshold(&self, looking_back_secs: u32, threshold: i64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow
    //             .source_records_lag_max
    //             .map(|lag| {
    //                 tracing::debug!(
    //                     "eval lag in catalog[{}]: {lag} < {threshold} = {}",
    //                     m.recv_timestamp,
    //                     lag < threshold
    //                 );
    //                 lag < threshold
    //             })
    //             .unwrap_or(false)
    //     })
    // }
    //
    // pub fn flow_source_records_lag_max_above_threshold(&self, looking_back_secs: u32, threshold: i64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow
    //             .source_records_lag_max
    //             .map(|lag| {
    //                 tracing::debug!(
    //                     "eval lag in catalog[{}]: {threshold} < {lag} = {}",
    //                     m.recv_timestamp,
    //                     threshold < lag
    //                 );
    //                 threshold < lag
    //             })
    //             .unwrap_or(false)
    //     })
    // }

    // pub fn flow_source_total_lag_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow.source_total_lag
    //     });
    //
    //     if size == 0 {
    //         0.0
    //     } else {
    //         sum.and_then(|lag| i32::try_from(lag).ok().map(|l| f64::from(l) / size as f64))
    //             .unwrap_or(0.0)
    //     }
    // }
    //
    // #[tracing::instrument(level = "trace")]
    // pub fn flow_source_total_lag_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |metric| {
    //             (metric.recv_timestamp, metric.flow.source_total_lag)
    //         })
    //         .into_iter()
    //         .flat_map(|(ts, lag)| lag.map(|l| (ts, l)))
    //         .collect::<Vec<_>>();
    //
    //     tracing::trace!(
    //         nr_total_lag_values=%values.len(),
    //         first_ts=?values.first().map(|f| f.0.to_string()), last_ts=?values.last().map(|l| l.0.to_string()),
    //         first_val=?values.first().map(|f| f.1), last_val=?values.last().map(|l| l.1),
    //         "total_lag values = {:?}", values
    //     );
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             let result = if first.0 == last.0 {
    //                 tracing::trace!("first and last timestamps are the same!");
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 tracing::trace!("duration_secs = {duration_secs}");
    //                 i32::try_from(last.1 - first.1)
    //                     .ok()
    //                     .map(|diff| f64::from(diff) / duration_secs)
    //             };
    //             tracing::trace!(
    //                 "total_lag_rolling_change_per_sec: last[{}] - first[{}] / secs[{:?}] = {result:?}",
    //                 last.1,
    //                 first.1,
    //                 last.0 - first.0,
    //             );
    //             result
    //         })
    //         .unwrap_or(0.0)
    // }
    //
    // pub fn flow_source_total_lag_below_threshold(&self, looking_back_secs: u32, threshold: i64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow
    //             .source_total_lag
    //             .map(|lag| {
    //                 tracing::debug!(
    //                     "eval total lag in catalog[{}]: {lag} < {threshold} = {}",
    //                     m.recv_timestamp,
    //                     lag < threshold
    //                 );
    //                 lag < threshold
    //             })
    //             .unwrap_or(false)
    //     })
    // }
    //
    // pub fn flow_source_total_lag_above_threshold(&self, looking_back_secs: u32, threshold: i64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow
    //             .source_total_lag
    //             .map(|lag| {
    //                 tracing::debug!(
    //                     "eval total lag in catalog[{}]: {threshold} < {lag} = {}",
    //                     m.recv_timestamp,
    //                     threshold < lag
    //                 );
    //                 threshold < lag
    //             })
    //             .unwrap_or(false)
    //     })
    // }

    /// Rolling average of the source `records_consumed_rate` kafka metric on the source operator.
    /// This metric is used with `total_lag` to calculate the `relative_lag_change_rate`.
    // #[tracing::instrument(level = "trace")]
    // pub fn flow_source_records_consumed_rate_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow.source_records_consumed_rate
    //     });
    //
    //     let result = if size == 0 {
    //         0.0
    //     } else {
    //         sum.map(|rate| rate / size as f64).unwrap_or(0.0)
    //     };
    //
    //     tracing::debug!("source_records_consumed_rate_rolling_average: {sum:?} / {size} = {result}");
    //     result
    // }
    //
    // pub fn flow_source_records_consumed_rate_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |metric| {
    //             (metric.recv_timestamp, metric.flow.source_records_consumed_rate)
    //         })
    //         .into_iter()
    //         .flat_map(|(ts, rate)| rate.map(|r| (ts, r)))
    //         .collect::<Vec<_>>();
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             if first.0 == last.0 {
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 Some((last.1 - first.1) / duration_secs)
    //             }
    //         })
    //         .unwrap_or(0.0)
    // }
    //
    // pub fn flow_source_records_consumed_rate_below_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow
    //             .source_records_consumed_rate
    //             .map(|rate| {
    //                 tracing::debug!(
    //                     "eval source consumed rate in catalog[{}]: {rate} < {threshold} = {}",
    //                     m.recv_timestamp,
    //                     rate < threshold
    //                 );
    //                 rate < threshold
    //             })
    //             .unwrap_or(false)
    //     })
    // }
    //
    // pub fn flow_source_records_consumed_rate_above_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow
    //             .source_records_consumed_rate
    //             .map(|rate| {
    //                 tracing::debug!(
    //                     "eval source consumed rate in catalog[{}]: {threshold} < {rate} = {}",
    //                     m.recv_timestamp,
    //                     threshold < rate
    //                 );
    //                 threshold < rate
    //             })
    //             .unwrap_or(false)
    //     })
    // }

    /// Metric that compares the rate of change of total lag with the rate at which the job
    /// processes records (in records/second). In order to smooth the effect of temporary spikes, a
    /// rolling average is used for the components. The ratio is a dimensionless value, which
    /// represents how much the workload is increasing (positive) or decreasing (negative) relative
    /// to the current processing capabilities.
    ///
    /// This number is only meaningful for scale up decisions, since a decreasing lag might still
    /// warrant the current number of task managers, until the application catches up to the latest
    /// records. Downscaling is likely not warranted until the lag is below a certain threshold.
    /// Utilization is a better measure to use for downscaling.
    ///
    /// (see B. Varga, M. Balassi, A. Kiss, Towards autoscaling of Apache Flink jobs,
    /// April 2021Acta Universitatis Sapientiae, Informatica 13(1):1-21)
    #[tracing::instrument(level = "trace")]
    pub fn flow_source_relative_lag_change_rate(&self, looking_back_secs: u32) -> f64 {
        let deriv_total_lag = self.flow_source_total_lag_rolling_change_per_sec(looking_back_secs);
        let total_rate = self.flow_source_records_consumed_rate_rolling_average(looking_back_secs);
        let result = if total_rate == 0.0 { 0.0 } else { deriv_total_lag / total_rate };

        tracing::trace!(
            "source_relative_lag_change_rate: derive_total_lag[{}] / total_rate[{}] = {result}",
            deriv_total_lag,
            total_rate
        );
        result
    }

    // pub fn flow_source_millis_behind_latest_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow.source_millis_behind_latest
    //     });
    //
    //     if size == 0 {
    //         0.0
    //     } else {
    //         sum.and_then(|lag| i32::try_from(lag).ok().map(|l| f64::from(l) / size as f64))
    //             .unwrap_or(0.0)
    //     }
    // }
    //
    // pub fn flow_source_millis_behind_latest_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |metric| {
    //             (metric.recv_timestamp, metric.flow.source_millis_behind_latest)
    //         })
    //         .into_iter()
    //         .flat_map(|(ts, lag)| lag.map(|l| (ts, l)))
    //         .collect::<Vec<_>>();
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             if first.0 == last.0 {
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 i32::try_from(last.1 - first.1)
    //                     .ok()
    //                     .map(|diff| f64::from(diff) / duration_secs)
    //             }
    //         })
    //         .unwrap_or(0.0)
    // }
    //
    // pub fn flow_source_millis_behind_latest_below_threshold(&self, looking_back_secs: u32, threshold: i64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow
    //             .source_millis_behind_latest
    //             .map(|millis_behind_latest| {
    //                 tracing::debug!(
    //                     "eval millis_behind_latest in catalog[{}]: {millis_behind_latest} < {threshold} = {}",
    //                     m.recv_timestamp,
    //                     millis_behind_latest < threshold
    //                 );
    //                 millis_behind_latest < threshold
    //             })
    //             .unwrap_or(false)
    //     })
    // }
    //
    // pub fn flow_source_millis_behind_latest_above_threshold(&self, looking_back_secs: u32, threshold: i64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow
    //             .source_millis_behind_latest
    //             .map(|millis_behind_latest| {
    //                 tracing::debug!(
    //                     "eval millis_behind_latest in catalog[{}]: {threshold} < {millis_behind_latest} = {}",
    //                     m.recv_timestamp,
    //                     threshold < millis_behind_latest
    //                 );
    //                 threshold < millis_behind_latest
    //             })
    //             .unwrap_or(false)
    //     })
    // }

    // pub fn flow_records_out_per_sec_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.flow.records_out_per_sec
    //     });
    //
    //     if size == 0 {
    //         0.0
    //     } else {
    //         sum / size as f64
    //     }
    // }
    //
    // pub fn flow_records_out_per_sec_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |metric| {
    //             (metric.recv_timestamp, metric.flow.records_out_per_sec)
    //         })
    //         .into_iter()
    //         .collect::<Vec<_>>();
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             if first.0 == last.0 {
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 Some((last.1 - first.1) / duration_secs)
    //             }
    //         })
    //         .unwrap_or(0.0)
    // }

    // pub fn cluster_task_cpu_load_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.cluster.task_cpu_load
    //     });
    //
    //     if size == 0 {
    //         0.0
    //     } else {
    //         sum / size as f64
    //     }
    // }
    //
    // pub fn cluster_task_cpu_load_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |metric| {
    //             (metric.recv_timestamp, metric.cluster.task_cpu_load)
    //         })
    //         .into_iter()
    //         .collect::<Vec<_>>();
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             if first.0 == last.0 {
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 Some((last.1 - first.1) / duration_secs)
    //             }
    //         })
    //         .unwrap_or(0.0)
    // }
    //
    // pub fn cluster_task_cpu_load_below_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         tracing::debug!(
    //             "eval task_cpu_load in catalog[{}]: {} < {threshold} = {}",
    //             m.recv_timestamp,
    //             m.cluster.task_cpu_load,
    //             m.cluster.task_cpu_load < threshold
    //         );
    //         m.cluster.task_cpu_load < threshold
    //     })
    // }
    //
    // pub fn cluster_task_cpu_load_above_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         tracing::debug!(
    //             "eval task_cpu_load in catalog[{}]: {threshold} < {} = {}",
    //             m.recv_timestamp,
    //             m.cluster.task_cpu_load,
    //             threshold < m.cluster.task_cpu_load
    //         );
    //         threshold < m.cluster.task_cpu_load
    //     })
    // }

    // pub fn cluster_task_heap_memory_used_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //     let (sum, size) = self.sum_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         m.cluster.task_heap_memory_used
    //     });
    //
    //     if size == 0 {
    //         0.0
    //     } else {
    //         sum / size as f64
    //     }
    // }
    //
    // pub fn cluster_task_heap_memory_used_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //     let values = self
    //         .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |metric| {
    //             (metric.recv_timestamp, metric.cluster.task_heap_memory_used)
    //         })
    //         .into_iter()
    //         .collect::<Vec<_>>();
    //
    //     // remember: head is the most recent, so we want to diff accordingly
    //     values
    //         .last()
    //         .zip(values.first())
    //         .and_then(|(first, last)| {
    //             if first.0 == last.0 {
    //                 None
    //             } else {
    //                 let duration_secs = (last.0 - first.0).as_secs_f64();
    //                 Some((last.1 - first.1) / duration_secs)
    //             }
    //         })
    //         .unwrap_or(0.0)
    // }
    //
    // pub fn cluster_task_heap_memory_used_below_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         tracing::debug!(
    //             "eval task_heap_memory_used in catalog[{}]: {} < {threshold} = {}",
    //             m.recv_timestamp,
    //             m.cluster.task_heap_memory_used,
    //             m.cluster.task_heap_memory_used < threshold
    //         );
    //         m.cluster.task_heap_memory_used < threshold
    //     })
    // }
    //
    // pub fn cluster_task_heap_memory_used_above_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //     self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //         tracing::debug!(
    //             "eval task_heap_memory_used in catalog[{}]: {threshold} < {} = {}",
    //             m.recv_timestamp,
    //             m.cluster.task_heap_memory_used,
    //             threshold < m.cluster.task_heap_memory_used
    //         );
    //         threshold < m.cluster.task_heap_memory_used
    //     })
    // }

    //     pub fn cluster_task_heap_memory_load_rolling_average(&self, looking_back_secs: u32) -> f64 {
    //         let (sum, size) = self.sum_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //             m.cluster.task_heap_memory_load()
    //         });
    //
    //         if size == 0 {
    //             0.0
    //         } else {
    //             sum / size as f64
    //         }
    //     }
    //
    //     pub fn cluster_task_heap_memory_load_rolling_change_per_sec(&self, looking_back_secs: u32) -> f64 {
    //         let values = self
    //             .extract_from_head(Duration::from_secs(u64::from(looking_back_secs)), |metric| {
    //                 (metric.recv_timestamp, metric.cluster.task_heap_memory_load())
    //             })
    //             .into_iter()
    //             .collect::<Vec<_>>();
    //
    //         // remember: head is the most recent, so we want to diff accordingly
    //         values
    //             .last()
    //             .zip(values.first())
    //             .and_then(|(first, last)| {
    //                 if first.0 == last.0 {
    //                     None
    //                 } else {
    //                     let duration_secs = (last.0 - first.0).as_secs_f64();
    //                     Some((last.1 - first.1) / duration_secs)
    //                 }
    //             })
    //             .unwrap_or(0.0)
    //     }
    //
    //     pub fn cluster_task_heap_memory_load_below_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //         self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //             tracing::debug!(
    //                 "eval task_heap_memory_load in catalog[{}]: {} < {threshold} = {}",
    //                 m.recv_timestamp,
    //                 m.cluster.task_heap_memory_load(),
    //                 m.cluster.task_heap_memory_load() < threshold
    //             );
    //             m.cluster.task_heap_memory_load() < threshold
    //         })
    //     }
    //
    //     pub fn cluster_task_heap_memory_load_above_threshold(&self, looking_back_secs: u32, threshold: f64) -> bool {
    //         self.for_duration_from_head(Duration::from_secs(u64::from(looking_back_secs)), |m| {
    //             tracing::debug!(
    //                 "eval task_heap_memory_load in catalog[{}]: {threshold} < {} = {}",
    //                 m.recv_timestamp,
    //                 m.cluster.task_heap_memory_load(),
    //                 threshold < m.cluster.task_heap_memory_load()
    //             );
    //             threshold < m.cluster.task_heap_memory_load()
    //         })
    //     }
}

impl<T> Window for AppDataWindow<T>
where
    T: AppData + ReceivedAt,
    // AppDataWindow<T>: Debug,
{
    type Item = T;

    fn window_interval(&self) -> Option<Interval> {
        // window vecdeque runs oldest to youngest
        let start = self.data.front().map(|m| m.recv_timestamp());
        let end = self.data.back().map(|m| m.recv_timestamp());
        start.zip(end).map(|i| {
            i.try_into()
                .expect("window represents invalid interval (end before start): {i:?}")
        })
    }

    fn time_window(&self) -> Duration {
        self.time_window
    }

    fn push(&mut self, data: Self::Item) {
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

    fn from_item(item: Self::Item, time_window: Duration, quorum_percentage: f64) -> Result<Self, ValidationErrors> {
        Self::builder()
            .with_item(item)
            .with_time_window(time_window)
            .with_quorum_percentage(quorum_percentage)
            .build()
    }
}

impl UpdateWindowMetrics for AppDataWindow<MetricCatalog> {
    fn update_metrics(&self) {
        let total_lag_1_min = self.flow_source_total_lag_rolling_average(60);
        let utilization_1_min = self.flow_task_utilization_rolling_average(60);
        let relative_lag_rate_1_min = self.flow_source_relative_lag_change_rate(60);
        let source_back_pressured_rate_1_min = self.flow_source_back_pressured_time_millis_per_sec_rolling_average(60);
        METRIC_CATALOG_FLOW_TASK_UTILIZATION_1_MIN_ROLLING_AVG.set(utilization_1_min);
        METRIC_CATALOG_FLOW_SOURCE_TOTAL_LAG_1_MIN_ROLLING_AVG.set(total_lag_1_min);
        METRIC_CATALOG_FLOW_SOURCE_RELATIVE_LAG_CHANGE_RATE_1_MIN_ROLLING_AVG.set(relative_lag_rate_1_min);
        METRIC_CATALOG_FLOW_SOURCE_BACK_PRESSURE_TIME_1_MIN_ROLLING_AVG.set(source_back_pressured_rate_1_min);
        tracing::debug!(
            %utilization_1_min, %total_lag_1_min, %relative_lag_rate_1_min, %source_back_pressured_rate_1_min,
            "updated metric catalog window metrics."
        );
    }
}

impl<T> std::ops::Deref for AppDataWindow<T>
where
    T: AppData + ReceivedAt,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.head().expect("failed nonempty AppDataWindow invariant")
    }
}

impl<T> std::ops::Add<T> for AppDataWindow<T>
where
    T: AppData + ReceivedAt,
{
    type Output = Self;

    fn add(mut self, rhs: T) -> Self::Output {
        self.push(rhs);
        self
    }
}

impl<T> std::ops::Add for AppDataWindow<T>
where
    T: AppData + ReceivedAt,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        self.combine(&rhs)
    }
}

impl<T> Semigroup for AppDataWindow<T>
where
    T: AppData + ReceivedAt,
{
    fn combine(&self, other: &Self) -> Self {
        let book = Self::do_ordered_combine(self, other);
        let required_coverage = self.quorum_percentage.max(other.quorum_percentage);
        Self {
            data: book,
            time_window: self.time_window,
            quorum_percentage: required_coverage,
        }
    }
}

impl<T> AppDataWindow<T>
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

    #[tracing::instrument(level = "trace", skip(lhs_window, rhs_window))]
    fn do_ordered_combine(lhs_window: &Self, rhs_window: &Self) -> VecDeque<T> {
        let (lhs_interval, rhs_interval) = match (lhs_window.window_interval(), rhs_window.window_interval()) {
            (None, None) => return VecDeque::new(),
            (Some(_), None) => return lhs_window.data.clone(),
            (None, Some(_)) => return rhs_window.data.clone(),
            (Some(lhs), Some(rhs)) => (lhs, rhs),
        };

        let time_window = lhs_window.time_window;
        if lhs_interval.is_before(rhs_interval) {
            Self::block_combine(time_window, &lhs_window.data, &rhs_window.data)
        } else if rhs_interval.is_before(lhs_interval) {
            Self::block_combine(time_window, &rhs_window.data, &lhs_window.data)
        } else {
            tracing::trace_span!("interspersed combination", ?time_window).in_scope(|| {
                let mut combined = lhs_window
                    .data
                    .iter()
                    .chain(rhs_window.data.iter())
                    .sorted_by(|lhs, rhs| lhs.recv_timestamp().cmp(&rhs.recv_timestamp()))
                    .cloned()
                    .collect::<VecDeque<_>>();

                tracing::trace!(
                    "combined window: {:?}",
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
pub struct AppDataWindowBuilder<T>
where
    T: AppData + ReceivedAt,
{
    data: Option<VecDeque<T>>,
    time_window: Option<Duration>,
    #[validate(custom = "AppDataWindow::<T>::check_quorum_percentage")]
    quorum_percentage: Option<f64>,
}

impl<T> Default for AppDataWindowBuilder<T>
where
    T: AppData + ReceivedAt,
{
    fn default() -> Self {
        Self {
            data: None,
            time_window: None,
            quorum_percentage: None,
        }
    }
}

impl<T> AppDataWindowBuilder<T>
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

    pub const fn with_quorum_percentage(mut self, quorum_percentage: f64) -> Self {
        self.quorum_percentage = Some(quorum_percentage);
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

    pub fn build(self) -> Result<AppDataWindow<T>, ValidationErrors> {
        let mut window: Vec<T> = self.data.map_or(Vec::default(), |d| d.into_iter().collect());
        window.sort_by_key(|item| item.recv_timestamp());
        let result = AppDataWindow {
            data: window.into_iter().collect(),
            time_window: self.time_window.expect("must supply time window before final build"),
            quorum_percentage: self.quorum_percentage.unwrap_or(DEFAULT_QUORUM_PERCENTAGE),
        };
        result.validate()?;
        Ok(result)
    }
}

pub static METRIC_CATALOG_FLOW_TASK_UTILIZATION_1_MIN_ROLLING_AVG: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_flow_task_utilization_1_min_rolling_avg",
            "1 min rolling average of task utilization",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_task_utilization_1_min_rolling_avg")
});

pub static METRIC_CATALOG_FLOW_SOURCE_TOTAL_LAG_1_MIN_ROLLING_AVG: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_flow_source_total_lag_1_min_rolling_avg",
            "1 min rolling average of source total lag",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_source_total_lag_1_min_rolling_avg")
});

pub static METRIC_CATALOG_FLOW_SOURCE_RELATIVE_LAG_CHANGE_RATE_1_MIN_ROLLING_AVG: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_flow_source_relative_lag_change_rate_1_min_rolling_avg",
            "1 min rolling average of source relative lag change rate",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_source_relative_lag_change_rate_1_min_rolling_avg")
});

pub static METRIC_CATALOG_FLOW_SOURCE_BACK_PRESSURE_TIME_1_MIN_ROLLING_AVG: Lazy<Gauge> = Lazy::new(|| {
    Gauge::with_opts(
        Opts::new(
            "metric_catalog_flow_source_back_pressure_time_1_min_rolling_avg",
            "1 min rolling average of source back pressured rate",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
    .expect("failed creating metric_catalog_flow_source_back_pressure_time_1_min_rolling_avg")
});
