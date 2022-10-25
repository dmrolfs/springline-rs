use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fmt;

use std::marker::PhantomData;
use std::ops::Deref;
use std::time::Duration;

use crate::model::Saturating;
use crate::phases::PhaseData;
use crate::Env;
use frunk::{Monoid, Semigroup};
use itertools::Itertools;
use once_cell::sync::Lazy;
use oso::{Oso, PolarClass};
use pretty_snowflake::{Label, MakeLabeling};
use proctor::elements::{Interval, PolicyContributor, Timestamp};
use proctor::error::PolicyError;
use proctor::{AppData, ReceivedAt};
use prometheus::{GaugeVec, Opts};
use serde::de::{self, DeserializeOwned, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use validator::{Validate, ValidationError, ValidationErrors};

use super::MetricCatalog;

pub trait Window: Sized {
    type Item: AppData;
    //todo: this needs to be rethought as builder would be appropriate; however this trait on the whole is a bit contrived.
    fn from_item(
        item: Self::Item, time_window: Duration, quorum_percentage: f64,
    ) -> Result<Self, ValidationErrors>;
    fn time_window(&self) -> Duration;
    fn window_interval(&self) -> Option<Interval>;
    fn push(&mut self, item: Self::Item);
}

pub trait UpdateWindowMetrics: Window {
    fn update_metrics(&self, window: Option<Duration>);
}

pub trait WindowEntry {
    type Entry: AppData + PartialEq + ReceivedAt;
    type Data: AppData;

    fn new(item: Self) -> Self::Entry;
    fn into_data(entry: Self::Entry) -> Self::Data;
    fn data_of(entry: &Self::Entry) -> &Self::Data;
}

impl<T> WindowEntry for Env<T>
where
    T: AppData + Label + PartialEq,
{
    type Entry = (T, Timestamp);
    type Data = T;

    fn new(item: Self) -> Self::Entry {
        let ts = item.recv_timestamp();
        (item.into_inner(), ts)
    }

    fn into_data(entry: Self::Entry) -> Self::Data {
        entry.0
    }

    fn data_of(entry: &Self::Entry) -> &Self::Data {
        &entry.0
    }
}

/// Window of data objects used for range queries; e.g., has CPU utilization exceeded a threshold
/// for 5 minutes.
/// `AppDataWindow` has an invariant that there is always at least one item in the window..
#[derive(Clone, PartialEq)]
pub struct AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    entries: VecDeque<<T as WindowEntry>::Entry>,
    pub time_window: Duration,
    pub quorum_percentile: f64,
}

impl<T> Validate for AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    fn validate(&self) -> Result<(), ValidationErrors> {
        let checks = vec![
            Self::check_quorum_percentile(self.quorum_percentile)
                .map_err(|err| ("insufficient quorum", err)),
            Self::check_nonempty(&self.entries).map_err(|err| ("empty window", err)),
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

impl<T> fmt::Debug for AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppDataWindow")
            .field("interval", &self.window_interval())
            .field(
                "interval_duration",
                &self.window_interval().map(|w| w.duration()),
            )
            .field("time_window", &self.time_window)
            .field("quorum_percentile", &self.quorum_percentile)
            .field("window_size", &self.entries.len())
            .field(
                "latest_ts",
                &self.latest_entry().recv_timestamp().to_string(),
            )
            .finish()
    }
}

impl<T> Label for AppDataWindow<T>
where
    T: AppData + Label + WindowEntry,
{
    type Labeler = <T as Label>::Labeler;

    fn labeler() -> Self::Labeler {
        <T as Label>::labeler()
    }
}

pub const DEFAULT_QUORUM_PERCENTILE: f64 = 0.8;

pub const fn default_quorum_percentile() -> f64 {
    DEFAULT_QUORUM_PERCENTILE
}

impl<T> AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    pub fn from_size(data: T, window_size: usize, interval: Duration) -> Self {
        let mut window = VecDeque::with_capacity(window_size);
        let entry = <T as WindowEntry>::new(data);
        window.push_back(entry);
        let time_window = interval * window_size as u32;
        let result = Self {
            entries: window,
            time_window,
            quorum_percentile: DEFAULT_QUORUM_PERCENTILE,
        };
        result.validate().expect("window parameters are not valid");
        result
    }

    pub fn from_time_window(data: T, time_window: Duration) -> Self {
        let mut window = VecDeque::new();
        let entry = <T as WindowEntry>::new(data);
        window.push_back(entry);
        let result = Self {
            entries: window,
            time_window,
            quorum_percentile: DEFAULT_QUORUM_PERCENTILE,
        };
        result.validate().expect("window parameters are not valid");
        result
    }

    pub fn builder() -> AppDataWindowBuilder<T> {
        AppDataWindowBuilder::default()
    }

    fn check_quorum_percentile(percentile: f64) -> Result<(), ValidationError> {
        if percentile <= 0.0 {
            Err(ValidationError::new(
                "not enough window quorum coverage to be meaningful.",
            ))
        } else if 1.0 < percentile {
            Err(ValidationError::new(
                "impossible for telemetry window to meet quorum coverage requirement.",
            ))
        } else {
            Ok(())
        }
    }

    fn check_nonempty(data: &VecDeque<<T as WindowEntry>::Entry>) -> Result<(), ValidationError> {
        if data.is_empty() {
            Err(ValidationError::new("window data cannot be empty."))
        } else {
            Ok(())
        }
    }
}

impl<T> PolarClass for AppDataWindow<T> where T: AppData + WindowEntry {}

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

impl PolicyContributor for AppDataWindow<PhaseData> {
    #[tracing::instrument(level = "trace", skip(engine))]
    fn register_with_policy_engine(engine: &mut Oso) -> Result<(), PolicyError> {
        MetricCatalog::register_with_policy_engine(engine)?;

        let builder = Self::get_polar_class_builder()
            .add_attribute_getter("health", |p| p.health.clone())
            .add_attribute_getter("flow", |p| p.flow.clone())
            .add_attribute_getter("cluster", |p| p.cluster.clone())
            .add_attribute_getter("custom", |p| p.custom.clone())
            .add_method(
                "has_quorum_looking_back_secs",
                Self::has_quorum_looking_back_secs,
            );

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
                    "flow_source_assigned_partitions_rolling_average",
                    Self::flow_source_assigned_partitions_rolling_average,
                )
                .add_method(
                    "flow_source_assigned_partitions_rolling_change_per_sec",
                    Self::flow_source_assigned_partitions_rolling_change_per_sec,
                )
                .add_method(
                    "flow_source_assigned_partitions_below_threshold",
                    Self::flow_source_assigned_partitions_below_threshold,
                )
                .add_method(
                    "flow_source_assigned_partitions_above_threshold",
                    Self::flow_source_assigned_partitions_above_threshold,
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
                    "flow_source_relative_lag_velocity",
                    Self::flow_source_relative_lag_velocity,
                )
                .add_method(
                    "flow_is_source_consumer_telemetry_populated_over_window",
                    Self::flow_is_source_consumer_telemetry_populated_over_window,
                )
                .add_method(
                    "flow_is_source_consumer_telemetry_empty_over_window",
                    Self::flow_is_source_consumer_telemetry_empty_over_window,
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
                    "flow_records_in_per_sec_rolling_average",
                    Self::flow_records_in_per_sec_rolling_average,
                )
                .add_method(
                    "flow_records_in_per_sec_rolling_change_per_sec",
                    Self::flow_records_in_per_sec_rolling_change_per_sec,
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
                .add_method(
                    "cluster_task_network_input_utilization_rolling_average",
                    Self::cluster_task_network_input_utilization_rolling_average,
                )
                .add_method(
                    "cluster_task_network_input_utilization_rolling_change_per_sec",
                    Self::cluster_task_network_input_utilization_rolling_change_per_sec,
                )
                .add_method(
                    "cluster_task_network_input_utilization_below_threshold",
                    Self::cluster_task_network_input_utilization_below_threshold,
                )
                .add_method(
                    "cluster_task_network_input_utilization_above_threshold",
                    Self::cluster_task_network_input_utilization_above_threshold,
                )
                .add_method(
                    "cluster_task_network_output_utilization_rolling_average",
                    Self::cluster_task_network_output_utilization_rolling_average,
                )
                .add_method(
                    "cluster_task_network_output_utilization_rolling_change_per_sec",
                    Self::cluster_task_network_output_utilization_rolling_change_per_sec,
                )
                .add_method(
                    "cluster_task_network_output_utilization_below_threshold",
                    Self::cluster_task_network_output_utilization_below_threshold,
                )
                .add_method(
                    "cluster_task_network_output_utilization_above_threshold",
                    Self::cluster_task_network_output_utilization_above_threshold,
                )
                .build(),
        )?;

        Ok(())
    }
}

impl<T> IntoIterator for AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    type IntoIter = std::vec::IntoIter<<T as WindowEntry>::Data>;
    type Item = <T as WindowEntry>::Data;

    fn into_iter(self) -> Self::IntoIter {
        self.entries
            .into_iter()
            .map(<T as WindowEntry>::into_data)
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl<T> AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    #[inline]
    pub fn latest(&self) -> &<T as WindowEntry>::Data {
        <T as WindowEntry>::data_of(self.latest_entry())
    }

    pub fn latest_entry(&self) -> &<T as WindowEntry>::Entry {
        self.entries.back().unwrap()
    }

    pub fn history(&self) -> impl Iterator<Item = &<T as WindowEntry>::Entry> {
        self.entries.iter().rev()
    }

    pub const fn is_empty(&self) -> bool {
        false
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn has_quorum_looking_back_secs(&self, looking_back_secs: u32) -> bool {
        let head_ts = self.latest_entry().recv_timestamp();
        let looking_back = Duration::from_secs(looking_back_secs as u64);
        self.has_quorum(Interval::new(head_ts - looking_back, head_ts).unwrap())
    }

    pub fn has_quorum(&self, interval: Interval) -> bool {
        self.quorum_percentile <= self.assess_coverage_of(interval).0
    }

    pub fn assess_coverage_of(
        &self, interval: Interval,
    ) -> (f64, impl Iterator<Item = &<T as WindowEntry>::Entry>) {
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

        let coverage = if interval.duration().is_zero() {
            0.0
        } else {
            coverage_interval.map_or(0.0, |quorum| {
                tracing::debug!(?quorum, quorum_duration=?quorum.duration(), ?interval, interval_duration=?interval.duration(), "calculating quorum coverage.");
                quorum.duration().as_secs_f64() / interval.duration().as_secs_f64()
            })
        };

        (coverage, range.into_iter())
    }

    /// Extracts metric properties from the head of the window (i.e., the current catalog) toward
    /// the past.
    #[tracing::instrument(level = "trace", skip(self, extractor))]
    pub fn extract_from_head<F, D>(&self, looking_back: Duration, extractor: F) -> Vec<D>
    where
        F: FnMut(&<T as WindowEntry>::Entry) -> Option<D>,
    {
        let head_ts = self.latest_entry().recv_timestamp();
        self.extract_in_interval(
            Interval::new(head_ts - looking_back, head_ts).unwrap(),
            extractor,
        )
        .collect()
    }

    /// Extracts metric properties from the end of the interval (i.e., the youngest catalog
    /// contained in the interval) toward the past.
    #[tracing::instrument(level = "trace", skip(self, extractor))]
    pub fn extract_in_interval<F, D>(
        &self, interval: Interval, extractor: F,
    ) -> impl Iterator<Item = D>
    where
        F: FnMut(&<T as WindowEntry>::Entry) -> Option<D>,
    {
        self.history()
            .take_while(|m| interval.contains_timestamp(m.recv_timestamp()))
            .flat_map(extractor)
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    fn fold_duration_from_head<F, R>(&self, init: R, looking_back: Duration, f: F) -> R
    where
        F: FnMut(R, &<T as WindowEntry>::Entry) -> R,
        R: Copy + fmt::Debug,
    {
        let latest_ts = self.latest_entry().recv_timestamp();
        let interval = Interval::new(latest_ts - looking_back, latest_ts);
        let interval = interval.unwrap();
        let (_quorum_percentage, range_iter) = self.assess_coverage_of(interval);
        range_iter.fold(init, f)
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    fn sum_from_head<F, R>(&self, looking_back: Duration, mut f: F) -> (R, usize)
    where
        F: FnMut(&<T as WindowEntry>::Entry) -> R,
        Saturating<R>: Semigroup,
        R: Copy + Monoid + fmt::Debug,
    {
        self.fold_duration_from_head((R::empty(), 0), looking_back, |acc: (R, usize), item| {
            let (acc_sum, acc_size) = acc;
            let acc_sat = Saturating(acc_sum);
            let rhs = Saturating(f(item));
            let new_acc = acc_sat.combine(&rhs);
            let result = (new_acc.0, acc_size + 1);
            tracing::debug!(fold_result=?result, "fold result");
            result
        })
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    pub fn for_duration_from_head<F>(&self, looking_back: Duration, f: F) -> bool
    where
        F: FnMut(&<T as WindowEntry>::Entry) -> bool,
    {
        let head_ts = self.latest_entry().recv_timestamp();
        self.for_coverage_in_interval(Interval::new(head_ts - looking_back, head_ts).unwrap(), f)
    }

    #[tracing::instrument(level = "trace", skip(self, f))]
    pub fn for_coverage_in_interval<F>(&self, interval: Interval, mut f: F) -> bool
    where
        F: FnMut(&<T as WindowEntry>::Entry) -> bool,
    {
        if self.is_empty() {
            tracing::debug!("empty window");
            false
        } else if self.len() == 1 && interval.duration() == Duration::ZERO {
            tracing::debug!("single metric window");
            let entry = self.latest_entry();
            interval.contains_timestamp(entry.recv_timestamp()) && f(entry)
        } else {
            tracing::debug!(window=?self.window_interval(), ?interval, "Checking for interval");

            let (coverage, range_iter) = self.assess_coverage_of(interval);
            if coverage < self.quorum_percentile {
                tracing::debug!(
                    ?interval,
                    interval_duration=?interval.duration(),
                    included_range=?range_iter.map(|m| m.recv_timestamp().to_string()).collect::<Vec<_>>(),
                    %coverage, coverage_duration=?(interval.duration().mul_f64(coverage)),
                    "not enough coverage for meaningful evaluation."

                );
                return false;
            }

            let range: Vec<&<T as WindowEntry>::Entry> = range_iter.collect();
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
    ($($name:ident = $property_accessor:expr)*) => {
        $(
            ::paste::paste! {
                pub fn [<$name _rolling_average>](&self, looking_back_secs: u32) -> f64 {
                    let entry_accessor = |(mc, _ts): &(MetricCatalog, Timestamp)| { $property_accessor(mc) };
                    let (sum, size): (_, usize) = self.sum_from_head(
                        Duration::from_secs(looking_back_secs as u64),
                        entry_accessor
                    );

                    if size == 0 {
                        0.0
                    } else {
                        sum
                            .and_then(|value| i32::try_from(value).ok().map(|val| f64::from(val) / size as f64))
                            .unwrap_or(0.0)
                    }
                }

                pub fn [<$name _rolling_change_per_sec>](&self, looking_back_secs: u32) -> f64 {
                    let entry_accessor = |(mc, ts): &(MetricCatalog, Timestamp)| {
                        $property_accessor(mc).map(|v| (*ts, v))
                    };

                    let values: Vec<(Timestamp, _)> = self.extract_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        entry_accessor
                    );

                    values
                        .last()
                        .zip(values.first())
                        .and_then(|(first, last)| {
                            if first.0 == last.0 {
                                None
                            } else {
                                let duration_secs = (last.0 - first.0).as_secs_f64();
                                let last_val: i64 = last.1.into();
                                let first_val: i64 = first.1.into();
                                tracing::debug!("{} rolling change rate last_val[{last_val}] - first_val[{first_val}]", stringify!($name));
                                i32::try_from(last_val - first_val)
                                    .ok()
                                    .map(|diff| f64::from(diff) / duration_secs)
                            }
                        })
                        .unwrap_or(0.0)
                }

                pub fn [<$name _below_threshold>](&self, looking_back_secs: u32, threshold: u32) -> bool {
                    let check_entry_threshold = |(mc, _ts): &(MetricCatalog, Timestamp)| {
                        $property_accessor(mc)
                            .map(|value| value < threshold)
                            .unwrap_or(false)
                    };

                    self.for_duration_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        check_entry_threshold
                    )
                }

                pub fn [<$name _above_threshold>](&self, looking_back_secs: u32, threshold: u32) -> bool {
                    let check_entry_threshold = |(mc, _ts): &(MetricCatalog, Timestamp)| {
                        $property_accessor(mc)
                            .map(|value| threshold < value)
                            .unwrap_or(false)
                    };

                    self.for_duration_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        check_entry_threshold
                    )
                }
            }
        )*
    }
}

macro_rules! window_opt_float_ops_for {
    ($($name:ident = $property_accessor:expr)*) => {
        $(
            ::paste::paste! {
                pub fn [<$name _rolling_average>](&self, looking_back_secs: u32) -> f64 {
                    let entry_accessor = |(mc, _ts): &(MetricCatalog, Timestamp)| { $property_accessor(mc) };
                    let sum_size: (_, usize) = self.sum_from_head(
                        Duration::from_secs(looking_back_secs as u64),
                        entry_accessor
                    );
                    let (sum, size) = sum_size;

                    let average = if size == 0 { 0.0 } else { sum.map(|value| { value / size as f64 }).unwrap_or(0.0) };
                    tracing::debug!("{} rolling average[{sum:?} / {size}]: {average:?}", stringify!($name));
                    average
                }

                pub fn [<$name _rolling_change_per_sec>](&self, looking_back_secs: u32) -> f64 {
                    let entry_accessor = |(mc, ts): &(MetricCatalog, Timestamp)| {
                        $property_accessor(mc).map(|v| (*ts, v))
                    };

                    let values: Vec<(Timestamp, _)> = self.extract_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        entry_accessor
                    );

                    values
                        .last()
                        .zip(values.first())
                        .and_then(|(first, last)| {
                            if first.0 == last.0 {
                                None
                            } else {
                                let duration_secs = (last.0 - first.0).as_secs_f64();
                                let last_val: f64 = last.1.into();
                                let first_val: f64 = first.1.into();
                                let change_rate = Some((last_val - first_val) / duration_secs);
                                tracing::debug!("{} rolling rate change last_val[{last_val}] - first_val[{first_val}] => change_rate = {change_rate:?}", stringify!($name));
                                change_rate
                            }
                        })
                        .unwrap_or(0.0)
                }

                pub fn [<$name _below_threshold>](&self, looking_back_secs: u32, threshold: f64) -> bool {
                    let check_entry_threshold = |(mc, _ts): &(MetricCatalog, Timestamp)| {
                        $property_accessor(mc)
                            .map(|value| value < threshold)
                            .unwrap_or(false)
                    };

                    self.for_duration_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        check_entry_threshold
                    )
                }

                pub fn [<$name _above_threshold>](&self, looking_back_secs: u32, threshold: f64) -> bool {
                    let check_entry_threshold = |(mc, _ts): &(MetricCatalog, Timestamp)| {
                        $property_accessor(mc)
                            .map(|value| threshold < value)
                            .unwrap_or(false)
                    };

                    self.for_duration_from_head(
                        Duration::from_secs(u64::from(looking_back_secs)),
                        check_entry_threshold
                    )
                }
            }
        )*
    }
}

impl AppDataWindow<PhaseData> {
    window_opt_float_ops_for!(
        flow_idle_time_millis_per_sec = |m: &MetricCatalog| Some(m.flow.idle_time_millis_per_sec)
        flow_task_utilization = |m: &MetricCatalog| Some(m.flow.task_utilization())
        flow_source_back_pressured_time_millis_per_sec = |m: &MetricCatalog| Some(m.flow.source_back_pressured_time_millis_per_sec)
        flow_source_back_pressure_percentage = |m: &MetricCatalog| Some(m.flow.source_back_pressure_percentage())
        flow_source_records_consumed_rate = |m: &MetricCatalog| m.flow.source_records_consumed_rate
        flow_records_in_per_sec = |m: &MetricCatalog| Some(m.flow.records_in_per_sec)
        flow_records_out_per_sec = |m: &MetricCatalog| Some(m.flow.records_out_per_sec)
        cluster_task_cpu_load = |m: &MetricCatalog| Some(m.cluster.task_cpu_load)
        cluster_task_heap_memory_used = |m: &MetricCatalog| Some(m.cluster.task_heap_memory_used)
        cluster_task_heap_memory_load = |m: &MetricCatalog| Some(m.cluster.task_heap_memory_load())
        cluster_task_network_input_utilization = |m: &MetricCatalog| Some(m.cluster.task_network_input_utilization())
        cluster_task_network_output_utilization = |m: &MetricCatalog| Some(m.cluster.task_network_output_utilization())
    );

    window_opt_integer_ops_for!(
        flow_source_records_lag_max = |m: &MetricCatalog| m.flow.source_records_lag_max
        flow_source_assigned_partitions = |m: &MetricCatalog| m.flow.source_assigned_partitions
        flow_source_total_lag = |m: &MetricCatalog| m.flow.source_total_lag
        flow_source_millis_behind_latest = |m: &MetricCatalog| m.flow.source_millis_behind_latest
    );

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
    pub fn flow_source_relative_lag_velocity(&self, looking_back_secs: u32) -> f64 {
        let deriv_total_lag = self.flow_source_total_lag_rolling_change_per_sec(looking_back_secs);
        let total_rate = self.flow_source_records_consumed_rate_rolling_average(looking_back_secs);
        let result = if total_rate == 0.0 { 0.0 } else { deriv_total_lag / total_rate };

        tracing::debug!(
            "source_relative_lag_change_rate: derive_total_lag[{}] / total_rate[{}] = {result}",
            deriv_total_lag,
            total_rate
        );
        result
    }

    pub fn flow_is_source_consumer_telemetry_populated_over_window(
        &self, looking_back_secs: u32,
    ) -> bool {
        self.for_duration_from_head(
            Duration::from_secs(u64::from(looking_back_secs)),
            |(m, _): &(MetricCatalog, Timestamp)| m.flow.is_source_consumer_telemetry_populated(),
        )
    }

    pub fn flow_is_source_consumer_telemetry_empty_over_window(
        &self, looking_back_secs: u32,
    ) -> bool {
        self.for_duration_from_head(
            Duration::from_secs(u64::from(looking_back_secs)),
            |(m, _): &(MetricCatalog, Timestamp)| !m.flow.is_source_consumer_telemetry_populated(),
        )
    }
}

impl<T> Window for AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    type Item = T;

    fn window_interval(&self) -> Option<Interval> {
        // window vecdeque runs oldest to youngest
        let start = self.entries.front().map(|m| m.recv_timestamp());
        let end = self.entries.back().map(|m| m.recv_timestamp());
        start.zip(end).map(|i| {
            i.try_into()
                .expect("window represents invalid interval (end before start): {i:?}")
        })
    }

    fn time_window(&self) -> Duration {
        self.time_window
    }

    fn push(&mut self, data: Self::Item) {
        let data_entry = <T as WindowEntry>::new(data);

        // efficient push to back if monotonically increasing recv_timestamp; otherwise expensive resort
        match self.entries.back() {
            Some(last) if data_entry.recv_timestamp() < last.recv_timestamp() => {
                tracing::warn!(
                    data_recv_ts=?data_entry.recv_timestamp(), window_last_ts=?last.recv_timestamp(),
                    "data window works best if data items' recv_timestamps are monotonically increasing - performing expensive data resort."
                );
                self.entries.push_back(data_entry);
                let mut my_data: Vec<_> = self.entries.iter().collect();
                my_data.sort_by_key(|item| item.recv_timestamp());
                self.entries = my_data.into_iter().cloned().collect();
            },
            _ => self.entries.push_back(data_entry),
        }

        let allowed = self.entries.back().map(|last| last.recv_timestamp() - self.time_window);

        while let Some((item, oldest_allowed)) = self.entries.front().zip(allowed) {
            if item.recv_timestamp() < oldest_allowed {
                let expiring = self.entries.pop_front();
                tracing::debug!(?expiring, %oldest_allowed, "expiring data item outside of time window");
            } else {
                break;
            }
        }
    }

    fn from_item(
        item: Self::Item, time_window: Duration, quorum_percentage: f64,
    ) -> Result<Self, ValidationErrors> {
        Self::builder()
            .with_item(item)
            .with_time_window(time_window)
            .with_quorum_percentile(quorum_percentage)
            .build()
    }
}

impl UpdateWindowMetrics for AppDataWindow<Env<MetricCatalog>> {
    #[allow(clippy::cognitive_complexity)]
    fn update_metrics(&self, window: Option<Duration>) {
        let window_secs = window.and_then(|w| u32::try_from(w.as_secs()).ok()).unwrap_or(60);
        let w_rep = window_secs.to_string();
        let labels = [w_rep.as_str()];

        let total_lag = self.flow_source_total_lag_rolling_average(window_secs);
        let utilization = self.flow_task_utilization_rolling_average(window_secs);
        let relative_lag_velocity = self.flow_source_relative_lag_velocity(window_secs);
        let source_back_pressured_rate =
            self.flow_source_back_pressured_time_millis_per_sec_rolling_average(window_secs);

        METRIC_CATALOG_FLOW_TASK_UTILIZATION_ROLLING_AVG
            .with_label_values(&labels)
            .set(utilization);
        METRIC_CATALOG_FLOW_SOURCE_TOTAL_LAG_ROLLING_AVG
            .with_label_values(&labels)
            .set(total_lag);
        METRIC_CATALOG_FLOW_SOURCE_RELATIVE_LAG_VELOCITY_ROLLING_AVG
            .with_label_values(&labels)
            .set(relative_lag_velocity);
        METRIC_CATALOG_FLOW_SOURCE_BACK_PRESSURE_TIME_ROLLING_AVG
            .with_label_values(&labels)
            .set(source_back_pressured_rate);
        tracing::debug!(
            ?window, %window_secs, %utilization, %total_lag, %relative_lag_velocity, %source_back_pressured_rate,
            "updated metric catalog window metrics."
        );
    }
}

impl<T> Window for Env<AppDataWindow<Env<T>>>
where
    T: AppData + Label + PartialEq,
{
    type Item = Env<T>;

    fn from_item(
        item: Self::Item, time_window: Duration, quorum_percentage: f64,
    ) -> Result<Self, ValidationErrors> {
        item.flat_map(|i| {
            AppDataWindow::builder()
                .with_item(i)
                .with_time_window(time_window)
                .with_quorum_percentile(quorum_percentage)
                .build()
        })
        .transpose()
    }

    fn time_window(&self) -> Duration {
        self.deref().time_window()
    }

    fn window_interval(&self) -> Option<Interval> {
        self.deref().window_interval()
    }

    fn push(&mut self, item: Self::Item) {
        self.adopt_metadata(item.metadata().clone());
        self.as_mut().push(item)
    }
}

impl<T> UpdateWindowMetrics for Env<AppDataWindow<T>>
where
    T: AppData + Label + PartialEq + WindowEntry,
    AppDataWindow<T>: UpdateWindowMetrics,
    Self: Window,
{
    fn update_metrics(&self, window: Option<Duration>) {
        self.deref().update_metrics(window);
    }
}

impl<T> std::ops::Deref for AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    type Target = <T as WindowEntry>::Data;

    fn deref(&self) -> &Self::Target {
        self.latest()
    }
}

impl<T> std::ops::Add<T> for AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    type Output = Self;

    fn add(mut self, rhs: T) -> Self::Output {
        self.push(rhs);
        self
    }
}

impl<T> std::ops::Add for AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        self.combine(&rhs)
    }
}

impl<T> Semigroup for AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    fn combine(&self, other: &Self) -> Self {
        let book = Self::do_ordered_combine(self, other);
        let required_coverage = self.quorum_percentile.max(other.quorum_percentile);
        Self {
            entries: book,
            time_window: self.time_window,
            quorum_percentile: required_coverage,
        }
    }
}

impl<T> AppDataWindow<T>
where
    T: AppData + WindowEntry,
{
    #[tracing::instrument(level = "trace", skip(older, younger))]
    fn block_combine(
        time_window: Duration, older: &VecDeque<<T as WindowEntry>::Entry>,
        younger: &VecDeque<<T as WindowEntry>::Entry>,
    ) -> VecDeque<<T as WindowEntry>::Entry> {
        younger.back().map_or_else(
            || older.clone(), // if younger has no back, then it must be empty.
            |youngest| {
                let cutoff = youngest.recv_timestamp() - time_window;
                let older_iter = older.iter().cloned();
                let younger_iter = younger.iter().cloned();
                let result: VecDeque<<T as WindowEntry>::Entry> = older_iter
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
    fn do_ordered_combine(
        lhs_window: &Self, rhs_window: &Self,
    ) -> VecDeque<<T as WindowEntry>::Entry> {
        let (lhs_interval, rhs_interval) =
            match (lhs_window.window_interval(), rhs_window.window_interval()) {
                (None, None) => return VecDeque::new(),
                (Some(_), None) => return lhs_window.entries.clone(),
                (None, Some(_)) => return rhs_window.entries.clone(),
                (Some(lhs), Some(rhs)) => (lhs, rhs),
            };

        let time_window = lhs_window.time_window;
        if lhs_interval.is_before(rhs_interval) {
            Self::block_combine(time_window, &lhs_window.entries, &rhs_window.entries)
        } else if rhs_interval.is_before(lhs_interval) {
            Self::block_combine(time_window, &rhs_window.entries, &lhs_window.entries)
        } else {
            tracing::trace_span!("interspersed combination", ?time_window).in_scope(|| {
                let mut combined = lhs_window
                    .entries
                    .iter()
                    .chain(rhs_window.entries.iter())
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

impl<T> Serialize for AppDataWindow<T>
where
    T: AppData + WindowEntry + Serialize,
    <T as WindowEntry>::Entry: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AppDataWindow", 3)?;
        state.serialize_field("time_window", &self.time_window)?;
        state.serialize_field("quorum_percentage", &self.quorum_percentile)?;
        state.serialize_field("entries", &self.entries)?;
        state.end()
    }
}

impl<'de, T> Deserialize<'de> for AppDataWindow<T>
where
    T: AppData + WindowEntry + DeserializeOwned,
    <T as WindowEntry>::Entry: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            TimeWindow,
            QuorumPercentage,
            Entries,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("`time_window`, `quorum_percentage` or `entries`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "time_window" => Ok(Field::TimeWindow),
                            "quorum_percentage" => Ok(Field::QuorumPercentage),
                            "entries" => Ok(Field::Entries),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct AppDataWindowVisitor<T0>(PhantomData<T0>);

        impl<T0> AppDataWindowVisitor<T0>
        where
            T0: DeserializeOwned + WindowEntry,
            <T0 as WindowEntry>::Entry: DeserializeOwned,
        {
            pub const fn new() -> Self {
                Self(PhantomData)
            }
        }

        impl<'de, T0> Visitor<'de> for AppDataWindowVisitor<T0>
        where
            T0: AppData + WindowEntry + DeserializeOwned,
            <T0 as WindowEntry>::Entry: DeserializeOwned,
        {
            type Value = AppDataWindow<T0>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("struct AppDataWindow<T>")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut time_window = None;
                let mut quorum_percentage = None;
                let mut entries = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::TimeWindow => {
                            if time_window.is_some() {
                                return Err(de::Error::duplicate_field("time_window"));
                            }
                            time_window = Some(map.next_value()?);
                        },
                        Field::QuorumPercentage => {
                            if quorum_percentage.is_some() {
                                return Err(de::Error::duplicate_field("quorum_percentage"));
                            }
                            quorum_percentage = Some(map.next_value()?);
                        },
                        Field::Entries => {
                            if entries.is_some() {
                                return Err(de::Error::duplicate_field("entries"));
                            }
                            entries = Some(map.next_value()?);
                        },
                    }
                }

                let time_window =
                    time_window.ok_or_else(|| de::Error::missing_field("time_window"))?;
                let quorum_percentage = quorum_percentage
                    .ok_or_else(|| de::Error::missing_field("quorum_percentage"))?;
                let entries: Vec<<T0 as WindowEntry>::Entry> =
                    entries.ok_or_else(|| de::Error::missing_field("entries"))?;

                AppDataWindow::builder()
                    .with_time_window(time_window)
                    .with_quorum_percentile(quorum_percentage)
                    .with_entries(entries)
                    .build()
                    .map_err(|err| {
                        de::Error::custom(format!(
                            "failed to deserialize valid data window: {err:?}"
                        ))
                    })
            }
        }

        const FIELDS: &[&str] = &["time_window", "quorum_percentage", "entries"];
        deserializer.deserialize_struct("AppDataWindow", FIELDS, AppDataWindowVisitor::<T>::new())
    }
}

#[derive(Debug, Clone, PartialEq, Validate)]
pub struct AppDataWindowBuilder<T>
where
    T: AppData + WindowEntry,
{
    entries: Option<VecDeque<<T as WindowEntry>::Entry>>,
    time_window: Option<Duration>,
    #[validate(custom = "AppDataWindow::<T>::check_quorum_percentile")]
    quorum_percentile: Option<f64>,
}

impl<T> Default for AppDataWindowBuilder<T>
where
    T: AppData + WindowEntry,
{
    fn default() -> Self {
        Self {
            entries: None,
            time_window: None,
            quorum_percentile: None,
        }
    }
}

impl<T> AppDataWindowBuilder<T>
where
    T: AppData + WindowEntry,
{
    pub fn with_item(mut self, item: T) -> Self {
        self.push_item(item);
        self
    }

    pub fn with_entry(mut self, entry: <T as WindowEntry>::Entry) -> Self {
        self.push_entry(entry);
        self
    }

    pub fn with_items(mut self, items: impl IntoIterator<Item = T>) -> Self {
        items.into_iter().for_each(|entry| {
            self.push_item(entry);
        });
        self
    }

    pub fn with_entries<E>(mut self, entries: E) -> Self
    where
        E: IntoIterator<Item = <T as WindowEntry>::Entry>,
    {
        entries.into_iter().for_each(|entry| {
            self.push_entry(entry);
        });
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

    pub const fn with_quorum_percentile(mut self, quorum_percentile: f64) -> Self {
        self.quorum_percentile = Some(quorum_percentile);
        self
    }

    pub fn push_item(&mut self, item: T) -> &mut Self {
        self.push_entry(<T as WindowEntry>::new(item))
    }

    pub fn push_entry(&mut self, entry: <T as WindowEntry>::Entry) -> &mut Self {
        if let Some(entries) = &mut self.entries {
            entries.push_back(entry);
        } else {
            let mut entries = VecDeque::new();
            entries.push_back(entry);
            self.entries = Some(entries);
        }

        self
    }

    pub fn is_empty(&self) -> bool {
        self.entries.as_ref().map_or(true, |d| d.is_empty())
    }

    pub fn len(&self) -> usize {
        self.entries.as_ref().map_or(0, |d| d.len())
    }

    pub fn build(self) -> Result<AppDataWindow<T>, ValidationErrors> {
        let mut window: Vec<<T as WindowEntry>::Entry> =
            self.entries.map_or(Vec::default(), |d| d.into_iter().collect());
        window.sort_by_key(|item| item.recv_timestamp());
        let result: AppDataWindow<T> = AppDataWindow {
            entries: window.into_iter().collect(),
            time_window: self.time_window.expect("must supply time window before final build"),
            quorum_percentile: self.quorum_percentile.unwrap_or(DEFAULT_QUORUM_PERCENTILE),
        };
        result.validate()?;
        Ok(result)
    }
}

impl<T> Label for AppDataWindowBuilder<T>
where
    T: AppData + WindowEntry,
{
    type Labeler = MakeLabeling<Self>;

    fn labeler() -> Self::Labeler {
        Self::Labeler::default()
    }
}

pub static METRIC_CATALOG_FLOW_TASK_UTILIZATION_ROLLING_AVG: Lazy<GaugeVec> = Lazy::new(|| {
    GaugeVec::new(
        Opts::new(
            "metric_catalog_flow_task_utilization_rolling_avg",
            "rolling average of task utilization",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
        &["window_secs"],
    )
    .expect("failed creating metric_catalog_flow_task_utilization_rolling_avg")
});

pub static METRIC_CATALOG_FLOW_SOURCE_TOTAL_LAG_ROLLING_AVG: Lazy<GaugeVec> = Lazy::new(|| {
    GaugeVec::new(
        Opts::new(
            "metric_catalog_flow_source_total_lag_rolling_avg",
            "rolling average of source total lag",
        )
        .const_labels(proctor::metrics::CONST_LABELS.clone()),
        &["window_secs"],
    )
    .expect("failed creating metric_catalog_flow_source_total_lag_rolling_avg")
});

pub static METRIC_CATALOG_FLOW_SOURCE_RELATIVE_LAG_VELOCITY_ROLLING_AVG: Lazy<GaugeVec> =
    Lazy::new(|| {
        GaugeVec::new(
            Opts::new(
                "metric_catalog_flow_source_relative_lag_velocity_rolling_avg",
                "rolling average of source relative lag change rate",
            )
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
            &["window_secs"],
        )
        .expect("failed creating metric_catalog_flow_source_relative_lag_velocity_rolling_avg")
    });

pub static METRIC_CATALOG_FLOW_SOURCE_BACK_PRESSURE_TIME_ROLLING_AVG: Lazy<GaugeVec> =
    Lazy::new(|| {
        GaugeVec::new(
            Opts::new(
                "metric_catalog_flow_source_back_pressure_time_rolling_avg",
                "rolling average of source back pressured rate",
            )
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
            &["window_secs"],
        )
        .expect("failed creating metric_catalog_flow_source_back_pressure_time_rolling_avg")
    });

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flink::FlowMetrics;
    use approx::*;
    use claim::*;
    use pretty_assertions::assert_eq;
    use proctor::MetaData;
    use serde_test::{assert_tokens, Token};

    #[derive(Debug, Clone, PartialEq, Label, Serialize, Deserialize)]
    struct TestData(pub i64);

    fn envelope_for<T: Label + Send>(data: T) -> Env<T> {
        Env::new(data)
    }
    fn envelope_for_ts<T: Label + Send>(data: T, ts: Timestamp) -> Env<T> {
        Env::from_parts(MetaData::default().with_recv_timestamp(ts), data)
    }

    #[test]
    fn test_app_data_window_serde_tokens() {
        let now = Timestamp::new(100, 0);

        let data_window = assert_ok!(AppDataWindow::builder()
            .with_time_window(Duration::from_secs(10))
            .with_quorum_percentile(0.67)
            .with_items(
                vec![
                    (TestData(10), now + Duration::from_secs(1)),
                    (TestData(20), now + Duration::from_secs(2)),
                    (TestData(30), now + Duration::from_secs(3)),
                    (TestData(40), now + Duration::from_secs(4)),
                    (TestData(50), now + Duration::from_secs(5)),
                ]
                .into_iter()
                .map(|(d, ts)| envelope_for_ts(d, ts))
            )
            .build());

        assert_tokens(
            &data_window,
            &vec![
                Token::Struct { name: "AppDataWindow", len: 3 },
                Token::Str("time_window"),
                Token::Struct { name: "Duration", len: 2 },
                Token::Str("secs"),
                Token::U64(10),
                Token::Str("nanos"),
                Token::U32(0),
                Token::StructEnd,
                Token::Str("quorum_percentage"),
                Token::F64(0.67),
                Token::Str("entries"),
                Token::Seq { len: Some(5) },
                Token::Tuple { len: 2 },
                Token::NewtypeStruct { name: "TestData" },
                Token::I64(10),
                Token::TupleStruct { name: "Timestamp", len: 2 },
                Token::I64(now.as_pair().0 + 1),
                Token::U32(now.as_pair().1),
                Token::TupleStructEnd,
                Token::TupleEnd,
                Token::Tuple { len: 2 },
                Token::NewtypeStruct { name: "TestData" },
                Token::I64(20),
                Token::TupleStruct { name: "Timestamp", len: 2 },
                Token::I64(now.as_pair().0 + 2),
                Token::U32(now.as_pair().1),
                Token::TupleStructEnd,
                Token::TupleEnd,
                Token::Tuple { len: 2 },
                Token::NewtypeStruct { name: "TestData" },
                Token::I64(30),
                Token::TupleStruct { name: "Timestamp", len: 2 },
                Token::I64(now.as_pair().0 + 3),
                Token::U32(now.as_pair().1),
                Token::TupleStructEnd,
                Token::TupleEnd,
                Token::Tuple { len: 2 },
                Token::NewtypeStruct { name: "TestData" },
                Token::I64(40),
                Token::TupleStruct { name: "Timestamp", len: 2 },
                Token::I64(now.as_pair().0 + 4),
                Token::U32(now.as_pair().1),
                Token::TupleStructEnd,
                Token::TupleEnd,
                Token::Tuple { len: 2 },
                Token::NewtypeStruct { name: "TestData" },
                Token::I64(50),
                Token::TupleStruct { name: "Timestamp", len: 2 },
                Token::I64(now.as_pair().0 + 5),
                Token::U32(now.as_pair().1),
                Token::TupleStructEnd,
                Token::TupleEnd,
                Token::SeqEnd,
                Token::StructEnd,
            ],
        )
    }

    #[test]
    fn test_app_data_window_serde_json() {
        let expected: AppDataWindow<Env<TestData>> = assert_ok!(AppDataWindow::builder()
            .with_time_window(Duration::from_secs(10))
            .with_quorum_percentile(0.67)
            .with_items(
                vec![
                    TestData(1),
                    TestData(2),
                    TestData(3),
                    TestData(4),
                    TestData(5)
                ]
                .into_iter()
                .map(envelope_for)
            )
            .build());

        let actual_rep = assert_ok!(serde_json::to_string(&expected));
        let actual: AppDataWindow<Env<TestData>> = assert_ok!(serde_json::from_str(&actual_rep));
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_numeric_saturating_combine() {
        assert_eq!(Saturating(1_i8).combine(&Saturating(1_i8)).0, 2);
        assert_eq!(Saturating(i8::MAX).combine(&Saturating(1_i8)).0, i8::MAX);

        assert_eq!(Saturating(1_i16).combine(&Saturating(1_i16)).0, 2);
        assert_eq!(Saturating(i16::MAX).combine(&Saturating(1_i16)).0, i16::MAX);

        assert_eq!(Saturating(1_i32).combine(&Saturating(1_i32)).0, 2);
        assert_eq!(Saturating(i32::MAX).combine(&Saturating(1_i32)).0, i32::MAX);

        assert_eq!(Saturating(1_i64).combine(&Saturating(1_i64)).0, 2);
        assert_eq!(Saturating(i64::MAX).combine(&Saturating(1_i64)).0, i64::MAX);

        assert_eq!(Saturating(1_u8).combine(&Saturating(1_u8)).0, 2);
        assert_eq!(Saturating(u8::MAX).combine(&Saturating(1_u8)).0, u8::MAX);

        assert_eq!(Saturating(1_u16).combine(&Saturating(1_u16)).0, 2);
        assert_eq!(Saturating(u16::MAX).combine(&Saturating(1_u16)).0, u16::MAX);

        assert_eq!(Saturating(1_u32).combine(&Saturating(1_u32)).0, 2);
        assert_eq!(Saturating(u32::MAX).combine(&Saturating(1_u32)).0, u32::MAX);

        assert_eq!(Saturating(1_u64).combine(&Saturating(1_u64)).0, 2);
        assert_eq!(Saturating(u64::MAX).combine(&Saturating(1_u64)).0, u64::MAX);

        assert_eq!(Saturating(1_isize).combine(&Saturating(1_isize)).0, 2);
        assert_eq!(
            Saturating(isize::MAX).combine(&Saturating(1_isize)).0,
            isize::MAX
        );

        assert_eq!(Saturating(1_usize).combine(&Saturating(1_usize)).0, 2);
        assert_eq!(
            Saturating(usize::MAX).combine(&Saturating(1_usize)).0,
            usize::MAX
        );

        assert_eq!(Saturating(1_f32).combine(&Saturating(1_f32)).0, 2_f32);
        assert_eq!(Saturating(f32::MAX).combine(&Saturating(1_f32)).0, f32::MAX);

        assert_eq!(Saturating(1_f64).combine(&Saturating(1_f64)).0, 2_f64);
        assert_eq!(Saturating(f64::MAX).combine(&Saturating(1_f64)).0, f64::MAX);
    }

    #[test]
    fn test_window_total_lag_rolling_average() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_window_total_lag_rolling_average");
        let _ = main_span.enter();

        let start = Timestamp::now();
        let incr = Duration::from_secs(15);
        let basis = MetricCatalog::empty();

        let builder = (0..10).into_iter().fold(
            AppDataWindow::builder().with_time_window(Duration::from_secs(120)),
            |acc, i| {
                let source_total_lag = Some(100 + i * 10);
                acc.with_item(envelope_for_ts(
                    MetricCatalog {
                        flow: FlowMetrics { source_total_lag, ..basis.flow.clone() },
                        ..basis.clone()
                    },
                    start + incr * i,
                ))
            },
        );
        let data = assert_ok!(builder.build());
        tracing::info!(?data, "AAA");
        assert_eq!(data.flow_source_total_lag_rolling_average(120), 150.0);
    }

    #[test]
    fn test_mixed_window_total_lag_rolling_average() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_mixed_window_total_lag_rolling_average");
        let _ = main_span.enter();

        let start = Timestamp::now();
        let incr = Duration::from_secs(15);
        let basis = MetricCatalog::empty();

        let builder = (0..10).into_iter().fold(
            AppDataWindow::builder().with_time_window(Duration::from_secs(120)),
            |acc, i| {
                let source_total_lag = if i % 2 == 0 { Some(100 + i * 10) } else { None };
                acc.with_item(envelope_for_ts(
                    MetricCatalog {
                        flow: FlowMetrics { source_total_lag, ..basis.flow.clone() },
                        ..basis.clone()
                    },
                    start + incr * i,
                ))
            },
        );
        let data = assert_ok!(builder.build());
        tracing::info!(?data, "AAA");
        assert_relative_eq!(
            data.flow_source_total_lag_rolling_average(120),
            66.66666666666666,
            epsilon = f64::EPSILON
        );
    }
}
