use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;

use ::serde_with::serde_as;
use approx::{AbsDiffEq, RelativeEq};
use proctor::elements::{RecordsPerSecond, TelemetryType, TelemetryValue, ToTelemetry};
use proctor::error::{PlanError, TelemetryError};
use serde::{Deserialize, Serialize};
use crate::model::MetricCatalog;


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkRange {
    pub nr_task_managers: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    lo_rate: Option<RecordsPerSecond>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    hi_rate: Option<RecordsPerSecond>,
}

impl BenchmarkRange {
    pub const fn lo_from(b: &Benchmark) -> Self {
        Self::new(b.nr_task_managers, Some(b.records_out_per_sec), None)
    }

    pub const fn hi_from(b: &Benchmark) -> Self {
        Self::new(b.nr_task_managers, None, Some(b.records_out_per_sec))
    }

    pub const fn new(
        nr_task_managers: usize, lo_rate: Option<RecordsPerSecond>, hi_rate: Option<RecordsPerSecond>,
    ) -> Self {
        Self { nr_task_managers, lo_rate, hi_rate }
    }
}

impl BenchmarkRange {
    pub fn hi_mark(&self) -> Option<Benchmark> {
        self.hi_rate.map(|records_out_per_sec| Benchmark {
            nr_task_managers: self.nr_task_managers,
            records_out_per_sec,
        })
    }

    pub fn lo_mark(&self) -> Option<Benchmark> {
        self.lo_rate.map(|records_out_per_sec| Benchmark {
            nr_task_managers: self.nr_task_managers,
            records_out_per_sec,
        })
    }

    #[inline]
    pub fn clear_lo(&mut self) {
        self.lo_rate.take();
    }

    #[inline]
    pub fn clear_hi(&mut self) {
        self.hi_rate.take();
    }

    pub fn set_lo_rate(&mut self, lo_rate: RecordsPerSecond) {
        // Tests whether this BenchmarkRange is inconsistent with a low benchmark. This test is not as
        // strong as to verify consistency; however it can be used to invalidate a BenchmarkRange.
        //
        // A BenchmarkRange may become invalid due to circumstances in the environment beyond the
        // application's control. This method helps identify circumstances where a BenchmarkRange
        // should be dropped to avoid bad cluster size estimates for workload.
        fn check_consistency(my_hi_rate: &Option<RecordsPerSecond>, new_lo_rate: RecordsPerSecond) -> bool {
            my_hi_rate.map(|hi| new_lo_rate <= hi).unwrap_or(true)
        }

        let dumped_hi = if !check_consistency(&self.hi_rate, lo_rate) {
            let doa = self.hi_rate.take();
            tracing::debug!(
                "dropping my_hi_rate({:?}) - inconsistent with new_lo_rate({})",
                doa,
                lo_rate
            );
            doa
        } else {
            None
        };

        let dumped_lo = self.lo_rate.replace(lo_rate);
        let dumped = (dumped_lo, dumped_hi);
        tracing::debug!(?dumped, benchmark_range=?self, "updated lo benchmark")
    }

    pub fn set_hi_rate(&mut self, hi_rate: RecordsPerSecond) {
        /// Tests whether this BenchmarkRange is inconsistent with an upper benchmark. This test is
        /// not as strong as to verify consistency; however it can be used to invalidate a
        /// BenchmarkRange.
        ///
        /// A BenchmarkRange may become invalid due to circumstances in the environment beyond the
        /// application's control. This method helps identify circumstances where a BenchmarkRange
        /// should be dropped to avoid bad cluster size estimates for workload.
        fn check_consistency(my_lo_rate: &Option<RecordsPerSecond>, new_hi_rate: RecordsPerSecond) -> bool {
            my_lo_rate.map(|lo| lo <= new_hi_rate).unwrap_or(true)
        }

        let dumped_lo = if !check_consistency(&self.lo_rate, hi_rate) {
            let doa = self.lo_rate.take();
            tracing::debug!(
                "dropping my_lo_rate({:?}) - inconsistent with new_hi_rate({})",
                doa,
                hi_rate
            );
            doa
        } else {
            None
        };

        let dumped_hi = self.hi_rate.replace(hi_rate);
        let dumped = (dumped_lo, dumped_hi);
        tracing::debug!(?dumped, benchmark_range=?self, "updated hi benchmark")
    }
}

impl AbsDiffEq for BenchmarkRange {
    type Epsilon = <Benchmark as AbsDiffEq>::Epsilon;

    #[inline]
    fn default_epsilon() -> Self::Epsilon {
        <Benchmark as AbsDiffEq>::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        // couldn't use nested fn due to desire to use generic outer variable, epsilon.
        let do_abs_diff_eq = |lhs: Option<&RecordsPerSecond>, rhs: Option<&RecordsPerSecond>| match (lhs, rhs) {
            (None, None) => true,
            (Some(lhs), Some(rhs)) => lhs.abs_diff_eq(rhs, epsilon),
            _ => false,
        };

        (self.nr_task_managers == other.nr_task_managers)
            && do_abs_diff_eq(self.lo_rate.as_ref(), other.lo_rate.as_ref())
            && do_abs_diff_eq(self.hi_rate.as_ref(), other.hi_rate.as_ref())
    }
}

impl RelativeEq for BenchmarkRange {
    #[inline]
    fn default_max_relative() -> Self::Epsilon {
        <Benchmark as RelativeEq>::default_max_relative()
    }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        let do_relative_eq = |lhs: Option<&RecordsPerSecond>, rhs: Option<&RecordsPerSecond>| match (lhs, rhs) {
            (None, None) => true,
            (Some(lhs), Some(rhs)) => lhs.relative_eq(rhs, epsilon, max_relative),
            _ => false,
        };

        (self.nr_task_managers == other.nr_task_managers)
            && do_relative_eq(self.lo_rate.as_ref(), other.lo_rate.as_ref())
            && do_relative_eq(self.hi_rate.as_ref(), other.hi_rate.as_ref())
    }
}

#[serde_as]
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct Benchmark {
    pub nr_task_managers: usize,
    pub records_out_per_sec: RecordsPerSecond,
}

impl fmt::Display for Benchmark {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}:{}]", self.nr_task_managers, self.records_out_per_sec)
    }
}

impl Benchmark {
    pub const fn new(nr_task_managers: usize, records_out_per_sec: RecordsPerSecond) -> Self {
        Self { nr_task_managers, records_out_per_sec }
    }
}

impl From<MetricCatalog> for Benchmark {
    fn from(that: MetricCatalog) -> Self {
        Self::from(&that)
    }
}

impl From<&MetricCatalog> for Benchmark {
    fn from(that: &MetricCatalog) -> Self {
        Self {
            nr_task_managers: that.cluster.nr_task_managers as usize,
            records_out_per_sec: that.flow.records_out_per_sec.into(),
        }
    }
}

impl PartialOrd for Benchmark {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.nr_task_managers.cmp(&other.nr_task_managers))
    }
}

impl AbsDiffEq for Benchmark {
    type Epsilon = <RecordsPerSecond as AbsDiffEq>::Epsilon;

    #[inline]
    fn default_epsilon() -> Self::Epsilon {
        <RecordsPerSecond as AbsDiffEq>::default_epsilon()
    }

    #[inline]
    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        (self.nr_task_managers == other.nr_task_managers)
            && (self.records_out_per_sec.abs_diff_eq(&other.records_out_per_sec, epsilon))
    }
}

impl RelativeEq for Benchmark {
    #[inline]
    fn default_max_relative() -> Self::Epsilon {
        <RecordsPerSecond as RelativeEq>::default_max_relative()
    }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        (self.nr_task_managers == other.nr_task_managers)
            && (self
                .records_out_per_sec
                .relative_eq(&other.records_out_per_sec, epsilon, max_relative))
    }
}

const T_NR_TASK_MANAGERS: &str = "nr_task_managers";
const T_RECORDS_OUT_PER_SEC: &str = "records_out_per_sec";

impl From<Benchmark> for TelemetryValue {
    fn from(that: Benchmark) -> Self {
        Self::Table(
            maplit::hashmap! {
                T_NR_TASK_MANAGERS.to_string() => that.nr_task_managers.to_telemetry(),
                T_RECORDS_OUT_PER_SEC.to_string() => that.records_out_per_sec.to_telemetry(),
            }
            .into(),
        )
    }
}

impl TryFrom<TelemetryValue> for Benchmark {
    type Error = PlanError;

    fn try_from(telemetry: TelemetryValue) -> Result<Self, Self::Error> {
        if let TelemetryValue::Table(rep) = telemetry {
            let nr_task_managers = rep
                .get(T_NR_TASK_MANAGERS)
                .map(|v| usize::try_from(v.clone()))
                .ok_or_else(|| PlanError::DataNotFound(T_NR_TASK_MANAGERS.to_string()))??;

            let records_out_per_sec = rep
                .get(T_RECORDS_OUT_PER_SEC)
                .map(|v| f64::try_from(v.clone()))
                .ok_or_else(|| PlanError::DataNotFound(T_RECORDS_OUT_PER_SEC.to_string()))??;

            Ok(Self {
                nr_task_managers,
                records_out_per_sec: records_out_per_sec.into(),
            })
        } else {
            Err(TelemetryError::TypeError {
                expected: TelemetryType::Table,
                actual: Some(format!("{:?}", telemetry)),
            }
            .into())
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_bench_add_lower_benchmark() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_bench_add_lower_benchmark");
        let _main_span_guard = main_span.enter();

        let mut actual = BenchmarkRange::new(4, None, None);

        actual.set_lo_rate(1.0.into());
        assert_eq!(actual, BenchmarkRange::new(4, Some(1.0.into()), None));

        actual.set_lo_rate(3.0.into());
        assert_eq!(actual, BenchmarkRange::new(4, Some(3.0.into()), None));

        actual.set_hi_rate(5.0.into());
        actual.set_lo_rate(1.0.into());
        assert_eq!(actual, BenchmarkRange::new(4, Some(1.0.into()), Some(5.0.into())));

        Ok(())
    }

    #[test]
    fn test_bench_add_upper_benchmark() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_bench_add_upper_benchmark");
        let _main_span_guard = main_span.enter();

        let mut actual = BenchmarkRange::new(4, None, None);

        actual.set_hi_rate(1.0.into());
        assert_eq!(actual, BenchmarkRange::new(4, None, Some(1.0.into())));

        actual.set_hi_rate(3.0.into());
        assert_eq!(actual, BenchmarkRange::new(4, None, Some(3.0.into())));

        actual.set_lo_rate(1.0.into());
        actual.set_hi_rate(1.0.into());
        assert_eq!(actual, BenchmarkRange::new(4, Some(1.0.into()), Some(1.0.into())));

        Ok(())
    }

    #[test]
    fn test_bench_add_lower_upper_benchmarks() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_bench_add_lower_upper_benchmarks");
        let _main_span_guard = main_span.enter();

        let mut actual = BenchmarkRange::new(4, None, None);
        actual.set_lo_rate(1.0.into());
        actual.set_hi_rate(5.0.into());
        assert_eq!(actual, BenchmarkRange::new(4, Some(1.0.into()), Some(5.0.into())));

        actual.set_lo_rate(7.0.into());
        assert_eq!(actual, BenchmarkRange::new(4, Some(7.0.into()), None));

        actual.set_hi_rate(2.5.into());
        assert_eq!(actual, BenchmarkRange::new(4, None, Some(2.5.into())));

        Ok(())
    }
}
