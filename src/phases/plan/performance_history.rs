use std::cmp;
use std::collections::BTreeMap;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use splines::{Interpolation, Key, Spline};

use super::Benchmark;
use crate::phases::plan::benchmark::BenchmarkRange;
use crate::phases::plan::MINIMAL_CLUSTER_SIZE;
use proctor::elements::RecordsPerSecond;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerformanceHistory(BTreeMap<u16, BenchmarkRange>);

impl PerformanceHistory {
    #[tracing::instrument(level = "info")]
    pub fn add_lower_benchmark(&mut self, b: Benchmark) {
        if let Some(entry) = self.0.get_mut(&b.nr_task_managers) {
            entry.set_lo_rate(b.records_out_per_sec);
        } else {
            let entry = BenchmarkRange::lo_from(&b);
            self.0.insert(entry.nr_task_managers, entry);
        }

        // todo: dropped clearing performance history inconsistencies (see todo at file bottom)
        // self.clear_inconsistencies_for_new_lo(&b);
    }

    #[tracing::instrument(level = "info")]
    pub fn add_upper_benchmark(&mut self, b: Benchmark) {
        if let Some(entry) = self.0.get_mut(&b.nr_task_managers) {
            entry.set_hi_rate(b.records_out_per_sec);
        } else {
            let entry = BenchmarkRange::hi_from(&b);
            self.0.insert(entry.nr_task_managers, entry);
        }

        // todo: dropped clearing performance history inconsistencies (see todo at file bottom)
        // self.clear_inconsistencies_for_new_hi(b);
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl PerformanceHistory {
    pub fn cluster_size_for_workload(&self, workload_rate: RecordsPerSecond) -> Option<u16> {
        self.evaluate_neighbors(workload_rate)
            .map(|neighbors| neighbors.cluster_size_for(workload_rate))
    }

    #[tracing::instrument(level = "debug")]
    fn evaluate_neighbors(&self, workload_rate: RecordsPerSecond) -> Option<BenchNeighbors> {
        let mut lo = None;
        let mut hi = None;

        for (_, benchmark_range) in self.0.iter() {
            if let Some(ref entry_hi) = benchmark_range.hi_mark() {
                if entry_hi.records_out_per_sec <= workload_rate {
                    lo = Some(*entry_hi);
                } else {
                    hi = Some(*entry_hi);
                    if let Some(ref entry_lo) = benchmark_range.lo_mark() {
                        if entry_lo.records_out_per_sec <= workload_rate {
                            lo = Some(*entry_lo);
                        }
                    }
                    break;
                }
            }
        }

        let neighbors = self.make_neighbors(lo, hi);
        tracing::debug!(?neighbors, "neighbors evaluated");
        neighbors
    }

    fn make_neighbors(&self, lo: Option<Benchmark>, hi: Option<Benchmark>) -> Option<BenchNeighbors> {
        match (lo, hi) {
            (None, None) => None,
            (Some(mark), None) => Some(BenchNeighbors::AboveHighest(mark)),
            (None, Some(mark)) => Some(BenchNeighbors::BelowLowest(mark)),
            (Some(lo), Some(hi)) if lo <= hi => Some(BenchNeighbors::Between { lo, hi }),
            (Some(hi), Some(lo)) => Some(BenchNeighbors::Between { lo, hi }),
        }
    }
}

impl IntoIterator for PerformanceHistory {
    type IntoIter = std::collections::btree_map::IntoIter<u16, BenchmarkRange>;
    type Item = (u16, BenchmarkRange);

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<BTreeMap<u16, BenchmarkRange>> for PerformanceHistory {
    fn from(that: BTreeMap<u16, BenchmarkRange>) -> Self {
        Self(that)
    }
}

#[derive(Debug, PartialEq, Clone)]
enum BenchNeighbors {
    BelowLowest(Benchmark),
    AboveHighest(Benchmark),
    Between { lo: Benchmark, hi: Benchmark },
}

impl BenchNeighbors {
    fn cluster_size_for(&self, workload_rate: RecordsPerSecond) -> u16 {
        match self {
            BenchNeighbors::BelowLowest(lo) => Self::extrapolate_lo(workload_rate, lo),
            BenchNeighbors::AboveHighest(hi) => Self::extrapolate_hi(workload_rate, hi),
            BenchNeighbors::Between { lo, hi } => Self::interpolate(workload_rate, lo, hi),
        }
    }

    #[tracing::instrument(level = "debug")]
    fn extrapolate_lo(workload_rate: RecordsPerSecond, lo: &Benchmark) -> u16 {
        let workload_rate: f64 = workload_rate.into();
        let lo_rate: f64 = lo.records_out_per_sec.into();

        let ratio: f64 = (lo.nr_task_managers as f64) / lo_rate;
        let calculated = (ratio * workload_rate).ceil() as u16;
        tracing::debug!(%ratio, %calculated, "calculations: {} ceil:{}", ratio * workload_rate, (ratio * workload_rate).ceil());

        let size = cmp::min(lo.nr_task_managers, cmp::max(MINIMAL_CLUSTER_SIZE, calculated));
        tracing::debug!(%size, %ratio, %calculated, "extrapolated cluster size below lowest neighbor.");
        size
    }

    #[tracing::instrument(level = "debug")]
    fn extrapolate_hi(workload_rate: RecordsPerSecond, hi: &Benchmark) -> u16 {
        let workload_rate: f64 = workload_rate.into();
        let hi_rate: f64 = hi.records_out_per_sec.into();

        let ratio: f64 = (hi.nr_task_managers as f64) / hi_rate;
        let calculated = ratio * workload_rate;

        let size = cmp::max(
            hi.nr_task_managers,
            cmp::max(MINIMAL_CLUSTER_SIZE, calculated.ceil() as u16),
        );
        tracing::debug!(%size, %ratio, extrapolated_size=%calculated, "extrapolated cluster size above highest neighbor.");
        size
    }

    #[tracing::instrument(level = "debug")]
    fn interpolate(workload_rate: RecordsPerSecond, lo: &Benchmark, hi: &Benchmark) -> u16 {
        let start: Key<f64, f64> = Key::new(
            lo.records_out_per_sec.into(),
            lo.nr_task_managers as f64,
            Interpolation::Linear,
        );
        let end: Key<f64, f64> = Key::new(
            hi.records_out_per_sec.into(),
            hi.nr_task_managers as f64,
            Interpolation::Linear,
        );
        let spline = Spline::from_vec(vec![start, end]);
        let sampled: f64 = spline.clamped_sample(workload_rate.into()).unwrap();

        let size = sampled.ceil() as u16;
        tracing::debug!(%size, interpolated_size=?sampled, "interpolated cluster size between neighbors.");
        size
    }
}

#[cfg(test)]
mod tests {
    use claim::*;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_performance_history_add_upper_benchmark_add_lower_benchmark() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_performance_history_add_lower_benchmark");
        let _main_span_guard = main_span.enter();

        let mut performance_history = PerformanceHistory::default();
        assert!(performance_history.0.is_empty());

        performance_history.add_lower_benchmark(Benchmark::new(4, 1.0.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! { 4 => BenchmarkRange::new(4, Some(1.0.into()), None), })
        );

        performance_history.add_lower_benchmark(Benchmark::new(4, 3.0.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! { 4 => BenchmarkRange::new(4, Some(3.0.into()), None), })
        );

        performance_history.add_lower_benchmark(Benchmark::new(2, 0.5.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! {
                2 => BenchmarkRange::new(2, Some(0.5.into()), None),
                4 => BenchmarkRange::new(4, Some(3.0.into()), None),
            })
        );

        performance_history.add_upper_benchmark(Benchmark::new(4, 5.0.into()));
        performance_history.add_lower_benchmark(Benchmark::new(4, 1.0.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! {
            2 => BenchmarkRange::new(2, Some(0.5.into()), None),
            4 => BenchmarkRange::new(4, Some(1.0.into()), Some(5.0.into())), }),
        );

        Ok(())
    }

    #[test]
    fn test_performance_history_add_upper_benchmark() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_performance_history_add_upper_benchmark");
        let _main_span_guard = main_span.enter();

        let mut performance_history = PerformanceHistory::default();
        assert!(performance_history.0.is_empty());

        performance_history.add_upper_benchmark(Benchmark::new(4, 1.0.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! { 4 => BenchmarkRange::new(4, None, Some(1.0.into())), })
        );

        performance_history.add_upper_benchmark(Benchmark::new(4, 3.0.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! { 4 => BenchmarkRange::new(4, None, Some(3.0.into())), })
        );

        performance_history.add_upper_benchmark(Benchmark::new(2, 0.5.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! {
                2 => BenchmarkRange::new(2, None, Some(0.5.into())),
                4 => BenchmarkRange::new(4, None, Some(3.0.into())),
            })
        );

        performance_history.add_lower_benchmark(Benchmark::new(4, 1.0.into()));
        performance_history.add_upper_benchmark(Benchmark::new(4, 1.0.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! {
            2 => BenchmarkRange::new(2, None, Some(0.5.into())),
            4 => BenchmarkRange::new(4, Some(1.0.into()), Some(1.0.into())), }),
        );

        Ok(())
    }

    #[test]
    fn test_performance_history_add_lower_upper_benchmarks() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_performance_history_add_lower_upper_benchmarks");
        let _main_span_guard = main_span.enter();

        let mut performance_history = PerformanceHistory::default();
        performance_history.add_lower_benchmark(Benchmark::new(4, 1.0.into()));
        performance_history.add_upper_benchmark(Benchmark::new(4, 5.0.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! { 4 => BenchmarkRange::new(4, Some(1.0.into()), Some(5.0.into())), }),
        );

        performance_history.add_lower_benchmark(Benchmark::new(4, 7.0.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! { 4 => BenchmarkRange::new(4, Some(7.0.into()), None), })
        );

        performance_history.add_upper_benchmark(Benchmark::new(4, 2.5.into()));
        assert_eq!(
            performance_history,
            PerformanceHistory(maplit::btreemap! { 4 => BenchmarkRange::new(4, None, Some(2.5.into())) })
        );

        Ok(())
    }

    #[test]
    fn test_performance_history_neighbors_interpolate() -> anyhow::Result<()> {
        let neighbors = BenchNeighbors::Between {
            lo: Benchmark::new(2, 0.5.into()),
            hi: Benchmark::new(4, 1.0.into()),
        };

        assert_eq!(3, neighbors.cluster_size_for(0.75.into()));
        assert_eq!(2, neighbors.cluster_size_for(0.5.into()));
        assert_eq!(4, neighbors.cluster_size_for(1.0.into()));
        assert_eq!(3, neighbors.cluster_size_for(0.55.into()));
        Ok(())
    }

    #[test]
    fn test_performance_history_between_neighbors_interpolate_clamped() -> anyhow::Result<()> {
        let neighbors = BenchNeighbors::Between {
            lo: Benchmark::new(2, 0.5.into()),
            hi: Benchmark::new(4, 1.0.into()),
        };

        // verify outside of boundary is clamped
        assert_eq!(2, neighbors.cluster_size_for(0.05.into()));
        assert_eq!(4, neighbors.cluster_size_for(1.5.into()));
        Ok(())
    }

    #[test]
    fn test_performance_history_interpolate_twin_neighbors() -> anyhow::Result<()> {
        let neighbors = BenchNeighbors::Between {
            lo: Benchmark::new(4, 3.0.into()),
            hi: Benchmark::new(4, 5.0.into()),
        };

        assert_eq!(4, neighbors.cluster_size_for(3.5.into()));
        assert_eq!(4, neighbors.cluster_size_for(4.0.into()));
        assert_eq!(4, neighbors.cluster_size_for(4.75.into()));
        assert_eq!(4, neighbors.cluster_size_for(3.0.into()));
        assert_eq!(4, neighbors.cluster_size_for(5.0.into()));
        assert_eq!(4, neighbors.cluster_size_for(2.5.into()));
        assert_eq!(4, neighbors.cluster_size_for(9.5.into()));
        Ok(())
    }

    #[test]
    fn test_performance_history_below_lowest_neighbor_extrapolate() -> anyhow::Result<()> {
        // lazy_static::initialize(&crate::tracing::TEST_TRACING);
        // let main_span = tracing::info_span!("test_bench_below_lowest_neighbor_extrapolate");
        // let _main_span_guard = main_span.enter();

        let neighbors = BenchNeighbors::BelowLowest(Benchmark::new(4, 1.0.into()));

        assert_eq!(1, neighbors.cluster_size_for(0.0.into()));
        assert_eq!(1, neighbors.cluster_size_for(0.1.into()));
        assert_eq!(1, neighbors.cluster_size_for(0.25.into()));
        assert_eq!(2, neighbors.cluster_size_for(0.35.into()));
        assert_eq!(2, neighbors.cluster_size_for(0.5.into()));
        assert_eq!(3, neighbors.cluster_size_for(0.6.into()));
        assert_eq!(4, neighbors.cluster_size_for(1.0.into()));
        assert_eq!(4, neighbors.cluster_size_for(10.0.into()));
        Ok(())
    }

    #[test]
    fn test_performance_history_above_highest_neighbor_extrapolate() -> anyhow::Result<()> {
        // lazy_static::initialize(&crate::tracing::TEST_TRACING);
        // let main_span = tracing::info_span!("test_bench_above_highest_neighbor_extrapolate");
        // let _main_span_guard = main_span.enter();

        let neighbors = BenchNeighbors::AboveHighest(Benchmark::new(4, 1.0.into()));

        assert_eq!(4, neighbors.cluster_size_for(0.0.into()));
        assert_eq!(4, neighbors.cluster_size_for(0.5.into()));
        assert_eq!(4, neighbors.cluster_size_for(1.0.into()));
        assert_eq!(6, neighbors.cluster_size_for(1.5.into()));
        assert_eq!(8, neighbors.cluster_size_for(2.0.into()));
        assert_eq!(7, neighbors.cluster_size_for(1.56.into()));
        assert_eq!(7, neighbors.cluster_size_for(1.66.into()));
        assert_eq!(20, neighbors.cluster_size_for(5.0.into()));
        assert_eq!(21, neighbors.cluster_size_for(5.00000000001.into()));
        assert_eq!(40, neighbors.cluster_size_for(10.0.into()));
        Ok(())
    }

    #[test]
    fn test_performance_history_simple_evaluate_neighbors() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_bench_evaluate_neighbors");
        let _main_span_guard = main_span.enter();

        let mut performance_history = PerformanceHistory::default();
        assert_none!(performance_history.evaluate_neighbors(375.0.into()));

        performance_history.add_upper_benchmark(Benchmark::new(4, 1.0.into()));
        assert_eq!(
            performance_history.evaluate_neighbors(0.5.into()),
            Some(BenchNeighbors::BelowLowest(Benchmark::new(4, 1.0.into())))
        );
        assert_eq!(
            performance_history.evaluate_neighbors(1.5.into()),
            Some(BenchNeighbors::AboveHighest(Benchmark::new(4, 1.0.into())))
        );
        assert_eq!(
            performance_history.evaluate_neighbors(1.0.into()),
            Some(BenchNeighbors::AboveHighest(Benchmark::new(4, 1.0.into())))
        );
        Ok(())
    }

    #[test]
    fn test_performance_history_evaluate_more_neighbors() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_bench_evaluate_neighbors");
        let _main_span_guard = main_span.enter();

        let mut performance_history = PerformanceHistory::default();
        performance_history.add_upper_benchmark(Benchmark::new(2, 3.0.into()));

        performance_history.add_upper_benchmark(Benchmark::new(4, 5.0.into()));
        performance_history.add_lower_benchmark(Benchmark::new(4, 3.0.into()));

        performance_history.add_upper_benchmark(Benchmark::new(6, 5.5.into()));
        performance_history.add_lower_benchmark(Benchmark::new(6, 3.0.into()));

        performance_history.add_upper_benchmark(Benchmark::new(9, 7.0.into()));
        performance_history.add_upper_benchmark(Benchmark::new(12, 9.0.into()));

        assert_eq!(
            performance_history.evaluate_neighbors(1.0.into()),
            Some(BenchNeighbors::BelowLowest(Benchmark::new(2, 3.0.into())))
        );
        assert_eq!(
            performance_history.evaluate_neighbors(3.25.into()),
            Some(BenchNeighbors::Between {
                lo: Benchmark::new(4, 3.0.into()),
                hi: Benchmark::new(4, 5.0.into()),
            })
        );
        assert_eq!(
            performance_history.evaluate_neighbors(5.0.into()),
            Some(BenchNeighbors::Between {
                lo: Benchmark::new(6, 3.0.into()),
                hi: Benchmark::new(6, 5.5.into()),
            })
        );
        assert_eq!(
            performance_history.evaluate_neighbors(6.17.into()),
            Some(BenchNeighbors::Between {
                lo: Benchmark::new(6, 5.5.into()),
                hi: Benchmark::new(9, 7.0.into()),
            })
        );
        assert_eq!(
            performance_history.evaluate_neighbors(100.0.into()),
            Some(BenchNeighbors::AboveHighest(Benchmark::new(12, 9.0.into())))
        );

        Ok(())
    }

    #[test]
    fn test_performance_history_estimate_cluster_size() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_performance_history_estimate_cluster_size");
        let _main_span_guard = main_span.enter();

        let mut performance_history = PerformanceHistory::default();
        assert_eq!(None, performance_history.cluster_size_for_workload(1_000_000.0.into()));

        performance_history.add_upper_benchmark(Benchmark::new(2, 3.0.into()));

        performance_history.add_upper_benchmark(Benchmark::new(4, 5.0.into()));
        performance_history.add_lower_benchmark(Benchmark::new(4, 3.25.into()));

        performance_history.add_upper_benchmark(Benchmark::new(6, 10.0.into()));
        performance_history.add_lower_benchmark(Benchmark::new(6, 1.0.into()));

        performance_history.add_upper_benchmark(Benchmark::new(9, 15.0.into()));
        performance_history.add_upper_benchmark(Benchmark::new(12, 25.0.into()));

        tracing::warn!("DMR: starting assertions...");
        assert_eq!(Some(1), performance_history.cluster_size_for_workload(1.05.into()));
        assert_eq!(Some(2), performance_history.cluster_size_for_workload(1.75.into()));
        assert_eq!(Some(2), performance_history.cluster_size_for_workload(2.75.into()));
        assert_eq!(Some(3), performance_history.cluster_size_for_workload(3.2.into()));
        assert_eq!(Some(4), performance_history.cluster_size_for_workload(3.75.into()));
        assert_eq!(Some(6), performance_history.cluster_size_for_workload(5.0.into()));
        assert_eq!(Some(6), performance_history.cluster_size_for_workload(10.0.into()));
        assert_eq!(Some(7), performance_history.cluster_size_for_workload(11.0.into()));
        assert_eq!(Some(8), performance_history.cluster_size_for_workload(12.0.into()));
        assert_eq!(Some(8), performance_history.cluster_size_for_workload(13.0.into()));
        assert_eq!(Some(9), performance_history.cluster_size_for_workload(14.0.into()));
        assert_eq!(Some(9), performance_history.cluster_size_for_workload(15.0.into()));

        assert_eq!(Some(10), performance_history.cluster_size_for_workload(18.0.into()));
        assert_eq!(Some(11), performance_history.cluster_size_for_workload(21.0.into()));
        assert_eq!(Some(12), performance_history.cluster_size_for_workload(24.0.into()));

        assert_eq!(Some(48), performance_history.cluster_size_for_workload(100.0.into()));

        Ok(())
    }
}

// todo: not pursuing because clearing inconsistencies at performance_history group level seems more
// and as a premature optimization. Keeping at Benchmark range simply because it doesn't make sense
// that a lo-bound could have a higher throughput than a hi-bound and therefore we should reset.
// fn clear_inconsistencies_for_new_lo(&mut self, new_lo: &Benchmark) {
//     for (nr, bench) in self.0.iter_mut() {
//         if *nr < new_lo.nr_task_managers {
//             if let Some(lo_mark) = bench.lo_mark() {
//                 if new_lo.records_out_per_sec < lo_mark.records_out_per_sec {
//                     bench.clear_lo();
//                 }
//             }
//         } else if new_lo.nr_task_managers < *nr {
//             if let Some(lo_mark) = bench.lo_mark() {
//                 if lo_mark.records_out_per_sec < new_lo.records_out_per_sec {
//                     bench.clear_lo();
//                 }
//             }
//
//             if let Some(hi_mark) = bench.hi_mark() {
//                 if hi_mark.records_out_per_sec < new_lo.records_out_per_sec {
//                     bench.clear_hi();
//                 }
//             }
//         }
//     }
// }
