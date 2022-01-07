use proctor::error::PlanError;

use crate::phases::MetricCatalog;

mod calculator;
pub mod least_squares;
mod regression;
mod ridge_regression;

use std::cmp::Ordering;
use std::fmt::Debug;

pub use calculator::ForecastCalculator;
pub use least_squares::{LeastSquaresWorkloadForecastBuilder, SpikeSettings};
#[cfg(test)]
use mockall::{automock, predicate::*};
use proctor::elements::{Point, RecordsPerSecond, Timestamp};
use serde::{Deserialize, Serialize};

#[cfg_attr(test, automock)]
pub trait WorkloadForecastBuilder: Debug + Sync + Send {
    fn observations_needed(&self) -> (usize, usize);
    fn add_observation(&mut self, measurement: WorkloadMeasurement);
    fn clear(&mut self);
    fn build_forecast(&mut self) -> Result<Box<dyn WorkloadForecast>, PlanError>;
}

#[cfg_attr(test, automock)]
pub trait WorkloadForecast: Debug {
    fn name(&self) -> &'static str;
    fn workload_at(&self, timestamp: Timestamp) -> Result<RecordsPerSecond, PlanError>;
    fn total_records_between(&self, start: Timestamp, end: Timestamp) -> Result<f64, PlanError>;
    fn correlation_coefficient(&self) -> f64;
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkloadMeasurement {
    pub timestamp_secs: i64,
    pub workload: RecordsPerSecond,
}

impl PartialOrd for WorkloadMeasurement {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.timestamp_secs.partial_cmp(&other.timestamp_secs)
    }
}

impl From<MetricCatalog> for WorkloadMeasurement {
    fn from(metrics: MetricCatalog) -> Self {
        Self {
            timestamp_secs: metrics.timestamp.into(),
            workload: metrics.flow.records_in_per_sec.into(),
        }
    }
}

impl From<Point> for WorkloadMeasurement {
    fn from(pt: Point) -> Self {
        Self { timestamp_secs: pt.0 as i64, workload: pt.1.into() }
    }
}

impl From<WorkloadMeasurement> for Point {
    fn from(measurement: WorkloadMeasurement) -> Self {
        (measurement.timestamp_secs as f64, measurement.workload.into())
    }
}
