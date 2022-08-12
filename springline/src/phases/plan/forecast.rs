use proctor::error::PlanError;

mod calculator;
mod least_squares;
mod regression;
mod ridge_regression;

use std::cmp::Ordering;
use std::fmt::Debug;

pub use calculator::{ForecastCalculator, ForecastInputs};
pub use least_squares::{LeastSquaresWorkloadForecaster, SpikeSettings};
#[cfg(test)]
use mockall::{automock, predicate::*};
use proctor::elements::{Point, RecordsPerSecond, Timestamp};
use serde::{Deserialize, Serialize};

use crate::phases::plan::PlanningMeasurement;

#[cfg_attr(test, automock)]
pub trait Forecaster: Debug + Sync + Send {
    /// report on how many observations are (needed, window_size)
    fn observations_needed(&self) -> (usize, usize);
    fn expected_next_observation_timestamp(&self, from_ts: f64) -> f64;
    fn add_observation(&mut self, measurement: WorkloadMeasurement);
    fn clear(&mut self);
    fn forecast(&mut self) -> Result<Box<dyn WorkloadForecast>, PlanError>;
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

impl From<super::PlanningMeasurement> for WorkloadMeasurement {
    fn from(measurement: PlanningMeasurement) -> Self {
        Self {
            timestamp_secs: measurement.recv_timestamp.into(),
            workload: measurement.records_in_per_sec,
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
        (
            measurement.timestamp_secs as f64,
            measurement.workload.into(),
        )
    }
}
