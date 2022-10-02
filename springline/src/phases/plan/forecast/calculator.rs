use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use proctor::elements::{RecordsPerSecond, Timestamp};
use proctor::error::PlanError;

use super::{Forecaster, WorkloadForecast, WorkloadMeasurement};
use crate::phases::plan::{
    ScaleDirection, PLANNING_RECOVERY_WORKLOAD_RATE, PLANNING_VALID_WORKLOAD_RATE,
};
use crate::settings::PlanSettings;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForecastInputs {
    pub direction_restart: HashMap<ScaleDirection, Duration>,
    pub max_catch_up: Duration,
    pub valid_offset: Duration,
}

static DEFAULT_DIRECTION_RESTART: Lazy<HashMap<ScaleDirection, Duration>> = Lazy::new(|| {
    maplit::hashmap! {
        ScaleDirection::Up => Duration::from_secs(180),
        ScaleDirection::Down => Duration::from_secs(180),
    }
});

impl ForecastInputs {
    pub fn new(
        restarts: HashMap<ScaleDirection, Duration>, max_catch_up: Duration, valid_offset: Duration,
    ) -> Result<Self, PlanError> {
        let mut direction_restart = DEFAULT_DIRECTION_RESTART.clone();
        direction_restart.extend(restarts);
        Self { direction_restart, max_catch_up, valid_offset }.check()
    }

    pub fn from_settings(settings: &PlanSettings) -> Result<Self, PlanError> {
        Self::new(
            settings.direction_restart.clone(),
            settings.max_catch_up,
            settings.recovery_valid,
        )
    }

    pub fn check(self) -> Result<Self, PlanError> {
        for (dir, dur) in self.direction_restart.iter() {
            let _ = Self::check_duration(format!("restart::{}", dir).as_str(), *dur)?;
        }

        let _ = Self::check_duration("max_catch_up", self.max_catch_up)?;
        let _ = Self::check_duration("valid_offset", self.valid_offset)?;
        Ok(self)
    }

    fn check_duration(label: &str, d: Duration) -> Result<Duration, PlanError> {
        if d == Duration::ZERO {
            return Err(PlanError::ZeroDuration(format!(
                "workload forecast {}",
                label
            )));
        }

        if (f64::MAX as u128) < d.as_millis() {
            return Err(PlanError::DurationLimitExceeded(d.as_millis()));
        }

        Ok(d)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForecastCalculator<F: Forecaster> {
    forecaster: F,
    pub inputs: ForecastInputs,
}

impl<F: Forecaster> ForecastCalculator<F> {
    pub fn new(forecaster: F, inputs: ForecastInputs) -> Result<Self, PlanError> {
        Ok(Self { forecaster, inputs: inputs.check()? })
    }

    pub fn with_inputs(mut self, inputs: ForecastInputs) -> Result<Self, PlanError> {
        let inputs = inputs.check()?;
        self.forecaster.clear();
        Ok(Self { inputs, ..self })
    }

    pub fn have_enough_data(&self) -> bool {
        let (needed, _required) = self.observations_needed();
        needed == 0
    }

    pub fn observations_needed(&self) -> (usize, usize) {
        self.forecaster.observations_needed()
    }

    pub fn add_observation(&mut self, measurement: impl Into<WorkloadMeasurement>) {
        let measurement = measurement.into();
        tracing::debug!(
            ?measurement,
            "adding workload measurement to forecast calculator."
        );
        self.forecaster.add_observation(measurement)
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.forecaster.clear()
    }

    pub fn calculate_next_workload(
        &mut self, trigger_point: Timestamp,
    ) -> Result<(Timestamp, RecordsPerSecond), PlanError> {
        let next = self
            .forecaster
            .expected_next_observation_timestamp(trigger_point.as_secs_f64());
        self.forecaster
            .forecast()?
            .workload_at(next.into())
            .map(|workload| (next.into(), workload))
    }

    #[tracing::instrument(level="debug", skip(self), fields(inputs=?self.inputs,))]
    pub fn calculate_target_rate(
        &mut self, trigger_point: Timestamp, direction: &ScaleDirection, buffered_records: f64,
    ) -> Result<RecordsPerSecond, PlanError> {
        let recovery = self.calculate_recovery_timestamp_from(trigger_point, direction);
        let valid = self.calculate_valid_timestamp_after_recovery(recovery);
        tracing::debug!( recovery=?recovery.as_utc(), valid=?valid.as_utc(), "cluster scaling timestamp markers estimated." );

        let forecast = self.forecaster.forecast()?;
        tracing::debug!(?forecast, "workload forecast model calculated.");

        let total_records =
            self.total_records_between(&*forecast, trigger_point, recovery)? + buffered_records;
        tracing::debug!(total_records_at_valid_time=%total_records, "estimated total records to process before valid time");

        let recovery_rate = self.recovery_rate(total_records);
        PLANNING_RECOVERY_WORKLOAD_RATE.set(*recovery_rate.as_ref());

        let valid_workload_rate = forecast.workload_at(valid)?;
        PLANNING_VALID_WORKLOAD_RATE.set(*valid_workload_rate.as_ref());

        let target_rate = RecordsPerSecond::max(recovery_rate, valid_workload_rate);

        tracing::debug!(
            %recovery_rate,
            %valid_workload_rate,
            %target_rate,
            "target rate calculated as max of recovery and workload (at valid time) rates."
        );

        Ok(target_rate)
    }

    fn calculate_recovery_timestamp_from(
        &self, timestamp: Timestamp, direction: &ScaleDirection,
    ) -> Timestamp {
        let mut recovery_ts = timestamp + self.inputs.max_catch_up;

        if let Some(restart) = self.inputs.direction_restart.get(direction) {
            recovery_ts = recovery_ts + *restart;
        }

        recovery_ts
    }

    fn calculate_valid_timestamp_after_recovery(&self, recovery: Timestamp) -> Timestamp {
        recovery + self.inputs.valid_offset
    }

    fn total_records_between(
        &self, forecast: &dyn WorkloadForecast, start: Timestamp, end: Timestamp,
    ) -> Result<f64, PlanError> {
        let total = forecast.total_records_between(start, end)?;
        tracing::debug!("total records between [{}, {}] = {}", start, end, total);
        Ok(total)
    }

    fn recovery_rate(&self, total_records: f64) -> RecordsPerSecond {
        let catch_up = self.inputs.max_catch_up.as_secs_f64();
        RecordsPerSecond::new(total_records / catch_up)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use chrono::Utc;
    use claim::*;
    use std::time::Duration;

    use super::*;
    use crate::phases::plan::forecast::*;

    #[test]
    fn test_creation() {
        let d1 = Duration::from_secs(2 * 60);
        let rd = maplit::hashmap! { ScaleDirection::Up => d1, ScaleDirection::Down => d1, };
        let d2 = Duration::from_secs(13 * 60);
        let d3 = Duration::from_secs(5 * 60);
        let inputs = assert_ok!(ForecastInputs::new(rd.clone(), d2, d3));

        assert_ok!(ForecastCalculator::new(MockForecaster::new(), inputs));
        assert_ok!(ForecastCalculator::new(
            MockForecaster::new(),
            assert_ok!(ForecastInputs::new(
                rd.clone(),
                Duration::from_millis(123_456_7),
                d3
            ))
        ));

        assert_ok!(ForecastInputs::new(HashMap::new(), d1, d3));

        let err2 = assert_err!(ForecastInputs::new(rd.clone(), Duration::ZERO, d3));
        claim::assert_matches!(err2, PlanError::ZeroDuration(msg) if msg == "workload forecast max_catch_up".to_string());

        let err3 = assert_err!(ForecastInputs::new(rd, d2, Duration::ZERO));
        claim::assert_matches!(err3, PlanError::ZeroDuration(msg) if msg == "workload forecast valid_offset".to_string());
    }

    #[test]
    fn test_recovery_rate() {
        let restart = maplit::hashmap! {
            ScaleDirection::Up => Duration::from_secs(2 * 60),
            ScaleDirection::Down => Duration::from_secs(2 * 60),
        };
        let max_catch_up = Duration::from_secs(13 * 60);
        let valid_offset = Duration::from_secs(5 * 60);
        let inputs = assert_ok!(ForecastInputs::new(
            restart.clone(),
            max_catch_up,
            valid_offset
        ));

        let c1 = assert_ok!(ForecastCalculator::new(MockForecaster::new(), inputs));
        assert_relative_eq!(
            c1.recovery_rate(100.),
            RecordsPerSecond::new(0.128_205),
            epsilon = 1.0e-6
        );
        assert_relative_eq!(
            c1.recovery_rate(0.),
            RecordsPerSecond::new(0.),
            epsilon = 1.0e-6
        );

        let c2 = assert_ok!(ForecastCalculator::new(
            MockForecaster::new(),
            assert_ok!(ForecastInputs::new(
                restart,
                Duration::from_millis(123_456_7),
                valid_offset
            ))
        ));
        assert_relative_eq!(
            c2.recovery_rate(std::f64::consts::PI * 1_000.),
            RecordsPerSecond::new(2.544_691_907_032_82),
            epsilon = 1.0e-10
        );
    }

    #[test]
    fn test_calculate_valid_timestamp_after_recovery() {
        let restart = maplit::hashmap! {
            ScaleDirection::Up => Duration::from_secs(2 * 60),
            ScaleDirection::Down => Duration::from_secs(2 * 60),
        };
        let max_catch_up = Duration::from_secs(13 * 60);
        let valid_offset = Duration::from_millis(5 * 60 * 1_000 + 750);
        let inputs = assert_ok!(ForecastInputs::new(restart, max_catch_up, valid_offset));
        let c1 = assert_ok!(ForecastCalculator::new(MockForecaster::new(), inputs));

        let now = Utc::now();
        let secs = now.timestamp() + 5 * 60;
        let nsecs = now.timestamp_subsec_nanos() + 750 * 1_000_000;
        let ts: Timestamp = now.into();
        assert_relative_eq!(
            c1.calculate_valid_timestamp_after_recovery(ts),
            Timestamp::new(secs, nsecs),
            epsilon = 1.0e-10
        );
    }

    #[test]
    fn test_calculate_recovery_timestamp_from() {
        let restart = maplit::hashmap! {
            ScaleDirection::Up => Duration::from_secs(2 * 60),
            ScaleDirection::Down => Duration::from_secs(2 * 60),
        };
        let max_catch_up = Duration::from_millis(13 * 60 * 1_000 + 400);
        let valid_offset = Duration::from_secs(5 * 60);
        let inputs = assert_ok!(ForecastInputs::new(restart, max_catch_up, valid_offset));
        let c1 = assert_ok!(ForecastCalculator::new(MockForecaster::new(), inputs));

        let now = Utc::now();
        let secs = now.timestamp() + (2 * 60) + (13 * 60);
        let nsecs = now.timestamp_subsec_nanos() + 400 * 1_000_000;
        let ts: Timestamp = now.into();
        assert_relative_eq!(
            c1.calculate_recovery_timestamp_from(ts, &ScaleDirection::Up),
            Timestamp::new(secs, nsecs),
            epsilon = 1.0e-10
        );
    }

    #[test]
    fn test_calculate_target_rate() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_calculate_target_rate");
        let _main_span_guard = main_span.enter();

        let now = Utc::now();
        let restart = maplit::hashmap! {
            ScaleDirection::Up => Duration::from_secs(2 * 60),
            ScaleDirection::Down => Duration::from_secs(2 * 60),
        };
        let max_catch_up = Duration::from_secs(13 * 60);
        let valid_offset = Duration::from_secs(5 * 60);
        let inputs = assert_ok!(ForecastInputs::new(restart, max_catch_up, valid_offset));

        fn mock_workload_calc_builder(valid_workload: RecordsPerSecond) -> impl Forecaster {
            let mut builder = MockForecaster::new();
            builder.expect_forecast().times(1).returning(move || {
                let mut forecast = MockWorkloadForecast::new();
                forecast.expect_total_records_between().times(1).returning(|_, _| Ok(100.));
                forecast
                    .expect_workload_at()
                    .times(1)
                    .returning(move |_| Ok(valid_workload));
                Ok(Box::new(forecast))
            });

            builder
        }

        let mut c1 = assert_ok!(ForecastCalculator::new(
            mock_workload_calc_builder(0.25.into()),
            inputs.clone()
        ));
        let actual = assert_ok!(c1.calculate_target_rate(now.into(), &ScaleDirection::Up, 333.));
        assert_relative_eq!(actual, RecordsPerSecond::new(0.5551282), epsilon = 1.0e-7);

        let mut c2 = assert_ok!(ForecastCalculator::new(
            mock_workload_calc_builder(314.159.into()),
            inputs
        ));
        let actual = assert_ok!(c2.calculate_target_rate(now.into(), &ScaleDirection::Up, 333.));
        assert_relative_eq!(actual, RecordsPerSecond::new(314.159), epsilon = 1.0e-10);
        Ok(())
    }
}
