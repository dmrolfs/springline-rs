use std::time::Duration;

use super::{WorkloadForecast, WorkloadForecastBuilder, WorkloadMeasurement};
use proctor::elements::{RecordsPerSecond, Timestamp};
use proctor::error::PlanError;

#[derive(Debug, Clone)]
pub struct ForecastCalculator<F: WorkloadForecastBuilder> {
    forecast_builder: F,
    pub restart: Duration,
    pub max_catch_up: Duration,
    pub valid_offset: Duration,
}

impl<F: WorkloadForecastBuilder> ForecastCalculator<F> {
    pub fn new(
        forecast_builder: F, restart: Duration, max_catch_up: Duration, valid_offset: Duration,
    ) -> Result<Self, PlanError> {
        let restart = Self::check_duration("restart", restart)?;
        let max_catch_up = Self::check_duration("max_catch_up", max_catch_up)?;
        let valid_offset = Self::check_duration("valid_offset", valid_offset)?;
        Ok(Self {
            forecast_builder,
            restart,
            max_catch_up,
            valid_offset,
        })
    }

    fn check_duration(label: &str, d: Duration) -> Result<Duration, PlanError> {
        if d == Duration::ZERO {
            return Err(PlanError::ZeroDuration(format!("workload forecast {}", label)));
        }

        if (f64::MAX as u128) < d.as_millis() {
            return Err(PlanError::DurationLimitExceeded(d.as_millis()));
        }

        Ok(d)
    }

    pub fn have_enough_data(&self) -> bool {
        let (needed, _required) = self.observations_needed();
        needed == 0
    }

    pub fn observations_needed(&self) -> (usize, usize) {
        self.forecast_builder.observations_needed()
    }

    pub fn add_observation(&mut self, measurement: WorkloadMeasurement) {
        tracing::debug!(?measurement, "adding workload measurement to forecast calculator.");
        self.forecast_builder.add_observation(measurement)
    }

    pub fn clear(&mut self) {
        self.forecast_builder.clear()
    }

    #[tracing::instrument(
        level="debug",
        skip(self, ),
        fields(
            restart=?self.restart,
            max_catch_up=?self.max_catch_up,
            valid_offset=?self.valid_offset
        )
    )]
    pub fn calculate_target_rate(
        &mut self, trigger: Timestamp, buffered_records: f64,
    ) -> Result<RecordsPerSecond, PlanError> {
        let recovery = self.calculate_recovery_timestamp_from(trigger);
        let valid = self.calculate_valid_timestamp_after_recovery(recovery);
        tracing::debug!( recovery=?recovery.as_utc(), valid=?valid.as_utc(), "cluster scaling timestamp markers estimated." );

        let forecast = self.forecast_builder.build_forecast()?;
        tracing::debug!(?forecast, "workload forecast model.rs calculated.");

        let total_records = self.total_records_between(&forecast, trigger, recovery)? + buffered_records;
        tracing::debug!(total_records_at_valid_time=%total_records, "estimated total records to process before valid time");

        let recovery_rate = self.recovery_rate(total_records);
        let valid_workload_rate = forecast.workload_at(valid)?;
        let target_rate = RecordsPerSecond::max(recovery_rate, valid_workload_rate);

        tracing::debug!(
            %recovery_rate,
            %valid_workload_rate,
            %target_rate,
            "target rate calculated as max of recovery and workload (at valid time) rates."
        );

        Ok(target_rate)
    }

    fn calculate_recovery_timestamp_from(&self, timestamp: Timestamp) -> Timestamp {
        timestamp + self.restart + self.max_catch_up
    }

    fn calculate_valid_timestamp_after_recovery(&self, recovery: Timestamp) -> Timestamp {
        recovery + self.valid_offset
    }

    fn total_records_between(
        &self, forecast: &Box<dyn WorkloadForecast>, start: Timestamp, end: Timestamp,
    ) -> Result<f64, PlanError> {
        let total = forecast.total_records_between(start, end)?;
        tracing::debug!("total records between [{}, {}] = {}", start, end, total);
        Ok(total)
    }

    fn recovery_rate(&self, total_records: f64) -> RecordsPerSecond {
        let catch_up = self.max_catch_up.as_secs_f64();
        RecordsPerSecond::new(total_records / catch_up)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use claim::*;

    use super::*;
    use crate::phases::plan::forecast::*;
    use chrono::Utc;

    #[test]
    fn test_creation() {
        let d1 = Duration::from_secs(2 * 60);
        let d2 = Duration::from_secs(13 * 60);
        let d3 = Duration::from_secs(5 * 60);

        assert_ok!(ForecastCalculator::new(MockWorkloadForecastBuilder::new(), d1, d2, d3));
        assert_ok!(ForecastCalculator::new(
            MockWorkloadForecastBuilder::new(),
            d1,
            Duration::from_millis(123_456_7),
            d3
        ));
        let err1 = assert_err!(ForecastCalculator::new(
            MockWorkloadForecastBuilder::new(),
            Duration::ZERO,
            d1,
            d3
        ));
        claim::assert_matches!(err1, PlanError::ZeroDuration(msg) if msg == "workload forecast restart".to_string());
        let err2 = assert_err!(ForecastCalculator::new(
            MockWorkloadForecastBuilder::new(),
            d1,
            Duration::ZERO,
            d3
        ));
        claim::assert_matches!(err2, PlanError::ZeroDuration(msg) if msg == "workload forecast max_catch_up".to_string());
        let err3 = assert_err!(ForecastCalculator::new(
            MockWorkloadForecastBuilder::new(),
            d1,
            d2,
            Duration::ZERO
        ));
        claim::assert_matches!(err3, PlanError::ZeroDuration(msg) if msg == "workload forecast valid_offset".to_string());
    }

    #[test]
    fn test_recovery_rate() {
        let restart = Duration::from_secs(2 * 60);
        let max_catch_up = Duration::from_secs(13 * 60);
        let valid_offset = Duration::from_secs(5 * 60);

        let c1 = assert_ok!(ForecastCalculator::new(
            MockWorkloadForecastBuilder::new(),
            restart,
            max_catch_up,
            valid_offset
        ));
        assert_relative_eq!(
            c1.recovery_rate(100.),
            RecordsPerSecond::new(0.128_205),
            epsilon = 1.0e-6
        );
        assert_relative_eq!(c1.recovery_rate(0.), RecordsPerSecond::new(0.), epsilon = 1.0e-6);

        let c2 = assert_ok!(ForecastCalculator::new(
            MockWorkloadForecastBuilder::new(),
            restart,
            Duration::from_millis(123_456_7),
            valid_offset
        ));
        assert_relative_eq!(
            c2.recovery_rate(std::f64::consts::PI * 1_000.),
            RecordsPerSecond::new(2.544_691_907_032_82),
            epsilon = 1.0e-10
        );

        let c3 = assert_err!(ForecastCalculator::new(
            MockWorkloadForecastBuilder::new(),
            restart,
            Duration::ZERO,
            valid_offset
        ));
        claim::assert_matches!(c3, PlanError::ZeroDuration(msg) if msg == "workload forecast max_catch_up".to_string());
    }

    #[test]
    fn test_calculate_valid_timestamp_after_recovery() {
        let restart = Duration::from_secs(2 * 60);
        let max_catch_up = Duration::from_secs(13 * 60);
        let valid_offset = Duration::from_millis(5 * 60 * 1_000 + 750);
        let c1 = assert_ok!(ForecastCalculator::new(
            MockWorkloadForecastBuilder::new(),
            restart,
            max_catch_up,
            valid_offset
        ));
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
        let restart = Duration::from_secs(2 * 60);
        let max_catch_up = Duration::from_millis(13 * 60 * 1_000 + 400);
        let valid_offset = Duration::from_secs(5 * 60);
        let c1 = assert_ok!(ForecastCalculator::new(
            MockWorkloadForecastBuilder::new(),
            restart,
            max_catch_up,
            valid_offset
        ));
        let now = Utc::now();
        let secs = now.timestamp() + (2 * 60) + (13 * 60);
        let nsecs = now.timestamp_subsec_nanos() + 400 * 1_000_000;
        let ts: Timestamp = now.into();
        assert_relative_eq!(
            c1.calculate_recovery_timestamp_from(ts),
            Timestamp::new(secs, nsecs),
            epsilon = 1.0e-10
        );
    }

    #[test]
    fn test_calculate_target_rate() -> anyhow::Result<()> {
        lazy_static::initialize(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_calculate_target_rate");
        let _main_span_guard = main_span.enter();

        let now = Utc::now();
        let restart = Duration::from_secs(2 * 60);
        let max_catch_up = Duration::from_secs(13 * 60);
        let valid_offset = Duration::from_secs(5 * 60);

        fn mock_workload_calc_builder(valid_workload: RecordsPerSecond) -> impl WorkloadForecastBuilder {
            let mut builder = MockWorkloadForecastBuilder::new();
            builder.expect_build_forecast().times(1).returning(move || {
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
            restart,
            max_catch_up,
            valid_offset
        ));
        let actual = assert_ok!(c1.calculate_target_rate(now.into(), 333.));
        assert_relative_eq!(actual, RecordsPerSecond::new(0.5551282), epsilon = 1.0e-7);

        let mut c2 = assert_ok!(ForecastCalculator::new(
            mock_workload_calc_builder(314.159.into()),
            restart,
            max_catch_up,
            valid_offset
        ));
        let actual = assert_ok!(c2.calculate_target_rate(now.into(), 333.));
        assert_relative_eq!(actual, RecordsPerSecond::new(314.159), epsilon = 1.0e-10);
        Ok(())
    }
}
