use std::collections::VecDeque;
use std::fmt::Debug;

use proctor::elements::{Point, SignalDetector};
use proctor::error::PlanError;
use serde::{Deserialize, Serialize};

use super::WorkloadForecast;
use crate::phases::plan::forecast::regression::{LinearRegression, QuadraticRegression};
use crate::phases::plan::forecast::Forecaster;
use crate::phases::plan::forecast::WorkloadMeasurement;

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpikeSettings {
    pub std_deviation_threshold: f64,
    pub influence: f64,
    pub length_threshold: usize,
}

pub const SPIKE_STD_DEV_THRESHOLD: f64 = 3.;
pub const SPIKE_INFLUENCE: f64 = 0.;
pub const SPIKE_LENGTH_THRESHOLD: usize = 3;

impl Default for SpikeSettings {
    fn default() -> Self {
        Self {
            std_deviation_threshold: SPIKE_STD_DEV_THRESHOLD,
            influence: SPIKE_INFLUENCE,
            length_threshold: SPIKE_LENGTH_THRESHOLD,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LeastSquaresWorkloadForecaster {
    window_size: usize,
    spike_length_threshold: usize,
    data: VecDeque<Point>,
    spike_detector: SignalDetector,
    spike_length: usize,
}

const OBSERVATION_WINDOW_SIZE: usize = 20;

impl Default for LeastSquaresWorkloadForecaster {
    fn default() -> Self {
        Self::new(OBSERVATION_WINDOW_SIZE, SpikeSettings::default())
    }
}

impl LeastSquaresWorkloadForecaster {
    pub fn new(window: usize, spike_settings: SpikeSettings) -> Self {
        Self {
            window_size: window,
            spike_length_threshold: spike_settings.length_threshold,
            data: VecDeque::with_capacity(window),
            spike_detector: SignalDetector::new(
                window,
                spike_settings.std_deviation_threshold,
                spike_settings.influence,
            ),
            spike_length: 0,
        }
    }

    fn data_slice(&self) -> &[Point] {
        self.data.as_slices().0
    }

    fn assess_spike(&mut self, observation: Point) -> usize {
        if let Some(_spike) = self.spike_detector.signal(observation.1) {
            self.spike_length += 1;
            tracing::debug!(consecutive_spikes=%self.spike_length, "anomaly detected at {:?}", observation);
        } else {
            self.spike_length = 0;
        }

        self.spike_length
    }

    const fn exceeded_spike_threshold(&self) -> bool {
        self.spike_length_threshold <= self.spike_length
    }

    fn drop_data(&mut self, range: impl std::ops::RangeBounds<usize>) -> Vec<Point> {
        self.spike_length = 0;
        let dropped = self.data.drain(range).collect();
        self.spike_detector.clear();
        self.data.iter().for_each(|point| {
            let _ = self.spike_detector.signal(point.1);
        });
        dropped
    }
}

impl Forecaster for LeastSquaresWorkloadForecaster {
    fn observations_needed(&self) -> (usize, usize) {
        (self.window_size - self.data.len(), self.window_size)
    }

    fn expected_next_observation_timestamp(&self, from_ts: f64) -> f64 {
        let (steps, _) = self.data.iter().map(|p| p.0).fold(
            (Vec::<f64>::with_capacity(self.data.len() - 1), None),
            |(mut acc, last), ts| match last {
                None => (acc, Some(ts)),
                Some(l) => {
                    acc.push(ts - l);
                    (acc, Some(ts))
                },
            },
        );

        let avg = if steps.is_empty() {
            0_f64
        } else {
            let len = steps.len() as f64;
            let total: f64 = steps.into_iter().sum();
            total / len
        };
        from_ts + avg
    }

    #[tracing::instrument(level="debug", skip(self), fields(spike_length=%self.spike_length,),)]
    fn add_observation(&mut self, measurement: WorkloadMeasurement) {
        let data = measurement.into();
        self.assess_spike(data);
        // drop up to start of spike in order to establish new prediction function
        if self.exceeded_spike_threshold() {
            let dropped = self.drop_data(..(self.data.len() - self.spike_length_threshold));
            tracing::info!(
                nr_dropped=%dropped.len(), nr_remaining=%self.data.len(),
                "exceeded spike threshold - dropping observations before spike."
            );
        }

        while self.window_size <= self.data.len() {
            self.data.pop_front();
        }
        self.data.push_back(data);
        assert!(self.data.len() <= self.window_size);
    }

    fn clear(&mut self) {
        self.data.clear();
        self.spike_detector.clear();
        self.spike_length = 0;
    }

    fn forecast(&mut self) -> Result<Box<dyn WorkloadForecast>, PlanError> {
        if !self.have_enough_data() {
            return Err(PlanError::NotEnoughData { supplied: self.data.len(), need: self.window_size });
        }

        self.data.make_contiguous();
        let data = self.data_slice();
        let model = Self::do_select_model(data);
        Ok(model)
    }
}

impl LeastSquaresWorkloadForecaster {
    pub fn have_enough_data(&self) -> bool {
        self.window_size <= self.data.len()
    }

    #[tracing::instrument(level = "debug", skip(data))]
    fn do_select_model(data: &[Point]) -> Box<dyn WorkloadForecast> {
        let linear = LinearRegression::from_data(data);
        let linear_r = linear.correlation_coefficient;

        QuadraticRegression::from_data(data).map_or_else(
            || {
                tracing::debug!(
                    "failed to calculate the quadratic model due to a matrix decomposition issue - using linear model."
                );
                let model: Box<dyn WorkloadForecast> = Box::new(linear);
                model
            },
            |quadratic| {
                let quadratic_r = quadratic.correlation_coefficient;

                let model: Box<dyn WorkloadForecast> = match (linear_r, quadratic_r) {
                    (_, q_r) if q_r.is_nan() => Box::new(linear),
                    (l_r, _) if l_r.is_nan() => Box::new(quadratic),
                    (l_r, q_r) if l_r < q_r => Box::new(quadratic),
                    _ => Box::new(linear),
                };

                tracing::debug!(%linear_r, %quadratic_r, "selected workload prediction model: {}", model.name());
                model
            },
        )
    }
}

impl std::ops::Add<WorkloadMeasurement> for LeastSquaresWorkloadForecaster {
    type Output = Self;

    fn add(mut self, rhs: WorkloadMeasurement) -> Self::Output {
        self.add_observation(rhs);
        self
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use chrono::{DateTime, TimeZone, Utc};
    use claim::{assert_err, assert_ok};
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::phases::plan::forecast::Point;

    #[test]
    fn test_plan_forecast_measure_spike() -> anyhow::Result<()> {
        let data: Vec<Point> = vec![
            1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 0.9, // 00 - 09
            1.0, 1.1, 1.0, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 1.0, // 10 - 19
            1.0, 1.0, 1.1, 0.9, 1.0, 1.1, 1.0, 1.0, 0.9, 1.0, // 20 - 29
            1.1, 1.0, 1.0, 1.1, 1.0, 0.8, 0.9, 1.0, 1.2, 0.9, // 30 - 39
            1.0, 1.0, 1.1, 1.2, 1.0, 1.5, 1.0, 3.0, 2.0, 5.0, // 40 - 49
            3.0, 2.0, 1.0, 1.0, 1.0, 0.9, 1.0, 1.0, 3.0, 2.6, // 50 - 59
            4.0, 3.0, 3.2, 2.0, 1.0, 1.0, 0.8, 4.0, 4.0, 2.0, // 60 - 69
            2.5, 1.0, 1.0, 1.0, // 70 - 73
        ]
        .into_iter()
        .enumerate()
        .map(|(x, y)| (x as f64, y))
        .collect();

        let test_scenario = |influence: f64, measurements: Vec<usize>| {
            let test_data: Vec<(Point, usize)> = data.clone().into_iter().zip(measurements).collect();

            let spike_settings: SpikeSettings = SpikeSettings {
                std_deviation_threshold: 5.,
                influence,
                length_threshold: SPIKE_LENGTH_THRESHOLD,
            };

            let mut forecast_builder = LeastSquaresWorkloadForecaster::new(30, spike_settings);

            for (pt, expected) in test_data.into_iter() {
                let actual = forecast_builder.assess_spike(pt);
                assert_eq!((influence, pt, actual), (influence, pt, expected));
            }
        };

        let spike_measure_0 = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00 - 09
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10 - 19
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20 - 29
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30 - 39
            0, 0, 0, 0, 0, 1, 0, 1, 2, 3, // 40 - 49
            4, 5, 0, 0, 0, 0, 0, 0, 1, 2, // 50 - 59
            3, 4, 5, 6, 0, 0, 0, 1, 2, 3, // 60 - 69
            4, 0, 0, 0, // 70 - 73
        ];
        test_scenario(0., spike_measure_0);

        let spike_measure_0_10 = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00 - 09
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10 - 19
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20 - 29
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30 - 39
            0, 0, 0, 0, 0, 1, 0, 1, 2, 3, // 40 - 49
            4, 0, 0, 0, 0, 0, 0, 0, 1, 2, // 50 - 59
            3, 4, 5, 0, 0, 0, 0, 1, 2, 0, // 60 - 69
            0, 0, 0, 0, // 70 - 73
        ];
        test_scenario(0.1, spike_measure_0_10);

        let spike_measure_0_25 = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00 - 09
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10 - 19
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20 - 29
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30 - 39
            0, 0, 0, 0, 0, 1, 0, 1, 2, 3, // 40 - 49
            4, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 50 - 59
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 60 - 69
            0, 0, 0, 0, // 70 - 73
        ];
        test_scenario(0.25, spike_measure_0_25);

        let spike_measure_0_5 = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00 - 09
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10 - 19
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20 - 29
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30 - 39
            0, 0, 0, 0, 0, 1, 0, 1, 0, 1, // 40 - 49
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 50 - 59
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 60 - 69
            0, 0, 0, 0, // 70 - 73
        ];
        test_scenario(0.5, spike_measure_0_5);

        let spike_measure_1 = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 00 - 09
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 10 - 19
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 20 - 29
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 30 - 39
            0, 0, 0, 0, 0, 1, 0, 1, 0, 1, // 40 - 49
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 50 - 59
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 60 - 69
            0, 0, 0, 0, // 70 - 73
        ];
        test_scenario(1., spike_measure_1);

        Ok(())
    }

    #[test]
    fn test_plan_forecast_estimate_next_timestamp() -> anyhow::Result<()> {
        let data = vec![(1., 32.5), (3., 37.3), (5., 36.4), (7., 32.4), (9., 28.5)];
        let spike_settings: SpikeSettings = SpikeSettings {
            std_deviation_threshold: 5.,
            influence: 0.75,
            length_threshold: SPIKE_LENGTH_THRESHOLD,
        };

        let mut forecaster = LeastSquaresWorkloadForecaster::new(3, spike_settings);
        data.into_iter().for_each(|(timestamp_secs, workload)| {
            forecaster.add_observation(WorkloadMeasurement {
                timestamp_secs: timestamp_secs as i64,
                workload: workload.into(),
            })
        });
        let actual = forecaster.expected_next_observation_timestamp(9.);
        assert_relative_eq!(actual, 11., epsilon = 1.0e-10);
        Ok(())
    }

    #[test]
    fn test_plan_forecast_model_selection() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_plan_forecast_model_selection");
        let _ = main_span.enter();

        let data_1 = vec![
            (-5., 15.88),
            (-4., 12.63),
            (-3., 12.50),
            (-2., 11.78),
            (-1., 11.38),
            (0., 9.18),
            (1., 10.43),
            (2., 11.02),
            (3., 11.57),
            (4., 11.97),
        ];

        let model_1 = LeastSquaresWorkloadForecaster::do_select_model(&data_1);
        assert_eq!(model_1.name(), "QuadraticRegression");

        let data_2 = vec![
            (1., 1.),
            (2., 2.),
            (3., 3.),
            (4., 4.),
            (5., 5.),
            (6., 6.),
            (7., 7.),
            (8., 8.),
            (9., 9.),
            (10., 10.),
        ];

        let model_2 = LeastSquaresWorkloadForecaster::do_select_model(&data_2);
        assert_eq!(model_2.name(), "LinearRegression");

        let data_3 = vec![
            (1., 0.),
            (2., 0.),
            (3., 0.),
            (4., 0.),
            (5., 0.),
            (6., 0.),
            (7., 0.),
            (8., 0.),
            (9., 0.),
            (10., 0.),
        ];

        let model_3 = LeastSquaresWorkloadForecaster::do_select_model(&data_3);
        assert_eq!(model_3.name(), "LinearRegression");

        let data_4 = vec![
            (1., 17.),
            (2., 17.),
            (3., 17.),
            (4., 17.),
            (5., 17.),
            (6., 17.),
            (7., 17.),
            (8., 17.),
            (9., 17.),
            (10., 17.),
        ];

        let model_4 = LeastSquaresWorkloadForecaster::do_select_model(&data_4);
        assert_eq!(model_4.name(), "LinearRegression");

        Ok(())
    }

    fn make_measurement(timestamp: DateTime<Utc>, workload: f64) -> WorkloadMeasurement {
        WorkloadMeasurement {
            timestamp_secs: timestamp.timestamp(),
            workload: workload.into(),
        }
    }

    #[test]
    fn test_plan_forecast_predict_workload() -> anyhow::Result<()> {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_plan_forecast_predict_workload");
        let _ = main_span.enter();

        let now: i64 = 1624061766;
        tracing::info!("NOW: {}", now);
        let step = 15;

        let workload_expected: Vec<(f64, Option<f64>)> = vec![
            (1., None),
            (2., None),
            (3., None),
            (4., None),
            (5., None),
            (6., None),
            (7., None),
            (8., None),
            (9., None),
            (10., None),
            (11., None),
            (12., None),
            (13., None),
            (14., None),
            (15., None),
            (16., None),
            (17., None),
            (18., None),
            (19., None),
            (20., Some(20.15286)),
            (21., Some(21.20346)),
            (22., Some(22.32689)),
            (23., Some(23.11524)),
            (24., Some(24.69824)),
            (25., Some(25.47945)),
            (26., Some(26.81368)),
            (27., Some(27.38790)),
            (28., Some(28.27270)),
            (29., Some(29.14475)),
            (30., Some(30.27993)),
        ];

        let spike_settings: SpikeSettings = SpikeSettings {
            std_deviation_threshold: 5.,
            influence: 0.5,
            length_threshold: SPIKE_LENGTH_THRESHOLD,
        };

        let mut forecast_builder = LeastSquaresWorkloadForecaster::new(20, spike_settings);

        for (i, (workload, expected)) in workload_expected.into_iter().enumerate() {
            let ts = Utc.timestamp(now + (i as i64) * step, 0);
            tracing::info!(
                "i:{}-timestamp:{:?} ==> test_workload:{} expected:{:?}",
                i,
                ts,
                workload,
                expected
            );

            let measurement = make_measurement(ts, workload);
            forecast_builder.add_observation(measurement);

            let forecast = forecast_builder.forecast();
            if let Some(e) = expected {
                let forecast = assert_ok!(forecast);
                let actual = assert_ok!(forecast.workload_at(ts.into()));
                tracing::info!(%actual, expected=%e, "[{}] testing workload prediction.", i);
                assert_relative_eq!(actual, e.into(), epsilon = 1.0e-4)
            } else {
                let plan_error = assert_err!(forecast);
                claim::assert_matches!((i, plan_error), (i, PlanError::NotEnoughData { supplied: s, need: 20 }) if s == i + 1);
            }
        }

        Ok(())
    }

    #[test]
    fn test_forecast_window_size_is_capped() {
        let mut forecast_builder = LeastSquaresWorkloadForecaster::new(
            13,
            SpikeSettings {
                std_deviation_threshold: 5.,
                influence: 0.5,
                length_threshold: SPIKE_LENGTH_THRESHOLD,
            },
        );

        let now = Utc::now().timestamp();
        let step = 15;
        let mut index = 0;
        while index < 100 {
            let ts = Utc.timestamp(now + index * step, 0);
            forecast_builder.add_observation(make_measurement(ts, index as f64));
            index += 1;
            let expected_len = index.min(forecast_builder.window_size as i64) as usize;
            assert_eq!(forecast_builder.data.len(), expected_len);
        }

        assert_eq!(forecast_builder.data.len(), forecast_builder.window_size);
    }
}
