use approx::assert_relative_eq;
use chrono::{DateTime, TimeZone, Utc};
use claim::{assert_err, assert_ok, assert_some};
use proctor::elements::RecordsPerSecond;
use proctor::error::PlanError;
use springline::phases::plan::{Forecaster, LeastSquaresWorkloadForecaster};
use springline::phases::plan::{SpikeSettings, WorkloadMeasurement};

fn make_measurement(timestamp: DateTime<Utc>, workload: RecordsPerSecond) -> WorkloadMeasurement {
    WorkloadMeasurement { timestamp_secs: timestamp.timestamp(), workload }
}

#[test]
fn test_flink_plan_calculator() -> anyhow::Result<()> {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_flink_plan_calculator");
    let _ = main_span.enter();

    let now = 1624061766; // Utc::now().timestamp();
    let step = 15;

    let expected_workload: Vec<(RecordsPerSecond, Option<RecordsPerSecond>)> = vec![
        (1.0.into(), None),
        (2.0.into(), None),
        (3.0.into(), None),
        (4.0.into(), None),
        (5.0.into(), None),
        (6.0.into(), None),
        (7.0.into(), None),
        (8.0.into(), None),
        (9.0.into(), None),
        (10.0.into(), None),
        (11.0.into(), None),
        (12.0.into(), None),
        (13.0.into(), None),
        (14.0.into(), None),
        (15.0.into(), None),
        (16.0.into(), None),
        (17.0.into(), None),
        (18.0.into(), None),
        (19.0.into(), None),
        (20.0.into(), Some(20.15286.into())),
        (21.0.into(), Some(21.20346.into())),
        (22.0.into(), Some(22.32689.into())),
        (23.0.into(), Some(23.11524.into())),
        (24.0.into(), Some(24.69824.into())),
        (25.0.into(), Some(25.47945.into())),
        (26.0.into(), Some(26.81368.into())),
        (27.0.into(), Some(27.38790.into())),
        (28.0.into(), Some(28.27270.into())),
        (29.0.into(), Some(29.14475.into())),
        (30.0.into(), Some(30.27993.into())),
    ];

    let mut forecast_builder = LeastSquaresWorkloadForecaster::new(
        20,
        SpikeSettings {
            std_deviation_threshold: 5.,
            influence: 0.5,
            length_threshold: 3,
        },
    );

    for (i, (workload, expected)) in expected_workload.into_iter().enumerate() {
        let ts = assert_some!(Utc.timestamp_opt(now + (i as i64) * step, 0).single());
        tracing::info!(
            "i:{}-timestamp:{:?} ==> actual:{} expected:{:?}",
            i,
            ts,
            workload,
            expected,
        );

        let measurement = make_measurement(ts, workload);
        forecast_builder.add_observation(measurement);

        let forecast = forecast_builder.forecast();
        if let Some(e) = expected {
            let forecast = assert_ok!(forecast);
            let actual = assert_ok!(forecast.workload_at(ts.into()));
            tracing::info!(%actual, expected=%e, "[{}] testing workload prediction.", i);
            assert_relative_eq!(actual, e, epsilon = 1.0e-4);
        } else {
            let plan_error = assert_err!(forecast);
            claim::assert_matches!((i, plan_error), (i, PlanError::NotEnoughData { supplied: s, need: 20 }) if s == i + 1);
        }
    }

    Ok(())
}
