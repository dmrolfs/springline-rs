mod decision_template_data_strategy;
mod metric_catalog_strategy;
mod policy_scenario;
mod resources_policy_tests;
mod resources_regression_tests;

pub use decision_template_data_strategy::*;
pub use metric_catalog_strategy::*;
pub use policy_scenario::*;
use std::ops::RangeInclusive;

pub use super::policy::DecisionTemplateData;
pub use super::{ScaleDirection, DECISION_DIRECTION};
pub use crate::flink::{AppDataWindow, MetricCatalog};
pub use pretty_snowflake::MachineNode;
pub use proctor::elements::{telemetry, TelemetryValue, Timestamp};
pub use proctor::error::PolicyError;
pub use proptest::prelude::*;
pub use std::time::Duration;
pub use claim::*;

pub use crate::phases::REASON;
pub const RELATIVE_LAG_VELOCITY: &str = "relative_lag_velocity";
pub const LOW_UTILIZATION: &str = "low_utilization";
pub const TOTAL_LAG: &str = "total_lag";
pub const CPU_LOAD: &str = "cpu_load";
pub const HEAP_MEMORY_LOAD: &str = "heap_memory_load";
pub const INPUT_NETWORK_IO: &str = "input_network_io";
pub const OUTPUT_NETWORK_IO: &str = "output_network_io";

#[tracing::instrument(level = "info")]
pub fn arb_perturbed_duration(center: Duration, perturb_factor: f64) -> impl Strategy<Value = Duration> {
    Just(center.as_secs_f64()).prop_perturb(move |center, mut rng| {
        let factor_range = (-perturb_factor)..=perturb_factor;
        let deviation = center * rng.gen_range(factor_range);
        Duration::from_secs_f64(center + deviation)
    })
}

pub fn arb_timestamp_window(
    start: Timestamp, window: impl Strategy<Value = Duration>, interval: impl Strategy<Value = Duration> + 'static,
) -> impl Strategy<Value = (Vec<Timestamp>, Duration)> {
    let interval = interval.boxed();
    let interval = move || interval.clone();
    window.prop_flat_map(move |window| {
        let acc_start = Just((vec![start], Some((start, window))));
        do_arb_timestamp_window_loop(acc_start, interval.clone()).prop_map(move |(ts, _)| (ts, window))
    })
}

#[tracing::instrument(level = "info", skip(acc, make_interval_strategy))]
fn do_arb_timestamp_window_loop<I>(
    acc: impl Strategy<Value = (Vec<Timestamp>, Option<(Timestamp, Duration)>)>, make_interval_strategy: I,
) -> impl Strategy<Value = (Vec<Timestamp>, Option<(Timestamp, Duration)>)>
where
    I: Fn() -> BoxedStrategy<Duration> + Clone + 'static,
{
    (acc, make_interval_strategy()).prop_flat_map(move |((mut acc_ts, next_remaining), next_interval)| {
        {
            match next_remaining {
                None => Just((acc_ts, None)).boxed(),
                Some((_, remaining)) if remaining < next_interval => Just((acc_ts, None)).boxed(),
                Some((last_ts, remaining)) => {
                    let next_ts = last_ts + next_interval;
                    let next_remaining = remaining - next_interval;
                    acc_ts.push(next_ts);
                    let next_acc: (Vec<Timestamp>, Option<(Timestamp, Duration)>) =
                        (acc_ts, Some((next_ts, next_remaining)));
                    do_arb_timestamp_window_loop(Just(next_acc), make_interval_strategy.clone()).boxed()
                },
            }
        }
        .boxed()
    })
}

pub fn arb_machine_node() -> impl Strategy<Value = MachineNode> {
    (0_i32..=31, 0_i32..=31).prop_map(|(machine_id, node_id)| MachineNode { machine_id, node_id })
}

pub fn arb_range_duration(range_secs: RangeInclusive<u64>) -> impl Strategy<Value = Duration> {
    range_secs.prop_map(Duration::from_secs)
}

pub fn arb_f64_range(min_inclusive: f64, max_exclusive: f64) -> impl Strategy<Value = f64> {
    any::<f64>().prop_filter("out of range", move |val| min_inclusive <= *val && *val < max_exclusive)
}

pub fn arb_timestamp() -> impl Strategy<Value = Timestamp> {
    (any::<i64>(), any::<u32>()).prop_filter_map("negative", |(secs, nanos)| {
        if secs <= 0 {
            None
        } else {
            Some(Timestamp::new(secs, nanos))
        }
    })
}

pub fn arb_timestamp_after(
    ts: Timestamp, duration: impl Strategy<Value = Duration>,
) -> impl Strategy<Value = Timestamp> {
    duration.prop_map(move |dur| ts + dur)
}

pub fn arb_telemetry_value() -> impl Strategy<Value = TelemetryValue> {
    let leaf = prop_oneof![
        any::<i64>().prop_map(TelemetryValue::Integer),
        any::<f64>().prop_map(TelemetryValue::Float),
        any::<bool>().prop_map(TelemetryValue::Boolean),
        "\\PC*".prop_map(TelemetryValue::Text),
        Just(TelemetryValue::Unit),
    ];
    leaf.prop_recursive(
        1,  // 8 levels deep
        16, // Shoot for maximum size of 256 nodes
        10, // We put up to 10 items per collection
        |inner| {
            prop_oneof![
                // Take the inner strategy and make the two recursive cases.
                prop::collection::vec(inner.clone(), 0..10).prop_map(TelemetryValue::Seq),
                arb_telemetry_table_type().prop_map(|table: telemetry::TableType| TelemetryValue::Table(table.into())),
            ]
        },
    )
    .boxed()
}

pub fn arb_telemetry_table_type() -> impl Strategy<Value = telemetry::TableType> {
    prop::collection::hash_map("\\PC*", arb_telemetry_value(), 0..3)
}

proptest! {
    #[test]
    fn test_arb_telemetry_value_doesnt_overflow_stack(value in arb_telemetry_value()) {
        let value_0 = value.clone();
        match value {
            TelemetryValue::Unit => prop_assert_eq!(value_0, TelemetryValue::Unit),
            TelemetryValue::Boolean(b) => prop_assert_eq!(value_0, TelemetryValue::Boolean(b)),
            TelemetryValue::Integer(i) => prop_assert_eq!(value_0, TelemetryValue::Integer(i)),
            TelemetryValue::Float(f) => prop_assert_eq!(value_0, TelemetryValue::Float(f)),
            TelemetryValue::Text(rep) => prop_assert_eq!(value_0, TelemetryValue::Text(rep)),
            TelemetryValue::Seq(rep) => prop_assert_eq!(value_0, TelemetryValue::Seq(rep)),
            TelemetryValue::Table(rep) => prop_assert_eq!(value_0, TelemetryValue::Table(rep)),
        }
    }

    #[test]
    fn test_arb_timestamp_window(
        (timestamps, window) in arb_timestamp_window(
            Timestamp::now(),
            (10_u64..=600).prop_map(Duration::from_secs),
            arb_perturbed_duration(Duration::from_secs(15), 0.2)
        )
    ) {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_arb_timestamp_window", ?timestamps, nr_timestamps=%timestamps.len(), ?window);
        let _main_span_guard = main_span.enter();

        tracing::info!(?timestamps, ?window, "DMR: timestamp window[{}]", timestamps.len());
        let mut last = None;
        if let Some((first, last)) = timestamps.first().zip(timestamps.last()) {
            let diff = *last - *first;
            prop_assert!(diff <= window);
        }

        let max_allowed = Duration::from_secs_f64(15.0 * 1.2);
        let min_allowed = Duration::from_secs_f64(15.0 * 0.8);

        for ts in timestamps.into_iter() {
            if let Some(last_ts) = last {
                let current_interval = ts - last_ts;
                tracing::info!(current_interval=?(ts - last_ts), ?min_allowed, ?max_allowed, "checking timestamp...");
                prop_assert!(last_ts < ts, "did not hold for last:[{last_ts:?}] < ts:[{ts:?}]");
                prop_assert!(min_allowed <= current_interval, "min_allowed did not hold for last_ts:[{last_ts:?}] ts:[{ts:?}]");
                prop_assert!(current_interval < max_allowed, "max_allowed did not hold for last_ts:[{last_ts:?}] ts:[{ts:?}]");
            }

            last = Some(ts);
        }
    }
}
