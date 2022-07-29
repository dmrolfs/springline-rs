mod decision_template_data_strategy;
mod metric_catalog_strategy;
mod policy_scenario;
mod resources_policy_tests;

pub use decision_template_data_strategy::*;
pub use metric_catalog_strategy::*;
pub use policy_scenario::*;
use std::ops::RangeInclusive;

pub use super::policy::DecisionTemplateData;
pub use super::{ScaleDirection, DECISION_DIRECTION};
pub use crate::flink::{AppDataWindow, MetricCatalog};
pub use pretty_snowflake::MachineNode;
pub use proctor::elements::{telemetry, TelemetryValue, Timestamp};
pub use proptest::prelude::*;
pub use std::time::Duration;

pub fn arb_timestamp_window(
    start: Timestamp, delay: impl Strategy<Value = Duration>, interval: impl Strategy<Value = Duration> + 'static,
    window: impl Strategy<Value = Duration>,
) -> impl Strategy<Value = (Vec<Timestamp>, Duration)> {
    let interval = interval.boxed();

    (delay, window)
        .prop_filter("delay exceeds window", |(delay, window)| delay < window)
        .prop_flat_map(move |(delay, window)| {
            let first_ts = start + delay;
            let remaining = window - delay;
            let acc = Just((vec![first_ts], Some((first_ts, remaining)))).boxed();
            let window_strategy = do_next_arb_window_timestamp(acc, interval.clone()).prop_map(|(acc_ts, extra)| {
                if let Some((extra_next_ts, extra_remaining)) = extra {
                    tracing::error!(
                        %extra_next_ts, ?extra_remaining,
                        "unexpected extra values from end of timestamp window strategy."
                    );
                }

                acc_ts
            });

            window_strategy.prop_map(move |strategy| (strategy, window))
        })
}

fn do_next_arb_window_timestamp(
    acc: BoxedStrategy<(Vec<Timestamp>, Option<(Timestamp, Duration)>)>, interval: BoxedStrategy<Duration>,
) -> BoxedStrategy<(Vec<Timestamp>, Option<(Timestamp, Duration)>)> {
    (acc, interval.clone())
        .prop_flat_map(
            move |((mut acc_ts, next_remaining), next_interval)| match next_remaining {
                None => Just((acc_ts, None)).boxed(),
                Some((_, remaining)) if remaining < next_interval => Just((acc_ts, None)).boxed(),
                Some((next, remaining)) => {
                    let next_ts = next + next_interval;
                    let next_remaining = remaining - next_interval;
                    acc_ts.push(next_ts);
                    let next_acc: (Vec<Timestamp>, Option<(Timestamp, Duration)>) =
                        (acc_ts, Some((next_ts, next_remaining)));
                    do_next_arb_window_timestamp(Just(next_acc).boxed(), interval.clone())
                },
            },
        )
        .boxed()
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

//todo - DMR - WORK HERE
// pub fn arb_duration_range(secs_range: prop::collection::SizeRange) -> impl Strategy<Value = Duration> {
//     secs_range.lift().prop_map(|(start, end)| {
//
//     })
// }

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
        ".*".prop_map(TelemetryValue::Text),
        Just(TelemetryValue::Unit),
    ];
    leaf.prop_recursive(
        2,  // 8 levels deep
        32, // Shoot for maximum size of 256 nodes
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
    prop::collection::hash_map(".*", arb_telemetry_value(), 0..10)
}

fn do_arb_timestamp_window() -> impl Strategy<Value = (Vec<Timestamp>, Duration)> {
    arb_timestamp().prop_flat_map(|start| {
        arb_timestamp_window(
            start,
            arb_range_duration(0..=10),
            arb_range_duration(1..=15),
            arb_range_duration(10..=300),
        )
    })
}

proptest! {
    #[test]
    fn test_arb_timestamp_window(
        (timestamps, window) in arb_timestamp_window(
            Timestamp::now(),
            arb_range_duration(0..=10),
            arb_range_duration(1..=15),
            arb_range_duration(10..=300),
        )
    ) {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_arb_timestamp_window");
        let _main_span_guard = main_span.enter();

        tracing::info!(?timestamps, ?window, "DMR: timestamp window[{}]", timestamps.len());
        let mut last = None;
        if let Some((first, last)) = timestamps.first().zip(timestamps.last()) {
            let diff = *last - *first;
            prop_assert!(diff <= window);
        }

        for ts in timestamps.into_iter() {
            if let Some(last_ts) = last {
                prop_assert!(last_ts < ts, "did not hold for last:[{last_ts:?}] < ts:[{ts:?}]");
                prop_assert!(ts - last_ts <= Duration::from_secs(15), "did not hold for last_ts:[{last_ts:?}] ts:[{ts:?}]");
            }

            last = Some(ts);
        }
    }
}
