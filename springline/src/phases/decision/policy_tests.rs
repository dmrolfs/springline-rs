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

// prop_compose! {
//     pub fn arb_timestamp_window(
//         start: Timestamp
//     )(
//         interval in arb_perturbed_duration(Duration::from_secs(15), 0.2),
//         window in (10_u64..=600).prop_map(Duration::from_secs),
//     ) -> (Vec<Timestamp>, Duration) {
//         once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
//         let span = tracing::info_span!("arb_timestamp_window", ?interval, ?window);
//         let _guard = span.enter();
//
//         let mut timestamps = vec![start];
//         let mut current = start;
//         let mut current_window_duration = Duration::ZERO;
//         if interval <= window {
//             tracing::info!(?current_window_duration, "building timestamps vec...");
//             while interval <= (window - current_window_duration) {
//                 current = current + interval;
//                 current_window_duration = current - start;
//                 timestamps.push(current);
//                 tracing::info!(?current_window_duration, "looking to next point");
//             }
//         }
//
//         (timestamps, current_window_duration)
//     }
// }
//
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
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let span = tracing::info_span!("arb_timestamp_window", ?start);
    let _guard = span.enter();

    let interval = interval.boxed();
    let interval = move || interval.clone();
    window.prop_flat_map(move |window| {
        let span = tracing::info_span!("arb_timestamp_window_inner", ?window);
        let _guard_inner = span.enter();
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
    tracing::info!("111");
    // let interval_strategy = interval.clone();
    tracing::info!("222");
    (acc, make_interval_strategy()).prop_flat_map(move |((mut acc_ts, next_remaining), next_interval)| {
        {
            let span = tracing::info_span!("do_arb_timestamp_window_loop_inner", ?next_remaining, ?next_interval);
            let _guard_inner = span.enter();

            let foo = match next_remaining {
                None => Just((acc_ts, None)).boxed(),
                Some((_, remaining)) if remaining < next_interval => Just((acc_ts, None)).boxed(),
                Some((last_ts, remaining)) => {
                    let span = tracing::info_span!("do_arb_timestamp_window_loop_inner_inner", ?remaining);
                    let _guard_inner_inner = span.enter();

                    let next_ts = last_ts + next_interval;
                    tracing::info!(?next_ts, "AAA");
                    let next_remaining = remaining - next_interval;
                    tracing::info!(?next_remaining, "BBB");
                    acc_ts.push(next_ts);
                    tracing::info!(?acc_ts, "CCC");
                    let next_acc: (Vec<Timestamp>, Option<(Timestamp, Duration)>) =
                        (acc_ts, Some((next_ts, next_remaining)));
                    tracing::info!(?next_acc, "DDD");
                    let result = do_arb_timestamp_window_loop(Just(next_acc), make_interval_strategy.clone());
                    tracing::info!("EEE");
                    result.boxed()
                },
            };
            tracing::info!("FFF");
            foo
        }
        .boxed()
    })
}

// pub fn arb_timestamp_window(
//     start: Timestamp, delay: impl Strategy<Value = Duration>, interval: impl Strategy<Value = Duration> + 'static,
//     window: impl Strategy<Value = Duration>,
// ) -> impl Strategy<Value = (Vec<Timestamp>, Duration)> {
//     let interval = interval.boxed();
//
//     (delay, window)
//         .prop_filter("delay exceeds window", |(delay, window)| delay < window)
//         .prop_flat_map(move |(delay, window)| {
//             let first_ts = start + delay;
//             let remaining = window - delay;
//             let acc = Just((vec![first_ts], Some((first_ts, remaining)))).boxed();
//             let window_strategy = do_next_arb_window_timestamp(acc, interval.clone()).prop_map(|(acc_ts, extra)| {
//                 if let Some((extra_next_ts, extra_remaining)) = extra {
//                     tracing::error!(
//                         %extra_next_ts, ?extra_remaining,
//                         "unexpected extra values from end of timestamp window strategy."
//                     );
//                 }
//
//                 acc_ts
//             });
//
//             window_strategy.prop_map(move |strategy| (strategy, window))
//         })
// }
//
// fn do_next_arb_window_timestamp(
//     acc: BoxedStrategy<(Vec<Timestamp>, Option<(Timestamp, Duration)>)>, interval: BoxedStrategy<Duration>,
// ) -> BoxedStrategy<(Vec<Timestamp>, Option<(Timestamp, Duration)>)> {
//     (acc, interval.clone())
//         .prop_flat_map(
//             move |((mut acc_ts, next_remaining), next_interval)| match next_remaining {
//                 None => Just((acc_ts, None)).boxed(),
//                 Some((_, remaining)) if remaining < next_interval => Just((acc_ts, None)).boxed(),
//                 Some((next, remaining)) => {
//                     let next_ts = next + next_interval;
//                     let next_remaining = remaining - next_interval;
//                     acc_ts.push(next_ts);
//                     let next_acc: (Vec<Timestamp>, Option<(Timestamp, Duration)>) =
//                         (acc_ts, Some((next_ts, next_remaining)));
//                     do_next_arb_window_timestamp(Just(next_acc).boxed(), interval.clone())
//                 },
//             },
//         )
//         .boxed()
// }

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
    prop::collection::hash_map(".*", arb_telemetry_value(), 0..5)
}

// fn do_arb_timestamp_window() -> impl Strategy<Value = (Vec<Timestamp>, Duration)> {
//     arb_timestamp().prop_flat_map(|start| {
//         arb_timestamp_window(
//             start,
//             arb_range_duration(0..=10),
//             arb_range_duration(1..=15),
//             arb_range_duration(10..=300),
//         )
//     })
// }

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
