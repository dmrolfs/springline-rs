use super::*;
use itertools::Itertools;

proptest! {
    #[test]
    fn doesnt_crash(scenario in PolicyScenario::strategy("decision_basis")) {
        match scenario.run() {
            Ok(_) => (),
            Err(PolicyError::Validation(_)) => (),
            result => prop_assert!(result.is_ok()),
        }
    }

    #[test]
    fn test_scale_up_relative_lag_velocity(
        scenario in PolicyScenario::builder()
        .template_data(prop::option::of(DecisionTemplateDataStrategyBuilder::default().finish()))
        .items(
            arb_metric_catalog_window(
                Timestamp::now(),
                arb_range_duration(30..=600),
                arb_perturbed_duration(Duration::from_secs(15), 0.2),
                |recv_ts| {
                    MetricCatalogStrategyBuilder::new()
                        .just_recv_timestamp(recv_ts)
                        // .flow(
                        //     FlowMetricsStrategyBuilder::new()
                                // .just_source_total_lag(Some(2))
                                // .just_source_records_consumed_rate(Some(1.0))
                                // .finish()
                        // )
                        .finish()
                        .boxed()
                }
            )
        )
        .strategy()
    ) {
        let query_result = match scenario.run() {
            Ok(r) => Some(r),
            Err(PolicyError::Validation(err)) => None,
            Err(err) => {
                prop_assert!(false, "validation error: {:?}", err);
                None
            },
        };


        if let Some(result) = query_result {
            let eval_secs = scenario.template_data.as_ref().and_then(|td| td.evaluate_duration_secs).unwrap_or(60);

            match scenario.template_data.and_then(|template| template.max_healthy_relative_lag_velocity) {
                _ if !result.passed => {
                    prop_assert!(result.bindings.is_empty());
                },
                None => {
                    prop_assert!(result.bindings.contains_key(REASON));
                    let reasons = assert_some!(result.bindings.get(REASON));
                    prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(RELATIVE_LAG_VELOCITY)));
                },
                Some(max_relative_lag) if max_relative_lag < scenario.item.flow_source_relative_lag_change_rate(eval_secs) => {
                    prop_assert!(result.passed);
                    prop_assert!(result.bindings.contains_key(REASON));
                    let reasons = assert_some!(result.bindings.get(REASON));
                    prop_assert!(reasons.into_iter().contains(&TelemetryValue::from(RELATIVE_LAG_VELOCITY)));
                },
                Some(_) if result.bindings.contains_key(REASON) => {
                    let reasons = assert_some!(result.bindings.get(REASON));
                    prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(RELATIVE_LAG_VELOCITY)));
                },

                Some(_) => {}
            }
        }
    }
}
