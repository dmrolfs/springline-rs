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
        .template_data(
            prop::option::of(
                DecisionTemplateDataStrategyBuilder::default()
                .finish()
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
                    prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(RELATIVE_LAG_VELOCITY)));
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
