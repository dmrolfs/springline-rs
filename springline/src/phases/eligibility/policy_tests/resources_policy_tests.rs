use super::*;
use itertools::Itertools;

proptest! {
    #[test]
    fn doesnt_crash(scenario in PolicyScenario::strategy()) {
        prop_assert!(scenario.run().is_ok())
    }

    #[test]
    fn test_is_rescaling(scenario in PolicyScenario::strategy()) {
        let result = assert_ok!(scenario.run());
        if scenario.is_rescaling {
            prop_assert_eq!(
                result,
                QueryResult { passed: false, bindings: maplit::hashmap! { REASON.to_string() => vec![RESCALING.into()] } }
            );
        }
    }

    #[test]
    fn test_no_active_jobs(scenario in (PolicyScenario::builder().just_is_rescaling(false).just_nr_active_jobs(0_u32).strategy())) {
        let result = assert_ok!(scenario.run());

        if scenario.nr_active_jobs == 0 {
            prop_assert!(result.passed == false);
            prop_assert!(result.bindings.contains_key(REASON));
            let reasons = assert_some!(result.bindings.get(REASON));
            prop_assert!(reasons.into_iter().contains(&TelemetryValue::from(NO_ACTIVE_JOBS)));
        } else if result.passed {
            prop_assert!(result.bindings.is_empty());
        } else {
            prop_assert!(result.bindings.contains_key(REASON));
            let reasons = assert_some!(result.bindings.get(REASON));
            prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(NO_ACTIVE_JOBS)));
        }
    }

    #[test]
    fn test_is_deploying(scenario in (PolicyScenario::builder().just_is_rescaling(false).strategy())) {
        let result = assert_ok!(scenario.run());

        if scenario.is_deploying {
            prop_assert!(result.passed == false);
            prop_assert!(result.bindings.contains_key(REASON));
            let reasons = assert_some!(result.bindings.get(REASON));
            prop_assert!(reasons.into_iter().contains(&TelemetryValue::from(DEPLOYING)));
        } else if result.passed {
            prop_assert!(result.bindings.is_empty());
        } else {
            prop_assert!(result.bindings.contains_key(REASON));
            let reasons = assert_some!(result.bindings.get(REASON));
            prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(DEPLOYING)));
        }
    }

    #[test]
    fn test_cooling_period(scenario in (PolicyScenario::builder().just_is_rescaling(false).strategy())) {
        let result = assert_ok!(scenario.run());

        let cooling_boundary = scenario.template_data
            .and_then(|data| data.cooling_secs)
            .map(|cooling_secs| Utc::now() - chrono::Duration::seconds(i64::from(cooling_secs)));

        match cooling_boundary {
            Some(boundary) if boundary < scenario.last_deployment => {
                prop_assert!(result.passed == false);
                prop_assert!(result.bindings.contains_key(REASON));
                let reasons = assert_some!(result.bindings.get(REASON));
                prop_assert!(reasons.into_iter().contains(&TelemetryValue::from(COOLING_PERIOD)));
            },
            _ if result.passed => prop_assert!(result.bindings.is_empty()),
            _ => {
                prop_assert!(result.bindings.contains_key(REASON));
                let reasons = assert_some!(result.bindings.get(REASON));
                prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(COOLING_PERIOD)));
            },
        }
    }

    #[test]
    fn test_stable_period(scenario in (PolicyScenario::builder().just_is_rescaling(false).strategy())) {
        let result = assert_ok!(scenario.run());

        let stability_boundary = scenario.template_data
            .and_then(|data| data.stable_secs)
            .map(|stable_secs| Utc::now() - chrono::Duration::seconds(i64::from(stable_secs)));

        match stability_boundary.zip(scenario.last_failure) {
            Some((boundary, last_failure)) if boundary < last_failure => {
                prop_assert!(result.passed == false);
                prop_assert!(result.bindings.contains_key(REASON));
                let reasons = assert_some!(result.bindings.get(REASON));
                prop_assert!(reasons.into_iter().contains(&TelemetryValue::from(RECENT_FAILURE)));
            },
            _ if result.passed => prop_assert!(result.bindings.is_empty()),
            _ => {
                prop_assert!(result.bindings.contains_key(REASON));
                let reasons = assert_some!(result.bindings.get(REASON));
                prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(RECENT_FAILURE)));
            },
        }
    }
}
