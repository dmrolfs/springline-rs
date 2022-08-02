use super::*;

proptest! {
    #[test]
    fn doesnt_crash(scenario in PolicyScenario::strategy("decision_basis")) {
        match scenario.run() {
            Ok(_) => (),
            Err(PolicyError::Validation(_)) => (),
            result => prop_assert!(result.is_ok()),
        }
    }
}
