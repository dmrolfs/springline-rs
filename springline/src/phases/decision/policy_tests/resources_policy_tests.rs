use super::*;

proptest! {
    #[test]
    fn doesnt_crash(scenario in PolicyScenario::strategy("decision_basis")) {
        prop_assert!(scenario.run().is_ok())
    }
}
