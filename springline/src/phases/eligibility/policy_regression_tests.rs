use super::policy_tests::*;
use super::*;
use chrono::{TimeZone, Utc};
use claim::*;
use proctor::elements::QueryResult;

#[test]
fn test_eligibility_policy_doesnt_crash_5691360e9c41174fbc7e() {
    once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
    let main_span = tracing::info_span!("test_eligibility_policy_doesnt_crash_5691360e9c41174fbc7e");
    let _main_span_guard = main_span.enter();

    // cc a40e874bda14826f7d93c6c08ff7c49b53d24ce587b55691360e9c41174fbc7e # shrinks to
    // template_data = None, nr_active_jobs = 0, is_deploying = false, is_rescaling = false,
    // last_deployment = 0000-01-01T00:00:00Z

    assert_ok!(resources_policy::run_policy_scenario(&PolicyScenario {
        template_data: Some(EligibilityTemplateData {
            cooling_secs: Some(300),
            stable_secs: Some(900),
            ..EligibilityTemplateData::default()
        }),
        nr_active_jobs: 0,
        is_deploying: false,
        is_rescaling: false,
        last_deployment: Utc.ymd(0, 1, 1).and_hms_nano(0, 0, 0, 0),
        last_failure: None,
    }));
}

#[test]
fn test_eligibility_policy_nr_active_jobs_ae1d6126() {
    let actual = assert_ok!(resources_policy::run_policy_scenario(&PolicyScenario {
        template_data: None,
        nr_active_jobs: 0,
        is_deploying: false,
        is_rescaling: false,
        last_deployment: Utc.ymd(0, 1, 1).and_hms_nano(0, 0, 0, 0),
        last_failure: None
    }));

    assert_eq!(
        actual,
        QueryResult {
            passed: false,
            bindings: maplit::hashmap! { REASON.to_string() => vec![NO_ACTIVE_JOBS.into()] },
        }
    )
}

#[ignore]
#[test]
fn test_eligibility_datetime() {
    let dt = Utc.ymd(21199, 11, 31);
    // let dt = Utc.ymd(9981,2,29).and_hms_nano(16,34,58,931874908);
    // let dt = Utc.ymd(9981,2,29).and_hms_nano(16,34,58,931874908);
    assert_eq!(dt.to_string(), "");
}
