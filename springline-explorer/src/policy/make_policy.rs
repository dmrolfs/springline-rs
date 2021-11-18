use springline::phases::eligibility::EligibilityPolicy;
use springline::settings::Settings;

pub trait MakePolicy {
    fn make(settings: &Settings) -> Self;
}

impl MakePolicy for EligibilityPolicy {
    fn make(settings: &Settings) -> Self {
        EligibilityPolicy::new(&settings.eligibility)
    }
}
