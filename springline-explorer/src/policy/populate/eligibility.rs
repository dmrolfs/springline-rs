use super::*;
use crate::THEME;
use proctor::elements::telemetry;

impl PopulateContext for EligibilityContext {
    type Settings = EligibilitySettings;

    fn make(now: DateTime<Utc>, settings: &Settings) -> Result<Self>
    where
        Self: Sized,
    {
        let settings = &settings.eligibility;
        let all_sinks_healthy = Confirm::with_theme(&*THEME)
            .with_prompt("Are all sinks healthy?")
            .interact()?;

        let is_deploying = Confirm::with_theme(&*THEME)
            .with_prompt("Is the cluster actively deploying at policy evaluation?")
            .interact()?;

        let is_rescaling = Confirm::with_theme(&*THEME)
            .with_prompt("Is the cluster actively rescaling at policy evaluation?")
            .interact()?;

        let since_last_deployment = Input::with_theme(&*THEME)
            .with_prompt("How many seconds before policy evaluation was the last deployment?")
            .default(settings.template_data.clone().and_then(|td| td.cooling_secs).unwrap_or(0))
            .interact()?;
        let last_deployment = now - chrono::Duration::seconds(since_last_deployment as i64);

        let last_failure = if Confirm::with_theme(&*THEME)
            .with_prompt("Model a failure?")
            .interact()?
        {
            let since_failure = Input::with_theme(&*THEME)
                .with_prompt("How many seconds before policy evaluation did the failure occur?")
                .default(settings.template_data.clone().and_then(|td| td.stable_secs).unwrap_or(0))
                .interact()?;
            Some(now - chrono::Duration::seconds(since_failure as i64))
        } else {
            None
        };

        let mut custom: telemetry::TableType = HashMap::default();
        loop {
            let key: String = Input::with_theme(&*THEME)
                .with_prompt("Add custom property?")
                .default("".to_string())
                .allow_empty(true)
                .interact_text()?;

            if key.is_empty() {
                break;
            }

            let value: String = Input::with_theme(&*THEME)
                .with_prompt(format!("{} = ", key))
                .interact_text()?;

            custom.insert(key, value.to_telemetry());
        }

        Ok(EligibilityContext {
            all_sinks_healthy,
            job: JobStatus { last_failure },
            cluster: ClusterStatus { is_deploying, is_rescaling, last_deployment },
            custom,
        })
    }
}
