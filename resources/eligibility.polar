eligible(item, context, reason) if not ineligible(item, context, root_reason) and reason = root_reason;

ineligible(item, _context, reason) if item.cluster.nr_active_jobs == 0 and reason = "no_active_jobs";

ineligible(_item, context, reason) if context.cluster.is_rescaling and reason = "rescaling";

ineligible(_item, context, reason) if context.cluster.is_deploying and reason = "deploying";

ineligible(item, context, reason) if in_cooling_period(item, context) and reason = "cooling_period";
{{#if cooling_secs}}
in_cooling_period(_, context) if context.cluster.last_deployment_within_seconds({{cooling_secs}});
{{else}}
in_cooling_period(_, _) if false;
{{/if}}

ineligible(item, context, reason) if recent_failure(item, context) and reason = "recent_failure";
{{#if stable_secs}}
recent_failure(_, context) if context.job.last_failure_within_seconds({{stable_secs}});
{{else}}
recent_failure(_, _) if false;
{{/if}}

# Do not scale during multi region failure
# Are there job failures
# Do not scale while there are current operations; e.g., Cancel, Upgrade, MultiRegion Failover.
# license considerations; e.g., Do not autoscale freemium pipelines

{{>  (lookup this "policy_extension")}}

