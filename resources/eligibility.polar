{{#*inline "preamble"}}
# define eligibile rule in eligibility polar basis file.

deploying(_, context) if context.cluster.is_deploying;

{{#if cooling_secs}}
in_cooling_period(_, context) if context.cluster.last_deployment_within_seconds({{cooling_secs}});
{{else}}
in_cooling_period(_, _) if false;
{{/if}}

{{#if stable_secs}}
recent_failure(_, context) if context.job.last_failure_within_seconds({{stable_secs}});
{{else}}
recent_failure(_, _) if false;
{{/if}}

# Do not scale during multi region failure
# Are there job failures
# Do not scale while there are current operations; e.g., Cancel, Upgrade, MultiRegion Failover.
# license considerations; e.g., Do not autoscale freemium pipelines

{{/inline}}
{{>  (lookup this "basis")}}

