{{#*inline "preamble"}}
# define eligibile rule in eligibility polar basis file.

outside_cooling_period(_, context) if context.cluster.last_deployment_within_seconds({{cooling_secs}});

sufficiently_stable(_, context) if context.task.last_failure_within_seconds({{stable_secs}});

# Do not scale during multi region failure
# Are there task failures
# Do not scale while there are current operations; e.g., Cancel, Upgrade, MultiRegion Failover.
# license considerations; e.g., Do not autoscale freemium pipelines

{{/inline}}
{{>  (lookup this "basis")}}

