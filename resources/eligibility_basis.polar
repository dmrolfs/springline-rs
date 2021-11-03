{{> preamble}}

eligible(item, context) if
    not deploying(item, context) and
    not in_cooling_period(item, context) and
    not recent_failure(item, context);
