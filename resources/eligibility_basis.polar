{{> preamble}}

eligible(item, context) if
    outside_cooling_period(item, context) and
    sufficiently_stable(item, context);
