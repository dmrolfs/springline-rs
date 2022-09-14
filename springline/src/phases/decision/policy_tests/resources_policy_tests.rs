use super::*;
use itertools::Itertools;

proptest! {
    #[test]
    fn doesnt_crash(scenario in PolicyScenario::strategy("decision_basis")) {
        match scenario.run() {
            Ok(_) => (),
            Err(PolicyError::Validation(_)) => (),
            result => prop_assert!(result.is_ok()),
        }
    }

    #[test]
    fn test_scale_up_relative_lag_velocity(
        scenario in PolicyScenario::builder()
        .template_data(prop::option::of(
            DecisionTemplateDataStrategyBuilder::default()
            // .just_max_healthy_relative_lag_velocity(Some(0.5))
            .finish()
        ))
        .items(
            arb_metric_catalog_window(
                Timestamp::now(),
                arb_range_duration(30..=600),
                arb_perturbed_duration(Duration::from_secs(15), 0.2),
                |recv_ts| {
                    MetricCatalogStrategyBuilder::new()
                        .just_recv_timestamp(recv_ts)
                        .flow(
                            FlowMetricsStrategyBuilder::new()
                                // .just_source_records_lag_max(Some(recv_ts.as_secs() as u32))
                                // .just_source_assigned_partitions(Some(2))
                                // .just_source_records_consumed_rate(Some(1.0))
                                .finish()
                        )
                        .finish()
                        .boxed()
                }
            )
        )
        .strategy()
    ) {
        let query_result = match scenario.run() {
            Ok(r) => Some(r),
            Err(PolicyError::Validation(err)) => {
                tracing::info!(error=?err, "query result validation error - ignoring");
                None
            },
            Err(err) => {
                tracing::error!(error=?err, "query result policy error");
                prop_assert!(false, "policy execution error: {:?}", err);
                None
            },
        };


        if let Some(result) = query_result {
            let eval_secs = scenario.template_data.as_ref().and_then(|td| td.evaluate_duration_secs).unwrap_or(60);

            match scenario.template_data.and_then(|template| template.max_healthy_relative_lag_velocity) {
                max_relative_lag if !result.passed => {
                    tracing::error!(
                        ?max_relative_lag,
                        actual=?scenario.item.flow_source_relative_lag_velocity(eval_secs),
                        %eval_secs,
                        total_lag=?scenario.item.flow.source_total_lag,
                        rec_lag_max=?scenario.item.flow.source_records_lag_max,
                        assigned_partitions=?scenario.item.flow.source_assigned_partitions,
                        "DMR - CCC - decision:no"
                    );
                    prop_assert!(result.bindings.is_empty());
                },
                None => {
                    tracing::error!("DMR - BBB - no template data");
                    prop_assert!(result.bindings.contains_key(REASON));
                    let reasons = assert_some!(result.bindings.get(REASON));
                    prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(RELATIVE_LAG_VELOCITY)));
                },
                Some(max_relative_lag) if max_relative_lag < scenario.item.flow_source_relative_lag_velocity(eval_secs) => {
                    tracing::error!(
                        ?max_relative_lag,
                        actual_rel_lag=?scenario.item.flow_source_relative_lag_velocity(eval_secs),
                        %eval_secs,
                        "DMR - AAA decision:yes:  unhealthy rel lag"
                    );
                    prop_assert!(result.passed);
                    prop_assert!(result.bindings.contains_key(REASON));
                    let reasons = assert_some!(result.bindings.get(REASON));
                    prop_assert!(reasons.into_iter().contains(&TelemetryValue::from(RELATIVE_LAG_VELOCITY)));
                },
                Some(_) if result.bindings.contains_key(REASON) => {
                    tracing::error!("DMR - DDD - decision:yes rel lag healthy");
                    let reasons = assert_some!(result.bindings.get(REASON));
                    prop_assert!(!reasons.into_iter().contains(&TelemetryValue::from(RELATIVE_LAG_VELOCITY)));
                },

                Some(_) => { tracing::error!("DMR - EEE unknown!!!"); }
            }
        }
    }
}
