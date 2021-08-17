pub mod context;
pub mod policy;

// pub mod policy {
// use crate::elements::{Policy, PolicySettings};
// use crate::flink::eligibility::context::{ClusterStatus, FlinkEligibilityContext, TaskStatus};
// use crate::flink::MetricCatalog;
// use oso::{Oso, PolarClass};
//
// pub fn make_eligibility_policy(
//     settings: &impl PolicySettings,
// ) -> impl Policy<MetricCatalog, FlinkEligibilityContext> {
//     let init = |engine: &mut Oso| {
//         engine.register_class(FlinkEligibilityContext::get_polar_class())?;
//         engine.register_class(
//             TaskStatus::get_polar_class_builder()
//                 .name("TaskStatus")
//                 .add_method("last_failure_within_seconds",
// TaskStatus::last_failure_within_seconds)                 .build(),
//         )?;
//         engine.register_class(
//             ClusterStatus::get_polar_class_builder()
//                 .name("ClusterStatus")
//                 .add_method(
//                     "last_deployment_within_seconds",
//                     ClusterStatus::last_deployment_within_seconds,
//                 )
//                 .build(),
//         )?;
//
//         Ok(())
//     };
//
//     make_item_context_policy("eligible", settings, init)
// }
// }
