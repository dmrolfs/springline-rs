use std::sync::Arc;
use proctor::{AppData, Correlation};
use proctor::elements::{PolicyFilterEvent, PolicyOutcome};
use proctor::graph::{Inlet, Outlet};
use super::GovernanceContext;
use tokio::sync::broadcast;

#[derive(Debug)]
pub struct Governance<In, Out> {
    name: String,
    context_inlet: Inlet<GovernanceContext>,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
    tx_monitor: broadcast::Sender<Arc<PolicyFilterEvent<In, GovernanceContext>>>,
}

impl<In> Governance<In, PolicyOutcome<In, GovernanceContext>>