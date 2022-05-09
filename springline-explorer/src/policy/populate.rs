use std::collections::HashMap;

use chrono::{DateTime, Utc};
use dialoguer::{Confirm, Input};
use proctor::elements::ToTelemetry;
use springline::flink::MetricCatalog;
use springline::phases::eligibility::{ClusterStatus, EligibilityContext, TaskStatus};
use springline::settings::{EligibilitySettings, Settings};

use crate::Result;

mod eligibility;
mod metric_catalog;
pub use eligibility::*;
pub use metric_catalog::*;

pub trait PopulateContext {
    type Settings;
    fn make(now: DateTime<Utc>, settings: &Settings) -> Result<Self>
    where
        Self: Sized;
}

pub trait PopulateData {
    fn make(now: DateTime<Utc>, settings: &Settings) -> Result<Self>
    where
        Self: Sized;
}
