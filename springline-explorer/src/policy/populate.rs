use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dialoguer::{Confirm, Input};
use pretty_snowflake::{Id, Label, Labeling};
use proctor::elements::ToTelemetry;
use proctor::{AppData, MetaData};
use springline::flink::{AppDataWindow, MetricCatalog};
use springline::phases::eligibility::{ClusterStatus, EligibilityContext, JobStatus};
use springline::settings::{EligibilitySettings, Settings};

use crate::Result;

mod eligibility;
mod metric_catalog;
pub use eligibility::*;
pub use metric_catalog::*;
use springline::Env;

pub trait PopulateContext {
    type Settings;
    fn make(now: DateTime<Utc>, settings: &Settings) -> Result<Self>
    where
        Self: Sized;
}

impl<T> PopulateContext for Env<T>
where
    T: AppData + PopulateContext + Label,
{
    type Settings = <T as PopulateContext>::Settings;

    fn make(now: DateTime<Utc>, settings: &Settings) -> Result<Self>
    where
        Self: Sized,
    {
        let metadata = MetaData::from_parts(
            Id::direct(<T as Label>::labeler().label(), 0, "<undefined>"),
            now.into(),
        );
        Env::from_parts(metadata, <T as PopulateContext>::make(now, settings)).transpose()
    }
}

pub trait PopulateData {
    fn make(now: DateTime<Utc>, settings: &Settings) -> Result<Self>
    where
        Self: Sized;
}

impl<T> PopulateData for Env<T>
where
    T: AppData + PopulateData + Label,
{
    fn make(now: DateTime<Utc>, settings: &Settings) -> Result<Self>
    where
        Self: Sized,
    {
        let metadata = MetaData::from_parts(
            Id::direct(<T as Label>::labeler().label(), 0, "<undefined>"),
            now.into(),
        );
        Env::from_parts(metadata, <T as PopulateData>::make(now, settings)).transpose()
    }
}

impl<T> PopulateData for AppDataWindow<T>
where
    T: AppData + PopulateData + Label + springline::flink::WindowEntry,
{
    fn make(now: DateTime<Utc>, settings: &Settings) -> Result<Self>
    where
        Self: Sized,
    {
        <T as PopulateData>::make(now, settings)
            .map(|data| AppDataWindow::from_time_window(data, Duration::from_secs(600)))
    }
}
