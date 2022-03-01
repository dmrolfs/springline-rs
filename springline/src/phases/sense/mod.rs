use std::collections::HashMap;

use crate::flink::FlinkContext;
use pretty_snowflake::MachineNode;
use proctor::elements::Telemetry;
use proctor::graph::stage::tick::{Tick, TickApi};
use proctor::graph::stage::{SourceStage, WithApi};
use proctor::phases::sense::builder::SenseBuilder;
use proctor::phases::sense::{Sense, SensorSetting, TelemetrySensor};

use crate::phases::MetricCatalog;
use crate::settings::SensorSettings;
use crate::Result;

pub mod flink;

#[tracing::instrument(level = "info", skip(name, settings, auxiliary_sensors))]
pub async fn make_sense_phase(
    name: &str, context: FlinkContext, settings: &SensorSettings,
    auxiliary_sensors: Vec<Box<dyn SourceStage<Telemetry>>>, machine_node: MachineNode,
) -> Result<(SenseBuilder<MetricCatalog>, TickApi)> {
    let mut sensors = do_make_modular_sensors(&settings.sensors, auxiliary_sensors).await?;

    let scheduler = Tick::new(
        "springline_flink",
        settings.flink.metrics_initial_delay,
        settings.flink.metrics_interval,
        (),
    );
    let tx_scheduler_api = scheduler.tx_api();

    let flink_sensor = flink::make_sensor("springline", context, Box::new(scheduler), &settings.flink).await?;
    sensors.push(flink_sensor);

    Ok((
        Sense::builder(name.to_string(), sensors, machine_node),
        tx_scheduler_api,
    ))
}

/// Makes `SourceStage`-based sensors specified in the sensor::sensors.
#[tracing::instrument(level = "info", skip())]
async fn do_make_modular_sensors(
    settings: &HashMap<String, SensorSetting>, auxiliary: Vec<Box<dyn SourceStage<Telemetry>>>,
) -> Result<Vec<Box<dyn SourceStage<Telemetry>>>> {
    let mut sensors = TelemetrySensor::from_settings::<MetricCatalog>(settings)
        .await?
        .into_iter()
        .flat_map(|mut s| s.stage.take())
        .collect::<Vec<_>>();

    auxiliary.into_iter().for_each(|s| sensors.push(s));

    Ok(sensors)
}
