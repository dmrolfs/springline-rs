use std::collections::{HashMap, HashSet};

use pretty_snowflake::MachineNode;
use proctor::elements::Telemetry;
use proctor::graph::stage::tick::{Tick, TickApi};
use proctor::graph::stage::{SourceStage, WithApi};
use proctor::phases::sense::builder::SenseBuilder;
use proctor::phases::sense::{Sense, SensorSetting, TelemetrySensor};

use crate::flink::{FlinkContext, MetricCatalog};
use crate::phases::sense::flink::FlinkSensorSpecification;
use crate::settings::SensorSettings;
use crate::Result;

pub mod flink;

#[tracing::instrument(level = "trace", skip(name, settings, auxiliary_sensors))]
pub async fn make_sense_phase(
    name: &str, context: FlinkContext, settings: &SensorSettings,
    auxiliary_sensors: Vec<Box<dyn SourceStage<Telemetry>>>, machine_node: MachineNode,
) -> Result<(SenseBuilder<MetricCatalog>, TickApi)> {
    do_set_metric_catalog_supplemental_telemetry(settings);

    let mut sensors = do_make_modular_sensors(&settings.sensors, auxiliary_sensors).await?;

    let scheduler = Tick::new(
        "springline_flink",
        settings.flink.metrics_initial_delay,
        settings.flink.metrics_interval,
        (),
    );
    let tx_scheduler_api = scheduler.tx_api();

    let flink_sensor = flink::make_sensor(FlinkSensorSpecification {
        name: "springline",
        context,
        scheduler: Box::new(scheduler),
        settings: &settings.flink,
        machine_node,
    })
    .await?;
    sensors.push(flink_sensor);

    Ok((
        Sense::builder(name, sensors, &settings.clearinghouse, machine_node),
        tx_scheduler_api,
    ))
}

#[tracing::instrument(level = "trace", skip(settings))]
fn do_set_metric_catalog_supplemental_telemetry(settings: &SensorSettings) {
    let assigned_telemetry: HashSet<&str> =
        flink::STD_METRIC_ORDERS.iter().map(|o| o.telemetry().1).collect();
    let ordered_telemetry: HashSet<&str> =
        settings.flink.metric_orders.iter().map(|o| o.telemetry().1).collect();
    let unassigned_telemetry: HashSet<String> = ordered_telemetry
        .difference(&assigned_telemetry)
        .map(|t| t.to_string())
        .collect();

    match crate::flink::SUPPLEMENTAL_TELEMETRY.set(unassigned_telemetry) {
        Ok(()) => {
            tracing::info!(
                supplemental_telemetry=?crate::flink::SUPPLEMENTAL_TELEMETRY,
                "adding supplemental metrics to the MetricCatalog's subscription. When (optionally) populated, they can be found in the MetricCatalog.custom field."
            )
        },
        Err(unassigned_telemetry) => {
            tracing::error!(
                ?unassigned_telemetry,
                "Could not set unassigned metric orders into MetricCatalog's supplemental telemetry subscription."
            );
        },
    };
}

/// Makes `SourceStage`-based sensors specified in the sensor::sensors.
#[tracing::instrument(level = "trace", skip())]
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
