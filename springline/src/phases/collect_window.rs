use std::fmt::{self, Debug};
use std::time::Duration;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use proctor::error::ProctorError;
use proctor::graph::stage::{Stage, WithApi};
use proctor::graph::{stage, Inlet, Outlet, Port, SinkShape, SourceShape, PORT_DATA};
use proctor::{Ack, AppData, ProctorResult, ReceivedAt};
use tokio::sync::{mpsc, oneshot};

use crate::flink::{AppDataWindow, MetricCatalog, UpdateWindowMetrics, Window};
use crate::settings::EngineSettings;

pub type WindowApi = mpsc::UnboundedSender<WindowCmd>;

#[derive(Debug)]
pub enum WindowCmd {
    Clear(oneshot::Sender<Ack>),
    Stop(oneshot::Sender<Ack>),
}

impl WindowCmd {
    pub async fn clear(api: &WindowApi) -> anyhow::Result<Ack> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Clear(tx))?;
        rx.await.map_err(|err| err.into())
    }

    pub async fn stop(api: &WindowApi) -> anyhow::Result<Ack> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Stop(tx))?;
        rx.await.map_err(|err| err.into())
    }
}

pub struct CollectMetricWindow<In, Out> {
    name: String,
    time_window: Duration,
    quorum_percentile: f64,
    evaluation_duration: Option<Duration>,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
    tx_api: WindowApi,
    rx_api: mpsc::UnboundedReceiver<WindowCmd>,
}

impl<In, Out> Debug for CollectMetricWindow<In, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectMetricWindow")
            .field("name", &self.name)
            .field("time_window", &self.time_window)
            .field("quorum_percentage", &self.quorum_percentile)
            .field("evaluate_duration", &self.evaluation_duration)
            .finish()
    }
}

impl CollectMetricWindow<MetricCatalog, AppDataWindow<MetricCatalog>> {
    pub fn new(
        name: impl Into<String>, evaluation_duration: Option<Duration>, settings: &EngineSettings,
    ) -> Self {
        let name = name.into();

        let time_window = settings.telemetry_window;
        let quorum_percentage = settings.telemetry_window_quorum_percentile;

        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let inlet = Inlet::new(&name, PORT_DATA);
        let outlet = Outlet::new(&name, PORT_DATA);

        Self {
            name,
            time_window,
            quorum_percentile: quorum_percentage,
            evaluation_duration,
            inlet,
            outlet,
            tx_api,
            rx_api,
        }
    }
}

impl<In, Out> SinkShape for CollectMetricWindow<In, Out> {
    type In = In;

    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<In, Out> SourceShape for CollectMetricWindow<In, Out> {
    type Out = Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<In, Out> WithApi for CollectMetricWindow<In, Out> {
    type Sender = WindowApi;

    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In, Out> Stage for CollectMetricWindow<In, Out>
where
    In: AppData + ReceivedAt,
    Out: AppData + Window<Item = In> + UpdateWindowMetrics,
{
    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.inlet.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", name = "run collect metric window", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        let mut window: Option<Out> = None;

        loop {
            let _timer = stage::start_stage_eval_time(self.name());

            tokio::select! {
                item = self.inlet.recv() => {
                    match item {
                        None => {
                            tracing::info!("collect window inlet depleted -- stopping stage.");
                            break;
                        },

                        Some(item) => {
                            match window.as_mut() {
                                Some(w) => w.push(item),
                                None => {
                                    let w = self.make_window(item)?;
                                    window = Some(w);
                                }
                            }

                            let out = window.as_ref().cloned().unwrap();
                            out.update_metrics(self.evaluation_duration);

                            tracing::debug!(
                                "pushing metric catalog window looking back {:?}",
                                out.window_interval().map(|i| i.duration())
                            );

                            // let looking_back = out.window_interval().map(|i| i.duration());
                            // out.update_metrics(looking_back);
                            // tracing::debug!("pushing metric catalog window looking back {looking_back:?}");

                            self.outlet.send(out).await?;
                        }
                    }
                },

                Some(command) = self.rx_api.recv() => {
                    match command {
                        WindowCmd::Clear(tx) => {
                            window = None;
                            if tx.send(()) == Err(()) {
                                tracing::warn!("failed to ack clear window command");
                            }
                            tracing::info!("metric window cleared, which resets policy rules over time.");
                        },

                        WindowCmd::Stop(tx) => {
                            tracing::info!("metric window stopped, which stops policy rules over time.");
                            if tx.send(()) == Err(()) {
                                tracing::warn!("failed to ack to policy collection stage");
                            }
                            break;
                        },
                    }
                },

                else => break,
            }
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::info!(stage=%self.name(), "closing collect metric window ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

impl<In, Out> CollectMetricWindow<In, Out>
where
    In: AppData + ReceivedAt,
    Out: Window<Item = In>,
{
    fn make_window(&self, data: In) -> ProctorResult<Out> {
        Out::from_item(data, self.time_window, self.quorum_percentile)
            .map_err(|err| ProctorError::Phase(err.into()))
    }
}
