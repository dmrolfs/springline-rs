use std::fmt::{self, Debug};
use std::time::Duration;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use proctor::graph::stage::{Stage, WithApi};
use proctor::graph::{stage, Inlet, Outlet, Port, SinkShape, SourceShape, PORT_DATA};
use proctor::{Ack, AppData, ProctorResult};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::flink::{MetricCatalog, MetricPortfolio, Portfolio};

pub type PortfolioApi = mpsc::UnboundedSender<PortfolioCmd>;

#[derive(Debug)]
pub enum PortfolioCmd {
    Clear(oneshot::Sender<Ack>),
    Stop(oneshot::Sender<Ack>),
}

impl PortfolioCmd {
    pub async fn clear(api: &PortfolioApi) -> anyhow::Result<Ack> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Clear(tx))?;
        rx.await.map_err(|err| err.into())
    }

    pub async fn stop(api: &PortfolioApi) -> anyhow::Result<Ack> {
        let (tx, rx) = oneshot::channel();
        api.send(Self::Stop(tx))?;
        rx.await.map_err(|err| err.into())
    }
}

pub struct CollectMetricPortfolio<In, Out> {
    name: String,
    time_window: Duration,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
    tx_api: PortfolioApi,
    rx_api: mpsc::UnboundedReceiver<PortfolioCmd>,
}

impl<In, Out> Debug for CollectMetricPortfolio<In, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectMetricPortfolio")
            .field("name", &self.name)
            .field("time_window", &self.time_window)
            .finish()
    }
}

impl CollectMetricPortfolio<MetricCatalog, MetricPortfolio> {
    pub fn new(name: impl Into<String>, time_window: Duration) -> Self {
        let name = name.into();
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let inlet = Inlet::new(&name, PORT_DATA);
        let outlet = Outlet::new(&name, PORT_DATA);
        Self { name, time_window, inlet, outlet, tx_api, rx_api }
    }
}

impl<In, Out> SinkShape for CollectMetricPortfolio<In, Out> {
    type In = In;

    fn inlet(&self) -> Inlet<Self::In> {
        self.inlet.clone()
    }
}

impl<In, Out> SourceShape for CollectMetricPortfolio<In, Out> {
    type Out = Out;

    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<In, Out> WithApi for CollectMetricPortfolio<In, Out> {
    type Sender = PortfolioApi;

    fn tx_api(&self) -> Self::Sender {
        self.tx_api.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<In, Out> Stage for CollectMetricPortfolio<In, Out>
where
    In: AppData,
    Out: Portfolio<Item = In>,
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

    #[tracing::instrument(level = "trace", name = "run collect metric portfolio", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        let mut portfolio = Out::empty();
        portfolio.set_time_window(self.time_window);
        let portfolio = Mutex::new(portfolio);

        loop {
            let _timer = stage::start_stage_eval_time(self.name());

            tokio::select! {
                catalog = self.inlet.recv() => {
                    match catalog {
                        None => {
                            tracing::info!("collect portfolio inlet depleted -- stopping stage.");
                            break;
                        },

                        Some(catalog) => {
                            let p = &mut *portfolio.lock().await;
                            // let p: &mut Out = &mut *portfolio;
                            p.push(catalog);

                            tracing::debug!(
                                portfolio=?p,
                                "pushing metric catalog portfolio looking back {:?}",
                                p.window_interval().map(|i| i.duration())
                            );
                            self.outlet.send(p.clone()).await?;
                        }
                    }
                },

                Some(command) = self.rx_api.recv() => {
                    match command {
                        PortfolioCmd::Clear(tx) => {
                            let p = &mut *portfolio.lock().await;
                            let window = p.time_window();
                            *p = Out::empty();
                            p.set_time_window(window);
                            if tx.send(()) == Err(()) {
                                tracing::warn!("failed to ack clear portfolio command");
                            }
                            tracing::info!("metric portfolio cleared, which resets policy rules over time.");
                        },

                        PortfolioCmd::Stop(tx) => {
                            tracing::info!("metric portfolio stopped, which stops policy rules over time.");
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
        tracing::info!(stage=%self.name(), "closing collect metric portfolio ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}
