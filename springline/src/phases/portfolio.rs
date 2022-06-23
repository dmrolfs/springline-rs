use std::fmt::{self, Debug};
use std::time::Duration;

use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use proctor::error::ProctorError;
use proctor::graph::stage::{Stage, WithApi};
use proctor::graph::{stage, Inlet, Outlet, Port, SinkShape, SourceShape, PORT_DATA};
use proctor::{Ack, AppData, ProctorResult, ReceivedAt};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::flink::{AppDataPortfolio, MetricCatalog, Portfolio};
use crate::settings::EngineSettings;

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
    sufficient_coverage: f64,
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
            .field("sufficient_coverage", &self.sufficient_coverage)
            .finish()
    }
}

impl CollectMetricPortfolio<MetricCatalog, AppDataPortfolio<MetricCatalog>> {
    pub fn new(name: impl Into<String>, settings: &EngineSettings) -> Self {
        let name = name.into();
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let inlet = Inlet::new(&name, PORT_DATA);
        let outlet = Outlet::new(&name, PORT_DATA);
        Self {
            name,
            time_window: settings.telemetry_portfolio_window,
            sufficient_coverage: settings.sufficient_window_coverage_percentage,
            inlet,
            outlet,
            tx_api,
            rx_api,
        }
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
    In: AppData + ReceivedAt,
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
        let portfolio: Mutex<Option<Out>> = Mutex::new(None);
        // let mut portfolio: Option<Out> = None;

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
                            let portfolio_ref = &mut *portfolio.lock().await;
                            match portfolio_ref.as_mut() {
                                Some(p) => p.push(catalog),
                                None => {
                                    let p = self.make_portfolio(catalog)?;
                                    *portfolio_ref = Some(p);
                                }
                            }

                            let out = portfolio_ref.as_ref().cloned().unwrap();

                            // match portfolio.as_mut() {
                            //     Some(p) => p.push(catalog),
                            //     None => {
                            //         let p = self.make_portfolio(catalog)?;
                            //         // let p = Out::from_item(catalog, self.time_window, self.sufficient_coverage).map_err?;
                            //         portfolio = Some(p);
                            //     }
                            // }
                            //
                            // let out = portfolio.unwrap().clone();

                            tracing::debug!(
                                portfolio=?out,
                                "pushing metric catalog portfolio looking back {:?}",
                                out.window_interval().map(|i| i.duration())
                            );

                            self.outlet.send(out).await?;
                        }
                    }
                },

                Some(command) = self.rx_api.recv() => {
                    match command {
                        PortfolioCmd::Clear(tx) => {
                            // portfolio = None;
                            let p = &mut *portfolio.lock().await;
                            // let window = p.time_window();
                            *p = None;
                            // p.set_time_window(window);
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

impl<In, Out> CollectMetricPortfolio<In, Out>
where
    In: AppData + ReceivedAt,
    Out: Portfolio<Item = In>,
{
    fn make_portfolio(&self, data: In) -> ProctorResult<Out> {
        Out::from_item(data, self.time_window, self.sufficient_coverage).map_err(|err| ProctorError::Phase(err.into()))
    }
}
