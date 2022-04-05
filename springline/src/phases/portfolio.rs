use crate::model::{MetricCatalog, MetricPortfolio, Portfolio};
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use proctor::graph::stage::Stage;
use proctor::graph::{Inlet, Outlet, Port, SinkShape, SourceShape, PORT_DATA};
use proctor::{AppData, ProctorResult, SharedString};
use std::time::Duration;

#[derive(Debug)]
pub struct CollectMetricPortfolio<In, Out> {
    name: SharedString,
    time_window: Duration,
    inlet: Inlet<In>,
    outlet: Outlet<Out>,
}

impl CollectMetricPortfolio<MetricCatalog, MetricPortfolio> {
    pub fn new(name: impl Into<SharedString>, time_window: Duration) -> Self {
        let name = name.into();
        let inlet = Inlet::new(name.clone(), PORT_DATA);
        let outlet = Outlet::new(name.clone(), PORT_DATA);
        Self { name, time_window, inlet, outlet }
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

#[dyn_upcast]
#[async_trait]
impl<In, Out> Stage for CollectMetricPortfolio<In, Out>
where
    In: AppData,
    Out: Portfolio<Item = In>,
{
    #[inline]
    fn name(&self) -> SharedString {
        self.name.clone()
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

        while let Some(catalog) = self.inlet.recv().await {
            portfolio = portfolio + catalog;
            tracing::debug!(interval=?portfolio.window_interval(), "portfolio: {:?}", portfolio);
            tracing::debug!(
                ?portfolio,
                "pushing metric catalog portfolio looking back {:?}",
                portfolio.window_interval().map(|i| i.duration())
            );
            self.outlet.send(portfolio.clone()).await?;
        }

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        tracing::trace!(stage=%self.name(), "closing collect metric portfolio ports.");
        self.inlet.close().await;
        self.outlet.close().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ClusterMetrics, FlowMetrics, JobHealthMetrics};
    use claim::*;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::{Id, Label, Labeling};
    use proctor::elements::Timestamp;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_test::block_on;
    use tracing_futures::Instrument;

    fn make_test_catalog(ts: Timestamp, value: i32) -> MetricCatalog {
        MetricCatalog {
            correlation_id: Id::direct(
                <MetricCatalog as Label>::labeler().label(),
                ts.as_secs(),
                ts.as_f64().to_string(),
            ),
            recv_timestamp: ts,
            health: JobHealthMetrics::default(),
            flow: FlowMetrics {
                input_records_lag_max: Some(i64::from(value)),
                records_in_per_sec: value as f64,
                ..FlowMetrics::default()
            },
            cluster: ClusterMetrics {
                task_cpu_load: value as f64,
                ..ClusterMetrics::default()
            },
            custom: HashMap::new(),
        }
    }

    #[test]
    fn test_basic_portfolio_collection() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_basic_portfolio_collection");
        let _main_span_guard = main_span.enter();

        let now = Timestamp::now();
        let start = now - Duration::from_secs(10);
        tracing::info!("NOW = {now:?} == {now}");
        tracing::info!("START = {start:?} == {start}");

        let data: Vec<MetricCatalog> = (0..20)
            .map(|i: u32| {
                let value = if i < 10 { (i + 1) as i32 } else { (20 - i) as i32 };
                make_test_catalog(start + Duration::from_secs(i as u64), value)
            })
            .collect();

        let (tx_in, rx_in) = mpsc::channel(8);
        let (tx_out, mut rx_out) = mpsc::channel(8);

        let mut stage = CollectMetricPortfolio::new("test_collect_metric_portfolio", Duration::from_secs(1) * 3);

        block_on(async {
            let mut inlet = stage.inlet();
            stage.inlet.attach("test_source".into(), rx_in).await;
            stage.outlet.attach("test_sink".into(), tx_out).await;

            let stage_handle = tokio::spawn(async move { assert_ok!(stage.run().await) });

            let expected = [
                false, // 0s, point check but checked range is not a point in time.
                false, // 1s - coverage: 0.2 - not enough coverage
                false, // 2s - coverage: 0.4 - not enough coverage
                true,  // 3s - coverage: 0.6 [4,3,2,1
                true,  // 4s - coverage: 0.6 [5,4,3,2]
                true,  // 5s - coverage: 0.6 [6,5,4,3]
                true,  // 6s - coverage: 0.6 [7,6,5,4]
                true,  // 7s - coverage: 0.6 [8,7,6,5]
                false, // 8s - coverage: 0.6 [x9x,8,7,6]
                false, // 9s - coverage: 0.6 [x10x,x9x,8,7]
                false, // 10s - coverage: 0.6 [x10x,x10x,x9x,8]
                false, // 11s - coverage: 0.6 [x9x,x10x,x10x,x9x]
                false, // 12s - coverage: 0.6 [8,x9x,x10x,x10x]
                false, // 13s - coverage: 0.6 [7,8,x9x,x10x]
                false, // 14s - coverage: 0.6 [6,7,8,x9x]
                true,  // 15s - coverage: 0.6 [5,6,7,8]
                true,  // 16s - coverage: 0.6 [4,5,6,7]
                true,  // 17s - coverage: 0.6 [3,4,5,6]
                true,  // 18s - coverage: 0.6 [2,3,4,5]
                true,  // 19s - coverage: 0.6 [1,2,3,4]
            ];
            for i in 0..data.len() {
                async {
                    assert_ok!(tx_in.send(data[i].clone()).await);
                    let actual = assert_some!(rx_out.recv().await);
                    assert_eq!((i, actual.flow_input_records_lag_max_within(5, 8)), (i, expected[i]));
                }
                .instrument(tracing::info_span!("test item", INDEX=%i))
                .await;
            }

            tracing::info!("DMR: EEE");
            stage_handle.abort();
            tracing::info!("DMR: FFF");
            inlet.close().await;
            tracing::info!("DMR: GGG");
        })
    }
}
