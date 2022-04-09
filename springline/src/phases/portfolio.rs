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
    use oso::Oso;
    use oso::PolarClass;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::{Id, Label, Labeling};
    use proctor::elements::{telemetry, PolicyOutcome, PolicySource, QueryPolicy, QueryResult, Timestamp};
    use proctor::elements::{PolicyContributor, PolicyFilterEvent};
    use proctor::error::{PolicyError, ProctorError};
    use proctor::graph::stage::WithMonitor;
    use proctor::graph::Connect;
    use proctor::phases::policy_phase::PolicyPhase;
    use proctor::phases::sense::SubscriptionRequirements;
    use proctor::{Correlation, ProctorContext};
    use serde::{Deserialize, Serialize};
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_test::block_on;
    use tracing_futures::Instrument;

    fn make_test_catalog(ts: Timestamp, value: i32) -> MetricCatalog {
        MetricCatalog {
            correlation_id: Id::direct(
                <MetricCatalog as Label>::labeler().label(),
                ts.as_secs(),
                ts.as_secs_f64().to_string(),
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
                true,  // 3s - coverage: 0.6 [4,3,2,1]
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
                    assert_eq!(
                        (i, actual.flow_input_records_lag_max_below_mark(5, 8)),
                        (i, expected[i])
                    );
                }
                .instrument(tracing::info_span!("test item", INDEX=%i))
                .await;
            }

            stage_handle.abort();
            inlet.close().await;
        })
    }

    #[derive(PolarClass, Label, Debug, Clone, Serialize, Deserialize)]
    struct TestContext {
        pub recv_timestamp: Timestamp,
        pub correlation_id: Id<Self>,
    }

    impl PartialEq for TestContext {
        fn eq(&self, _other: &Self) -> bool {
            true
        }
    }

    impl Correlation for TestContext {
        type Correlated = Self;

        fn correlation(&self) -> &Id<Self::Correlated> {
            &self.correlation_id
        }
    }

    #[async_trait]
    impl ProctorContext for TestContext {
        type Error = ProctorError;
        fn custom(&self) -> telemetry::TableType {
            telemetry::TableType::default()
        }
    }

    impl SubscriptionRequirements for TestContext {
        fn required_fields() -> HashSet<SharedString> {
            HashSet::new()
        }
    }

    #[derive(Debug)]
    struct TestPolicy {
        pub policies: Vec<PolicySource>,
    }

    impl QueryPolicy for TestPolicy {
        type Item = MetricPortfolio;
        type Context = TestContext;
        type Args = (Self::Item, Self::Context);
        type TemplateData = ();

        fn base_template_name() -> &'static str {
            "test_policy"
        }

        fn policy_template_data(&self) -> Option<&Self::TemplateData> {
            None
        }

        fn policy_template_data_mut(&mut self) -> Option<&mut Self::TemplateData> {
            None
        }

        fn sources(&self) -> &[PolicySource] {
            self.policies.as_slice()
        }

        fn sources_mut(&mut self) -> &mut Vec<PolicySource> {
            &mut self.policies
        }

        fn initialize_policy_engine(&self, engine: &mut Oso) -> Result<(), PolicyError> {
            MetricPortfolio::register_with_policy_engine(engine)
        }

        fn make_query_args(&self, item: &Self::Item, context: &Self::Context) -> Self::Args {
            (item.clone(), context.clone())
        }

        fn query_policy(&self, engine: &Oso, args: Self::Args) -> Result<QueryResult, PolicyError> {
            let q = assert_ok!(engine.query_rule("healthy", args));
            QueryResult::from_query(q)
        }
    }

    #[test]
    fn test_portfolio_in_policy() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_portfolio_in_policy");
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

        let (tx_ctx_in, rx_ctx_in) = mpsc::channel(8);
        let (tx_in, rx_in) = mpsc::channel(8);
        let (tx_out, mut rx_out) = mpsc::channel(8);

        let mut collect_stage =
            CollectMetricPortfolio::new("test_collect_metric_portfolio", Duration::from_secs(1) * 3);
        let policy = TestPolicy {
            policies: vec![assert_ok!(PolicySource::from_complete_string(
                "test_policy",
                r##"|healthy(item, c) if above_low_water(item, c) and below_high_water(item, c);
                    |below_high_water(item, _) if item.flow_input_records_lag_max_below_mark(5, 8);
                    |above_low_water(item, _) if item.flow_input_records_lag_max_above_mark(5, 3);
                    |"##
            ))],
        };

        block_on(async {
            let mut inlet = collect_stage.inlet();
            let mut policy_stage = assert_ok!(PolicyPhase::carry_policy_outcome("test_policy", policy).await);
            let mut rx_monitor = policy_stage.rx_monitor();
            let mut ctx_inlet = policy_stage.context_inlet();
            let mut outlet = policy_stage.outlet();

            collect_stage.inlet.attach("test_source".into(), rx_in).await;
            ctx_inlet.attach("test_context".into(), rx_ctx_in).await;
            (collect_stage.outlet.clone(), policy_stage.inlet()).connect().await;
            outlet.attach("test_sink".into(), tx_out).await;

            let collect_stage_handle = tokio::spawn(async move { assert_ok!(collect_stage.run().await) });
            let policy_stage_handle = tokio::spawn(async move { assert_ok!(policy_stage.run().await) });

            let expected = [
                None,    // [1x] - point check conditions not met
                None,    // [x2, x1] - not enough coverage
                None,    // [3, x2, 1] - not enough coverage
                None,    // [4, 3, x2, x1]
                None,    // [5, 4, 3, x2]
                Some(6), // [6, 5, 4, 3]
                Some(7), // [7, 6, 5, 4]
                Some(8), // [8, 7, 6, 5]
                None,    // [9x, 8, 7, 6]
                None,    // [10x, 9x, 8, 7]
                None,    // [10x, 10x, 9x, 8]
                None,    // [9x, 10x, 10x, 9x]
                None,    // [8, 9x, 10x, 10x]
                None,    // [7, 8, 9x, 10x]
                None,    // [6, 7, 8, 9x]
                Some(5), // [5, 6, 7, 8]
                Some(4), // [4, 5, 6, 7]
                Some(3), // [3, 4, 5, 6]
                None,    // [x2, 3, 4, 5]
                None,    // [x1, x2, 3, 4]
            ];

            assert_ok!(
                tx_ctx_in
                    .send(TestContext {
                        recv_timestamp: Timestamp::now(),
                        correlation_id: Id::direct("text_context", 123, "abc"),
                    })
                    .await
            );

            assert_matches!(
                &*assert_ok!(rx_monitor.recv().await),
                &PolicyFilterEvent::ContextChanged(_)
            );

            for i in 0..data.len() {
                async {
                    tracing::warn!("**** ITERATION {i} ****");
                    assert_ok!(tx_in.send(data[i].clone()).await);

                    if let Some(expected) = expected[i] {
                        assert_matches!(
                            &*assert_ok!(rx_monitor.recv().await),
                            &PolicyFilterEvent::ItemPassed(_, _)
                        );

                        let actual: PolicyOutcome<MetricPortfolio, TestContext> = assert_some!(rx_out.recv().await);

                        assert_eq!((i, assert_some!(actual.item.flow.input_records_lag_max)), (i, expected));
                    } else {
                        assert_matches!(
                            (i, &*assert_ok!(rx_monitor.recv().await)),
                            (i, &PolicyFilterEvent::ItemBlocked(_, _))
                        );
                    }
                }
                .instrument(tracing::info_span!("test item", INDEX=%i))
                .await;
            }

            collect_stage_handle.abort();
            assert_ok!(policy_stage_handle.await);
            inlet.close().await;
        })
    }
}
