use super::FlinkScope;
use super::{api_model, Aggregation, MetricOrder};
use crate::phases::collection::flink::TaskContext;
use async_trait::async_trait;
use cast_trait_object::dyn_upcast;
use futures_util::TryFutureExt;
use heck::ToSnakeCase;
use itertools::Itertools;
use proctor::error::{CollectionError, ProctorError};
use proctor::graph::stage::{self, Stage};
use proctor::graph::{Inlet, Outlet, Port, SinkShape, SourceShape};
use proctor::{AppData, ProctorResult, SharedString};
use reqwest::Method;
use std::sync::Arc;
use tracing::Instrument;
use url::Url;

/// Load telemetry for a specify scope from the Flink Job Manager REST API; e.g., Job or Taskmanager.
/// Note: cast_trait_object issues a conflicting impl error if no generic is specified (at least for
/// my use cases), so a simple Telemetry doesn't work and I need to parameterize even though
/// I'll only use wrt Telemetry.
#[derive(Debug)]
pub struct CollectScope<Out> {
    scope: FlinkScope,
    context: TaskContext,
    orders: Arc<Vec<MetricOrder>>,
    trigger: Inlet<()>,
    outlet: Outlet<Out>,
}

impl<Out> CollectScope<Out> {
    pub fn new(scope: FlinkScope, orders: Arc<Vec<MetricOrder>>, context: TaskContext) -> Self {
        let name: SharedString = format!("Collect{}", scope).to_snake_case().into();
        let trigger = Inlet::new(name.clone(), "trigger");
        let outlet = Outlet::new(name, "outlet");
        Self { scope, context, orders, trigger, outlet }
    }
}

impl<Out> SourceShape for CollectScope<Out> {
    type Out = Out;
    fn outlet(&self) -> Outlet<Self::Out> {
        self.outlet.clone()
    }
}

impl<Out> SinkShape for CollectScope<Out> {
    type In = ();
    fn inlet(&self) -> Inlet<Self::In> {
        self.trigger.clone()
    }
}

#[dyn_upcast]
#[async_trait]
impl<Out> Stage for CollectScope<Out>
where
    Out: AppData + serde::de::DeserializeOwned,
{
    fn name(&self) -> SharedString {
        self.scope.to_string().into()
    }

    #[tracing::instrument(Level = "info", skip(self))]
    async fn check(&self) -> ProctorResult<()> {
        self.do_check().await.map_err(|err| ProctorError::PhaseError(err.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = "run collect flink scope stage", skip(self))]
    async fn run(&mut self) -> ProctorResult<()> {
        self.do_run().await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> ProctorResult<()> {
        self.do_close().await?;
        Ok(())
    }
}

impl<Out> CollectScope<Out>
where
    Out: AppData + serde::de::DeserializeOwned,
{
    async fn do_check(&self) -> Result<(), CollectionError> {
        self.trigger.check_attachment().await?;
        self.outlet.check_attachment().await?;
        Ok(())
    }

    fn extend_url_for<'a>(
        &self, metrics: impl Iterator<Item = &'a String>, agg: impl Iterator<Item = &'a Aggregation>,
    ) -> Url {
        let scope_rep = self.scope.to_string().to_lowercase();
        let mut url = self.context.base_url.clone();
        url.path_segments_mut().unwrap().push(&scope_rep).push("metrics");
        url.query_pairs_mut()
            .clear()
            .append_pair("get", metrics.cloned().join(",").as_str())
            .append_pair("agg", agg.copied().join(",").as_str());

        url
    }

    async fn do_run(&mut self) -> Result<(), CollectionError> {
        let scopes = maplit::hashset! { self.scope };
        let (metric_orders, agg_span) = super::distill_metric_orders_and_agg(&scopes, &self.orders);
        if metric_orders.is_empty() {
            //todo: best to end this useless stage or do nothing in loop? I hope end is best.
            tracing::warn!(
                stage=%self.name(), scope=%self.scope,
                "no flink metric orders to collect for scope - stopping scope collection stage."
            );
            return Ok(());
        }

        let scope_rep = SharedString::Owned(self.scope.to_string().to_lowercase());
        let metrics = metric_orders.keys();
        let url = self.extend_url_for(metrics, agg_span.iter());

        let outlet = self.outlet.clone();
        let client = &self.context.client.clone();

        while self.trigger.recv().await.is_some() {
            let _timer = stage::start_stage_eval_time(self.name().as_ref());

            let url = url.clone();
            let metric_orders = &metric_orders;
            let scope_rep = scope_rep.clone();
            let scope = self.scope;

            let span = tracing::info_span!("collect Flink scope telemetry", scope=%scope);
            let collection_and_send = outlet
                .reserve_send::<_, CollectionError>(async move {
                    let response: Result<api_model::FlinkMetricResponse, CollectionError> = client
                        .request(Method::GET, url)
                        .send()
                        .and_then(|resp| {
                            super::log_response(format!("{} scope response", scope_rep.clone()).as_str(), &resp);
                            resp.json::<api_model::FlinkMetricResponse>().map_err(|err| err.into())
                        })
                        .map_err(|err| err.into())
                        .instrument(tracing::info_span!("Flink REST API", scope=%scope))
                        .await;

                    let response = response.and_then(|resp| {
                        api_model::build_telemetry(resp, metric_orders)
                            // this is only needed because async_trait forcing me to parameterize this stage
                            .and_then(|telemetry| telemetry.try_into())
                            .map_err(|err| err.into())
                    });

                    super::identity_and_track_errors(scope, response)
                })
                .instrument(span)
                .await;

            let _ = super::identity_and_track_errors(scope, collection_and_send);
        }

        Ok(())
    }

    async fn do_close(mut self: Box<Self>) -> Result<(), CollectionError> {
        self.trigger.close().await;
        self.outlet.close().await;
        Ok(())
    }
}
