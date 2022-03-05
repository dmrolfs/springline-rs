use crate::flink::FlinkContext;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RestartFlink<P> {
    pub flink: FlinkContext,
    pub restart_timeout: Duration,
    pub polling_interval: Duration,
    marker: std::marker::PhantomData<P>,
}