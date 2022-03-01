use crate::flink::FlinkContext;

#[derive(Debug, Clone)]
pub struct StopFlinkWithSavepoint {
    pub flink: FlinkContext,
    pub savepoint: String,
}

impl StopFlinkWithSavepoint {}
