use crate::flink::FlinkContext;
use http::Uri;

#[derive(Debug, Clone)]
pub struct StopFlinkWithSavepoint {
    pub flink: FlinkContext,
    pub savepoint_dir: Option<String>,
}

impl StopFlinkWithSavepoint {
}
