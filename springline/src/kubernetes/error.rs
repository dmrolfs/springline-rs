use either::{Either, Left};
use proctor::error::MetricLabel;
use proctor::SharedString;
use thiserror::Error;

pub fn convert_kube_error(error: kube::Error) -> KubernetesError {
    match error {
        kube::Error::Api(response) => response.into(),
        err => err.into(),
    }
}

#[derive(Debug, Error)]
pub enum KubernetesError {
    #[error(transparent)]
    InferConfig(#[from] kube::config::InferConfigError),

    #[error(transparent)]
    InCluster(#[from] kube::config::InClusterError),

    #[error(transparent)]
    KubeConfig(#[from] kube::config::KubeconfigError),

    #[error("failure in kubernetes api:{0}")]
    KubeApi(#[from] kube::error::ErrorResponse),

    #[error("failed in kube client request:{0}")]
    Kube(#[from] kube::Error),

    #[error("could not make HTTP URI from kubernetes url: {0}")]
    Http(#[from] http::Error),

    #[error("timeout setting too big for kubernetes: {0}")]
    TimeoutTooBig(#[from] std::num::TryFromIntError),
}

const CONFIG_LABEL: &str = "config";

impl MetricLabel for KubernetesError {
    fn slug(&self) -> SharedString {
        SharedString::Borrowed("kubernetes")
    }

    fn next(&self) -> Either<SharedString, Box<&dyn MetricLabel>> {
        match self {
            Self::InferConfig(_) => Left(CONFIG_LABEL.into()),
            Self::InCluster(_) => Left(CONFIG_LABEL.into()),
            Self::KubeConfig(_) => Left(CONFIG_LABEL.into()),
            Self::KubeApi(_) => Left("api".into()),
            Self::Kube(_) => Left("client".into()),
            Self::Http(_) => Left("http".into()),
            Self::TimeoutTooBig(_) => Left(CONFIG_LABEL.into()),
        }
    }
}
