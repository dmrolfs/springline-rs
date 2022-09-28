use once_cell::sync::Lazy;
use prometheus::core::{AtomicU64, GenericGauge};
use prometheus::Opts;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};
use std::cell::RefCell;
use std::time::{Duration, Instant};

#[serde_as]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClippingHandlingSettings {
    Ignore,
    TemporaryLimit {
        #[serde(rename = "reset_timeout_secs")]
        #[serde_as(as = "DurationSeconds<u64>")]
        reset_timeout: Duration,
    },
    PermanentLimit,
}

impl Default for ClippingHandlingSettings {
    fn default() -> Self {
        Self::Ignore
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TemporaryLimitCell {
    clipping_point: Option<u32>,
    triggered: Option<std::time::Instant>,
    reset_timeout: Duration,
}

impl TemporaryLimitCell {
    pub const fn new(reset_timeout: Duration) -> Self {
        Self {
            reset_timeout,
            clipping_point: None,
            triggered: None,
        }
    }

    pub fn clipping_point(&mut self) -> Option<u32> {
        self.triggered
            .map(|t| {
                if self.reset_timeout <= t.elapsed() {
                    tracing::info!("clipping reset - clearing clipping point");
                    self.clipping_point = None;
                    self.triggered = None;
                }

                self.clipping_point
            })
            .unwrap_or(self.clipping_point)
    }

    pub fn set_clipping_point(&mut self, clipping_point: u32) -> Option<u32> {
        let clipping_point = self
            .clipping_point
            .map(|cp| u32::min(cp, clipping_point))
            .unwrap_or(clipping_point);

        tracing::info!(
            %clipping_point, prior_clipping_point=?self.clipping_point,
            "possible source clipping identified - setting temporary clipping point to be reset in {:?}.",
            self.reset_timeout
        );

        self.clipping_point = Some(clipping_point);
        self.triggered = Some(Instant::now());
        PLANNING_PARALLELISM_CLIPPING_POINT.set(u64::from(clipping_point));
        tracing::warn!(%clipping_point, saved=?self, "DMR: COMPARE SAVED vs SET.");
        self.clipping_point
    }
}

impl PartialEq for TemporaryLimitCell {
    fn eq(&self, other: &Self) -> bool {
        self.reset_timeout == other.reset_timeout
    }
}

#[derive(Debug, Clone)]
pub enum ClippingHandling {
    Ignore,
    TemporaryLimit { cell: RefCell<TemporaryLimitCell> },
    PermanentLimit(Option<u32>),
}

impl ClippingHandling {
    pub const fn new(settings: &ClippingHandlingSettings) -> Self {
        match settings {
            ClippingHandlingSettings::Ignore => Self::Ignore,
            ClippingHandlingSettings::PermanentLimit => Self::PermanentLimit(None),
            ClippingHandlingSettings::TemporaryLimit { reset_timeout } => Self::TemporaryLimit {
                cell: RefCell::new(TemporaryLimitCell::new(*reset_timeout)),
            },
        }
    }

    pub fn clipping_point(&self) -> Option<u32> {
        match self {
            Self::Ignore => None,
            Self::PermanentLimit(clipping_point) => *clipping_point,
            Self::TemporaryLimit { cell } => cell.borrow_mut().clipping_point(),
        }
    }

    pub fn set_clipping_point(&mut self, clipping_point: u32) {
        match self {
            Self::Ignore => (),
            Self::PermanentLimit(pt) => {
                let new_pt = pt.map(|p| u32::min(p, clipping_point)).unwrap_or(clipping_point);
                tracing::info!(clipping_point=%new_pt, "possible source clipping identified - setting permanent clipping point.");
                *pt = Some(new_pt);
            },
            Self::TemporaryLimit { cell } => {
                cell.borrow_mut().set_clipping_point(clipping_point);
            },
        }
    }
}

impl PartialEq for ClippingHandling {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Ignore, Self::Ignore) => true,
            (Self::PermanentLimit(_), Self::PermanentLimit(_)) => true,
            (Self::TemporaryLimit { cell: lhs }, Self::TemporaryLimit { cell: rhs }) => lhs == rhs,
            _ => false,
        }
    }
}

pub static PLANNING_PARALLELISM_CLIPPING_POINT: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "planning_parallelism_clipping_point",
            "If set, the lowest parallelism point at which job performance 'clips' and stability is suspect",
        )
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
        .expect("failed creating planning_parallelism_clipping_point metric")
});
