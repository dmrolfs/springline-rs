use once_cell::sync::Lazy;
use prometheus::core::{AtomicU64, GenericGauge};
use prometheus::Opts;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant, SystemTime};

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

#[derive(Debug, Copy, Clone, Eq)]
pub enum ClippingState {
    Clipped(Instant),
    HalfClipped,
    Clear,
}

impl PartialEq for ClippingState {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Clear, Self::Clear)
                | (Self::HalfClipped, Self::HalfClipped)
                | (Self::Clipped(_), Self::Clipped(_))
        )
    }
}

impl fmt::Display for ClippingState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rep = match self {
            Self::Clipped(_) => "Clipped",
            Self::HalfClipped => "HalfClipped",
            Self::Clear => "Clear",
        };
        f.write_str(rep)
    }
}

const CLIPPING_STATE_CLEAR: u64 = 0;
const CLIPPING_STATE_HALF_CLIPPED: u64 = 1;
const CLIPPING_STATE_CLIPPED: u64 = 2;

impl ClippingState {
    pub const fn gauge_rep(&self) -> u64 {
        match self {
            Self::Clear => CLIPPING_STATE_CLEAR,
            Self::HalfClipped => CLIPPING_STATE_HALF_CLIPPED,
            Self::Clipped(_) => CLIPPING_STATE_CLIPPED,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TemporaryLimitCell {
    clipping_history: VecDeque<u32>,
    state: ClippingState,
    reset_timeout: Duration,
}

impl TemporaryLimitCell {
    pub fn new(reset_timeout: Duration) -> Self {
        Self {
            reset_timeout,
            clipping_history: VecDeque::new(),
            state: ClippingState::Clear,
        }
    }

    pub const fn state_gauge(&self) -> u64 {
        self.state.gauge_rep()
    }

    pub fn clipping_point(&self) -> Option<u32> {
        use ClippingState as S;

        match self.state {
            S::Clear | S::HalfClipped => None,
            S::Clipped(_) => self.clipping_history.back().copied(),
        }
    }

    #[allow(clippy::cognitive_complexity)]
    pub fn note_clipping(&mut self, clipping_point: Option<u32>) {
        use ClippingState as S;

        match clipping_point {
            Some(observed) => {
                let prior_cp = self.clipping_history.back().copied();
                let effective_cp = match prior_cp {
                    Some(p) if p <= observed => p,
                    _ => {
                        tracing::info!(
                            ?prior_cp,
                            "{}: pushing new, smalled clipping point: {observed}",
                            self.state
                        );
                        self.clipping_history.push_back(observed);
                        observed
                    },
                };

                tracing::info!(
                    %observed,
                    "clipping state: {} -> Clipped[{effective_cp}] in effect until {:?}", self.state,
                    self.expiry_from_now()
                );

                self.state = S::Clipped(Instant::now());
            },
            None => match self.state {
                S::Clear => (),
                S::HalfClipped => {
                    let popped_cp = self.clipping_history.pop_back();
                    tracing::info!(
                        "clipping fully cleared: {}{} -> Clear.",
                        self.state,
                        popped_cp.map(|p| format!("[{p}]")).unwrap_or_else(String::new),
                    );
                    self.state = S::Clear;
                },
                S::Clipped(triggered) => {
                    if self.reset_timeout <= triggered.elapsed() {
                        let cp_rep = self
                            .clipping_point()
                            .map(|cp| format!("[{cp}]"))
                            .unwrap_or_else(String::new);
                        tracing::info!(
                            "clipping state: {}{} -> HalfClipped{}",
                            self.state,
                            &cp_rep,
                            &cp_rep,
                        );
                        self.state = S::HalfClipped;
                    }
                },
            },
        }
    }

    fn expiry_from_now(&self) -> SystemTime {
        SystemTime::now() + self.reset_timeout
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
    pub fn new(settings: &ClippingHandlingSettings) -> Self {
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

    pub fn note_clipping(&mut self, clipping_point: u32) {
        match self {
            Self::Ignore => (),
            Self::PermanentLimit(pt) => {
                let new_pt = pt.map(|p| u32::min(p, clipping_point)).unwrap_or(clipping_point);
                tracing::info!(
                    clipping_point=%new_pt, prior_clipping_point=?pt,
                    "possible source clipping identified - setting permanent clipping point."
                );
                *pt = Some(new_pt);

                PLANNING_PARALLELISM_CLIPPING_STATE.set(CLIPPING_STATE_CLIPPED);
                PLANNING_PARALLELISM_CLIPPING_POINT.set(new_pt.into());
            },
            Self::TemporaryLimit { cell } => {
                cell.borrow_mut().note_clipping(Some(clipping_point));

                PLANNING_PARALLELISM_CLIPPING_STATE.set(cell.borrow_mut().state_gauge());

                if let Some(cp) = self.clipping_point() {
                    PLANNING_PARALLELISM_CLIPPING_POINT.set(cp.into());
                }
            },
        };
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

pub static PLANNING_PARALLELISM_CLIPPING_STATE: Lazy<GenericGauge<AtomicU64>> = Lazy::new(|| {
    GenericGauge::with_opts(
        Opts::new(
            "planning_parallelism_clipping_state",
            "Indicates which clipping state the autoscaler considers the jop to be in: 0: Clear, 1: HalfClipped, 2: Clipped",
        )
            .const_labels(proctor::metrics::CONST_LABELS.clone()),
    )
        .expect("failed creating planning_parallelism_clipping_state metric")
});

#[cfg(test)]
mod tests {
    use super::*;
    use claim::*;
    use pretty_assertions::assert_eq;
    use ClippingState as S;

    #[test]
    fn test_clipping_temporary_limit() {
        once_cell::sync::Lazy::force(&proctor::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_clipping_temporary_limit");
        let _main_span_guard = main_span.enter();

        let mut temp_limit = TemporaryLimitCell::new(Duration::from_millis(10));
        assert_eq!(temp_limit.state, S::Clear);
        assert_none!(temp_limit.clipping_point());

        tracing::info!("AAA: clear: no clipping point");
        temp_limit.note_clipping(None);
        assert_eq!(temp_limit.state, S::Clear);
        assert_none!(temp_limit.clipping_point());

        tracing::info!("BBB: clear: first clipping point");
        temp_limit.note_clipping(Some(10));
        assert_eq!(temp_limit.state, S::Clipped(Instant::now()));
        assert_eq!(assert_some!(temp_limit.clipping_point()), 10);

        tracing::info!("CCC: clipped: note higher point");
        temp_limit.note_clipping(Some(20));
        assert_eq!(temp_limit.state, S::Clipped(Instant::now()));
        assert_eq!(assert_some!(temp_limit.clipping_point()), 10);

        tracing::info!("DDD: clipped: note lower point");
        temp_limit.note_clipping(Some(9));
        assert_eq!(temp_limit.state, S::Clipped(Instant::now()));
        assert_eq!(assert_some!(temp_limit.clipping_point()), 9);

        tracing::info!("EEE: sleep to half-clipped");
        std::thread::sleep(Duration::from_millis(11));
        temp_limit.note_clipping(None);
        assert_eq!(temp_limit.state, S::HalfClipped);
        assert_none!(temp_limit.clipping_point());

        tracing::info!("FFF: back to clipped (higher)");
        temp_limit.note_clipping(Some(15));
        assert_eq!(temp_limit.state, S::Clipped(Instant::now()));
        assert_eq!(assert_some!(temp_limit.clipping_point()), 9);

        tracing::info!("GGG: back to half clipped");
        std::thread::sleep(Duration::from_millis(11));
        temp_limit.note_clipping(None);
        assert_eq!(temp_limit.state, S::HalfClipped);
        assert_none!(temp_limit.clipping_point());

        tracing::info!("HHH: back to clipped (lower)");
        temp_limit.note_clipping(Some(7));
        assert_eq!(temp_limit.state, S::Clipped(Instant::now()));
        assert_eq!(assert_some!(temp_limit.clipping_point()), 7);

        tracing::info!("III: back to half clipped");
        std::thread::sleep(Duration::from_millis(11));
        temp_limit.note_clipping(None);
        assert_eq!(temp_limit.state, S::HalfClipped);
        assert_none!(temp_limit.clipping_point());

        tracing::info!("JJJ: go to clear");
        temp_limit.note_clipping(None);
        assert_eq!(temp_limit.state, S::Clear);
        assert_none!(temp_limit.clipping_point());

        tracing::info!("KKK: clip higher still but last point at 9");
        temp_limit.note_clipping(Some(20));
        assert_eq!(temp_limit.state, S::Clipped(Instant::now()));
        assert_eq!(assert_some!(temp_limit.clipping_point()), 9);
    }
}
