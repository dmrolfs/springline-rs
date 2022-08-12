use chrono::{DateTime, TimeZone, Utc};
use claim::*;
use oso::Oso;
use proctor::elements::{PolicySource, QueryPolicy};
use proctor::error::PolicyError;
use proptest::prelude::*;
use std::io::Read;

#[tracing::instrument(level = "info")]
pub fn prepare_policy_engine<P: QueryPolicy>(policy: &P) -> Result<Oso, PolicyError> {
    for source in policy.sources() {
        match source {
            PolicySource::File { path, .. } => {
                let f = std::fs::File::open(path);
                match &f {
                    Ok(f0) => tracing::info!(file=?f0, ?path, "opened policy source file."),
                    Err(err) => {
                        tracing::error!(error=?err, ?path, "failed to open policy source file.")
                    },
                };

                let mut contents = String::new();
                let size = assert_ok!(assert_ok!(f).read_to_string(&mut contents));
                tracing::info!(%contents, %size, "reading policy FILE {path:?}.");
            },
            PolicySource::String { name, polar, .. } => {
                tracing::info!(contents=%polar, %name, "reading policy STRING {name}.");
            },
        }
    }

    for rendered in assert_ok!(policy.render_policy_sources()) {
        match rendered {
            proctor::elements::policy_filter::PolicySourcePath::File(path) => {
                let mut f = assert_ok!(std::fs::File::open(path.clone()));
                let mut contents = String::new();
                let size = assert_ok!(f.read_to_string(&mut contents));
                tracing::info!(%contents, %size, "reading RENDERED policy FILE {path:?}.");
            },
            proctor::elements::policy_filter::PolicySourcePath::String(tf) => {
                let mut f = assert_ok!(tf.reopen());
                let mut contents = String::new();
                let size = assert_ok!(f.read_to_string(&mut contents));
                tracing::info!(%contents, %size, "reading RENDERED policy STRING {tf:?}.");
            },
        }
    }

    let mut oso = Oso::new();
    policy.load_policy_engine(&mut oso)?;
    policy.initialize_policy_engine(&mut oso)?;
    Ok(oso)
}

pub fn arb_date_time() -> impl Strategy<Value = DateTime<Utc>> {
    let min_year = i32::MIN >> 13;
    let max_year = i32::MAX >> 13;

    fn is_leap_year(year: i32) -> bool {
        return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    }

    fn last_day_of_month(year: i32, month: u32) -> u32 {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if is_leap_year(year) {
                    29
                } else {
                    28
                }
            },
            _ => panic!("invalid month: {}", month),
        }
    }

    (
        min_year..=max_year,
        1_u32..=12,
        0_u32..24,
        0_u32..60,
        0_u32..60,
        0_u32..1_000_000_000,
    )
        .prop_flat_map(|(yr, mnth, h, m, s, n): (i32, u32, u32, u32, u32, u32)| {
            let day = 1_u32..=last_day_of_month(yr, mnth);
            (
                Just(yr),
                Just(mnth),
                day,
                Just(h),
                Just(m),
                Just(s),
                Just(n),
            )
        })
        .prop_map(
            |(yr, mnth, day, h, m, s, n): (i32, u32, u32, u32, u32, u32, u32)| {
                // tracing::info!("DMR: yr:{yr} mnth:{mnth} day:{day} h:{h} m:{m} s:{s} n:{n}");
                Utc.ymd(yr, mnth, day).and_hms_nano(h, m, s, n)
            },
        )
}
