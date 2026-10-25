//! UTC calendar arithmetic without a date library (SigV4 timestamps, credential expiry).

use std::time::{SystemTime, UNIX_EPOCH};

pub fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

pub fn unix_now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// (year, month, day) of a day count since 1970-01-01 (Howard Hinnant's `civil_from_days`).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `(YYYYMMDD, YYYYMMDDTHHMMSSZ)` for SigV4.
pub fn amz_dates(unix: u64) -> (String, String) {
    let days = (unix / 86_400) as i64;
    let secs = unix % 86_400;
    let (y, m, d) = civil(days);
    let date = format!("{y:04}{m:02}{d:02}");
    let time = format!("{date}T{:02}{:02}{:02}Z", secs / 3600, secs / 60 % 60, secs % 60);
    (date, time)
}

/// `YYYY-MM-DD HH:MM:SS` (UTC).
pub fn iso(unix: u64) -> String {
    let (y, m, d) = civil((unix / 86_400) as i64);
    let s = unix % 86_400;
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
}

/// Parses `YYYY-MM-DDTHH:MM:SS[.fff](Z|±HH:MM)` to unix seconds.
pub fn parse_iso8601(s: &str) -> Option<u64> {
    let s = s.trim();
    let num = |a: usize, b: usize| -> Option<i64> { s.get(a..b)?.parse().ok() };
    let (y, mo, d) = (num(0, 4)?, num(5, 7)? as u32, num(8, 10)? as u32);
    let (h, mi, se) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    let mut rest = &s[19..];
    if let Some(r) = rest.strip_prefix('.') {
        rest = r.trim_start_matches(|c: char| c.is_ascii_digit());
    }
    let offset = match rest {
        "" | "Z" | "z" => 0,
        r if r.len() == 6 && (r.starts_with('+') || r.starts_with('-')) => {
            let oh: i64 = r[1..3].parse().ok()?;
            let om: i64 = r[4..6].parse().ok()?;
            let o = oh * 3600 + om * 60;
            if r.starts_with('+') {
                o
            } else {
                -o
            }
        }
        _ => return None,
    };
    let t = days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + se - offset;
    u64::try_from(t).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates() {
        assert_eq!(amz_dates(1_369_353_600), ("20130524".into(), "20130524T000000Z".into()));
        assert_eq!(amz_dates(951_782_400 + 3_723), ("20000229".into(), "20000229T010203Z".into()));
        assert_eq!(iso(0), "1970-01-01 00:00:00");
        assert_eq!(parse_iso8601("2013-05-24T00:00:00Z"), Some(1_369_353_600));
        assert_eq!(parse_iso8601("2013-05-24T02:00:00.123+02:00"), Some(1_369_353_600));
        assert_eq!(parse_iso8601("2000-02-29T01:02:03Z"), Some(951_782_400 + 3_723));
        assert_eq!(parse_iso8601("garbage"), None);
    }
}
