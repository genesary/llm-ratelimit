//! The quota lookup's answer, as JSON (ADR 0003).

use crate::config::RateLimit;
use crate::config::window::Window;
use crate::service::{LimitQuota, LimitReading, QuotaLookup};
use serde_json::{Value, json};

/// `{"now": …, "descriptors": [{"limits": [...]}, ...]}`.
pub fn render(lookup: &QuotaLookup) -> String {
    let descriptors: Vec<Value> = lookup
        .descriptors
        .iter()
        .map(|limits| {
            let limits: Vec<Value> = limits
                .iter()
                .map(|quota| match quota {
                    LimitQuota::Unlimited(limit) => {
                        json!({"name": limit.name, "unlimited": true})
                    }
                    LimitQuota::Limited(limit, reading) => limit_entry(limit, reading, lookup.now),
                })
                .collect();
            json!({ "limits": limits })
        })
        .collect();
    json!({"now": rfc3339(lookup.now), "descriptors": descriptors}).to_string()
}

/// A limit's entry: its window, its figures, when it resets.
fn limit_entry(limit: &RateLimit, reading: &LimitReading, now: i64) -> Value {
    // A token bucket's figures are of its capacity.
    let requests = match &limit.window {
        Some(Window::TokenBucket(w)) => w.capacity(limit.limit.requests_per_unit),
        _ => u64::from(limit.limit.requests_per_unit),
    };
    let window = match &limit.window {
        Some(Window::Fixed(w)) => json!({"type": "fixed", "duration_s": w.duration}),
        Some(Window::Anchored(w)) => json!({"type": "anchored", "duration_s": w.duration}),
        Some(Window::Concurrency(w)) => {
            json!({"type": "concurrency", "lease_s": w.lease, "retry_after_s": w.retry_after})
        }
        Some(Window::TokenBucket(w)) => {
            json!({"type": "token_bucket", "duration_s": w.duration, "burst": requests})
        }
        None => json!({"type": "unit", "unit": limit.limit.unit().as_str_name()}),
    };
    let mut entry = json!({
        "name": limit.name,
        "window": window,
        "limit": requests,
        "used": reading.used,
        "remaining": requests.saturating_sub(reading.used),
        "used_pct": used_pct(reading.used, requests),
        "resets_at": reading.reset.map(|r| rfc3339(now + r)),
        "resets_in_s": reading.reset,
        "shadow_mode": limit.shadow_mode,
        "quota_mode": limit.quota_mode,
    });
    if let Some(Window::Anchored(_)) = &limit.window {
        entry["open"] = json!(reading.reset.is_some());
    }
    if let Some(Window::Concurrency(_)) = &limit.window {
        entry["in_flight"] = json!(reading.used);
    }
    if let Some(Window::TokenBucket(_)) = &limit.window {
        entry["level"] = json!(requests as i64 - reading.used as i64);
    }
    if limit.soft.is_some() {
        entry["soft"] = json!(true);
        entry["over"] = json!(reading.over);
    }
    entry
}

/// `used / limit × 100` to a tenth, uncapped; a limit of 0 is at 100 once used.
fn used_pct(used: u64, limit: u64) -> f64 {
    match limit {
        0 if used > 0 => 100.0,
        0 => 0.0,
        _ => (used as f64 * 1000.0 / limit as f64).round() / 10.0,
    }
}

/// A Unix time in RFC 3339, UTC, to the second.
fn rfc3339(seconds: i64) -> String {
    jiff::Timestamp::from_second(seconds).map_or_else(|_| seconds.to_string(), |t| t.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentages_are_to_a_tenth_and_uncapped() {
        assert_eq!(used_pct(1_600_000, 4_000_000), 40.0);
        assert_eq!(used_pct(1, 3), 33.3);
        assert_eq!(used_pct(2, 3), 66.7);
        assert_eq!(used_pct(207, 200), 103.5);
        assert_eq!(used_pct(0, 0), 0.0);
        assert_eq!(used_pct(1, 0), 100.0);
    }

    #[test]
    fn times_are_utc_seconds() {
        assert_eq!(rfc3339(1_000_000_000), "2001-09-09T01:46:40Z");
    }
}
