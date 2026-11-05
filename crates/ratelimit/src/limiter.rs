//! Counting logic shared by the backends: time, cache keys and descriptor statuses for
//! fixed windows. A port of envoyproxy/ratelimit `src/limiter/{base_limiter,cache_key}.go`
//! and the time helpers of `src/utils`.

use crate::config::window::Window;
use crate::config::{RateLimit, escape_hash_tag};
use crate::service::CacheError;
use crate::stats::RateLimitStats;
use ratelimit_proto::envoy::extensions::common::ratelimit::v3::RateLimitDescriptor;
use ratelimit_proto::envoy::service::ratelimit::v3::RateLimitRequest;
use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::{
    Code, DescriptorStatus, rate_limit::Unit,
};

/// The current time, in Unix seconds.
pub trait Clock: Send + Sync + 'static {
    fn unix_now(&self) -> i64;
}

/// The system clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn unix_now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64)
    }
}

/// The length of a fixed window, in seconds (a month is 30 days, a year 365).
/// The start of the window containing `now`: a multiple of the unit since the epoch, or with
/// `USE_CALENDAR_MONTH_RATE_LIMIT` for a month, the first of the UTC calendar month.
pub(crate) fn window_start(unit: Unit, now: i64, calendar: bool) -> Result<i64, CacheError> {
    if calendar && unit == Unit::Month {
        return month_start(now);
    }
    let window = unit_seconds(unit)?;
    Ok(now / window * window)
}

/// `ExpirationSeconds`: a key's TTL, the unit's length, or with
/// `USE_CALENDAR_MONTH_RATE_LIMIT` for a month, the seconds until the next month.
pub(crate) fn expiration_seconds(unit: Unit, now: i64, calendar: bool) -> Result<i64, CacheError> {
    if calendar && unit == Unit::Month {
        return Ok(next_month_start(now)? - now);
    }
    unit_seconds(unit)
}

/// `CalculateReset`: the seconds until the window containing `now` ends.
pub(crate) fn reset_seconds(unit: Unit, now: i64, calendar: bool) -> Result<i64, CacheError> {
    if calendar && unit == Unit::Month {
        return Ok(next_month_start(now)? - now);
    }
    let window = unit_seconds(unit)?;
    Ok(window - now % window)
}

/// The UTC calendar month of `now`: its year and month.
fn utc_month(now: i64) -> Result<(i16, i8), CacheError> {
    let date = jiff::Timestamp::from_second(now)
        .map_err(|e| CacheError(format!("calendar month of {now}: {e}")))?
        .to_zoned(jiff::tz::TimeZone::UTC)
        .date();
    Ok((date.year(), date.month()))
}

/// `MonthStartUnix`: the first of `now`'s UTC month, at midnight.
fn month_start(now: i64) -> Result<i64, CacheError> {
    let (year, month) = utc_month(now)?;
    midnight(year, month)
}

/// The first of the month after `now`'s, at midnight UTC.
fn next_month_start(now: i64) -> Result<i64, CacheError> {
    match utc_month(now)? {
        (year, 12) => midnight(year + 1, 1),
        (year, month) => midnight(year, month + 1),
    }
}

/// Midnight UTC on the first of a month (years beyond 9999 are not dates here).
fn midnight(year: i16, month: i8) -> Result<i64, CacheError> {
    jiff::civil::Date::new(year, month, 1)
        .and_then(|date| date.to_zoned(jiff::tz::TimeZone::UTC))
        .map(|z| z.timestamp().as_second())
        .map_err(|e| CacheError(format!("calendar month {year}-{month}: {e}")))
}

/// The length of one `unit`, in seconds (a month is 30 days, a year 365).
pub(crate) fn unit_seconds(unit: Unit) -> Result<i64, CacheError> {
    Ok(match unit {
        Unit::Second => 1,
        Unit::Minute => 60,
        Unit::Hour => 60 * 60,
        Unit::Day => 60 * 60 * 24,
        Unit::Week => 60 * 60 * 24 * 7,
        Unit::Month => 60 * 60 * 24 * 30,
        Unit::Year => 60 * 60 * 24 * 365,
        // Upstream panics and the process dies; reported as an error instead (D1).
        Unit::Unknown => return Err(CacheError("invalid rate limit unit UNKNOWN".to_string())),
    })
}

/// A descriptor's entries as its keys are built from them: with `share_threshold`, an entry
/// that matched a shared wildcard has the pattern in place of its value.
fn key_entries<'a>(
    descriptor: &'a RateLimitDescriptor,
    limit: &'a RateLimit,
) -> impl Iterator<Item = (&'a str, &'a str)> {
    descriptor.entries.iter().enumerate().map(|(i, entry)| {
        let shared = limit
            .share_threshold_key_pattern
            .as_ref()
            .and_then(|patterns| patterns.get(i))
            .filter(|pattern| !pattern.is_empty());
        (entry.key.as_str(), shared.unwrap_or(&entry.value).as_str())
    })
}

/// The storage key of `limit` for `descriptor` in the window containing `now`. For
/// `rate_limit:`, upstream's: `prefix + domain + "_" + (key "_" value "_")* + windowStart`.
/// For a `rate_limits:` entry, ADR 0001's: that descriptor part, escaped, as the hash tag,
/// then the window's shape, its start and the limit's name.
pub fn cache_key(
    prefix: &str,
    domain: &str,
    descriptor: &RateLimitDescriptor,
    limit: &RateLimit,
    now: i64,
    calendar: bool,
) -> Result<String, CacheError> {
    let Some(window) = &limit.window else {
        let mut key = format!("{prefix}{domain}_");
        for (k, v) in key_entries(descriptor, limit) {
            key += k;
            key += "_";
            key += v;
            key += "_";
        }
        key += &window_start(limit.limit.unit(), now, calendar)?.to_string();
        return Ok(key);
    };
    let mut key = format!("{{{}{}_", escape_hash_tag(prefix), escape_hash_tag(domain));
    for (k, v) in key_entries(descriptor, limit) {
        key += &escape_hash_tag(k);
        key += "_";
        key += &escape_hash_tag(v);
        key += "_";
    }
    key += &format!("}}_{}_", window.shape());
    // A fixed window's key changes with the window; an anchored one's holds its anchor.
    if let Window::Fixed(fixed) = window {
        let start = fixed.start(now, fixed.phase(key_entries(descriptor, limit)));
        key += &format!("{start}_");
    }
    key += &limit.name;
    Ok(key)
}

/// A key's TTL: [`expiration_seconds`], or a list limit's window.
pub(crate) fn expiration(limit: &RateLimit, now: i64, calendar: bool) -> Result<i64, CacheError> {
    match &limit.window {
        Some(window) => Ok(window.duration()),
        None => expiration_seconds(limit.limit.unit(), now, calendar),
    }
}

/// The seconds until `limit`'s window containing `now` ends, for `descriptor`; for an
/// anchored window, which only its state knows, a window opened now's.
pub(crate) fn reset(
    limit: &RateLimit,
    descriptor: &RateLimitDescriptor,
    now: i64,
    calendar: bool,
) -> Result<i64, CacheError> {
    match &limit.window {
        Some(Window::Fixed(w)) => Ok(w.reset(now, w.phase(key_entries(descriptor, limit)))),
        Some(Window::Anchored(w)) => Ok(w.reset(None, now)),
        Some(Window::Concurrency(w)) => Ok(w.retry_after),
        // Only its state knows: decisions and headers read that.
        Some(Window::TokenBucket(_)) => Ok(0),
        None => reset_seconds(limit.limit.unit(), now, calendar),
    }
}

/// Every limit of every descriptor: the descriptor's index, the limit's in its list, the limit.
pub(crate) fn slots(limits: &[Vec<RateLimit>]) -> impl Iterator<Item = (usize, usize, &RateLimit)> {
    limits.iter().enumerate().flat_map(|(index, list)| {
        list.iter()
            .enumerate()
            .map(move |(slot, limit)| (index, slot, limit))
    })
}

/// Whether `limit`'s keys go to the per-second Redis (`REDIS_PERSECOND`): a limit in seconds,
/// or a list with a window shorter than a minute (D15).
pub(crate) fn per_second(limits: &[RateLimit]) -> bool {
    limits.iter().any(|limit| match &limit.window {
        // Its lease says nothing of how often it is asked: on the main Redis.
        Some(Window::Concurrency(_)) => false,
        Some(window) => window.duration() < 60,
        None => limit.limit.unit() == Unit::Second,
    })
}

/// How much a descriptor adds to (or, for a refund, removes from) its counter.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Hits {
    pub value: u64,
    pub refund: bool,
}

/// Per descriptor: its own `hits_addend` when set (0 checks without counting), else the
/// request's, where 0 (unset) means 1.
pub(crate) fn hits(request: &RateLimitRequest) -> Vec<Hits> {
    request
        .descriptors
        .iter()
        .map(|d| Hits {
            value: d
                .hits_addend
                .unwrap_or(u64::from(request.hits_addend.max(1))),
            refund: d.is_negative_hits,
        })
        .collect()
}

pub(crate) fn status(
    code: Code,
    limit: &RateLimit,
    limit_remaining: u32,
    reset: i64,
) -> DescriptorStatus {
    DescriptorStatus {
        code: code as i32,
        current_limit: Some(limit.limit.clone()),
        limit_remaining,
        duration_until_reset: Some(prost_types::Duration {
            seconds: reset,
            nanos: 0,
        }),
        quota: None,
    }
}

/// Whether a counter of `count` is over `limit`.
pub(crate) fn exceeds(limit: &RateLimit, count: u64) -> bool {
    count > u64::from(limit.limit.requests_per_unit)
}

/// What the backend found for a descriptor.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Count {
    /// Its counter once its hits are applied.
    After(u64),
    /// Over its limit according to the local cache; the backend was not asked.
    OverInLocalCache,
}

/// Counts `hits` towards `limit`'s `total_hits`, or `total_negative_hits` for a refund, as
/// upstream does when it builds the keys, before asking the backend.
pub(crate) fn count_hits(stats: &RateLimitStats, hits: Hits) {
    if hits.refund {
        stats.total_negative_hits.add(hits.value);
    } else {
        stats.total_hits.add(hits.value);
    }
}

/// Splits a request's `hits` between `limit`'s `within_limit`, `near_limit`, `over_limit`,
/// `over_limit_with_local_cache` and `shadow_mode` counters, as upstream's
/// `GetResponseDescriptorStatus`. Usage before the hits is taken as the counter minus the hits,
/// wrapping like upstream's unsigned arithmetic when fewer were counted (D22).
fn count_outcome(
    stats: &RateLimitStats,
    limit: &RateLimit,
    hits: u64,
    count: Count,
    near_limit_ratio: f32,
) {
    let after = match count {
        Count::OverInLocalCache => {
            stats.over_limit.add(hits);
            stats.over_limit_with_local_cache.add(hits);
            if limit.shadow_mode {
                stats.shadow_mode.add(hits);
            }
            return;
        }
        Count::After(after) => after,
    };
    let before = after.wrapping_sub(hits);
    let over = u64::from(limit.limit.requests_per_unit);
    // In float32, as upstream: `floor(float64(float32(limit) * ratio))`.
    let near = f64::from(over as f32 * near_limit_ratio).floor() as u64;
    if after > over {
        // All the hits were over when the counter already was; otherwise only those past the
        // limit, the rest being near it.
        let over_hits = if before >= over { hits } else { after - over };
        stats.over_limit.add(over_hits);
        if before < over {
            stats.near_limit.add(over.wrapping_sub(near.max(before)));
        }
        if limit.shadow_mode {
            stats.shadow_mode.add(over_hits);
        }
    } else {
        if after > near {
            stats
                .near_limit
                .add(if before >= near { hits } else { after - near });
        }
        stats.within_limit.add(hits);
    }
}

/// The status of `limit`, its outcome counted in the limit's `stats`, `reset` seconds from
/// its window's end. A refund is always OK, with what remains. Otherwise it is over the limit
/// when the counter exceeds it (or the local cache says so); the service then applies shadow
/// mode.
pub(crate) fn limit_status(
    (limit, stats, key): (&RateLimit, &RateLimitStats, &str),
    near_limit_ratio: f32,
    hits: Hits,
    count: Count,
    reset: i64,
) -> DescriptorStatus {
    if !hits.refund {
        count_outcome(stats, limit, hits.value, count, near_limit_ratio);
    }
    let threshold = u64::from(limit.limit.requests_per_unit);
    let answer = |code, remaining| status(code, limit, remaining, reset);
    match count {
        Count::After(after) if hits.refund => {
            return answer(Code::Ok, threshold.saturating_sub(after) as u32);
        }
        Count::After(after) => {
            crate::debug!("cache key: {key} current: {after} limit: {threshold}");
            if !exceeds(limit, after) {
                return answer(Code::Ok, (threshold - after) as u32);
            }
        }
        Count::OverInLocalCache => {}
    }
    answer(Code::OverLimit, 0)
}

/// What a request's keys look like before counting, for
/// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestState {
    /// A key of the request is over its limit in the local cache.
    OverInLocalCache,
    /// The hits will take some keys over their limit.
    SomeGoingOver,
    /// Neither.
    WithinLimits,
}

/// How much to add to a key's counter: upstream's `getHitsAddendValue`. With
/// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`, nothing is counted once a key of the request is
/// over in the local cache, and when some keys are about to go over, only those are counted.
pub(crate) fn hits_to_count(
    hits: Hits,
    stop_increment_when_overlimit: bool,
    request: RequestState,
    going_over: bool,
) -> u64 {
    if !stop_increment_when_overlimit {
        return hits.value;
    }
    match request {
        RequestState::OverInLocalCache => 0,
        RequestState::WithinLimits => hits.value,
        RequestState::SomeGoingOver if going_over => hits.value,
        RequestState::SomeGoingOver => 0,
    }
}

/// What a cache (Redis, Memcached) takes from the settings.
#[derive(Debug, Clone)]
pub struct CacheSettings {
    /// `CACHE_KEY_PREFIX`.
    pub cache_key_prefix: String,
    /// `LOCAL_CACHE_SIZE_IN_BYTES`: memory for remembering keys over their limit, so later
    /// requests skip the backend. 0 disables it.
    pub local_cache_size_in_bytes: usize,
    /// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`: Redis's only, as upstream's.
    pub stop_cache_key_increment_when_overlimit: bool,
    /// `NEAR_LIMIT_RATIO`: the share of a limit past which hits count as `near_limit`
    /// (upstream's default: 0.8).
    pub near_limit_ratio: f32,
    /// `EXPIRATION_JITTER_MAX_SECONDS`: each key's TTL gets a random extra in `[0, max)`,
    /// so that keys do not all expire at once; none at 0 or less.
    pub expiration_jitter_max_seconds: i64,
    /// `USE_CALENDAR_MONTH_RATE_LIMIT`: a month is the UTC calendar month, not 30 days.
    pub use_calendar_month: bool,
    /// `SHADOW_MODE`: the local cache keeps no token bucket then (D18).
    pub global_shadow_mode: bool,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            cache_key_prefix: String::new(),
            local_cache_size_in_bytes: 0,
            stop_cache_key_increment_when_overlimit: false,
            near_limit_ratio: 0.8,
            expiration_jitter_max_seconds: 0,
            use_calendar_month: false,
            global_shadow_mode: false,
        }
    }
}
