//! The Memcached backend (`BACKEND_TYPE=memcache`), a port of envoyproxy/ratelimit
//! `src/memcached/cache_impl.go`: the counters read with one `GetMulti` answer the request,
//! then each is increased in the background: `incr`, else `add` (a key memcached does not
//! have yet), else `incr` again (another `add` won the race). Refunds are `decr`, which
//! memcached floors at 0. Its client's operations are counted in `memcache.*`.

pub mod client;
pub mod srv;

use crate::config::RateLimit;
use crate::limiter::{self, CacheSettings, Clock, Count};
use crate::local_cache::LocalCache;
use crate::service::{CacheError, LimitOutcome, LimitReading, RateLimitCache};
use crate::stats::{Counter, Scope, Stats};
use client::{Client, Error};
use ratelimit_proto::envoy::service::ratelimit::v3::RateLimitRequest;
use std::sync::Arc;

/// `memcachedMaxRelativeExpirationSeconds`: past 30 days, memcached reads an expiration as a
/// Unix time.
const MAX_RELATIVE_EXPIRATION: i64 = 60 * 60 * 24 * 30;

/// `statsCollectingClient`'s counters, in `memcache.*`.
struct ClientStats {
    multiget_success: Counter,
    multiget_error: Counter,
    increment_success: Counter,
    increment_miss: Counter,
    increment_error: Counter,
    decrement_success: Counter,
    decrement_miss: Counter,
    decrement_error: Counter,
    add_success: Counter,
    add_error: Counter,
    add_not_stored: Counter,
    keys_requested: Counter,
    keys_found: Counter,
}

impl ClientStats {
    /// `CollectStats(client, scope)`.
    fn new(stats: &Stats, scope: &Scope) -> Self {
        let code = |name, code| stats.scope_counter_with_tags(scope, name, &[("code", code)]);
        Self {
            multiget_success: code("multiget", "success"),
            multiget_error: code("multiget", "error"),
            increment_success: code("increment", "success"),
            increment_miss: code("increment", "miss"),
            increment_error: code("increment", "error"),
            decrement_success: code("decrement", "success"),
            decrement_miss: code("decrement", "miss"),
            decrement_error: code("decrement", "error"),
            add_success: code("add", "success"),
            add_error: code("add", "error"),
            add_not_stored: code("add", "not_stored"),
            keys_requested: stats.scope_counter(scope, "keys_requested"),
            keys_found: stats.scope_counter(scope, "keys_found"),
        }
    }
}

/// The client, its operations counted.
struct CountedClient {
    client: Arc<Client>,
    stats: ClientStats,
}

impl CountedClient {
    async fn get_multi(
        &self,
        keys: &[String],
    ) -> (
        std::collections::HashMap<String, client::Item>,
        Option<Error>,
    ) {
        self.stats.keys_requested.add(keys.len() as u64);
        let (items, error) = self.client.get_multi(keys).await;
        match error {
            Some(_) => self.stats.multiget_error.add(1),
            None => {
                self.stats.keys_found.add(items.len() as u64);
                self.stats.multiget_success.add(1);
            }
        }
        (items, error)
    }

    async fn increment(&self, key: &str, delta: u64) -> Result<u64, Error> {
        let result = self.client.increment(key, delta).await;
        match &result {
            Err(Error::CacheMiss) => self.stats.increment_miss.add(1),
            Ok(_) => self.stats.increment_success.add(1),
            Err(_) => self.stats.increment_error.add(1),
        }
        result
    }

    async fn decrement(&self, key: &str, delta: u64) -> Result<u64, Error> {
        let result = self.client.decrement(key, delta).await;
        match &result {
            Err(Error::CacheMiss) => self.stats.decrement_miss.add(1),
            Ok(_) => self.stats.decrement_success.add(1),
            Err(_) => self.stats.decrement_error.add(1),
        }
        result
    }

    async fn add(&self, key: &str, value: &[u8], expiration: i32) -> Result<(), Error> {
        let result = self.client.add(key, value, expiration).await;
        match &result {
            Err(Error::NotStored) => self.stats.add_not_stored.add(1),
            Ok(()) => self.stats.add_success.add(1),
            Err(_) => self.stats.add_error.add(1),
        }
        result
    }
}

pub struct MemcacheCache {
    client: Arc<CountedClient>,
    clock: Arc<dyn Clock>,
    stats: Arc<Stats>,
    local_cache: Option<Arc<LocalCache>>,
    settings: CacheSettings,
}

impl MemcacheCache {
    /// `NewRateLimitCacheImplFromSettings`: `client`'s operations counted in `scope`'s
    /// `memcache.*`.
    pub fn new(
        client: Arc<Client>,
        clock: Arc<dyn Clock>,
        stats: Arc<Stats>,
        scope: &Scope,
        settings: CacheSettings,
        local_cache: Option<Arc<LocalCache>>,
    ) -> Self {
        let counted = CountedClient {
            client,
            stats: ClientStats::new(&stats, &scope.scope("memcache")),
        };
        Self {
            client: Arc::new(counted),
            clock,
            stats,
            local_cache,
            settings,
        }
    }
}

/// A descriptor's limit: its key and what it counts.
struct Counted {
    /// The descriptor's index in the request.
    index: usize,
    key: String,
    limit: RateLimit,
    hits: limiter::Hits,
    over_in_local_cache: bool,
}

impl RateLimitCache for MemcacheCache {
    /// The quota lookup (ADR 0003): one `GetMulti` of the keys, on the client itself so that
    /// nothing is counted in `memcache.*`.
    async fn lookup(
        &self,
        request: &RateLimitRequest,
        limits: &[Vec<RateLimit>],
    ) -> Result<(i64, Vec<Vec<LimitReading>>), CacheError> {
        let now = self.clock.unix_now();
        let calendar = self.settings.use_calendar_month;
        let mut keys = Vec::new();
        for (index, _, limit) in limiter::slots(limits) {
            let descriptor = &request.descriptors[index];
            let prefix = &self.settings.cache_key_prefix;
            keys.push(limiter::cache_key(
                prefix,
                &request.domain,
                descriptor,
                limit,
                now,
                calendar,
            )?);
        }
        let (values, error) = match keys.is_empty() {
            true => Default::default(),
            false => self.client.client.get_multi(&keys).await,
        };
        if let Some(e) = error {
            return Err(CacheError(format!(
                "Error multi-getting memcache keys: {e}"
            )));
        }
        let mut readings: Vec<Vec<LimitReading>> = limits
            .iter()
            .map(|list| Vec::with_capacity(list.len()))
            .collect();
        for ((index, _, limit), key) in limiter::slots(limits).zip(&keys) {
            // As a decision reads it: not a 32-bit number counts as 0.
            let used = values.get(key).map_or(0, |item| counter(item).unwrap_or(0));
            let reset = limiter::reset(limit, &request.descriptors[index], now, calendar)?;
            readings[index].push(LimitReading {
                used,
                reset: Some(reset),
            });
        }
        Ok((now, readings))
    }

    async fn do_limit(
        &self,
        request: &RateLimitRequest,
        limits: &[Vec<RateLimit>],
    ) -> Result<Vec<Vec<LimitOutcome>>, CacheError> {
        crate::debug!("starting cache lookup");
        let now = self.clock.unix_now();
        let hits = limiter::hits(request);
        let mut checks = Vec::new();
        let mut keys_to_get = Vec::new();
        for (index, _, limit) in limiter::slots(limits) {
            let key = limiter::cache_key(
                &self.settings.cache_key_prefix,
                &request.domain,
                &request.descriptors[index],
                limit,
                now,
                self.settings.use_calendar_month,
            )?;
            let hits = hits[index];
            limiter::count_hits(&self.stats.rate_limit(&limit.full_key), hits);
            // Refunds always reach memcached.
            let over_in_local_cache = !hits.refund
                && self
                    .local_cache
                    .as_ref()
                    .is_some_and(|cache| cache.contains(&key, now));
            if over_in_local_cache {
                crate::debug!("cache key is over the limit: {key}");
            } else {
                if !hits.refund {
                    crate::debug!("looking up cache key: {key}");
                }
                keys_to_get.push(key.clone());
            }
            checks.push(Counted {
                index,
                key,
                limit: limit.clone(),
                hits,
                over_in_local_cache,
            });
        }

        // Until the statuses are built.
        let _span = crate::trace::start(
            crate::trace::MEMCACHED,
            "Memcached Fetch Execution",
            crate::trace::Kind::Internal,
            crate::trace::parent().as_ref(),
            || {
                vec![(
                    "keysToGet length".into(),
                    crate::trace::Value::Int(keys_to_get.len() as i64),
                )]
            },
        );
        let mut values = std::collections::HashMap::new();
        if !keys_to_get.is_empty() {
            let (found, error) = self.client.get_multi(&keys_to_get).await;
            values = found;
            if let Some(e) = error {
                crate::log::error(&format!(
                    "Error multi-getting memcache keys ([{}]): {e}",
                    keys_to_get.join(" ")
                ));
            }
        }

        let mut counts = Vec::with_capacity(checks.len());
        for c in &checks {
            let before = match values.get(&c.key) {
                Some(item) => match counter(item) {
                    Ok(decoded) => decoded,
                    Err(_) => {
                        crate::log::error(&format!(
                            "Unexpected non-numeric value in memcached: {}",
                            item.go_format()
                        ));
                        0
                    }
                },
                None => 0,
            };
            let count = if c.over_in_local_cache {
                Count::OverInLocalCache
            } else if c.hits.refund {
                Count::After(before.saturating_sub(c.hits.value))
            } else {
                Count::After(before.wrapping_add(c.hits.value))
            };
            counts.push(count);
            if let (false, Count::After(after), Some(cache)) =
                (c.hits.refund, count, &self.local_cache)
                && limiter::exceeds(&c.limit, after)
            {
                // Cached for a whole window, shadow rules included, as upstream.
                let calendar = self.settings.use_calendar_month;
                let ttl = limiter::expiration(&c.limit, now, calendar)?;
                if !cache.insert(&c.key, ttl, now) {
                    crate::log::error(&format!("Failing to set local cache key: {}", c.key));
                }
            }
        }

        let mut statuses: Vec<Vec<LimitOutcome>> = limits
            .iter()
            .map(|list| Vec::with_capacity(list.len()))
            .collect();
        for (c, count) in checks.iter().zip(counts) {
            let stats = self.stats.rate_limit(&c.limit.full_key);
            let calendar = self.settings.use_calendar_month;
            let reset = limiter::reset(&c.limit, &request.descriptors[c.index], now, calendar)?;
            let ratio = self.settings.near_limit_ratio;
            let status =
                limiter::limit_status((&c.limit, &stats, &c.key), ratio, c.hits, count, reset);
            statuses[c.index].push(LimitOutcome {
                status,
                opened_without_hits: false,
                slot_pending: false,
            });
        }

        // `increaseAsync`, after the answer.
        let (client, clock, settings) = (
            self.client.clone(),
            self.clock.clone(),
            self.settings.clone(),
        );
        tokio::spawn(async move { increase(&client, &*clock, &settings, checks).await });
        Ok(statuses)
    }
}

/// `increaseAsync`: each key not over its limit in the local cache counted, its failures
/// logged.
async fn increase(
    client: &CountedClient,
    clock: &dyn Clock,
    settings: &CacheSettings,
    checks: Vec<Counted>,
) {
    for c in checks {
        if c.over_in_local_cache {
            continue;
        }
        let key = &c.key;
        if c.hits.refund {
            if let Err(e) = client.decrement(key, c.hits.value).await
                && e != Error::CacheMiss
            {
                crate::log::error(&format!("Failed to decrement key {key}: {e}"));
            }
            continue;
        }
        match client.increment(key, c.hits.value).await {
            Ok(_) => {}
            Err(Error::CacheMiss) => {
                let now = clock.unix_now();
                let calendar = settings.use_calendar_month;
                // It was computed for the key already: it cannot fail now but past the year
                // 9999, when nothing is counted.
                let Ok(mut expiration) = limiter::expiration(&c.limit, now, calendar) else {
                    continue;
                };
                let jitter = settings.expiration_jitter_max_seconds;
                if jitter > 0 {
                    expiration =
                        expiration.wrapping_add(crate::random::below(jitter as u64) as i64);
                }
                let value = c.hits.value.to_string();
                match client
                    .add(key, value.as_bytes(), expiration_for(expiration, now))
                    .await
                {
                    Ok(()) => {}
                    // Another request added it: increment it now.
                    Err(Error::NotStored) => {
                        if let Err(e) = client.increment(key, c.hits.value).await {
                            crate::log::error(&format!(
                                "Failed to increment key {key} after failing to add: {e}"
                            ));
                        }
                    }
                    Err(e) => crate::log::error(&format!("Failed to add key {key}: {e}")),
                }
            }
            Err(e) => crate::log::error(&format!("Failed to increment key {key}: {e}")),
        }
    }
}

/// A counter's value: `ParseInt(value, 10, 32)`, a negative one wrapping into a `uint64`.
fn counter(item: &client::Item) -> Result<u64, ()> {
    crate::go::parse_int10(&String::from_utf8_lossy(&item.value), 32)
        .map(|n| n as u64)
        .map_err(|_| ())
}

/// `memcacheExpiration`: past 30 days, the Unix time it ends, as an `int32`: at most its largest,
/// since a wrapped one would be expired already.
fn expiration_for(seconds: i64, now: i64) -> i32 {
    if seconds > MAX_RELATIVE_EXPIRATION {
        now.saturating_add(seconds).min(i32::MAX.into()) as i32
    } else {
        seconds as i32
    }
}
