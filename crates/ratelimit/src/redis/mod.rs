//! The Redis backend for fixed windows. A port of envoyproxy/ratelimit
//! `src/redis/fixed_cache_impl.go`: one pipeline of `INCRBY` + `EXPIRE` per request, and a Lua
//! script for refunds. How the service reaches Redis is in [`connection`].

mod aws;
mod connection;
mod live;
pub mod tls;

pub use connection::{Connected, RedisConnection, StartupBackoff, Tls, mask_credentials_in_url};

use crate::config::RateLimit;
use crate::config::window::{
    Anchored, AnchoredWindow, BucketResult, ConcurrencyWindow, MAX_ANCHORED_COUNT, MAX_TAT, OpenOn,
    SlotResult, TokenBucketWindow, Window,
};
use crate::limiter::{self, CacheSettings, Clock, Count, Hits, RequestState};
use crate::local_cache::LocalCache;
use crate::service::{
    CacheError, HeldSlot, LimitOutcome, LimitReading, RateLimitCache, SlotAnswer, SlotCheck,
};
use crate::stats::{RateLimitStats, Stats};
use fred::prelude::*;
use fred::types::{ClusterHash, CustomCommand};
use live::LivePool;
use ratelimit_proto::envoy::service::ratelimit::v3::RateLimitRequest;
use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::{Code, DescriptorStatus};
use std::sync::Arc;

/// Upstream's `DecrementScript`, byte for byte: subtract, floor at 0, reset the TTL.
const DECREMENT_SCRIPT: &str = "
local current = redis.call('GET', KEYS[1])                   -- get current count
if current == false then return 0 end                        -- key absent: nothing to refund
local new_val = math.floor(math.max(0, tonumber(current) - tonumber(ARGV[1]))) -- subtract hits, floor at 0
redis.call('SET', KEYS[1], tostring(new_val))                -- persist new value
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))             -- reset TTL
return new_val                                               -- return count after decrement
";

/// An `anchored` window (docs/design.md D9), as [`AnchoredWindow::step`] decides it: the
/// anchor and the count in one hash, the window closed once `now ≥ anchor + duration`, never
/// by the key's absence. ARGV: now, duration, rounding (0: none), hits, opened by a check
/// (1) or a first hit (0), refund (1) or not (0), TTL margin, the largest count (2^53, which
/// hits are capped at too). Returns the anchor (-1: no window), the count, and whether this
/// request opened it.
const ANCHORED_SCRIPT: &str = "
local now, duration, rounding = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local hits, on_check, refund = tonumber(ARGV[4]), ARGV[5] == '1', ARGV[6] == '1'
local anchor = tonumber(redis.call('HGET', KEYS[1], 'a'))
local count = tonumber(redis.call('HGET', KEYS[1], 'c')) or 0
if anchor ~= nil and now - anchor >= duration then anchor = nil end
if refund then
  if anchor == nil then return {-1, 0, 0} end
  count = math.max(0, count - hits)
  redis.call('HSET', KEYS[1], 'c', string.format('%d', count))
  return {anchor, count, 0}
end
if anchor == nil then
  if not on_check and hits == 0 then return {-1, 0, 0} end
  anchor = now
  if rounding > 0 then anchor = now - now % rounding end
  redis.call('HSET', KEYS[1], 'a', string.format('%d', anchor), 'c', string.format('%d', hits))
  redis.call('EXPIRE', KEYS[1], anchor + duration - now + tonumber(ARGV[7]))
  return {anchor, hits, 1}
end
count = math.min(count + hits, tonumber(ARGV[8]))
redis.call('HSET', KEYS[1], 'c', string.format('%d', count))
return {anchor, count, 0}
";

/// How long an anchored window's key outlives it, for replicas whose clocks are behind.
const ANCHORED_MARGIN_SECONDS: i64 = 60;

/// A `concurrency` window's slots (docs/design.md D35), as [`ConcurrencyWindow::step`] decides
/// them: a sorted set of members scored by when they were acquired. ARGV: now, lease, TTL,
/// then `acquire`, the limit, force (1) or not (0), the member; `release` and how many, the
/// newest first; or `remove` and the member. Returns whether it acquired, how many it
/// released, how many leases had ended, and how many slots are left.
const CONCURRENCY_SCRIPT: &str = "
local now, lease = tonumber(ARGV[1]), tonumber(ARGV[2])
local expired = redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - lease)
local acquired, released = 0, 0
if ARGV[4] == 'acquire' then
  if ARGV[6] == '1' or redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[5]) then
    redis.call('ZADD', KEYS[1], now, ARGV[7])
    acquired = 1
  end
elseif ARGV[4] == 'release' then
  released = #redis.call('ZPOPMAX', KEYS[1], ARGV[5]) / 2
else
  released = redis.call('ZREM', KEYS[1], ARGV[5])
end
local in_flight = redis.call('ZCARD', KEYS[1])
if in_flight > 0 then redis.call('EXPIRE', KEYS[1], ARGV[3]) end
return {acquired, released, expired, in_flight}
";

/// A `token_bucket` window (docs/design.md D11), as [`TokenBucketWindow::step`] decides it: the
/// deadline at which the bucket is full, in microseconds. ARGV: now, a token's interval, how
/// far past now a charge may go (-1: no limit), the largest deadline, hits, refund (1) or
/// not (0), TTL margin. Returns the deadlines before and after, and the hits past the limit.
const TOKEN_BUCKET_SCRIPT: &str = "
local now, interval, limit = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local largest, hits = tonumber(ARGV[4]), tonumber(ARGV[5])
local stored = tonumber(redis.call('GET', KEYS[1])) or now
local before = math.max(stored, now)
local after, capped = before, 0
if ARGV[6] == '1' then
  after = math.max(now, before - hits * interval)
else
  after = before + hits * interval
  local most = largest
  if limit >= 0 then most = math.min(now + limit, largest) end
  most = math.max(most, before)
  if after > most then
    capped = math.min(hits, math.ceil((after - most) / interval))
    after = most
  end
end
after = math.ceil(after)
if after ~= before then
  if after > now then
    redis.call('SET', KEYS[1], string.format('%.0f', after))
    redis.call('EXPIRE', KEYS[1], math.ceil((after - now) / 1000000) + tonumber(ARGV[7]))
  else
    redis.call('DEL', KEYS[1])
  end
end
return {string.format('%.0f', before), string.format('%.0f', after), string.format('%.0f', capped)}
";

/// How long a token bucket's key outlives the moment it is full, for replicas whose clocks are
/// behind.
const TOKEN_BUCKET_MARGIN_SECONDS: i64 = 60;

/// How long a concurrency window's key outlives its newest lease.
const CONCURRENCY_MARGIN_SECONDS: i64 = 60;

pub struct RedisCache {
    /// Connections, each request's pipeline on one of them (radix's pool).
    pool: LivePool,
    /// `REDIS_PERSECOND`: the pool for limits in seconds, in a pipeline of their own.
    per_second: Option<LivePool>,
    clock: Arc<dyn Clock>,
    stats: Arc<Stats>,
    local_cache: Option<Arc<LocalCache>>,
    settings: CacheSettings,
}

impl RedisCache {
    pub fn new(
        pool: Pool,
        clock: Arc<dyn Clock>,
        stats: Arc<Stats>,
        settings: CacheSettings,
    ) -> Self {
        let local_cache = (settings.local_cache_size_in_bytes > 0)
            .then(|| Arc::new(LocalCache::new(settings.local_cache_size_in_bytes)));
        Self::with_local_cache(pool, clock, stats, settings, local_cache)
    }

    /// The cache using `local_cache` (made by the runner, whose stats read it).
    pub fn with_local_cache(
        pool: Pool,
        clock: Arc<dyn Clock>,
        stats: Arc<Stats>,
        settings: CacheSettings,
        local_cache: Option<Arc<LocalCache>>,
    ) -> Self {
        Self {
            pool: LivePool::new(pool),
            per_second: None,
            clock,
            stats,
            local_cache,
            settings,
        }
    }

    /// The cache sending limits in seconds to `pool` (`REDIS_PERSECOND`).
    pub fn with_per_second(mut self, pool: Pool) -> Self {
        self.per_second = Some(LivePool::new(pool));
        self
    }

    /// The pipeline a descriptor's `limits` go in: 0 the main one, 1 the per-second one.
    fn pipeline_of(&self, limits: &[RateLimit]) -> usize {
        match &self.per_second {
            Some(_) if limiter::per_second(limits) => 1,
            _ => 0,
        }
    }

    /// The pipelines' pools: the main one, then the per-second one if any.
    fn pools(&self) -> [Option<&LivePool>; 2] {
        [Some(&self.pool), self.per_second.as_ref()]
    }
}

fn cache_error(e: fred::error::Error) -> CacheError {
    CacheError(e.details().to_string())
}

/// A Redis command as upstream's client (radix) sends it: every argument as a string.
struct Command {
    name: &'static str,
    args: Vec<String>,
}

impl Command {
    /// radix's rendering in errors: `["INCRBY" "key" "1"]`, each part quoted like Go's
    /// `strconv.QuoteToASCII`.
    fn describe(&self) -> String {
        let parts: Vec<String> = std::iter::once(self.name)
            .chain(self.args.iter().map(String::as_str))
            .map(go_quote_ascii)
            .collect();
        format!("[{}]", parts.join(" "))
    }
}

/// Go's `strconv.QuoteToASCII`.
fn go_quote_ascii(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '\x07' => out += "\\a",
            '\x08' => out += "\\b",
            '\x0c' => out += "\\f",
            '\n' => out += "\\n",
            '\r' => out += "\\r",
            '\t' => out += "\\t",
            '\x0b' => out += "\\v",
            '\\' => out += "\\\\",
            '"' => out += "\\\"",
            ' '..='~' => out.push(c),
            c if (c as u32) < 0x80 => out += &format!("\\x{:02x}", c as u32),
            c if (c as u32) < 0x10000 => out += &format!("\\u{:04x}", c as u32),
            c => out += &format!("\\U{:08x}", c as u32),
        }
    }
    out.push('"');
    out
}

/// Sends `commands` in one pipeline and returns their replies, or upstream's error for the
/// first command that failed.
async fn send(client: &Client, commands: &[Command]) -> Result<Vec<Value>, CacheError> {
    // A client that is not connected fails the pipeline at once, as radix's dead connection
    // does, rather than holding it until Redis is back.
    let fail_fast = Options {
        fail_fast: true,
        ..Default::default()
    };
    // `with_options(..).pipeline()` would build it on the bare client, through `Deref`.
    let pipeline = fred::clients::Pipeline::from(client.with_options(&fail_fast));
    for command in commands {
        // EVAL is routed by its key, the third argument, in a cluster.
        let hash = if command.name == "EVAL" {
            ClusterHash::Offset(2)
        } else {
            ClusterHash::FirstKey
        };
        let custom = CustomCommand::new_static(command.name, hash, false);
        let () = pipeline
            .custom(custom, command.args.clone())
            .await
            .map_err(cache_error)?;
    }
    let replies: Vec<Result<Value, fred::error::Error>> = pipeline.try_all().await;
    commands
        .iter()
        .zip(replies)
        .map(|(command, reply)| reply.map_err(|e| command.failed(e.details())))
        .collect()
}

impl Command {
    /// Upstream's error for this command: radix wraps the first failure of a pipeline.
    fn failed(&self, error: impl std::fmt::Display) -> CacheError {
        CacheError(format!(
            "command {} in pipeline returned error: {error}",
            self.describe()
        ))
    }

    /// A reply read as a counter, like radix unmarshals it into a `uint64`: a missing value is 0,
    /// anything else must be an unsigned decimal.
    fn counter(&self, reply: Value) -> Result<u64, CacheError> {
        let text = match reply {
            Value::Null => return Ok(0),
            Value::Integer(i) => i.to_string(),
            Value::String(s) => s.to_string(),
            Value::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
            other => return Err(self.failed(format!("cannot read {other:?} as a counter"))),
        };
        if text.is_empty() {
            return Err(self.failed("empty slice given to parseUint"));
        }
        let mut n: u64 = 0;
        for (i, c) in text.bytes().enumerate() {
            if !c.is_ascii_digit() {
                return Err(self.failed(format!(
                    "invalid character {} at position {i} in parseUint",
                    c as char
                )));
            }
            n = n.wrapping_mul(10).wrapping_add(u64::from(c - b'0'));
        }
        Ok(n)
    }
}

/// A descriptor's limit, to be checked against Redis or the local cache.
struct Counted<'a> {
    /// The descriptor's index in the request.
    index: usize,
    /// The limit's index in the descriptor's limits.
    slot: usize,
    limit: &'a RateLimit,
    stats: RateLimitStats,
    key: String,
    hits: Hits,
    /// Already over its limit according to the local cache: Redis is skipped.
    over_in_local_cache: bool,
    /// With `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`: these hits will take it over the limit.
    going_over: bool,
    /// A `rate_limits:` entry, which `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` leaves alone
    /// (D20).
    listed: bool,
    /// The pipeline it goes in: 0 the main one, 1 the per-second one (`REDIS_PERSECOND`).
    pipeline: usize,
}

impl RateLimitCache for RedisCache {
    async fn lookup(
        &self,
        request: &RateLimitRequest,
        limits: &[Vec<RateLimit>],
    ) -> Result<(i64, Vec<Vec<LimitReading>>), CacheError> {
        let now = self.clock.unix_now();
        Ok((now, self.read_limits(request, limits, now).await?))
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
        for (index, slot, limit) in limiter::slots(limits) {
            let key = limiter::cache_key(
                &self.settings.cache_key_prefix,
                &request.domain,
                &request.descriptors[index],
                limit,
                now,
                self.settings.use_calendar_month,
            )?;
            let hits = hits[index];
            let stats = self.stats.rate_limit(&limit.full_key);
            // A concurrency window's hits are slots, counted when they are decided.
            let concurrency = matches!(limit.window, Some(Window::Concurrency(_)));
            if !concurrency {
                limiter::count_hits(&stats, hits);
            }
            // Refunds always reach Redis; the local cache does not hold anchored or concurrency
            // windows.
            let anchored = matches!(limit.window, Some(Window::Anchored(_)));
            let over_in_local_cache = !hits.refund
                && !anchored
                && !concurrency
                && self
                    .local_cache
                    .as_ref()
                    .is_some_and(|cache| cache.contains(&key, now));
            if over_in_local_cache {
                match limit.shadow_mode {
                    true => crate::debug!(
                        "Cache key {key} would be rate limited but shadow mode is enabled on this rule"
                    ),
                    false => crate::debug!("cache key is over the limit: {key}"),
                }
            }
            checks.push(Counted {
                index,
                slot,
                limit,
                stats,
                key,
                hits,
                over_in_local_cache,
                going_over: false,
                listed: limit.window.is_some(),
                pipeline: self.pipeline_of(&limits[index]),
            });
        }

        let mut state = if checks.iter().any(|c| c.over_in_local_cache && !c.listed) {
            RequestState::OverInLocalCache
        } else {
            RequestState::WithinLimits
        };
        let stop_increment = self.settings.stop_cache_key_increment_when_overlimit;
        if stop_increment && state == RequestState::WithinLimits {
            self.find_keys_going_over(&mut checks).await?;
            if checks.iter().any(|c| c.going_over) {
                state = RequestState::SomeGoingOver;
            }
        }

        // Per descriptor not answered by the local cache: INCRBY + EXPIRE, or the refund
        // script. Hits go over the wire as signed 64-bit integers, as upstream sends them.
        let mut pipelines: [Vec<Command>; 2] = [Vec::new(), Vec::new()];
        let mut reply_of = Vec::new();
        for c in &checks {
            if c.over_in_local_cache {
                continue;
            }
            // A check waits for the request's decision (D35).
            if matches!(c.limit.window, Some(Window::Concurrency(_))) && slot_pending(c.hits) {
                continue;
            }
            crate::debug!("looking up cache key: {}", c.key);
            let commands = &mut pipelines[c.pipeline];
            reply_of.push((c.index, c.slot, c.pipeline, commands.len()));
            if let Some(Window::Concurrency(window)) = &c.limit.window {
                commands.push(release_command(window, &c.key, c.hits.value, now));
                continue;
            }
            if let Some(Window::TokenBucket(window)) = &c.limit.window {
                let requests = c.limit.limit.requests_per_unit;
                commands.push(bucket_command(window, &c.key, c.hits, now, requests));
                continue;
            }
            // An anchored window's key expires with the window, which the script knows.
            if let Some(Window::Anchored(window)) = &c.limit.window {
                commands.push(anchored_command(window, &c.key, c.hits, now));
                continue;
            }
            let calendar = self.settings.use_calendar_month;
            let mut expiration = limiter::expiration(c.limit, now, calendar)?;
            let jitter = self.settings.expiration_jitter_max_seconds;
            if jitter > 0 {
                // Go's int64 addition wraps.
                expiration = expiration.wrapping_add(crate::random::below(jitter as u64) as i64);
            }
            let expiration = expiration.to_string();
            if c.hits.refund {
                let amount = (c.hits.value as i64).to_string();
                let args = vec![
                    DECREMENT_SCRIPT.to_string(),
                    "1".to_string(),
                    c.key.clone(),
                    amount,
                    expiration,
                ];
                commands.push(Command { name: "EVAL", args });
            } else {
                let counted = match c.listed {
                    true => c.hits.value,
                    false => limiter::hits_to_count(c.hits, stop_increment, state, c.going_over),
                };
                let args = vec![c.key.clone(), (counted as i64).to_string()];
                commands.push(Command {
                    name: "INCRBY",
                    args,
                });
                commands.push(Command {
                    name: "EXPIRE",
                    args: vec![c.key.clone(), expiration],
                });
            }
        }
        // Until the statuses are built.
        let _span = crate::trace::start(
            crate::trace::REDIS,
            "Redis Pipeline Execution",
            crate::trace::Kind::Internal,
            crate::trace::parent().as_ref(),
            || {
                vec![
                    (
                        "pipeline length".into(),
                        crate::trace::Value::Int(pipelines[0].len() as i64),
                    ),
                    (
                        "perSecondPipeline length".into(),
                        crate::trace::Value::Int(pipelines[1].len() as i64),
                    ),
                ]
            },
        );
        let mut counts: Vec<Vec<Count>> = limits
            .iter()
            .map(|list| vec![Count::After(0); list.len()])
            .collect();
        // The main pipeline, then the per-second one.
        let mut replies: [Vec<_>; 2] = [Vec::new(), Vec::new()];
        for (pipeline, pool) in self.pools().into_iter().enumerate() {
            if let (Some(pool), false) = (pool, pipelines[pipeline].is_empty()) {
                replies[pipeline] = send(pool.next()?, &pipelines[pipeline]).await?;
            }
        }
        // An anchored window's reset, from its state, and whether this request opened it
        // without hits.
        let mut resets: Vec<Vec<Option<i64>>> =
            limits.iter().map(|list| vec![None; list.len()]).collect();
        let mut opened_without_hits: Vec<Vec<bool>> =
            limits.iter().map(|list| vec![false; list.len()]).collect();
        let mut buckets: Vec<Vec<Option<BucketResult>>> =
            limits.iter().map(|list| vec![None; list.len()]).collect();
        for (index, slot, pipeline, at) in reply_of {
            let reply = replies[pipeline][at].clone();
            let command = &pipelines[pipeline][at];
            if let Some(Window::Concurrency(_)) = &limits[index][slot].window {
                let result = command.slots(reply)?;
                count_slots(&self.stats, &limits[index][slot], result, hits[index].value);
                counts[index][slot] = Count::After(result.in_flight);
                continue;
            }
            if let Some(Window::TokenBucket(_)) = &limits[index][slot].window {
                buckets[index][slot] = Some(command.bucket(reply)?);
                continue;
            }
            let Some(Window::Anchored(window)) = &limits[index][slot].window else {
                counts[index][slot] = Count::After(command.counter(reply)?);
                continue;
            };
            let (state, opened) = command.anchored(reply)?;
            counts[index][slot] = Count::After(state.map_or(0, |w| w.count));
            resets[index][slot] = Some(window.reset(state, now));
            opened_without_hits[index][slot] = opened && hits[index].value == 0;
        }
        for c in &checks {
            if c.over_in_local_cache {
                counts[c.index][c.slot] = Count::OverInLocalCache;
                // Only the entry knows when the bucket it holds admits again.
                if let (Some(Window::TokenBucket(_)), Some(cache)) =
                    (&c.limit.window, &self.local_cache)
                {
                    resets[c.index][c.slot] = Some(cache.ttl(&c.key, now).unwrap_or(1));
                }
            } else if let (false, Count::After(after), Some(cache)) =
                (c.hits.refund, counts[c.index][c.slot], &self.local_cache)
                && limiter::exceeds(c.limit, after)
                && !matches!(
                    c.limit.window,
                    Some(Window::Anchored(_) | Window::Concurrency(_) | Window::TokenBucket(_))
                )
            {
                // Cached for a whole window, shadow rules included, as upstream.
                let calendar = self.settings.use_calendar_month;
                let ttl = limiter::expiration(c.limit, now, calendar)?;
                if !cache.insert(&c.key, ttl, now) {
                    crate::log::error(&format!("Failing to set local cache key: {}", c.key));
                }
            }
        }

        let mut statuses: Vec<Vec<LimitOutcome>> = limits
            .iter()
            .map(|list| Vec::with_capacity(list.len()))
            .collect();
        for c in &checks {
            if let Some(Window::Concurrency(window)) = &c.limit.window {
                // A release is always OK; a pending check stands for a free slot until then.
                // The local cache never answers for it.
                let in_flight = match counts[c.index][c.slot] {
                    Count::After(in_flight) => in_flight,
                    Count::OverInLocalCache => 0,
                };
                statuses[c.index].push(LimitOutcome {
                    status: slots_status(c.limit, window, in_flight),
                    opened_without_hits: false,
                    slot_pending: slot_pending(c.hits),
                });
                continue;
            }
            if let (Some(Window::TokenBucket(window)), Some(result)) =
                (&c.limit.window, buckets[c.index][c.slot])
            {
                statuses[c.index].push(LimitOutcome {
                    status: self.bucket_status(c, window, result, now),
                    opened_without_hits: false,
                    slot_pending: false,
                });
                continue;
            }
            let reset = match resets[c.index][c.slot] {
                Some(reset) => reset,
                None => {
                    let calendar = self.settings.use_calendar_month;
                    limiter::reset(c.limit, &request.descriptors[c.index], now, calendar)?
                }
            };
            let count = counts[c.index][c.slot];
            let ratio = self.settings.near_limit_ratio;
            let status =
                limiter::limit_status((c.limit, &c.stats, &c.key), ratio, c.hits, count, reset);
            statuses[c.index].push(LimitOutcome {
                status,
                opened_without_hits: opened_without_hits[c.index][c.slot],
                slot_pending: false,
            });
        }
        Ok(statuses)
    }

    async fn check_slots(
        &self,
        request: &RateLimitRequest,
        limits: &[Vec<RateLimit>],
        checks: &[SlotCheck],
    ) -> Result<Vec<SlotAnswer>, CacheError> {
        let now = self.clock.unix_now();
        let mut commands = Vec::with_capacity(checks.len());
        let mut pending = Vec::with_capacity(checks.len());
        for check in checks {
            let limit = &limits[check.index][check.limit];
            let Some(Window::Concurrency(window)) = &limit.window else {
                return Err(CacheError("a slot for a limit without one".into()));
            };
            let key = limiter::cache_key(
                &self.settings.cache_key_prefix,
                &request.domain,
                &request.descriptors[check.index],
                limit,
                now,
                self.settings.use_calendar_month,
            )?;
            let member = check.acquire.then(new_member);
            commands.push(match &member {
                Some(member) => acquire_command(window, &key, limit, check.force, member, now),
                None => in_flight_command(window, &key, now),
            });
            pending.push((check, limit, window, key, member));
        }
        let replies = match send(self.pool.next()?, &commands).await {
            Ok(replies) => replies,
            Err(e) => {
                self.release_after_failure(&pending, now).await;
                return Err(e);
            }
        };
        let mut answers = Vec::with_capacity(checks.len());
        for (((check, limit, window, key, member), command), reply) in
            pending.into_iter().zip(&commands).zip(replies)
        {
            let Some(member) = member else {
                answers.push(SlotAnswer {
                    check: *check,
                    status: slots_status(limit, window, command.counter(reply)?),
                    held: None,
                });
                continue;
            };
            let result = command.slots(reply)?;
            count_slots(&self.stats, limit, result, 0);
            let over = !result.acquired || limiter::exceeds(limit, result.in_flight);
            let stats = self.stats.rate_limit(&limit.full_key);
            stats.total_hits.add(1);
            match over {
                true => stats.over_limit.add(1),
                false => stats.within_limit.add(1),
            }
            if over && result.acquired {
                stats.shadow_mode.add(1);
            }
            let (code, left) = match over {
                true => (Code::OverLimit, 0),
                false => (Code::Ok, slots_left(limit, result.in_flight)),
            };
            answers.push(SlotAnswer {
                check: *check,
                status: limiter::status(code, limit, left, window.retry_after),
                held: result.acquired.then_some(HeldSlot { key, member }),
            });
        }
        Ok(answers)
    }

    async fn return_slots(
        &self,
        limits: &[Vec<RateLimit>],
        answers: &[SlotAnswer],
    ) -> Result<(), CacheError> {
        let now = self.clock.unix_now();
        let mut commands = Vec::new();
        let mut returned = Vec::new();
        for answer in answers {
            let limit = &limits[answer.check.index][answer.check.limit];
            let (Some(held), Some(Window::Concurrency(window))) = (&answer.held, &limit.window)
            else {
                continue;
            };
            commands.push(remove_command(window, &held.key, &held.member, now));
            returned.push(limit);
        }
        if commands.is_empty() {
            return Ok(());
        }
        let replies = send(self.pool.next()?, &commands).await?;
        for ((limit, command), reply) in returned.into_iter().zip(&commands).zip(replies) {
            count_slots(&self.stats, limit, command.slots(reply)?, 0);
        }
        Ok(())
    }
}

impl RedisCache {
    /// Releases what a failed pipeline of checks may have taken: its other commands still
    /// run, and its request gets an error, so no amendment will come. A slot it cannot release
    /// ends with its lease.
    async fn release_after_failure(&self, pending: &[PendingSlot<'_>], now: i64) {
        let commands: Vec<Command> = pending
            .iter()
            .filter_map(|(_, _, window, key, member)| {
                Some(remove_command(window, key, member.as_ref()?, now))
            })
            .collect();
        if commands.is_empty() {
            return;
        }
        let released = match self.pool.next() {
            Ok(pool) => send(pool, &commands).await.map(|_| ()),
            Err(e) => Err(e),
        };
        if let Err(e) = released {
            crate::log::error(&format!("Failing to release a slot: {}", e.0));
        }
    }
}

/// A slot being checked: its check, limit and window, its key, and the member it would take.
type PendingSlot<'a> = (
    &'a SlotCheck,
    &'a RateLimit,
    &'a ConcurrencyWindow,
    String,
    Option<String>,
);

impl RedisCache {
    /// A token bucket's status (D11): a bucket that is not empty admits; a refund is always OK.
    /// Its outcome counted (D22), and a denied bucket kept in the local cache until it would
    /// admit, a refund clearing it (D18).
    fn bucket_status(
        &self,
        c: &Counted<'_>,
        window: &TokenBucketWindow,
        result: BucketResult,
        now: i64,
    ) -> DescriptorStatus {
        let requests = c.limit.limit.requests_per_unit;
        let admitted = c.hits.refund || window.admits(result.before, now, requests);
        if c.hits.refund {
            if let Some(cache) = &self.local_cache {
                cache.remove(&c.key);
            }
        } else {
            count_bucket(
                c,
                window,
                result,
                admitted,
                now,
                self.settings.near_limit_ratio,
            );
        }
        if result.capped > 0 {
            self.stats
                .rate_limit_counter(&c.limit.full_key, "total_capped_hits")
                .add(result.capped);
        }
        if !admitted {
            let wait = window.until_admitted(result.after, now, requests);
            // freecache's expiry is 32 bits: a wait past it is not worth an entry.
            let shadow = c.limit.shadow_mode || self.settings.global_shadow_mode;
            if let (false, true, Some(cache)) = (shadow, wait < 1 << 31, &self.local_cache)
                && !cache.insert(&c.key, wait, now)
            {
                crate::log::error(&format!("Failing to set local cache key: {}", c.key));
            }
            return limiter::status(Code::OverLimit, c.limit, 0, wait);
        }
        limiter::status(
            Code::Ok,
            c.limit,
            window.remaining(result.after, now, requests),
            window.until_full(result.after, now),
        )
    }
}

/// Splits a token bucket's hits between its counters with legacy arithmetic (D22), usage being
/// [`TokenBucketWindow::used`]. Unlike the legacy, what is past the capacity may still be
/// admitted: `within_limit` is then every hit, as it is what `OK` means.
fn count_bucket(
    c: &Counted<'_>,
    window: &TokenBucketWindow,
    result: BucketResult,
    admitted: bool,
    now: i64,
    near_limit_ratio: f32,
) {
    let (stats, hits) = (&c.stats, c.hits.value);
    let requests = c.limit.limit.requests_per_unit;
    let capacity = window.capacity(requests);
    let before = window.used(result.before, now, requests);
    let after = window.used(result.after, now, requests);
    let near = f64::from(capacity as f32 * near_limit_ratio).floor() as u64;
    if admitted {
        stats.within_limit.add(hits);
    }
    if !admitted || after > capacity {
        let over = if before >= capacity {
            hits
        } else {
            (after - capacity).min(hits)
        };
        stats.over_limit.add(over);
        if before < capacity {
            stats.near_limit.add(capacity - near.max(before));
        }
        if c.limit.shadow_mode {
            stats.shadow_mode.add(over);
        }
    } else if after > near {
        stats
            .near_limit
            .add(if before >= near { hits } else { after - near });
    }
}

/// The token bucket script's call.
fn bucket_command(
    window: &TokenBucketWindow,
    key: &str,
    hits: Hits,
    now: i64,
    requests_per_unit: u32,
) -> Command {
    let limit = window
        .deadline_limit(requests_per_unit)
        .map_or("-1".to_string(), |limit| limit.to_string());
    let args = vec![
        TOKEN_BUCKET_SCRIPT.to_string(),
        "1".to_string(),
        key.to_string(),
        (now as f64 * 1e6).to_string(),
        window.interval(requests_per_unit).to_string(),
        limit,
        MAX_TAT.to_string(),
        (hits.value as f64).to_string(),
        if hits.refund { "1" } else { "0" }.to_string(),
        TOKEN_BUCKET_MARGIN_SECONDS.to_string(),
    ];
    Command { name: "EVAL", args }
}

/// An OK status with `in_flight` slots held.
fn slots_status(limit: &RateLimit, window: &ConcurrencyWindow, in_flight: u64) -> DescriptorStatus {
    limiter::status(
        Code::Ok,
        limit,
        slots_left(limit, in_flight),
        window.retry_after,
    )
}

/// Whether these hits are a check taking a slot, rather than a release (D35).
fn slot_pending(hits: Hits) -> bool {
    hits.value == 0 && !hits.refund
}

/// What is left of `limit` with `in_flight` slots held.
fn slots_left(limit: &RateLimit, in_flight: u64) -> u32 {
    u64::from(limit.limit.requests_per_unit).saturating_sub(in_flight) as u32
}

/// Counts what a concurrency script did: `asked` slots were to be released.
fn count_slots(stats: &Stats, limit: &RateLimit, result: SlotResult, asked: u64) {
    let key = &limit.full_key;
    let add = |name: &str, count: u64| {
        if count > 0 {
            stats.rate_limit_counter(key, name).add(count);
        }
    };
    add("slots_acquired", u64::from(result.acquired));
    add("slots_released", result.released);
    add("slots_expired", result.expired);
    add(
        "releases_without_slot",
        asked.saturating_sub(result.released),
    );
}

/// A slot's member, unique across instances.
fn new_member() -> String {
    let mut bytes = [0u8; 16];
    crate::random::fill(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The script call with the arguments every concurrency operation starts with.
fn concurrency_command(window: &ConcurrencyWindow, key: &str, now: i64, op: &[&str]) -> Command {
    let ttl = window.lease + CONCURRENCY_MARGIN_SECONDS;
    let mut args = vec![
        CONCURRENCY_SCRIPT.to_string(),
        "1".to_string(),
        key.to_string(),
        now.to_string(),
        window.lease.to_string(),
        ttl.to_string(),
    ];
    args.extend(op.iter().map(|part| part.to_string()));
    Command { name: "EVAL", args }
}

fn acquire_command(
    window: &ConcurrencyWindow,
    key: &str,
    limit: &RateLimit,
    force: bool,
    member: &str,
    now: i64,
) -> Command {
    let max = limit.limit.requests_per_unit.to_string();
    let force = if force { "1" } else { "0" };
    concurrency_command(window, key, now, &["acquire", &max, force, member])
}

fn release_command(window: &ConcurrencyWindow, key: &str, count: u64, now: i64) -> Command {
    // Sent as a signed 64-bit integer, as hits are.
    let count = count.min(i64::MAX as u64).to_string();
    concurrency_command(window, key, now, &["release", &count])
}

fn remove_command(window: &ConcurrencyWindow, key: &str, member: &str, now: i64) -> Command {
    concurrency_command(window, key, now, &["remove", member])
}

/// The slots in flight at `now`, read without changing anything.
fn in_flight_command(window: &ConcurrencyWindow, key: &str, now: i64) -> Command {
    Command {
        name: "ZCOUNT",
        args: vec![
            key.to_string(),
            format!("({}", now - window.lease),
            "+inf".to_string(),
        ],
    }
}

impl RedisCache {
    /// The quota lookup's reads (ADR 0003): each limit's key at `now`, as a decision would
    /// find it, with `GET` or, for an anchored window, `HMGET` of its anchor and count; on
    /// the pool the decision would use. Nothing is written.
    async fn read_limits(
        &self,
        request: &RateLimitRequest,
        limits: &[Vec<RateLimit>],
        now: i64,
    ) -> Result<Vec<Vec<LimitReading>>, CacheError> {
        let mut pipelines: [Vec<Command>; 2] = [Vec::new(), Vec::new()];
        let mut reply_of = Vec::new();
        for (index, slot, limit) in limiter::slots(limits) {
            let key = limiter::cache_key(
                &self.settings.cache_key_prefix,
                &request.domain,
                &request.descriptors[index],
                limit,
                now,
                self.settings.use_calendar_month,
            )?;
            let pipeline = self.pipeline_of(&limits[index]);
            let command = match &limit.window {
                Some(Window::Anchored(_)) => Command {
                    name: "HMGET",
                    args: vec![key, "a".into(), "c".into()],
                },
                Some(Window::Concurrency(window)) => in_flight_command(window, &key, now),
                _ => Command {
                    name: "GET",
                    args: vec![key],
                },
            };
            reply_of.push((index, slot, pipeline, pipelines[pipeline].len()));
            pipelines[pipeline].push(command);
        }
        let mut replies: [Vec<_>; 2] = [Vec::new(), Vec::new()];
        for (pipeline, pool) in self.pools().into_iter().enumerate() {
            if let (Some(pool), false) = (pool, pipelines[pipeline].is_empty()) {
                replies[pipeline] = send(pool.next()?, &pipelines[pipeline]).await?;
            }
        }
        let mut readings: Vec<Vec<LimitReading>> = limits
            .iter()
            .map(|list| Vec::with_capacity(list.len()))
            .collect();
        for (index, slot, pipeline, at) in reply_of {
            let (command, reply) = (&pipelines[pipeline][at], replies[pipeline][at].clone());
            let limit = &limits[index][slot];
            let reading = match &limit.window {
                Some(Window::Anchored(window)) => {
                    // Closed as a decision closes it, whether or not the key expired yet.
                    let open = window.open(command.stored_anchored(reply)?, now);
                    LimitReading {
                        used: open.map_or(0, |w| w.count),
                        reset: open.map(|w| window.reset(Some(w), now)),
                    }
                }
                Some(Window::TokenBucket(window)) => {
                    // No key is a full bucket.
                    let stored = command.counter(reply)? as f64;
                    LimitReading {
                        used: window.used(stored, now, limit.limit.requests_per_unit),
                        reset: Some(window.until_full(stored, now)),
                    }
                }
                // Slots have no window to reset.
                Some(Window::Concurrency(_)) => LimitReading {
                    used: command.counter(reply)?,
                    reset: None,
                },
                _ => {
                    let calendar = self.settings.use_calendar_month;
                    let descriptor = &request.descriptors[index];
                    LimitReading {
                        used: command.counter(reply)?,
                        reset: Some(limiter::reset(limit, descriptor, now, calendar)?),
                    }
                }
            };
            readings[index].push(reading);
        }
        Ok(readings)
    }
}

/// The script call counting `hits` in an anchored window's `key` at `now`.
fn anchored_command(window: &AnchoredWindow, key: &str, hits: Hits, now: i64) -> Command {
    let flag = |on: bool| if on { "1" } else { "0" }.to_string();
    let on_check = window.open_on == OpenOn::Check;
    let args = vec![
        ANCHORED_SCRIPT.to_string(),
        "1".to_string(),
        key.to_string(),
        now.to_string(),
        window.duration.to_string(),
        window.rounding.unwrap_or(0).to_string(),
        hits.value.min(MAX_ANCHORED_COUNT).to_string(),
        flag(on_check),
        flag(hits.refund),
        ANCHORED_MARGIN_SECONDS.to_string(),
        MAX_ANCHORED_COUNT.to_string(),
    ];
    Command { name: "EVAL", args }
}

impl Command {
    /// The token bucket script's reply, whose deadlines are strings: past 2^53 they would not
    /// be exact integers, and Lua would truncate them.
    fn bucket(&self, reply: Value) -> Result<BucketResult, CacheError> {
        let malformed = || self.failed("a token bucket's reply is not 3 numbers");
        let Value::Array(values) = reply else {
            return Err(malformed());
        };
        let numbers: Vec<f64> = values
            .iter()
            .filter_map(|value| value.as_str()?.parse().ok())
            .collect();
        let [before, after, capped] = numbers[..] else {
            return Err(malformed());
        };
        Ok(BucketResult {
            before,
            after,
            capped: capped as u64,
        })
    }

    /// The concurrency script's reply.
    fn slots(&self, reply: Value) -> Result<SlotResult, CacheError> {
        let malformed = || self.failed("a concurrency window's reply is not 4 integers");
        let Value::Array(values) = reply else {
            return Err(malformed());
        };
        let numbers: Vec<u64> = values
            .iter()
            .filter_map(Value::as_i64)
            .filter_map(|n| u64::try_from(n).ok())
            .collect();
        let [acquired, released, expired, in_flight] = numbers[..] else {
            return Err(malformed());
        };
        Ok(SlotResult {
            acquired: acquired == 1,
            released,
            expired,
            in_flight,
        })
    }

    /// An anchored window's stored state, from `HMGET key a c`: none without an anchor.
    fn stored_anchored(&self, reply: Value) -> Result<Option<Anchored>, CacheError> {
        let malformed = || self.failed("an anchored window's state is not 2 integers");
        let Value::Array(values) = reply else {
            return Err(malformed());
        };
        let [anchor, count] = &values[..] else {
            return Err(malformed());
        };
        let number = |value: &Value| match value {
            Value::Null => Ok(None),
            other => other
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .map(Some)
                .ok_or_else(malformed),
        };
        let Some(anchor) = number(anchor)? else {
            return Ok(None);
        };
        // A count the script cannot read, it reads as 0.
        let count = number(count)
            .ok()
            .flatten()
            .and_then(|c| u64::try_from(c).ok());
        Ok(Some(Anchored {
            anchor,
            count: count.unwrap_or(0),
        }))
    }

    /// The anchored script's reply: the open window, if any, and whether it just opened.
    fn anchored(&self, reply: Value) -> Result<(Option<Anchored>, bool), CacheError> {
        let malformed = || self.failed("an anchored window's reply is not 3 integers");
        let Value::Array(values) = reply else {
            return Err(malformed());
        };
        let numbers: Vec<i64> = values.iter().filter_map(Value::as_i64).collect();
        let [anchor, count, opened] = numbers[..] else {
            return Err(malformed());
        };
        // A negative count is no count the script writes.
        let count = u64::try_from(count).map_err(|_| malformed())?;
        let window = (anchor >= 0).then_some(Anchored { anchor, count });
        Ok((window, opened == 1))
    }
}

impl RedisCache {
    /// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`: reads the counters first to mark the keys
    /// these hits will take over the limit.
    async fn find_keys_going_over(&self, checks: &mut [Counted<'_>]) -> Result<(), CacheError> {
        // The main pipeline's reads, then the per-second one's.
        for (pipeline, pool) in self.pools().into_iter().enumerate() {
            let Some(pool) = pool else { continue };
            let mut reads: Vec<&mut Counted> = checks
                .iter_mut()
                .filter(|c| !c.hits.refund && !c.listed && c.pipeline == pipeline)
                .collect();
            if reads.is_empty() {
                continue;
            }
            let commands: Vec<Command> = reads
                .iter()
                .map(|c| Command {
                    name: "GET",
                    args: vec![c.key.clone()],
                })
                .collect();
            let replies = send(pool.next()?, &commands).await?;
            for ((c, command), reply) in reads.iter_mut().zip(&commands).zip(replies) {
                let before = command.counter(reply)?;
                c.going_over = limiter::exceeds(c.limit, before.wrapping_add(c.hits.value));
            }
        }
        Ok(())
    }
}
