//! The windows of `rate_limits:` entries: `fixed` (docs/design.md D8, ADR 0002), of any
//! duration, starting at the epoch, at a constant offset, or at a phase hashed from the
//! descriptor; `anchored` (D9), opened by a request and lasting its duration from then.

use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::rate_limit::Unit;

/// A list limit's window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    /// `type: fixed`.
    Fixed(FixedWindow),
    /// `type: anchored`.
    Anchored(AnchoredWindow),
    /// `type: concurrency`.
    Concurrency(ConcurrencyWindow),
    /// `type: token_bucket`.
    TokenBucket(TokenBucketWindow),
}

impl Window {
    /// Its length, in seconds.
    pub fn duration(&self) -> i64 {
        match self {
            Window::Fixed(w) => w.duration,
            Window::Anchored(w) => w.duration,
            Window::Concurrency(w) => w.lease,
            Window::TokenBucket(w) => w.duration,
        }
    }

    /// The window's part of the storage key (ADR 0001).
    pub fn shape(&self) -> String {
        match self {
            Window::Fixed(w) => w.shape(),
            Window::Anchored(w) => w.shape(),
            Window::Concurrency(_) => "cc".to_string(),
            Window::TokenBucket(w) => format!("tb{}", w.duration),
        }
    }

    /// The window as `/rlconfig` shows it.
    pub fn describe(&self) -> String {
        match self {
            Window::Fixed(w) => w.describe(),
            Window::Anchored(w) => w.describe(),
            Window::Concurrency(w) => w.describe(),
            Window::TokenBucket(w) => w.describe(),
        }
    }

    /// The unit the window is reported in, the largest whose length divides the duration,
    /// and how many of them it lasts (D13).
    pub fn unit(&self) -> (Unit, u32) {
        let duration = self.duration();
        let (unit, length) = UNITS
            .into_iter()
            .find(|(_, length)| duration % length == 0)
            .expect("a whole number of seconds");
        (unit, (duration / length) as u32)
    }

    /// A client-sent limit's window of `multiplier` units, from the epoch (D23); `None` if
    /// it has no such window: no unit, a multiplier of 0, or longer than a window may be.
    pub(super) fn of_units(unit: Unit, multiplier: u32) -> Option<Self> {
        let (_, length) = UNITS.into_iter().find(|(u, _)| *u == unit)?;
        let duration = length * i64::from(multiplier);
        (1..=MAX_DURATION)
            .contains(&duration)
            .then_some(Window::Fixed(FixedWindow {
                duration,
                origin: Origin::Epoch,
            }))
    }
}

/// The units, longest first, and their lengths in seconds.
const UNITS: [(Unit, i64); 7] = [
    (Unit::Year, 365 * 86_400),
    (Unit::Month, 30 * 86_400),
    (Unit::Week, 7 * 86_400),
    (Unit::Day, 86_400),
    (Unit::Hour, 3_600),
    (Unit::Minute, 60),
    (Unit::Second, 1),
];

/// A `fixed` window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedWindow {
    /// Its length, in seconds.
    pub duration: i64,
    /// Where its windows start.
    pub origin: Origin,
}

/// Where a fixed window's windows start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// At multiples of the duration since the epoch.
    Epoch,
    /// `offset: <duration>`: that many seconds after them, `0 < offset < duration`.
    Offset(i64),
    /// `offset: hashed`: at a phase of the descriptor's, a multiple of `rounding`.
    Hashed { rounding: i64 },
}

impl FixedWindow {
    /// The phase of the windows for a descriptor whose entries are `entries` (`key`, `value`,
    /// after the `share_threshold` substitution).
    pub fn phase<'a>(&self, entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> i64 {
        match self.origin {
            Origin::Epoch => 0,
            Origin::Offset(offset) => offset,
            Origin::Hashed { rounding } => {
                let steps = (self.duration / rounding) as u64;
                (hash(entries) % steps) as i64 * rounding
            }
        }
    }

    /// The start of the window containing `now`.
    pub fn start(&self, now: i64, phase: i64) -> i64 {
        now - (now - phase).rem_euclid(self.duration)
    }

    /// The seconds until the window containing `now` ends (D12).
    pub fn reset(&self, now: i64, phase: i64) -> i64 {
        self.duration - (now - phase).rem_euclid(self.duration)
    }

    /// The window's part of the storage key (ADR 0001): `fx<duration>`, then `o<offset>` or
    /// `oh<rounding>`.
    pub fn shape(&self) -> String {
        match self.origin {
            Origin::Epoch => format!("fx{}", self.duration),
            Origin::Offset(offset) => format!("fx{}o{offset}", self.duration),
            Origin::Hashed { rounding } => format!("fx{}oh{rounding}", self.duration),
        }
    }

    /// The window as `/rlconfig` shows it: `fixed <duration>s`, then its offset.
    pub fn describe(&self) -> String {
        match self.origin {
            Origin::Epoch => format!("fixed {}s", self.duration),
            Origin::Offset(offset) => format!("fixed {}s offset={offset}s", self.duration),
            Origin::Hashed { rounding } => {
                format!("fixed {}s offset=hashed/{rounding}s", self.duration)
            }
        }
    }
}

/// An `anchored` window (D9): opened by a request while none is, it lasts its duration from
/// its anchor, never moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchoredWindow {
    /// Its length, in seconds.
    pub duration: i64,
    /// `start_rounding`: the anchor is rounded down to a multiple of it (UTC).
    pub rounding: Option<i64>,
    /// `open_on`: what opens it.
    pub open_on: OpenOn,
}

/// What opens an anchored window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenOn {
    /// `check`: any request presented, whatever its hits (0 included) and verdict.
    Check,
    /// `first_hit`: a request whose hits are counted.
    FirstHit,
}

/// The most hits an anchored window counts: 2^53, the largest integer the Redis script's
/// numbers (doubles) hold exactly. Hits and counts beyond saturate there.
pub const MAX_ANCHORED_COUNT: u64 = 1 << 53;

/// An anchored window's state: its anchor and the hits counted since.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Anchored {
    /// When it opened, rounded, in Unix seconds.
    pub anchor: i64,
    /// The hits counted since, refunds taken back.
    pub count: u64,
}

/// What a request does to an anchored window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchoredOp {
    /// The request's time, in Unix seconds.
    pub now: i64,
    /// Its hits for the limit.
    pub hits: u64,
    /// Whether it takes them back (`is_negative_hits`).
    pub refund: bool,
}

/// What [`AnchoredWindow::step`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchoredResult {
    /// What is stored after the request: the last window opened, closed or not (a clock
    /// going back can find it open again).
    pub state: Option<Anchored>,
    /// The open window after the request, if any.
    pub window: Option<Anchored>,
    /// Whether the request opened it.
    pub opened: bool,
}

impl AnchoredWindow {
    /// The window's part of the storage key (ADR 0001): `an<duration>`, then `r<rounding>`,
    /// then `f` when the first hit opens it.
    pub fn shape(&self) -> String {
        let rounding = self.rounding.map_or(String::new(), |r| format!("r{r}"));
        let first_hit = match self.open_on {
            OpenOn::Check => "",
            OpenOn::FirstHit => "f",
        };
        format!("an{}{rounding}{first_hit}", self.duration)
    }

    /// The window as `/rlconfig` shows it.
    pub fn describe(&self) -> String {
        let rounding = self
            .rounding
            .map_or(String::new(), |r| format!(" start_rounding={r}s"));
        let open_on = match self.open_on {
            OpenOn::Check => "check",
            OpenOn::FirstHit => "first_hit",
        };
        format!("anchored {}s{rounding} open_on={open_on}", self.duration)
    }

    /// The anchor a window opened at `now` gets.
    pub fn anchor(&self, now: i64) -> i64 {
        match self.rounding {
            Some(rounding) => now - now.rem_euclid(rounding),
            None => now,
        }
    }

    /// The stored window, if it is still open at `now`: until `now ≥ anchor + duration`, a
    /// clock behind the anchor counting as no time elapsed.
    pub fn open(&self, stored: Option<Anchored>, now: i64) -> Option<Anchored> {
        stored.filter(|w| now - w.anchor < self.duration)
    }

    /// The seconds until the window ends: the open one's, else one opened now's (D12); a
    /// clock behind the anchor counts as no time elapsed (D27).
    pub fn reset(&self, window: Option<Anchored>, now: i64) -> i64 {
        let anchor = window.map_or_else(|| self.anchor(now), |w| w.anchor);
        self.duration - (now - anchor).max(0)
    }

    /// The state after `op` (D9, D21): a window is open while `now − anchor < duration`, a
    /// clock before the anchor counting as no time elapsed; a request opens one while none
    /// is (with `first_hit`, only if it has hits); a refund never opens or closes one, and
    /// floors the count at 0. The Redis script ([`crate::redis`]) does the same.
    pub fn step(&self, state: Option<Anchored>, op: AnchoredOp) -> AnchoredResult {
        let open = self.open(state, op.now);
        let hits = op.hits.min(MAX_ANCHORED_COUNT);
        let changed = |window: Anchored, opened| AnchoredResult {
            state: Some(window),
            window: Some(window),
            opened,
        };
        match open {
            Some(w) if op.refund => changed(
                Anchored {
                    count: w.count.saturating_sub(hits),
                    ..w
                },
                false,
            ),
            Some(w) => changed(
                Anchored {
                    count: (w.count + hits).min(MAX_ANCHORED_COUNT),
                    ..w
                },
                false,
            ),
            None if op.refund || (self.open_on == OpenOn::FirstHit && hits == 0) => {
                AnchoredResult {
                    state,
                    window: None,
                    opened: false,
                }
            }
            None => changed(
                Anchored {
                    anchor: self.anchor(op.now),
                    count: hits,
                },
                true,
            ),
        }
    }
}

/// 64-bit FNV-1a over the entries as `key=value`, joined by NUL (ADR 0002).
fn hash<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> u64 {
    let mut input = Vec::new();
    for (i, (key, value)) in entries.into_iter().enumerate() {
        if i > 0 {
            input.push(0);
        }
        input.extend_from_slice(key.as_bytes());
        input.push(b'=');
        input.extend_from_slice(value.as_bytes());
    }
    fnv1a(&input)
}

/// 64-bit FNV-1a.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, b| {
        (hash ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// A `concurrency` window (D35, ADR 0004): a limit on requests in flight, each holding a slot
/// until it is released or its lease ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyWindow {
    /// `lease`: a slot this old is gone, in seconds.
    pub lease: i64,
    /// `retry_after`: `duration_until_reset` on every answer, in seconds.
    pub retry_after: i64,
}

/// What a request does to a concurrency window's slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotOp {
    /// Takes a slot if fewer than `max` are held, or anyway with `force` (shadow mode).
    Acquire { max: u64, force: bool },
    /// Gives back this many slots, the newest first.
    Release(u64),
    /// Gives back the slot acquired at this time: a denied request's.
    Remove(i64),
}

/// What an operation did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SlotResult {
    pub acquired: bool,
    pub released: u64,
    /// Slots gone because their lease ended.
    pub expired: u64,
    pub in_flight: u64,
}

impl ConcurrencyWindow {
    /// The state after `op` at `now`, `slots` being when each slot was acquired. The Redis
    /// script ([`crate::redis`]) does the same.
    pub fn step(&self, slots: &mut Vec<i64>, op: SlotOp, now: i64) -> SlotResult {
        let before = slots.len();
        slots.retain(|acquired| *acquired > now - self.lease);
        let mut result = SlotResult {
            expired: (before - slots.len()) as u64,
            ..Default::default()
        };
        match op {
            SlotOp::Acquire { max, force } => {
                if force || (slots.len() as u64) < max {
                    slots.push(now);
                    result.acquired = true;
                }
            }
            SlotOp::Release(count) => {
                slots.sort_unstable();
                let released = (slots.len() as u64).min(count);
                slots.truncate(slots.len() - released as usize);
                result.released = released;
            }
            SlotOp::Remove(acquired) => {
                if let Some(at) = slots.iter().position(|slot| *slot == acquired) {
                    slots.swap_remove(at);
                    result.released = 1;
                }
            }
        }
        result.in_flight = slots.len() as u64;
        result
    }

    /// The window as `/rlconfig` shows it.
    pub fn describe(&self) -> String {
        format!(
            "concurrency lease={}s retry_after={}s",
            self.lease, self.retry_after
        )
    }
}

/// A `token_bucket` window (D11): GCRA, a single deadline (the TAT, in microseconds) at which
/// the bucket is full. It refills at `requests_per_unit` tokens per `duration`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenBucketWindow {
    /// Its length, in seconds.
    pub duration: i64,
    /// `burst`: its capacity, by default `requests_per_unit`.
    pub burst: Option<u64>,
    /// `max_debt`: how far below empty it can go, by default a whole `burst`.
    pub max_debt: Option<MaxDebt>,
}

/// A token bucket's `max_debt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxDebt {
    Tokens(u64),
    /// What the bucket refills in this many seconds.
    Seconds(i64),
    Unlimited,
}

/// The largest deadline, in microseconds: a double is exact up to here (the year 2255).
pub const MAX_TAT: f64 = 9_007_199_254_740_992.0;

/// What a request does to a token bucket.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BucketOp {
    /// In seconds.
    pub now: i64,
    pub hits: u64,
    pub refund: bool,
}

/// What an operation found and left: the deadlines in microseconds, and the hits past
/// `max_debt`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BucketResult {
    pub before: f64,
    pub after: f64,
    pub capped: u64,
}

impl TokenBucketWindow {
    /// How many microseconds a token takes to refill.
    pub fn interval(&self, requests_per_unit: u32) -> f64 {
        self.duration as f64 * 1e6 / f64::from(requests_per_unit)
    }

    /// Its capacity, in tokens.
    pub fn capacity(&self, requests_per_unit: u32) -> u64 {
        self.burst.unwrap_or(u64::from(requests_per_unit))
    }

    /// How far past `now` the deadline may go, in microseconds: `None` without a limit.
    pub fn deadline_limit(&self, requests_per_unit: u32) -> Option<f64> {
        let capacity = self.capacity(requests_per_unit) as f64;
        let interval = self.interval(requests_per_unit);
        let debt = match self.max_debt {
            None => capacity,
            Some(MaxDebt::Tokens(tokens)) => tokens as f64,
            Some(MaxDebt::Seconds(seconds)) => seconds as f64 * 1e6 / interval,
            Some(MaxDebt::Unlimited) => return None,
        };
        Some((capacity + debt) * interval)
    }

    /// The state after `op`, `tat` being the stored deadline. The Redis script
    /// ([`crate::redis`]) does the same. A deadline is whole microseconds, so past a million
    /// tokens a second, a hit costs more than its token.
    pub fn step(&self, tat: Option<f64>, op: BucketOp, requests_per_unit: u32) -> BucketResult {
        let now = op.now as f64 * 1e6;
        let interval = self.interval(requests_per_unit);
        let before = tat.unwrap_or(now).max(now);
        let hits = op.hits as f64;
        let (after, capped) = if op.refund {
            (now.max(before - hits * interval), 0)
        } else {
            let after = before + hits * interval;
            let most = match self.deadline_limit(requests_per_unit) {
                Some(limit) => (now + limit).min(MAX_TAT),
                None => MAX_TAT,
            }
            // A charge never moves the deadline back.
            .max(before);
            match after > most {
                true => (most, hits.min(((after - most) / interval).ceil()) as u64),
                false => (after, 0),
            }
        };
        BucketResult {
            before,
            after: after.ceil(),
            capped,
        }
    }

    /// The tokens in the bucket with this deadline at `now`: negative in debt.
    pub fn level(&self, tat: f64, now: i64, requests_per_unit: u32) -> f64 {
        let used = (tat - now as f64 * 1e6).max(0.0) / self.interval(requests_per_unit);
        self.capacity(requests_per_unit) as f64 - used
    }

    /// Whether the bucket is not empty, which admits a request of any size (D11).
    pub fn admits(&self, tat: f64, now: i64, requests_per_unit: u32) -> bool {
        self.level(tat, now, requests_per_unit) > 0.0
    }

    /// Usage: the capacity minus the whole tokens left, past the capacity in debt (D22).
    pub fn used(&self, tat: f64, now: i64, requests_per_unit: u32) -> u64 {
        let level = self.level(tat, now, requests_per_unit).floor();
        (self.capacity(requests_per_unit) as f64 - level) as u64
    }

    /// The whole tokens left, never below 0.
    pub fn remaining(&self, tat: f64, now: i64, requests_per_unit: u32) -> u32 {
        self.level(tat, now, requests_per_unit)
            .floor()
            .clamp(0.0, f64::from(u32::MAX)) as u32
    }

    /// The seconds until the bucket is full, rounded up (D12).
    pub fn until_full(&self, tat: f64, now: i64) -> i64 {
        ((tat - now as f64 * 1e6).max(0.0) / 1e6).ceil() as i64
    }

    /// The seconds until a denied request would be admitted: strictly past the instant the
    /// level reaches 0, so at least 1 (D12).
    pub fn until_admitted(&self, tat: f64, now: i64, requests_per_unit: u32) -> i64 {
        let empty_until =
            tat - self.capacity(requests_per_unit) as f64 * self.interval(requests_per_unit);
        (((empty_until - now as f64 * 1e6) / 1e6).floor() as i64 + 1).max(1)
    }

    /// The window as `/rlconfig` shows it: `token_bucket <duration>s`, then its options.
    pub fn describe(&self) -> String {
        let burst = self.burst.map_or(String::new(), |b| format!(" burst={b}"));
        let max_debt = match self.max_debt {
            None => String::new(),
            Some(MaxDebt::Tokens(tokens)) => format!(" max_debt={tokens}"),
            Some(MaxDebt::Seconds(seconds)) => format!(" max_debt={seconds}s"),
            Some(MaxDebt::Unlimited) => " max_debt=unlimited".to_string(),
        };
        format!("token_bucket {}s{burst}{max_debt}", self.duration)
    }
}

/// The longest duration a window may have: the reported multiplier is a `uint32`.
const MAX_DURATION: i64 = u32::MAX as i64;

/// A duration as written in a window: whole numbers of `w`, `d`, `h`, `m` then `s`, each unit
/// at most once and in that order (`1h30m`), in seconds.
pub(super) fn parse_duration(text: &str) -> Option<i64> {
    if text.is_empty() {
        return None;
    }
    let mut total: i64 = 0;
    let mut rest = text;
    // The longest unit the next number may have.
    let mut longest = i64::MAX;
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return None;
        }
        let count: i64 = rest[..digits].parse().ok()?;
        let unit = match rest.as_bytes().get(digits) {
            Some(b'w') => 7 * 86_400,
            Some(b'd') => 86_400,
            Some(b'h') => 3_600,
            Some(b'm') => 60,
            Some(b's') => 1,
            _ => return None,
        };
        if unit >= longest {
            return None;
        }
        longest = unit;
        total = total.checked_add(count.checked_mul(unit)?)?;
        rest = &rest[digits + 1..];
    }
    (total <= MAX_DURATION).then_some(total)
}

/// `offset_rounding`'s default: the largest of an hour, a minute and a second that divides
/// the duration (ADR 0002).
pub(super) fn default_rounding(duration: i64) -> i64 {
    [3_600, 60]
        .into_iter()
        .find(|r| duration % r == 0)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FNV-1a 64's published test vectors.
    #[test]
    fn the_hash_is_fnv_1a_64() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
        assert_eq!(
            hash([("user", "alice"), ("org", "x")]),
            fnv1a(b"user=alice\0org=x")
        );
    }
}
