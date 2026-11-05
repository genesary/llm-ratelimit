//! Rate limit configuration: loading YAML config files and looking up the limit that applies
//! to a descriptor. A port of envoyproxy/ratelimit `src/config/config_impl.go`.

mod schema;
pub mod window;
mod xds;
pub(crate) mod yaml;

use rate_limit_response::rate_limit::Unit;
use ratelimit_proto::envoy::extensions::common::ratelimit::v3::RateLimitDescriptor;
use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response;
use schema::YamlDescriptor;
use std::collections::{BTreeMap, HashMap, HashSet};
use window::{
    AnchoredWindow, ConcurrencyWindow, FixedWindow, MaxDebt, OpenOn, Origin, TokenBucketWindow,
    Window,
};

/// A config file to load: its name (used in error messages) and its YAML content.
#[derive(Debug, Clone)]
pub struct ConfigFile {
    pub name: String,
    pub content: String,
}

#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// `MERGE_DOMAIN_CONFIG`: several files may contribute descriptors to the same domain.
    pub merge_domain_configs: bool,
    /// `SANITIZE_DESCRIPTOR_METRIC_DOTS`: `.` in descriptor keys and values becomes `_` in
    /// stats keys (never in matching).
    pub sanitize_descriptor_metric_dots: bool,
    /// `BACKEND_TYPE=memcache`: only `fixed` windows can be counted (D24).
    pub memcached: bool,
    /// `CACHE_KEY_PREFIX`, part of the keys whose length is warned about on Memcached.
    pub cache_key_prefix: String,
    /// `QUOTA_MODE`: every descriptor counts as `quota_mode` for what is warned about (D19).
    pub global_quota_mode: bool,
}

/// What a list key may take on Memcached without its window's start and the request's values,
/// which get the other 64 of its 250 bytes (ADR 0001).
const MEMCACHED_STATIC_KEY_BYTES: usize = 250 - 64;

/// A config error, formatted like envoyproxy/ratelimit's `RateLimitConfigError`: `<file>: <message>`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(String);

impl ConfigError {
    fn new(file: &str, message: impl std::fmt::Display) -> Self {
        Self(format!("{file}: {message}"))
    }
}

/// A configured limit, as found for a descriptor.
#[derive(Debug, Clone, PartialEq)]
pub struct RateLimit {
    /// The descriptor path the limit's stats are named after, e.g. `domain.key_value.subkey`.
    pub full_key: String,
    pub limit: rate_limit_response::RateLimit,
    pub name: String,
    pub unlimited: bool,
    /// `detailed_metric`: stats are named after the request's values, not the config's.
    pub detailed_metric: bool,
    pub shadow_mode: bool,
    pub quota_mode: bool,
    /// Names of limits this one replaces in the same request.
    pub replaces: Vec<String>,
    /// For each descriptor entry, the wildcard value (`files/*`) it matched with
    /// `share_threshold`, or empty; `None` when no entry matched that way.
    pub share_threshold_key_pattern: Option<Vec<String>>,
    pub metadata: Option<prost_types::Struct>,
    /// A `rate_limits:` entry's window; `None` for `rate_limit:`, whose window is its unit.
    pub window: Option<Window>,
}

impl RateLimit {
    fn new(full_key: String, requests_per_unit: u32, unit: i32, name: String) -> Self {
        Self {
            full_key,
            limit: rate_limit_response::RateLimit {
                name: name.clone(),
                requests_per_unit,
                unit,
                unit_multiplier: None,
            },
            name,
            unlimited: false,
            detailed_metric: false,
            shadow_mode: false,
            quota_mode: false,
            replaces: Vec::new(),
            share_threshold_key_pattern: None,
            metadata: None,
            window: None,
        }
    }
}

/// The result of looking up a descriptor.
#[derive(Debug, Clone, PartialEq)]
pub enum LimitLookup {
    /// The request's domain is not configured.
    UnknownDomain,
    /// The domain is configured but no limit applies to the descriptor.
    NoLimit,
    Limit(RateLimit),
    /// The descriptor's `rate_limits:`, in config order (never empty).
    List(Vec<RateLimit>),
}

#[derive(Debug, Default)]
struct Descriptor {
    descriptors: HashMap<String, Descriptor>,
    limit: Option<RateLimit>,
    /// `rate_limits:`, in config order; empty without.
    list: Vec<RateLimit>,
    /// The `key_value` patterns containing `*` among `descriptors`, in config order,
    /// pre-split on `*`.
    wildcards: Vec<(String, Vec<String>)>,
    value_to_metric: bool,
    /// With `share_threshold`, the descriptor's own `key_value` wildcard pattern.
    share_threshold_pattern: Option<String>,
}

/// A loaded set of config files.
#[derive(Debug)]
pub struct RateLimitConfig {
    domains: HashMap<String, Descriptor>,
    sanitize_dots: bool,
    global_quota_mode: bool,
}

/// Loads and validates config files, failing on the first invalid one.
pub fn load(files: &[ConfigFile], options: &LoadOptions) -> Result<RateLimitConfig, ConfigError> {
    // Like upstream, every file is parsed and key-validated before any domain is loaded.
    let roots = files
        .iter()
        .map(|file| {
            schema::parse_config(&file.content).map_err(|e| ConfigError::new(&file.name, e))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
    load_roots(&names, roots, options)
}

/// [`load`] for files read from disk (name, bytes): each is decoded as yaml.v2 reads its
/// input (UTF-8, or UTF-16 with a byte order mark), then parsed, in order; the first failure
/// is the error.
pub fn load_bytes(
    files: &[(String, Vec<u8>)],
    options: &LoadOptions,
) -> Result<RateLimitConfig, ConfigError> {
    let roots = files
        .iter()
        .map(|(name, bytes)| {
            let loading = |e: String| ConfigError::new(name, e);
            let content = yaml::decode(bytes).map_err(|e| {
                let e = format!("error loading config file: {e}");
                crate::debug!("{e}");
                loading(e)
            })?;
            schema::parse_config(&content).map_err(loading)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let names: Vec<&str> = files.iter().map(|(name, _)| name.as_str()).collect();
    load_roots(&names, roots, options)
}

/// The configs an xDS management server sent (`ConfigXdsProtoToYaml`, then the loader), each
/// named after its `name` in errors.
pub fn load_xds(
    configs: &[ratelimit_proto::ratelimit::config::ratelimit::v3::RateLimitConfig],
    options: &LoadOptions,
) -> Result<RateLimitConfig, ConfigError> {
    let names: Vec<&str> = configs.iter().map(|c| c.name.as_str()).collect();
    load_roots(&names, configs.iter().map(xds::root).collect(), options)
}

/// The domains of parsed files, in order.
fn load_roots(
    names: &[&str],
    roots: Vec<schema::YamlRoot>,
    options: &LoadOptions,
) -> Result<RateLimitConfig, ConfigError> {
    let mut config = RateLimitConfig {
        domains: HashMap::new(),
        sanitize_dots: options.sanitize_descriptor_metric_dots,
        global_quota_mode: options.global_quota_mode,
    };
    for (name, root) in names.iter().zip(roots) {
        if root.domain.is_empty() {
            return Err(ConfigError::new(
                name,
                "config file cannot have empty domain",
            ));
        }
        if config.domains.contains_key(&root.domain) && !options.merge_domain_configs {
            return Err(ConfigError::new(
                name,
                format!("duplicate domain '{}' in config file", root.domain),
            ));
        }
        match config.domains.contains_key(&root.domain) {
            true => crate::debug!("patching domain: {}", root.domain),
            false => crate::debug!("loading domain: {}", root.domain),
        }
        let domain = config.domains.entry(root.domain.clone()).or_default();
        let key_start = format!(
            "{{{}{}_",
            escape_hash_tag(&options.cache_key_prefix),
            escape_hash_tag(&root.domain)
        );
        domain.load_descriptors(
            name,
            &format!("{}.", root.domain),
            &key_start,
            &root.descriptors,
            options,
        )?;
    }
    Ok(config)
}

/// `RateLimitResponse_RateLimit_Unit.String()`: the name, or the number when it has none.
pub fn unit_name(unit: i32) -> String {
    crate::protojson::RESPONSE_UNITS
        .iter()
        .find(|(_, n)| *n == unit)
        .map_or_else(|| unit.to_string(), |(name, _)| name.to_string())
}

/// A unit name as accepted by upstream: any case, `UNKNOWN` excluded.
fn parse_unit(unit: &str) -> Option<Unit> {
    Unit::from_str_name(&unit.to_uppercase()).filter(|u| *u != Unit::Unknown)
}

fn load_limit(
    file: &str,
    full_key: &str,
    config: &YamlDescriptor,
) -> Result<Option<RateLimit>, ConfigError> {
    let Some(rl) = &config.rate_limit else {
        return Ok(None);
    };
    let unit = parse_unit(&rl.unit);
    if rl.unlimited {
        if unit.is_some() {
            return Err(ConfigError::new(
                file,
                "should not specify rate limit unit when unlimited",
            ));
        }
    } else if unit.is_none() {
        return Err(ConfigError::new(
            file,
            format!("invalid rate limit unit '{}'", rl.unit),
        ));
    }
    // `NewRateLimit`'s `NewStats`.
    crate::debug!("Creating stats for key: '{full_key}'");
    for replaced in &rl.replaces {
        if replaced.is_empty() {
            return Err(ConfigError::new(
                file,
                "should not have an empty replaces entry",
            ));
        }
        if *replaced == rl.name {
            return Err(ConfigError::new(
                file,
                "replaces should not contain name of same descriptor",
            ));
        }
    }
    Ok(Some(RateLimit {
        unlimited: rl.unlimited,
        detailed_metric: config.detailed_metric,
        shadow_mode: config.shadow_mode,
        quota_mode: config.quota_mode,
        replaces: rl.replaces.clone(),
        metadata: config.metadata.clone(),
        ..RateLimit::new(
            full_key.to_string(),
            rl.requests_per_unit,
            unit.unwrap_or(Unit::Unknown) as i32,
            rl.name.clone(),
        )
    }))
}

/// Whether `name` can name a list limit (ADR 0001): it ends its keys and stats names.
fn valid_limit_name(name: &str) -> bool {
    let mut chars = name.chars();
    name.len() <= 64
        && chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// A list limit's `window`.
fn load_window(file: &str, at: &str, window: &schema::YamlWindow) -> Result<Window, ConfigError> {
    let error = |message: String| ConfigError::new(file, format!("{at}: {message}"));
    let anchored = match window.kind.as_str() {
        "fixed" => false,
        "anchored" => true,
        "concurrency" => return load_concurrency(window, &error).map(Window::Concurrency),
        "" => return Err(error("window type is required".into())),
        "token_bucket" => false,
        "sliding" | "sliding_approx" => {
            return Err(error(format!(
                "window type '{}' is not supported yet",
                window.kind
            )));
        }
        other => return Err(error(format!("unknown window type '{other}'"))),
    };
    for (key, value) in [
        ("lease", &window.lease),
        ("retry_after", &window.retry_after),
    ] {
        if !value.is_empty() {
            return Err(error(format!("{key} is for concurrency windows")));
        }
    }
    let duration = match window.duration.as_str() {
        "" => return Err(error("window duration is required".into())),
        text => window::parse_duration(text)
            .filter(|d| *d > 0)
            .ok_or_else(|| error(format!("invalid window duration '{text}'")))?,
    };
    if window.kind == "token_bucket" {
        return load_token_bucket(window, duration, &error).map(Window::TokenBucket);
    }
    for (key, value) in [("burst", &window.burst), ("max_debt", &window.max_debt)] {
        if !value.is_empty() {
            return Err(error(format!("{key} is for token_bucket windows")));
        }
    }
    if anchored {
        return load_anchored(window, duration, &error).map(Window::Anchored);
    }
    for (key, value) in [
        ("start_rounding", &window.start_rounding),
        ("open_on", &window.open_on),
    ] {
        if !value.is_empty() {
            return Err(error(format!("{key} is for anchored windows")));
        }
    }
    let rounding = (!window.offset_rounding.is_empty())
        .then(|| {
            window::parse_duration(&window.offset_rounding).ok_or_else(|| {
                error(format!(
                    "invalid offset_rounding '{}'",
                    window.offset_rounding
                ))
            })
        })
        .transpose()?;
    let origin = match window.offset.as_str() {
        "" if rounding.is_some() => {
            return Err(error("offset_rounding needs offset: hashed".into()));
        }
        "" => Origin::Epoch,
        "hashed" => {
            let rounding = rounding.unwrap_or_else(|| window::default_rounding(duration));
            if rounding <= 0 || rounding > duration || duration % rounding != 0 {
                return Err(error(format!(
                    "offset_rounding '{}' must divide the window duration '{}'",
                    window.offset_rounding, window.duration
                )));
            }
            Origin::Hashed { rounding }
        }
        text => {
            if rounding.is_some() {
                return Err(error("offset_rounding needs offset: hashed".into()));
            }
            // `0` alone is a duration of nothing.
            let offset = match text {
                "0" => Some(0),
                text => window::parse_duration(text),
            };
            match offset {
                Some(0) => Origin::Epoch,
                Some(offset) if offset < duration => Origin::Offset(offset),
                Some(_) => {
                    return Err(error(format!(
                        "offset '{text}' must be shorter than the window duration '{}'",
                        window.duration
                    )));
                }
                None => return Err(error(format!("invalid offset '{text}'"))),
            }
        }
    };
    Ok(Window::Fixed(FixedWindow { duration, origin }))
}

/// A `token_bucket` window's `burst` and `max_debt` (D11).
fn load_token_bucket(
    window: &schema::YamlWindow,
    duration: i64,
    error: &impl Fn(String) -> ConfigError,
) -> Result<TokenBucketWindow, ConfigError> {
    for (key, value) in [
        ("offset", &window.offset),
        ("offset_rounding", &window.offset_rounding),
        ("start_rounding", &window.start_rounding),
        ("open_on", &window.open_on),
    ] {
        if !value.is_empty() {
            return Err(error(format!("{key} is not for token_bucket windows")));
        }
    }
    let burst = match window.burst.as_str() {
        "" => None,
        text => match text.parse::<u64>() {
            Ok(burst) if burst > 0 => Some(burst),
            _ => return Err(error(format!("invalid burst '{text}'"))),
        },
    };
    let max_debt = match window.max_debt.as_str() {
        "" => None,
        "unlimited" => Some(MaxDebt::Unlimited),
        text => match (text.parse::<u64>(), window::parse_duration(text)) {
            (Ok(tokens), _) => Some(MaxDebt::Tokens(tokens)),
            (_, Some(seconds)) => Some(MaxDebt::Seconds(seconds)),
            _ => return Err(error(format!("invalid max_debt '{text}'"))),
        },
    };
    Ok(TokenBucketWindow {
        duration,
        burst,
        max_debt,
    })
}

/// A `concurrency` window's `lease` and `retry_after` (D35).
fn load_concurrency(
    window: &schema::YamlWindow,
    error: &impl Fn(String) -> ConfigError,
) -> Result<ConcurrencyWindow, ConfigError> {
    for (key, value) in [
        ("duration", &window.duration),
        ("offset", &window.offset),
        ("offset_rounding", &window.offset_rounding),
        ("start_rounding", &window.start_rounding),
        ("open_on", &window.open_on),
    ] {
        if !value.is_empty() {
            return Err(error(format!("{key} is not for concurrency windows")));
        }
    }
    let seconds = |key: &str, text: &str| {
        window::parse_duration(text)
            .filter(|s| *s > 0)
            .ok_or_else(|| error(format!("invalid {key} '{text}'")))
    };
    let lease = match window.lease.as_str() {
        "" => return Err(error("lease is required".into())),
        text => seconds("lease", text)?,
    };
    let retry_after = match window.retry_after.as_str() {
        "" => 1,
        text => seconds("retry_after", text)?,
    };
    Ok(ConcurrencyWindow { lease, retry_after })
}

/// An `anchored` window's `start_rounding` and `open_on` (D9).
fn load_anchored(
    window: &schema::YamlWindow,
    duration: i64,
    error: &impl Fn(String) -> ConfigError,
) -> Result<AnchoredWindow, ConfigError> {
    for (key, value) in [
        ("offset", &window.offset),
        ("offset_rounding", &window.offset_rounding),
    ] {
        if !value.is_empty() {
            return Err(error(format!("{key} is for fixed windows")));
        }
    }
    let rounding = match window.start_rounding.as_str() {
        "" => None,
        text => match window::parse_duration(text) {
            Some(r) if r > 0 && r <= duration => Some(r),
            Some(_) => {
                return Err(error(format!(
                    "start_rounding '{text}' must not be longer than the window duration '{}'",
                    window.duration
                )));
            }
            None => return Err(error(format!("invalid start_rounding '{text}'"))),
        },
    };
    let open_on = match window.open_on.as_str() {
        "" | "check" => OpenOn::Check,
        "first_hit" => OpenOn::FirstHit,
        other => {
            return Err(error(format!(
                "invalid open_on '{other}': check or first_hit"
            )));
        }
    };
    Ok(AnchoredWindow {
        duration,
        rounding,
        open_on,
    })
}

/// `overflow` (D14): `envoy` counts everything, which is all there is yet.
fn load_overflow(file: &str, full_key: &str, config: &YamlDescriptor) -> Result<(), ConfigError> {
    let message = match config.overflow.as_str() {
        "" | "envoy" => return Ok(()),
        "strict" => "overflow: strict is not supported yet".to_string(),
        other => format!("invalid overflow '{other}': envoy or strict"),
    };
    Err(ConfigError::new(file, format!("{full_key}: {message}")))
}

/// `text` as it is written in a list key's hash tag (ADR 0001).
pub(crate) fn escape_hash_tag(text: &str) -> String {
    text.replace('%', "%25")
        .replace('{', "%7B")
        .replace('}', "%7D")
}

/// A descriptor's `rate_limits:` (D7): named limits, each with its window.
fn load_list(
    file: &str,
    full_key: &str,
    key_start: &str,
    config: &YamlDescriptor,
    options: &LoadOptions,
) -> Result<Vec<RateLimit>, ConfigError> {
    let Some(list) = &config.rate_limits else {
        return Ok(Vec::new());
    };
    let error = |message: String| ConfigError::new(file, format!("{full_key}: {message}"));
    if config.rate_limit.is_some() {
        return Err(error(
            "should not specify both rate_limit and rate_limits".into(),
        ));
    }
    if list.is_empty() {
        return Err(error("rate_limits should not be empty".into()));
    }
    let names: Vec<&str> = list.iter().map(|l| l.name.as_str()).collect();
    let mut seen = HashSet::new();
    let mut limits = Vec::new();
    for entry in list {
        if !valid_limit_name(&entry.name) {
            return Err(error(format!(
                "invalid rate_limits name '{}': 1 to 64 letters, digits, '_' or '-', starting with a letter or digit",
                entry.name
            )));
        }
        let at = format!("{full_key}~{}", entry.name);
        let error = |message: &str| ConfigError::new(file, format!("{at}: {message}"));
        if !seen.insert(entry.name.as_str()) {
            return Err(error("duplicate rate_limits name"));
        }
        if entry.unit {
            return Err(error("rate_limits entries take a window, not a unit"));
        }
        if entry.unlimited {
            return Err(error(
                "rate_limits entries cannot be unlimited: use rate_limit: {unlimited: true}",
            ));
        }
        for replaced in &entry.replaces {
            if replaced.is_empty() {
                return Err(error("should not have an empty replaces entry"));
            }
            if names.contains(&replaced.as_str()) {
                return Err(error(
                    "replaces should not contain a name of the same rate_limits",
                ));
            }
        }
        let Some(window) = &entry.window else {
            return Err(error("window is required"));
        };
        let window = load_window(file, &at, window)?;
        if options.memcached && !matches!(window, Window::Fixed(_)) {
            return Err(error("only fixed windows can be counted on Memcached"));
        }
        if matches!(window, Window::Concurrency(_)) {
            if list.len() > 1 {
                return Err(error(
                    "a concurrency limit must be alone in its rate_limits: hits release its slots",
                ));
            }
            if config.quota_mode {
                return Err(error("a concurrency limit cannot be in quota_mode"));
            }
        }
        // `<hash tag>}_<shape>_<name>`, the window's start left out.
        let static_bytes = key_start.len() + 2 + window.shape().len() + 1 + entry.name.len();
        if options.memcached && static_bytes > MEMCACHED_STATIC_KEY_BYTES {
            crate::log::warn(&format!(
                "{file}: {at}: its key takes {static_bytes} bytes before its window's start and the request's values: memcached refuses a key over 250 bytes, which then counts nothing"
            ));
        }
        if let Window::TokenBucket(bucket) = &window {
            if entry.requests_per_unit == 0 {
                return Err(error("a token_bucket needs requests_per_unit above 0"));
            }
            let quota_mode = config.quota_mode || options.global_quota_mode;
            warn_of_token_bucket(file, &at, bucket, entry.requests_per_unit, quota_mode);
        }
        crate::debug!("Creating stats for key: '{at}'");
        let (unit, multiplier) = window.unit();
        let mut limit = RateLimit {
            detailed_metric: config.detailed_metric,
            shadow_mode: config.shadow_mode,
            quota_mode: config.quota_mode,
            replaces: entry.replaces.clone(),
            metadata: config.metadata.clone(),
            window: Some(window),
            ..RateLimit::new(
                full_key.to_string(),
                entry.requests_per_unit,
                unit as i32,
                entry.name.clone(),
            )
        };
        limit.limit.unit_multiplier = (multiplier != 1).then_some(multiplier);
        limits.push(limit);
    }
    Ok(limits)
}

/// What a token bucket's settings may not do well: a bucket lasting a day or more (D11), and
/// a quota group blocked as long as traffic exceeds the rate (D19).
fn warn_of_token_bucket(
    file: &str,
    at: &str,
    bucket: &TokenBucketWindow,
    requests_per_unit: u32,
    quota_mode: bool,
) {
    let refill = u128::from(bucket.capacity(requests_per_unit)) * bucket.duration as u128;
    if refill >= 86_400 * u128::from(requests_per_unit) {
        crate::log::warn(&format!(
            "{file}: {at}: this token_bucket takes a day or more to fill: it does not suit a daily or weekly budget, use a fixed window with an offset"
        ));
    }
    if quota_mode && bucket.max_debt == Some(MaxDebt::Unlimited) {
        crate::log::warn(&format!(
            "{file}: {at}: quota_mode with max_debt: unlimited can block a quota group as long as traffic exceeds the rate"
        ));
    }
}

/// A limit's line in the text a fingerprint is taken of (D36).
fn fingerprint_line(limit: &RateLimit) -> String {
    let requests = limit.limit.requests_per_unit;
    let window = limit.window.as_ref().map_or_else(
        || format!("unit={}", unit_name(limit.limit.unit)),
        |window| window.fingerprint_text(requests),
    );
    let mut replaces = limit.replaces.clone();
    replaces.sort();
    format!(
        "{}~{} {window} requests_per_unit={requests} unlimited={} shadow_mode={} quota_mode={} detailed_metric={} replaces={replaces:?}\n",
        limit.full_key,
        limit.name,
        limit.unlimited,
        limit.shadow_mode,
        limit.quota_mode,
        limit.detailed_metric
    )
}

impl Descriptor {
    /// Its limits, and those under it, sorted where order changes nothing (D36): a list's first
    /// limit wins a tie, and the first wildcard to match wins.
    fn fingerprint_text(&self, text: &mut String) {
        if let Some(limit) = &self.limit {
            text.push_str(&fingerprint_line(limit));
        }
        for limit in &self.list {
            text.push_str(&fingerprint_line(limit));
        }
        for (pattern, _) in &self.wildcards {
            text.push_str(&format!("wildcard {pattern}\n"));
        }
        if let Some(pattern) = &self.share_threshold_pattern {
            text.push_str(&format!("share_threshold={pattern}\n"));
        }
        let mut children: Vec<_> = self.descriptors.iter().collect();
        children.sort_by(|a, b| a.0.cmp(b.0));
        for (key, child) in children {
            text.push_str(&format!(
                "descriptor {key} value_to_metric={}\n",
                child.value_to_metric
            ));
            child.fingerprint_text(text);
        }
    }

    /// Whether it or one under it has a `rate_limits:` list.
    fn has_list(&self) -> bool {
        !self.list.is_empty() || self.descriptors.values().any(Descriptor::has_list)
    }

    /// Its limits, and those under it, in `quota_mode` with `max_debt: unlimited` (D19).
    fn lasting_lockouts(&self, global_quota_mode: bool) -> u64 {
        let own = self.list.iter().filter(|limit| {
            (limit.quota_mode || global_quota_mode)
                && matches!(
                    limit.window,
                    Some(Window::TokenBucket(TokenBucketWindow {
                        max_debt: Some(MaxDebt::Unlimited),
                        ..
                    }))
                )
        });
        own.count() as u64
            + self
                .descriptors
                .values()
                .map(|descriptor| descriptor.lasting_lockouts(global_quota_mode))
                .sum::<u64>()
    }

    fn load_descriptors(
        &mut self,
        file: &str,
        parent_key: &str,
        key_start: &str,
        descriptors: &[YamlDescriptor],
        options: &LoadOptions,
    ) -> Result<(), ConfigError> {
        for config in descriptors {
            if config.key.is_empty() {
                return Err(ConfigError::new(file, "descriptor has empty key"));
            }
            let mut final_key = config.key.clone();
            if !config.value.is_empty() {
                final_key += "_";
                final_key += &config.value;
            }
            let new_parent_key = format!("{parent_key}{final_key}");
            if self.descriptors.contains_key(&final_key) {
                return Err(ConfigError::new(
                    file,
                    format!("duplicate descriptor composite key '{new_parent_key}'"),
                ));
            }
            let limit = load_limit(file, &new_parent_key, config)?;
            let key_start = format!(
                "{key_start}{}_{}_",
                escape_hash_tag(&config.key),
                escape_hash_tag(&config.value)
            );
            let list = load_list(file, &new_parent_key, &key_start, config, options)?;
            load_overflow(file, &new_parent_key, config)?;
            if config.share_threshold && !final_key.contains('*') {
                return Err(ConfigError::new(
                    file,
                    format!(
                        "share_threshold can only be used with wildcard values (containing '*'), but found key '{final_key}'"
                    ),
                ));
            }
            if final_key.contains('*') {
                let parts = final_key.split('*').map(str::to_string).collect();
                self.wildcards.push((final_key.clone(), parts));
            }
            crate::debug!(
                "loading descriptor: key={new_parent_key}{}",
                limit.as_ref().map_or(String::new(), |l| format!(
                    " ratelimit={{requests_per_unit={}, unit={}, unlimited={}, shadow_mode={}, quota_mode={}}}",
                    l.limit.requests_per_unit,
                    unit_name(l.limit.unit),
                    l.unlimited,
                    l.shadow_mode,
                    l.quota_mode
                ))
            );
            let mut descriptor = Descriptor {
                limit,
                list,
                value_to_metric: config.value_to_metric,
                share_threshold_pattern: config.share_threshold.then(|| final_key.clone()),
                ..Default::default()
            };
            descriptor.load_descriptors(
                file,
                &format!("{new_parent_key}."),
                &key_start,
                &config.descriptors,
                options,
            )?;
            self.descriptors.insert(final_key, descriptor);
        }
        Ok(())
    }
}

impl RateLimitConfig {
    /// The fingerprint of each domain with a `rate_limits:` list (D36): the same for the same
    /// keys, windows and thresholds, however they were written or where they came from.
    pub fn fingerprints(&self) -> BTreeMap<String, String> {
        self.domains
            .iter()
            .filter(|(_, domain)| domain.has_list())
            .map(|(name, domain)| {
                let mut text = format!("v1 {name}\n");
                domain.fingerprint_text(&mut text);
                let hash = xxhash_rust::xxh64::xxh64(text.as_bytes(), 0);
                (name.clone(), format!("{hash:016x}"))
            })
            .collect()
    }

    /// The limits that can block a quota group for as long as traffic lasts (D19).
    pub fn lasting_lockouts(&self) -> u64 {
        self.domains
            .values()
            .map(|domain| domain.lasting_lockouts(self.global_quota_mode))
            .sum()
    }

    /// Whether `domain` is configured.
    pub fn has_domain(&self, domain: &str) -> bool {
        self.domains.contains_key(domain)
    }

    /// `IsEmptyDomains`: whether the config has no domain.
    pub fn is_empty_domains(&self) -> bool {
        self.domains.is_empty()
    }

    /// `Dump`: a line per limit, `<full key>: unit=<UNIT> requests_per_unit=<n>, shadow_mode:
    /// <bool>, quota_mode: <bool>`, each descriptor before those under it. Upstream walks Go
    /// maps, in a random order; domains and descriptors are sorted here.
    pub fn dump(&self) -> String {
        fn dump(descriptor: &Descriptor, out: &mut String) {
            if let Some(limit) = &descriptor.limit {
                let unit = unit_name(limit.limit.unit);
                out.push_str(&format!(
                    "{}: unit={unit} requests_per_unit={}, shadow_mode: {}, quota_mode: {}\n",
                    limit.full_key,
                    limit.limit.requests_per_unit,
                    limit.shadow_mode,
                    limit.quota_mode
                ));
            }
            // A list limit: its stats key and its window.
            for limit in &descriptor.list {
                let window = limit.window.as_ref().expect("a list limit's window");
                out.push_str(&format!(
                    "{}~{}: window={} requests_per_unit={}, shadow_mode: {}, quota_mode: {}\n",
                    limit.full_key,
                    limit.name,
                    window.describe(),
                    limit.limit.requests_per_unit,
                    limit.shadow_mode,
                    limit.quota_mode
                ));
            }
            let mut children: Vec<_> = descriptor.descriptors.iter().collect();
            children.sort_by(|a, b| a.0.cmp(b.0));
            for (_, child) in children {
                dump(child, out);
            }
        }
        let mut domains: Vec<_> = self.domains.iter().collect();
        domains.sort_by(|a, b| a.0.cmp(b.0));
        let mut out = String::new();
        for (_, domain) in domains {
            dump(domain, &mut out);
        }
        for (domain, fingerprint) in self.fingerprints() {
            out.push_str(&format!("fingerprint {domain} {fingerprint}\n"));
        }
        out
    }

    /// `GetLimit`: the limit that applies to `descriptor` in `domain`.
    pub fn get_limit<'a>(&'a self, domain: &str, descriptor: &RateLimitDescriptor) -> LimitLookup {
        crate::debug!("starting get limit lookup");
        let Some(mut current) = self.domains.get(domain) else {
            crate::debug!("unknown domain '{domain}'");
            return LimitLookup::UnknownDomain;
        };
        if let Some(limit) = &descriptor.limit {
            // The override replaces the config entirely; shadow mode is deliberately off.
            // envoy.type.v3.RateLimitUnit shares its numbering with the response unit.
            let key = self.descriptor_key(domain, descriptor);
            crate::debug!("Creating stats for key: '{key}'");
            let mut custom =
                RateLimit::new(key, limit.requests_per_unit, limit.unit, String::new());
            // D23: a multiplier of 1 is none. One with no window is refused by the service.
            if let Some(multiplier) = limit.unit_multiplier.filter(|m| *m != 1) {
                custom.limit.unit_multiplier = Some(multiplier);
                custom.window = Window::of_units(custom.limit.unit(), multiplier);
            }
            return LimitLookup::Limit(custom);
        }
        // The limits found, and whether they are a `rate_limits:` list.
        let mut found: Option<(Vec<RateLimit>, bool)> = None;
        let mut detailed_key = domain.to_string();
        let mut value_to_metric_key = domain.to_string();
        let mut share_patterns: HashMap<usize, String> = HashMap::new();
        let mut stopped_at_leaf = false;
        for (i, entry) in descriptor.entries.iter().enumerate() {
            detailed_key += &format!(
                ".{}_{}",
                self.sanitize(&entry.key),
                self.sanitize(&entry.value)
            );
            let key_value = format!("{}_{}", entry.key, entry.value);
            crate::debug!("looking up key: {key_value}");
            let mut matched_wildcard = None;
            let mut next = current.descriptors.get(&key_value);
            if next.is_none()
                && let Some((pattern, _)) = current
                    .wildcards
                    .iter()
                    .find(|(_, parts)| wildcard_match(parts, &key_value))
            {
                next = current.descriptors.get(pattern);
                matched_wildcard = Some(pattern.as_str());
            }
            let matched_using_value = next.is_some();
            if next.is_none() {
                crate::debug!("looking up key: {}", entry.key);
                next = current.descriptors.get(&entry.key);
            }

            // The value part of a `key_value` wildcard pattern, e.g. `files*` in `key_files*`.
            let pattern_value = |pattern: &'a str| {
                pattern
                    .strip_prefix(&format!("{}_", entry.key))
                    .unwrap_or(pattern)
            };
            if let (Some(_), Some(pattern)) = (
                matched_wildcard,
                next.and_then(|n| n.share_threshold_pattern.as_deref()),
            ) {
                let pattern = pattern_value(pattern);
                crate::debug!(
                    "tracking share_threshold for entry index {i} (key {}), wildcard pattern {pattern}",
                    entry.key
                );
                share_patterns.insert(i, pattern.to_string());
            }

            value_to_metric_key += ".";
            value_to_metric_key += &self.sanitize(&entry.key);
            if let Some(next) = next {
                let value = match matched_wildcard {
                    Some(_) if share_patterns.contains_key(&i) => share_patterns[&i].as_str(),
                    Some(_) if next.value_to_metric => entry.value.as_str(),
                    Some(pattern) => pattern_value(pattern),
                    None if matched_using_value || next.value_to_metric => entry.value.as_str(),
                    None => "",
                };
                if !value.is_empty() {
                    value_to_metric_key += "_";
                    value_to_metric_key += &self.sanitize(value);
                }
            }

            let Some(next) = next else {
                stopped_at_leaf = true;
                break;
            };
            let limits = match (&next.limit, next.list.is_empty()) {
                (Some(limit), _) => Some((vec![limit.clone()], false)),
                (None, false) => Some((next.list.clone(), true)),
                (None, true) => None,
            };
            if let Some(limits) = limits {
                let final_key = match matched_using_value {
                    true => key_value.as_str(),
                    false => entry.key.as_str(),
                };
                crate::debug!("found rate limit: {final_key}");
                if i == descriptor.entries.len() - 1 {
                    found = Some(limits);
                    // Upstream walks a Go map, in a random order (docs/parity.md §7).
                    if crate::log::enabled(crate::log::Level::Debug) {
                        let mut patterns: Vec<_> = share_patterns.iter().collect();
                        patterns.sort();
                        for (index, pattern) in patterns {
                            crate::debug!(
                                "share_threshold enabled for entry index {index}, using wildcard pattern {pattern}"
                            );
                        }
                    }
                } else {
                    crate::debug!(
                        "request depth does not match config depth, there are more entries in the request's descriptor"
                    );
                }
            }
            if next.descriptors.is_empty() {
                stopped_at_leaf = true;
                // A detailed limit is rebuilt with its stats.
                if let Some((limits, _)) = found.as_ref().filter(|(l, _)| l[0].detailed_metric) {
                    crate::debug!("Creating stats for key: '{}'", limits[0].full_key);
                }
                break;
            }
            crate::debug!("iterating to next level");
            current = next;
        }
        for limit in found.iter_mut().flat_map(|(limits, _)| limits.iter_mut()) {
            if !share_patterns.is_empty() {
                limit.share_threshold_key_pattern = Some(
                    (0..descriptor.entries.len())
                        .map(|i| share_patterns.get(&i).cloned().unwrap_or_default())
                        .collect(),
                );
            }
            // Upstream rebuilds the limit with `NewRateLimit`, which drops its metadata, when a
            // detailed limit is found at a leaf or when value_to_metric changes the stats key.
            let rebuilt = if limit.detailed_metric {
                stopped_at_leaf
            } else {
                value_to_metric_key != limit.full_key
            };
            if rebuilt {
                limit.metadata = None;
            }
            let full_key = match &limit.share_threshold_key_pattern {
                Some(patterns) if limit.detailed_metric => {
                    self.share_threshold_key(domain, descriptor, patterns)
                }
                _ if limit.detailed_metric => detailed_key.clone(),
                _ => value_to_metric_key.clone(),
            };
            if crate::log::enabled(crate::log::Level::Debug)
                && (limit.detailed_metric || full_key != limit.full_key)
            {
                crate::debug!("Creating stats for key: '{full_key}'");
            }
            limit.full_key = full_key;
            // A list limit's stats are its descriptor's, then its name (D29).
            if limit.window.is_some() {
                limit.full_key = format!("{}~{}", limit.full_key, limit.name);
            }
        }
        match found {
            None => LimitLookup::NoLimit,
            Some((limits, true)) => LimitLookup::List(limits),
            Some((mut limits, false)) => LimitLookup::Limit(limits.remove(0)),
        }
    }
}

/// Whether `value` matches a pattern pre-split on `*`, each `*` matching zero or more characters.
fn wildcard_match(parts: &[String], value: &str) -> bool {
    let [first, middle @ .., last] = parts else {
        return parts.first().is_some_and(|p| p == value);
    };
    if !value.starts_with(first.as_str()) || !value.ends_with(last.as_str()) {
        return false;
    }
    if value.len() < parts.iter().map(String::len).sum() {
        return false;
    }
    let mut remaining = &value[first.len()..value.len() - last.len()];
    for part in middle {
        let Some(idx) = remaining.find(part.as_str()) else {
            return false;
        };
        remaining = &remaining[idx + part.len()..];
    }
    true
}

impl RateLimitConfig {
    /// A descriptor key or value as written into a stats key.
    fn sanitize<'a>(&self, s: &'a str) -> std::borrow::Cow<'a, str> {
        if self.sanitize_dots {
            s.replace('.', "_").into()
        } else {
            s.into()
        }
    }

    /// `domain.key1_value1.key2`: the stats key of a request's limit override.
    fn descriptor_key(&self, domain: &str, descriptor: &RateLimitDescriptor) -> String {
        let entries: Vec<String> = descriptor
            .entries
            .iter()
            .map(|e| {
                if e.value.is_empty() {
                    self.sanitize(&e.key).into_owned()
                } else {
                    format!("{}_{}", self.sanitize(&e.key), self.sanitize(&e.value))
                }
            })
            .collect();
        format!("{domain}.{}", entries.join("."))
    }

    /// The detailed stats key when `share_threshold` applies: shared entries use their pattern.
    fn share_threshold_key(
        &self,
        domain: &str,
        descriptor: &RateLimitDescriptor,
        patterns: &[String],
    ) -> String {
        let mut key = domain.to_string();
        for (entry, pattern) in descriptor.entries.iter().zip(patterns) {
            key += ".";
            key += &self.sanitize(&entry.key);
            let value = if pattern.is_empty() {
                &entry.value
            } else {
                pattern
            };
            if !value.is_empty() {
                key += "_";
                key += &self.sanitize(value);
            }
        }
        key
    }
}
