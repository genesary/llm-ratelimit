//! statsd_exporter v0.26.1's metric mapper (`pkg/mapper`), which upstream's Prometheus sink
//! uses to turn stat names into Prometheus names and labels: the YAML file decoded with
//! yaml.v2's rules, the same validation, globs matched by the ported FSM, then regexes in order.

use super::MetricType;
use super::fsm::{Fsm, needs_backtracking};
use crate::config::yaml::{self, Node, Resolved};
use regex::Regex;
use std::collections::BTreeMap;
use std::sync::LazyLock;

/// client_golang's `DefBuckets`.
pub(super) const DEFAULT_BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

const DEFAULT_QUANTILES: [Objective; 3] = [
    Objective {
        quantile: 0.5,
        error: 0.05,
    },
    Objective {
        quantile: 0.9,
        error: 0.01,
    },
    Objective {
        quantile: 0.99,
        error: 0.001,
    },
];

static METRIC_LINE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(\*|[a-zA-Z_][a-zA-Z0-9_\-]*)(\.\*|\.[a-zA-Z0-9_][a-zA-Z0-9_\-]*)*$").unwrap()
});
static METRIC_NAME: LazyLock<Regex> = LazyLock::new(|| {
    let template = r"(\$\{?[0-9]+\}?)";
    Regex::new(&format!(
        "^([a-zA-Z_]|{template})([a-zA-Z0-9_]|{template})*$"
    ))
    .unwrap()
});
static LABEL_NAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]+$").unwrap());

/// How a mapping's `match` is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchType {
    Glob,
    Regex,
}

/// What happens to a stat a mapping matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Map,
    Drop,
}

/// What a timer becomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObserverType {
    Histogram,
    Summary,
}

/// A summary quantile and its allowed error.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Objective {
    pub quantile: f64,
    pub error: f64,
}

/// `HistogramOptions`. `None` buckets is Go's nil slice, which some checks tell from empty.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HistogramOptions {
    pub buckets: Option<Vec<f64>>,
    pub native_histogram_bucket_factor: f64,
    pub native_histogram_max_buckets: u32,
}

/// `SummaryOptions`; `max_age` in nanoseconds.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SummaryOptions {
    pub quantiles: Option<Vec<Objective>>,
    pub max_age: i64,
    pub age_buckets: u32,
    pub buf_cap: u32,
}

/// `MapperConfigDefaults`.
#[derive(Debug, Clone, Default)]
struct Defaults {
    observer_type: Option<ObserverType>,
    match_type: Option<MatchType>,
    glob_disable_ordering: bool,
    ttl: i64,
    summary_options: SummaryOptions,
    histogram_options: HistogramOptions,
}

/// A `MetricMapping` as configured, then completed by the mapper.
#[derive(Debug, Clone, Default)]
struct MappingConfig {
    pattern: String,
    name: String,
    /// Label name → template, as Go's map (iteration order never shows).
    labels: BTreeMap<String, String>,
    honor_labels: bool,
    observer_type: Option<ObserverType>,
    legacy_buckets: Option<Vec<f64>>,
    legacy_quantiles: Option<Vec<Objective>>,
    match_type: Option<MatchType>,
    help: String,
    action: Option<Action>,
    match_metric_type: Option<MetricType>,
    ttl: i64,
    summary_options: Option<SummaryOptions>,
    histogram_options: Option<HistogramOptions>,
    scale: Option<f64>,
}

#[derive(Debug)]
struct Mapping {
    config: MappingConfig,
    matcher: Matcher,
}

#[derive(Debug)]
enum Matcher {
    Glob {
        name: TemplateFormatter,
        labels: Vec<(String, TemplateFormatter)>,
    },
    Regex(Regex),
}

/// What the mapper decided for a name: the mapping's options, with the name and labels filled
/// in from the captures.
#[derive(Debug, Clone, PartialEq)]
pub struct Mapped {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub action: Action,
    pub help: String,
    pub honor_labels: bool,
    pub observer_type: Option<ObserverType>,
    pub match_type: MatchType,
    pub match_metric_type: Option<MetricType>,
    /// Nanoseconds.
    pub ttl: i64,
    pub scale: Option<f64>,
    pub histogram_options: Option<HistogramOptions>,
    pub summary_options: Option<SummaryOptions>,
}

/// A mapper file upstream cannot use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapperError {
    /// statsd_exporter rejects it; upstream ignores the error and maps nothing.
    Invalid(String),
    /// Decoding it panics upstream, which stops the process.
    Panic(String),
}

/// `MetricMapper`, loaded once.
#[derive(Debug)]
pub struct Mapper {
    defaults: Defaults,
    mappings: Vec<Mapping>,
    fsm: Option<Fsm>,
}

impl Default for Mapper {
    /// A mapper that failed to load: no mapping, zero defaults.
    fn default() -> Self {
        Self {
            defaults: Defaults::default(),
            mappings: Vec::new(),
            fsm: None,
        }
    }
}

impl Mapper {
    /// `InitFromYAMLString`.
    pub fn from_yaml(content: &str) -> Result<Self, MapperError> {
        let root = yaml::parse(content).map_err(MapperError::Invalid)?;
        let mut decoder = Decoder::default();
        let (mut defaults, configs) = decoder.root(root.as_ref())?;
        if let Some(error) = decoder.errors.into_iter().next() {
            return Err(MapperError::Invalid(error));
        }
        complete_defaults(&mut defaults);
        let mut fsm = Fsm::new(defaults.glob_disable_ordering);
        let mut has_glob = false;
        let mut mappings = Vec::with_capacity(configs.len());
        for (i, mut config) in configs.into_iter().enumerate() {
            let invalid = |message: String| Err(MapperError::Invalid(message));
            if let Some(label) = config.labels.keys().find(|k| !LABEL_NAME.is_match(k)) {
                return invalid(format!("invalid label key: {label}"));
            }
            if config.name.is_empty() {
                return invalid(format!("line {i}: metric mapping didn't set a metric name"));
            }
            if !METRIC_NAME.is_match(&config.name) {
                return invalid(format!("metric name '{}' doesn't match regex", config.name));
            }
            config.match_type = config.match_type.or(defaults.match_type);
            config.action = config.action.or(Some(Action::Map));
            let matcher = if config.match_type == Some(MatchType::Glob) {
                has_glob = true;
                if !METRIC_LINE.is_match(&config.pattern) {
                    return invalid(format!("invalid match: {}", config.pattern));
                }
                let captures = fsm.add_state(&config.pattern, config.match_metric_type, i);
                Matcher::Glob {
                    name: TemplateFormatter::new(&config.name, captures),
                    labels: config
                        .labels
                        .iter()
                        .map(|(k, v)| (k.clone(), TemplateFormatter::new(v, captures)))
                        .collect(),
                }
            } else {
                match super::super::dogstatsd::go_regex(&config.pattern) {
                    Ok(regex) => Matcher::Regex(regex),
                    Err(e) => {
                        return invalid(format!(
                            "invalid regex {} in mapping: {e}",
                            config.pattern
                        ));
                    }
                }
            };
            if deprecation_warning(&config) {
                return Err(MapperError::Panic(format!(
                    "the top-level buckets or quantiles of mapping {i} make upstream log a \
                     deprecation warning without a logger, which panics"
                )));
            }
            complete_observer(&mut config, &defaults).map_err(MapperError::Invalid)?;
            mappings.push(Mapping { config, matcher });
        }
        let fsm = has_glob.then(|| {
            let globs: Vec<&str> = mappings
                .iter()
                .filter(|m| matches!(m.matcher, Matcher::Glob { .. }))
                .map(|m| m.config.pattern.as_str())
                .collect();
            fsm.backtracking_needed = needs_backtracking(&globs, defaults.glob_disable_ordering);
            fsm
        });
        Ok(Self {
            defaults,
            mappings,
            fsm,
        })
    }

    /// The default observer type, for an event no mapping gives one.
    pub fn default_observer_type(&self) -> Option<ObserverType> {
        self.defaults.observer_type
    }

    /// The default classic buckets of histograms; none for a mapper that failed to load.
    pub fn default_buckets(&self) -> Option<Vec<f64>> {
        self.defaults.histogram_options.buckets.clone()
    }

    /// The default native histogram bucket factor (0 for a mapper that failed to load).
    pub fn native_histogram_bucket_factor(&self) -> f64 {
        self.defaults
            .histogram_options
            .native_histogram_bucket_factor
    }

    /// The default summary options.
    pub fn default_summary_options(&self) -> &SummaryOptions {
        &self.defaults.summary_options
    }

    /// `defaults.ttl`, in nanoseconds.
    pub fn default_ttl(&self) -> i64 {
        self.defaults.ttl
    }

    /// `GetMapping`: the glob mapping of `name`, else the first regex mapping matching it
    /// (anywhere in the name) whose metric type fits. (Upstream skips the regexes when there
    /// are none, which is the same.)
    pub fn get_mapping(&self, name: &str, metric_type: MetricType) -> Option<Mapped> {
        if let Some(fsm) = &self.fsm {
            let (found, captures) = fsm.get_mapping(name, metric_type);
            if let Some(index) = found {
                let mapping = &self.mappings[index];
                let Matcher::Glob {
                    name: formatter,
                    labels,
                } = &mapping.matcher
                else {
                    unreachable!("the FSM only holds glob mappings")
                };
                let labels = labels
                    .iter()
                    .map(|(k, f)| (k.clone(), f.format(&captures)))
                    .collect();
                return Some(mapped(&mapping.config, formatter.format(&captures), labels));
            }
        }
        self.mappings.iter().find_map(|mapping| {
            let Matcher::Regex(regex) = &mapping.matcher else {
                return None;
            };
            let captures = regex.captures(name)?;
            if mapping
                .config
                .match_metric_type
                .is_some_and(|t| t != metric_type)
            {
                return None;
            }
            let labels = mapping
                .config
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), expand(regex, v, &captures)))
                .collect();
            Some(mapped(
                &mapping.config,
                expand(regex, &mapping.config.name, &captures),
                labels,
            ))
        })
    }
}

fn mapped(config: &MappingConfig, name: String, labels: BTreeMap<String, String>) -> Mapped {
    Mapped {
        name,
        labels,
        action: config.action.unwrap_or(Action::Map),
        help: config.help.clone(),
        honor_labels: config.honor_labels,
        observer_type: config.observer_type,
        match_type: config.match_type.unwrap_or(MatchType::Glob),
        match_metric_type: config.match_metric_type,
        ttl: config.ttl,
        scale: config.scale,
        histogram_options: config.histogram_options.clone(),
        summary_options: config.summary_options.clone(),
    }
}

fn complete_defaults(defaults: &mut Defaults) {
    let histogram = &mut defaults.histogram_options;
    if histogram.buckets.as_ref().is_none_or(Vec::is_empty) {
        histogram.buckets = Some(DEFAULT_BUCKETS.to_vec());
    }
    if histogram.native_histogram_bucket_factor == 0.0 {
        histogram.native_histogram_bucket_factor = 1.1;
    }
    if histogram.native_histogram_max_buckets == 0 {
        histogram.native_histogram_max_buckets = 256;
    }
    let summary = &mut defaults.summary_options;
    if summary.quantiles.as_ref().is_none_or(Vec::is_empty) {
        summary.quantiles = Some(DEFAULT_QUANTILES.to_vec());
    }
    defaults.match_type = defaults.match_type.or(Some(MatchType::Glob));
}

/// Whether statsd_exporter warns that a mapping's top-level `quantiles` or `buckets` are
/// deprecated. Upstream's sink sets no logger on the mapper, so the warning panics.
fn deprecation_warning(config: &MappingConfig) -> bool {
    let quantiles = config.legacy_quantiles.is_some()
        && config
            .summary_options
            .as_ref()
            .is_none_or(|s| s.quantiles.is_some());
    let buckets = config.legacy_buckets.is_some()
        && config
            .histogram_options
            .as_ref()
            .is_none_or(|h| h.buckets.is_some());
    quantiles || buckets
}

/// The observer options of a mapping, completed from the defaults, with their checks.
fn complete_observer(config: &mut MappingConfig, defaults: &Defaults) -> Result<(), String> {
    config.observer_type = config.observer_type.or(defaults.observer_type);
    if config.summary_options.is_some()
        && config.legacy_quantiles.is_some()
        && config
            .summary_options
            .as_ref()
            .is_some_and(|s| s.quantiles.is_some())
    {
        return Err(format!(
            "cannot use quantiles in both the top level and summary options at the same time in {}",
            config.pattern
        ));
    }
    if config.legacy_buckets.is_some()
        && config
            .histogram_options
            .as_ref()
            .is_some_and(|h| h.buckets.is_some())
    {
        return Err(format!(
            "cannot use buckets in both the top level and histogram options at the same time in {}",
            config.pattern
        ));
    }
    match config.observer_type {
        Some(ObserverType::Histogram) => {
            if config.summary_options.is_some() {
                return Err(
                    "cannot use histogram observer and summary options at the same time".into(),
                );
            }
            let options = config.histogram_options.get_or_insert_default();
            if let Some(buckets) = config.legacy_buckets.as_ref().filter(|b| !b.is_empty()) {
                options.buckets = Some(buckets.clone());
            }
            if options.buckets.as_ref().is_none_or(Vec::is_empty) {
                options.buckets = defaults.histogram_options.buckets.clone();
            }
        }
        Some(ObserverType::Summary) => {
            if config.histogram_options.is_some() {
                return Err(
                    "cannot use summary observer and histogram options at the same time".into(),
                );
            }
            let options = config.summary_options.get_or_insert_default();
            if let Some(quantiles) = config.legacy_quantiles.as_ref().filter(|q| !q.is_empty()) {
                options.quantiles = Some(quantiles.clone());
            }
            if options.quantiles.as_ref().is_none_or(Vec::is_empty) {
                options.quantiles = defaults.summary_options.quantiles.clone();
            }
            let fallback = &defaults.summary_options;
            if options.max_age == 0 {
                options.max_age = fallback.max_age;
            }
            if options.age_buckets == 0 {
                options.age_buckets = fallback.age_buckets;
            }
            if options.buf_cap == 0 {
                options.buf_cap = fallback.buf_cap;
            }
        }
        None => {}
    }
    if config.ttl == 0 && defaults.ttl > 0 {
        config.ttl = defaults.ttl;
    }
    Ok(())
}

/// `fsm.TemplateFormatter`: each `$n`/`${n}` of a glob mapping's name or label becomes the
/// n-th capture, through `strings.Replace` then `fmt.Sprintf`, like the original.
#[derive(Debug)]
struct TemplateFormatter {
    capture_indexes: Vec<usize>,
    format: String,
}

static TEMPLATE_REFERENCE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{?([a-zA-Z0-9_\$]+)\}?").unwrap());

impl TemplateFormatter {
    fn new(template: &str, capture_count: usize) -> Self {
        let mut format = template.to_string();
        let mut capture_indexes = Vec::new();
        for reference in TEMPLATE_REFERENCE.captures_iter(template) {
            let whole = &reference[0];
            match reference[1].parse::<i64>() {
                Ok(index) if index >= 1 && index as u64 <= capture_count as u64 => {
                    format = format.replace(whole, "%s");
                    capture_indexes.push(index as usize - 1);
                }
                _ => format = format.replace(whole, ""),
            }
        }
        Self {
            capture_indexes,
            format,
        }
    }

    fn format(&self, captures: &[String]) -> String {
        if self.capture_indexes.is_empty() {
            return self.format.clone();
        }
        let args: Vec<&str> = self
            .capture_indexes
            .iter()
            .map(|&i| captures[i].as_str())
            .collect();
        go_sprintf(&self.format, &args)
    }
}

/// Go's `fmt.Sprintf` with string arguments, for the directives a template can hold: `%%`,
/// `%s`, `%v`, `%q`, `%x`, `%X` and `%T` as Go prints a string, other verbs and missing or
/// extra arguments in Go's error forms. Flags, widths and precisions are not supported (`docs/parity.md` §7).
fn go_sprintf(format: &str, args: &[&str]) -> String {
    let mut out = String::new();
    let mut next = 0;
    let mut chars = format.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            None => out += "%!(NOVERB)",
            Some('%') => out.push('%'),
            Some(verb) => match args.get(next) {
                None => out += &format!("%!{verb}(MISSING)"),
                Some(arg) => {
                    next += 1;
                    match verb {
                        's' | 'v' => out += arg,
                        'q' => out += &crate::go::quote(arg),
                        'x' => arg.bytes().for_each(|b| out += &format!("{b:02x}")),
                        'X' => arg.bytes().for_each(|b| out += &format!("{b:02X}")),
                        'T' => out += "string",
                        _ => out += &format!("%!{verb}(string={arg})"),
                    }
                }
            },
        }
    }
    if next < args.len() {
        let extra: Vec<String> = args[next..].iter().map(|a| format!("string={a}")).collect();
        out += &format!("%!(EXTRA {})", extra.join(", "));
    }
    out
}

/// Go's `Regexp.ExpandString`: `$n`, `${n}`, `$name` and `${name}` become that group (the
/// longest name of letters, digits and `_`), `$$` a `$`; a malformed reference stays as is.
fn expand(regex: &Regex, template: &str, captures: &regex::Captures) -> String {
    static NAME_CHAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[\p{L}\p{Nd}_]").unwrap());
    let mut out = String::new();
    let mut rest = template;
    while let Some(at) = rest.find('$') {
        out += &rest[..at];
        rest = &rest[at + 1..];
        if let Some(after) = rest.strip_prefix('$') {
            out.push('$');
            rest = after;
            continue;
        }
        let (braced, body) = match rest.strip_prefix('{') {
            Some(body) => (true, body),
            None => (false, rest),
        };
        let len: usize = body
            .char_indices()
            .take_while(|(i, _)| NAME_CHAR.is_match(&body[*i..]))
            .map(|(_, c)| c.len_utf8())
            .sum();
        let name = &body[..len];
        let closed = !braced || body[len..].starts_with('}');
        if len == 0 || !closed {
            out.push('$');
            continue;
        }
        rest = &body[len + usize::from(braced)..];
        let number = (name.bytes().all(|b| b.is_ascii_digit())
            && !(name.starts_with('0') && name.len() > 1))
            .then(|| name.parse::<usize>().ok().filter(|n| *n < 100_000_000))
            .flatten();
        let group = match number {
            Some(n) => captures.get(n),
            None => regex
                .capture_names()
                .position(|n| n == Some(name))
                .and_then(|i| captures.get(i)),
        };
        if let Some(group) = group {
            out += group.as_str();
        }
    }
    out + rest
}

/// Decodes the mapper file as yaml.v2 decodes it into statsd_exporter's types. Type errors are
/// collected and decoding goes on, as yaml.v2 does; any error makes the file invalid.
#[derive(Default)]
struct Decoder {
    errors: Vec<String>,
}

fn is_null(node: &Node) -> bool {
    matches!(node, Node::Scalar(s) if s.resolve() == Resolved::Null)
}

/// A null that yaml.v2 does not hand to an `UnmarshalYAML` method (`prepare`).
fn skips_unmarshaler(node: &Node) -> bool {
    matches!(node, Node::Scalar(s) if s.is_nil())
}

impl Decoder {
    fn error(&mut self, node: &Node, go_type: &str) {
        self.errors.push(format!(
            "line {}: cannot unmarshal {} into {go_type}",
            node.line() + 1,
            node.short_tag()
        ));
    }

    /// The entries of a mapping decoded into a struct, in order. yaml.v2 decodes a repeated
    /// key (or one merged with `<<`) again into the same field: a scalar is replaced, but a
    /// map or a struct gets the new entries added.
    fn fields<'a>(&mut self, node: &'a Node, go_type: &str) -> Vec<(String, &'a Node)> {
        match node {
            Node::Map { entries, .. } => entries
                .iter()
                .map(|(key, value)| (self.string(key), value))
                .collect(),
            n if is_null(n) => Vec::new(),
            other => {
                self.error(other, go_type);
                Vec::new()
            }
        }
    }

    fn string(&mut self, node: &Node) -> String {
        match node {
            Node::Scalar(s) if s.resolve() == Resolved::Null => String::new(),
            Node::Scalar(s) => s.text.clone(),
            other => {
                self.error(other, "string");
                String::new()
            }
        }
    }

    fn boolean(&mut self, node: &Node) -> bool {
        match node {
            Node::Scalar(s) => match s.resolve() {
                Resolved::Null => false,
                Resolved::Bool(b) => b,
                _ => {
                    self.error(node, "bool");
                    false
                }
            },
            _ => {
                self.error(node, "bool");
                false
            }
        }
    }

    fn float(&mut self, node: &Node) -> f64 {
        if let Node::Scalar(s) = node {
            match s.resolve() {
                Resolved::Null => return 0.0,
                Resolved::Int(v) => return v as f64,
                Resolved::Uint(v) => return v as f64,
                Resolved::Float(v) => return v,
                _ => {}
            }
        }
        self.error(node, "float64");
        0.0
    }

    fn uint32(&mut self, node: &Node) -> u32 {
        if let Node::Scalar(s) = node {
            let value = match s.resolve() {
                Resolved::Null => return 0,
                Resolved::Int(v) => u32::try_from(v).ok(),
                Resolved::Uint(v) => u32::try_from(v).ok(),
                Resolved::Float(v) if v >= 0.0 && v.trunc() <= f64::from(u32::MAX) => {
                    Some(v as u32)
                }
                _ => None,
            };
            if let Some(value) = value {
                return value;
            }
        }
        self.error(node, "uint32");
        0
    }

    /// A `time.Duration`: an integer of nanoseconds, or text for `time.ParseDuration`.
    fn duration(&mut self, node: &Node) -> i64 {
        if let Node::Scalar(s) = node {
            let value = match s.resolve() {
                Resolved::Null => return 0,
                Resolved::Int(v) => Some(v),
                Resolved::Uint(v) => i64::try_from(v).ok(),
                Resolved::Float(v) if v.is_finite() && v.trunc() <= i64::MAX as f64 => {
                    Some(v as i64)
                }
                Resolved::Str(text) => crate::go::parse_duration(&text).ok(),
                _ => None,
            };
            if let Some(value) = value {
                return value;
            }
        }
        self.error(node, "time.Duration");
        0
    }

    fn floats(&mut self, node: &Node) -> Option<Vec<f64>> {
        match node {
            Node::Seq { items, .. } => Some(items.iter().map(|item| self.float(item)).collect()),
            n if is_null(n) => None,
            other => {
                self.error(other, "[]float64");
                None
            }
        }
    }

    fn objectives(&mut self, node: &Node) -> Option<Vec<Objective>> {
        match node {
            Node::Seq { items, .. } => Some(
                items
                    .iter()
                    .map(|item| {
                        let mut objective = Objective {
                            quantile: 0.0,
                            error: 0.0,
                        };
                        for (key, value) in self.fields(item, "mapper.MetricObjective") {
                            match key.as_str() {
                                "quantile" => objective.quantile = self.float(value),
                                "error" => objective.error = self.float(value),
                                _ => {}
                            }
                        }
                        objective
                    })
                    .collect(),
            ),
            n if is_null(n) => None,
            other => {
                self.error(other, "[]mapper.MetricObjective");
                None
            }
        }
    }

    /// Labels, added to those already decoded; a null clears them.
    fn labels(&mut self, node: &Node, labels: &mut BTreeMap<String, String>) {
        match node {
            Node::Map { entries, .. } => {
                for (k, v) in entries {
                    let (k, v) = (self.string(k), self.string(v));
                    labels.insert(k, v);
                }
            }
            n if is_null(n) => labels.clear(),
            other => self.error(other, "prometheus.Labels"),
        }
    }

    /// Histogram options, into those already decoded.
    fn histogram_options(&mut self, node: &Node, options: &mut HistogramOptions) {
        for (key, value) in self.fields(node, "mapper.HistogramOptions") {
            match key.as_str() {
                "buckets" => options.buckets = self.floats(value),
                "native_histogram_bucket_factor" => {
                    options.native_histogram_bucket_factor = self.float(value);
                }
                "native_histogram_max_buckets" => {
                    options.native_histogram_max_buckets = self.uint32(value);
                }
                _ => {}
            }
        }
    }

    /// Summary options, into those already decoded.
    fn summary_options(&mut self, node: &Node, options: &mut SummaryOptions) {
        for (key, value) in self.fields(node, "mapper.SummaryOptions") {
            match key.as_str() {
                "quantiles" => options.quantiles = self.objectives(value),
                "max_age" => options.max_age = self.duration(value),
                "age_buckets" => options.age_buckets = self.uint32(value),
                "buf_cap" => options.buf_cap = self.uint32(value),
                _ => {}
            }
        }
    }

    /// A pointer to options: nil for a null, else decoded into the options already there.
    fn optional<T: Default>(
        &mut self,
        node: &Node,
        options: &mut Option<T>,
        decode: impl FnOnce(&mut Self, &Node, &mut T),
    ) {
        if is_null(node) {
            *options = None;
        } else {
            decode(self, node, options.get_or_insert_default());
        }
    }

    /// A string-based enum's `UnmarshalYAML`: `None` when yaml.v2 skips it for a null.
    fn word(&mut self, node: &Node) -> Option<String> {
        (!skips_unmarshaler(node)).then(|| self.string(node))
    }

    fn match_type(&mut self, node: &Node) -> Result<Option<MatchType>, MapperError> {
        match self.word(node).as_deref() {
            None => Ok(None),
            Some("regex") => Ok(Some(MatchType::Regex)),
            Some("glob" | "") => Ok(Some(MatchType::Glob)),
            Some(other) => Err(MapperError::Invalid(format!(
                "invalid match type \"{other}\""
            ))),
        }
    }

    fn observer_type(&mut self, node: &Node) -> Result<Option<ObserverType>, MapperError> {
        match self.word(node).as_deref() {
            None => Ok(None),
            Some("histogram") => Ok(Some(ObserverType::Histogram)),
            Some("summary" | "") => Ok(Some(ObserverType::Summary)),
            Some(other) => Err(MapperError::Invalid(format!(
                "invalid observer type '{other}'"
            ))),
        }
    }

    fn action(&mut self, node: &Node) -> Result<Option<Action>, MapperError> {
        match self.word(node).as_deref() {
            None => Ok(None),
            Some("drop") => Ok(Some(Action::Drop)),
            Some("map" | "") => Ok(Some(Action::Map)),
            Some(other) => Err(MapperError::Invalid(format!(
                "invalid action type \"{other}\""
            ))),
        }
    }

    fn metric_type(&mut self, node: &Node) -> Result<Option<MetricType>, MapperError> {
        match self.word(node).as_deref() {
            None => Ok(None),
            Some("counter") => Ok(Some(MetricType::Counter)),
            Some("gauge") => Ok(Some(MetricType::Gauge)),
            Some("observer" | "timer") => Ok(Some(MetricType::Observer)),
            Some(other) => Err(MapperError::Invalid(format!(
                "invalid metric type '{other}'"
            ))),
        }
    }

    /// `MaybeFloat64.UnmarshalYAML`: any non-null value sets it.
    fn scale(&mut self, node: &Node) -> Option<f64> {
        (!skips_unmarshaler(node)).then(|| self.float(node))
    }

    /// `MetricMapping.UnmarshalYAML`, which reads `timer_type` when `observer_type` is unset.
    fn mapping(&mut self, node: &Node) -> Result<MappingConfig, MapperError> {
        let mut config = MappingConfig::default();
        if skips_unmarshaler(node) {
            return Ok(config);
        }
        let mut timer_type = None;
        for (key, value) in self.fields(node, "mapper.MetricMappingAlias") {
            match key.as_str() {
                "match" => config.pattern = self.string(value),
                "name" => config.name = self.string(value),
                "labels" => self.labels(value, &mut config.labels),
                "honor_labels" => config.honor_labels = self.boolean(value),
                "observer_type" => config.observer_type = self.observer_type(value)?,
                "timer_type" => timer_type = self.observer_type(value)?,
                "buckets" => config.legacy_buckets = self.floats(value),
                "quantiles" => config.legacy_quantiles = self.objectives(value),
                "match_type" => config.match_type = self.match_type(value)?,
                "help" => config.help = self.string(value),
                "action" => config.action = self.action(value)?,
                "match_metric_type" => config.match_metric_type = self.metric_type(value)?,
                "ttl" => config.ttl = self.duration(value),
                "summary_options" => {
                    self.optional(value, &mut config.summary_options, Self::summary_options);
                }
                "histogram_options" => {
                    self.optional(
                        value,
                        &mut config.histogram_options,
                        Self::histogram_options,
                    );
                }
                "scale" => config.scale = self.scale(value),
                _ => {}
            }
        }
        if config.observer_type.is_none() {
            config.observer_type = timer_type;
        }
        Ok(config)
    }

    /// `MapperConfigDefaults.UnmarshalYAML`, with the deprecated keys.
    fn defaults(&mut self, node: &Node) -> Result<Defaults, MapperError> {
        let mut defaults = Defaults::default();
        if skips_unmarshaler(node) {
            return Ok(defaults);
        }
        let (mut timer_type, mut buckets, mut quantiles) = (None, None, None);
        for (key, value) in self.fields(node, "mapper.mapperConfigDefaultsAlias") {
            match key.as_str() {
                "observer_type" => defaults.observer_type = self.observer_type(value)?,
                "timer_type" => timer_type = self.observer_type(value)?,
                "buckets" => buckets = self.floats(value),
                "quantiles" => quantiles = self.objectives(value),
                "match_type" => defaults.match_type = self.match_type(value)?,
                "glob_disable_ordering" => defaults.glob_disable_ordering = self.boolean(value),
                "ttl" => defaults.ttl = self.duration(value),
                // Struct values: a null resets them.
                "summary_options" => {
                    if is_null(value) {
                        defaults.summary_options = SummaryOptions::default();
                    }
                    self.summary_options(value, &mut defaults.summary_options);
                }
                "histogram_options" => {
                    if is_null(value) {
                        defaults.histogram_options = HistogramOptions::default();
                    }
                    self.histogram_options(value, &mut defaults.histogram_options);
                }
                _ => {}
            }
        }
        if defaults.observer_type.is_none() {
            defaults.observer_type = timer_type;
        }
        fn empty<T>(v: &Option<Vec<T>>) -> bool {
            v.as_ref().is_none_or(Vec::is_empty)
        }
        if empty(&defaults.summary_options.quantiles) && !empty(&quantiles) {
            defaults.summary_options = SummaryOptions {
                quantiles,
                ..Default::default()
            };
        }
        if empty(&defaults.histogram_options.buckets) && !empty(&buckets) {
            defaults.histogram_options = HistogramOptions {
                buckets,
                ..Default::default()
            };
        }
        Ok(defaults)
    }

    /// The `MetricMapper` struct: its tagged fields, and the untagged exported ones yaml.v2
    /// also fills (by lowercased name). Those hold interfaces: a value there panics upstream.
    fn root(&mut self, root: Option<&Node>) -> Result<(Defaults, Vec<MappingConfig>), MapperError> {
        let mut defaults = Defaults::default();
        let mut mappings = Vec::new();
        let Some(root) = root else {
            return Ok((defaults, mappings));
        };
        for (key, value) in self.fields(root, "mapper.MetricMapper") {
            match key.as_str() {
                "defaults" => defaults = self.defaults(value)?,
                "mappings" => match value {
                    Node::Seq { items, .. } => {
                        mappings = items
                            .iter()
                            .map(|item| self.mapping(item))
                            .collect::<Result<_, _>>()?;
                    }
                    n if is_null(n) => mappings = Vec::new(),
                    other => self.error(other, "[]mapper.MetricMapping"),
                },
                "registerer" | "mappingscount" | "logger" if !is_null(value) => {
                    return Err(MapperError::Panic(format!(
                        "the mapper key {key} cannot be set"
                    )));
                }
                "fsm" => self.fsm(value),
                _ => {}
            }
        }
        Ok((defaults, mappings))
    }

    /// `fsm: …` decodes into an `fsm.FSM` that is then replaced; only its type errors count.
    fn fsm(&mut self, node: &Node) {
        for (key, value) in self.fields(node, "fsm.FSM") {
            if matches!(key.as_str(), "backtrackingneeded" | "orderingdisabled") {
                self.boolean(value);
            }
        }
    }
}
