//! `USE_PROMETHEUS`: upstream's Prometheus sink (`src/stats/prom/prometheus_sink.go`). Each
//! flushed stat goes through statsd_exporter's mapper and exporter into a registry served in
//! Prometheus' text format on `PROMETHEUS_ADDR` + `PROMETHEUS_PATH`.

mod exporter;
mod fsm;
mod mapper;
mod summary;

pub use mapper::{
    Action, HistogramOptions, Mapped, Mapper, MapperError, MatchType, Objective, ObserverType,
    SummaryOptions,
};

use crate::goserver::{Answer, Request};
use crate::mux::{Route, ServeMux};
use exporter::{Event, Exporter};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// The mapper upstream uses when `PROMETHEUS_MAPPER_YAML` is not set.
const DEFAULT_MAPPER: &str = include_str!("default_mapper.yaml");

/// The type of a stat, as the mapper's `match_metric_type` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Observer,
}

impl MetricType {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Observer => "observer",
        }
    }
}

const DEFAULT_ADDR: &str = ":9090";
const DEFAULT_PATH: &str = "/metrics";

/// The `PROMETHEUS_*` settings.
#[derive(Debug, Clone)]
pub struct PrometheusSettings {
    /// `PROMETHEUS_ADDR`.
    pub addr: String,
    /// `PROMETHEUS_PATH`.
    pub path: String,
    /// `PROMETHEUS_MAPPER_YAML`: a file, or empty for the default mapper.
    pub mapper_yaml: String,
    /// `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS`.
    pub response_time_as_milliseconds: bool,
}

impl Default for PrometheusSettings {
    fn default() -> Self {
        Self {
            addr: DEFAULT_ADDR.into(),
            path: DEFAULT_PATH.into(),
            mapper_yaml: String::new(),
            response_time_as_milliseconds: false,
        }
    }
}

/// The mapper the settings select, as upstream loads it: a file that cannot be read or is
/// invalid leaves a mapper that maps nothing (the error is ignored, and the default mapper is
/// not used instead). Only a file that would make upstream panic is an error.
pub fn load_mapper(settings: &PrometheusSettings) -> Result<Mapper, String> {
    let content = if settings.mapper_yaml.is_empty() {
        if settings.response_time_as_milliseconds {
            // Upstream drops its one `scale`; the quota lookup's timer goes with it.
            DEFAULT_MAPPER.replace("    scale: 0.001\n", "")
        } else {
            DEFAULT_MAPPER.to_string()
        }
    } else {
        match std::fs::read(&settings.mapper_yaml)
            .ok()
            .and_then(yaml_text)
        {
            Some(text) => text,
            None => return Ok(Mapper::default()),
        }
    };
    match Mapper::from_yaml(&content) {
        Ok(mapper) => Ok(mapper),
        Err(MapperError::Invalid(_)) => Ok(Mapper::default()),
        Err(MapperError::Panic(e)) => Err(format!("invalid PROMETHEUS_MAPPER_YAML: {e}")),
    }
}

/// A file's text as yaml.v2 reads it: UTF-8 (a byte order mark skipped), or UTF-16 with a
/// byte order mark; anything else is an error.
fn yaml_text(bytes: Vec<u8>) -> Option<String> {
    let utf16 = |bytes: &[u8], read: fn([u8; 2]) -> u16| {
        let units: Vec<u16> = bytes
            .chunks(2)
            .map(|pair| (pair.len() == 2).then(|| read([pair[0], pair[1]])))
            .collect::<Option<_>>()?;
        String::from_utf16(&units).ok()
    };
    match bytes.as_slice() {
        [0xFF, 0xFE, rest @ ..] => utf16(rest, u16::from_le_bytes),
        [0xFE, 0xFF, rest @ ..] => utf16(rest, u16::from_be_bytes),
        [0xEF, 0xBB, 0xBF, rest @ ..] => String::from_utf8(rest.to_vec()).ok(),
        _ => String::from_utf8(bytes).ok(),
    }
}

/// The clock of the exporter, in nanoseconds; only differences matter.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// A monotonic clock, as Go's time comparisons are.
pub fn system_clock() -> Clock {
    let start = std::time::Instant::now();
    Arc::new(move || i64::try_from(start.elapsed().as_nanos()).unwrap_or(i64::MAX))
}

/// The Prometheus sink. Clones share the registry.
#[derive(Clone)]
pub struct PrometheusSink {
    exporter: Arc<Mutex<Exporter>>,
    clock: Clock,
    /// `PROMETHEUS_PATH`, the default mux's only pattern here.
    mux: Arc<ServeMux<()>>,
    /// The scrapes being served.
    in_flight: Arc<AtomicU64>,
}

impl std::fmt::Debug for PrometheusSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusSink").finish_non_exhaustive()
    }
}

impl PrometheusSink {
    /// A sink serving `path`, without its listener or its stale-series ticker.
    pub fn new(mapper: Mapper, path: &str, clock: Clock) -> Result<Self, String> {
        Ok(Self {
            exporter: Arc::new(Mutex::new(Exporter::new(mapper))),
            clock,
            mux: Arc::new(metrics_mux(path)?),
            in_flight: Arc::default(),
        })
    }

    /// `NewPrometheusSink`: loads the mapper, then serves on `PROMETHEUS_ADDR` (an address it
    /// cannot listen on is ignored, as upstream) and removes stale series every second.
    pub fn start(settings: &PrometheusSettings) -> Result<Self, String> {
        // Upstream's sink falls back to its defaults for empty settings too.
        let path = match settings.path.as_str() {
            "" => DEFAULT_PATH,
            path => path,
        };
        let sink = Self::new(load_mapper(settings)?, path, system_clock())?;
        let addr = match settings.addr.as_str() {
            "" => DEFAULT_ADDR,
            addr => addr,
        };
        let addr = addr.to_string();
        let server = sink.clone();
        let starting = format!("Starting prometheus sink on {addr}{path}");
        tokio::spawn(async move {
            crate::log::info(&starting);
            if let Ok(listener) = crate::net::listen(&addr, false).await {
                server.serve(listener).await;
            }
        });
        let ticker = sink.clone();
        tokio::spawn(async move {
            let second = std::time::Duration::from_secs(1);
            let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + second, second);
            loop {
                ticks.tick().await;
                ticker.remove_stale_metrics();
            }
        });
        Ok(sink)
    }

    fn handle(&self, name: &str, event: Event) {
        let now = (self.clock)();
        self.exporter.lock().unwrap().handle(name, event, now);
    }

    pub fn flush_counter(&self, name: &str, value: u64) {
        crate::debug!("FlushCounter: {name} {value}");
        self.handle(name, Event::Counter(value as f64));
    }

    pub fn flush_gauge(&self, name: &str, value: u64) {
        crate::debug!("FlushGauge: {name} {value}");
        self.handle(name, Event::Gauge(value as f64));
    }

    pub fn flush_timer(&self, name: &str, value: f64) {
        crate::debug!("FlushTimer: {name} {}", crate::go::format_float(value));
        self.handle(name, Event::Observer(value));
    }

    /// Deletes the series whose TTL has passed (run every second by `start`).
    pub fn remove_stale_metrics(&self) {
        let now = (self.clock)();
        self.exporter.lock().unwrap().remove_stale_metrics(now);
    }

    /// Serves HTTP/1 requests from `listener`, as Go's `http.Serve` with the default mux.
    pub async fn serve(&self, listener: tokio::net::TcpListener) {
        let sink = self.clone();
        let handler = move |request: Request| {
            let sink = sink.clone();
            async move { sink.respond(&request) }
        };
        crate::goserver::serve(listener, handler, std::future::pending()).await;
    }

    fn respond(&self, request: &Request) -> Answer {
        let header = |name| {
            request
                .headers()
                .get(name)
                .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
                .unwrap_or_default()
        };
        let uri = request.uri();
        // A bad escape was refused before routing (`goserver`).
        let path = crate::goserver::request_path(uri).unwrap_or_default();
        let connect = request.method() == hyper::Method::CONNECT;
        match self.mux.route(&path, uri.query(), connect) {
            Route::Handler(()) => self.metrics(&header("accept"), &header("accept-encoding")),
            Route::BadRequest => Answer::star(request.version()),
            Route::Redirect(location) => Answer::redirect(&location, request.method()),
            Route::NotFound => Answer::error(404, "404 page not found"),
        }
    }

    /// promhttp's handler for the default registry.
    fn metrics(&self, accept: &str, accept_encoding: &str) -> Answer {
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        let scraped = self.exporter.lock().unwrap().scrape(in_flight);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        let text = match scraped {
            Ok(text) => text,
            Err(e) => {
                return Answer::error(
                    500,
                    &format!("An error has occurred while serving metrics:\n\n{e}"),
                );
            }
        };
        let mut headers = vec![("content-type", content_type(accept))];
        let body = if gzip_accepted(accept_encoding) {
            headers.push(("content-encoding", "gzip".into()));
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(text.as_bytes()).unwrap();
            encoder.finish().unwrap()
        } else {
            text.into_bytes()
        };
        Answer::new(200, headers, &body)
    }
}

/// `gzipAccepted`: a `gzip` element in the first `Accept-Encoding` header, whatever its q.
fn gzip_accepted(accept_encoding: &str) -> bool {
    accept_encoding
        .split(',')
        .map(str::trim)
        .any(|part| part == "gzip" || part.starts_with("gzip;"))
}

/// expfmt's `Negotiate` without OpenMetrics: always the text format here (a protobuf request
/// gets it too, `docs/parity.md` §7), with the `escaping` parameter of the Accept clauses
/// read up to the one chosen.
fn content_type(accept: &str) -> String {
    let mut escaping = "values".to_string();
    for clause in parse_accept(accept) {
        if let Some(e) = clause.params.get("escaping")
            && matches!(
                e.as_str(),
                "allow-utf-8" | "underscores" | "dots" | "values"
            )
        {
            escaping.clone_from(e);
        }
        let version = clause.params.get("version").map_or("", String::as_str);
        let proto = clause.media == "application/vnd.google.protobuf"
            && clause.params.get("proto").map(String::as_str)
                == Some("io.prometheus.client.MetricFamily")
            && matches!(
                clause.params.get("encoding").map(String::as_str),
                Some("delimited" | "text" | "compact-text")
            );
        if proto || (clause.media == "text/plain" && matches!(version, "0.0.4" | "")) {
            break;
        }
    }
    format!("text/plain; version=0.0.4; charset=utf-8; escaping={escaping}")
}

struct AcceptClause {
    media: String,
    is_wild_type: bool,
    is_wild_subtype: bool,
    q: f64,
    params: std::collections::HashMap<String, String>,
}

/// goautoneg's `ParseAccept`, then its `sort.Sort` by q and specificity (Go's insertion sort
/// below 13 clauses, which keeps this order exact).
fn parse_accept(header: &str) -> Vec<AcceptClause> {
    let mut clauses = Vec::new();
    for part in header.split(',') {
        let part = part.trim_matches(' ');
        let mut pieces = part.split(';');
        let range = pieces.next().unwrap_or("");
        let types: Vec<&str> = range.split('/').collect();
        let main = types[0].trim_matches(' ');
        let sub = match types.len() {
            1 if main == "*" => "*",
            2 => types[1].trim_matches(' '),
            _ => continue,
        };
        let mut clause = AcceptClause {
            media: format!("{main}/{sub}"),
            is_wild_type: main == "*",
            is_wild_subtype: sub == "*",
            q: 1.0,
            params: Default::default(),
        };
        for param in pieces {
            let Some((token, value)) = param.split_once('=') else {
                continue;
            };
            let token = token.trim_matches(' ');
            if token == "q" {
                clause.q = value.parse::<f32>().map_or(0.0, f64::from);
            } else {
                clause
                    .params
                    .insert(token.to_string(), value.trim_matches(' ').to_string());
            }
        }
        clauses.push(clause);
    }
    let less = |a: &AcceptClause, b: &AcceptClause| {
        a.q > b.q
            || (!a.is_wild_type && b.is_wild_type)
            || (!a.is_wild_subtype && b.is_wild_subtype)
    };
    for i in 1..clauses.len() {
        let mut j = i;
        while j > 0 && less(&clauses[j], &clauses[j - 1]) {
            clauses.swap(j, j - 1);
            j -= 1;
        }
    }
    clauses
}

/// `PROMETHEUS_PATH` as the default mux's pattern: a literal path (patterns with a method,
/// a host or wildcards are not supported, §7).
fn metrics_mux(path: &str) -> Result<ServeMux<()>, String> {
    let mut mux = ServeMux::default();
    mux.handle(path, ())
        .map_err(|_| format!("unsupported PROMETHEUS_PATH {path:?}"))?;
    Ok(mux)
}
