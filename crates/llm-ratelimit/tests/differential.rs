//! Differential tests (docs/design.md D32): every scenario runs against envoyproxy/ratelimit
//! itself (the Go program in `tools/oracle`) and against this implementation, each with its own
//! Redis, on the same simulated clock. Responses and the final Redis contents must be identical.
//!
//! Needs `go` and a Redis-compatible server: `redis-server` by default, or the binary named by
//! `RATELIMIT_REDIS_SERVER` (e.g. `valkey-server`). Both the oracle and this implementation
//! use that server. Without them the tests are skipped, unless `RATELIMIT_REQUIRE_DIFFERENTIAL`
//! is set (as in CI), in which case they fail.

use fred::prelude::*;
use fred::types::{ClusterHash, CustomCommand};
use llm_ratelimit::config::{self, ConfigFile, LoadOptions};
use llm_ratelimit::grpc;
use llm_ratelimit::limiter::CacheSettings;
use llm_ratelimit::limiter::Clock;
use llm_ratelimit::redis::RedisCache;
use llm_ratelimit::service::{LimitHeaders, RateLimitService, ServiceSettings};
use llm_ratelimit::stats::prometheus::{self, PrometheusSettings, PrometheusSink};
use llm_ratelimit::stats::{
    DogStatsdSettings, DogStatsdSink, Mogrifier, Sink, Stats, StatsdProtocol, StatsdSink,
};
use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor, rate_limit_descriptor::Entry,
};
use llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::Code;
use llm_ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse, rate_limit_service_client::RateLimitServiceClient,
};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Channel;

#[derive(Default)]
struct ManualClock(AtomicI64);

impl Clock for ManualClock {
    fn unix_now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// The Redis-compatible server to run.
fn redis_server() -> String {
    std::env::var("RATELIMIT_REDIS_SERVER").unwrap_or_else(|_| "redis-server".to_string())
}

fn tools_available() -> bool {
    let found = |tool: &str| {
        std::process::Command::new(tool)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    };
    let available = found("go") && found(&redis_server());
    if !available {
        assert!(
            std::env::var_os("RATELIMIT_REQUIRE_DIFFERENTIAL").is_none(),
            "differential tests need `go` and `{}`",
            redis_server()
        );
        eprintln!(
            "skipping: differential tests need `go` and `{}`",
            redis_server()
        );
    }
    available
}

/// Builds the Go oracle once per test binary.
fn oracle_binary() -> PathBuf {
    static BINARY: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BINARY
        .get_or_init(|| {
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
            let binary = root.join("target/oracle");
            let status = std::process::Command::new("go")
                .args(["build", "-o"])
                .arg(&binary)
                .arg(".")
                .current_dir(root.join("tools/oracle"))
                .status()
                .expect("run go build");
            assert!(status.success(), "go build failed");
            binary
        })
        .clone()
}

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A port for a server that cannot report the one it gets (upstream's Prometheus listener):
/// taken from a range below the ephemeral ports, so that no other test's ephemeral port can
/// take it between this check and the server's bind, and never handed out twice, since the
/// services bind with `SO_REUSEPORT` and two of them would share a port.
fn reserved_port() -> u16 {
    static GIVEN: std::sync::Mutex<Vec<u16>> = std::sync::Mutex::new(Vec::new());
    const RANGE: u16 = 10_000;
    let base = (std::process::id() % RANGE as u32) as u16;
    let mut given = GIVEN.lock().unwrap();
    (0..RANGE)
        .map(|offset| 20_000 + (base + offset) % RANGE)
        .find(|port| {
            !given.contains(port) && std::net::TcpListener::bind(("127.0.0.1", *port)).is_ok()
        })
        .inspect(|port| given.push(*port))
        .expect("a free port")
}

/// A throwaway Redis server, stopped when dropped.
struct Redis {
    _process: Child,
    /// The harness's own connection, for dumps and setup; its commands are not compared.
    client: fred::clients::Client,
    client_address: String,
    address: String,
    monitor: Monitor,
    /// Its working directory, for what it writes (a replica's snapshots).
    _dir: TempDir,
}

/// Every command the server receives, as `MONITOR` reports it.
struct Monitor {
    lines: Arc<std::sync::Mutex<Vec<String>>>,
}

impl Monitor {
    async fn start(address: &str) -> Self {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (read, mut write) = stream.into_split();
        tokio::io::AsyncWriteExt::write_all(&mut write, b"*1\r\n$7\r\nMONITOR\r\n")
            .await
            .unwrap();
        let mut lines = BufReader::new(read).lines();
        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("+OK"));
        let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = collected.clone();
        tokio::spawn(async move {
            let _write = write; // keep the connection open
            while let Ok(Some(line)) = lines.next_line().await {
                sink.lock().unwrap().push(line);
            }
        });
        Self { lines: collected }
    }
}

impl Redis {
    async fn start() -> Self {
        // The port is free when chosen but may be taken before the server binds it (by another
        // test's server, which this one would then share): try again on another port.
        for _ in 0..5 {
            if let Some(redis) = Self::start_on(free_port().await).await {
                return redis;
            }
        }
        panic!("could not start {}", redis_server());
    }

    /// The server started on `port`, once it answers and is the one started here.
    async fn start_on(port: u16) -> Option<Self> {
        Self::start_with(port, &[]).await
    }

    /// [`start_on`](Self::start_on) with `extra` arguments.
    async fn start_with(port: u16, extra: &[&str]) -> Option<Self> {
        // Replication writes snapshots even without `save`: never in the working directory.
        let dir = temp_dir(&format!("redis-{port}"));
        let mut process = Command::new(redis_server())
            .args([
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                &dir.to_string_lossy(),
            ])
            .args(extra)
            .stdout(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start the Redis server");
        let address = format!("127.0.0.1:{port}");
        let config = Config::from_url(&format!("redis://{address}")).unwrap();
        let client = Builder::from_config(config).build().unwrap();
        let mut ready = false;
        for _ in 0..250 {
            if process.try_wait().unwrap().is_some() {
                return None;
            }
            if client.init().await.is_ok() {
                ready = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let server: String = client
            .custom(
                CustomCommand::new_static("INFO", ClusterHash::Random, false),
                vec!["server"],
            )
            .await
            .ok()?;
        let ours = format!("process_id:{}", process.id()?);
        if !ready || !server.lines().any(|line| line.trim() == ours) {
            return None;
        }
        let info: String = client
            .custom(
                CustomCommand::new_static("CLIENT", ClusterHash::Random, false),
                vec!["INFO"],
            )
            .await
            .unwrap();
        let client_address = info
            .split_whitespace()
            .find_map(|field| field.strip_prefix("addr="))
            .unwrap()
            .to_string();
        let monitor = Monitor::start(&address).await;
        Some(Self {
            _process: process,
            client,
            client_address,
            address,
            monitor,
            _dir: dir,
        })
    }

    /// A new connection, for the implementation under test.
    async fn connect(&self) -> fred::clients::Pool {
        let config = Config::from_url(&format!("redis://{}", self.address)).unwrap();
        let pool = Builder::from_config(config).build_pool(1).unwrap();
        pool.init().await.unwrap();
        pool
    }

    /// The data commands received so far from anyone but the harness, in order, as `MONITOR`
    /// renders them (commands run by Lua scripts included).
    async fn commands(&self) -> Vec<String> {
        // Wait until the monitor has seen everything sent before this point.
        let marker = format!("sync-{}", self.monitor.lines.lock().unwrap().len());
        let _: String = self
            .client
            .custom(
                CustomCommand::new_static("ECHO", ClusterHash::Random, false),
                vec![marker.clone()],
            )
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !self
            .monitor
            .lines
            .lock()
            .unwrap()
            .iter()
            .any(|l| l.contains(&marker))
        {
            assert!(
                std::time::Instant::now() < deadline,
                "MONITOR did not report {marker}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        const CONNECTION: &[&str] = &[
            "\"CLIENT\"",
            "\"HELLO\"",
            "\"PING\"",
            "\"COMMAND\"",
            "\"INFO\"",
            "\"SELECT\"",
        ];
        self.monitor
            .lines
            .lock()
            .unwrap()
            .iter()
            .filter_map(|line| {
                let (source, command) = line.split_once("] ")?;
                let from_harness = source.ends_with(&format!(" {}", self.client_address));
                let connection = CONNECTION.iter().any(|c| command.starts_with(c));
                (!from_harness && !connection).then(|| command.to_string())
            })
            .collect()
    }

    /// Every key with its value and absolute expiry time (Unix seconds).
    async fn dump(&self) -> BTreeMap<String, (String, i64)> {
        let keys: Vec<String> = self
            .client
            .custom(
                CustomCommand::new_static("KEYS", ClusterHash::Random, false),
                vec!["*"],
            )
            .await
            .unwrap();
        // Absolute expiry times (EXPIRETIME), so the result does not depend on when each key is
        // read.
        let pipeline = self.client.pipeline();
        for key in &keys {
            let () = pipeline.get(key.as_str()).await.unwrap();
            let expiretime = CustomCommand::new_static("EXPIRETIME", ClusterHash::FirstKey, false);
            let () = pipeline
                .custom(expiretime, vec![key.as_str()])
                .await
                .unwrap();
        }
        let replies: Vec<Value> = if keys.is_empty() {
            Vec::new()
        } else {
            pipeline.all().await.unwrap()
        };
        keys.into_iter()
            .zip(replies.chunks(2))
            // A key may expire between KEYS and GET.
            .filter(|(_, reply)| !reply[0].is_null())
            .map(|(key, reply)| {
                let value: String = reply[0].clone().convert().unwrap();
                let expires: i64 = reply[1].clone().convert().unwrap();
                (key, (value, expires))
            })
            .collect()
    }
}

/// envoyproxy/ratelimit, controlled over HTTP.
struct Oracle {
    _process: Child,
    client: RateLimitServiceClient<Channel>,
    control: String,
}

/// The environment that configures the oracle's service like `settings`. Upstream reads it on
/// every config load. Nothing else is inherited, so a variable set on the developer's machine
/// cannot change the oracle's answers.
fn oracle_environment(settings: &Settings) -> Vec<(&'static str, String)> {
    let load = &settings.load;
    let extra_tags: Vec<String> = settings
        .extra_tags
        .iter()
        .map(|(key, value)| format!("{key}:{value}"))
        .collect();
    let dogstatsd = settings.dogstatsd.as_ref().map(DogStatsd::environment);
    let runtime = settings.runtime.clone();
    let settings = &settings.service;
    let mut environment = vec![
        ("EXTRA_TAGS", extra_tags.join(",")),
        (
            "SANITIZE_DESCRIPTOR_METRIC_DOTS",
            load.sanitize_descriptor_metric_dots.to_string(),
        ),
        ("SHADOW_MODE", settings.global_shadow_mode.to_string()),
        (
            "USE_CALENDAR_MONTH_RATE_LIMIT",
            settings.use_calendar_month.to_string(),
        ),
        ("QUOTA_MODE", settings.global_quota_mode.to_string()),
        (
            "RESPONSE_DYNAMIC_METADATA",
            settings.response_dynamic_metadata.to_string(),
        ),
        (
            "LIMIT_PER_UNIT_HEADERS_ENABLED",
            settings.per_unit_headers.to_string(),
        ),
        (
            "LIMIT_RESPONSE_HEADERS_ENABLED",
            settings.response_headers.is_some().to_string(),
        ),
        (
            "LIMIT_REQUEST_HEADERS_ENABLED",
            settings.request_headers.is_some().to_string(),
        ),
    ];
    if let Some(names) = &settings.response_headers {
        environment.extend([
            ("LIMIT_LIMIT_HEADER", names.limit.clone()),
            ("LIMIT_REMAINING_HEADER", names.remaining.clone()),
            ("LIMIT_RESET_HEADER", names.reset.clone()),
        ]);
    }
    if let Some(names) = &settings.request_headers {
        environment.extend([
            ("LIMIT_REQUEST_LIMIT_HEADER", names.limit.clone()),
            ("LIMIT_REQUEST_REMAINING_HEADER", names.remaining.clone()),
            ("LIMIT_REQUEST_RESET_HEADER", names.reset.clone()),
        ]);
    }
    environment.extend(dogstatsd.into_iter().flatten());
    environment.extend(runtime.into_iter().flatten());
    environment
}

impl Oracle {
    async fn start(
        redis: &Redis,
        per_second: Option<&Redis>,
        all: &Settings,
        statsd: Option<&StatsdServer>,
        dogstatsd: Option<&DogStatsdServer>,
        prometheus: Option<&Prometheus>,
    ) -> Self {
        let settings = &all.cache;
        let near_limit_ratio = settings.near_limit_ratio.to_string();
        let local_cache_size = settings.local_cache_size_in_bytes.to_string();
        let stop_increment = format!(
            "-stop-cache-key-increment={}",
            settings.stop_cache_key_increment_when_overlimit
        );
        let mut process = Command::new(oracle_binary())
            .args(["-redis", &redis.address])
            .args(["-cache-key-prefix", &settings.cache_key_prefix])
            .args(["-local-cache-size", &local_cache_size, &stop_increment])
            .args(["-near-limit-ratio", &near_limit_ratio])
            .arg(format!("-calendar-month={}", settings.use_calendar_month))
            .args(per_second.map(|redis| format!("-redis-per-second={}", redis.address)))
            .args(statsd.map(|server| format!("-statsd={}", server.address)))
            .args(dogstatsd.map(|server| format!("-dogstatsd={}", server.address)))
            .args(prometheus.map(Prometheus::oracle_args).unwrap_or_default())
            .args(all.runtime.as_ref().map(|_| "-file-provider"))
            .env_clear()
            .envs(oracle_environment(all))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start oracle");
        let mut lines = BufReader::new(process.stdout.take().unwrap()).lines();
        let address = |name: &str, line: Option<String>| {
            let line = line.expect("oracle output");
            line.strip_prefix(&format!("{name} "))
                .expect("oracle address")
                .to_string()
        };
        // The oracle waits for Redis before listening; a timeout turns a hang into a failure.
        let mut next_line = async || {
            tokio::time::timeout(std::time::Duration::from_secs(30), lines.next_line())
                .await
                .expect("the oracle did not start within 30 seconds")
                .unwrap()
        };
        let grpc = address("grpc", next_line().await);
        let control = address("control", next_line().await);
        let client = RateLimitServiceClient::connect(format!("http://{grpc}"))
            .await
            .unwrap();
        Self {
            _process: process,
            client,
            control,
        }
    }

    async fn put(&self, path: &str, body: String) {
        let (status, response) = self.send("PUT", path, &body).await;
        assert_eq!(status, 200, "oracle {path}: {response}");
    }

    /// Sends an HTTP/1.0 request (so the answer is not chunked); returns the status and body.
    async fn send(&self, method: &str, path: &str, body: &str) -> (u16, String) {
        let mut stream = tokio::net::TcpStream::connect(&self.control).await.unwrap();
        let request = format!(
            "{method} {path} HTTP/1.0\r\nHost: oracle\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        tokio::io::AsyncWriteExt::write_all(&mut stream, request.as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut stream, &mut response)
            .await
            .unwrap();
        let status = response[9..12].parse().unwrap();
        let body = response.split_once("\r\n\r\n").unwrap().1.to_string();
        (status, body)
    }

    /// The service's counters above 0 (one that wrapped around to 0 is left out too).
    async fn stats(&self) -> BTreeMap<String, u64> {
        let (status, body) = self.send("GET", "/stats", "").await;
        assert_eq!(status, 200, "oracle /stats: {body}");
        body.lines()
            .map(|line| {
                let (name, value) = line.rsplit_once(' ').unwrap();
                (name.to_string(), value.parse().unwrap())
            })
            .filter(|(_, value)| *value > 0)
            .collect()
    }
}

/// This implementation, served over gRPC like the oracle.
struct Subject {
    client: RateLimitServiceClient<Channel>,
    service: Arc<RateLimitService<RedisCache>>,
    clock: Arc<ManualClock>,
    stats: Arc<Stats>,
    sink: Option<Sink>,
    /// With `Settings::prometheus`: the sink, its exporter's clock and where it serves.
    prometheus: Option<(PrometheusSink, Arc<AtomicI64>, String)>,
    /// With `Settings::runtime`, the file provider feeding the service.
    _provider: Option<llm_ratelimit::runtime::FileProvider>,
}

impl Subject {
    async fn start(
        redis: &Redis,
        per_second: Option<&Redis>,
        settings: &Settings,
        statsd: Option<&StatsdServer>,
        dogstatsd: Option<&DogStatsdServer>,
        prometheus: Option<&Prometheus>,
    ) -> Self {
        let clock = Arc::new(ManualClock::default());
        let stats = Arc::new(Stats::new(&settings.extra_tags));
        let mut sink = statsd.map(|server| {
            let port = server.address.port();
            Sink::Statsd(StatsdSink::new("127.0.0.1", port, StatsdProtocol::Tcp))
        });
        if let (Some(server), Some(dog)) = (dogstatsd, &settings.dogstatsd) {
            let port = server.address.port();
            let sink_settings = dog.settings();
            sink = Some(Sink::Dogstatsd(
                DogStatsdSink::new("127.0.0.1", port, sink_settings).unwrap(),
            ));
        }
        let prometheus = match prometheus {
            Some(prometheus) => {
                let exporter_clock = Arc::new(AtomicI64::new(0));
                let now = exporter_clock.clone();
                let mapper = prometheus::load_mapper(&prometheus.settings()).unwrap();
                let clock: prometheus::Clock = Arc::new(move || now.load(Ordering::SeqCst));
                let prometheus_sink = PrometheusSink::new(mapper, prometheus.path, clock).unwrap();
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap().to_string();
                let server = prometheus_sink.clone();
                tokio::spawn(async move { server.serve(listener).await });
                sink = Some(Sink::Prometheus(prometheus_sink.clone()));
                Some((prometheus_sink, exporter_clock, address))
            }
            None => None,
        };
        let mut cache = RedisCache::new(
            redis.connect().await,
            clock.clone(),
            stats.clone(),
            settings.cache.clone(),
        );
        if let Some(redis) = per_second {
            cache = cache.with_per_second(redis.connect().await);
        }
        let service = Arc::new(RateLimitService::new(
            cache,
            clock.clone(),
            stats.clone(),
            settings.service.clone(),
        ));
        let provider = settings.runtime.as_ref().map(|runtime| {
            let lookup = |key: &str| {
                runtime
                    .iter()
                    .chain(&oracle_environment(settings))
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.clone())
            };
            let all = llm_ratelimit::settings::Settings::from_lookup(&lookup).unwrap();
            let service = service.clone();
            llm_ratelimit::runtime::FileProvider::start(&all, &stats, move |config| {
                service.update_config(config)
            })
            .unwrap()
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                // The oracle's server has no interceptor: these stats are kept apart.
                .add_service(grpc::server(
                    service.clone(),
                    llm_ratelimit::stats::ServerStats::new(Arc::new(Stats::default())),
                ))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let client = RateLimitServiceClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        Self {
            client,
            service,
            clock,
            stats,
            sink,
            prometheus,
            _provider: provider,
        }
    }
}

struct Harness {
    oracle: Oracle,
    oracle_redis: Redis,
    subject: Subject,
    subject_redis: Redis,
    /// With `Settings::per_second`, each side's per-second Redis (upstream's, then ours).
    per_second: Option<(Redis, Redis)>,
    load_options: LoadOptions,
    /// With `Settings::statsd`, what the oracle and this implementation sent to statsd.
    statsd: Option<StatsdServers>,
    /// With `Settings::dogstatsd`, what they sent to DogStatsD.
    dogstatsd: Option<DogStatsdServers>,
    /// With `Settings::prometheus`, where the oracle serves its metrics.
    prometheus: Option<String>,
    prometheus_path: &'static str,
    flushes: std::sync::atomic::AtomicUsize,
}

/// Everything both sides are configured with.
#[derive(Default)]
struct Settings {
    cache: CacheSettings,
    /// `REDIS_PERSECOND`: limits in seconds in a second Redis on each side.
    per_second: bool,
    service: ServiceSettings,
    load: LoadOptions,
    /// `EXTRA_TAGS`.
    extra_tags: BTreeMap<String, String>,
    /// Also send the stats to a statsd server for each side, and compare what they receive.
    statsd: bool,
    /// The same with DogStatsD, configured so.
    dogstatsd: Option<DogStatsd>,
    /// Also send the stats to the Prometheus sink on each side, and compare what they serve.
    prometheus: Option<Prometheus>,
    /// Load the config with each side's file provider, configured by these `RUNTIME_*`
    /// variables, instead of pushing it.
    runtime: Option<Vec<(&'static str, String)>>,
}

/// The Prometheus sink's configuration, for both sides.
#[derive(Clone)]
struct Prometheus {
    /// `PROMETHEUS_PATH`.
    path: &'static str,
    /// `PROMETHEUS_MAPPER_YAML`: a file with this content, a missing file, or the default.
    mapper: MapperFile,
    /// `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS`.
    milliseconds: bool,
    /// Where the oracle serves it (a free port, as upstream cannot report the one it gets).
    oracle_address: String,
}

#[derive(Clone)]
enum MapperFile {
    Default,
    Content(PathBuf),
    Missing,
}

impl MapperFile {
    /// A temporary file holding `content`.
    async fn write(content: &[u8]) -> Self {
        let path = std::env::temp_dir().join(format!(
            "ratelimit-mapper-{}-{}.yaml",
            std::process::id(),
            free_port().await
        ));
        std::fs::write(&path, content).unwrap();
        MapperFile::Content(path)
    }
}

impl Prometheus {
    async fn new(mapper: Option<&str>) -> Self {
        let mapper = match mapper {
            None => MapperFile::Default,
            Some(content) => MapperFile::write(content.as_bytes()).await,
        };
        Self {
            path: "/metrics",
            mapper,
            milliseconds: false,
            oracle_address: format!("127.0.0.1:{}", reserved_port()),
        }
    }

    fn mapper_path(&self) -> String {
        match &self.mapper {
            MapperFile::Default => String::new(),
            MapperFile::Content(path) => path.to_string_lossy().into_owned(),
            MapperFile::Missing => "/nonexistent/ratelimit-mapper.yaml".into(),
        }
    }

    fn settings(&self) -> PrometheusSettings {
        PrometheusSettings {
            addr: String::new(),
            path: self.path.into(),
            mapper_yaml: self.mapper_path(),
            response_time_as_milliseconds: self.milliseconds,
        }
    }

    fn oracle_args(&self) -> Vec<String> {
        vec![
            format!("-prometheus={}", self.oracle_address),
            format!("-prometheus-path={}", self.path),
            format!("-prometheus-mapper={}", self.mapper_path()),
            format!("-prometheus-ms={}", self.milliseconds),
        ]
    }
}

/// An HTTP answer, as far as the two sides must agree on it.
#[derive(Debug, PartialEq)]
struct Scraped {
    status: u16,
    /// Content-Type, Content-Encoding, Location and X-Content-Type-Options.
    headers: BTreeMap<String, String>,
    /// Decompressed.
    body: String,
}

/// Sends an HTTP/1.0 request to `address`; returns the answer.
async fn http_request(
    address: &str,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
) -> Scraped {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = None;
    // Up to 30 s: a loaded machine can be slow to start a listener.
    for _ in 0..3000 {
        match tokio::net::TcpStream::connect(address).await {
            Ok(connected) => {
                stream = Some(connected);
                break;
            }
            // The oracle's listener starts in the background.
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
        }
    }
    let mut stream = stream.expect("the Prometheus listener");
    let mut request = format!("{method} {target} HTTP/1.0\r\nHost: scrape\r\n");
    for (name, value) in headers {
        request += &format!("{name}: {value}\r\n");
    }
    request += "\r\n";
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("an HTTP answer");
    let head = String::from_utf8_lossy(&response[..split]).into_owned();
    let raw_body = &response[split + 4..];
    let mut lines = head.lines();
    let status = lines.next().unwrap()[9..12].parse().unwrap();
    let mut kept = BTreeMap::new();
    for line in lines {
        let (name, value) = line.split_once(':').unwrap();
        let name = name.trim().to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "content-type" | "content-encoding" | "location" | "x-content-type-options"
        ) {
            kept.insert(name, value.trim().to_string());
        }
    }
    let body = match kept.get("content-encoding").map(String::as_str) {
        Some("gzip") => {
            let mut text = String::new();
            std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(raw_body), &mut text)
                .unwrap();
            text
        }
        _ => String::from_utf8_lossy(raw_body).into_owned(),
    };
    Scraped {
        status,
        headers: kept,
        body,
    }
}

/// Upstream's answer without what only Go serves: the `go_*` and `process_*` families
/// (docs/parity.md §7). In Gather errors, which print protobuf messages with randomized
/// spacing and a creation time, spaces are collapsed and the time left out.
fn comparable(mut scraped: Scraped, upstream: bool) -> Scraped {
    if upstream {
        scraped.body = scraped
            .body
            .lines()
            .filter(|line| {
                let name = line
                    .strip_prefix("# HELP ")
                    .or_else(|| line.strip_prefix("# TYPE "))
                    .unwrap_or(line);
                !name.starts_with("go_") && !name.starts_with("process_")
            })
            .map(|line| format!("{line}\n"))
            .collect();
    }
    if scraped.status == 500 {
        let created = regex::Regex::new(r"created_timestamp:\s*\{[^}]*\}\s*").unwrap();
        let body = created.replace_all(&scraped.body, "");
        let spaces = regex::Regex::new(r" +").unwrap();
        scraped.body = spaces.replace_all(&body, " ").replace(" }", "}");
        // Go gathers concurrently: which of two help texts is the expected one, and so the
        // series named, is random (docs/parity.md §7). The pair is kept, not its order.
        let help = regex::Regex::new(
            r#"collected metric (\S+) .* has help "([^"]*)" but should have "([^"]*)""#,
        )
        .unwrap();
        scraped.body = help
            .replace_all(&scraped.body, |c: &regex::Captures| {
                let mut helps = [c[2].to_string(), c[3].to_string()];
                helps.sort();
                format!("collected metric {} has helps {helps:?}", &c[1])
            })
            .into_owned();
    }
    scraped
}

/// A mogrifier, for both sides.
#[derive(Clone)]
struct MogrifierSpec {
    key: &'static str,
    pattern: &'static str,
    name: &'static str,
    tags: Vec<(&'static str, &'static str)>,
}

/// The DogStatsD sink's configuration, for both sides.
#[derive(Default, Clone)]
struct DogStatsd {
    /// `DD_ENTITY_ID`, `DD_ENV`, `DD_SERVICE`, `DD_VERSION`.
    entity_id: String,
    env: String,
    service: String,
    version: String,
    mogrifiers: Vec<MogrifierSpec>,
}

impl DogStatsd {
    fn settings(&self) -> DogStatsdSettings {
        DogStatsdSettings {
            entity_id: self.entity_id.clone(),
            env: self.env.clone(),
            service: self.service.clone(),
            version: self.version.clone(),
            mogrifiers: self
                .mogrifiers
                .iter()
                .map(|m| {
                    let tags = m.tags.iter().map(|(k, v)| (k.to_string(), v.to_string()));
                    Mogrifier::new(m.key, m.pattern, m.name, tags.collect()).unwrap()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// The oracle's environment for the same configuration (`DOG_STATSD_MOGRIFIER_<KEY>_*`).
    fn environment(&self) -> Vec<(&'static str, String)> {
        let leak = |s: String| -> &'static str { Box::leak(s.into_boxed_str()) };
        let mut environment = vec![
            ("DD_ENTITY_ID", self.entity_id.clone()),
            ("DD_ENV", self.env.clone()),
            ("DD_SERVICE", self.service.clone()),
            ("DD_VERSION", self.version.clone()),
        ];
        let keys: Vec<&str> = self.mogrifiers.iter().map(|m| m.key).collect();
        environment.push(("USE_DOG_STATSD_MOGRIFIERS", keys.join(",")));
        for m in &self.mogrifiers {
            let var = |field: &str| leak(format!("DOG_STATSD_MOGRIFIER_{}_{field}", m.key));
            let tags: Vec<String> = m.tags.iter().map(|(k, v)| format!("{k}:{v}")).collect();
            environment.push((var("PATTERN"), m.pattern.to_string()));
            environment.push((var("NAME"), m.name.to_string()));
            environment.push((var("TAGS"), tags.join(",")));
        }
        environment
    }
}

/// The DogStatsD servers each side sends its stats to.
struct DogStatsdServers {
    oracle: DogStatsdServer,
    subject: DogStatsdServer,
}

/// A DogStatsD server over UDP, keeping every datagram it receives.
struct DogStatsdServer {
    address: std::net::SocketAddr,
    datagrams: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

impl DogStatsdServer {
    async fn start() -> Self {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let datagrams = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = datagrams.clone();
        tokio::spawn(async move {
            let mut buffer = vec![0; 65536];
            while let Ok(n) = socket.recv(&mut buffer).await {
                sink.lock().unwrap().push(buffer[..n].to_vec());
            }
        });
        Self { address, datagrams }
    }

    /// Every line received once `count` flushes have arrived and nothing more for 300 ms (the
    /// client sends every 100 ms, from 32 buffers in turn), sorted; datadog-go's telemetry and
    /// the oracle's Redis client stats are left out. Every datagram holds whole lines, at most
    /// 1432 bytes of them.
    async fn lines(&self, count: usize) -> Vec<String> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut seen = 0;
        let mut quiet_since = std::time::Instant::now();
        loop {
            let datagrams = self.datagrams.lock().unwrap().clone();
            if datagrams.len() != seen {
                seen = datagrams.len();
                quiet_since = std::time::Instant::now();
            }
            let text: Vec<String> = datagrams
                .iter()
                .map(|d| String::from_utf8(d.clone()).unwrap())
                .collect();
            let flushes = text
                .iter()
                .flat_map(|d| d.lines())
                .filter(|line| line.starts_with("harness.flushed:"))
                .count();
            if flushes >= count && quiet_since.elapsed() >= std::time::Duration::from_millis(300) {
                for datagram in &text {
                    assert!(
                        datagram.len() <= 1432 && datagram.ends_with('\n'),
                        "datagram {datagram:?}"
                    );
                }
                let mut lines: Vec<String> = text
                    .iter()
                    .flat_map(|d| d.lines())
                    .filter(|line| {
                        !line.starts_with("datadog.dogstatsd.client.")
                            && !line.starts_with("redis.")
                    })
                    .map(str::to_string)
                    .collect();
                lines.sort();
                return lines;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "DogStatsD received {text:?}, expected {count} flushes"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
}

/// The flush that marks the end of each flush in a statsd stream.
const FLUSHED: &str = "harness.flushed:1|c";

/// The statsd servers each side sends its stats to.
struct StatsdServers {
    oracle: StatsdServer,
    subject: StatsdServer,
}

/// A statsd server over TCP, keeping every line it receives.
struct StatsdServer {
    address: std::net::SocketAddr,
    lines: Arc<std::sync::Mutex<Vec<String>>>,
}

impl StatsdServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let lines = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = lines.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let sink = sink.clone();
                tokio::spawn(async move {
                    let mut stream = BufReader::new(stream).lines();
                    while let Ok(Some(line)) = stream.next_line().await {
                        sink.lock().unwrap().push(line);
                    }
                });
            }
        });
        Self { address, lines }
    }

    /// The lines of the first `count` flushes, each sorted (a flush's order is not defined).
    /// The stats of the oracle's own Redis client are left out.
    async fn flushes(&self, count: usize) -> Vec<Vec<String>> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let lines = self.lines.lock().unwrap().clone();
            let mut flushes = lines
                .split(|line| line == FLUSHED)
                .map(|flush| {
                    let mut flush: Vec<String> = flush
                        .iter()
                        .filter(|line| !line.starts_with("redis."))
                        .cloned()
                        .collect();
                    flush.sort();
                    flush
                })
                .collect::<Vec<_>>();
            flushes.pop(); // after the last marker
            if flushes.len() >= count {
                flushes.truncate(count);
                return flushes;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "statsd received {lines:?}, expected {count} flushes"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
}

impl Harness {
    async fn start(config: &str) -> Self {
        Self::with_settings(config, CacheSettings::default()).await
    }

    async fn with_prefix(config: &str, prefix: &str) -> Self {
        let settings = CacheSettings {
            cache_key_prefix: prefix.to_string(),
            ..Default::default()
        };
        Self::with_settings(config, settings).await
    }

    async fn with_settings(config: &str, cache: CacheSettings) -> Self {
        let settings = Settings {
            cache,
            ..Default::default()
        };
        Self::configured(config, settings).await
    }

    async fn with_service_settings(config: &str, service: ServiceSettings) -> Self {
        let settings = Settings {
            service,
            ..Default::default()
        };
        Self::configured(config, settings).await
    }

    /// Both sides configured alike: the settings for this implementation, the matching flags
    /// and environment for the oracle.
    async fn configured(config: &str, settings: Settings) -> Self {
        let oracle_redis = Redis::start().await;
        let subject_redis = Redis::start().await;
        let per_second = match settings.per_second {
            true => Some((Redis::start().await, Redis::start().await)),
            false => None,
        };
        let statsd = match settings.statsd {
            true => Some(StatsdServers {
                oracle: StatsdServer::start().await,
                subject: StatsdServer::start().await,
            }),
            false => None,
        };
        let dogstatsd = match settings.dogstatsd {
            Some(_) => Some(DogStatsdServers {
                oracle: DogStatsdServer::start().await,
                subject: DogStatsdServer::start().await,
            }),
            None => None,
        };
        let oracle = Oracle::start(
            &oracle_redis,
            per_second.as_ref().map(|(oracle, _)| oracle),
            &settings,
            statsd.as_ref().map(|s| &s.oracle),
            dogstatsd.as_ref().map(|s| &s.oracle),
            settings.prometheus.as_ref(),
        )
        .await;
        let subject = Subject::start(
            &subject_redis,
            per_second.as_ref().map(|(_, subject)| subject),
            &settings,
            statsd.as_ref().map(|s| &s.subject),
            dogstatsd.as_ref().map(|s| &s.subject),
            settings.prometheus.as_ref(),
        )
        .await;
        let runtime = settings.runtime.is_some();
        let mut h = Self {
            oracle,
            oracle_redis,
            subject,
            subject_redis,
            per_second,
            load_options: settings.load,
            statsd,
            dogstatsd,
            prometheus_path: settings.prometheus.as_ref().map_or("/metrics", |p| p.path),
            prometheus: settings.prometheus.map(|p| p.oracle_address),
            flushes: Default::default(),
        };
        match runtime {
            true => h.await_config_loads(1).await,
            false => h.load_config(config).await.unwrap(),
        }
        h
    }

    /// Waits until each side has loaded a config (or failed to) `count` times in all, then
    /// compares their stats.
    async fn await_config_loads(&self, count: u64) {
        let loads = |stats: &BTreeMap<String, u64>| {
            stats
                .iter()
                .filter(|(name, _)| name.starts_with("ratelimit.service.config_load_"))
                .map(|(_, value)| value)
                .sum::<u64>()
        };
        for _ in 0..500 {
            let upstream = loads(&self.oracle.stats().await);
            let ours = loads(&self.subject.stats.snapshot());
            if upstream >= count && ours >= count {
                self.assert_same_stats().await;
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("config loads did not reach {count}");
    }

    /// Loads `config` on both sides, as a reload; returns upstream's error if it does not load,
    /// after checking ours is the same and both kept the previous config.
    async fn load_config(&mut self, config: &str) -> Result<(), String> {
        let (status, error) = self.oracle.send("PUT", "/config", config).await;
        let file = ConfigFile {
            name: "config.yaml".into(),
            content: config.into(),
        };
        let loaded = config::load(&[file], &self.load_options);
        let actual = loaded.as_ref().map(|_| ()).map_err(|e| e.to_string());
        self.subject.service.update_config(loaded);
        let expected = match status {
            200 => Ok(()),
            _ => Err(error.trim_end().to_string()),
        };
        assert_eq!(actual, expected, "loading {config}");
        self.assert_same_stats().await;
        expected
    }

    /// Flushes the stats on both sides and compares the counters, and what statsd received.
    async fn assert_same_stats(&self) {
        let expected = self.oracle.stats().await;
        if let Some(sink) = &self.subject.sink {
            self.subject.stats.flush(sink).await;
            if !matches!(sink, Sink::Prometheus(_)) {
                sink.flush_counter("harness.flushed", 1);
            }
            sink.flush().await;
        }
        assert_eq!(self.subject.stats.snapshot(), expected, "counters");
        let count = self.flushes.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(dogstatsd) = &self.dogstatsd {
            assert_eq!(
                dogstatsd.subject.lines(count).await,
                dogstatsd.oracle.lines(count).await,
                "DogStatsD lines"
            );
        }
        if let Some(statsd) = &self.statsd {
            assert_eq!(
                statsd.subject.flushes(count).await,
                statsd.oracle.flushes(count).await,
                "statsd lines, per flush"
            );
        }
        if self.prometheus.is_some() {
            self.scrape("GET", self.prometheus_path, &[]).await;
        }
    }

    /// Sends the same request to both Prometheus listeners and checks they answer the same;
    /// returns upstream's answer.
    async fn scrape(&self, method: &str, target: &str, headers: &[(&str, &str)]) -> Scraped {
        let oracle = self.prometheus.as_ref().expect("Settings::prometheus");
        let subject = &self.subject.prometheus.as_ref().unwrap().2;
        let expected = comparable(http_request(oracle, method, target, headers).await, true);
        let actual = comparable(http_request(subject, method, target, headers).await, false);
        assert_eq!(actual, expected, "{method} {target} {headers:?}");
        expected
    }

    /// Flushes `stats` (type, name, value) straight into both Prometheus sinks, then compares
    /// what they serve.
    async fn flush_to_prometheus(&self, stats: &[(&str, &str, f64)]) -> Scraped {
        let body: String = stats
            .iter()
            .map(|(kind, name, value)| format!("{kind} {name} {value}\n"))
            .collect();
        let (status, response) = self.oracle.send("POST", "/flush", &body).await;
        assert_eq!(status, 200, "oracle /flush: {response}");
        let sink = &self.subject.prometheus.as_ref().unwrap().0;
        for (kind, name, value) in stats {
            match *kind {
                "counter" => sink.flush_counter(name, *value as u64),
                "gauge" => sink.flush_gauge(name, *value as u64),
                "timer" => sink.flush_timer(name, *value),
                other => panic!("unknown stat type {other}"),
            }
        }
        self.scrape("GET", self.prometheus_path, &[]).await
    }

    /// Moves statsd_exporter's clock on both sides (nanoseconds), then lets them remove stale
    /// series, and compares what they serve.
    async fn exporter_at(&self, nanos: i64) -> Scraped {
        self.oracle.put("/exporter-time", nanos.to_string()).await;
        let (sink, clock, _) = self.subject.prometheus.as_ref().unwrap();
        clock.store(nanos, Ordering::SeqCst);
        sink.remove_stale_metrics();
        self.scrape("GET", self.prometheus_path, &[]).await
    }

    async fn at(&mut self, now: i64) {
        self.oracle.put("/time", now.to_string()).await;
        self.subject.clock.0.store(now, Ordering::SeqCst);
    }

    /// Sends `request` to both and checks they answer the same; returns the answer.
    async fn call(&mut self, request: RateLimitRequest) -> Result<RateLimitResponse, String> {
        let expected = self.oracle.client.should_rate_limit(request.clone()).await;
        let actual = self.subject.client.should_rate_limit(request.clone()).await;
        let simplify = |r: Result<tonic::Response<RateLimitResponse>, tonic::Status>| {
            r.map(tonic::Response::into_inner)
                .map_err(|s| format!("{:?}: {}", s.code(), s.message()))
        };
        let (mut expected, actual) = (simplify(expected), simplify(actual));
        if let Ok(response) = &mut expected {
            single_space_limit_text(response);
        }
        assert_eq!(actual, expected, "response to {request:?}");
        self.assert_same_stats().await;
        actual
    }

    /// Same keys and values, and expiry times within a second: TTLs run on the servers' real
    /// clocks, and each side sets them a moment apart.
    async fn assert_same_redis(&self) {
        if std::env::var_os("RATELIMIT_SHOW_COMMANDS").is_some() {
            eprintln!("{:#?}", self.oracle_redis.commands().await);
        }
        assert_eq!(
            self.subject_redis.commands().await,
            self.oracle_redis.commands().await,
            "commands received by Redis"
        );
        let (expected, actual) = tokio::join!(self.oracle_redis.dump(), self.subject_redis.dump());
        assert!(
            !expected.is_empty(),
            "the scenario should leave keys in Redis"
        );
        let values = |dump: &BTreeMap<String, (String, i64)>| {
            dump.iter()
                .map(|(k, (v, _))| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(values(&actual), values(&expected), "Redis keys and values");
        for (key, (_, expires)) in &expected {
            let actual_expires = actual[key].1;
            assert!(
                (actual_expires - expires).abs() <= 1,
                "expiry of {key}: {actual_expires}, upstream {expires}"
            );
        }
        if let Some((oracle, subject)) = &self.per_second {
            assert_eq!(
                subject.commands().await,
                oracle.commands().await,
                "commands received by the per-second Redis"
            );
            let (expected, actual) = tokio::join!(oracle.dump(), subject.dump());
            assert_eq!(
                values(&actual),
                values(&expected),
                "per-second keys and values"
            );
        }
    }

    /// Stores a raw value under `key` in both Redis servers.
    async fn set_raw(&self, key: &str, value: &str) {
        for redis in [&self.oracle_redis, &self.subject_redis] {
            let () = redis
                .client
                .set(key, value, None, None, false)
                .await
                .unwrap();
        }
    }
}

/// Go's text format puts one or two spaces between fields, depending on the binary
/// (protobuf-go's `detrand`); ours always puts one. Applied to the oracle's
/// `descriptors[].limit` metadata.
fn single_space_limit_text(response: &mut RateLimitResponse) {
    use prost_types::value::Kind;
    let Some(metadata) = &mut response.dynamic_metadata else {
        return;
    };
    let Some(Kind::ListValue(descriptors)) = metadata
        .fields
        .get_mut("descriptors")
        .and_then(|v| v.kind.as_mut())
    else {
        return;
    };
    for descriptor in &mut descriptors.values {
        if let Some(Kind::StructValue(descriptor)) = &mut descriptor.kind
            && let Some(Kind::StringValue(limit)) = descriptor
                .fields
                .get_mut("limit")
                .and_then(|v| v.kind.as_mut())
        {
            *limit = limit.split_whitespace().collect::<Vec<_>>().join(" ");
        }
    }
}

fn request(domain: &str, descriptors: &[&[(&str, &str)]]) -> RateLimitRequest {
    RateLimitRequest {
        domain: domain.into(),
        descriptors: descriptors
            .iter()
            .map(|entries| RateLimitDescriptor {
                entries: entries
                    .iter()
                    .map(|(k, v)| Entry {
                        key: k.to_string(),
                        value: v.to_string(),
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        hits_addend: 0,
    }
}

#[tokio::test]
async fn fixed_window_counts_and_resets_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(
        "
domain: d
descriptors:
  - key: k
    rate_limit: {unit: second, requests_per_unit: 2}
  - key: m
    rate_limit: {unit: minute, requests_per_unit: 10}
",
    )
    .await;

    h.at(1_000_000_000).await;
    for _ in 0..3 {
        h.call(request(
            "d",
            &[&[("k", "v")], &[("m", "x")], &[("none", "y")]],
        ))
        .await
        .unwrap();
    }
    h.at(1_000_000_001).await;
    h.call(request("d", &[&[("k", "v")]])).await.unwrap();
    h.at(1_000_000_059).await;
    h.call(request("d", &[&[("m", "x")]])).await.unwrap();
    h.assert_same_redis().await;
}

/// `USE_CALENDAR_MONTH_RATE_LIMIT`: a month limit counts in the UTC calendar month (its
/// key, its TTL, its reset, refunds included, and the response's and request's reset
/// headers), across short, leap and year-end months; off, a month is 30 days from the epoch.
/// Other units are not affected.
#[tokio::test]
async fn calendar_months_are_counted_like_upstream() {
    if !tools_available() {
        return;
    }
    const MONTHLY: &str = "
domain: d
descriptors:
  - key: monthly
    rate_limit: {unit: month, requests_per_unit: 2}
  - key: daily
    rate_limit: {unit: day, requests_per_unit: 5}
";
    for calendar in [true, false] {
        let settings = Settings {
            cache: CacheSettings {
                local_cache_size_in_bytes: 1 << 20,
                use_calendar_month: calendar,
                ..Default::default()
            },
            service: ServiceSettings {
                response_headers: Some(LimitHeaders::default()),
                request_headers: Some(LimitHeaders {
                    limit: "X-Limit".into(),
                    remaining: "X-Remaining".into(),
                    reset: "X-Reset".into(),
                }),
                use_calendar_month: calendar,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut h = Harness::configured(MONTHLY, settings).await;
        for now in [
            1_706_745_599, // 2024-01-31 23:59:59
            1_706_745_600, // 2024-02-01 00:00:00
            1_709_208_000, // 2024-02-29 12:00:00, a leap year
            1_677_625_200, // 2023-02-28 23:00:00
            1_710_460_800, // 2024-03-15 00:00:00
            1_735_689_570, // 2024-12-31 23:59:30
        ] {
            h.at(now).await;
            for _ in 0..4 {
                h.call(request("d", &[&[("monthly", "m")], &[("daily", "x")]]))
                    .await
                    .unwrap();
            }
            h.call(refund(request("d", &[&[("monthly", "m")]]), 1))
                .await
                .unwrap();
        }
        h.assert_same_redis().await;
    }
}

/// `REDIS_PERSECOND`: limits in seconds go to the per-second Redis, in pipelines of their
/// own (the reads of `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` too), the others to the main
/// one; each Redis receives what upstream's does.
#[tokio::test]
async fn per_second_limits_use_their_own_redis_like_upstream() {
    if !tools_available() {
        return;
    }
    const MIXED: &str = "
domain: d
descriptors:
  - key: fast
    rate_limit: {unit: second, requests_per_unit: 2}
  - key: slow
    rate_limit: {unit: minute, requests_per_unit: 3}
";
    for stop_increment in [false, true] {
        let settings = Settings {
            per_second: true,
            cache: CacheSettings {
                stop_cache_key_increment_when_overlimit: stop_increment,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut h = Harness::configured(MIXED, settings).await;
        h.at(1_700_000_000).await;
        for _ in 0..4 {
            h.call(request("d", &[&[("fast", "a")], &[("slow", "b")]]))
                .await
                .unwrap();
        }
        h.call(request("d", &[&[("fast", "only")]])).await.unwrap();
        h.call(request("d", &[&[("slow", "only")]])).await.unwrap();
        h.call(refund(
            request("d", &[&[("fast", "a")], &[("slow", "b")]]),
            1,
        ))
        .await
        .unwrap();
        h.at(1_700_000_001).await;
        h.call(request("d", &[&[("fast", "a")]])).await.unwrap();
        h.assert_same_redis().await;
    }
}

fn with_hits(
    mut request: RateLimitRequest,
    request_hits: u32,
    per_descriptor: &[Option<u64>],
) -> RateLimitRequest {
    request.hits_addend = request_hits;
    for (descriptor, hits) in request.descriptors.iter_mut().zip(per_descriptor) {
        descriptor.hits_addend = *hits;
    }
    request
}

fn refund(mut request: RateLimitRequest, hits: u64) -> RateLimitRequest {
    for descriptor in &mut request.descriptors {
        descriptor.hits_addend = Some(hits);
        descriptor.is_negative_hits = true;
    }
    request
}

const TOKENS: &str = "
domain: llm
descriptors:
  - key: user
    rate_limit: {unit: minute, requests_per_unit: 1000}
  - key: shadow
    shadow_mode: true
    rate_limit: {unit: hour, requests_per_unit: 1}
";

#[tokio::test]
async fn hits_addend_checks_and_counts_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "alice")]]);

    h.call(with_hits(user(), 0, &[Some(0)])).await.unwrap(); // check only
    h.call(with_hits(user(), 300, &[None])).await.unwrap(); // request-level
    h.call(with_hits(user(), 7, &[Some(600)])).await.unwrap(); // descriptor wins
    h.call(with_hits(user(), 0, &[Some(0)])).await.unwrap(); // at 900
    h.call(with_hits(user(), 0, &[Some(200)])).await.unwrap(); // over: 1100
    h.call(with_hits(user(), 0, &[Some(0)])).await.unwrap(); // still over
    h.assert_same_redis().await;
}

#[tokio::test]
async fn refunds_floor_at_zero_and_always_answer_ok() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "bob")]]);

    h.call(refund(user(), 50)).await.unwrap(); // absent key: nothing to refund
    h.call(with_hits(user(), 0, &[Some(1200)])).await.unwrap(); // over
    h.call(refund(user(), 500)).await.unwrap(); // back to 700
    h.call(refund(user(), 5000)).await.unwrap(); // floors at 0
    h.call(with_hits(user(), 0, &[Some(1)])).await.unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn shadow_mode_rules_count_but_answer_ok() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    for _ in 0..3 {
        h.call(request("llm", &[&[("shadow", "x")]])).await.unwrap();
    }
    h.assert_same_redis().await;
}

#[tokio::test]
async fn keys_follow_upstream_for_every_unit_and_option() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::with_prefix(
        "
domain: keys
descriptors:
  - key: s
    rate_limit: {unit: second, requests_per_unit: 5}
  - key: m
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: h
    rate_limit: {unit: hour, requests_per_unit: 5}
  - key: d
    rate_limit: {unit: day, requests_per_unit: 5}
  - key: w
    rate_limit: {unit: week, requests_per_unit: 5}
  - key: mo
    rate_limit: {unit: month, requests_per_unit: 5}
  - key: y
    rate_limit: {unit: year, requests_per_unit: 5}
  - key: files
    value: files/*
    share_threshold: true
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: parent
    value: p
    descriptors:
      - key: child
        rate_limit: {unit: minute, requests_per_unit: 5}
  - key: open
    rate_limit: {unlimited: true, name: open, replaces: [{name: replaced}]}
  - key: replaced
    rate_limit: {unit: minute, requests_per_unit: 5, name: replaced}
",
        "prefix:",
    )
    .await;
    h.at(1_712_345_678).await;
    for unit in ["s", "m", "h", "d", "w", "mo", "y"] {
        h.call(request("keys", &[&[(unit, "v")]])).await.unwrap();
    }
    h.call(request(
        "keys",
        &[&[("files", "files/a.pdf")], &[("files", "files/b.csv")]],
    ))
    .await
    .unwrap();
    h.call(request("keys", &[&[("parent", "p"), ("child", "c")]]))
        .await
        .unwrap();
    h.call(request("keys", &[&[("open", "x")], &[("replaced", "y")]]))
        .await
        .unwrap();
    h.call(request("keys", &[&[("unknown", "x")]]))
        .await
        .unwrap();
    // A request limit override applies even without a matching rule.
    let mut overridden = request("keys", &[&[("free", "x")]]);
    overridden.descriptors[0].limit = Some(
        llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride {
            requests_per_unit: 1,
            unit: 2, // MINUTE
            unit_multiplier: None,
        },
    );
    h.call(overridden.clone()).await.unwrap();
    h.call(overridden).await.unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn invalid_requests_fail_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    assert!(h.call(request("", &[&[("user", "x")]])).await.is_err());
    assert!(h.call(request("llm", &[])).await.is_err());
}

#[tokio::test]
async fn huge_refunds_are_sent_as_signed_integers_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "dave")]]);

    h.call(with_hits(user(), 0, &[Some(1)])).await.unwrap();
    // u64::MAX goes over the wire as the signed integer -1, so the refund adds one.
    h.call(refund(user(), u64::MAX)).await.unwrap();
    h.call(with_hits(user(), 0, &[Some(u64::MAX)]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn redis_errors_are_reported_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let window = 1_700_000_000 / 60 * 60;
    h.set_raw(&format!("llm_user_carol_{window}"), "not a number")
        .await;

    let error = h
        .call(request("llm", &[&[("user", "carol")]]))
        .await
        .unwrap_err();
    assert!(error.contains("in pipeline returned error"), "{error}");
    h.call(refund(request("llm", &[&[("user", "carol")]]), 1))
        .await
        .unwrap_err();
}

#[tokio::test]
async fn refunds_reset_the_ttl_to_a_full_window() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "erin")]]);

    h.call(with_hits(user(), 0, &[Some(10)])).await.unwrap();
    // TTLs run on the servers' real clocks: let them drop before refunding.
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    h.call(refund(user(), 4)).await.unwrap();
    h.assert_same_redis().await;
    // A refund in a later window finds no key.
    h.at(1_700_000_060).await;
    h.call(refund(user(), 4)).await.unwrap();
}

#[tokio::test]
async fn empty_values_and_nested_shared_wildcards_make_upstream_keys() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(
        "
domain: keys
descriptors:
  - key: k
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: user
    value: user/*
    share_threshold: true
    descriptors:
      - key: resource
        value: res/*
        share_threshold: true
        descriptors:
          - key: action
            rate_limit: {unit: minute, requests_per_unit: 5}
",
    )
    .await;
    h.at(1_712_345_678).await;
    h.call(request("keys", &[&[("k", "")]])).await.unwrap();
    h.call(request(
        "keys",
        &[&[
            ("user", "user/alice"),
            ("resource", "res/file1"),
            ("action", "read"),
        ]],
    ))
    .await
    .unwrap();
    h.call(request(
        "keys",
        &[&[
            ("user", "user/bob"),
            ("resource", "res/file2"),
            ("action", "write"),
        ]],
    ))
    .await
    .unwrap();
    h.assert_same_redis().await;
}

const OVERLIMIT: &str = "
domain: d
descriptors:
  - key: a
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: b
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: shadow
    shadow_mode: true
    rate_limit: {unit: minute, requests_per_unit: 1}
";

fn hits_each(descriptors: &[&[(&str, &str)]], hits: &[u64]) -> RateLimitRequest {
    let hits: Vec<_> = hits.iter().map(|h| Some(*h)).collect();
    with_hits(request("d", descriptors), 0, &hits)
}

#[tokio::test]
async fn stop_cache_key_increment_when_overlimit_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = CacheSettings {
        stop_cache_key_increment_when_overlimit: true,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;
    let ab: &[&[(&str, &str)]] = &[&[("a", "x")], &[("b", "y")]];

    h.call(hits_each(ab, &[4, 1])).await.unwrap(); // nobody near: both counted
    h.call(hits_each(ab, &[2, 1])).await.unwrap(); // a goes over: only a counted, b gets 0
    h.call(hits_each(ab, &[1, 1])).await.unwrap();
    h.call(hits_each(&[&[("b", "y")], &[("none", "z")]], &[3, 1]))
        .await
        .unwrap();
    h.call(refund(request("d", &[&[("a", "x")]]), 3))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn local_cache_skips_redis_for_keys_over_the_limit_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = CacheSettings {
        local_cache_size_in_bytes: 1024 * 1024,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;
    let ab: &[&[(&str, &str)]] = &[&[("a", "x")], &[("b", "y")]];

    h.call(hits_each(ab, &[6, 1])).await.unwrap(); // a over: cached
    h.call(hits_each(ab, &[1, 1])).await.unwrap(); // a answered from the cache
    h.call(hits_each(&[&[("shadow", "s")]], &[2]))
        .await
        .unwrap(); // shadow rules are cached too
    h.call(hits_each(&[&[("shadow", "s")]], &[1]))
        .await
        .unwrap();
    h.call(refund(request("d", &[&[("a", "x")]]), 6))
        .await
        .unwrap(); // refunds ignore the cache
    h.call(hits_each(ab, &[1, 1])).await.unwrap(); // still cached
    h.at(1_700_000_060).await; // next window: a new key
    h.call(hits_each(ab, &[1, 1])).await.unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn local_cache_and_stop_increment_together_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = CacheSettings {
        local_cache_size_in_bytes: 1024 * 1024,
        stop_cache_key_increment_when_overlimit: true,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;
    let ab: &[&[(&str, &str)]] = &[&[("a", "x")], &[("b", "y")]];

    h.call(hits_each(ab, &[6, 1])).await.unwrap();
    // a is over in the local cache: nothing is incremented, b gets INCRBY 0.
    h.call(hits_each(ab, &[1, 2])).await.unwrap();
    h.call(hits_each(&[&[("b", "y")]], &[1])).await.unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn keys_too_long_for_freecache_are_never_cached_like_upstream() {
    if !tools_available() {
        return;
    }
    // At freecache's 512 KiB minimum, entries over 2048 / 4 - 24 = 488 bytes are refused.
    let settings = CacheSettings {
        local_cache_size_in_bytes: 1,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;
    let long = "v".repeat(600);
    let short = "v".repeat(470);
    for value in [&long, &short] {
        h.call(hits_each(&[&[("a", value)]], &[6])).await.unwrap();
        h.call(hits_each(&[&[("a", value)]], &[1])).await.unwrap();
    }
    h.assert_same_redis().await;
}

#[tokio::test]
async fn unreadable_counters_fail_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = CacheSettings {
        stop_cache_key_increment_when_overlimit: true,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;
    let window = 1_700_000_000 / 60 * 60;
    h.set_raw(&format!("d_a_x_{window}"), "abc").await;
    let error = h.call(hits_each(&[&[("a", "x")]], &[1])).await.unwrap_err();
    assert!(error.contains("in parseUint"), "{error}");

    // A counter driven below zero cannot be read as an unsigned number either.
    let mut h = Harness::start(OVERLIMIT).await;
    h.at(1_700_000_000).await;
    h.call(hits_each(&[&[("b", "y")]], &[u64::MAX]))
        .await
        .unwrap_err();
}

#[tokio::test]
async fn stop_increment_branches_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = CacheSettings {
        local_cache_size_in_bytes: 1024 * 1024,
        stop_cache_key_increment_when_overlimit: true,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;

    // A brand-new key next to one going over gets INCRBY 0: a ghost key at 0.
    h.call(hits_each(&[&[("a", "x")], &[("b", "new")]], &[6, 1]))
        .await
        .unwrap();
    // A shadow rule going over is counted and cached, but answers OK.
    h.call(hits_each(&[&[("shadow", "s")], &[("b", "y")]], &[2, 1]))
        .await
        .unwrap();
    // A refund next to a key answered by the local cache.
    let mut mixed = hits_each(&[&[("a", "x")], &[("b", "y")]], &[1, 3]);
    mixed.descriptors[1].is_negative_hits = true;
    h.call(mixed).await.unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn a_full_local_cache_evicts_like_freecache() {
    if !tools_available() {
        return;
    }
    // freecache's minimum: 256 segments of 2 KiB, about 45 of these keys each.
    let settings = CacheSettings {
        local_cache_size_in_bytes: 1,
        ..Default::default()
    };
    let mut h = Harness::with_settings(
        "
domain: d
descriptors:
  - key: k
    rate_limit: {unit: hour, requests_per_unit: 0}
",
        settings,
    )
    .await;
    const KEYS: usize = 30_000;
    const BATCH: usize = 100;
    let values: Vec<String> = (0..KEYS).map(|i| format!("v{i}")).collect();
    let batch = |chunk: &[String]| {
        let descriptors: Vec<Vec<(&str, &str)>> =
            chunk.iter().map(|v| vec![("k", v.as_str())]).collect();
        let refs: Vec<&[(&str, &str)]> = descriptors.iter().map(Vec::as_slice).collect();
        request("d", &refs)
    };

    let start = 1_700_000_000 / 3600 * 3600;
    for (n, chunk) in values.chunks(BATCH).enumerate() {
        // Time moves on, so access times differ and freecache's second chance comes into play.
        h.at(start + (n / 20) as i64).await;
        h.call(batch(chunk)).await.unwrap();
        // Re-reading early keys refreshes their access time.
        if n % 10 == 9 {
            h.call(batch(&values[..BATCH])).await.unwrap();
        }
    }
    // Probe every key: those still cached send nothing to Redis.
    for chunk in values.chunks(BATCH) {
        h.call(batch(chunk)).await.unwrap();
    }
    h.assert_same_redis().await;
}

/// Response headers name the descriptor closest to its limit: fewest remaining, the first on a
/// tie, unless a regular descriptor is over its limit; the reset re-reads the clock.
#[tokio::test]
async fn response_headers_describe_the_closest_descriptor_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        response_headers: Some(LimitHeaders {
            limit: "X-Limit".into(),
            remaining: "X-Remaining".into(),
            reset: "X-Reset".into(),
        }),
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(
        "
domain: d
descriptors:
  - key: second
    rate_limit: {unit: second, requests_per_unit: 3}
  - key: minute
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: hour
    rate_limit: {unit: hour, requests_per_unit: 2}
  - key: minute3
    rate_limit: {unit: minute, requests_per_unit: 3}
  - key: free
    rate_limit: {unlimited: true}
",
        settings,
    )
    .await;
    h.at(1_700_000_017).await;
    let all = [
        &[("free", "x")][..],
        &[("none", "x")],
        &[("minute", "x")],
        &[("second", "x")],
        &[("hour", "x")],
    ];
    for _ in 0..4 {
        let response = h.call(request("d", &all)).await.unwrap();
        assert_eq!(response.response_headers_to_add.len(), 3);
    }
    h.at(1_700_000_018).await;
    // A tie: same limit and remaining hits, different resets.
    h.call(request("d", &[&[("minute3", "x")], &[("second", "x")]]))
        .await
        .unwrap();
    h.call(request("d", &[&[("free", "x")], &[("none", "x")]]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

/// Request headers describe the same descriptor as the response headers, under their own
/// names, and are independent of them.
#[tokio::test]
async fn request_headers_describe_the_closest_descriptor_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        request_headers: Some(LimitHeaders {
            limit: "X-Request-Limit".into(),
            remaining: "X-Request-Remaining".into(),
            reset: "X-Request-Reset".into(),
        }),
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(
        "
domain: d
descriptors:
  - key: second
    rate_limit: {unit: second, requests_per_unit: 1}
  - key: minute
    rate_limit: {unit: minute, requests_per_unit: 3}
",
        settings,
    )
    .await;
    h.at(1_700_000_017).await;
    for _ in 0..3 {
        let response = h
            .call(request("d", &[&[("minute", "x")], &[("second", "x")]]))
            .await
            .unwrap();
        assert_eq!(response.request_headers_to_add.len(), 3);
    }
    h.call(request("d", &[&[("none", "x")]])).await.unwrap();
    h.assert_same_redis().await;
}

/// Per-unit headers: one pair per unit, in the order units first appear, for the descriptor
/// of that unit with the fewest remaining hits (the first on a tie), before the response
/// headers.
#[tokio::test]
async fn per_unit_headers_describe_each_unit_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        per_unit_headers: true,
        response_headers: Some(LimitHeaders::default()),
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(
        "
domain: d
descriptors:
  - key: minute5
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: second
    rate_limit: {unit: second, requests_per_unit: 1}
  - key: minute4
    rate_limit: {unit: minute, requests_per_unit: 4}
  - key: day
    rate_limit: {unit: day, requests_per_unit: 9}
  - key: free
    rate_limit: {unlimited: true}
",
        settings,
    )
    .await;
    h.at(1_700_000_017).await;
    h.call(request("d", &[&[("minute5", "x")]])).await.unwrap();
    // minute5 and minute4 tie at 3 remaining.
    let all = [
        &[("free", "x")][..],
        &[("minute5", "x")],
        &[("second", "x")],
        &[("none", "x")],
        &[("minute4", "x")],
        &[("day", "x")],
    ];
    for _ in 0..3 {
        let response = h.call(request("d", &all)).await.unwrap();
        assert_eq!(response.response_headers_to_add.len(), 3 * 2 + 3);
    }
    h.assert_same_redis().await;
}

/// Dynamic metadata describes the request: its domain, each descriptor's entries and hits,
/// and the request's hits when not 0; set under global shadow mode too.
#[tokio::test]
async fn dynamic_metadata_describes_the_request_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        response_dynamic_metadata: true,
        global_shadow_mode: true,
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    let both = || {
        request(
            "llm",
            &[
                &[("user", "alice"), ("model", "m=1")],
                &[("shadow", "x")],
                &[],
            ],
        )
    };
    for hits in [
        with_hits(both(), 0, &[None, None, None]),
        with_hits(both(), 7, &[Some(0), None, Some(3)]),
        with_hits(both(), 0, &[Some(1 << 60), Some(2), None]),
    ] {
        let response = h.call(hits).await.unwrap();
        assert!(response.dynamic_metadata.is_some());
    }
    h.assert_same_redis().await;
}

/// A client-sent limit appears in the metadata in protobuf's text format.
#[tokio::test]
async fn dynamic_metadata_shows_client_limits_like_upstream() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride;
    let settings = ServiceSettings {
        response_dynamic_metadata: true,
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    let mut overridden = request("llm", &[&[("user", "alice")], &[("free", "x")]]);
    overridden.descriptors[0].limit = Some(RateLimitOverride {
        requests_per_unit: 10,
        unit: 2, // MINUTE
        unit_multiplier: None,
    });
    overridden.descriptors[1].limit = Some(RateLimitOverride {
        requests_per_unit: 0,
        unit: 4, // DAY
        unit_multiplier: None,
    });
    let response = h.call(overridden).await.unwrap();
    assert!(response.dynamic_metadata.is_some());
    h.assert_same_redis().await;
}

const WITH_METADATA: &str = "
domain: d
descriptors:
  - key: a
    rate_limit: {unit: minute, requests_per_unit: 10}
    metadata: {route: a, shared: {x: 1, list: [1, two]}, weight: 1.5}
  - key: b
    rate_limit: {unit: minute, requests_per_unit: 10}
    metadata: {route: b, shared: {y: two, x: 9}, b: {nested: {deep: 1}}}
  - key: over
    rate_limit: {unit: minute, requests_per_unit: 0}
    metadata: {over: o}
  - key: free
    rate_limit: {unlimited: true}
    metadata: {free: f}
  - key: replaced
    rate_limit: {unit: minute, requests_per_unit: 10, name: old}
    metadata: {replaced: r}
  - key: replacer
    rate_limit: {unit: minute, requests_per_unit: 10, replaces: [{name: old}]}
    metadata: {replacer: r}
";

/// The metadata of the limits that passed is merged into the response's: the first value wins,
/// structs merge recursively. Over-limit, unlimited and replaced limits add nothing, nor does a
/// limit replaced by a client-sent one.
#[tokio::test]
async fn dynamic_metadata_merges_the_metadata_of_passed_limits_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        response_dynamic_metadata: true,
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(WITH_METADATA, settings).await;
    h.at(1_700_000_000).await;
    let all = [
        &[("over", "x")][..],
        &[("a", "x")],
        &[("free", "x")],
        &[("b", "x")],
        &[("replaced", "x")],
        &[("replacer", "x")],
        &[("none", "x")],
    ];
    for _ in 0..2 {
        let response = h.call(request("d", &all)).await.unwrap();
        assert!(
            response
                .dynamic_metadata
                .unwrap()
                .fields
                .contains_key("metadata")
        );
    }
    h.call(request("d", &[&[("b", "x")], &[("a", "x")]]))
        .await
        .unwrap();
    let mut overridden = request("d", &[&[("replacer", "x")]]);
    overridden.descriptors[0].limit = Some(
        llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride {
            requests_per_unit: 5,
            unit: 2, // MINUTE
            unit_multiplier: None,
        },
    );
    h.call(overridden).await.unwrap();
    h.assert_same_redis().await;
}

const MODELS: &str = "
domain: ai
descriptors:
  - key: backend_name
    value: gpt
    descriptors:
      - key: model_name_override
        quota_mode: true
        rate_limit: {unit: minute, requests_per_unit: 1}
        metadata: {gpt: g}
  - key: backend_name
    value: claude
    descriptors:
      - key: model_name_override
        quota_mode: true
        rate_limit: {unit: minute, requests_per_unit: 5}
        metadata: {claude: c}
  - key: seat
    rate_limit: {unit: hour, requests_per_unit: 1}
";

/// In quota mode the metadata lists the models that still have quota (`passedBackends`), once
/// each; a model whose group is exhausted is left out, along with its limits' metadata, even
/// for its descriptors that passed. Passed regular descriptors naming a model are listed too.
/// A quota descriptor over its limit does not take over the headers.
#[tokio::test]
async fn dynamic_metadata_lists_the_models_with_quota_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        response_dynamic_metadata: true,
        response_headers: Some(LimitHeaders::default()),
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(MODELS, settings).await;
    h.at(1_700_000_000).await;
    let gpt = &[("backend_name", "gpt"), ("model_name_override", "m1")][..];
    let claude = &[("backend_name", "claude"), ("model_name_override", "m2")][..];
    let regular = &[("model_name_override", "m3")][..];
    let anonymous = &[("tenant", "t")][..];
    h.call(request("ai", &[claude, regular, anonymous]))
        .await
        .unwrap();
    // gpt's first descriptor passes, its second exhausts it.
    h.call(request("ai", &[gpt, claude, claude, gpt, regular]))
        .await
        .unwrap();
    // Every model exhausted: nothing to list.
    h.call(request("ai", &[gpt])).await.unwrap();
    // The headers keep the first descriptor with no hits left, not the exhausted model.
    h.call(request("ai", &[&[("seat", "s")], gpt]))
        .await
        .unwrap();
    // Without a quota descriptor there is no list, even for regular ones naming a model.
    h.call(request("ai", &[regular, &[("backend_name", "other")]]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

/// Every config load is counted; one that fails is counted as an error and the previous config
/// stays in use.
#[tokio::test]
async fn config_reloads_are_counted_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "alice")]]);
    h.call(user()).await.unwrap();
    let error = h
        .load_config("domain: llm\ndescriptors:\n  - key: user\n    rate_limit: {unit: fortnight, requests_per_unit: 1}\n")
        .await
        .unwrap_err();
    assert!(error.contains("fortnight"), "{error}");
    h.call(user()).await.unwrap();
    h.load_config("domain: llm\ndescriptors:\n  - key: user\n    rate_limit: {unit: minute, requests_per_unit: 1}\n")
        .await
        .unwrap();
    h.call(user()).await.unwrap();
    h.call(user()).await.unwrap();
    h.assert_same_redis().await;
}

/// Hits are split between `within_limit`, `near_limit`, `over_limit` and `shadow_mode` like
/// upstream, with the near threshold computed in float32: 0.29 × 100 is 29 there, 28 in
/// float64.
#[tokio::test]
async fn near_and_over_limit_hits_are_split_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: CacheSettings {
            near_limit_ratio: 0.29,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut h = Harness::configured(
        "
domain: d
descriptors:
  - key: k
    rate_limit: {unit: minute, requests_per_unit: 100}
  - key: shadow
    shadow_mode: true
    rate_limit: {unit: minute, requests_per_unit: 100}
",
        settings,
    )
    .await;
    h.at(1_700_000_000).await;
    let both = || request("d", &[&[("k", "x")], &[("shadow", "x")]]);
    // 28 (under near), 29, 30 (near), 95 (straddles near), 105 (straddles over), 110 (over).
    for hits in [28, 1, 1, 65, 10, 5] {
        h.call(with_hits(both(), hits, &[None, None]))
            .await
            .unwrap();
    }
    // A new window: from 0 straight past the near threshold, then past both.
    h.at(1_700_000_060).await;
    h.call(with_hits(both(), 40, &[None, None])).await.unwrap();
    h.at(1_700_000_120).await;
    h.call(with_hits(both(), 150, &[None, None])).await.unwrap();
    h.assert_same_redis().await;
}

/// With `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`, a descriptor left uncounted because another
/// is going over still has its hits taken from its counter for the stats, wrapping below 0 as
/// in upstream's unsigned arithmetic.
#[tokio::test]
async fn uncounted_hits_wrap_in_the_stats_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: CacheSettings {
            near_limit_ratio: 0.29,
            stop_cache_key_increment_when_overlimit: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut h = Harness::configured(
        "
domain: d
descriptors:
  - key: small
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: big
    rate_limit: {unit: minute, requests_per_unit: 100}
",
        settings,
    )
    .await;
    h.at(1_700_000_000).await;
    h.call(with_hits(request("d", &[&[("big", "x")]]), 30, &[None]))
        .await
        .unwrap();
    // `small` goes over, so `big` (30 + 40 = 70, within its limit) is not counted.
    h.call(with_hits(
        request("d", &[&[("small", "x")], &[("big", "x")]]),
        40,
        &[None, None],
    ))
    .await
    .unwrap();
    h.assert_same_redis().await;
}

const NAMED: &str = "
domain: d
descriptors:
  - key: detailed
    detailed_metric: true
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: metric
    value_to_metric: true
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: plain
    rate_limit: {unit: minute, requests_per_unit: 10}
";

/// Counters are named after the limit's stats key, sanitized: `:` and `|` and the dots of IPv4
/// addresses become `_`; an unknown domain is counted per descriptor.
#[tokio::test]
async fn counters_are_named_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(NAMED).await;
    h.at(1_700_000_000).await;
    for value in ["a:b|c", "10.0.0.1", "v1.2.3.4.5", "1234.5.6.7.8"] {
        h.call(request(
            "d",
            &[
                &[("detailed", value)],
                &[("metric", value)],
                &[("plain", value)],
            ],
        ))
        .await
        .unwrap();
    }
    let mut overridden = request("d", &[&[("client", "192.168.1.1:80")]]);
    overridden.descriptors[0].limit = Some(
        llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride {
            requests_per_unit: 5,
            unit: 2, // MINUTE
            unit_multiplier: None,
        },
    );
    h.call(overridden).await.unwrap();
    h.call(request("unknown:1.2.3.4", &[&[("a", "b")], &[("c", "d")]]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

/// `SANITIZE_DESCRIPTOR_METRIC_DOTS` turns dots into `_` in the keys and values of stats keys
/// built per request.
#[tokio::test]
async fn descriptor_dots_are_sanitized_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        load: LoadOptions {
            sanitize_descriptor_metric_dots: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut h = Harness::configured(NAMED, settings).await;
    h.at(1_700_000_000).await;
    for value in ["a.b", "10.0.0.1"] {
        h.call(request(
            "d",
            &[
                &[("detailed", value)],
                &[("metric", value)],
                &[("plain", value)],
            ],
        ))
        .await
        .unwrap();
    }
    h.call(request("unknown.domain", &[&[("a.b", "c.d")]]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

/// Global shadow mode counts each request it lets through.
#[tokio::test]
async fn global_shadow_mode_is_counted_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        global_shadow_mode: true,
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    for hits in [600, 600, 1] {
        let response = h
            .call(with_hits(
                request("llm", &[&[("user", "alice")]]),
                hits,
                &[None],
            ))
            .await
            .unwrap();
        assert_eq!(response.overall_code, 1); // OK
    }
    h.assert_same_redis().await;
}

/// With `NEAR_LIMIT_RATIO` above 1 the near threshold is past the limit: hits going over then
/// add the limit minus that threshold to `near_limit`, wrapping below 0 like upstream.
#[tokio::test]
async fn a_near_threshold_past_the_limit_wraps_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: CacheSettings {
            near_limit_ratio: 1.5,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    for hits in [900, 200, 50] {
        h.call(with_hits(
            request("llm", &[&[("user", "alice")]]),
            hits,
            &[None],
        ))
        .await
        .unwrap();
    }
    h.assert_same_redis().await;
}

/// The counters reach statsd over TCP as upstream's sink sends them: each flush, every counter's
/// increase since the previous one, when not 0.
#[tokio::test]
async fn counters_are_flushed_to_statsd_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        statsd: true,
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "alice")]]);
    h.call(with_hits(user(), 600, &[None])).await.unwrap();
    h.call(user()).await.unwrap();
    h.call(with_hits(user(), 600, &[None])).await.unwrap();
    h.call(request("unknown", &[&[("a", "b")]])).await.unwrap();
    h.assert_same_redis().await;
}

/// `EXTRA_TAGS` are appended to every name, sorted by key, values sanitized, empty values left
/// out; each reserved key counts in `reserved_tag`.
#[tokio::test]
async fn extra_tags_are_appended_like_upstream() {
    if !tools_available() {
        return;
    }
    let tags = [
        ("zone", "eu.west|1"),
        ("app", "rl|x"),
        ("host", "h1"),
        ("region", ""),
    ];
    let settings = Settings {
        statsd: true,
        extra_tags: tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    h.call(request("llm", &[&[("user", "alice")], &[("x", "y")]]))
        .await
        .unwrap();
    h.call(request("unknown", &[&[("a", "b")]])).await.unwrap();
    h.assert_same_redis().await;
}

/// The counters reach DogStatsD as upstream's sink sends them: `name:value|c` lines with the
/// global tags, then `EXTRA_TAGS` pulled out of the name, then the first matching mogrifier's.
#[tokio::test]
async fn counters_are_sent_to_dogstatsd_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        dogstatsd: Some(DogStatsd {
            entity_id: "entity".into(),
            env: "prod".into(),
            service: "rl".into(),
            version: "1.2".into(),
            mogrifiers: vec![
                MogrifierSpec {
                    key: "UNKNOWN",
                    pattern: r"^ratelimit\.service\.rate_limit\.(\w+)\.domain_not_found$",
                    name: "ratelimit.unknown",
                    tags: vec![("domain", "$1")],
                },
                MogrifierSpec {
                    key: "HITS",
                    pattern: r"^ratelimit\.service\.rate_limit\.(\w+)\.(\w+)\.total_hits$",
                    name: "ratelimit.hits",
                    tags: vec![("domain", "$1")],
                },
                MogrifierSpec {
                    key: "ANY",
                    pattern: r"^ratelimit\.service\.rate_limit\.([^.]+)\.(.+)$",
                    name: "ratelimit.$2.$9.$1a.$01.$",
                    tags: vec![("domain", "$1")],
                },
            ],
        }),
        extra_tags: [("cluster", "c1")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "alice")]]);
    h.call(with_hits(user(), 600, &[None])).await.unwrap();
    h.call(request("unknown", &[&[("a", "b")]])).await.unwrap();
    // Not a word in Go's ASCII `\w`: UNKNOWN does not match, ANY does.
    h.call(request("é٣", &[&[("a", "b")]])).await.unwrap();
    h.assert_same_redis().await;
}

/// Without `DD_ENTITY_ID`, origin detection is on: both sides find the same container ID, if
/// any; the other global tags still come first.
#[tokio::test]
async fn dogstatsd_without_an_entity_id_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        dogstatsd: Some(DogStatsd {
            env: "prod".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    h.call(request("llm", &[&[("user", "alice")]]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

const NESTED: &str = "
domain: llm
descriptors:
  - key: user
    rate_limit: {unit: minute, requests_per_unit: 1000}
  - key: org
    descriptors:
      - key: model
        rate_limit: {unit: minute, requests_per_unit: 2}
  - key: shadow
    shadow_mode: true
    rate_limit: {unit: hour, requests_per_unit: 1}
";

/// The counters reach Prometheus through upstream's default mapper: one-key descriptors by its
/// globs, deeper ones by its unanchored regexes, the rest escaped; values in Go's format.
#[tokio::test]
async fn counters_are_exposed_to_prometheus_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: CacheSettings {
            local_cache_size_in_bytes: 1024 * 1024,
            ..Default::default()
        },
        prometheus: Some(Prometheus::new(None).await),
        ..Default::default()
    };
    let mut h = Harness::configured(NESTED, settings).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "alice")]]);
    h.call(with_hits(user(), 1_000_000, &[None])).await.unwrap();
    h.call(user()).await.unwrap();
    let model = || request("llm", &[&[("org", "acme"), ("model", "gpt")]]);
    for _ in 0..4 {
        h.call(model()).await.unwrap();
    }
    h.call(request("llm", &[&[("shadow", "s")]])).await.unwrap();
    h.call(request("é-x", &[&[("a", "b")]])).await.unwrap();
    h.call(request("9th", &[&[("a", "b")]])).await.unwrap();
}

/// promhttp's answer: gzip when the first Accept-Encoding lists it, the `escaping` parameter
/// echoed, any method; Go's mux around the path.
#[tokio::test]
async fn prometheus_scrapes_are_answered_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        prometheus: Some(Prometheus::new(None).await),
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    h.call(request("llm", &[&[("user", "alice")]]))
        .await
        .unwrap();
    for headers in [
        &[("Accept-Encoding", "gzip")][..],
        &[("Accept-Encoding", "br, gzip;q=0")],
        &[("Accept-Encoding", "GZIP")],
        &[("Accept", "text/plain;version=0.0.4;escaping=underscores")],
        &[("Accept", "application/json;escaping=dots, text/plain;q=0.5")],
        &[("Accept", "text/plain;escaping=bogus")],
        &[(
            "Accept",
            "*/*;escaping=allow-utf-8,text/plain;version=1.0.0",
        )],
    ] {
        h.scrape("GET", "/metrics", headers).await;
    }
    h.scrape("POST", "/metrics", &[]).await;
    h.scrape("HEAD", "/metrics", &[]).await;
    for target in [
        "/metrics?x=1",
        "/metrics/",
        "/",
        "/other",
        "/a/../metrics",
        "//metrics?q",
        "/%6Detrics",
        "/metric%73?a",
        "/%2Fmetrics",
        "/metrics%2F",
    ] {
        h.scrape("GET", target, &[]).await;
    }
}

/// What the mapper decided for one name, shaped like the oracle's JSON.
fn mapped_json(mapped: Option<prometheus::Mapped>) -> serde_json::Value {
    use prometheus::{Action, MatchType, ObserverType};
    use serde_json::json;
    let Some(m) = mapped else {
        return json!({"matched": false, "name": "", "labels": null, "action": "", "help": "",
            "honor_labels": false, "observer_type": "", "match_type": "", "match_metric_type": "",
            "ttl": 0, "scale": null, "histogram_options": null, "summary_options": null});
    };
    let objectives = |o: &Option<Vec<prometheus::Objective>>| {
        o.as_ref().map(|o| {
            o.iter()
                .map(|q| json!({"Quantile": q.quantile, "Error": q.error}))
                .collect::<Vec<_>>()
        })
    };
    json!({
        "matched": true,
        "name": m.name,
        "labels": m.labels,
        "action": match m.action { Action::Map => "map", Action::Drop => "drop" },
        "help": m.help,
        "honor_labels": m.honor_labels,
        "observer_type": match m.observer_type {
            None => "",
            Some(ObserverType::Histogram) => "histogram",
            Some(ObserverType::Summary) => "summary",
        },
        "match_type": match m.match_type { MatchType::Glob => "glob", MatchType::Regex => "regex" },
        "match_metric_type": m.match_metric_type.map_or("", |t| t.as_str()),
        "ttl": m.ttl,
        "scale": m.scale,
        "histogram_options": m.histogram_options.map(|h| json!({
            "Buckets": h.buckets,
            "NativeHistogramBucketFactor": h.native_histogram_bucket_factor,
            "NativeHistogramMaxBuckets": h.native_histogram_max_buckets,
        })),
        "summary_options": m.summary_options.map(|s| json!({
            "Quantiles": objectives(&s.quantiles),
            "MaxAge": s.max_age,
            "AgeBuckets": s.age_buckets,
            "BufCap": s.buf_cap,
        })),
    })
}

/// JSON numbers as floats, so that Go's `1` and Rust's `1.0` compare equal.
fn floats(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Number(n) => serde_json::json!(n.as_f64().unwrap()),
        Value::Array(items) => Value::Array(items.into_iter().map(floats).collect()),
        Value::Object(map) => Value::Object(map.into_iter().map(|(k, v)| (k, floats(v))).collect()),
        other => other,
    }
}

/// Mapper files, valid or not, that exercise statsd_exporter's decoding, validation, glob
/// FSM (ordering, prefixes, duplicates, backtracking), regexes and templates.
const MAPPERS: &[&str] = &[
    include_str!("../src/stats/prometheus/default_mapper.yaml"),
    "",
    "mappings:",
    "defaults: {observer_type: histogram, ttl: 1m, buckets: [1, 2], quantiles: [{quantile: 0.4, error: 0.1}]}
mappings:
- match: a.*.c
  name: abc_$1
  labels: {first: $1, both: \"$1-$1\", none: $2, braced: \"${1}x\", zero: $0, pct: \"%d$1%\", big: $10, mix: \"$1 $10\"}
- match: a.b.*
  name: ab
  match_metric_type: counter
  help: The help.
  ttl: 10
- match: a.b
  name: shorter_prefix
- match: a.*.c
  name: duplicate
- match: '*.*.*'
  name: any_three
  action: drop
  match_metric_type: gauge
- match: x.*
  name: x_$1
  timer_type: summary
  scale: 0.5
  summary_options: {max_age: 1h30m, age_buckets: 3, buf_cap: 7}
",
    "defaults: {glob_disable_ordering: true, match_type: regex, observer_type: \"\"}
mappings:
- match: '^ratelimit\\.(\\w+)\\.(?P<rest>.+)$'
  name: rl_${1}
  labels: {lit: \"$$1\", rest: $rest, named: \"${rest}\", missing: $9, tail: \"$1x$\", word: $rest_}
- match: 'b(.)'
  name: unanchored
  match_metric_type: timer
  histogram_options: {buckets: [0.5, 5], native_histogram_bucket_factor: 2}
  observer_type: histogram
- match: a.*
  match_type: glob
  name: glob_$1
- match: a.b
  match_type: glob
  name: glob_exact
",
    "mappings:
- match: a.*
  name: one
- match: '*.b'
  name: two
- match: '*.*.c'
  name: three
- match: a.b.*
  name: four
- match: test.*.*.*
  name: deep_$3_$1
  labels: {kk: v, xx: '$2'}
",
    "mappings:
- {match: a.b, name: \"$1\"}
- {match: a.c, name: ok}",
    "mappings:\n- {match: a, name: n, labels: {a: b}}",
    "mappings:\n- {match: a, name: 1n}",
    "mappings:\n- {match: 'a*', name: n}",
    "mappings:\n- {match: '(', name: n, match_type: regex}",
    "mappings:\n- {match: a, name: n, match_type: other}",
    "mappings:\n- {match: a, name: n, buckets: [1], histogram_options: {buckets: [2]}}",
    "mappings:\n- {match: a, name: n, buckets: [], histogram_options: {buckets: []}}",
    "mappings:\n- {match: a, name: n, observer_type: histogram, summary_options: {}}",
    "mappings:\n- {match: a, name: n, observer_type: summary, histogram_options: ~}",
    "mappings:\n- {match: a, name: n, scale: '0.5'}",
    "mappings:\n- {match: a, name: n, ttl: 1.5x}",
    "mappings:\n- {match: a, name: n, ttl: 2.5}\n- {match: b, name: m, ttl: '-1h2m3.5s'}",
    "mappings:\n- {match: a, name: n, unknown_key: 1, action: ''}\nother: true",
    "mappings:\n- ~\n",
    "mappings: 3",
    "defaults: {ttl: 5s, match_type: glob}\nmappings:\n- {match: a, name: n, ttl: 0}\n- {match: b, name: m, match_metric_type: observer, quantiles: [{quantile: 0.1}]}",
    // Repeated keys and merges: scalars replaced, maps and structs added to.
    "base: &base {name: base_$1, labels: {kind: a, id: x}, histogram_options: {buckets: [1]}}
mappings:
- <<: *base
  match: m.*
  labels: {id: $1}
  histogram_options: {native_histogram_max_buckets: 3}
  observer_type: histogram
  name: merged_$1
- match: n.*
  name: first
  name: second
  labels: {aa: x}
  labels: ~
  labels: {bb: \"%x-%X-%q-%T-%d-$1\"}
defaults: {ttl: 1s}
defaults: {match_type: glob}",
    "mappings:\n- {match: a, name: n, help: x, help: [1]}",
    "registerer: x",
    "registerer: ~\nmappingscount: ~",
    "mappingscount: 1",
    "logger: {a: b}",
    "fsm: {backtrackingneeded: yes, orderingdisabled: 2}",
    "fsm: {backtrackingneeded: yes}\nmappings:\n- {match: a, name: n}",
    "fsm: 3",
    "mappings:\n- {match: a.*.*, name: \"n_$2\", labels: {ll: \"${2}${1}\"}}\n- {match: a.*.*, name: second}\n- {match: a.x.*, name: specific}",
];

/// Names to map: the stats the service emits, and every short path over a few segments that
/// the mapper files above care about.
fn mapper_names() -> Vec<String> {
    let mut names: Vec<String> = [
        "ratelimit.service.rate_limit.llm.user_alice.total_hits",
        "ratelimit.service.rate_limit.llm.org_acme.model_gpt.near_limit",
        "ratelimit.service.rate_limit.llm.org_acme.model_gpt.over_limit_with_local_cache",
        "ratelimit.service.rate_limit.a.b.c.d.within_limit",
        "ratelimit.service.rate_limit.x.shadow_mode",
        "ratelimit.service.rate_limit.llm.user~five_hour.opened_without_hits",
        "ratelimit.service.rate_limit.llm.user_a~b~week-1.total_hits",
        "ratelimit.service.rate_limit.llm.org_acme.model~weekly.near_limit",
        "ratelimit.service.rate_limit.llm.a.b.c~_x.within_limit",
        "ratelimit.service.rate_limit.llm.a.b~c.over_limit_with_local_cache",
        "ratelimit.service.rate_limit.llm.a.b.c~d.over_limit_with_local_cache",
        "ratelimit.service.call.should_rate_limit.redis_error",
        "ratelimit_server.ShouldRateLimit.response_time",
        "ratelimit_server.ShouldRateLimit.total_requests",
        "ratelimit.service.config_load_success",
        "ratelimit.service.config_load_error",
        "test.dispatcher.FooProcessor.send.success",
        "x.y",
        "",
        "a..c",
        "a.*.c",
        "é.b.c",
        "m.x",
        "n.x",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let segments = ["a", "b", "c", "x", "*", "test"];
    let mut paths = vec![String::new()];
    for _ in 0..4 {
        let mut longer = Vec::new();
        for path in &paths {
            for segment in segments {
                let name = match path.is_empty() {
                    true => segment.to_string(),
                    false => format!("{path}.{segment}"),
                };
                longer.push(name);
            }
        }
        names.extend(longer.iter().cloned());
        paths = longer;
    }
    names
}

/// statsd_exporter's mapper, ported: the same files load or fail, and every name maps to the
/// same name, labels and options.
#[tokio::test]
async fn the_prometheus_mapper_maps_like_statsd_exporter() {
    if !tools_available() {
        return;
    }
    let h = Harness::start(TOKENS).await;
    let names = mapper_names();
    let types = [
        prometheus::MetricType::Counter,
        prometheus::MetricType::Gauge,
        prometheus::MetricType::Observer,
    ];
    for yaml in MAPPERS {
        let (status, error) = h.oracle.send("PUT", "/mapper", yaml).await;
        let mapper = prometheus::Mapper::from_yaml(yaml);
        let outcome = match &mapper {
            Ok(_) => 200,
            Err(prometheus::MapperError::Invalid(_)) => 422,
            Err(prometheus::MapperError::Panic(_)) => 500,
        };
        assert_eq!(
            outcome, status,
            "loading {yaml}: upstream {error}, ours {mapper:?}"
        );
        let mapper = mapper.unwrap_or_default();
        let mut body = String::new();
        let mut expected_order = Vec::new();
        for name in &names {
            for t in types {
                body += &format!("{} {name}\n", t.as_str());
                expected_order.push((t, name));
            }
        }
        let (status, response) = h.oracle.send("POST", "/map", &body).await;
        assert_eq!(status, 200, "{response}");
        assert_eq!(response.lines().count(), expected_order.len(), "{response}");
        for ((t, name), line) in expected_order.into_iter().zip(response.lines()) {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            let actual = mapped_json(mapper.get_mapping(name, t));
            assert_eq!(
                floats(actual),
                floats(expected),
                "{} {name} with {yaml}",
                t.as_str()
            );
        }
    }
}

/// The shipped mapper rewrites upstream's globs as regexes, after its list rules (D29): every
/// stat a list limit does not emit maps as upstream's own default mapper maps it.
#[tokio::test]
async fn the_shipped_mapper_maps_upstream_stats_like_upstream() {
    if !tools_available() {
        return;
    }
    let h = Harness::start(TOKENS).await;
    let output = std::process::Command::new("go")
        .args([
            "list",
            "-m",
            "-f",
            "{{.Dir}}",
            "github.com/envoyproxy/ratelimit",
        ])
        .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/oracle"))
        .output()
        .expect("run go list");
    assert!(output.status.success(), "go list failed");
    let module = PathBuf::from(String::from_utf8(output.stdout).unwrap().trim());
    let upstream =
        std::fs::read_to_string(module.join("src/stats/prom/default_mapper.yaml")).unwrap();
    let (status, error) = h.oracle.send("PUT", "/mapper", &upstream).await;
    assert_eq!(status, 200, "{error}");
    let shipped = prometheus::load_mapper(&Default::default()).unwrap();

    let mut names = mapper_names();
    let stats = [
        "near_limit",
        "over_limit",
        "total_hits",
        "within_limit",
        "shadow_mode",
        "over_limit_with_local_cache",
        "other",
    ];
    let segments = ["d", "k_v", "", "k_v_x-y"];
    let mut paths = vec![String::new()];
    for _ in 0..4 {
        paths = paths
            .iter()
            .flat_map(|path| segments.iter().map(move |s| format!("{path}.{s}")))
            .collect();
        for path in &paths {
            for stat in stats {
                names.push(format!("ratelimit.service.rate_limit{path}.{stat}"));
            }
        }
    }
    // Upstream's glob FSM takes a field that is `*` for its wildcard without capturing it
    // (`d.*` gives `key1=""`), which a regex does not do: a divergence (`docs/parity.md` §7).
    names.retain(|name| {
        let rate_limit = name.starts_with("ratelimit.service.rate_limit.");
        !name.contains('~') && !(rate_limit && name.split('.').any(|field| field == "*"))
    });
    let types = [
        prometheus::MetricType::Counter,
        prometheus::MetricType::Gauge,
        prometheus::MetricType::Observer,
    ];
    let mut body = String::new();
    let mut expected_order = Vec::new();
    for name in &names {
        for t in types {
            body += &format!("{} {name}\n", t.as_str());
            expected_order.push((t, name));
        }
    }
    let (status, response) = h.oracle.send("POST", "/map", &body).await;
    assert_eq!(status, 200, "{response}");
    assert_eq!(response.lines().count(), expected_order.len(), "{response}");
    // Only how the rule was written differs.
    let without_match_type = |mut value: serde_json::Value| {
        value.as_object_mut().unwrap().remove("match_type");
        floats(value)
    };
    for ((t, name), line) in expected_order.into_iter().zip(response.lines()) {
        let expected: serde_json::Value = serde_json::from_str(line).unwrap();
        let actual = mapped_json(shipped.get_mapping(name, t));
        assert_eq!(
            without_match_type(actual),
            without_match_type(expected),
            "{} {name}",
            t.as_str()
        );
    }
}

/// Gauges and timers too: response times in a histogram scaled to seconds, other observers
/// by the defaults; a name taken by another type is a conflict, a negative scaled counter an
/// error; histogram and counter names that collide make the scrape fail.
#[tokio::test]
async fn gauges_and_timers_reach_prometheus_like_upstream() {
    if !tools_available() {
        return;
    }
    let mapper = "
defaults: {observer_type: histogram, buckets: [1, 10, .inf]}
mappings:
- match: ratelimit_server.*.response_time
  name: response_time_seconds
  timer_type: histogram
  scale: 0.001
  labels: {grpc_method: $1}
- match: neg.*
  name: negative
  scale: -1
- match: empty.*
  name: $2
- match: drop.*
  name: dropped
  action: drop
";
    let settings = Settings {
        prometheus: Some(Prometheus::new(Some(mapper)).await),
        ..Default::default()
    };
    let h = Harness::configured(TOKENS, settings).await;
    h.flush_to_prometheus(&[
        (
            "timer",
            "ratelimit_server.ShouldRateLimit.response_time",
            3.0,
        ),
        (
            "timer",
            "ratelimit_server.ShouldRateLimit.response_time",
            1500.0,
        ),
        (
            "timer",
            "ratelimit_server.ShouldRateLimit.response_time",
            20_000.0,
        ),
        ("timer", "other.timer", 0.5),
        ("timer", "other.timer", 12.0),
        ("gauge", "cache.size", 10.0),
        ("gauge", "cache.size", 3.0),
        ("gauge", "local.cache", 1_234_567.0),
        ("counter", "neg.x", 2.0),
        ("counter", "empty.x", 1.0),
        ("counter", "drop.x", 1.0),
        ("gauge", "other.timer", 1.0),
        ("counter", "other_timer_bucket", 1.0),
        ("counter", "plain", 1.0),
        ("counter", "dash--es---here-", 1.0),
        ("counter", "late_sum", 1.0),
        ("timer", "late", 1.0),
    ])
    .await;
    // A counter named like a histogram's series: upstream's registry takes it, Gather refuses.
    h.flush_to_prometheus(&[("counter", "other_timer_sum", 1.0)])
        .await;
}

/// A custom mapper file: its help texts and labels; a series not updated within its TTL
/// disappears and restarts from 0.
#[tokio::test]
async fn a_prometheus_mapper_file_is_used_like_upstream() {
    if !tools_available() {
        return;
    }
    let mapper = "
mappings:
- match: ratelimit.service.rate_limit.*.*.*
  name: rl_$3
  help: Rate limit counters.
  ttl: 2s
  labels: {domain: $1, descriptor: $2}
";
    let settings = Settings {
        prometheus: Some(Prometheus::new(Some(mapper)).await),
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    h.exporter_at(1_000_000_000).await;
    h.call(request("llm", &[&[("user", "alice")]]))
        .await
        .unwrap();
    h.exporter_at(2_000_000_000).await;
    h.call(request("llm", &[&[("user", "bob")]])).await.unwrap();
    h.exporter_at(3_000_000_001).await;
    let scraped = h.exporter_at(4_000_000_001).await;
    assert!(!scraped.body.contains("alice"), "{}", scraped.body);
    h.call(request("llm", &[&[("user", "alice")]]))
        .await
        .unwrap();
}

/// Series the registry takes but Gather refuses make every scrape fail with its error: a
/// reserved label name, a series that duplicates one of statsd_exporter's own.
#[tokio::test]
async fn invalid_series_fail_prometheus_scrapes_like_upstream() {
    if !tools_available() {
        return;
    }
    for mapping in [
        "{match: ratelimit.service.config_load_success, name: loads, labels: {__reserved: x}}",
        // The same help, as which family Go collects first, and so which help it expects, is
        // random.
        "{match: ratelimit.service.config_load_success, name: statsd_exporter_events_total, \
         labels: {type: counter}, help: The total number of StatsD events seen.}",
    ] {
        let mapper = format!("mappings:\n- {mapping}\n");
        let settings = Settings {
            prometheus: Some(Prometheus::new(Some(&mapper)).await),
            ..Default::default()
        };
        let h = Harness::configured(TOKENS, settings).await;
        let scraped = h
            .scrape("GET", "/metrics", &[("Accept-Encoding", "gzip")])
            .await;
        assert_eq!(scraped.status, 500, "{}", scraped.body);
    }
}

/// A mapper file that cannot be read, is not UTF-8 (UTF-16 with a byte order mark is), or does
/// not load maps nothing: the default mapper is not used instead.
#[tokio::test]
async fn an_unusable_prometheus_mapper_file_maps_nothing_like_upstream() {
    if !tools_available() {
        return;
    }
    let utf16: Vec<u8> = [0xFF, 0xFE]
        .into_iter()
        .chain(
            "mappings: [{match: ratelimit.service.config_load_success, name: utf16}]"
                .encode_utf16()
                .flat_map(u16::to_le_bytes),
        )
        .collect();
    let files: [Option<Vec<u8>>; 4] = [
        None,
        Some(b"mappings: [{match: a, name: 1}]".to_vec()),
        Some(
            b"# \xff\nmappings: [{match: ratelimit.service.config_load_success, name: x}]".to_vec(),
        ),
        Some(utf16),
    ];
    for file in files {
        let mut prometheus = Prometheus::new(None).await;
        prometheus.mapper = match file {
            None => MapperFile::Missing,
            Some(bytes) => MapperFile::write(&bytes).await,
        };
        let settings = Settings {
            prometheus: Some(prometheus),
            ..Default::default()
        };
        let mut h = Harness::configured(TOKENS, settings).await;
        h.at(1_700_000_000).await;
        h.call(request("llm", &[&[("user", "alice")]]))
            .await
            .unwrap();
        h.flush_to_prometheus(&[("gauge", "ratelimit.localcache.entryCount", 2.0)])
            .await;
    }
}

/// `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS` drops the default mapper's scaling; a subtree
/// `PROMETHEUS_PATH` serves everything under it and redirects to it.
#[tokio::test]
async fn prometheus_options_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut prometheus = Prometheus::new(None).await;
    prometheus.milliseconds = true;
    prometheus.path = "/stats/";
    let settings = Settings {
        prometheus: Some(prometheus),
        ..Default::default()
    };
    let h = Harness::configured(TOKENS, settings).await;
    for target in [
        "/stats/",
        "/stats",
        "/stats/x?y",
        "/metrics",
        "/stats/../stats/",
        "/%73tats",
        "/%73tats/x%20y",
    ] {
        h.scrape("GET", target, &[]).await;
    }
    let time = "ratelimit_server.ShouldRateLimit.response_time";
    let body = h
        .oracle
        .send("POST", "/flush", &format!("timer {time} 7\n"))
        .await;
    assert_eq!(body.0, 200);
    h.subject
        .prometheus
        .as_ref()
        .unwrap()
        .0
        .flush_timer(time, 7.0);
    h.scrape("GET", "/stats/", &[]).await;
}

/// Looks variables up in `environment`, as an environment would.
fn lookup_in<'a>(environment: &'a [(&'a str, String)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |key| {
        environment
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.clone())
    }
}

/// Upstream's `settings.NewSettings()` for `environment`, from the oracle: the settings as JSON,
/// or the panic's text.
async fn upstream_settings(environment: &[(&str, &str)]) -> Result<serde_json::Value, String> {
    let output = Command::new(oracle_binary())
        .arg("-settings")
        .env_clear()
        .envs(environment.iter().copied())
        .output()
        .await
        .expect("run the oracle");
    let printed: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("oracle output: {}", String::from_utf8_lossy(&output.stdout)));
    match printed.get("error") {
        Some(error) => Err(error.as_str().unwrap().to_string()),
        None => Ok(serde_json::json!({"settings": printed["settings"], "sink": printed["sink"]})),
    }
}

/// How selecting our stats sink fails for `settings`, or "" (the Prometheus sink, which would
/// listen, is left out as on the oracle).
fn sink_failure(
    settings: &llm_ratelimit::settings::Settings,
    lookup: llm_ratelimit::settings::Lookup,
) -> String {
    let sinks = match settings.sinks(lookup) {
        Ok(sinks) => sinks,
        Err(e) => return e,
    };
    if !sinks.disable_stats && !sinks.use_dog_statsd && !sinks.use_statsd && sinks.use_prometheus {
        return String::new();
    }
    match llm_ratelimit::stats::select_sink(&sinks) {
        Ok(_) => String::new(),
        Err(e) => e,
    }
}

/// Our settings, shaped like the oracle's JSON.
fn settings_json(settings: &llm_ratelimit::settings::Settings) -> serde_json::Value {
    use llm_ratelimit::settings::Value;
    let values = settings.values().into_iter().map(|(key, value)| {
        let json = match value {
            Value::Str(s) => serde_json::json!(s),
            Value::Int(i) => serde_json::json!(i),
            Value::Bool(b) => serde_json::json!(b),
            Value::Float(f) => serde_json::json!(llm_ratelimit::go::format_float(f)),
            Value::List(l) => serde_json::json!(l),
            Value::Map(m) => serde_json::json!(m),
        };
        (key.to_string(), json)
    });
    serde_json::Value::Object(values.collect())
}

/// Every variable is read as envconfig reads it: same values, or the same first error.
#[tokio::test]
async fn settings_are_read_like_upstream() {
    if !tools_available() {
        return;
    }
    let ints = [
        "",
        "0",
        "00",
        "-1",
        "+7",
        "0x1F",
        "0X1f",
        "0o17",
        "0O17",
        "017",
        "08",
        "0b101",
        "1_000",
        "0x_1f",
        "0_7",
        "_1",
        "1_",
        "1__0",
        "0x",
        "0b",
        "0o",
        "1.0",
        " 1",
        "9223372036854775807",
        "9223372036854775808",
        "-9223372036854775808",
        "-9223372036854775809",
        "99999999999999999999x",
        "0xFFFFFFFFFFFFFFFF",
        "-0x8000000000000000",
    ];
    let bools = [
        "", "1", "t", "T", "TRUE", "true", "True", "0", "f", "F", "FALSE", "false", "False", "yes",
        "tRue", " true",
    ];
    let durations = [
        "",
        "0",
        "-0",
        "+0",
        "1s",
        "1.5h",
        "1h2m3.5s",
        "-1m",
        ".5s",
        "5.s",
        "1",
        "1x",
        "1µs",
        "1μs",
        "1us",
        "9223372036854775807ns",
        "9223372036854775808ns",
        "-9223372036854775808ns",
        "2562047h",
        "2562048h",
        "0.0000000001s",
        "1.123456789123s",
        "s",
        ".s",
        "1ss",
        "1s2",
        "3m.5s",
        "-",
        "1e3s",
        "1é",
        "é",
        "1\"s\\",
        "1\t",
    ];
    let floats = [
        "",
        "0.8",
        "1e39",
        "-1e39",
        "3.4028235e38",
        "3.4028236e38",
        "1e309",
        "1.7976931348623157e308",
        "1e-50",
        "inf",
        "+Inf",
        "-infinity",
        "INFINITY",
        "infinit",
        "nan",
        "NaN",
        "+nan",
        "-NaN",
        "0x1p-2",
        "0x1.8p1",
        "0X1P3",
        "0x1p",
        "0x.8p1",
        "0xp1",
        "1_000.5",
        "1__0",
        "_1",
        "1e",
        ".5",
        "5.",
        ".",
        "1E+3",
        "1e-3",
        "0x10",
        " 1",
        "1_e3",
        "1e_3",
        "0.1_5",
        "0x1p-1074",
        "0x0.8p-1022",
        "0x1.0000000000000801p0",
        "0x1.0000010000000000001p0",
        "0x0p999999999999",
        "0x1p99999999999999999999",
        "0x1p-99999999999",
        "0x1.fffffffffffff8p1023",
        "0x1p-1075",
        "0x1p-149",
        "0x_1p0",
        "0x1p_0",
        "0x1p0x",
        "0x1.8",
        "0x.p1",
        "0x1p+",
        "-0x0p0",
    ];
    let lists = ["", " ", "a", "a,b", "a,,b", ",", " a , b "];
    let maps = [
        "",
        " ",
        "a:b",
        "a:b,c:d",
        "a:b,a:c",
        "a",
        "a:b:c",
        "a:b,",
        ":",
        "a:",
        ":b",
        "é",
        "a\tb",
        "a\u{1}b",
        "\u{a0}",
        "\u{200b}",
        "\u{feff}",
        "\u{85}",
        "\u{2028}",
        "\u{ad}",
        "\u{1f600}",
        "\u{e0001}",
    ];
    let mut cases: Vec<Vec<(&str, &str)>> = vec![vec![]];
    for (variables, values) in [
        (
            &[
                "PORT",
                "XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES",
                "EXPIRATION_JITTER_MAX_SECONDS",
            ][..],
            &ints[..],
        ),
        (&["USE_STATSD", "RUNTIME_WATCH_ROOT"], &bools),
        (&["REDIS_TIMEOUT", "RUNTIME_SUBDIRECTORY"], &durations),
        (
            &["NEAR_LIMIT_RATIO", "XDS_CLIENT_BACKOFF_RANDOM_FACTOR"],
            &floats,
        ),
        (&["USE_DOG_STATSD_MOGRIFIERS", "MEMCACHE_HOST_PORT"], &lists),
        (
            &["EXTRA_TAGS", "CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS"],
            &maps,
        ),
    ] {
        for variable in variables {
            for value in values {
                cases.push(vec![(variable, value)]);
            }
        }
    }
    // The sinks: selection, gostats' own checks, DogStatsD mogrifiers from the environment.
    let statsd_off = ("USE_STATSD", "false");
    let dog = ("USE_DOG_STATSD", "true");
    let mogrifiers = ("USE_DOG_STATSD_MOGRIFIERS", "a,b");
    for case in [
        vec![("STATSD_PORT", "0x1F")],
        vec![("STATSD_PORT", "+8125")],
        vec![("STATSD_PORT", "1_000")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "x")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "99999999999999999999999x")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "9223372036854775808")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "-9223372036854775808")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "+")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "")],
        vec![("GOSTATS_LOGGING_SINK_DISABLED", "yes")],
        vec![("GOSTATS_LOGGING_SINK_DISABLED", "yes"), statsd_off],
        vec![dog],
        vec![
            dog,
            statsd_off,
            ("STATSD_PORT", "0x1F"),
            ("GOSTATS_FLUSH_INTERVAL_SECONDS", "x"),
        ],
        vec![dog, statsd_off, ("USE_PROMETHEUS", "true")],
        vec![("DISABLE_STATS", "true"), dog, ("STATSD_PORT", "x1")],
        vec![dog, statsd_off, mogrifiers],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("DOG_STATSD_MOGRIFIER_A_PATTERN", "^a"),
        ],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("PATTERN", "^x"),
            ("NAME", "n"),
        ],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("PATTERN", "^x"),
            ("NAME", "n"),
            ("TAGS", "t"),
        ],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("PATTERN", "^x"),
            ("NAME", "n"),
            ("TAGS", "t:"),
        ],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("PATTERN", "^x"),
            ("NAME", "n"),
            ("TAGS", ":v"),
        ],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("PATTERN", "^x"),
            ("NAME", "n"),
            ("DOG_STATSD_MOGRIFIER_B_PATTERN", ""),
        ],
        vec![
            dog,
            statsd_off,
            ("USE_DOG_STATSD_MOGRIFIERS", "lower"),
            ("DOG_STATSD_MOGRIFIER_LOWER_PATTERN", "x"),
            ("DOG_STATSD_MOGRIFIER_LOWER_NAME", "y"),
        ],
        vec![dog, statsd_off, ("STATSD_PORT", "70000")],
        vec![
            dog,
            statsd_off,
            ("USE_DOG_STATSD_MOGRIFIERS", "ß"),
            ("DOG_STATSD_MOGRIFIER_ß_PATTERN", "x"),
            ("DOG_STATSD_MOGRIFIER_ß_NAME", "y"),
        ],
        vec![dog, statsd_off, ("STATSD_HOST", "a:b")],
    ] {
        cases.push(case);
    }
    // envconfig also reads the fields of the untagged `*tls.Config` settings.
    for (variable, value) in [
        ("REDISTLSCONFIG_MINVERSION", "abc"),
        ("REDISTLSCONFIG_MINVERSION", "70000"),
        ("REDISTLSCONFIG_MAXVERSION", "0x304"),
        ("GRPCSERVERTLSCONFIG_INSECURESKIPVERIFY", "x"),
        ("CONFIGGRPCXDSTLSCONFIG_CLIENTAUTH", "z"),
        ("MEMCACHETLSCONFIG_CURVEPREFERENCES", "1,x"),
        ("MEMCACHETLSCONFIG_CURVEPREFERENCES", " "),
        ("REDISTLSCONFIG_CIPHERSUITES", "+1"),
        ("REDISTLSCONFIG_NAMETOCERTIFICATE", "a"),
        ("REDISTLSCONFIG_NAMETOCERTIFICATE", "a:b"),
        ("REDISTLSCONFIG_RENEGOTIATION", "1_0"),
        ("REDISTLSCONFIG_SERVERNAME", "anything"),
        ("REDISTLSCONFIG_SESSIONTICKETKEY", "x"),
        ("REDISTLSCONFIG_CERTIFICATES", "x,y"),
        ("REDISTLSCONFIG_ROOTCAS", "x"),
    ] {
        cases.push(vec![(variable, value)]);
    }
    // In field order: GrpcServerTlsConfig comes before GRPC_MAX_CONNECTION_AGE.
    cases.push(vec![
        ("GRPC_MAX_CONNECTION_AGE", "x"),
        ("GRPCSERVERTLSCONFIG_MINVERSION", "y"),
    ]);
    // Two invalid values: the first field in upstream's order is reported.
    cases.push(vec![
        ("TRACING_SAMPLING_RATE", "x"),
        ("HOST", "h"),
        ("PORT", "y"),
    ]);
    cases.push(vec![
        ("REDIS_URL", ""),
        ("PROMETHEUS_ADDR", ""),
        ("LOG_LEVEL", "debug"),
    ]);
    for environment in cases {
        let lookup = |key: &str| {
            environment
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        };
        let ours = llm_ratelimit::settings::Settings::from_lookup(&lookup)
            .map(|s| serde_json::json!({"settings": settings_json(&s), "sink": sink_failure(&s, &lookup)}))
            .map_err(|e| e.to_string());
        let upstream = upstream_settings(&environment).await;
        match (&ours, &upstream) {
            (Ok(ours), Ok(upstream)) => {
                assert_eq!(ours["settings"], upstream["settings"], "{environment:?}");
                assert_eq!(ours["sink"], upstream["sink"], "sink for {environment:?}");
            }
            _ => assert_eq!(ours, upstream, "{environment:?}"),
        }
    }
}

/// A temporary directory, removed when dropped (even when a test fails).
struct TempDir(PathBuf);

impl std::ops::Deref for TempDir {
    type Target = std::path::Path;
    fn deref(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A fresh temporary directory.
fn temp_dir(name: &str) -> TempDir {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "ratelimit-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

/// Writes `files` (path → contents; a contents of `->target` makes a symlink) under `root`.
fn write_tree(root: &std::path::Path, files: &[(&str, &[u8])]) {
    for (path, contents) in files {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        match contents.strip_prefix(b"->") {
            Some(target) => {
                std::os::unix::fs::symlink(std::str::from_utf8(target).unwrap(), &path).unwrap()
            }
            None if contents == b"/" => std::fs::create_dir_all(&path).unwrap(),
            None => std::fs::write(&path, contents).unwrap(),
        }
    }
}

/// Upstream's first runtime snapshot for `environment`, from the oracle.
async fn upstream_snapshot(environment: &[(&str, String)]) -> serde_json::Value {
    let output = Command::new(oracle_binary())
        .arg("-runtime-snapshot")
        .env_clear()
        .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
        .output()
        .await
        .expect("run the oracle");
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("oracle output: {}", String::from_utf8_lossy(&output.stdout)))
}

/// Ours, shaped like the oracle's JSON.
fn our_snapshot(environment: &[(&str, String)]) -> serde_json::Value {
    use base64::Engine;
    let settings = llm_ratelimit::settings::Settings::from_lookup(&lookup_in(environment)).unwrap();
    let stats = Stats::new(&settings.extra_tags);
    match llm_ratelimit::runtime::loader(&settings, &stats) {
        Err(e) => serde_json::json!({ "error": e }),
        Ok(loader) => {
            let snapshot: serde_json::Map<String, serde_json::Value> = loader
                .snapshot()
                .entries
                .into_iter()
                .map(|(k, v)| {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(v);
                    (k, serde_json::json!(encoded))
                })
                .collect();
            let counters: serde_json::Map<String, serde_json::Value> = stats
                .snapshot()
                .into_iter()
                .filter(|(name, _)| name.starts_with("runtime.") || name == "reserved_tag")
                .map(|(k, v)| (k, serde_json::json!(v)))
                .collect();
            serde_json::json!({ "snapshot": snapshot, "counters": counters })
        }
    }
}

/// goruntime's loader takes the same first snapshot of a directory as upstream: keys from
/// relative paths, symlinked files read through, symlinked directories and unreadable entries
/// counted as failures, dotfiles skipped only when asked, the nil loader without a
/// subdirectory; and fails to start the same way.
#[tokio::test]
async fn runtime_snapshots_are_taken_like_upstream() {
    if !tools_available() {
        return;
    }
    let tree: &[(&str, &[u8])] = &[
        ("v1/ratelimit/config/a.yaml", b"domain: a\n"),
        ("v1/ratelimit/config/b.yml", b"x"),
        ("v1/ratelimit/config/.hidden", b"h"),
        ("v1/ratelimit/config/sub/c.yaml", b"c"),
        ("v1/ratelimit/config/.dotdir/d.yaml", b"d"),
        ("v1/ratelimit/config/link.yaml", b"->a.yaml"),
        ("v1/ratelimit/config/dirlink", b"->sub"),
        ("v1/ratelimit/config/dangling", b"->nowhere"),
        ("v1/ratelimit/config/latin1", b"caf\xe9"),
        ("v1/ratelimit/config/empty", b""),
        ("v1/ratelimit/config.yaml", b"root level"),
        ("v1/ratelimit/other/e.yaml", b"e"),
        ("v1/ratelimit/config/..2024_01/k.yaml", b"k"),
        ("v1/ratelimit/config/..data", b"->..2024_01"),
        ("v1/ratelimit/config/k.yaml", b"->..data/k.yaml"),
        ("v1/ratelimit/afile", b"a file as the app directory"),
        ("v1/ratelimit/.x/y", b"y"),
        ("current", b"->v1"),
    ];
    let root = temp_dir("runtime");
    write_tree(&root, tree);
    use std::os::unix::ffi::OsStrExt;
    let latin1_name = std::ffi::OsStr::from_bytes(b"caf\xe9.yaml");
    // macOS refuses names that are not UTF-8.
    if cfg!(target_os = "linux") {
        std::fs::write(
            root.join("v1/ratelimit/config").join(latin1_name),
            b"latin1",
        )
        .unwrap();
    }
    let unreadable = root.join("v1/ratelimit/config/secret");
    std::fs::write(&unreadable, b"s").unwrap();
    let locked = root.join("v1/ratelimit/config/locked");
    std::fs::create_dir_all(&locked).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let path = |p: &str| root.join(p).to_string_lossy().into_owned();
    let env = |pairs: &[(&'static str, String)]| pairs.to_vec();
    let mut cases = Vec::new();
    for ignore in ["false", "true"] {
        let ignore = ("RUNTIME_IGNOREDOTFILES", ignore.to_string());
        cases.push(env(&[
            ("RUNTIME_ROOT", path("current")),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ignore.clone(),
        ]));
        for (subdirectory, app) in [
            ("ratelimit", "config"),
            ("ratelimit", "afile"),
            ("ratelimit", ".x"),
            ("", "ratelimit"),
        ] {
            cases.push(env(&[
                ("RUNTIME_ROOT", path("current")),
                ("RUNTIME_SUBDIRECTORY", subdirectory.into()),
                ("RUNTIME_APPDIRECTORY", app.into()),
                ("RUNTIME_WATCH_ROOT", "false".into()),
                ignore.clone(),
            ]));
        }
    }
    // A missing RUNTIME_ROOT is a failure; a missing watched directory, an error.
    cases.push(env(&[
        ("RUNTIME_ROOT", path("missing")),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
    ]));
    cases.push(env(&[
        ("RUNTIME_ROOT", path("missing/deeper")),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
    ]));
    cases.push(env(&[
        ("RUNTIME_ROOT", path("current")),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("RUNTIME_APPDIRECTORY", "missing".into()),
        ("RUNTIME_WATCH_ROOT", "false".into()),
    ]));
    // Relative paths, down to `.` (the test's directory, the same on both sides).
    cases.push(env(&[
        ("RUNTIME_ROOT", ".".into()),
        ("RUNTIME_SUBDIRECTORY", ".".into()),
        ("RUNTIME_APPDIRECTORY", "src".into()),
        ("RUNTIME_WATCH_ROOT", "false".into()),
    ]));
    cases.push(env(&[
        ("RUNTIME_ROOT", ".".into()),
        ("RUNTIME_SUBDIRECTORY", ".".into()),
        ("RUNTIME_APPDIRECTORY", ".".into()),
        ("RUNTIME_WATCH_ROOT", "false".into()),
        ("RUNTIME_IGNOREDOTFILES", "true".into()),
    ]));
    // Without a subdirectory: the nil loader; the runtime scope still counts reserved tags.
    cases.push(env(&[
        ("RUNTIME_ROOT", path("current")),
        ("EXTRA_TAGS", "host:h,az:z".into()),
    ]));
    cases.push(env(&[
        ("RUNTIME_ROOT", path("current")),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("EXTRA_TAGS", "host:h.1,app:x".into()),
    ]));
    for environment in cases {
        let (mut ours, mut upstream) = (
            our_snapshot(&environment),
            upstream_snapshot(&environment).await,
        );
        // Off Linux, upstream's fsnotify uses kqueue, whose errors read differently.
        if !cfg!(target_os = "linux")
            && ours.get("error").is_some()
            && upstream.get("error").is_some()
        {
            ours["error"] = serde_json::json!("error");
            upstream["error"] = serde_json::json!("error");
        }
        assert_eq!(ours, upstream, "{environment:?}");
    }
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
}

const LLM_CONFIG: &[u8] = b"domain: llm\ndescriptors:\n  - key: user\n    rate_limit: {unit: minute, requests_per_unit: 2}\n";
const OTHER_CONFIG: &[u8] = b"domain: other\ndescriptors:\n  - key: k\n    rate_limit: {unit: hour, requests_per_unit: 1}\n";

/// `RUNTIME_*` variables for a runtime directory at `root`, watched as a symlink
/// (`root/current`) or as a directory.
fn runtime_environment(root: &std::path::Path, watch_root: bool) -> Vec<(&'static str, String)> {
    vec![
        (
            "RUNTIME_ROOT",
            root.join("current").to_string_lossy().into_owned(),
        ),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("RUNTIME_APPDIRECTORY", "config".into()),
        ("RUNTIME_WATCH_ROOT", watch_root.to_string()),
        ("RUNTIME_IGNOREDOTFILES", "true".into()),
    ]
}

async fn file_provider_harness(root: &std::path::Path, watch_root: bool) -> Harness {
    let settings = Settings {
        runtime: Some(runtime_environment(root, watch_root)),
        extra_tags: [("host", "h")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..Default::default()
    };
    Harness::configured("", settings).await
}

/// The file provider loads the config files of the runtime directory at start, as upstream:
/// in watch-root mode only those under the app directory; a bad file fails the whole load.
#[tokio::test]
async fn the_file_provider_loads_the_runtime_directory_like_upstream() {
    if !tools_available() {
        return;
    }
    for watch_root in [true, false] {
        let root = temp_dir("provider");
        let wide: Vec<u8> = [0xFF, 0xFE]
            .into_iter()
            .chain(
                "domain: wide\ndescriptors:\n  - key: k\n    rate_limit: {unit: second, requests_per_unit: 1}\n"
                    .encode_utf16()
                    .flat_map(u16::to_le_bytes),
            )
            .collect();
        write_tree(
            &root,
            &[
                ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
                ("v1/ratelimit/config/wide.yaml", &wide),
                ("v1/ratelimit/config/nested/other.yaml", OTHER_CONFIG),
                ("v1/ratelimit/config/.ignored.yaml", b"not: yaml: at all"),
                ("v1/ratelimit/elsewhere.yaml", b"domain: elsewhere\n"),
                ("current", b"->v1"),
            ],
        );
        let mut h = file_provider_harness(&root, watch_root).await;
        h.at(1_700_000_000).await;
        for _ in 0..3 {
            h.call(request("llm", &[&[("user", "a")]])).await.unwrap();
        }
        h.call(request("other", &[&[("k", "v")]])).await.unwrap();
        h.call(request("wide", &[&[("k", "v")]])).await.unwrap();
        h.call(request("elsewhere", &[&[("k", "v")]]))
            .await
            .unwrap();
    }
    let root = temp_dir("provider");
    write_tree(
        &root,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v1/ratelimit/config/zz.yaml", b"domain: [unclosed\n"),
            ("current", b"->v1"),
        ],
    );
    let mut h = file_provider_harness(&root, true).await;
    h.at(1_700_000_000).await;
    let _ = h.call(request("llm", &[&[("user", "a")]])).await;
    // Bytes yaml.v2 cannot read fail the load too.
    for bad in [
        &b"domain: caf\xe9\n"[..],
        b"domain: a\x01\n",
        b"\xff\xfed\x00\x00\xdc",
    ] {
        let root = temp_dir("provider");
        write_tree(
            &root,
            &[("v1/ratelimit/config/bad.yaml", bad), ("current", b"->v1")],
        );
        let mut h = file_provider_harness(&root, true).await;
        let _ = h.call(request("a", &[&[("k", "v")]])).await;
    }
}

/// yaml.v2 reads a quoted scalar continued on lines less indented than its key (libyaml does
/// not check a flow scalar's indentation), folding its lines as usual.
#[tokio::test]
async fn quoted_scalars_continue_on_any_indentation_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    let config = "domain: \"dom\nain\"\ndescriptors:\n  - key: \"k\n  e\"\n    value: 'it''s\nv'\n    rate_limit:\n      unit: \"sec\nond\"\n      requests_per_unit: 1\n";
    // The unit folds to `sec ond`, not a unit: refused as upstream refuses it.
    assert!(h.load_config(config).await.is_err());
    let config = "domain: \"dom\nain\"\ndescriptors:\n  - key: \"k\n  e\"\n    value: 'it''s\nv'\n    rate_limit: {unit: second, requests_per_unit: 1}\n  - key: \"tab\\\n\t  bed\"\n    rate_limit: {unit: second, requests_per_unit: 2}\n  - key: plain\n    value: a 'b\n      \"c\n    rate_limit: {unit: second, requests_per_unit: 3}\n";
    h.load_config(config).await.unwrap();
    h.at(1_700_000_000).await;
    for _ in 0..2 {
        h.call(request(
            "dom ain",
            &[
                &[("k e", "it's v")],
                &[("tabbed", "")],
                // A plain scalar holding quotes, continued on a line starting with one.
                &[("plain", "a 'b \"c")],
            ],
        ))
        .await
        .unwrap();
    }
    h.assert_same_redis().await;
}

/// yaml.v2 reads a flow collection continued on lines less indented than its key (libyaml
/// does not check a flow collection's indentation either), and refuses what libyaml refuses: an
/// implicit key over several lines, an anchor with other characters than `[A-Za-z0-9_-]`.
#[tokio::test]
async fn flow_collections_continue_on_any_indentation_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    for config in [
        "domain: d\ndescriptors: [{\"ke\ny\": k}]\n",
        "domain: d\ndescriptors:\n  - \"ke\n  y\": k\n",
        "domain: &d.x d\n",
        "domain: &x: d\n",
    ] {
        assert!(h.load_config(config).await.is_err(), "{config:?}");
    }
    // An anchor's name ends at its first char out of `[A-Za-z0-9_-]`, the rest read on; a
    // document after the first is not read.
    h.load_config("domain: &d:x d\n").await.unwrap();
    h.call(request(":x d", &[&[("k", "v")]])).await.unwrap();
    h.load_config("domain: d\n---\nbad: &a.b c\n")
        .await
        .unwrap();
    let config = "domain: d\ndescriptors: [{key: k,\nrate_limit: {unit: second,\nrequests_per_unit: 1}},\n  {key: &n-1 j, value: [v,\nw], rate_limit: {unit: second, requests_per_unit: 2}}]\n";
    let config = config.replace("value: [v,\nw]", "value: \"v\nw\"");
    h.load_config(&config).await.unwrap();
    h.at(1_700_000_000).await;
    for _ in 0..3 {
        h.call(request("d", &[&[("k", "x")], &[("j", "v w")]]))
            .await
            .unwrap();
    }
    h.assert_same_redis().await;
}

/// yaml.v2's reader refuses control characters, and skips a byte order mark.
#[tokio::test]
async fn config_characters_are_read_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    for config in [
        "domain: a\x01",
        "domain: a\x7f",
        "# \x0b\ndomain: a",
        "domain: a\u{fffe}",
    ] {
        assert!(h.load_config(config).await.is_err(), "{config:?}");
    }
    h.load_config("\u{feff}domain: bom\n").await.unwrap();
    // NEL breaks lines in YAML 1.1; LS stays a character in a scalar.
    for config in [
        "domain: tab\tand\u{a0}\u{85}descriptors: []\n",
        "domain: \"a\u{2028}b\"\n",
        "domain: a\u{85}descriptors:\u{85}  - key: k\u{85}    rate_limit: {unit: second, requests_per_unit: 1}\n",
    ] {
        let _ = h.load_config(config).await;
    }
}

/// Reloads follow the same file system events as upstream's (inotify, so Linux only): an
/// atomic symlink swap in watch-root mode, a file moved into the directory otherwise.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_file_provider_reloads_like_upstream() {
    if !tools_available() {
        return;
    }
    // Watch root: v2 replaces v1 by renaming a new symlink over `current`.
    let root = temp_dir("reload");
    write_tree(
        &root,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v2/ratelimit/config/other.yaml", OTHER_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let mut h = file_provider_harness(&root, true).await;
    h.at(1_700_000_000).await;
    h.call(request("llm", &[&[("user", "a")]])).await.unwrap();
    std::os::unix::fs::symlink("v2", root.join("next")).unwrap();
    std::fs::rename(root.join("next"), root.join("current")).unwrap();
    h.await_config_loads(2).await;
    h.call(request("llm", &[&[("user", "a")]])).await.unwrap();
    h.call(request("other", &[&[("k", "v")]])).await.unwrap();
    // Edits inside the target do not reload, nor does a change of the symlink's own times
    // (a chmod event on it).
    std::fs::write(root.join("v2/ratelimit/config/llm.yaml"), LLM_CONFIG).unwrap();
    let touched = std::process::Command::new("touch")
        .arg("-h")
        .arg(root.join("current"))
        .status()
        .unwrap();
    assert!(touched.success());
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    h.call(request("llm", &[&[("user", "a")]])).await.unwrap();

    // Directory: a file moved in reloads; one moved out does not (a rename), a deleted one does.
    let root = temp_dir("reload");
    write_tree(
        &root,
        &[
            ("current/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("staging/other.yaml", OTHER_CONFIG),
        ],
    );
    let mut h = file_provider_harness(&root, false).await;
    h.at(1_700_000_000).await;
    h.call(request("other", &[&[("k", "v")]])).await.unwrap();
    std::fs::rename(
        root.join("staging/other.yaml"),
        root.join("current/ratelimit/config/other.yaml"),
    )
    .unwrap();
    h.await_config_loads(2).await;
    h.call(request("other", &[&[("k", "v")]])).await.unwrap();
    std::fs::rename(
        root.join("current/ratelimit/config/llm.yaml"),
        root.join("staging/llm.yaml"),
    )
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    h.call(request("llm", &[&[("user", "a")]])).await.unwrap();
    std::fs::remove_file(root.join("current/ratelimit/config/other.yaml")).unwrap();
    h.await_config_loads(3).await;
    h.call(request("llm", &[&[("user", "a")]])).await.unwrap();
    h.call(request("other", &[&[("k", "v")]])).await.unwrap();
}

/// Each config loaded makes upstream read the settings again (`SetConfig`'s
/// `settings.NewSettings`): a TLS file gone, a reload stops the service with its error.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn config_reloads_reread_the_tls_files_like_upstream() {
    if !tools_available() {
        return;
    }
    let root = temp_dir("reread");
    write_tree(
        &root,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v2/ratelimit/config/other.yaml", OTHER_CONFIG),
        ],
    );
    let test_pki = pki(&root.join("pki"));
    let (cert, key) = server_files(&root.join("certs"), &test_pki.server);
    let ca_pem = std::fs::read(&test_pki.ca).unwrap();
    let message = format!(
        "failed to read file: {0}: open {0}: no such file or directory",
        test_pki.ca.display()
    );
    let redis = Redis::start().await;
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        std::fs::write(&test_pki.ca, &ca_pem).unwrap();
        let _ = std::fs::remove_file(root.join("current"));
        std::os::unix::fs::symlink("v1", root.join("current")).unwrap();
        let http = reserved_port();
        let mut environment = binary_environment(&root, &redis);
        environment.extend([
            ("GRPC_HOST", "127.0.0.1".to_string()),
            ("GRPC_PORT", reserved_port().to_string()),
            ("PORT", http.to_string()),
            ("DEBUG_PORT", reserved_port().to_string()),
            ("GRPC_SERVER_USE_TLS", "true".into()),
            ("GRPC_SERVER_TLS_CERT", cert.clone()),
            ("GRPC_SERVER_TLS_KEY", key.clone()),
            (
                "GRPC_CLIENT_TLS_CACERT",
                test_pki.ca.to_string_lossy().into_owned(),
            ),
        ]);
        let logged = Logged::start(&binary, &environment, http).await;
        std::fs::remove_file(&test_pki.ca).unwrap();
        std::os::unix::fs::symlink("v2", root.join("next")).unwrap();
        std::fs::rename(root.join("next"), root.join("current")).unwrap();
        let stderr = logged.exited().await;
        assert!(stderr.contains(&message), "{}: {stderr}", binary.display());
    }
}

/// On SIGTERM, a gRPC client that answers grpc-go's drain (its ping) lets the service stop at
/// once; one gone silent holds it 5 s, then is cut, as upstream's.
#[tokio::test]
async fn silent_grpc_clients_hold_the_stop_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("silent");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        for silent in [false, true] {
            let (grpc, http) = (reserved_port(), reserved_port());
            let mut environment = binary_environment(&runtime, &redis);
            environment.extend([
                ("GRPC_HOST", "127.0.0.1".to_string()),
                ("GRPC_PORT", grpc.to_string()),
                ("PORT", http.to_string()),
                ("DEBUG_PORT", reserved_port().to_string()),
            ]);
            let logged = Logged::start(&binary, &environment, http).await;
            // A client that sent HTTP/2's preface and settings, then reads nothing; or one
            // kept connected, its connection task answering the ping.
            let mut _silent = None;
            let mut _client = None;
            if silent {
                use tokio::io::AsyncWriteExt;
                let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", grpc))
                    .await
                    .unwrap();
                stream
                    .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0")
                    .await
                    .unwrap();
                _silent = Some(stream);
            } else {
                let mut client =
                    RateLimitServiceClient::connect(format!("http://127.0.0.1:{grpc}"))
                        .await
                        .unwrap();
                client
                    .should_rate_limit(request("llm", &[&[("user", "a")]]))
                    .await
                    .unwrap();
                _client = Some(client);
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            let started = std::time::Instant::now();
            logged.stop().await;
            let took = started.elapsed();
            let expected = match silent {
                true => std::time::Duration::from_millis(4500)..std::time::Duration::from_secs(8),
                false => std::time::Duration::ZERO..std::time::Duration::from_secs(2),
            };
            assert!(
                expected.contains(&took),
                "{} with a silent client: {silent}, stopped in {took:?}",
                binary.display()
            );
        }
    }
}

/// Requests that are not gRPC get grpc-go's early answers: `415` for another content type,
/// then `405` for another method than `POST`, with their `grpc-status` and `grpc-message`.
#[tokio::test]
async fn non_grpc_requests_are_refused_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("non-grpc");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    let path = "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit";
    let cases: [(&str, Option<&str>); 7] = [
        ("GET", None),
        ("POST", Some("text/plain")),
        ("POST", Some("application/grpcx")),
        ("POST", Some("application/gRPC; x=\"é%\"")),
        ("GET", Some("application/grpc")),
        ("PUT", Some("application/grpc+proto")),
        ("POST", Some("application/json")),
    ];
    let mut answers = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let (grpc, http) = (reserved_port(), reserved_port());
        let mut environment = binary_environment(&runtime, &redis);
        environment.extend([
            ("GRPC_HOST", "127.0.0.1".to_string()),
            ("GRPC_PORT", grpc.to_string()),
            ("PORT", http.to_string()),
            ("DEBUG_PORT", reserved_port().to_string()),
        ]);
        let logged = Logged::start(&binary, &environment, http).await;
        let mut seen = Vec::new();
        for (method, content_type) in cases {
            let mut curl = std::process::Command::new("curl");
            curl.args([
                "-s",
                "-i",
                "--max-time",
                "5",
                "--http2-prior-knowledge",
                "-X",
                method,
            ]);
            if let Some(content_type) = content_type {
                curl.args(["-H", &format!("content-type: {content_type}")]);
            }
            let output = curl
                .arg(format!("http://127.0.0.1:{grpc}{path}"))
                .output()
                .unwrap();
            // The status line and the gRPC headers.
            let answer: Vec<String> = String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(|l| l.trim_end().to_string())
                .filter(|l| {
                    l.starts_with("HTTP/")
                        || l.starts_with("content-type")
                        || l.starts_with("grpc-")
                })
                .collect();
            seen.push(answer);
        }
        logged.stop().await;
        answers.push(seen);
    }
    assert!(
        answers[0][0][0].starts_with("HTTP/2 415"),
        "{:?}",
        answers[0]
    );
    assert_eq!(answers[1], answers[0]);
}

/// Upstream's own service binary (`src/service_cmd`), built at the pinned commit.
fn upstream_binary() -> PathBuf {
    static BINARY: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BINARY
        .get_or_init(|| {
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
            let binary = root.join("target/upstream-ratelimit");
            let status = std::process::Command::new("go")
                .args(["build", "-o"])
                .arg(&binary)
                .arg("./upstream")
                .current_dir(root.join("tools/oracle"))
                .status()
                .expect("run go build");
            assert!(status.success(), "go build failed");
            binary
        })
        .clone()
}

/// A service binary running with `environment`, killed when dropped.
struct Service {
    _process: Child,
    grpc: String,
    /// `HOST:PORT`.
    http: String,
    /// `DEBUG_HOST:DEBUG_PORT`.
    debug: String,
}

impl Service {
    async fn start(binary: &std::path::Path, environment: &[(&str, String)]) -> Self {
        let (grpc_port, http_port, debug_port) =
            (reserved_port(), reserved_port(), reserved_port());
        // The caller's variables come last, so that they win.
        let mut defaults = vec![
            ("GRPC_HOST", "127.0.0.1".to_string()),
            ("GRPC_PORT", grpc_port.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", http_port.to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", debug_port.to_string()),
        ];
        defaults.extend(environment.iter().cloned());
        let environment = defaults;
        let process = Command::new(binary)
            .env_clear()
            .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start the service");
        Self {
            _process: process,
            grpc: format!("http://127.0.0.1:{grpc_port}"),
            http: format!("127.0.0.1:{http_port}"),
            debug: format!("127.0.0.1:{debug_port}"),
        }
    }

    async fn client(&self) -> RateLimitServiceClient<Channel> {
        for _ in 0..300 {
            if let Ok(client) = RateLimitServiceClient::connect(self.grpc.clone()).await {
                return client;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("the service at {} did not start", self.grpc);
    }
}

/// What both binaries share: the runtime directory, Redis over TCP, no stats, no jitter.
fn binary_environment(runtime: &std::path::Path, redis: &Redis) -> Vec<(&'static str, String)> {
    vec![
        (
            "RUNTIME_ROOT",
            runtime.join("current").to_string_lossy().into_owned(),
        ),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("REDIS_SOCKET_TYPE", "tcp".into()),
        ("REDIS_URL", redis.address.clone()),
        ("DISABLE_STATS", "true".into()),
        ("EXPIRATION_JITTER_MAX_SECONDS", "0".into()),
        ("LOG_LEVEL", "error".into()),
    ]
}

/// Our binary serves like upstream's: the config from the runtime directory, the limits in
/// Redis, over gRPC.
#[tokio::test]
async fn the_binary_serves_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("binary");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let (upstream_redis, our_redis) = (Redis::start().await, Redis::start().await);
    let upstream = Service::start(
        &upstream_binary(),
        &binary_environment(&runtime, &upstream_redis),
    )
    .await;
    let ours = Service::start(
        std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
        &binary_environment(&runtime, &our_redis),
    )
    .await;
    let (mut upstream_client, mut our_client) = (upstream.client().await, ours.client().await);
    for request in [
        request("llm", &[&[("user", "a")]]),
        request("llm", &[&[("user", "a")]]),
        request("llm", &[&[("user", "a")]]),
        request("unknown", &[&[("k", "v")]]),
        request("", &[&[("k", "v")]]),
    ] {
        let simplify = |r: Result<tonic::Response<RateLimitResponse>, tonic::Status>| {
            r.map(tonic::Response::into_inner)
                .map_err(|s| format!("{:?}: {}", s.code(), s.message()))
        };
        let expected = simplify(upstream_client.should_rate_limit(request.clone()).await);
        let mut actual = simplify(our_client.should_rate_limit(request.clone()).await);
        // Each side reads its own clock: the time to reset may differ by a second.
        if let (Ok(expected), Ok(actual)) = (&expected, &mut actual) {
            for (e, a) in expected.statuses.iter().zip(actual.statuses.iter_mut()) {
                if let (Some(e), Some(a)) = (&e.duration_until_reset, &mut a.duration_until_reset) {
                    assert!((e.seconds - a.seconds).abs() <= 1, "{e:?} {a:?}");
                    a.seconds = e.seconds;
                    a.nanos = e.nanos;
                }
            }
        }
        assert_eq!(actual, expected, "response to {request:?}");
    }
}

/// What a binary prints when it cannot start, after it exits (it must).
async fn startup_failure(binary: &std::path::Path, environment: &[(&str, String)]) -> String {
    let child = Command::new(binary)
        .env_clear()
        .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start the service");
    let output = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
        .await
        .unwrap_or_else(|_| panic!("{} did not exit with {environment:?}", binary.display()))
        .unwrap();
    assert!(
        !output.status.success(),
        "{} started with {environment:?}",
        binary.display()
    );
    // logrus quotes its messages.
    String::from_utf8_lossy(&output.stderr)
        .replace("\\\"", "\"")
        .replace("\\n", "\n")
}

/// Our binary refuses to start where upstream's does (exits or panics), with its message.
#[tokio::test]
async fn the_binary_fails_to_start_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("binary");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let busy_port = busy.local_addr().unwrap().port().to_string();
    let test_pki = pki(&runtime.join("pki"));
    let ca = test_pki.ca.to_string_lossy().into_owned();
    let client_cert = test_pki.client_cert.to_string_lossy().into_owned();
    let other_key = runtime.join("pki").join("other-key.pem");
    std::fs::write(&other_key, &test_pki.server.1).unwrap();
    let other_key = other_key.to_string_lossy().into_owned();
    // A server key pair whose files are in two directories.
    let (server_cert, _) = server_files(&runtime.join("pki"), &test_pki.server);
    let (_, apart_key) = server_files(&runtime.join("apart"), &test_pki.server);
    // ElastiCache IAM: a home without AWS files, IMDS off, and profiles of their own.
    let home = runtime.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let aws_config = runtime.join("aws-config");
    std::fs::write(
        &aws_config,
        "[profile bogus-source]\ncredential_source = Elsewhere\nrole_arn = arn:aws:iam::1:role/r\n\
         [profile orphan]\nsource_profile = missing\nrole_arn = arn:aws:iam::1:role/r\n\
         [profile no-role]\nsource_profile = keys\n\
         [profile keys]\naws_access_key_id = AKID\naws_secret_access_key = secret\n\
         [profile mfa]\nsource_profile = keys\nrole_arn = arn:aws:iam::1:role/r\nmfa_serial = arn:aws:iam::1:mfa/me\n\
         [profile two-kinds]\ncredential_process = /bin/true\ncredential_source = Environment\nrole_arn = arn:aws:iam::1:role/r\n\
         [profile bundle]\nca_bundle = /nowhere/profile-ca.pem\n\
         [profile loop-a]\nrole_arn = arn:aws:iam::1:role/a\nsource_profile = loop-b\n\
         [profile loop-b]\nrole_arn = arn:aws:iam::1:role/b\nsource_profile = loop-a\n\
         [profile itself]\nrole_arn = arn:aws:iam::1:role/i\nsource_profile = itself\n\
         [profile from-environment]\nrole_arn = arn:aws:iam::1:role/e\ncredential_source = Environment\n",
    )
    .unwrap();
    let iam = |extra: &[(&'static str, &str)]| {
        let mut variables: Vec<(&'static str, String)> = vec![
            ("HOME", home.to_string_lossy().into_owned()),
            ("AWS_CONFIG_FILE", aws_config.to_string_lossy().into_owned()),
            ("AWS_EC2_METADATA_DISABLED", "true".into()),
            ("REDIS_AWS_IAM_AUTH", "true".into()),
            ("REDIS_AWS_IAM_CACHE_NAME", "MyCache".into()),
            ("REDIS_AWS_IAM_USER_ID", "app".into()),
            ("AWS_REGION", "eu-west-3".into()),
        ];
        variables.extend(extra.iter().map(|(k, v)| (*k, v.to_string())));
        variables
    };
    let cases: Vec<(Vec<(&'static str, String)>, String)> = vec![
        (iam(&[("REDIS_AUTH", "pw")]), "REDIS_AWS_IAM_AUTH cannot be combined with REDIS_AUTH or REDIS_AUTH_FILE".into()),
        // The per-second pool, its own variables named.
        (iam(&[("REDIS_PERSECOND", "true"), ("REDIS_PERSECOND_AWS_IAM_AUTH", "true"), ("REDIS_PERSECOND_AUTH_FILE", "/nowhere")]), "REDIS_PERSECOND_AWS_IAM_AUTH cannot be combined with REDIS_PERSECOND_AUTH or REDIS_PERSECOND_AUTH_FILE".into()),
        (iam(&[("REDIS_PERSECOND", "true"), ("REDIS_PERSECOND_AWS_IAM_AUTH", "true")]), "REDIS_PERSECOND_AWS_IAM_CACHE_NAME and REDIS_PERSECOND_AWS_IAM_USER_ID are required when REDIS_PERSECOND_AWS_IAM_AUTH is enabled".into()),
        (iam(&[("REDIS_AWS_IAM_USER_ID", "")]), "REDIS_AWS_IAM_CACHE_NAME and REDIS_AWS_IAM_USER_ID are required when REDIS_AWS_IAM_AUTH is enabled".into()),
        (iam(&[("AWS_REGION", "")]), "REDIS_AWS_IAM_REGION is required when the AWS SDK cannot resolve a region".into()),
        // Memcached: its TLS files, read with the settings whatever the backend, after Redis's;
        // its servers from one place; an SRV record that does not parse or resolve.
        (vec![("REDIS_TLS", "true".into()), ("REDIS_TLS_CACERT", "/nowhere/redis.pem".into()), ("MEMCACHE_TLS", "true".into()), ("MEMCACHE_TLS_CACERT", "/nowhere/mc.pem".into())], "failed to read file: /nowhere/redis.pem: open /nowhere/redis.pem: no such file or directory".into()),
        (vec![("MEMCACHE_TLS", "true".into()), ("MEMCACHE_TLS_CACERT", "/nowhere/mc.pem".into())], "failed to read file: /nowhere/mc.pem: open /nowhere/mc.pem: no such file or directory".into()),
        (vec![("MEMCACHE_TLS", "true".into()), ("MEMCACHE_TLS_CACERT", "/nowhere/mc.pem".into()), ("GRPC_SERVER_USE_TLS", "true".into()), ("GRPC_CLIENT_TLS_CACERT", "/nowhere/grpc.pem".into())], "failed to read file: /nowhere/mc.pem: open /nowhere/mc.pem: no such file or directory".into()),
        (vec![("BACKEND_TYPE", "memcache".into()), ("MEMCACHE_HOST_PORT", "127.0.0.1:1".into()), ("MEMCACHE_SRV", "_memcache._tcp.example.com".into())], "Both MEMCACHE_HOST_PORT and MEMCACHE_SRV are set".into()),
        (vec![("BACKEND_TYPE", "memcache".into()), ("MEMCACHE_SRV", "memcache.example.com".into())], "Unable to fetch servers from SRV".into()),
        (vec![("BACKEND_TYPE", "memcache".into()), ("MEMCACHE_SRV", "_memcache._tcp.nosuch.invalid".into())], "Unable to fetch servers from SRV".into()),
        // The AWS configuration's own failures, before the region's.
        (iam(&[("AWS_REGION", ""), ("AWS_PROFILE", "nope")]), "failed to get shared config profile, nope".into()),
        (iam(&[("AWS_PROFILE", "bogus-source")]), "credential_source values must be EcsContainer, Ec2InstanceMetadata, or Environment".into()),
        (iam(&[("AWS_PROFILE", "orphan")]), "failed to load assume role arn:aws:iam::1:role/r, of profile missing, failed to get shared config profile, missing".into()),
        (iam(&[("AWS_PROFILE", "no-role")]), "credential type source_profile requires role_arn, profile no-role".into()),
        (iam(&[("AWS_PROFILE", "mfa")]), "assume role with MFA enabled, but AssumeRoleTokenProvider session option not set.".into()),
        (iam(&[("AWS_PROFILE", "two-kinds")]), "only one credential type may be specified per profile: source profile, credential source, credential process, web identity token".into()),
        (iam(&[("AWS_WEB_IDENTITY_TOKEN_FILE", "/nowhere/token")]), "role ARN is not set".into()),
        // A chain of source profiles back to one met before, which then has no credentials.
        (iam(&[("AWS_PROFILE", "loop-a")]), "failed to load assume role arn:aws:iam::1:role/b, of profile loop-a, <nil>".into()),
        (iam(&[("AWS_PROFILE", "itself")]), "failed to load assume role arn:aws:iam::1:role/i, of profile itself, <nil>".into()),
        // The environment's keys, absent: refused at each connection, before any call to STS
        // (the SDK's context around the error is its own, docs/parity.md §7).
        (iam(&[("AWS_PROFILE", "from-environment"), ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s")]), "static credentials are empty".into()),
        // A CA bundle, the environment's before the profile's, must hold a certificate.
        (iam(&[("AWS_PROFILE", "bundle"), ("AWS_CA_BUNDLE", "/nowhere/ca.pem")]), "open /nowhere/ca.pem: no such file or directory".into()),
        (iam(&[("AWS_PROFILE", "bundle")]), "open /nowhere/profile-ca.pem: no such file or directory".into()),
        (iam(&[("AWS_CA_BUNDLE", &aws_config.to_string_lossy())]), "failed to load custom CA bundle PEM file".into()),
        (iam(&[("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://10.1.2.3/creds")]), "invalid endpoint host, \"10.1.2.3\", only loopback/ecs/eks hosts are allowed".into()),
        (vec![("PORT", "x".into())], "envconfig.Process: assigning PORT to Port: converting 'x' to type int. details: strconv.ParseInt: parsing \"x\": invalid syntax".into()),
        (vec![("LOG_LEVEL", "bogus".into())], "Could not parse log level. not a valid logrus Level: \"bogus\"".into()),
        (vec![("DISABLE_STATS", "false".into()), ("USE_DOG_STATSD", "true".into())], "Error: unable to use more than one stats sink at the same time. Set one of USE_DOG_STATSD, USE_STATSD, USE_PROMETHEUS.".into()),
        (vec![("DISABLE_STATS", "false".into()), ("STATS_FLUSH_INTERVAL", "0".into())], "non-positive interval for NewTicker".into()),
        (vec![("CONFIG_TYPE", "bogus".into())], "Invalid setting for ConfigType: bogus".into()),
        (vec![("RUNTIME_WATCH_ROOT", "false".into()), ("RUNTIME_APPDIRECTORY", "missing".into())], format!("unable to watch file ({}/ratelimit/missing)", runtime.join("current").display())),
        (vec![("BACKEND_TYPE", "bogus".into())], "Invalid setting for BackendType: bogus".into()),
        (vec![("TRACING_ENABLED", "true".into()), ("TRACING_EXPORTER_PROTOCOL", "bogus".into())], "Invalid otlptrace client protocol: bogus".into()),
        // An HTTP exporter given a CA for an insecure endpoint.
        (vec![("TRACING_ENABLED", "true".into()), ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1".into()), ("OTEL_EXPORTER_OTLP_CERTIFICATE", ca.clone())], "creating OTLP trace exporter: insecure HTTP endpoint cannot use TLS client configuration".into()),
        (vec![("REDIS_POOL_ON_EMPTY_BEHAVIOR", "create".into())], "REDIS_POOL_ON_EMPTY_BEHAVIOR=CREATE is not supported in radix v4.".into()),
        (vec![("REDIS_POOL_ON_EMPTY_BEHAVIOR", "Error".into())], "REDIS_POOL_ON_EMPTY_BEHAVIOR=ERROR is not supported in radix v4. Pool will block instead of failing fast. Remove this setting or set to WAIT, and use context timeouts for fail-fast behavior.".into()),
        (vec![("REDIS_TYPE", "Foo".into())], "Unrecognized redis type Foo".into()),
        // CLUSTER: the parallelism checked first; then no node that dials, or one that is no
        // cluster's (READONLY refused).
        (vec![("REDIS_TYPE", "cluster".into()), ("REDIS_CLUSTER_PIPELINE_PARALLELISM", "-1".into())], "redis cluster pipeline parallelism must be >= 0".into()),
        (vec![("REDIS_TYPE", "cluster".into()), ("REDIS_URL", "127.0.0.1:1".into()), ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into())], "could not connect to any redis instances, last error was: dial tcp 127.0.0.1:1: connect: connection refused".into()),
        // Redis refuses READONLY outside a cluster; Valkey takes it, then refuses the topology
        // (the error then follows the elapsed time, `after 1.0s:`).
        (vec![("REDIS_TYPE", "CLUSTER".into()), ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into())], match redis_server().contains("valkey") {
            false => "could not connect to any redis instances, last error was: response returned from Conn: ERR This instance has cluster support disabled".into(),
            true => "s: response returned from Conn: ERR This instance has cluster support disabled".into(),
        }),
        // Without READONLY (its dialer replaced), the topology is refused instead.
        (vec![("REDIS_TYPE", "cluster".into()), ("REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR", "true".into()), ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into())], "s: response returned from Conn: ERR This instance has cluster support disabled".into()),
        // The TLS files, read with the settings when either pool uses TLS.
        (vec![("REDIS_TLS", "true".into()), ("REDIS_TLS_CACERT", "/nowhere/ca.pem".into())], "failed to read file: /nowhere/ca.pem: open /nowhere/ca.pem: no such file or directory".into()),
        (vec![("REDIS_PERSECOND_TLS", "true".into()), ("REDIS_TLS_CACERT", runtime.join("current").to_string_lossy().into_owned() + "/ratelimit/config/llm.yaml")], format!("failed to load the provided TLS CA certificate: {}/ratelimit/config/llm.yaml", runtime.join("current").display())),
        (vec![("REDIS_TLS", "true".into()), ("REDIS_TLS_CLIENT_CERT", "/nowhere/c.pem".into()), ("REDIS_TLS_CLIENT_KEY", "/nowhere/k.pem".into())], "failed lo load TLS key pair (/nowhere/c.pem,/nowhere/k.pem): open /nowhere/c.pem: no such file or directory".into()),
        // A key not for the certificate: refused before the CA is read.
        (vec![("REDIS_TLS", "true".into()), ("REDIS_TLS_CLIENT_CERT", client_cert.clone()), ("REDIS_TLS_CLIENT_KEY", other_key.clone()), ("REDIS_TLS_CACERT", "/nowhere/ca.pem".into())], format!("failed lo load TLS key pair ({client_cert},{other_key}): tls: private key does not match public key")),
        (vec![("REDIS_AUTH", "pw".into()), ("REDIS_AUTH_FILE", "/nowhere".into())], "REDIS_AUTH and REDIS_AUTH_FILE are mutually exclusive".into()),
        // The gRPC server's TLS files, read with the settings, then by the cert provider.
        (vec![("GRPC_SERVER_USE_TLS", "true".into()), ("GRPC_SERVER_TLS_CERT", "/nowhere/c.pem".into()), ("GRPC_SERVER_TLS_KEY", "/nowhere/k.pem".into())], "failed lo load TLS key pair (/nowhere/c.pem,/nowhere/k.pem): open /nowhere/c.pem: no such file or directory".into()),
        (vec![("GRPC_SERVER_USE_TLS", "true".into()), ("GRPC_CLIENT_TLS_CACERT", "/nowhere/ca.pem".into())], "failed to read file: /nowhere/ca.pem: open /nowhere/ca.pem: no such file or directory".into()),
        (vec![("GRPC_SERVER_USE_TLS", "true".into()), ("GRPC_SERVER_TLS_CERT", server_cert.clone()), ("GRPC_SERVER_TLS_KEY", apart_key.clone())], "certFile and keyFile must be in the same directory".into()),
        (vec![("GRPC_SERVER_USE_TLS", "true".into()), ("GRPC_SERVER_TLS_CERT", server_cert.clone()), ("GRPC_SERVER_TLS_KEY", runtime.join("pki/missing.pem").to_string_lossy().into_owned())], format!("failed lo load TLS key pair ({server_cert},{}/pki/missing.pem): open {}/pki/missing.pem: no such file or directory", runtime.display(), runtime.display())),
        // The xDS client's TLS files, read with the settings whatever the config type, after
        // the gRPC server's.
        (vec![("CONFIG_GRPC_XDS_SERVER_USE_TLS", "true".into()), ("CONFIG_GRPC_XDS_SERVER_TLS_CACERT", "/nowhere/xds.pem".into())], "failed to read file: /nowhere/xds.pem: open /nowhere/xds.pem: no such file or directory".into()),
        (vec![("CONFIG_TYPE", "GRPC_XDS_SOTW".into()), ("CONFIG_GRPC_XDS_SERVER_USE_TLS", "true".into()), ("CONFIG_GRPC_XDS_CLIENT_TLS_CERT", "/nowhere/c.pem".into()), ("CONFIG_GRPC_XDS_CLIENT_TLS_KEY", "/nowhere/k.pem".into())], "failed lo load TLS key pair (/nowhere/c.pem,/nowhere/k.pem): open /nowhere/c.pem: no such file or directory".into()),
        (vec![("GRPC_SERVER_USE_TLS", "true".into()), ("GRPC_CLIENT_TLS_CACERT", "/nowhere/grpc.pem".into()), ("CONFIG_GRPC_XDS_SERVER_USE_TLS", "true".into()), ("CONFIG_GRPC_XDS_SERVER_TLS_CACERT", "/nowhere/xds.pem".into())], "failed to read file: /nowhere/grpc.pem: open /nowhere/grpc.pem: no such file or directory".into()),
        // Without a key pair, the cert provider loads none.
        (vec![("GRPC_SERVER_USE_TLS", "true".into())], "CertProvider failed to load any certificate, exiting.".into()),
        // Redis refuses the database: its error ends the message.
        (vec![("REDIS_URL", format!("redis://{}/abc", redis.address)), ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into())], "value is not an integer or out of range".into()),
        (vec![("REDIS_URL", "::1:6379".into()), ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into())], "timed out waiting for Redis connection to ::1:6379 after ".into()),
        (vec![("REDIS_URL", "127.0.0.1:1".into()), ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into())], "timed out waiting for Redis connection to 127.0.0.1:1 after ".into()),
        (vec![("GRPC_PORT_BUSY", busy_port.clone())], format!("Failed to listen for gRPC on '127.0.0.1:{busy_port}': ")),
    ];
    for (overrides, message) in cases {
        let mut environment = binary_environment(&runtime, &redis);
        environment.extend([("GRPC_HOST", "127.0.0.1".to_string())]);
        environment.extend([
            ("PORT", reserved_port().to_string()),
            ("DEBUG_PORT", reserved_port().to_string()),
        ]);
        for (key, value) in overrides {
            let key = if key == "GRPC_PORT_BUSY" {
                "GRPC_PORT"
            } else {
                key
            };
            environment.retain(|(k, _)| *k != key);
            environment.push((key, value));
        }
        if !environment.iter().any(|(k, _)| *k == "GRPC_PORT") {
            environment.push(("GRPC_PORT", reserved_port().to_string()));
        }
        let upstream = startup_failure(&upstream_binary(), &environment).await;
        assert!(
            upstream.contains(&message),
            "upstream with {environment:?}:\n{upstream}"
        );
        let ours = startup_failure(
            std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
            &environment,
        )
        .await;
        assert!(
            ours.contains(&message),
            "ours with {environment:?}:\n{ours}"
        );
        // A `logger.Fatal` upstream: the same logrus lines.
        let fatal = upstream.contains("level=fatal");
        if fatal {
            // Line by line: each line's time masked.
            let lines = |stderr: &str| logrus_lines(stderr, &[]);
            assert_eq!(lines(&ours), lines(&upstream), "{environment:?}");
        }
    }
    drop(busy);
}

/// A gRPC client over the Unix socket at `path`, over TLS with `tls` if given.
async fn unix_client(
    path: &std::path::Path,
    tls: Option<rustls::ClientConfig>,
) -> RateLimitServiceClient<Channel> {
    let tls = tls.map(|config| tokio_rustls::TlsConnector::from(Arc::new(config)));
    for _ in 0..300 {
        let (path, tls) = (path.to_path_buf(), tls.clone());
        let connector = tower::service_fn(move |_: tonic::transport::Uri| {
            let (path, tls) = (path.clone(), tls.clone());
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                let stream: Box<dyn TokioStream> = match tls {
                    Some(tls) => {
                        let name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
                        Box::new(tls.connect(name, stream).await?)
                    }
                    None => Box::new(stream),
                };
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        });
        let endpoint = tonic::transport::Endpoint::from_static("http://[::]:50051");
        if let Ok(channel) = endpoint.connect_with_connector(connector).await {
            return RateLimitServiceClient::new(channel);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("no gRPC server on {}", path.display());
}

/// A connection a client reads and writes.
trait TokioStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> TokioStream for T {}

/// With `GRPC_UDS`, both binaries serve gRPC on that Unix socket instead; with
/// `GRPC_SERVER_USE_TLS` too, over TLS.
#[tokio::test]
async fn the_binary_serves_on_a_unix_socket_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("uds");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let pki = pki(&runtime.join("pki"));
    let (cert, key) = server_files(&runtime.join("certs"), &pki.server);
    let ca = std::fs::read_to_string(&pki.ca).unwrap();
    for tls in [false, true] {
        let (upstream_redis, our_redis) = (Redis::start().await, Redis::start().await);
        let mut clients = Vec::new();
        let mut services = Vec::new();
        for (binary, redis, socket) in [
            (
                upstream_binary(),
                &upstream_redis,
                runtime.join(format!("upstream-{tls}.sock")),
            ),
            (
                PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
                &our_redis,
                runtime.join(format!("ours-{tls}.sock")),
            ),
        ] {
            let mut environment = binary_environment(&runtime, redis);
            environment.push(("GRPC_UDS", socket.to_string_lossy().into_owned()));
            if tls {
                environment.extend([
                    ("GRPC_SERVER_USE_TLS", "true".to_string()),
                    ("GRPC_SERVER_TLS_CERT", cert.clone()),
                    ("GRPC_SERVER_TLS_KEY", key.clone()),
                ]);
            }
            services.push(Service::start(&binary, &environment).await);
            let config = tls.then(|| tls_client_config(&ca, None, true));
            clients.push(unix_client(&socket, config).await);
        }
        let request = request("llm", &[&[("user", "a")]]);
        let expected = clients[0]
            .should_rate_limit(request.clone())
            .await
            .unwrap()
            .into_inner();
        let actual = clients[1]
            .should_rate_limit(request)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(actual.overall_code, expected.overall_code);
        assert_eq!(actual.statuses.len(), expected.statuses.len());
        assert_eq!(
            actual.statuses[0].limit_remaining,
            expected.statuses[0].limit_remaining
        );
    }
}

/// On SIGTERM both binaries stop serving gracefully and exit successfully.
#[tokio::test]
async fn the_binary_stops_on_sigterm_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("sigterm");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let mut service = Service::start(&binary, &binary_environment(&runtime, &redis)).await;
        let mut client = service.client().await;
        client
            .should_rate_limit(request("llm", &[&[("user", "a")]]))
            .await
            .unwrap();
        let pid = service._process.id().unwrap();
        let killed = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .unwrap();
        assert!(killed.success());
        let status =
            tokio::time::timeout(std::time::Duration::from_secs(30), service._process.wait())
                .await
                .expect("the service did not stop")
                .unwrap();
        assert!(status.success(), "{}: {status}", binary.display());
    }
}

type HealthClient = llm_ratelimit_proto::grpc::health::v1::health_client::HealthClient<Channel>;

/// Both binaries started alike, ready to serve.
async fn both_services(
    environment: impl Fn(&Redis) -> Vec<(&'static str, String)>,
) -> (Vec<Service>, Vec<Redis>) {
    let redis = vec![Redis::start().await, Redis::start().await];
    let mut services = Vec::new();
    for (binary, redis) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ]
    .iter()
    .zip(&redis)
    {
        let service = Service::start(binary, &environment(redis)).await;
        service.client().await;
        services.push(service);
    }
    (services, redis)
}

/// `/healthcheck` and gorilla/mux's routing around it answer like upstream's.
#[tokio::test]
async fn the_healthcheck_answers_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("health");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let (services, _redis) = both_services(|redis| binary_environment(&runtime, redis)).await;
    for (method, target) in [
        ("GET", "/healthcheck"),
        ("POST", "/healthcheck"),
        ("HEAD", "/healthcheck"),
        ("GET", "/healthcheck/"),
        ("GET", "//healthcheck"),
        ("GET", "/x/../healthcheck?y=1"),
        ("GET", "/health%63heck"),
        ("GET", "/other"),
        ("GET", "/"),
        ("GET", "/a%20b/../c%2Fd"),
    ] {
        let upstream = http_request(&services[0].http, method, target, &[]).await;
        let ours = http_request(&services[1].http, method, target, &[]).await;
        assert_eq!(ours, upstream, "{method} {target}");
    }
}

/// The gRPC health service answers like grpc-go's: "" and "ratelimit" serving, other names
/// unknown; with HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED, a config without domains is
/// unhealthy.
#[tokio::test]
async fn health_is_reported_like_upstream() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit_proto::grpc::health::v1::{HealthCheckRequest, HealthListRequest};
    let check = |service: &str| HealthCheckRequest {
        service: service.into(),
    };
    let summary = |r: Result<
        tonic::Response<llm_ratelimit_proto::grpc::health::v1::HealthCheckResponse>,
        tonic::Status,
    >| {
        r.map(|r| r.into_inner().status)
            .map_err(|s| format!("{:?}: {}", s.code(), s.message()))
    };
    for empty in [false, true] {
        let runtime = temp_dir("health");
        let files: &[(&str, &[u8])] = match empty {
            false => &[
                ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
                ("current", b"->v1"),
            ],
            true => &[("v1/ratelimit/config", b"/"), ("current", b"->v1")],
        };
        write_tree(&runtime, files);
        let (services, _redis) = both_services(|redis| {
            let mut environment = binary_environment(&runtime, redis);
            environment.push(("HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED", "true".into()));
            environment
        })
        .await;
        let mut answers = Vec::new();
        for service in &services {
            let mut client = HealthClient::connect(service.grpc.clone()).await.unwrap();
            let mut statuses = Vec::new();
            for name in ["", "ratelimit", "other"] {
                statuses.push(summary(client.check(check(name)).await));
            }
            let mut listed: Vec<(String, i32)> = client
                .list(HealthListRequest {})
                .await
                .unwrap()
                .into_inner()
                .statuses
                .into_iter()
                .map(|(k, v)| (k, v.status))
                .collect();
            listed.sort();
            let http = http_request(&service.http, "GET", "/healthcheck", &[]).await;
            answers.push((statuses, listed, http.status, http.body));
        }
        assert_eq!(answers[1], answers[0], "empty config: {empty}");
    }
}

/// On SIGTERM the service turns unhealthy at once (an open Watch sees it, /healthcheck
/// answers 500) and stops once the Watch ends.
#[tokio::test]
async fn sigterm_turns_health_off_like_upstream() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit_proto::grpc::health::v1::HealthCheckRequest;
    let runtime = temp_dir("health");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let (mut services, _redis) = both_services(|redis| binary_environment(&runtime, redis)).await;
    let mut seen = Vec::new();
    for service in &mut services {
        let mut client = HealthClient::connect(service.grpc.clone()).await.unwrap();
        let request = HealthCheckRequest {
            service: "ratelimit".into(),
        };
        let mut watch = client.watch(request).await.unwrap().into_inner();
        let first = tokio::time::timeout(std::time::Duration::from_secs(10), watch.message())
            .await
            .expect("no initial health status")
            .unwrap()
            .unwrap()
            .status;
        let pid = service._process.id().unwrap().to_string();
        assert!(
            std::process::Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        let second = tokio::time::timeout(std::time::Duration::from_secs(10), watch.message())
            .await
            .expect("no health change after SIGTERM")
            .unwrap()
            .unwrap()
            .status;
        let http = http_request(&service.http, "GET", "/healthcheck", &[])
            .await
            .status;
        drop(watch);
        drop(client);
        let status =
            tokio::time::timeout(std::time::Duration::from_secs(30), service._process.wait())
                .await
                .expect("the service did not stop")
                .unwrap();
        seen.push((first, second, http, status.success()));
    }
    assert_eq!(seen[1], seen[0]);
}

/// While Redis is down, every decision fails at once with upstream's gRPC status and radix's
/// error, so that Envoy and agentgateway apply their failure mode. Upstream fails one decision
/// per pooled connection, then waits for Redis (radix's `WAIT` pool): a deliberate divergence
/// (docs/parity.md §7). Once Redis is back, decisions succeed again.
#[tokio::test]
async fn decisions_fail_at_once_while_redis_is_down() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("redis-down");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let (services, redis) = both_services(|redis| binary_environment(&runtime, redis)).await;
    let decide = |service: &Service| {
        let grpc = service.grpc.clone();
        async move {
            let mut client = RateLimitServiceClient::connect(grpc).await.unwrap();
            let call = client.should_rate_limit(request("llm", &[&[("user", "alice")]]));
            match tokio::time::timeout(std::time::Duration::from_secs(5), call).await {
                Ok(Ok(response)) => format!("{:?}", response.into_inner().overall_code),
                Ok(Err(status)) => format!("{:?}: {}", status.code(), status.message()),
                Err(_) => "no answer within 5 s".to_string(),
            }
        }
    };
    for service in &services {
        assert_eq!(decide(service).await, "1", "{}", service.grpc);
    }
    for redis in &redis {
        let _: Result<(), _> = redis
            .client
            .custom(
                CustomCommand::new_static("SHUTDOWN", ClusterHash::Random, false),
                vec!["NOSAVE"],
            )
            .await;
    }
    // radix's message for the connection Redis closed: `EOF`, or the socket's own error.
    let failed = "Unknown: response returned from Conn: ";
    // Upstream fails a decision per pooled connection left (REDIS_POOL_SIZE, 10; its pings
    // drop some meanwhile), then waits for Redis.
    let mut upstream_failures = 0;
    for attempt in 0..12 {
        let ours = decide(&services[1]).await;
        assert_eq!(ours, format!("{failed}EOF"), "decision {attempt}");
        let upstream = decide(&services[0]).await;
        if upstream == "no answer within 5 s" {
            break;
        }
        // radix also says `closed` for a connection of its pool that it closed.
        assert!(upstream.starts_with("Unknown: "), "{attempt}: {upstream}");
        upstream_failures += 1;
    }
    assert!(
        (1..=10).contains(&upstream_failures),
        "upstream failed {upstream_failures} decisions before waiting"
    );
    let mut restarted = Vec::new();
    for redis in &redis {
        let port = redis.address.rsplit_once(':').unwrap().1.parse().unwrap();
        restarted.push(Redis::start_on(port).await.expect("Redis restarted"));
    }
    for service in &services {
        let mut answer = String::new();
        for _ in 0..150 {
            answer = decide(service).await;
            if answer == "1" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(answer, "1", "{} once Redis is back", service.grpc);
    }
}

/// With REDIS_HEALTH_CHECK_ACTIVE_CONNECTION, losing Redis makes the service unhealthy;
/// without it, health does not follow Redis.
#[tokio::test]
async fn redis_connections_drive_health_like_upstream() {
    if !tools_available() {
        return;
    }
    for active in [true, false] {
        let runtime = temp_dir("health");
        write_tree(
            &runtime,
            &[
                ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
                ("current", b"->v1"),
            ],
        );
        let (services, redis) = both_services(|redis| {
            let mut environment = binary_environment(&runtime, redis);
            environment.push(("REDIS_HEALTH_CHECK_ACTIVE_CONNECTION", active.to_string()));
            environment
        })
        .await;
        for redis in &redis {
            let _: Result<(), _> = redis
                .client
                .custom(
                    CustomCommand::new_static("SHUTDOWN", ClusterHash::Random, false),
                    vec!["NOSAVE"],
                )
                .await;
        }
        let expected = if active { 500 } else { 200 };
        if !active {
            // Give a wrong implementation time to turn unhealthy before checking it did not.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        for service in &services {
            let mut status = 0;
            for _ in 0..100 {
                status = http_request(&service.http, "GET", "/healthcheck", &[])
                    .await
                    .status;
                if status == expected {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            assert_eq!(status, expected, "{} with active={active}", service.grpc);
        }
        if !active {
            continue;
        }
        // Redis back on the same port: healthy again once the pool reconnects.
        let mut restarted = Vec::new();
        for redis in &redis {
            let port = redis.address.rsplit_once(':').unwrap().1.parse().unwrap();
            restarted.push(Redis::start_on(port).await.expect("Redis restarted"));
        }
        let mut recovered = Vec::new();
        for service in &services {
            let mut status = 0;
            for _ in 0..150 {
                status = http_request(&service.http, "GET", "/healthcheck", &[])
                    .await
                    .status;
                if status == 200 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            recovered.push(status);
        }
        assert_eq!(recovered, [200, 200]);
    }
}

/// Requests Go's HTTP server answers before routing (a bad escape, a missing or malformed
/// Host, `OPTIONS *`) and the router's redirects for absolute-form targets and non-UTF-8
/// paths are answered like upstream: the status line's code and reason, the headers but the
/// date, and the body.
#[tokio::test]
async fn the_http_server_reads_requests_like_upstream() {
    if !tools_available() {
        return;
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let runtime = temp_dir("http");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let (services, _redis) = both_services(|redis| binary_environment(&runtime, redis)).await;
    for request in [
        "GET /%zz HTTP/1.0\r\n\r\n",
        "GET /a/../%zz HTTP/1.0\r\n\r\n",
        "GET /healthcheck HTTP/1.1\r\nConnection: close\r\n\r\n",
        "GET /healthcheck HTTP/1.1\r\nHost: a\"b\r\nConnection: close\r\n\r\n",
        "GET /healthcheck HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n",
        "OPTIONS * HTTP/1.0\r\n\r\n",
        "OPTIONS * HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n",
        "GET http://h:1/a/../healthcheck?q HTTP/1.0\r\n\r\n",
        "GET http://h:1/healthcheck HTTP/1.0\r\n\r\n",
        "GET /a/../%ff HTTP/1.0\r\n\r\n",
        "HEAD /healthcheck HTTP/1.0\r\n\r\n",
        "GET /nothing HTTP/1.0\r\n\r\n",
    ] {
        let mut answers = Vec::new();
        for service in &services {
            let mut stream = tokio::net::TcpStream::connect(&service.http).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = Vec::new();
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                stream.read_to_end(&mut response),
            )
            .await
            .expect("the connection is closed")
            .unwrap();
            let response = String::from_utf8_lossy(&response).into_owned();
            let (head, body) = response.split_once("\r\n\r\n").expect("an HTTP answer");
            let mut lines = head.lines();
            // The code and reason; the version follows the request's in hyper.
            let status = lines.next().unwrap()[9..].to_string();
            // Go ends its own 400s by closing the connection; hyper sends a length too.
            let refused = status.starts_with("400");
            let mut headers: Vec<String> = lines
                .map(str::to_ascii_lowercase)
                .filter(|line| !line.starts_with("date:"))
                .filter(|line| !(refused && line.starts_with("content-length:")))
                .collect();
            headers.sort();
            answers.push((status, headers, body.to_string()));
        }
        assert_eq!(answers[1], answers[0], "{request:?}");
    }
}

/// `/json`'s decoding (`protojson.Unmarshal` into a `RateLimitRequest`) accepts and rejects
/// what upstream's does, into the same request: JSON syntax, field names, `null`, numbers,
/// enums, strings and their escapes.
#[test]
fn json_requests_are_decoded_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut inputs: Vec<Vec<u8>> = [
        // Syntax.
        "",
        " ",
        "{}",
        " {} ",
        "{}\n",
        "{} {}",
        "{},",
        "{}x",
        "[]",
        "null",
        "{",
        "}",
        "{\"domain\":\"d\",}",
        "{,}",
        "{\"domain\" \"d\"}",
        "{\"domain\":}",
        "{\"domain\":\"a\" \"descriptors\":[]}",
        "\t{\r\n\"domain\"\t:\n\"d\"\r}",
        "{\"domain\":\"d\"}\u{0}",
        "\u{feff}{}",
        "{}\u{b}",
        // Field names: either form, once; no unknown or extension names.
        "{\"hitsAddend\":1}",
        "{\"hits_addend\":1}",
        "{\"hitsAddend\":1,\"hits_addend\":2}",
        "{\"domain\":\"a\",\"domain\":\"b\"}",
        "{\"Domain\":\"a\"}",
        "{\"other\":1}",
        "{\"[x.y]\":1}",
        "{\"hitsaddend\":1}",
        "{\"dom\\u0061in\":\"a\"}",
        // null: unset, but the field counts as seen.
        "{\"domain\":null}",
        "{\"domain\":null,\"domain\":\"a\"}",
        "{\"descriptors\":null}",
        "{\"descriptors\":[null]}",
        "{\"descriptors\":[{\"limit\":null,\"hitsAddend\":null}]}",
        "{\"descriptors\":[{\"entries\":[null]}]}",
        "{\"hitsAddend\":null}",
        // Lists and messages.
        "{\"descriptors\":[]}",
        "{\"descriptors\":[[]]}",
        "{\"descriptors\":{}}",
        "{\"descriptors\":[{},{}]}",
        "{\"descriptors\":[{\"entries\":[{\"key\":\"k\",\"value\":\"v\"},{\"key\":\"k2\"}]}]}",
        "{\"descriptors\":[{\"entries\":[{\"key\":1}]}]}",
        "{\"descriptors\":[{\"entries\":[{\"key\":\"k\",\"x\":1}]}]}",
        "{\"descriptors\":[{\"limit\":{\"requestsPerUnit\":5,\"unit\":\"SECOND\"}}]}",
        "{\"descriptors\":[{\"limit\":{\"requests_per_unit\":\"5\",\"unit\":2}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":\"second\"}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":\"WEEK\"}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":99}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":-1}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":\"2\"}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":2.0}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":2147483648}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":-2147483648}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":true}}]}",
        "{\"descriptors\":[{\"limit\":[]}]}",
        "{\"descriptors\":[{\"isNegativeHits\":true}]}",
        "{\"descriptors\":[{\"is_negative_hits\":false}]}",
        "{\"descriptors\":[{\"isNegativeHits\":\"true\"}]}",
        "{\"descriptors\":[{\"isNegativeHits\":1}]}",
        "{\"descriptors\":[{\"hitsAddend\":\"18446744073709551615\"}]}",
        "{\"descriptors\":[{\"hitsAddend\":18446744073709551616}]}",
        "{\"descriptors\":[{\"hitsAddend\":0}]}",
        "{\"descriptors\":[{\"hitsAddend\":{\"value\":1}}]}",
        // Strings: escapes, surrogates, UTF-8, control characters.
        "{\"domain\":\"a\\\"b\\\\c\\/d\\b\\f\\n\\r\\t\"}",
        "{\"domain\":\"\\u00e9\\u0000\"}",
        "{\"domain\":\"\\u00E9\"}",
        "{\"domain\":\"\\ud83d\\ude00\"}",
        "{\"domain\":\"\\ud83d\"}",
        "{\"domain\":\"\\ude00\\ud83d\"}",
        "{\"domain\":\"\\ud83d\\u0041\"}",
        "{\"domain\":\"\\ud83dx\\ude00\"}",
        "{\"domain\":\"\\u12\"}",
        "{\"domain\":\"\\u12g4\"}",
        "{\"domain\":\"\\x\"}",
        "{\"domain\":\"\\'\"}",
        "{\"domain\":\"a\tb\"}",
        "{\"domain\":\"a\u{7f}b\"}",
        "{\"domain\":\"é😀\u{fffd}\"}",
        "{\"domain\":\"a",
        "{\"domain\":\"a\\",
        "{\"domain\":\"\\ud83d\\ude0",
        // Literals.
        "{\"descriptors\":[{\"isNegativeHits\":truex}]}",
        "{\"descriptors\":[{\"isNegativeHits\":tru}]}",
        "{\"domain\":nul}",
        "{\"domain\":nullx}",
        "{\"domain\":NULL}",
    ]
    .iter()
    .map(|s| s.as_bytes().to_vec())
    .collect();
    inputs.extend([
        b"{\"domain\":\"\xff\"}".to_vec(),
        b"{\"domain\":\"\xc3\"}".to_vec(),
        b"{\"domain\":\"\xed\xa0\x80\"}".to_vec(),
        b"{\"domain\":\"\xc0\xaf\"}".to_vec(),
        b"{\"\xff\":1}".to_vec(),
    ]);
    // Numbers, bare and in strings, in each kind of numeric field.
    let numbers = [
        "0",
        "-0",
        "1",
        "01",
        "-1",
        "1.0",
        "1.5",
        "1.",
        ".5",
        "1e2",
        "1E2",
        "1e+2",
        "1e-2",
        "10e-1",
        "100e-2",
        "1e",
        "1e+",
        "1e-",
        "1ex",
        "1.0e1",
        "0.5e1",
        "0.05e1",
        "5e-1",
        "0e5",
        "-0.0",
        "1e010",
        "1e08",
        "4294967295",
        "4294967296",
        "18446744073709551615",
        "18446744073709551616",
        "1e19",
        "1e20",
        "2e19",
        "1e99999999999",
        "15e-1",
        "-15e-1",
        "100e-1",
        "+1",
        "0x10",
        "1_000",
        "Infinity",
        "NaN",
        "-",
    ];
    for number in numbers {
        for template in [
            "{\"hitsAddend\":N}",
            "{\"hitsAddend\":\"N\"}",
            "{\"descriptors\":[{\"hitsAddend\":N}]}",
            "{\"descriptors\":[{\"hitsAddend\":\"N\"}]}",
            "{\"descriptors\":[{\"limit\":{\"unit\":N}}]}",
        ] {
            inputs.push(template.replace('N', number).into_bytes());
        }
    }
    // A number then a delimiter: the tokenizer takes `1e`, `1e+` as numbers.
    for tail in ["1e,", "1e]", "1e}", "1e ", "1e+,", "1e-}", "1.5e,"] {
        inputs.push(format!("{{\"hitsAddend\":{tail}\"domain\":\"d\"}}").into_bytes());
        inputs.push(
            format!("{{\"descriptors\":[{{\"hitsAddend\":{tail}\"entries\":[]}}]}}").into_bytes(),
        );
    }
    // Strings holding numbers: no space around, only the first token counts.
    for text in [
        "\\u0031",
        "\\u00312",
        "\\u0020\\u0031",
        " 1",
        "1 ",
        "1 2",
        "1,2",
        "1}",
        "1]",
        "1\u{b}2",
        "1\u{a0}",
        "\u{a0}1",
        "1\u{2028}",
        "\"1\"",
        "[1]",
        "",
        "1e,",
    ] {
        let text = text.replace('"', "\\\"");
        inputs.push(format!("{{\"hitsAddend\":\"{text}\"}}").into_bytes());
    }

    // Generated: fragments of the schema and of JSON put together at random (a fixed seed).
    let fragments = [
        "{",
        "}",
        "[",
        "]",
        ",",
        ":",
        " ",
        "\"domain\"",
        "\"descriptors\"",
        "\"entries\"",
        "\"key\"",
        "\"value\"",
        "\"limit\"",
        "\"unit\"",
        "\"requestsPerUnit\"",
        "\"hitsAddend\"",
        "\"hits_addend\"",
        "\"isNegativeHits\"",
        "\"SECOND\"",
        "\"d\"",
        "\"\\u00e9\"",
        "null",
        "true",
        "false",
        "0",
        "1",
        "-1",
        "1e2",
        "1.5",
        "\"7\"",
        "4294967296",
        "{}",
        "[]",
    ];
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = |bound: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % bound as u64) as usize
    };
    for _ in 0..3000 {
        let mut input = String::from("{\"descriptors\":[{");
        for _ in 0..1 + next(12) {
            input += fragments[next(fragments.len())];
        }
        if next(2) == 0 {
            input += "}]}";
        }
        inputs.push(input.into_bytes());
    }
    // Generated objects: each field under either name (or an unknown one), any kind of value.
    let values = [
        "null",
        "true",
        "false",
        "0",
        "-0",
        "7",
        "1e1",
        "1.0",
        "4294967295",
        "4294967296",
        "\"3\"",
        "\" 3\"",
        "\"SECOND\"",
        "\"DAY\"",
        "\"x\"",
        "\"\\ud83d\\ude00\"",
        "{}",
        "[]",
        "[{}]",
        "{\"key\":\"k\"}",
        "[{\"key\":\"k\",\"value\":\"v\"}]",
        "18446744073709551615",
    ];
    let names = [
        "domain",
        "descriptors",
        "hitsAddend",
        "hits_addend",
        "entries",
        "limit",
        "isNegativeHits",
        "is_negative_hits",
        "requestsPerUnit",
        "requests_per_unit",
        "unit",
        "key",
        "value",
        "other",
    ];
    let object = |next: &mut dyn FnMut(usize) -> usize| {
        let fields: Vec<String> = (0..next(4))
            .map(|_| {
                format!(
                    "\"{}\":{}",
                    names[next(names.len())],
                    values[next(values.len())]
                )
            })
            .collect();
        format!("{{{}}}", fields.join(","))
    };
    for _ in 0..3000 {
        let descriptors: Vec<String> = (0..next(3)).map(|_| object(&mut next)).collect();
        let limit = object(&mut next);
        let input = match next(3) {
            0 => object(&mut next),
            1 => format!("{{\"descriptors\":[{}]}}", descriptors.join(",")),
            _ => format!("{{\"domain\":\"d\",\"descriptors\":[{{\"limit\":{limit}}}]}}"),
        };
        inputs.push(input.into_bytes());
    }

    let mut oracle = std::process::Command::new(oracle_binary())
        .arg("-protojson")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the oracle");
    let lines: String = inputs
        .iter()
        .map(|input| format!("{}\n", hex(input)))
        .collect();
    // Written while the answers are read, so that neither pipe fills up.
    let mut stdin = oracle.stdin.take().unwrap();
    let writer =
        std::thread::spawn(move || std::io::Write::write_all(&mut stdin, lines.as_bytes()));
    let output = oracle.wait_with_output().unwrap();
    writer.join().unwrap().unwrap();
    let upstream: Vec<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(String::from)
        .collect();
    assert_eq!(upstream.len(), inputs.len());
    let accepted = upstream
        .iter()
        .filter(|line| line.starts_with("ok"))
        .count();
    assert!(accepted > 1000, "only {accepted} inputs accepted");
    for (input, upstream) in inputs.iter().zip(&upstream) {
        let ours = match llm_ratelimit::protojson::decode_request(input) {
            Ok(request) => format!("ok {}", hex(&prost::Message::encode_to_vec(&request))),
            Err(_) => "error".to_string(),
        };
        assert_eq!(&ours, upstream, "{:?}", String::from_utf8_lossy(input));
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `/json` answers like upstream's: statuses (200, 429, 400, 500), headers and the protojson
/// body, with metadata, response headers and every method. Go adds a space after every comma
/// or after none, depending on its build: those spaces are left out of the comparison.
#[tokio::test]
async fn json_is_served_like_upstream() {
    if !tools_available() {
        return;
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let runtime = temp_dir("json");
    let metadata = "domain: meta
descriptors:
  - key: a
    rate_limit: {unit: hour, requests_per_unit: 10, name: named}
    metadata: {s: \"q\\\"b\\\\<>&\\u2028\\x01\\t\", n: 3, f: 1.5, tiny: 0.0000001, big: 1.0e+21, neg: -0.0, l: [1, two, {x: null}], t: true}
  - key: nan
    rate_limit: {unit: hour, requests_per_unit: 10}
    metadata: {bad: .nan}
";
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v1/ratelimit/config/meta.yaml", metadata.as_bytes()),
            ("current", b"->v1"),
        ],
    );
    let (services, _redis) = both_services(|redis| {
        let mut environment = binary_environment(&runtime, redis);
        environment.push(("RESPONSE_DYNAMIC_METADATA", "true".into()));
        environment.push(("LIMIT_RESPONSE_HEADERS_ENABLED", "true".into()));
        environment
    })
    .await;
    let send = |address: String, method: &'static str, target: &'static str, body: String| async move {
        let mut stream = tokio::net::TcpStream::connect(&address).await.unwrap();
        let request = format!(
            "{method} {target} HTTP/1.0\r\nHost: h\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            stream.read_to_end(&mut response),
        )
        .await
        .expect("the connection is closed")
        .unwrap();
        String::from_utf8(response).unwrap()
    };
    // The status, the headers but the date and framing, and the body without Go's spaces.
    let simplify = |response: &str| -> (String, Vec<String>, String) {
        let (head, body) = response.split_once("\r\n\r\n").expect("an HTTP answer");
        let mut lines = head.lines();
        let status = lines.next().unwrap()[9..].to_string();
        let mut headers: Vec<String> = lines
            .map(str::to_ascii_lowercase)
            .filter(|l| !l.starts_with("date:") && !l.starts_with("content-length:"))
            .collect();
        headers.sort();
        let text = without_go_spaces(body);
        (status, headers, text)
    };
    let user = r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"a"}]}]}"#;
    let cases = [
        ("POST", "/json", user.to_string()),
        ("POST", "/json", user.to_string()),
        ("POST", "/json", user.to_string()),
        ("GET", "/json", r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"b"}]}],"hitsAddend":2}"#.into()),
        ("PUT", "/json", r#"{"domain":"meta","descriptors":[{"entries":[{"key":"a"}]}]}"#.into()),
        ("POST", "/json", r#"{"domain":"meta","descriptors":[{"entries":[{"key":"nan"}]}]}"#.into()),
        ("POST", "/json", r#"{"domain":"unknown","descriptors":[{"entries":[{"key":"k"}]}]}"#.into()),
        ("POST", "/json", r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"c"}],"limit":{"requestsPerUnit":5,"unit":"HOUR"}}]}"#.into()),
        ("POST", "/json", r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"d"}],"hitsAddend":0}]}"#.into()),
        ("POST", "/json", "{}".into()),
        ("POST", "/json", r#"{"domain":"llm"}"#.into()),
        ("POST", "/json", "not json".into()),
        ("POST", "/json", String::new()),
        ("DELETE", "/json?x=1", user.to_string()),
        (
            "HEAD",
            "/json",
            r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"h"}]}]}"#.into(),
        ),
        ("POST", "/json/", user.to_string()),
    ];
    let reset =
        regex::Regex::new(r#"("durationUntilReset":"|"key":"RateLimit-Reset","value":")(\d+)"#)
            .unwrap();
    clear_of_a_minute_boundary().await;
    for (method, target, body) in cases {
        let upstream = send(services[0].http.clone(), method, target, body.clone()).await;
        let ours = send(services[1].http.clone(), method, target, body.clone()).await;
        let (mut upstream, mut ours) = (simplify(&upstream), simplify(&ours));
        // The metadata's prototext limits: one or two spaces between fields in Go (§7).
        upstream.2 = upstream.2.replace("  unit:", " unit:");
        // Each side reads its own clock: the time to reset may differ by a second.
        let resets = |text: &str| -> Vec<i64> {
            reset
                .captures_iter(text)
                .map(|c| c[2].parse().unwrap())
                .collect()
        };
        let (expected, actual) = (resets(&upstream.2), resets(&ours.2));
        assert_eq!(expected.len(), actual.len(), "{method} {target} {body}");
        for (e, a) in expected.iter().zip(&actual) {
            assert!((e - a).abs() <= 1, "{e} {a}");
        }
        let mut times = expected.iter();
        ours.2 = reset
            .replace_all(&ours.2, |c: &regex::Captures| {
                format!("{}{}", &c[1], times.next().unwrap())
            })
            .into_owned();
        assert_eq!(ours, upstream, "{method} {target} {body}");
    }
    // A client limit with an unknown unit makes upstream panic, which closes the connection
    // with no answer; it is a bad request here. `unitMultiplier` is unknown to upstream.
    for unit in ["", r#","unit":-1"#, r#","unit":8"#] {
        let request = format!(
            r#"{{"domain":"llm","descriptors":[{{"entries":[{{"key":"user","value":"e"}}],"limit":{{"requestsPerUnit":5{unit}}}}}]}}"#
        );
        let upstream = send(services[0].http.clone(), "POST", "/json", request.clone()).await;
        assert_eq!(upstream, "", "{request}");
        let ours = send(services[1].http.clone(), "POST", "/json", request.clone()).await;
        assert_eq!(simplify(&ours).0, "400 Bad Request", "{request}");
    }
    let multiplier = r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"f"}],"limit":{"requestsPerUnit":5,"unit":"HOUR","unitMultiplier":2}}]}"#;
    let upstream = send(services[0].http.clone(), "POST", "/json", multiplier.into()).await;
    assert_eq!(simplify(&upstream).0, "400 Bad Request");
    let ours = simplify(&send(services[1].http.clone(), "POST", "/json", multiplier.into()).await);
    assert_eq!(ours.0, "200 OK");
    let no_window = multiplier.replace(r#""unitMultiplier":2"#, r#""unitMultiplier":0"#);
    let refused = simplify(&send(services[1].http.clone(), "POST", "/json", no_window).await);
    assert_eq!(refused.0, "400 Bad Request");
    assert!(
        ours.2.contains("unit:HOUR unit_multiplier:{value:2}"),
        "{}",
        ours.2
    );
    // Bodies up to gRPC's largest message (4 MiB) are read, larger ones refused.
    let request = r#"{"domain":"d","descriptors":[{"entries":[{"key":"k"}]}]}"#;
    for (size, status) in [(4 << 20, "200 OK"), ((4 << 20) + 1, "400 Bad Request")] {
        let body = format!("{request}{}", " ".repeat(size - request.len()));
        let ours = simplify(&send(services[1].http.clone(), "POST", "/json", body).await);
        assert_eq!(ours.0, status, "{size}");
    }
}

/// `/json`'s encoding writes responses like `protojson.Marshal` (Go's spaces after commas
/// left out), and refuses the same ones: every field, enum numbers with no name, Duration and
/// Timestamp fractions and ranges, base64 padding, numbers in the metadata.
#[test]
fn json_responses_are_encoded_like_upstream() {
    if !tools_available() {
        return;
    }
    let responses = sample_responses();
    let mut oracle = std::process::Command::new(oracle_binary())
        .arg("-protojson-marshal")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the oracle");
    let lines: String = responses
        .iter()
        .map(|response| format!("{}\n", hex(&prost::Message::encode_to_vec(response))))
        .collect();
    let mut stdin = oracle.stdin.take().unwrap();
    let writer =
        std::thread::spawn(move || std::io::Write::write_all(&mut stdin, lines.as_bytes()));
    let output = oracle.wait_with_output().unwrap();
    writer.join().unwrap().unwrap();
    let upstream: Vec<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(without_go_spaces)
        .collect();
    assert_eq!(upstream.len(), responses.len());
    for (response, upstream) in responses.iter().zip(&upstream) {
        let ours = match llm_ratelimit::protojson::encode_response(response) {
            Ok(json) => format!("ok {json}"),
            Err(_) => "error".to_string(),
        };
        assert_eq!(&ours, upstream, "{response:?}");
    }
}

/// The debug lines print requests, responses and statuses as Go's `%+v` does (prototext, with
/// the oracle's random second space between fields turned off): every field, defaults left out, wrappers
/// and oneofs set to their defaults, escapes, enum numbers with no name.
#[test]
fn messages_are_printed_like_upstream() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
        RateLimitDescriptor,
        rate_limit_descriptor::{Entry, RateLimitOverride},
    };
    let entry = |key: &str, value: &str| Entry {
        key: key.into(),
        value: value.into(),
    };
    let requests = vec![
        RateLimitRequest::default(),
        RateLimitRequest {
            domain: "d \"q\" \\ \n\r\t\u{0}\u{1f}\u{7f}\u{80}\u{85}\u{9f}\u{a0}é\u{2028}😀".into(),
            descriptors: vec![
                RateLimitDescriptor {
                    entries: vec![entry("k", "v"), entry("", ""), entry("only", "")],
                    limit: Some(RateLimitOverride {
                        requests_per_unit: 10,
                        unit: 2,
                        unit_multiplier: None,
                    }),
                    hits_addend: Some(5),
                    is_negative_hits: true,
                },
                RateLimitDescriptor::default(),
                RateLimitDescriptor {
                    entries: vec![entry("a", "b")],
                    limit: Some(RateLimitOverride {
                        requests_per_unit: 0,
                        unit: 9,
                        unit_multiplier: None,
                    }),
                    hits_addend: Some(0),
                    is_negative_hits: false,
                },
                RateLimitDescriptor {
                    limit: Some(RateLimitOverride::default()),
                    ..Default::default()
                },
            ],
            hits_addend: 7,
        },
    ];
    let responses = sample_responses();
    let mut lines = String::new();
    for request in &requests {
        lines += &format!("request {}\n", hex(&prost::Message::encode_to_vec(request)));
    }
    for response in &responses {
        let encoded = hex(&prost::Message::encode_to_vec(response));
        lines += &format!("response {encoded}\nstatuses {encoded}\n");
    }
    let mut oracle = std::process::Command::new(oracle_binary())
        .arg("-prototext")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the oracle");
    let mut stdin = oracle.stdin.take().unwrap();
    let writer =
        std::thread::spawn(move || std::io::Write::write_all(&mut stdin, lines.as_bytes()));
    let output = oracle.wait_with_output().unwrap();
    writer.join().unwrap().unwrap();
    let upstream: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_string)
        .collect();
    let mut ours: Vec<String> = requests
        .iter()
        .map(llm_ratelimit::prototext::request)
        .collect();
    for response in &responses {
        ours.push(llm_ratelimit::prototext::response(response));
        ours.push(llm_ratelimit::prototext::statuses(&response.statuses));
    }
    assert_eq!(upstream.len(), ours.len());
    for (ours, upstream) in ours.iter().zip(&upstream) {
        assert_eq!(ours, upstream);
    }
    // `unit_multiplier`, unknown to upstream (its field number there), is an extension here.
    let mut request = requests[1].clone();
    request.descriptors[0]
        .limit
        .as_mut()
        .unwrap()
        .unit_multiplier = Some(3);
    request.descriptors[2]
        .limit
        .as_mut()
        .unwrap()
        .unit_multiplier = Some(0);
    let text = llm_ratelimit::prototext::request(&request);
    assert!(
        text.contains("unit:MINUTE unit_multiplier:{value:3}}"),
        "{text}"
    );
    assert!(text.contains("unit:9 unit_multiplier:{}}"), "{text}");
}

/// Responses with every field, enum numbers with no name, Duration and Timestamp fractions
/// and ranges, bytes, numbers in the metadata, and what protojson refuses to write.
fn sample_responses() -> Vec<RateLimitResponse> {
    use llm_ratelimit_proto::envoy::config::core::v3::HeaderValue;
    use llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::{
        DescriptorStatus, Quota, RateLimit, quota::ExpirationSpecifier,
    };
    use prost_types::{ListValue, Struct, Timestamp, Value, value::Kind};
    let number = |n: f64| Value {
        kind: Some(Kind::NumberValue(n)),
    };
    let metadata = |values: Vec<(&str, Value)>| Struct {
        fields: values
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    };
    let with_metadata = |values: Vec<(&str, Value)>| RateLimitResponse {
        dynamic_metadata: Some(metadata(values)),
        ..Default::default()
    };
    let with_reset = |seconds: i64, nanos: i32| RateLimitResponse {
        statuses: vec![DescriptorStatus {
            duration_until_reset: Some(prost_types::Duration { seconds, nanos }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let with_time = |seconds: i64, nanos: i32| RateLimitResponse {
        quota: Some(Quota {
            expiration_specifier: Some(ExpirationSpecifier::ValidUntil(Timestamp {
                seconds,
                nanos,
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    let header = |raw: &[u8]| HeaderValue {
        key: "k".into(),
        value: String::new(),
        raw_value: raw.to_vec(),
    };
    let mut responses = vec![
        RateLimitResponse::default(),
        RateLimitResponse {
            overall_code: 1,
            statuses: vec![
                DescriptorStatus {
                    code: 2,
                    current_limit: Some(RateLimit {
                        name: "n\"\\\u{1}\u{1f}\u{7f}é\u{2028}<>&".into(),
                        requests_per_unit: 5,
                        unit: 7,
                        unit_multiplier: None,
                    }),
                    limit_remaining: 3,
                    duration_until_reset: Some(prost_types::Duration {
                        seconds: 5,
                        nanos: 0,
                    }),
                    quota: Some(Quota {
                        requests: 2,
                        expiration_specifier: None,
                        id: "q".into(),
                    }),
                },
                DescriptorStatus::default(),
                DescriptorStatus {
                    current_limit: Some(RateLimit::default()),
                    ..Default::default()
                },
            ],
            response_headers_to_add: vec![
                header(&[]),
                header(&[1]),
                header(&[1, 2]),
                header(&[0, 1, 2, 255]),
            ],
            request_headers_to_add: vec![HeaderValue {
                key: String::new(),
                value: "v".into(),
                raw_value: vec![],
            }],
            raw_body: b"body\xff".to_vec(),
            dynamic_metadata: Some(Struct::default()),
            quota: Some(Quota::default()),
        },
        // Enum numbers with no name.
        RateLimitResponse {
            overall_code: 9,
            statuses: vec![DescriptorStatus {
                code: -3,
                current_limit: Some(RateLimit {
                    unit: 42,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
        with_metadata(vec![
            (
                "null",
                Value {
                    kind: Some(Kind::NullValue(0)),
                },
            ),
            (
                "b",
                Value {
                    kind: Some(Kind::BoolValue(false)),
                },
            ),
            (
                "s",
                Value {
                    kind: Some(Kind::StringValue("x\ty".into())),
                },
            ),
            ("é", number(1.0)),
            ("B", number(-0.0)),
            (
                "",
                Value {
                    kind: Some(Kind::ListValue(ListValue { values: vec![] })),
                },
            ),
            (
                "nested",
                Value {
                    kind: Some(Kind::StructValue(metadata(vec![(
                        "l",
                        Value {
                            kind: Some(Kind::ListValue(ListValue {
                                values: vec![
                                    number(2.5),
                                    Value {
                                        kind: Some(Kind::StructValue(Struct::default())),
                                    },
                                ],
                            })),
                        },
                    )]))),
                },
            ),
        ]),
    ];
    for n in [
        1e-10,
        1.5e-100,
        1e100,
        1e21,
        1e20,
        999999999999999999999.0,
        123456789.123,
        -2.5e-7,
        1e-6,
        9.99e-7,
        5e-324,
        f64::MAX,
        f64::MIN_POSITIVE,
        0.1,
        1.0 / 3.0,
        1e15,
        1e16,
        1e-5,
        123e300,
    ] {
        responses.push(with_metadata(vec![("n", number(n))]));
    }
    for (seconds, nanos) in [
        (0, 0),
        (1, 500_000_000),
        (0, 1),
        (0, 1_000),
        (0, 1_000_000),
        (0, -1_000),
        (-1, -500_000_000),
        (315_576_000_000, 999_999_999),
        (-315_576_000_000, -999_999_999),
        (315_576_000_001, 0),
        (0, 1_000_000_000),
        (0, -1_000_000_000),
        (1, -1),
        (-1, 1),
    ] {
        responses.push(with_reset(seconds, nanos));
    }
    for (seconds, nanos) in [
        (0, 0),
        (-1, 0),
        (951_782_400, 0),
        (1_700_000_000, 120_000_000),
        (253_402_300_799, 999_999_999),
        (-62_135_596_800, 0),
        (-62_135_596_801, 0),
        (253_402_300_800, 0),
        (0, -1),
        (0, 1_000_000_000),
        (68_256_000, 7),
        (-2_208_988_800, 0),
    ] {
        responses.push(with_time(seconds, nanos));
    }
    // What protojson refuses to write.
    responses.push(with_metadata(vec![("unset", Value { kind: None })]));
    for n in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        responses.push(with_metadata(vec![("n", number(n))]));
    }

    responses
}

/// `text` without the spaces Go's protojson may put after commas, outside strings.
fn without_go_spaces(text: &str) -> String {
    let (mut out, mut in_string, mut escaped, mut after_comma) =
        (String::new(), false, false, false);
    for c in text.chars() {
        if !(after_comma && c == ' ') {
            out.push(c);
        }
        after_comma = !in_string && c == ',';
        match c {
            _ if escaped => escaped = false,
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            _ => {}
        }
    }
    out
}

/// Sends `request` as is to `address`; returns the answer once the connection closes.
async fn send_raw(address: &str, request: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        stream.read_to_end(&mut response),
    )
    .await
    .expect("the connection is closed")
    .unwrap();
    String::from_utf8_lossy(&response).into_owned()
}

/// An answer's status line (but its version), headers (but the date) and body.
fn answer_parts(response: &str) -> (String, Vec<String>, String) {
    let (head, body) = response.split_once("\r\n\r\n").expect("an HTTP answer");
    let mut lines = head.lines();
    let status = lines.next().unwrap()[9..].to_string();
    // Go ends its own 400s by closing the connection; hyper sends a length too (§7).
    let refused = status.starts_with("400");
    let mut headers: Vec<String> = lines
        .map(str::to_ascii_lowercase)
        .filter(|l| !l.starts_with("date:"))
        .filter(|l| !(refused && l.starts_with("content-length:")))
        .collect();
    headers.sort();
    (status, headers, body.to_string())
}

/// The debug port answers like upstream's: the endpoints' list for any path `/` catches,
/// Go's mux redirects, `/rlconfig` (whose order is random upstream: compared line by line),
/// `/stats`' `cmdline`, and pprof's answer for profiles it does not have.
#[tokio::test]
async fn the_debug_port_answers_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("debug");
    let dump = "domain: dump
descriptors:
  - key: a
    rate_limit: {unit: week, requests_per_unit: 3}
    descriptors:
      - key: b
        value: v
        shadow_mode: true
        rate_limit: {unit: second, requests_per_unit: 1}
      - key: c
        rate_limit: {unlimited: true, requests_per_unit: 7}
  - key: w
    value: pre*
    rate_limit: {unit: day, requests_per_unit: 9}
  - key: q
    quota_mode: true
    rate_limit: {unit: month, requests_per_unit: 2}
  - key: nolimit
    descriptors:
      - key: deep
        rate_limit: {unit: year, requests_per_unit: 4}
";
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v1/ratelimit/config/dump.yaml", dump.as_bytes()),
            ("current", b"->v1"),
        ],
    );
    let (services, _redis) = both_services(|redis| binary_environment(&runtime, redis)).await;
    for (method, target) in [
        ("GET", "/"),
        ("HEAD", "/"),
        ("POST", "/"),
        ("GET", "/foo"),
        ("GET", "/stats/"),
        ("GET", "/debug"),
        ("GET", "/debug/pprof"),
        ("GET", "/debug/pprof?x=1"),
        ("HEAD", "/debug/pprof"),
        ("POST", "/debug/pprof"),
        ("GET", "/debug/ppro%66"),
        ("GET", "/a/../rlconfig"),
        ("GET", "/a/../%2Fx"),
        ("GET", "/a/../b!"),
        ("GET", "//stats"),
        ("GET", "/x/../debug/pprof"),
        ("GET", "/debug/pprof/cmdline"),
        ("GET", "/debug/pprof/symbol"),
        ("GET", "/debug/pprof/profile/"),
        ("GET", "/debug/pprof/x/y"),
        ("GET", "/debug/pprof/trace/"),
        ("GET", "/%zz"),
        ("GET", "/a|b"),
        ("GET", "/x/../a|b"),
    ] {
        let request = format!("{method} {target} HTTP/1.0\r\nHost: h\r\n\r\n");
        let upstream = answer_parts(&send_raw(&services[0].debug, &request).await);
        let ours = answer_parts(&send_raw(&services[1].debug, &request).await);
        assert_eq!(ours, upstream, "{method} {target}");
    }
    for request in [
        "GET * HTTP/1.0\r\n\r\n",
        "HEAD * HTTP/1.1\r\nHost: h\r\n\r\n",
        "GET http://h/a/../stats HTTP/1.0\r\n\r\n",
        "CONNECT h:1 HTTP/1.0\r\n\r\n",
        "CONNECT /debug/pprof HTTP/1.0\r\n\r\n",
        "GET /a/../x?q=\u{e9} HTTP/1.0\r\n\r\n",
        "GET /debug/pprof?\u{e9} HTTP/1.0\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\nConnection: close\r\n\r\n",
    ] {
        let upstream = answer_parts(&send_raw(&services[0].debug, request).await);
        let ours = answer_parts(&send_raw(&services[1].debug, request).await);
        assert_eq!(ours, upstream, "{request:?}");
    }
    // The config, in any order.
    for target in ["/rlconfig", "/rlconfi%67"] {
        let request = format!("GET {target} HTTP/1.0\r\nHost: h\r\n\r\n");
        let lines = |response: &str| {
            let (status, headers, body) = answer_parts(response);
            let mut lines: Vec<String> = body.lines().map(String::from).collect();
            lines.sort();
            (status, headers, lines, body.ends_with('\n'))
        };
        let upstream = lines(&send_raw(&services[0].debug, &request).await);
        let ours = lines(&send_raw(&services[1].debug, &request).await);
        assert_eq!(ours, upstream, "{target}");
        assert!(ours.2.len() > 5, "{ours:?}");
    }
    // `/stats`: the command line as JSON, then upstream's `memstats` only.
    let request = "GET /stats HTTP/1.0\r\nHost: h\r\n\r\n";
    let (upstream_status, upstream_headers, upstream_body) =
        answer_parts(&send_raw(&services[0].debug, request).await);
    let (status, headers, body) = answer_parts(&send_raw(&services[1].debug, request).await);
    assert_eq!(
        (
            &status,
            &headers
                .iter()
                .filter(|h| !h.starts_with("content-length"))
                .collect::<Vec<_>>()
        ),
        (
            &upstream_status,
            &upstream_headers
                .iter()
                .filter(|h| !h.starts_with("content-length"))
                .collect::<Vec<_>>()
        )
    );
    let upstream_lines: Vec<&str> = upstream_body.lines().collect();
    assert_eq!(upstream_lines.len(), 2);
    assert!(
        upstream_lines[1].starts_with("memstats: {"),
        "{}",
        upstream_lines[1]
    );
    let binary = |path: &std::path::Path| {
        format!(
            "cmdline: [{}]",
            serde_json::to_string(&path.display().to_string()).unwrap()
        )
    };
    assert_eq!(upstream_lines[0], binary(&upstream_binary()));
    assert_eq!(
        body,
        format!(
            "{}\n",
            binary(std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")))
        )
    );
    // pprof cannot profile here: its own answer when profiling does not start.
    for (target, text) in [
        (
            "/debug/pprof/profile",
            "Could not enable CPU profiling: not available",
        ),
        (
            "/debug/pprof/trace",
            "Could not enable tracing: not available",
        ),
    ] {
        let request = format!("GET {target} HTTP/1.0\r\nHost: h\r\n\r\n");
        let (status, _, body) = answer_parts(&send_raw(&services[1].debug, &request).await);
        assert_eq!(
            (status.as_str(), body.as_str()),
            ("500 Internal Server Error", format!("{text}\n").as_str())
        );
    }
}

/// The content type Go's server sniffs for a body (`http.DetectContentType`), and Go's JSON
/// strings (`json.Marshal`, as `/stats` writes the command line), for bytes of any kind.
#[test]
fn bodies_are_sniffed_and_strings_quoted_like_go() {
    if !tools_available() {
        return;
    }
    let mut inputs: Vec<Vec<u8>> = [
        &b""[..],
        b"text",
        b"  \t\n<html>",
        b"<HTML ",
        b"<html",
        b"<htmlx",
        b"<!DOCTYPE html>",
        b"<!doctype HTML ",
        b"<head>",
        b"<script ",
        b"<iframe>",
        b"<h1>",
        b"<div>",
        b"<font>",
        b"<table>",
        b"<a>",
        b"<a",
        b"<style>",
        b"<title>",
        b"<b>",
        b"<body>",
        b"<br>",
        b"<p>",
        b"<!-->",
        b"<!--x",
        b" <?xml",
        b"<?XML",
        b"%PDF-1",
        b"%!PS-Adobe-3",
        b"\xFE\xFF\x00\x00",
        b"\xFF\xFE\x00\x01",
        b"\xEF\xBB\xBFx",
        b"\xEF\xBB\xBF",
        b"\x00\x00\x01\x00",
        b"\x00\x00\x02\x00",
        b"BMx",
        b"GIF87a",
        b"GIF89a!",
        b"RIFF1234WEBPVP8",
        b"\x89PNG\x0D\x0A\x1A\x0A",
        b"\xFF\xD8\xFF\xE0",
        b"FORM1234AIFF",
        b"ID3x",
        b"OggS\x00",
        b"MThd\x00\x00\x00\x06",
        b"RIFF1234AVI ",
        b"RIFF1234WAVE",
        b"\x1A\x45\xDF\xA3",
        b"\x00\x01\x00\x00",
        b"OTTO",
        b"ttcf",
        b"wOFF",
        b"wOF2",
        b"\x1F\x8B\x08",
        b"PK\x03\x04",
        b"Rar!\x1A\x07\x00",
        b"Rar!\x1A\x07\x01\x00",
        b"\x00asm",
        b"a\x00b",
        b"a\x08",
        b"a\x0b",
        b"a\x0c",
        b"a\x1b",
        b"a\x1c",
        b"a\x7f",
        b"\xff\xfe",
        b"\x80",
        b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp41isom",
        b"\x00\x00\x00\x10ftypisom\x00\x00\x00\x00",
        b"\x00\x00\x00\x14ftypxxxxmp4\x00\x00\x00\x00\x00",
        // JSON strings.
        b"\"\\/",
        b"\x01\x1f\x08\x0c\n\r\t",
        b"<>&",
        "\u{2028}\u{2029}é😀".as_bytes(),
        b"\xe2\x82",
        b"\xe2\x82A",
        b"\xed\xa0\x80",
        b"\xc0\xaf",
        b"\xf4\x90\x80\x80",
        b"a\xffb\xfe",
        "\u{fffd}".as_bytes(),
    ]
    .iter()
    .map(|b| b.to_vec())
    .collect();
    let mut font = vec![0u8; 34];
    font.extend_from_slice(b"LP");
    inputs.push(font);
    let mut webm_font = b"\x1A\x45\xDF\xA3".to_vec();
    webm_font.resize(34, 0);
    webm_font.extend_from_slice(b"LP");
    inputs.push(webm_font);
    let mut long = vec![b'a'; 512];
    long.push(0);
    inputs.push(long);
    let mut late_html = vec![b' '; 600];
    late_html.extend_from_slice(b"<html>");
    inputs.push(late_html);

    let mut oracle = std::process::Command::new(oracle_binary())
        .arg("-sniff")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the oracle");
    let lines: String = inputs
        .iter()
        .map(|input| format!("{}\n", hex(input)))
        .collect();
    let mut stdin = oracle.stdin.take().unwrap();
    let writer =
        std::thread::spawn(move || std::io::Write::write_all(&mut stdin, lines.as_bytes()));
    let output = oracle.wait_with_output().unwrap();
    writer.join().unwrap().unwrap();
    let upstream: Vec<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(String::from)
        .collect();
    assert_eq!(upstream.len(), inputs.len());
    for (input, upstream) in inputs.iter().zip(&upstream) {
        let ours = format!(
            "{}\t{}",
            llm_ratelimit::goserver::detect_content_type(input),
            llm_ratelimit::go::json_string(input)
        );
        assert_eq!(&ours, upstream, "{input:?}");
    }
}

/// A debug port that cannot be listened on is not fatal: the service serves anyway.
#[tokio::test]
async fn a_busy_debug_port_is_ignored_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("busy-debug");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    // Taken without SO_REUSEPORT, so that neither service can share it.
    let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = busy.local_addr().unwrap().port().to_string();
    let (services, _redis) = both_services(|redis| {
        let mut environment = binary_environment(&runtime, redis);
        environment.push(("DEBUG_PORT", port.clone()));
        environment
    })
    .await;
    for service in &services {
        let status = http_request(&service.http, "GET", "/healthcheck", &[])
            .await
            .status;
        assert_eq!(status, 200, "{}", service.grpc);
    }
}

/// The calls whose server stats are compared: `ShouldRateLimit`, health `Check` (one for a
/// service it does not know) and `List`, which upstream's interceptor counts, and a `Watch`,
/// which it does not (streams are not intercepted). The rate limit's hits start from an empty
/// `redis`, in one minute (`LLM_CONFIG`'s window), so that each run counts the same.
async fn server_calls(service: &Service, redis: &Redis) {
    use llm_ratelimit_proto::grpc::health::v1::{HealthCheckRequest, HealthListRequest};
    resp_command(&redis.address, &["FLUSHALL"]).await;
    clear_of_a_minute_boundary().await;
    let mut client = service.client().await;
    for _ in 0..3 {
        client
            .should_rate_limit(request("llm", &[&[("user", "a")]]))
            .await
            .unwrap();
    }
    let mut health = HealthClient::connect(service.grpc.clone()).await.unwrap();
    for name in ["ratelimit", "unknown"] {
        let _ = health
            .check(HealthCheckRequest {
                service: name.into(),
            })
            .await;
    }
    health.list(HealthListRequest {}).await.unwrap();
    let mut watch = health
        .watch(HealthCheckRequest {
            service: "ratelimit".into(),
        })
        .await
        .unwrap()
        .into_inner();
    watch.message().await.unwrap();
}

/// Statsd lines summed by name: counters by value, timers by count once their value (a few
/// milliseconds, read from each side's clock) is checked to be small.
fn server_stat_lines(lines: &[String]) -> BTreeMap<String, u64> {
    let mut sums = BTreeMap::new();
    for line in lines {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        if !name.starts_with("ratelimit_server.") && name != "reserved_tag" {
            continue;
        }
        let (value, kind) = rest.split_once('|').unwrap();
        let kind = kind.split('|').next().unwrap();
        let value: f64 = value.parse().unwrap();
        let (key, add) = match kind {
            "ms" => {
                assert!((0.0..1000.0).contains(&value), "{line}");
                assert_eq!(value.trunc(), value, "{line}");
                (format!("{name}|ms {}", rest.split_once('|').unwrap().1), 1)
            }
            _ => (
                format!("{name}|{}", rest.split_once('|').unwrap().1),
                value as u64,
            ),
        };
        *sums.entry(key).or_default() += add;
    }
    sums
}

/// The gRPC server's stats (`ratelimit_server.<method>.total_requests` and `response_time`)
/// reach statsd, DogStatsD and Prometheus like upstream's, with `EXTRA_TAGS`.
#[tokio::test]
async fn server_stats_are_reported_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("server-stats");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let stats_environment = |redis: &Redis, sink: Vec<(&'static str, String)>| {
        let mut environment = binary_environment(&runtime, redis);
        environment.extend([
            ("DISABLE_STATS", "false".to_string()),
            ("STATS_FLUSH_INTERVAL", "100ms".into()),
        ]);
        environment.extend(sink);
        environment
    };
    let tagged = ("EXTRA_TAGS", "host:h,k:v".to_string());

    // statsd over TCP.
    let servers = [StatsdServer::start().await, StatsdServer::start().await];
    let mut sums = Vec::new();
    let redis = [Redis::start().await, Redis::start().await];
    for ((binary, redis), server) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ]
    .iter()
    .zip(&redis)
    .zip(&servers)
    {
        let sink = vec![
            ("STATSD_HOST", "127.0.0.1".to_string()),
            ("STATSD_PORT", server.address.port().to_string()),
            tagged.clone(),
        ];
        let service = Service::start(binary, &stats_environment(redis, sink)).await;
        server_calls(&service, redis).await;
        sums.push(wait_for_server_stats(|| server.lines.lock().unwrap().clone()).await);
    }
    assert!(
        sums[0]
            .keys()
            .any(|k| k.contains("ShouldRateLimit.response_time")),
        "{:?}",
        sums[0]
    );
    assert_eq!(sums[1], sums[0], "statsd");

    // DogStatsD.
    let servers = [
        DogStatsdServer::start().await,
        DogStatsdServer::start().await,
    ];
    let mut sums = Vec::new();
    for ((binary, redis), server) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ]
    .iter()
    .zip(&redis)
    .zip(&servers)
    {
        let sink = vec![
            ("USE_STATSD", "false".to_string()),
            ("USE_DOG_STATSD", "true".into()),
            ("STATSD_HOST", "127.0.0.1".into()),
            ("STATSD_PORT", server.address.port().to_string()),
            tagged.clone(),
        ];
        let service = Service::start(binary, &stats_environment(redis, sink)).await;
        server_calls(&service, redis).await;
        let lines = || {
            let datagrams = server.datagrams.lock().unwrap();
            datagrams
                .iter()
                .flat_map(|d| {
                    String::from_utf8_lossy(d)
                        .lines()
                        .map(String::from)
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        sums.push(wait_for_server_stats(lines).await);
    }
    assert!(
        sums[0]
            .keys()
            .any(|k| k.contains("ShouldRateLimit.response_time")),
        "{:?}",
        sums[0]
    );
    assert_eq!(sums[1], sums[0], "DogStatsD");

    // Prometheus with the default mapper: a counter and a histogram per method; with
    // `EXTRA_TAGS` the names are not mapped, and the timers become summaries. Bucket and
    // quantile values depend on each side's timing and are left out.
    for tags in [None, Some(tagged.clone())] {
        let mut series = Vec::new();
        for (binary, redis) in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ]
        .iter()
        .zip(&redis)
        {
            let address = format!("127.0.0.1:{}", reserved_port());
            let mut sink = vec![
                ("USE_STATSD", "false".to_string()),
                ("USE_PROMETHEUS", "true".into()),
                ("PROMETHEUS_ADDR", address.clone()),
            ];
            sink.extend(tags.clone());
            let service = Service::start(binary, &stats_environment(redis, sink)).await;
            server_calls(&service, redis).await;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut previous: Option<Vec<String>> = None;
            let lines = loop {
                let scraped = http_request(&address, "GET", "/metrics", &[]).await;
                let lines: Vec<String> = scraped
                    .body
                    .lines()
                    .filter(|l| {
                        l.starts_with("# TYPE ratelimit_s")
                            || (l.starts_with("ratelimit_s")
                                && (l.contains("total_requests") || l.contains("_count")))
                    })
                    .map(String::from)
                    .collect();
                // 6 calls counted, and 6 timed; then the same over three flushes (100 ms
                // each), so that counters flushed later (the rate limit's, the config's) are in.
                let total: f64 = lines
                    .iter()
                    .filter(|l| !l.starts_with('#'))
                    .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
                    .sum();
                if std::time::Instant::now() > deadline {
                    break lines;
                }
                if total >= 12.0 {
                    if previous.as_ref() == Some(&lines) {
                        break lines;
                    }
                    previous = Some(lines);
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    continue;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            };
            series.push(lines);
        }
        assert!(
            series[0].iter().any(|l| l.contains("_count")),
            "{:?}",
            series[0]
        );
        assert_eq!(series[1], series[0], "Prometheus with {tags:?}");
    }
}

/// The server's stats from `lines` once every call of `server_calls` is in them (3
/// `ShouldRateLimit`, 2 `Check`, 1 `List`, each counted and timed) and the sums stay the same
/// over 300 ms, or after 10 s.
async fn wait_for_server_stats(lines: impl Fn() -> Vec<String>) -> BTreeMap<String, u64> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut previous = None;
    loop {
        let sums = server_stat_lines(&lines());
        let calls: u64 = sums
            .iter()
            .filter(|(name, _)| name.starts_with("ratelimit_server."))
            .map(|(_, n)| n)
            .sum();
        if std::time::Instant::now() > deadline {
            return sums;
        }
        // Then the same over three flushes (100 ms each): other counters can come later.
        if calls >= 12 {
            if previous.as_ref() == Some(&sums) {
                return sums;
            }
            previous = Some(sums);
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            continue;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Timers mapped to summaries (the default observer type) reach Prometheus like upstream's:
/// statsd_exporter's quantiles, the defaults' or the mapping's (a quantile given twice keeps
/// its last error), the mapping's options in full, the streams' estimates over many
/// observations and buffer flushes, and the names a summary takes from other metrics.
#[tokio::test]
async fn summaries_reach_prometheus_like_upstream() {
    if !tools_available() {
        return;
    }
    let dense: Vec<String> = (1..20)
        .map(|i| format!("{{quantile: {}, error: 0.005}}", i as f64 / 20.0))
        .chain(["{quantile: 0.99, error: 0.0005}".to_string()])
        .collect();
    let mapper = format!(
        "
defaults:
  summary_options:
    quantiles: [{{quantile: 0.3, error: 0.03}}, {{quantile: 0.7, error: 0.01}}]
mappings:
- match: labelled.*
  name: labelled
  labels: {{kind: $1}}
- match: custom.*
  name: custom
  summary_options:
    quantiles: [{{quantile: 0.25, error: 0.001}}, {{quantile: 0.75, error: 0.02}}, {{quantile: 0.25, error: 0.2}}]
    buf_cap: 3
- match: empty.*
  name: empty
  summary_options: {{quantiles: [], age_buckets: 2}}
- match: legacy.*
  name: legacy
  summary_options: {{quantiles: [{{quantile: 0.1, error: 0.01}}, {{quantile: 0.999, error: 0.0001}}]}}
- match: dense.*
  name: dense
  summary_options: {{quantiles: [{}]}}
- match: hist.*
  name: plain_timer_sum
  observer_type: histogram
- match: na.*
  name: nm
  labels: {{lv: $1}}
- match: nb.*
  name: nm
  labels: {{lv: $1}}
  summary_options: {{max_age: -1s}}
- match: helpa.*
  name: helped
  help: a
- match: helpb.*
  name: helped
  help: b
  labels: {{lv: $1}}
",
        dense.join(", ")
    );
    let settings = Settings {
        prometheus: Some(Prometheus::new(Some(&mapper)).await),
        ..Default::default()
    };
    let h = Harness::configured(TOKENS, settings).await;
    // Each side's age buckets turn every 2 minutes from its series' creation: the test runs
    // well within one, so that both sides' streams hold the same observations.
    // Values in a shuffled order, with repeats, negatives and large ones.
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let mut values: Vec<f64> = (0..4000)
        .map(|_| (next() % 100_000) as f64 - 1000.0)
        .collect();
    values.extend([0.0, -0.5, 1e9, 3.25, 3.25, 3.25]);
    let batch = |name: &'static str, values: &[f64]| -> Vec<(&'static str, &'static str, f64)> {
        values.iter().map(|v| ("timer", name, *v)).collect()
    };
    // A counter named like a summary's `_sum` makes the summary a conflict.
    h.flush_to_prometheus(&[("counter", "pre_sum", 1.0), ("timer", "pre", 1.0)])
        .await;
    h.flush_to_prometheus(&batch("plain_timer", &values[..10]))
        .await;
    for name in [
        "labelled.a",
        "labelled.b",
        "custom.x",
        "empty.x",
        "legacy.x",
        "dense.x",
    ] {
        h.flush_to_prometheus(&batch(name, &values[..7])).await;
    }
    // Scraped between batches, so that each Write flushes the buffers in between.
    let mut scraped = None;
    for chunk in values.chunks(800) {
        let mut stats = batch("plain_timer", chunk);
        for name in ["custom.x", "dense.x", "labelled.a", "empty.x"] {
            stats.extend(batch(name, chunk));
        }
        scraped = Some(h.flush_to_prometheus(&stats).await);
    }
    let body = scraped.unwrap().body;
    for series in [
        "dense{quantile=\"0.95\"}",
        "custom{quantile=\"0.25\"}",
        "empty{quantile=\"0.7\"}",
        "legacy{quantile=\"0.999\"}",
        "labelled{kind=\"a\",quantile=\"0.3\"}",
        "plain_timer_count 4016",
    ] {
        assert!(body.contains(series), "{series} in {body}");
    }
    // Zeros of either sign are `0`; a mapping with a negative `max_age` reuses the vector
    // another made (only a new vector would panic upstream).
    let scraped = h
        .flush_to_prometheus(&[
            ("timer", "zero.x", -0.0),
            ("timer", "na.x", 1.0),
            ("timer", "nb.y", 2.0),
        ])
        .await;
    assert!(
        scraped.body.contains("zero_x{quantile=\"0.3\"} 0\n"),
        "{}",
        scraped.body
    );
    assert!(
        scraped.body.contains("nm{lv=\"y\",quantile=\"0.3\"} 2\n"),
        "{}",
        scraped.body
    );
    // A summary's `_count` taken first, a counter's name taken by a summary.
    h.flush_to_prometheus(&[
        ("counter", "taken", 1.0),
        ("timer", "taken", 1.0),
        ("counter", "late_count", 1.0),
        ("timer", "late", 1.0),
    ])
    .await;
    // The same name with other help (and labels): Gather refuses the second summary.
    h.flush_to_prometheus(&[("timer", "helpa.x", 1.0), ("timer", "helpb.y", 2.0)])
        .await;
    // A gauge named like a summary's `_sum` is refused; a histogram is registered, but
    // Gather refuses it.
    h.flush_to_prometheus(&[("gauge", "plain_timer_sum", 1.0)])
        .await;
    h.flush_to_prometheus(&[("timer", "hist.x", 1.0)]).await;
}

/// The latest value of each gauge and the sum of each counter in statsd `lines`, for the
/// names `keep` selects.
fn stat_values(lines: &[String], keep: impl Fn(&str) -> bool) -> BTreeMap<String, u64> {
    let mut values = BTreeMap::new();
    for line in lines {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        if !keep(name) {
            continue;
        }
        let (value, kind) = rest.split_once('|').unwrap();
        let value: u64 = value.parse().unwrap();
        match kind {
            "g" => {
                values.insert(format!("{name}|g"), value);
            }
            _ => *values.entry(format!("{name}|{kind}")).or_default() += value,
        }
    }
    values
}

/// The gauges and pool stats upstream flushes like upstream: `runtime.num_values`, the local
/// cache's counts (`localcache.*`, but its average access time, read from each side's clock),
/// and the Redis pool's connections at startup and after Redis comes back.
#[tokio::test]
async fn gauges_are_reported_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("gauges");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            // Two files with the same key count once; dotfiles are ignored.
            ("v1/ratelimit/other/b", b"1"),
            ("v1/ratelimit/other.b", b"2"),
            ("v1/ratelimit/other/c", b"3"),
            ("v1/ratelimit/.hidden", b"4"),
            ("current", b"->v1"),
        ],
    );
    let servers = [StatsdServer::start().await, StatsdServer::start().await];
    let redis = [Redis::start().await, Redis::start().await];
    let mut services = Vec::new();
    for ((binary, redis), server) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ]
    .iter()
    .zip(&redis)
    .zip(&servers)
    {
        let mut environment = binary_environment(&runtime, redis);
        environment.extend([
            ("DISABLE_STATS", "false".to_string()),
            ("STATS_FLUSH_INTERVAL", "100ms".into()),
            ("STATSD_HOST", "127.0.0.1".into()),
            ("STATSD_PORT", server.address.port().to_string()),
            ("RUNTIME_IGNOREDOTFILES", "true".into()),
            ("REDIS_POOL_SIZE", "2".into()),
            ("LOCAL_CACHE_SIZE_IN_BYTES", "1048576".into()),
        ]);
        services.push(Service::start(binary, &environment).await);
    }
    // Over the limit (2 a minute): the key goes to the local cache, then hits it. Each call
    // goes to both sides in turn, so that a minute cannot end between them.
    let mut clients = [services[0].client().await, services[1].client().await];
    for user in ["a", "a", "a", "a", "a", "b"] {
        for client in &mut clients {
            client
                .should_rate_limit(request("llm", &[&[("user", user)]]))
                .await
                .unwrap();
        }
    }
    let values = settled_gauges(&servers, |v| {
        v.get("ratelimit.localcache.hitCount|g") == Some(&2)
    })
    .await;
    assert_eq!(values[1], values[0]);
    let hits = values[0].get("ratelimit.localcache.hitCount|g");
    // The third call over the limit fills the cache, the next two hit it.
    assert_eq!(hits, Some(&2), "{:?}", values[0]);
    assert_eq!(
        values[0].get("runtime.num_values|g"),
        Some(&3),
        "{:?}",
        values[0]
    );
    assert_eq!(
        values[0].get("ratelimit.redis_pool.cx_active|g"),
        Some(&2),
        "{:?}",
        values[0]
    );

    // Redis back on the same port: our pool replaces its two connections at once. Radix only
    // finds a dead connection when it uses it (docs/parity.md §7), so upstream's count is not
    // compared here.
    for redis in &redis {
        let _: Result<(), _> = redis
            .client
            .custom(
                CustomCommand::new_static("SHUTDOWN", ClusterHash::Random, false),
                vec!["NOSAVE"],
            )
            .await;
    }
    let mut restarted = Vec::new();
    for redis in &redis {
        let port = redis.address.rsplit_once(':').unwrap().1.parse().unwrap();
        restarted.push(Redis::start_on(port).await.expect("Redis restarted"));
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let expected = [
        ("ratelimit.redis_pool.cx_active|g", 2),
        ("ratelimit.redis_pool.cx_local_close|c", 2),
        ("ratelimit.redis_pool.cx_total|c", 4),
    ];
    let ours = loop {
        let values = stat_values(&servers[1].lines.lock().unwrap(), |n| {
            n.starts_with("ratelimit.redis_pool.")
        });
        let pool: Vec<(&str, u64)> = expected
            .iter()
            .map(|(k, _)| (*k, values.get(*k).copied().unwrap_or(0)))
            .collect();
        if pool == expected || std::time::Instant::now() > deadline {
            break pool;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert_eq!(ours, expected);
}

/// The pool, local cache and runtime stats of both statsd servers once they are the same and
/// `expected` holds, or after 15 s; the local cache's average access time, read from each
/// side's clock, as 0.
async fn settled_gauges(
    servers: &[StatsdServer; 2],
    expected: impl Fn(&BTreeMap<String, u64>) -> bool,
) -> Vec<BTreeMap<String, u64>> {
    let keep = |name: &str| {
        name.starts_with("ratelimit.redis_pool.")
            || name.starts_with("ratelimit.localcache.")
            || name == "runtime.num_values"
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let values: Vec<BTreeMap<String, u64>> = servers
            .iter()
            .map(|s| {
                let mut v = stat_values(&s.lines.lock().unwrap(), keep);
                if let Some(time) = v.get_mut("ratelimit.localcache.averageAccessTime|g") {
                    *time = 0;
                }
                v
            })
            .collect();
        if (values[0] == values[1] && expected(&values[0])) || std::time::Instant::now() > deadline
        {
            return values;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Before Redis answers, the pool's and the local cache's gauges are already flushed, at 0:
/// upstream makes them before its startup loop.
#[tokio::test]
async fn gauges_are_flushed_while_redis_is_unreachable_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("gauges-no-redis");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let unreachable = format!("127.0.0.1:{}", reserved_port());
    let servers = [StatsdServer::start().await, StatsdServer::start().await];
    let mut services = Vec::new();
    for (binary, server) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ]
    .iter()
    .zip(&servers)
    {
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", unreachable.clone()),
            ("LOG_LEVEL", "error".into()),
            ("STATS_FLUSH_INTERVAL", "100ms".into()),
            ("STATSD_HOST", "127.0.0.1".into()),
            ("STATSD_PORT", server.address.port().to_string()),
            ("LOCAL_CACHE_SIZE_IN_BYTES", "1048576".into()),
        ];
        services.push(Service::start(binary, &environment).await);
    }
    // The runtime loader, started before Redis, has its one file.
    let values = settled_gauges(&servers, |v| v.len() == 10).await;
    assert_eq!(values[1], values[0]);
    assert_eq!(values[0].len(), 10, "{:?}", values[0]);
    let zeros = values[0]
        .iter()
        .filter(|(k, _)| *k != "runtime.num_values|g");
    assert!(zeros.clone().all(|(_, v)| *v == 0), "{:?}", values[0]);
}

/// Runs `binary` with `environment` until it serves gRPC, sends it one request, stops it
/// with SIGTERM, and returns what it wrote to stderr.
async fn logged_run(
    binary: &std::path::Path,
    environment: &[(&str, String)],
    grpc: &str,
) -> String {
    let child = Command::new(binary)
        .env_clear()
        .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start the service");
    let mut client = None;
    for _ in 0..300 {
        if let Ok(c) = RateLimitServiceClient::connect(grpc.to_string()).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let Some(mut client) = client else {
        // Its stderr says why.
        let mut child = child;
        let _ = child.start_kill();
        let output = child.wait_with_output().await.unwrap();
        panic!(
            "the service does not serve:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    if let Err(e) = client
        .should_rate_limit(request("llm", &[&[("user", "a")]]))
        .await
    {
        // Its stderr says why.
        let mut child = child;
        let _ = child.start_kill();
        let output = child.wait_with_output().await.unwrap();
        panic!(
            "{e:?}; the service wrote:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    drop(client);
    let pid = child.id().unwrap().to_string();
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let output = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("the service stops")
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The logrus lines of `stderr` (gostats' own JSON lines left out), sorted, with the time and
/// what differs between the two runs (`replace`) masked.
fn logrus_lines(stderr: &str, replace: &[(String, &str)]) -> Vec<String> {
    let time = regex::Regex::new(r#"(time="|"@timestamp":")[^"]+""#).unwrap();
    let mut lines: Vec<String> = stderr
        .lines()
        .filter(|l| !l.contains("gostats.loggingsink"))
        .map(|l| {
            let mut l = time.replace(l, "${1}TIME\"").into_owned();
            for (from, to) in replace {
                l = l.replace(from.as_str(), to);
            }
            l
        })
        .collect();
    lines.sort();
    lines
}

/// Upstream's log lines, in its text and JSON formats, at the info level: the sink, tracing,
/// the runtime directory's unreadable entries, Redis, the config, the listeners, and the
/// shutdown on SIGTERM; and the time's shape in the local time zone.
#[tokio::test]
async fn logs_are_written_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("logs");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v1/ratelimit/dangling", b"->missing"),
            ("v1/ratelimit/linked", b"->config"),
            ("current", b"->v1"),
        ],
    );
    let time_shape = |stderr: &str, key: &str| -> Vec<String> {
        let digits = regex::Regex::new(r"\d").unwrap();
        let time = regex::Regex::new(&format!(r#"{key}"([^"]+)""#)).unwrap();
        // The fraction's trailing zeros are trimmed: its length varies.
        let fraction = regex::Regex::new(r"\.N+").unwrap();
        let mut shapes: Vec<String> = time
            .captures_iter(stderr)
            .map(|c| digits.replace_all(&c[1], "N").into_owned())
            .map(|shape| fraction.replace(&shape, ".F").into_owned())
            .collect();
        shapes.sort();
        shapes.dedup();
        shapes
    };
    for (format, tz, time_key, offset) in [
        ("text", None, "time=", None),
        (
            "json",
            Some("Asia/Kolkata"),
            "\"@timestamp\":",
            Some("+05:30\""),
        ),
        ("text", Some(""), "time=", Some("Z\"")),
    ] {
        let redis = [Redis::start().await, Redis::start().await];
        let mut runs = Vec::new();
        for (binary, redis) in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ]
        .iter()
        .zip(&redis)
        {
            let (grpc, http, debug) = (reserved_port(), reserved_port(), reserved_port());
            let mut environment = binary_environment(&runtime, redis);
            environment.retain(|(k, _)| *k != "LOG_LEVEL");
            environment.extend([
                ("LOG_LEVEL", "info".to_string()),
                ("LOG_FORMAT", format.into()),
                ("GRPC_HOST", "127.0.0.1".into()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", http.to_string()),
                // The wildcard: Go names the dual-stack socket `[::]` once it closes.
                ("DEBUG_HOST", "0.0.0.0".into()),
                ("DEBUG_PORT", debug.to_string()),
            ]);
            if let Some(tz) = tz {
                environment.push(("TZ", tz.into()));
            }
            let stderr =
                logged_run(binary, &environment, &format!("http://127.0.0.1:{grpc}")).await;
            if let Some(offset) = offset {
                assert!(stderr.contains(offset), "{offset} in {stderr}");
            }
            let replace = [
                (runtime.display().to_string(), "RUNTIME"),
                (redis.address.clone(), "REDIS"),
                (grpc.to_string(), "GRPC"),
                (http.to_string(), "HTTP"),
                (debug.to_string(), "DEBUG"),
            ];
            runs.push((
                logrus_lines(&stderr, &replace),
                time_shape(&stderr, time_key),
            ));
        }
        assert!(runs[0].0.len() > 5, "{:?}", runs[0].0);
        assert_eq!(runs[1].0, runs[0].0, "{format} {tz:?}");
        assert_eq!(runs[1].1, runs[0].1, "{format} {tz:?}");
    }
}

/// With `LOG_LEVEL=debug`, upstream's debug lines: the configs loaded (a domain patched),
/// each request's lookup (exact, default, wildcard with share_threshold, detailed, value_to_metric,
/// unlimited, replaced, shadow, override, unknown domain, no match, too deep), the cache and the
/// local cache, statuses and responses as `%+v`, errors, `/json`. Times, cache key windows and
/// resets are masked; lines upstream writes from other goroutines, in a racy order, are compared
/// as a set.
#[tokio::test]
async fn debug_lines_are_written_like_upstream() {
    if !tools_available() {
        return;
    }
    const DEBUG_CONFIG: &[u8] = b"domain: dbg
descriptors:
  - key: k1
    rate_limit: {unit: minute, requests_per_unit: 2}
  - key: k2
    value: v2
    detailed_metric: true
    rate_limit: {unit: hour, requests_per_unit: 5, name: named}
    descriptors:
      - key: inner
        rate_limit: {unit: second, requests_per_unit: 100}
  - key: wild
    value: pre*
    share_threshold: true
    detailed_metric: true
    rate_limit: {unit: day, requests_per_unit: 3}
  - key: vtm
    value_to_metric: true
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: free
    rate_limit: {unlimited: true}
  - key: rep
    rate_limit:
      unit: minute
      requests_per_unit: 1
      replaces: [{name: named}]
  - key: shadow
    shadow_mode: true
    rate_limit: {unit: minute, requests_per_unit: 1}
";
    const PATCH_CONFIG: &[u8] =
        b"domain: dbg\ndescriptors:\n  - key: extra\n    rate_limit: {unit: second, requests_per_unit: 1}\n";
    let runtime = temp_dir("debug-logs");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/a.yaml", DEBUG_CONFIG),
            ("v1/ratelimit/config/b.yaml", PATCH_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let requests = vec![
        request("dbg", &[&[("k1", "a")]]),
        request("dbg", &[&[("k1", "a")]]),
        request("dbg", &[&[("k1", "a")]]),
        request("dbg", &[&[("k1", "a")]]),
        request("dbg", &[&[("k2", "v2")], &[("k2", "v2"), ("inner", "x")]]),
        request("dbg", &[&[("wild", "prefix1")]]),
        request("dbg", &[&[("vtm", "val")]]),
        request("dbg", &[&[("free", "x")], &[("rep", "1")], &[("k2", "v2")]]),
        request("dbg", &[&[("shadow", "s")]]),
        request("dbg", &[&[("shadow", "s")]]),
        request("dbg", &[&[("shadow", "s")]]),
        request("nope", &[&[("a", "b")]]),
        request("dbg", &[&[("zzz", "1")], &[("k2", "v2"), ("inner", "x"), ("deep", "y")]]),
        RateLimitRequest {
            descriptors: vec![RateLimitDescriptor {
                limit: Some(
                    llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride {
                        requests_per_unit: 7,
                        unit: 1,
                        unit_multiplier: None,
                    },
                ),
                ..request("dbg", &[&[("o", "v")]]).descriptors[0].clone()
            }],
            ..request("dbg", &[])
        },
        request("", &[&[("a", "b")]]),
    ];
    let json = r#"{"domain":"dbg","descriptors":[{"entries":[{"key":"k1","value":"json"}]}]}"#;
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let redis = Redis::start().await;
        let (grpc, http) = (reserved_port(), reserved_port());
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", redis.address.clone()),
            ("MERGE_DOMAIN_CONFIG", "true".into()),
            ("LOCAL_CACHE_SIZE_IN_BYTES", "1048576".into()),
            ("DISABLE_STATS", "true".into()),
            ("LOG_LEVEL", "debug".into()),
            ("GRPC_HOST", "127.0.0.1".into()),
            ("GRPC_PORT", grpc.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", http.to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", reserved_port().to_string()),
        ];
        let stderr = requested_run(
            &binary,
            &environment,
            (grpc, http),
            &requests,
            Some(json),
            std::time::Duration::ZERO,
        )
        .await;
        runs.push(debug_lines(&stderr));
    }
    let (upstream, ours) = (&runs[0], &runs[1]);
    assert!(upstream.0.len() > 50, "{:?}", upstream.0);
    for (i, (u, o)) in upstream.0.iter().zip(&ours.0).enumerate() {
        assert_eq!(o, u, "line {i}");
    }
    assert_eq!(
        ours.0.len(),
        upstream.0.len(),
        "{:?}",
        &ours.0[upstream.0.len().min(ours.0.len())..]
    );
    assert_eq!(ours.1, upstream.1);
}

/// With Prometheus and `LOG_LEVEL=debug`, each stat flushed is logged (`FlushCounter`,
/// `FlushGauge`, `FlushTimer`): the same stats, values formatted like Go's (timers' values,
/// which are timings, by their shape). Go's runtime stats are upstream's only.
#[tokio::test]
async fn prometheus_flushes_are_logged_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("prometheus-debug");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let timer = regex::Regex::new(r"^FlushTimer: (\S+) (\S+)$").unwrap();
    let go_float = regex::Regex::new(r"^\d+(\.\d+)?(e[-+]\d\d+)?$").unwrap();
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let redis = Redis::start().await;
        let (grpc, http) = (reserved_port(), reserved_port());
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", redis.address.clone()),
            ("USE_STATSD", "false".into()),
            ("USE_PROMETHEUS", "true".into()),
            ("PROMETHEUS_ADDR", format!("127.0.0.1:{}", reserved_port())),
            ("STATS_FLUSH_INTERVAL", "100ms".into()),
            ("LOG_LEVEL", "debug".into()),
            ("GRPC_HOST", "127.0.0.1".into()),
            ("GRPC_PORT", grpc.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", http.to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", reserved_port().to_string()),
        ];
        let requests = vec![request("llm", &[&[("user", "a")]]); 3];
        let stderr = requested_run(
            &binary,
            &environment,
            (grpc, http),
            &requests,
            None,
            std::time::Duration::from_millis(500),
        )
        .await;
        // Counters summed: a flush may split them.
        let mut flushed = std::collections::BTreeSet::new();
        let mut counters = std::collections::BTreeMap::<String, u64>::new();
        for line in logrus_lines(&stderr, &[]) {
            let Some(message) = line
                .split_once("level=debug msg=\"")
                .and_then(|(_, m)| m.strip_suffix('"'))
            else {
                continue;
            };
            if let Some(c) = timer.captures(message) {
                assert!(go_float.is_match(&c[2]), "{message}");
                flushed.insert(format!("FlushTimer: {}", &c[1]));
            } else if message.contains(" ratelimit.go.") {
            } else if let Some(counter) = message.strip_prefix("FlushCounter: ") {
                let (name, value) = counter.rsplit_once(' ').unwrap();
                *counters.entry(name.to_string()).or_default() += value.parse::<u64>().unwrap();
            } else if message.starts_with("Flush") {
                flushed.insert(message.to_string());
            }
        }
        runs.push((flushed, counters));
    }
    assert!(
        runs[0].0.iter().any(|l| l.starts_with("FlushTimer: ")),
        "{:?}",
        runs[0]
    );
    assert!(runs[0].1.len() > 3, "{:?}", runs[0]);
    assert_eq!(runs[1], runs[0]);
}

/// `EXPIRATION_JITTER_MAX_SECONDS`: each key's TTL is its window plus a random extra below
/// the maximum, on both sides (the extras themselves are random), refunds' too; none at 0.
#[tokio::test]
async fn expirations_are_jittered_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("jitter");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    for (jitter, spread) in [("100", 60..160), ("0", 60..61)] {
        for binary in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ] {
            let redis = Redis::start().await;
            let (grpc, http) = (reserved_port(), reserved_port());
            let environment = vec![
                (
                    "RUNTIME_ROOT",
                    runtime.join("current").to_string_lossy().into_owned(),
                ),
                ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
                ("REDIS_SOCKET_TYPE", "tcp".into()),
                ("REDIS_URL", redis.address.clone()),
                ("DISABLE_STATS", "true".into()),
                ("EXPIRATION_JITTER_MAX_SECONDS", jitter.into()),
                ("GRPC_HOST", "127.0.0.1".into()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", http.to_string()),
                ("DEBUG_HOST", "127.0.0.1".into()),
                ("DEBUG_PORT", reserved_port().to_string()),
            ];
            let users: Vec<String> = (0..20).map(|i| format!("u{i}")).collect();
            // Each key counted then refunded: its TTL is the refund script's, jittered too.
            let mut requests: Vec<RateLimitRequest> = users
                .iter()
                .map(|user| request("llm", &[&[("user", user)]]))
                .collect();
            requests.extend(
                users
                    .iter()
                    .map(|user| refund(request("llm", &[&[("user", user)]]), 1)),
            );
            requested_run(
                &binary,
                &environment,
                (grpc, http),
                &requests,
                None,
                std::time::Duration::ZERO,
            )
            .await;
            let keys: Vec<String> = redis
                .client
                .custom(
                    CustomCommand::new_static("KEYS", ClusterHash::Random, false),
                    vec!["*"],
                )
                .await
                .unwrap();
            assert_eq!(keys.len(), users.len(), "{keys:?}");
            let mut ttls = std::collections::BTreeSet::new();
            for key in keys {
                let ttl: i64 = redis
                    .client
                    .custom(
                        CustomCommand::new_static("TTL", ClusterHash::Random, false),
                        vec![key],
                    )
                    .await
                    .unwrap();
                // A second may have passed since the EXPIRE.
                assert!(
                    (spread.start - 2..spread.end).contains(&ttl),
                    "{} {jitter}: {ttl}",
                    binary.display()
                );
                ttls.insert(ttl);
            }
            if jitter != "0" {
                assert!(ttls.len() > 5, "{} {jitter}: {ttls:?}", binary.display());
            }
        }
    }
}

/// With `REDIS_PERSECOND`, the per-second pool connects first, with its own log lines, then
/// the main one; its stats are under `redis_per_second_pool`.
#[tokio::test]
async fn a_per_second_redis_starts_like_upstream() {
    if !tools_available() {
        return;
    }
    const FAST: &[u8] = b"domain: d\ndescriptors:\n  - key: fast\n    rate_limit: {unit: second, requests_per_unit: 5}\n";
    let runtime = temp_dir("per-second");
    write_tree(
        &runtime,
        &[("v1/ratelimit/config/d.yaml", FAST), ("current", b"->v1")],
    );
    let stat =
        regex::Regex::new(r#""name":"(ratelimit\.redis_per_second_pool\.[^"]+)","type":"(\w+)""#)
            .unwrap();
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let (main, per_second) = (Redis::start().await, Redis::start().await);
        let (grpc, http) = (reserved_port(), reserved_port());
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", main.address.clone()),
            ("REDIS_POOL_SIZE", "2".into()),
            ("REDIS_PERSECOND", "true".into()),
            ("REDIS_PERSECOND_SOCKET_TYPE", "tcp".into()),
            ("REDIS_PERSECOND_URL", per_second.address.clone()),
            ("REDIS_PERSECOND_POOL_SIZE", "3".into()),
            ("REDIS_PERSECOND_AUTH", "secret".into()),
            ("REDIS_PERSECOND_POOL_ON_EMPTY_BEHAVIOR", "wait".into()),
            ("DISABLE_STATS", "false".into()),
            ("USE_STATSD", "false".into()),
            ("STATS_FLUSH_INTERVAL", "100ms".into()),
            ("LOG_LEVEL", "info".into()),
            ("GRPC_HOST", "127.0.0.1".into()),
            ("GRPC_PORT", grpc.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", http.to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", reserved_port().to_string()),
        ];
        // The per-second Redis wants that password.
        let _: String = per_second
            .client
            .custom(
                CustomCommand::new_static("CONFIG", ClusterHash::Random, false),
                vec!["SET", "requirepass", "secret"],
            )
            .await
            .unwrap();
        let requests = vec![request("d", &[&[("fast", "x")]]); 2];
        let stderr = requested_run(
            &binary,
            &environment,
            (grpc, http),
            &requests,
            None,
            std::time::Duration::from_millis(400),
        )
        .await;
        let ports = [
            (main.address.clone(), "MAIN".to_string()),
            (per_second.address.clone(), "PER_SECOND".to_string()),
        ];
        let mut lines: Vec<String> = stderr
            .lines()
            .filter(|l| l.starts_with("time=") && (l.contains("redis") || l.contains("Redis")))
            .map(|l| {
                let mut l = l
                    .split_once(" level=")
                    .map_or(l, |(_, rest)| rest)
                    .to_string();
                for (address, name) in &ports {
                    l = l.replace(address, name);
                }
                l
            })
            .collect();
        lines.dedup();
        let stats: std::collections::BTreeSet<String> = stat
            .captures_iter(&stderr)
            .map(|c| format!("{} {}", &c[1], &c[2]))
            .collect();
        runs.push((lines, stats));
    }
    assert!(runs[0].1.len() >= 2, "{:?}", runs[0].1);
    assert_eq!(runs[1], runs[0]);
}

/// The stderr of `binary` run with `environment`, serving gRPC and HTTP on `ports`: once it
/// serves, each request in turn (errors ignored) clear of a minute boundary, then `json` on
/// `/json` (answered with 200), then `linger`, then SIGTERM.
async fn requested_run(
    binary: &std::path::Path,
    environment: &[(&str, String)],
    (grpc, http): (u16, u16),
    requests: &[RateLimitRequest],
    json: Option<&str>,
    linger: std::time::Duration,
) -> String {
    let child = Command::new(binary)
        .env_clear()
        .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start the service");
    let mut client = None;
    for _ in 0..300 {
        if let Ok(c) = RateLimitServiceClient::connect(format!("http://127.0.0.1:{grpc}")).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let mut client = client.expect("the service serves");
    clear_of_a_minute_boundary().await;
    for request in requests {
        let _ = client.should_rate_limit(request.clone()).await;
    }
    if let Some(json) = json {
        let answer = send_raw(
            &format!("127.0.0.1:{http}"),
            &format!(
                "POST /json HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{json}",
                json.len()
            ),
        )
        .await;
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    }
    drop(client);
    tokio::time::sleep(linger).await;
    let pid = child.id().unwrap().to_string();
    std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap();
    let output = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("the service stops")
        .unwrap();
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The debug lines of `stderr` (time masked, Go's second space between message fields, cache
/// key windows and resets masked): those written in request order, then the others sorted:
/// the startup's (upstream loads the files in a random order, docs/parity.md §7) and those
/// written from other goroutines.
fn debug_lines(stderr: &str) -> (Vec<String>, Vec<String>) {
    let window = regex::Regex::new(r"_\d{10}\b").unwrap();
    let reset = regex::Regex::new(r"duration_until_reset:\{seconds:\d+\}").unwrap();
    let racy = [
        "msg=\"waiting for runtime update\"",
        "msg=\"Waiting for config update event\"",
        "msg=\"Setting config retrieved from config provider\"",
        "msg=\"Health status of components",
        "msg=\"got runtime update",
    ];
    let (mut ordered, mut others) = (Vec::new(), Vec::new());
    for line in stderr.lines().filter(|l| l.contains(" level=debug ")) {
        let line = line.split_once(" level=").map_or(line, |(_, rest)| rest);
        let line = window.replace_all(line, "_WINDOW");
        let line = reset.replace_all(&line, "duration_until_reset:{seconds:S}");
        let line = without_text_spaces(&line);
        let started = !ordered.is_empty() || line.contains("msg=\"ShouldRateLimit: ");
        match !started || racy.iter().any(|r| line.contains(r)) {
            true => others.push(line),
            false => ordered.push(line),
        }
    }
    others.sort();
    (ordered, others)
}

/// `text` with the second space Go's prototext may put between fields left out, outside
/// strings (logrus quotes the message, so its own quotes are escaped).
fn without_text_spaces(text: &str) -> String {
    let (mut out, mut in_string, mut escaped) = (String::new(), false, false);
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if !in_string && c == ' ' && out.ends_with(' ') {
            continue;
        }
        out.push(c);
        // Inside logrus' quoted message a string of the message is `\"…\"`.
        if c == '\\' && chars.peek() == Some(&'"') && !escaped {
            out.push(chars.next().unwrap());
            in_string = !in_string;
            continue;
        }
        escaped = c == '\\' && !escaped;
    }
    out
}

/// Upstream's Redis startup lines: the pool, the authentication, each failed connection
/// (unreachable, or refused by Redis: AUTH, the database, a cluster's READONLY), each retry (its wait and elapsed
/// time masked: they are random), then the fatal line once it gives up.
#[tokio::test]
async fn redis_startup_is_logged_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("redis-logs");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let unreachable = format!("127.0.0.1:{}", reserved_port());
    let redis = Redis::start().await;
    let refused_database = format!("redis://{}/abc", redis.address);
    let waits = regex::Regex::new(r"in [^ ]+ \(elapsed: [^)]+\)").unwrap();
    let after = regex::Regex::new(r"after [^:]+:").unwrap();
    // CLUSTER: each node tried in turn, the last error retried.
    let no_cluster = format!("{unreachable},{}", redis.address);
    for (url, auth, behavior, redis_type) in [
        (&unreachable, "user:secret", "wait", "single"),
        (&unreachable, "secret", "other", "single"),
        (&redis.address, "secret", "wait", "single"),
        (&refused_database, "", "wait", "single"),
        (&no_cluster, "", "wait", "cluster"),
    ] {
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", url.clone()),
            ("REDIS_AUTH", auth.into()),
            ("REDIS_POOL_SIZE", "3".into()),
            ("REDIS_PIPELINE_LIMIT", "8".into()),
            ("REDIS_POOL_ON_EMPTY_BEHAVIOR", behavior.into()),
            ("REDIS_TYPE", redis_type.into()),
            ("REDIS_STARTUP_INITIAL_INTERVAL", "50ms".into()),
            ("REDIS_STARTUP_MAX_INTERVAL", "100ms".into()),
            ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into()),
            ("DISABLE_STATS", "true".into()),
            ("LOG_LEVEL", "warn".into()),
        ];
        let lines = |stderr: &str| {
            // The logrus lines: upstream's panic (our fatal line, §7) is compared elsewhere, as
            // are the dials radix cancels when it gives up on a cluster's node (§7).
            let cancelled = |l: &String| {
                redis_type == "cluster"
                    && (l.contains("operation was canceled") || l.contains("context canceled"))
            };
            let mut lines: Vec<String> = logrus_lines(stderr, &[])
                .into_iter()
                .filter(|l| l.starts_with("time=") && !l.contains("level=fatal"))
                .filter(|l| !cancelled(l))
                .map(|l| waits.replace(&l, "in WAIT (elapsed: ELAPSED)").into_owned())
                .map(|l| after.replace(&l, "after ELAPSED:").into_owned())
                .collect();
            lines.dedup();
            lines
        };
        let upstream = lines(&startup_failure(&upstream_binary(), &environment).await);
        let ours = lines(
            &startup_failure(
                std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
                &environment,
            )
            .await,
        );
        assert!(
            upstream.iter().any(|l| l.contains("Retrying")),
            "{upstream:?}"
        );
        assert_eq!(ours, upstream, "{url} {auth} {behavior}");
    }
}

/// A Redis with a password: the right one serves (the startup probe authenticates too), a
/// wrong one is refused at each attempt, as upstream logs it.
#[tokio::test]
async fn a_password_protected_redis_is_logged_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("redis-password");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    // Set once the harness's connections are open: they stay authenticated.
    let mut stream = tokio::net::TcpStream::connect(&redis.address)
        .await
        .unwrap();
    tokio::io::AsyncWriteExt::write_all(
        &mut stream,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$11\r\nrequirepass\r\n$2\r\npw\r\n",
    )
    .await
    .unwrap();
    let mut reply = String::new();
    BufReader::new(&mut stream)
        .read_line(&mut reply)
        .await
        .unwrap();
    assert_eq!(reply, "+OK\r\n");
    let waits = regex::Regex::new(r"in [^ ]+ \(elapsed: [^)]+\)").unwrap();
    let after = regex::Regex::new(r"after [^:]+:").unwrap();
    let redis_lines = |stderr: &str| {
        let mut lines: Vec<String> = logrus_lines(stderr, &[])
            .into_iter()
            .filter(|l| l.starts_with("time=") && !l.contains("level=fatal"))
            .filter(|l| l.contains("redis") || l.contains("Redis"))
            .map(|l| waits.replace(&l, "in WAIT (elapsed: ELAPSED)").into_owned())
            .map(|l| after.replace(&l, "after ELAPSED:").into_owned())
            .collect();
        lines.dedup();
        lines
    };
    // REDIS_AUTH, or REDIS_AUTH_FILE (read at each connection, trimmed; missing or empty, the
    // dial fails); `fails` when the service cannot start.
    let files = runtime.join("auth");
    std::fs::create_dir_all(&files).unwrap();
    let file = |name: &str, contents: &str| {
        let path = files.join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_string_lossy().into_owned()
    };
    let cases = vec![
        ("REDIS_AUTH", "pw".to_string(), false),
        ("REDIS_AUTH", "default:pw".to_string(), false),
        ("REDIS_AUTH", "wrong".to_string(), true),
        ("REDIS_AUTH_FILE", file("password", " pw\n"), false),
        ("REDIS_AUTH_FILE", file("user", "default:pw"), false),
        ("REDIS_AUTH_FILE", file("wrong", "wrong"), true),
        ("REDIS_AUTH_FILE", file("empty", " \n"), true),
        (
            "REDIS_AUTH_FILE",
            files.join("missing").to_string_lossy().into_owned(),
            true,
        ),
    ];
    let mask = |l: String| l.replace(&*files.to_string_lossy(), "FILES");
    for (variable, auth, fails) in cases {
        let auth = auth.as_str();
        let mut runs = Vec::new();
        for binary in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ] {
            let grpc = reserved_port();
            let environment = vec![
                (
                    "RUNTIME_ROOT",
                    runtime.join("current").to_string_lossy().into_owned(),
                ),
                ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
                ("REDIS_SOCKET_TYPE", "tcp".into()),
                ("REDIS_URL", redis.address.clone()),
                (variable, auth.into()),
                ("REDIS_STARTUP_INITIAL_INTERVAL", "50ms".into()),
                ("REDIS_STARTUP_MAX_INTERVAL", "100ms".into()),
                ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into()),
                ("DISABLE_STATS", "true".into()),
                ("LOG_LEVEL", "info".into()),
                ("GRPC_HOST", "127.0.0.1".into()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", reserved_port().to_string()),
                ("DEBUG_HOST", "127.0.0.1".into()),
                ("DEBUG_PORT", reserved_port().to_string()),
            ];
            let stderr = match fails {
                true => startup_failure(&binary, &environment).await,
                false => {
                    logged_run(&binary, &environment, &format!("http://127.0.0.1:{grpc}")).await
                }
            };
            runs.push(
                redis_lines(&stderr)
                    .into_iter()
                    .map(mask)
                    .collect::<Vec<_>>(),
            );
        }
        assert!(runs[0].len() > 2, "{auth}: {:?}", runs[0]);
        assert_eq!(runs[1], runs[0], "{auth}");
    }
}

/// `REDIS_AUTH_FILE` is read at each connection: a password rotated in Redis and in the file
/// is used by the connections made after it, as upstream's.
#[tokio::test]
async fn auth_files_are_reread_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("auth-rotation");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let config_set = |password: &'static str| {
        move |redis: &Redis| {
            let client = redis.client.clone();
            async move {
                let _: String = client
                    .custom(
                        CustomCommand::new_static("CONFIG", ClusterHash::Random, false),
                        vec!["SET", "requirepass", password],
                    )
                    .await
                    .unwrap();
            }
        }
    };
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let redis = Redis::start().await;
        config_set("one")(&redis).await;
        let file = runtime.join(format!("auth-{}", reserved_port()));
        std::fs::write(&file, "one").unwrap();
        let grpc = reserved_port();
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", redis.address.clone()),
            ("REDIS_POOL_SIZE", "2".into()),
            ("REDIS_AUTH_FILE", file.to_string_lossy().into_owned()),
            ("DISABLE_STATS", "true".into()),
            ("GRPC_HOST", "127.0.0.1".into()),
            ("GRPC_PORT", grpc.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", reserved_port().to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", reserved_port().to_string()),
        ];
        let _child = Command::new(&binary)
            .env_clear()
            .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start the service");
        let mut client = None;
        for _ in 0..300 {
            if let Ok(c) = RateLimitServiceClient::connect(format!("http://127.0.0.1:{grpc}")).await
            {
                client = Some(c);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let mut client = client.expect("the service serves");
        client
            .should_rate_limit(request("llm", &[&[("user", "a")]]))
            .await
            .unwrap();
        // The rotation: Redis's password, the file, then the service's connections gone.
        config_set("two")(&redis).await;
        std::fs::write(&file, "two\n").unwrap();
        let _: i64 = redis
            .client
            .custom(
                CustomCommand::new_static("CLIENT", ClusterHash::Random, false),
                vec!["KILL", "SKIPME", "yes", "TYPE", "normal"],
            )
            .await
            .unwrap();
        // Requests may fail until the pool has connected again, with the new password.
        let mut answered = false;
        for _ in 0..100 {
            if client
                .should_rate_limit(request("llm", &[&[("user", "a")]]))
                .await
                .is_ok()
            {
                answered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(answered, "{} after the rotation", binary.display());
    }
}

/// A Redis serving TLS on `port` with the server certificate of `pki` (clients' certificates
/// required with `clients`), and in clear on another port, to know it is up.
async fn tls_redis(
    pki: &Pki,
    dir: &std::path::Path,
    port: u16,
    clients: bool,
    expired: bool,
) -> Child {
    let cert = dir.join(format!("server-{port}.pem"));
    let key = dir.join(format!("server-key-{port}.pem"));
    let (cert_pem, key_pem) = if expired {
        &pki.expired_server
    } else {
        &pki.server
    };
    std::fs::write(&cert, cert_pem).unwrap();
    std::fs::write(&key, key_pem).unwrap();
    let plain = reserved_port();
    let process = Command::new(redis_server())
        .args([
            "--port",
            &plain.to_string(),
            "--tls-port",
            &port.to_string(),
        ])
        .args(["--tls-cert-file", &cert.to_string_lossy()])
        .args(["--tls-key-file", &key.to_string_lossy()])
        .args(["--tls-ca-cert-file", &pki.ca.to_string_lossy()])
        .args(["--tls-auth-clients", if clients { "yes" } else { "no" }])
        .args(["--save", "", "--appendonly", "no"])
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("start the TLS Redis server");
    for _ in 0..250 {
        if tokio::net::TcpStream::connect(("127.0.0.1", plain))
            .await
            .is_ok()
        {
            return process;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("the TLS Redis server did not start");
}

/// `REDIS_TLS`: the connection over TLS, the server verified with the system's roots and
/// `REDIS_TLS_CACERT` (by its name, unless `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION`), a client
/// certificate when the server asks; what fails is retried and logged as upstream does.
#[tokio::test]
async fn redis_over_tls_is_used_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("redis-tls");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let pki = pki(&runtime.join("pki"));
    let ca = pki.ca.to_string_lossy().into_owned();
    let client = (
        pki.client_cert.to_string_lossy().into_owned(),
        pki.client_key.to_string_lossy().into_owned(),
    );
    let waits = regex::Regex::new(r"in [^ ]+ \(elapsed: [^)]+\)").unwrap();
    let after = regex::Regex::new(r"after [^:]+:").unwrap();
    // An unknown authority: Go on macOS asks the system, whose words differ from Linux's.
    let untrusted = regex::Regex::new(
        r"x509: (“[^”]*” certificate is not trusted|certificate signed by unknown authority)",
    )
    .unwrap();
    // An expired certificate: the current time is masked, not the certificate's end.
    let now = regex::Regex::new(r"current time \S+ is after").unwrap();
    // Each case: the host, a CA given, the hostname not verified, a client certificate given,
    // one asked by the server, the server's certificate expired, the start failing.
    struct Case {
        name: &'static str,
        host: &'static str,
        with_ca: bool,
        skip: bool,
        with_client: bool,
        clients: bool,
        expired: bool,
        fails: bool,
    }
    let base = Case {
        name: "",
        host: "127.0.0.1",
        with_ca: true,
        skip: false,
        with_client: false,
        clients: false,
        expired: false,
        fails: false,
    };
    let cases = [
        Case { name: "CA", ..base },
        Case {
            name: "system roots only",
            with_ca: false,
            fails: true,
            ..base
        },
        Case {
            name: "another name",
            host: "localhost",
            fails: true,
            ..base
        },
        Case {
            name: "another name, skipped",
            host: "localhost",
            skip: true,
            ..base
        },
        Case {
            name: "client certificate",
            with_client: true,
            clients: true,
            ..base
        },
        Case {
            name: "expired",
            expired: true,
            fails: true,
            ..base
        },
    ];
    for Case {
        name: case,
        host,
        with_ca,
        skip,
        with_client,
        clients,
        expired,
        fails,
    } in cases
    {
        let mut runs = Vec::new();
        for binary in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ] {
            let port = reserved_port();
            let _redis = tls_redis(&pki, &runtime.join("pki"), port, clients, expired).await;
            let grpc = reserved_port();
            let mut environment = vec![
                (
                    "RUNTIME_ROOT",
                    runtime.join("current").to_string_lossy().into_owned(),
                ),
                ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
                ("REDIS_SOCKET_TYPE", "tcp".into()),
                ("REDIS_URL", format!("{host}:{port}")),
                ("REDIS_TLS", "true".into()),
                ("REDIS_TLS_SKIP_HOSTNAME_VERIFICATION", skip.to_string()),
                ("REDIS_STARTUP_INITIAL_INTERVAL", "50ms".into()),
                ("REDIS_STARTUP_MAX_INTERVAL", "100ms".into()),
                ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into()),
                ("DISABLE_STATS", "true".into()),
                ("LOG_LEVEL", "info".into()),
                ("GRPC_HOST", "127.0.0.1".into()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", reserved_port().to_string()),
                ("DEBUG_HOST", "127.0.0.1".into()),
                ("DEBUG_PORT", reserved_port().to_string()),
            ];
            if with_ca {
                environment.push(("REDIS_TLS_CACERT", ca.clone()));
            }
            if with_client {
                environment.push(("REDIS_TLS_CLIENT_CERT", client.0.clone()));
                environment.push(("REDIS_TLS_CLIENT_KEY", client.1.clone()));
            }
            let stderr = match fails {
                true => startup_failure(&binary, &environment).await,
                false => {
                    logged_run(&binary, &environment, &format!("http://127.0.0.1:{grpc}")).await
                }
            };
            let mut lines: Vec<String> = logrus_lines(&stderr, &[(port.to_string(), "PORT")])
                .into_iter()
                .filter(|l| l.starts_with("time=") && !l.contains("level=fatal"))
                .filter(|l| l.contains("redis") || l.contains("Redis"))
                // radix fills its pool in the background: the dials still running when the
                // service stops are cancelled and logged, as many as it was slow (§7).
                .filter(|l| {
                    !l.ends_with("context canceled\"") && !l.ends_with("operation was canceled\"")
                })
                .map(|l| waits.replace(&l, "in WAIT (elapsed: ELAPSED)").into_owned())
                .map(|l| after.replace(&l, "after ELAPSED:").into_owned())
                .map(|l| untrusted.replace(&l, "x509: UNTRUSTED").into_owned())
                .map(|l| now.replace(&l, "current time NOW is after").into_owned())
                .collect();
            lines.dedup();
            runs.push(lines);
        }
        assert!(runs[0].len() > 2, "{case}: {:?}", runs[0]);
        assert_eq!(runs[1], runs[0], "{case}");
    }
}

/// `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR`: its warning at startup; with the Redis demoted
/// to a replica, then promoted again, each request answers and logs as upstream's. In SINGLE
/// mode upstream's commands are pipelined, whose errors its wrapper does not see: nothing more
/// is logged and no connection is closed.
#[tokio::test]
async fn readonly_replies_are_handled_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("readonly");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let command = |redis: &Redis, args: Vec<String>| {
        let client = redis.client.clone();
        async move {
            let (name, args) = args.split_first().unwrap();
            let name: &'static str = Box::leak(name.clone().into_boxed_str());
            let _: fred::types::Value = client
                .custom(
                    CustomCommand::new_static(name, ClusterHash::Random, false),
                    args.to_vec(),
                )
                .await
                .unwrap();
        }
    };
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let (master, redis) = (Redis::start().await, Redis::start().await);
        let grpc = reserved_port();
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", redis.address.clone()),
            ("REDIS_POOL_SIZE", "1".into()),
            ("REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR", "true".into()),
            ("DISABLE_STATS", "true".into()),
            ("LOG_LEVEL", "info".into()),
            ("GRPC_HOST", "127.0.0.1".into()),
            ("GRPC_PORT", grpc.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", reserved_port().to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", reserved_port().to_string()),
        ];
        let child = Command::new(&binary)
            .env_clear()
            .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("start the service");
        let mut client = None;
        for _ in 0..300 {
            if let Ok(c) = RateLimitServiceClient::connect(format!("http://127.0.0.1:{grpc}")).await
            {
                client = Some(c);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let mut client = client.expect("the service serves");
        let mut call = async || {
            client
                .should_rate_limit(request("llm", &[&[("user", "a")]]))
                .await
                .map(|_| "ok".to_string())
                .unwrap_or_else(|status| format!("{:?}", status.code()))
        };
        let mut outcomes = vec![call().await];
        // Demoted: writes are refused with READONLY.
        let (host, port) = master.address.split_once(':').unwrap();
        command(&redis, vec!["REPLICAOF".into(), host.into(), port.into()]).await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        outcomes.push(call().await);
        outcomes.push(call().await);
        // Promoted again.
        command(&redis, vec!["REPLICAOF".into(), "NO".into(), "ONE".into()]).await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        outcomes.push(call().await);
        let pid = child.id().unwrap().to_string();
        std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap();
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
                .await
                .expect("the service stops")
                .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let lines: Vec<String> = logrus_lines(&stderr, &[(redis.address.clone(), "REDIS")])
            .into_iter()
            .filter(|l| l.contains("READONLY") || l.contains("redis connection"))
            .collect();
        runs.push((outcomes, lines));
    }
    assert!(
        runs[0]
            .1
            .iter()
            .any(|l| l.contains("closing connections on READONLY")),
        "{:?}",
        runs[0]
    );
    // Refused while demoted, served again once promoted.
    assert_eq!(
        runs[0].0,
        ["ok", "Unknown", "Unknown", "ok"],
        "{:?}",
        runs[0]
    );
    assert_eq!(runs[1], runs[0]);
}

/// A Redis that answers only after the first dials fail: each failure, each retry, then the
/// connection restored, as upstream logs them; meanwhile, no /healthcheck yet. A binary slowed down enough to dial only once
/// Redis is up has nothing to compare, and a dial made while Redis comes up gets that
/// instant's error: the pair of runs is tried again, three times at most.
#[tokio::test]
async fn a_late_redis_is_logged_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("late-redis");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let waits = regex::Regex::new(r"in [^ ]+ \(elapsed: [^)]+\)").unwrap();
    let mut last = None;
    for _ in 0..3 {
        let runs = late_redis_runs(&runtime, &waits).await;
        if runs
            .iter()
            .all(|r| r.iter().any(|l| l.contains("re-established")))
        {
            if runs[1] == runs[0] {
                return;
            }
            last = Some(runs);
        }
    }
    let runs = last.expect("a run dialled before Redis was up");
    assert_eq!(runs[1], runs[0]);
}

/// The Redis lines of upstream's binary then ours, each with a Redis started 800 ms after it.
async fn late_redis_runs(runtime: &std::path::Path, waits: &regex::Regex) -> Vec<Vec<String>> {
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let port = reserved_port();
        let (grpc, http) = (reserved_port(), reserved_port());
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", format!("127.0.0.1:{port}")),
            ("REDIS_STARTUP_INITIAL_INTERVAL", "100ms".into()),
            ("REDIS_STARTUP_MAX_INTERVAL", "200ms".into()),
            ("DISABLE_STATS", "true".into()),
            ("LOG_LEVEL", "info".into()),
            ("GRPC_HOST", "127.0.0.1".into()),
            ("GRPC_PORT", grpc.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", http.to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", reserved_port().to_string()),
        ];
        let late = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
            Redis::start_on(port).await.expect("Redis started")
        });
        // Before Redis is up, the HTTP port (and its /healthcheck) does not answer yet.
        let early = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            let address = format!("127.0.0.1:{http}");
            match tokio::net::TcpStream::connect(&address).await {
                Err(_) => "healthcheck before Redis: refused".to_string(),
                Ok(_) => {
                    let answer = send_raw(
                        &address,
                        "GET /healthcheck HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
                    )
                    .await;
                    format!(
                        "healthcheck before Redis: {}",
                        answer.lines().next().unwrap_or_default()
                    )
                }
            }
        });
        let stderr = logged_run(&binary, &environment, &format!("http://127.0.0.1:{grpc}")).await;
        let _redis = late.await.unwrap();
        let mut lines: Vec<String> = logrus_lines(&stderr, &[(port.to_string(), "REDIS")])
            .into_iter()
            .filter(|l| l.contains("redis") || l.contains("Redis"))
            .map(|l| waits.replace(&l, "in WAIT (elapsed: ELAPSED)").into_owned())
            .collect();
        lines.dedup();
        lines.push(early.await.unwrap());
        runs.push(lines);
    }
    runs
}

/// What a fake OTLP collector received: each export's spans and the HTTP or gRPC headers
/// that came with it.
#[derive(Default)]
struct Collected {
    requests: Vec<
        llm_ratelimit_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest,
    >,
    headers: Vec<(String, String)>,
    paths: Vec<String>,
}

/// A certificate authority, a server certificate for 127.0.0.1 and a client certificate it
/// issued, as PEM files in `dir`.
struct Pki {
    ca: PathBuf,
    server: (String, String),
    /// A server certificate for 127.0.0.1 that expired on 2020-01-01.
    expired_server: (String, String),
    /// For 127.0.0.1, as a server and as a client: a Redis cluster node's (its bus).
    node: (String, String),
    client_cert: PathBuf,
    client_key: PathBuf,
}

fn pki(dir: &std::path::Path) -> Pki {
    use rcgen::{CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair};
    // Each its own subject: OpenSSL takes a certificate named as its issuer for a root.
    let named = |params: &mut CertificateParams, name: &str| {
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
    };
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    named(&mut ca, "test CA");
    ca.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca, KeyPair::generate().unwrap()).unwrap();
    let issue = |name: &str, purposes: Vec<ExtendedKeyUsagePurpose>, expired: bool| {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![name.to_string()]).unwrap();
        named(&mut params, name);
        params.extended_key_usages = purposes;
        if expired {
            params.not_before = rcgen::date_time_ymd(2019, 1, 1);
            params.not_after = rcgen::date_time_ymd(2020, 1, 1);
        }
        let cert = params.signed_by(&key, &ca).unwrap();
        (cert.pem(), key.serialize_pem())
    };
    use ExtendedKeyUsagePurpose::{ClientAuth, ServerAuth};
    let server = issue("127.0.0.1", vec![ServerAuth], false);
    let expired_server = issue("127.0.0.1", vec![ServerAuth], true);
    let node = issue("127.0.0.1", vec![ServerAuth, ClientAuth], false);
    let (client_cert, client_key) = issue("client", vec![ClientAuth], false);
    std::fs::create_dir_all(dir).unwrap();
    let write = |name: &str, pem: &str| {
        let path = dir.join(name);
        std::fs::write(&path, pem).unwrap();
        path
    };
    Pki {
        ca: write("ca.pem", &ca.pem()),
        server,
        expired_server,
        node,
        client_cert: write("client.pem", &client_cert),
        client_key: write("client-key.pem", &client_key),
    }
}

/// A fake OTLP collector on a port it returns: gRPC's `TraceService/Export` (gzip accepted), or HTTP's
/// `POST /v1/traces` with a protobuf body (gzipped or not); over TLS with `pki`, requiring a
/// client certificate it issued.
async fn otlp_collector(grpc: bool, pki: Option<&Pki>) -> (Arc<std::sync::Mutex<Collected>>, u16) {
    use llm_ratelimit_proto::opentelemetry::proto::collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    };
    let collected = Arc::new(std::sync::Mutex::new(Collected::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    if grpc {
        struct Collector(Arc<std::sync::Mutex<Collected>>);
        #[tonic::async_trait]
        impl TraceService for Collector {
            async fn export(
                &self,
                request: tonic::Request<ExportTraceServiceRequest>,
            ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
                let mut collected = self.0.lock().unwrap();
                for (key, value) in request.metadata().clone().into_headers() {
                    if let (Some(key), Ok(value)) = (key, value.to_str()) {
                        collected.headers.push((key.to_string(), value.to_string()));
                    }
                }
                collected.requests.push(request.into_inner());
                Ok(tonic::Response::new(ExportTraceServiceResponse::default()))
            }
        }
        let service = TraceServiceServer::new(Collector(collected.clone()))
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        let mut server = tonic::transport::Server::builder();
        if let Some(pki) = pki {
            let tls = tonic::transport::ServerTlsConfig::new()
                .identity(tonic::transport::Identity::from_pem(
                    &pki.server.0,
                    &pki.server.1,
                ))
                .client_ca_root(tonic::transport::Certificate::from_pem(
                    std::fs::read(&pki.ca).unwrap(),
                ));
            server = server.tls_config(tls).unwrap();
        }
        tokio::spawn(
            server
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
    } else {
        let sink = collected.clone();
        let acceptor = pki.map(|pki| {
            use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_file_iter(&pki.ca).unwrap() {
                roots.add(cert.unwrap()).unwrap();
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider.clone(),
            )
            .build()
            .unwrap();
            let config = rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_client_cert_verifier(verifier)
                .with_single_cert(
                    CertificateDer::pem_slice_iter(pki.server.0.as_bytes())
                        .map(Result::unwrap)
                        .collect(),
                    PrivateKeyDer::from_pem_slice(pki.server.1.as_bytes()).unwrap(),
                )
                .unwrap();
            tokio_rustls::TlsAcceptor::from(Arc::new(config))
        });
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let sink = sink.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(
                        move |request: hyper::Request<hyper::body::Incoming>| {
                            let sink = sink.clone();
                            async move {
                                use http_body_util::BodyExt;
                                let headers: Vec<(String, String)> = request
                                    .headers()
                                    .iter()
                                    .map(|(k, v)| {
                                        (k.to_string(), v.to_str().unwrap_or_default().to_string())
                                    })
                                    .collect();
                                let path = request.uri().path().to_string();
                                let gzip = request
                                    .headers()
                                    .get("content-encoding")
                                    .is_some_and(|v| v == "gzip");
                                let mut body = request
                                    .into_body()
                                    .collect()
                                    .await
                                    .unwrap()
                                    .to_bytes()
                                    .to_vec();
                                if gzip {
                                    let mut plain = Vec::new();
                                    std::io::Read::read_to_end(
                                        &mut flate2::read::GzDecoder::new(&body[..]),
                                        &mut plain,
                                    )
                                    .unwrap();
                                    body = plain;
                                }
                                let decoded =
                                    <ExportTraceServiceRequest as prost::Message>::decode(
                                        &body[..],
                                    )
                                    .unwrap();
                                let mut collected = sink.lock().unwrap();
                                collected.headers.extend(headers);
                                collected.paths.push(path);
                                collected.requests.push(decoded);
                                Ok::<_, std::convert::Infallible>(hyper::Response::new(
                                    http_body_util::Full::new(bytes::Bytes::new()),
                                ))
                            }
                        },
                    );
                    let http = hyper::server::conn::http1::Builder::new();
                    let _ = match acceptor {
                        Some(acceptor) => match acceptor.accept(stream).await {
                            Ok(tls) => {
                                http.serve_connection(hyper_util::rt::TokioIo::new(tls), service)
                                    .await
                            }
                            Err(_) => return,
                        },
                        None => {
                            http.serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                                .await
                        }
                    };
                });
            }
        });
    }
    (collected, port)
}

/// The spans a collector received, comparable across binaries: the resource and schema, then
/// each span's scope, name, kind, flags, trace state, status, sorted attributes (the port and
/// the instance ID masked, Go's prototext spacing left out), and its trace and parent by what
/// they are (the request's, another span's by name, a new trace). Resets are masked.
fn received_spans(collected: &Collected, known: &[(&str, [u8; 16], [u8; 8])]) -> Vec<String> {
    use llm_ratelimit_proto::opentelemetry::proto::common::v1::any_value::Value as V;
    let value =
        |v: &Option<llm_ratelimit_proto::opentelemetry::proto::common::v1::AnyValue>| match v
            .as_ref()
            .and_then(|v| v.value.clone())
        {
            Some(V::StringValue(s)) => {
                let reset = regex::Regex::new(r"duration_until_reset:\{seconds:\d+\}").unwrap();
                let s = reset.replace_all(&s, "duration_until_reset:{seconds:S}");
                format!("{:?}", without_text_spaces(&s))
            }
            Some(V::IntValue(i)) => format!("int {i}"),
            other => format!("{other:?}"),
        };
    let mut spans = Vec::new();
    let mut all = Vec::new();
    for request in &collected.requests {
        for resource_spans in &request.resource_spans {
            let resource: Vec<String> = resource_spans
                .resource
                .iter()
                .flat_map(|r| &r.attributes)
                .map(|kv| format!("{}={}", kv.key, value(&kv.value)))
                .collect();
            spans.push(format!(
                "resource {} {:?}",
                resource_spans.schema_url, resource
            ));
            for scope_spans in &resource_spans.scope_spans {
                for span in &scope_spans.spans {
                    all.push((
                        scope_spans.scope.clone().unwrap_or_default(),
                        scope_spans.schema_url.clone(),
                        span.clone(),
                    ));
                }
            }
        }
    }
    let name_of = |id: &[u8]| {
        all.iter()
            .find(|(_, _, s)| s.span_id == id)
            .map(|(_, _, s)| format!("span {}", s.name))
            .or_else(|| {
                known
                    .iter()
                    .find(|(_, _, s)| s[..] == *id)
                    .map(|(n, _, _)| format!("{n}'s parent"))
            })
            .unwrap_or_else(|| {
                if id.is_empty() {
                    "none".into()
                } else {
                    "unknown".into()
                }
            })
    };
    for (scope, schema, span) in &all {
        let trace = known
            .iter()
            .find(|(_, t, _)| t[..] == span.trace_id[..])
            .map_or("a new trace".to_string(), |(n, _, _)| {
                format!("{n}'s trace")
            });
        let mut attributes: Vec<String> = span
            .attributes
            .iter()
            .map(|kv| match kv.key.as_str() {
                "server.port" => "server.port=PORT".to_string(),
                _ => format!("{}={}", kv.key, value(&kv.value)),
            })
            .collect();
        attributes.sort();
        let status = span.status.clone().unwrap_or_default();
        spans.push(format!(
            "{} {:?} {} | {} kind {} flags {:#x} state {:?} | {} | parent {} | status {} {:?} | {:?} dropped {}",
            scope.name,
            scope.version,
            schema,
            span.name,
            span.kind,
            span.flags,
            span.trace_state,
            trace,
            name_of(&span.parent_span_id),
            status.code,
            status.message,
            attributes,
            span.dropped_attributes_count
        ));
    }
    spans.sort();
    spans.dedup();
    spans
}

/// With `TRACING_ENABLED`, both binaries export the same spans, over gRPC (gzipped) and
/// over HTTP: the resource (`TRACING_SERVICE_*` over `OTEL_RESOURCE_ATTRIBUTES` and
/// `OTEL_SERVICE_NAME`), otelgrpc's server spans (the health check's too) under the caller's
/// trace (W3C, or B3 when both are sent; a deferred B3 decision is not sampled), the service's
/// and Redis's spans beside them, `/json`'s three traces, and the configured headers; the
/// semantic conventions `OTEL_SEMCONV_STABILITY_OPT_IN` chooses; `TRACING_SAMPLING_RATE`.
#[tokio::test]
async fn traces_are_exported_like_upstream() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit_proto::grpc::health::v1::{HealthCheckRequest, health_client::HealthClient};
    let runtime = temp_dir("traces");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let w3c_trace = [0x4b; 16];
    let w3c_span = [0x0f; 8];
    let b3_trace = [0x0a; 16];
    let b3_span = [0x0b; 8];
    let deferred_trace = [0x0c; 16];
    let known = [
        ("traceparent", w3c_trace, w3c_span),
        ("b3", b3_trace, b3_span),
        ("deferred b3", deferred_trace, [0x0d; 8]),
    ];
    let hex = |b: &[u8]| b.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let traceparent = format!("00-{}-{}-01", hex(&w3c_trace), hex(&w3c_span));
    let b3 = format!("{}-{}-1", hex(&b3_trace), hex(&b3_span));
    // Each protocol, compressed or not; the previous and both semantic conventions; a rate
    // of 0, which keeps only what a sampled caller's trace asks for.
    // Over TLS, the collector verified with `OTEL_EXPORTER_OTLP_CERTIFICATE`, the exporter
    // with its client certificate.
    let pki = pki(&runtime.join("pki"));
    // With span limits: strings cut to 10 characters, at most 4 attributes.
    for (protocol, compression, conventions, rate, tls, limits) in [
        ("grpc", "gzip", "", "1", false, false),
        ("http", "none", "rpc/old", "1", false, true),
        ("http", "gzip", "x, rpc/dup", "0", false, false),
        ("grpc", "none", "", "1", true, false),
        ("http", "none", "", "1", true, false),
    ] {
        let mut runs = Vec::new();
        for binary in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ] {
            let redis = Redis::start().await;
            let (collected, collector_port) =
                otlp_collector(protocol == "grpc", tls.then_some(&pki)).await;
            let scheme = if tls { "https" } else { "http" };
            let (grpc, http) = (reserved_port(), reserved_port());
            let environment = vec![
                (
                    "RUNTIME_ROOT",
                    runtime.join("current").to_string_lossy().into_owned(),
                ),
                ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
                ("REDIS_SOCKET_TYPE", "tcp".into()),
                ("REDIS_URL", redis.address.clone()),
                ("DISABLE_STATS", "true".into()),
                ("LOG_LEVEL", "info".into()),
                ("TRACING_ENABLED", "true".into()),
                ("TRACING_EXPORTER_PROTOCOL", protocol.into()),
                ("TRACING_SERVICE_NAMESPACE", "ns".into()),
                ("TRACING_SERVICE_INSTANCE_ID", "instance-1".into()),
                ("TRACING_SAMPLING_RATE", rate.into()),
                ("OTEL_SEMCONV_STABILITY_OPT_IN", conventions.into()),
                ("OTEL_SERVICE_NAME", "ignored".into()),
                (
                    "OTEL_RESOURCE_ATTRIBUTES",
                    "deployment= test%20env ,team=rl,team=limits,=empty".into(),
                ),
                (
                    "OTEL_EXPORTER_OTLP_ENDPOINT",
                    format!("{scheme}://127.0.0.1:{collector_port}"),
                ),
                (
                    "OTEL_EXPORTER_OTLP_HEADERS",
                    "x-team=rl,x-env=test%20env".into(),
                ),
                ("OTEL_EXPORTER_OTLP_COMPRESSION", compression.into()),
                ("GRPC_HOST", "127.0.0.1".into()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", http.to_string()),
                ("DEBUG_HOST", "127.0.0.1".into()),
                ("DEBUG_PORT", reserved_port().to_string()),
            ];
            let mut environment = environment;
            if limits {
                environment.extend([
                    ("OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT", "10".to_string()),
                    ("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "4".to_string()),
                    ("OTEL_ATTRIBUTE_COUNT_LIMIT", "1".to_string()),
                ]);
            }
            if tls {
                environment.extend([
                    (
                        "OTEL_EXPORTER_OTLP_CERTIFICATE",
                        pki.ca.to_string_lossy().into_owned(),
                    ),
                    (
                        "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
                        pki.client_cert.to_string_lossy().into_owned(),
                    ),
                    (
                        "OTEL_EXPORTER_OTLP_CLIENT_KEY",
                        pki.client_key.to_string_lossy().into_owned(),
                    ),
                ]);
            }
            let child = Command::new(&binary)
                .env_clear()
                .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .expect("start the service");
            let address = format!("http://127.0.0.1:{grpc}");
            let mut channel = None;
            for _ in 0..300 {
                if let Ok(c) = Channel::from_shared(address.clone())
                    .unwrap()
                    .connect()
                    .await
                {
                    channel = Some(c);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            let channel = channel.expect("the service serves");
            // Serving for sure, with no traced call: its health check over HTTP answers.
            let mut healthy = false;
            for _ in 0..600 {
                let answer =
                    http_request(&format!("127.0.0.1:{http}"), "GET", "/healthcheck", &[]).await;
                if answer.status == 200 {
                    healthy = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(healthy, "the service is healthy");
            let mut client = RateLimitServiceClient::new(channel.clone());
            let call = |headers: &[(&'static str, String)]| {
                let mut call = tonic::Request::new(request("llm", &[&[("user", "t")]]));
                for (key, value) in headers {
                    call.metadata_mut().insert(*key, value.parse().unwrap());
                }
                call
            };
            client
                .should_rate_limit(call(&[
                    ("traceparent", traceparent.clone()),
                    ("tracestate", "vendor=a, other@x=b ".into()),
                ]))
                .await
                .unwrap();
            client
                .should_rate_limit(call(&[
                    ("traceparent", traceparent.clone()),
                    ("b3", b3.clone()),
                ]))
                .await
                .unwrap();
            client
                .should_rate_limit(call(&[
                    ("x-b3-traceid", hex(&deferred_trace)),
                    ("x-b3-spanid", hex(&[0x0d; 8])),
                ]))
                .await
                .unwrap();
            let _ = client
                .should_rate_limit(tonic::Request::new(request("", &[&[("user", "t")]])))
                .await;
            HealthClient::new(channel.clone())
                .check(HealthCheckRequest {
                    service: "ratelimit".into(),
                })
                .await
                .unwrap();
            // A method no service has: no span.
            let mut unknown = tonic::client::Grpc::new(channel);
            unknown.ready().await.unwrap();
            let codec =
                tonic_prost::ProstCodec::<HealthCheckRequest, HealthCheckRequest>::default();
            let answer = unknown
                .unary(
                    tonic::Request::new(HealthCheckRequest::default()),
                    http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Nope"),
                    codec,
                )
                .await;
            assert_eq!(answer.unwrap_err().code(), tonic::Code::Unimplemented);
            let json =
                r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"j"}]}]}"#;
            let answer = send_raw(
                &format!("127.0.0.1:{http}"),
                &format!(
                    "POST /json HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{json}",
                    json.len()
                ),
            )
            .await;
            assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
            let pid = child.id().unwrap().to_string();
            std::process::Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap();
            let output =
                tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
                    .await
                    .expect("the service stops")
                    .unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            let collected = collected.lock().unwrap();
            let mut headers: Vec<String> = collected
                .headers
                .iter()
                .filter(|(k, _)| {
                    k.starts_with("x-") || k == "content-type" || k == "content-encoding"
                })
                .map(|(k, v)| format!("{k}: {v}"))
                .collect();
            headers.sort();
            headers.dedup();
            let mut paths = collected.paths.clone();
            paths.dedup();
            let started: Vec<String> = logrus_lines(&stderr, &[])
                .into_iter()
                .filter(|l| l.contains("racerProvider") || l.contains("Tracing"))
                .collect();
            assert_eq!(started.len(), 1, "{stderr}");
            runs.push((received_spans(&collected, &known), headers, paths, started));
        }
        // The resource and, at a rate of 0, the six spans of the two sampled callers.
        assert!(runs[0].0.len() >= 7, "{protocol}: {:#?}", runs[0].0);
        for (u, o) in runs[0].0.iter().zip(&runs[1].0) {
            assert_eq!(o, u, "{protocol} {compression}");
        }
        assert_eq!(runs[1], runs[0], "{protocol} {compression}");
    }
}

/// A TLS client trusting `ca` (PEM), with `identity` (certificate and key PEM) if any, and
/// ALPN `h2` if `h2`.
fn tls_client_config(
    ca: &str,
    identity: Option<&(String, String)>,
    h2: bool,
) -> rustls::ClientConfig {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca.as_bytes()) {
        roots.add(cert.unwrap()).unwrap();
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots);
    let mut config = match identity {
        Some((cert, key)) => builder
            .with_client_auth_cert(
                CertificateDer::pem_slice_iter(cert.as_bytes())
                    .map(Result::unwrap)
                    .collect(),
                PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap(),
            )
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    if h2 {
        config.alpn_protocols = vec![b"h2".to_vec()];
    }
    config
}

/// What a TLS client finds on the gRPC port: `served` when the server answers HTTP/2's
/// preface, `closed` when it closes the connection, `refused` when the handshake or the first
/// read fails; `plain` for a client without TLS.
async fn tls_probe(port: u16, config: Option<rustls::ClientConfig>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0";
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let read = match config {
        None => {
            let mut tcp = tcp;
            let _ = tcp.write_all(PREFACE).await;
            tokio::time::timeout(std::time::Duration::from_secs(5), tcp.read(&mut buf)).await
        }
        Some(config) => {
            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
            let name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
            let Ok(mut tls) = connector.connect(name, tcp).await else {
                return "refused".into();
            };
            let _ = tls.write_all(PREFACE).await;
            tokio::time::timeout(std::time::Duration::from_secs(5), tls.read(&mut buf)).await
        }
    };
    match read {
        Ok(Ok(0)) => "closed".into(),
        Ok(Ok(_)) => "served".into(),
        Ok(Err(_)) => "refused".into(),
        Err(_) => "silent".into(),
    }
}

/// A gRPC client over TLS on `port`.
async fn tls_grpc_client(
    port: u16,
    config: rustls::ClientConfig,
) -> RateLimitServiceClient<Channel> {
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let connect = tower::service_fn(move |_: tonic::transport::Uri| {
        let connector = connector.clone();
        async move {
            let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
            let name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
            let tls = connector.connect(name, tcp).await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tls))
        }
    });
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1")
        .connect_with_connector(connect)
        .await
        .unwrap();
    RateLimitServiceClient::new(channel)
}

/// A service binary whose stderr is collected as it writes it; stopped with SIGTERM.
struct Logged {
    child: Child,
    lines: Arc<std::sync::Mutex<Vec<String>>>,
    reader: tokio::task::JoinHandle<()>,
}

impl Logged {
    async fn start(binary: &std::path::Path, environment: &[(&str, String)], http: u16) -> Self {
        let logged = Self::spawn(binary, environment);
        logged.serving(binary, http).await;
        logged
    }

    /// The binary started, its stderr collected.
    fn spawn(binary: &std::path::Path, environment: &[(&str, String)]) -> Self {
        let mut child = Command::new(binary)
            .env_clear()
            .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("start the service");
        let lines = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut stderr = BufReader::new(child.stderr.take().unwrap()).lines();
        let sink = lines.clone();
        let reader = tokio::spawn(async move {
            while let Ok(Some(line)) = stderr.next_line().await {
                sink.lock().unwrap().push(line);
            }
        });
        Self {
            child,
            lines,
            reader,
        }
    }

    /// Waits until the HTTP port opens (once gRPC listens).
    async fn serving(&self, binary: &std::path::Path, http: u16) {
        for _ in 0..300 {
            if tokio::net::TcpStream::connect(("127.0.0.1", http))
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!(
            "{} does not serve: {:?}",
            binary.display(),
            self.lines.lock().unwrap()
        );
    }

    /// Waits until `count` lines hold `text`.
    async fn wait_for(&self, text: &str, count: usize) {
        for _ in 0..200 {
            let found = self
                .lines
                .lock()
                .unwrap()
                .iter()
                .filter(|l| l.contains(text))
                .count();
            if found >= count {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("no line with {text:?} in {:?}", self.lines.lock().unwrap());
    }

    /// Waits until the service exits by itself, unsuccessfully.
    async fn exited(mut self) -> String {
        let status = tokio::time::timeout(std::time::Duration::from_secs(30), self.child.wait())
            .await
            .expect("the service exits")
            .unwrap();
        assert!(!status.success());
        let _ = self.reader.await;
        self.lines.lock().unwrap().join("\n")
    }

    async fn stop(mut self) -> String {
        let pid = self.child.id().unwrap().to_string();
        std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), self.child.wait())
            .await
            .expect("the service stops")
            .unwrap();
        let _ = self.reader.await;
        self.lines.lock().unwrap().join("\n")
    }
}

/// The server's key pair as files in `dir`.
fn server_files(dir: &std::path::Path, (cert, key): &(String, String)) -> (String, String) {
    std::fs::create_dir_all(dir).unwrap();
    let (cert_path, key_path) = (dir.join("server.pem"), dir.join("server-key.pem"));
    std::fs::write(&cert_path, cert).unwrap();
    std::fs::write(&key_path, key).unwrap();
    (
        cert_path.to_string_lossy().into_owned(),
        key_path.to_string_lossy().into_owned(),
    )
}

/// `GRPC_SERVER_USE_TLS`: gRPC over TLS with the cert provider's key pair, only for clients
/// negotiating `h2`; with `GRPC_CLIENT_TLS_CACERT`, only for clients with a certificate the
/// CA issued, valid for `GRPC_CLIENT_TLS_SAN` if set (a mismatch is logged); the `certs.*`
/// stats and the log lines as upstream's.
#[tokio::test]
async fn grpc_over_tls_is_served_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("grpc-tls");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let other = pki(&runtime.join("other-pki"));
    let pki = pki(&runtime.join("pki"));
    let (cert, key) = server_files(&runtime.join("certs"), &pki.server);
    // Not counted in `certs.num_values`: the loader ignores dotfiles.
    std::fs::write(runtime.join("certs/.hidden"), "x").unwrap();
    let ca_pem = std::fs::read_to_string(&pki.ca).unwrap();
    let client = (
        std::fs::read_to_string(&pki.client_cert).unwrap(),
        std::fs::read_to_string(&pki.client_key).unwrap(),
    );
    let stranger = (
        std::fs::read_to_string(&other.client_cert).unwrap(),
        std::fs::read_to_string(&other.client_key).unwrap(),
    );
    let ca = pki.ca.to_string_lossy().into_owned();
    // The client CA, the client SAN, and whether ALPN is enforced (grpc-go's default).
    let clients: [(&str, Option<&str>, &str); 7] = [
        ("", None, ""),
        ("", None, "False"),
        (&ca, None, ""),
        (&ca, Some("client"), ""),
        (&ca, Some("other.example"), ""),
        (&ca, Some("10.0.0.1"), ""),
        // No host name: Go matches nothing to it.
        (&ca, Some("spiffe://x"), ""),
    ];
    for (client_ca, san, enforce_alpn) in clients {
        let mut runs = Vec::new();
        for binary in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ] {
            let (grpc, http, debug) = (reserved_port(), reserved_port(), reserved_port());
            let statsd = StatsdServer::start().await;
            // A Redis of its own: the decision below is the first hit, whatever the minute.
            let redis = Redis::start().await;
            let mut environment = binary_environment(&runtime, &redis);
            environment.retain(|(k, _)| *k != "LOG_LEVEL" && *k != "DISABLE_STATS");
            environment.extend([
                ("LOG_LEVEL", "info".to_string()),
                ("GRPC_HOST", "127.0.0.1".into()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", http.to_string()),
                ("DEBUG_HOST", "127.0.0.1".into()),
                ("DEBUG_PORT", debug.to_string()),
                ("STATS_FLUSH_INTERVAL", "100ms".into()),
                ("STATSD_HOST", "127.0.0.1".into()),
                ("STATSD_PORT", statsd.address.port().to_string()),
                ("GRPC_SERVER_USE_TLS", "true".into()),
                ("GRPC_SERVER_TLS_CERT", cert.clone()),
                ("GRPC_SERVER_TLS_KEY", key.clone()),
                ("GRPC_CLIENT_TLS_CACERT", client_ca.to_string()),
            ]);
            if let Some(san) = san {
                environment.push(("GRPC_CLIENT_TLS_SAN", san.to_string()));
            }
            if !enforce_alpn.is_empty() {
                environment.push(("GRPC_ENFORCE_ALPN_ENABLED", enforce_alpn.to_string()));
            }
            let service = Logged::start(&binary, &environment, http).await;
            let mut outcomes = vec![
                tls_probe(grpc, None).await,
                tls_probe(grpc, Some(tls_client_config(&ca_pem, None, true))).await,
                tls_probe(grpc, Some(tls_client_config(&ca_pem, Some(&client), true))).await,
                tls_probe(grpc, Some(tls_client_config(&ca_pem, Some(&client), false))).await,
                tls_probe(
                    grpc,
                    Some(tls_client_config(&ca_pem, Some(&stranger), true)),
                )
                .await,
            ];
            if outcomes[2] == "served" {
                let mut grpc_client =
                    tls_grpc_client(grpc, tls_client_config(&ca_pem, Some(&client), true)).await;
                let response = grpc_client
                    .should_rate_limit(request("llm", &[&[("user", "a")]]))
                    .await
                    .unwrap()
                    .into_inner();
                outcomes.push(format!("{:?}", response.overall_code));
            }
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let stats = loop {
                let stats = stat_values(&statsd.lines.lock().unwrap(), |name| {
                    name.starts_with("certs.")
                });
                // `certs.load_failures` stays at 0, so unflushed.
                let flushed = stats.contains_key("certs.load_attempts|c")
                    && stats.contains_key("certs.num_values|g");
                if flushed || std::time::Instant::now() > deadline {
                    break stats;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            };
            let stderr = service.stop().await;
            let replace = [
                (runtime.to_string_lossy().into_owned(), "RUNTIME"),
                (format!(":{grpc}'"), ":GRPC'"),
                (format!(":{http}'"), ":HTTP'"),
                (format!(":{debug}"), ":DEBUG"),
                (redis.address.clone(), "REDIS"),
            ];
            let lines: Vec<String> = logrus_lines(&stderr, &replace)
                .into_iter()
                .filter(|l| !l.contains("gostats"))
                .collect();
            runs.push((outcomes, stats, lines));
        }
        let (ours, upstream) = (&mut runs[1].clone(), &runs[0]);
        // Upstream's process may exit before its debug server logs that it stops: that line,
        // when upstream has it, ours has, but it is compared no further.
        let stopping = |l: &String| l.contains("Failed to start debug server");
        for line in upstream.2.iter().filter(|l| stopping(l)) {
            assert!(ours.2.contains(line), "{line} only upstream's");
        }
        if !upstream.2.iter().any(stopping) {
            ours.2.retain(|l| !stopping(l));
        }
        assert_eq!(
            ours.0, upstream.0,
            "outcomes with {client_ca:?} {san:?} {enforce_alpn:?}"
        );
        assert_eq!(
            ours.1, upstream.1,
            "certs stats with {client_ca:?} {san:?} {enforce_alpn:?}"
        );
        assert_eq!(
            ours.2, upstream.2,
            "logs with {client_ca:?} {san:?} {enforce_alpn:?}"
        );
    }
}

/// The cert provider reloads the key pair on each change in its directory, and keeps the one
/// it serves when the new one does not load (a key for another certificate, a file without
/// PEM data), as upstream's.
#[tokio::test]
async fn grpc_certificates_are_reloaded_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("grpc-tls-reload");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let first = pki(&runtime.join("first"));
    let second = pki(&runtime.join("second"));
    let trusting = |pki: &Pki| {
        let ca = std::fs::read_to_string(&pki.ca).unwrap();
        move || Some(tls_client_config(&ca, None, true))
    };
    let (trusts_first, trusts_second) = (trusting(&first), trusting(&second));
    let redis = Redis::start().await;
    let mut runs = Vec::new();
    for (i, binary) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ]
    .iter()
    .enumerate()
    {
        let certs = runtime.join(format!("certs-{i}"));
        let (cert, key) = server_files(&certs, &first.server);
        // Each file moves in at once, from beside the watched directory.
        let staging = runtime.join(format!("staging-{i}"));
        std::fs::create_dir_all(&staging).unwrap();
        let replace = |name: &str, contents: &str| {
            std::fs::write(staging.join(name), contents).unwrap();
            std::fs::rename(staging.join(name), certs.join(name)).unwrap();
        };
        let (grpc, http, debug) = (reserved_port(), reserved_port(), reserved_port());
        let mut environment = binary_environment(&runtime, &redis);
        environment.retain(|(k, _)| *k != "LOG_LEVEL");
        environment.extend([
            ("LOG_LEVEL", "info".to_string()),
            ("GRPC_HOST", "127.0.0.1".into()),
            ("GRPC_PORT", grpc.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", http.to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", debug.to_string()),
            ("GRPC_SERVER_USE_TLS", "true".into()),
            ("GRPC_SERVER_TLS_CERT", cert.clone()),
            ("GRPC_SERVER_TLS_KEY", key.clone()),
        ]);
        let service = Logged::start(binary, &environment, http).await;
        let probes = || async {
            vec![
                tls_probe(grpc, trusts_first()).await,
                tls_probe(grpc, trusts_second()).await,
            ]
        };
        let mut outcomes = vec![probes().await];
        // The second key, for a certificate not yet there: the first pair is kept.
        replace("server-key.pem", &second.server.1);
        service
            .wait_for("CertProvider failed to load TLS key pair", 1)
            .await;
        outcomes.push(probes().await);
        replace("server.pem", &second.server.0);
        service.wait_for("CertProvider reloaded cert", 2).await;
        outcomes.push(probes().await);
        replace("server.pem", "no PEM here");
        service
            .wait_for("CertProvider failed to load TLS key pair", 2)
            .await;
        outcomes.push(probes().await);
        let stderr = service.stop().await;
        let replace = [
            (runtime.to_string_lossy().into_owned(), "RUNTIME"),
            (format!("certs-{i}"), "certs"),
            (format!(":{grpc}'"), ":GRPC'"),
            (format!(":{http}'"), ":HTTP'"),
            (format!(":{debug}"), ":DEBUG"),
        ];
        runs.push((outcomes, logrus_lines(&stderr, &replace)));
    }
    assert_eq!(
        runs[0].0,
        [
            ["served", "refused"],
            ["served", "refused"],
            ["refused", "served"],
            ["refused", "served"],
        ]
    );
    assert_eq!(runs[1], runs[0]);
}

/// With `GRPC_CLIENT_TLS_CACERT`, upstream also accepts client certificates the system's roots
/// issued (here `SSL_CERT_FILE`, which Go reads on Linux); ours only those of that CA
/// (docs/parity.md §7).
#[cfg(target_os = "linux")]
#[tokio::test]
async fn grpc_clients_are_verified_by_their_ca_alone() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("grpc-tls-roots");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let system = pki(&runtime.join("system"));
    let pki = pki(&runtime.join("pki"));
    let (cert, key) = server_files(&runtime.join("certs"), &pki.server);
    let ca_pem = std::fs::read_to_string(&pki.ca).unwrap();
    let identity = |pki: &Pki| {
        (
            std::fs::read_to_string(&pki.client_cert).unwrap(),
            std::fs::read_to_string(&pki.client_key).unwrap(),
        )
    };
    let (client, public) = (identity(&pki), identity(&system));
    let redis = Redis::start().await;
    let mut outcomes = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let (grpc, http) = (reserved_port(), reserved_port());
        let mut environment = binary_environment(&runtime, &redis);
        environment.extend([
            ("GRPC_HOST", "127.0.0.1".to_string()),
            ("GRPC_PORT", grpc.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", http.to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", reserved_port().to_string()),
            ("SSL_CERT_FILE", system.ca.to_string_lossy().into_owned()),
            ("GRPC_SERVER_USE_TLS", "true".into()),
            ("GRPC_SERVER_TLS_CERT", cert.clone()),
            ("GRPC_SERVER_TLS_KEY", key.clone()),
            (
                "GRPC_CLIENT_TLS_CACERT",
                pki.ca.to_string_lossy().into_owned(),
            ),
        ]);
        let service = Logged::start(&binary, &environment, http).await;
        outcomes.push(vec![
            tls_probe(grpc, Some(tls_client_config(&ca_pem, Some(&client), true))).await,
            tls_probe(grpc, Some(tls_client_config(&ca_pem, Some(&public), true))).await,
        ]);
        service.stop().await;
    }
    assert_eq!(outcomes[0], ["served", "served"]);
    assert_eq!(outcomes[1], ["served", "refused"]);
}

/// Sends one command to `redis` and reads its reply's first line.
async fn redis_command(redis: &Redis, args: &[&str]) -> String {
    use tokio::io::AsyncWriteExt;
    let mut command = format!("*{}\r\n", args.len());
    for arg in args {
        command.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
    }
    let mut stream = tokio::net::TcpStream::connect(&redis.address)
        .await
        .unwrap();
    stream.write_all(command.as_bytes()).await.unwrap();
    let mut reply = String::new();
    BufReader::new(&mut stream)
        .read_line(&mut reply)
        .await
        .unwrap();
    reply
}

/// On SIGTERM, while a request is still being answered, the gRPC listener is already closed
/// (on TCP, over TLS, and on a Unix socket, whose file is removed), as `GracefulStop` closes
/// it before draining.
#[tokio::test]
async fn the_grpc_listener_closes_on_sigterm_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("grpc-stop");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let pki = pki(&runtime.join("pki"));
    let (cert, key) = server_files(&runtime.join("certs"), &pki.server);
    let ca = std::fs::read_to_string(&pki.ca).unwrap();
    let redis = Redis::start().await;
    for mode in ["tcp", "unix", "tls"] {
        let mut runs = Vec::new();
        for (i, binary) in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ]
        .iter()
        .enumerate()
        {
            let (grpc, http) = (reserved_port(), reserved_port());
            let socket = runtime.join(format!("{mode}-{i}.sock"));
            let mut environment = binary_environment(&runtime, &redis);
            environment.extend([
                ("GRPC_HOST", "127.0.0.1".to_string()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", http.to_string()),
                ("DEBUG_HOST", "127.0.0.1".into()),
                ("DEBUG_PORT", reserved_port().to_string()),
            ]);
            match mode {
                "unix" => environment.push(("GRPC_UDS", socket.to_string_lossy().into_owned())),
                "tls" => environment.extend([
                    ("GRPC_SERVER_USE_TLS", "true".to_string()),
                    ("GRPC_SERVER_TLS_CERT", cert.clone()),
                    ("GRPC_SERVER_TLS_KEY", key.clone()),
                ]),
                _ => {}
            }
            let service = Logged::start(binary, &environment, http).await;
            let mut client = match mode {
                "unix" => unix_client(&socket, None).await,
                "tls" => tls_grpc_client(grpc, tls_client_config(&ca, None, true)).await,
                _ => RateLimitServiceClient::connect(format!("http://127.0.0.1:{grpc}"))
                    .await
                    .unwrap(),
            };
            // A request held in Redis while the service stops, past grpc-go's 5 s drain.
            assert_eq!(
                redis_command(&redis, &["CLIENT", "PAUSE", "7000"]).await,
                "+OK\r\n"
            );
            let answered = tokio::spawn(async move {
                client
                    .should_rate_limit(request("llm", &[&[("user", "a")]]))
                    .await
                    .is_ok()
            });
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            let mut child = service.child;
            let pid = child.id().unwrap().to_string();
            std::process::Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let connects = match mode {
                "unix" => tokio::net::UnixStream::connect(&socket).await.is_ok(),
                _ => tokio::net::TcpStream::connect(("127.0.0.1", grpc))
                    .await
                    .is_ok(),
            };
            let draining = child.try_wait().unwrap().is_none();
            let status = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait())
                .await
                .expect("the service stops")
                .unwrap();
            runs.push((
                connects,
                draining,
                answered.await.unwrap(),
                status.success(),
                socket.exists(),
            ));
        }
        assert_eq!(
            runs[0],
            (false, true, true, true, false),
            "upstream on {mode}"
        );
        assert_eq!(runs[1], runs[0], "{mode}");
    }
}

use llm_ratelimit_proto::envoy::service::discovery::v3::{
    DiscoveryRequest, DiscoveryResponse,
    aggregated_discovery_service_server::{
        AggregatedDiscoveryService, AggregatedDiscoveryServiceServer,
    },
};
use llm_ratelimit_proto::ratelimit::config::ratelimit::v3 as rls_conf;

const RATE_LIMIT_CONFIG_TYPE: &str =
    "type.googleapis.com/ratelimit.config.ratelimit.v3.RateLimitConfig";

/// A stream an xDS client opened: its metadata (without the transport's own headers) and
/// the requests it sent.
#[derive(Debug, Clone, Default, PartialEq)]
struct XdsStream {
    metadata: Vec<(String, String)>,
    requests: Vec<DiscoveryRequest>,
}

#[derive(Default)]
struct XdsState {
    streams: Vec<XdsStream>,
    /// The latest stream's responses.
    responder: Option<tokio::sync::mpsc::UnboundedSender<Result<DiscoveryResponse, tonic::Status>>>,
}

/// A fake xDS management server: it records each ADS stream and answers with what the test
/// pushes on the latest one.
#[derive(Clone, Default)]
struct XdsServer(Arc<std::sync::Mutex<XdsState>>);

#[tonic::async_trait]
impl AggregatedDiscoveryService for XdsServer {
    type StreamAggregatedResourcesStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<DiscoveryResponse, tonic::Status>> + Send>,
    >;

    async fn stream_aggregated_resources(
        &self,
        request: tonic::Request<tonic::Streaming<DiscoveryRequest>>,
    ) -> Result<tonic::Response<Self::StreamAggregatedResourcesStream>, tonic::Status> {
        let metadata = request
            .metadata()
            .iter()
            .map(|entry| match entry {
                tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                    (k.to_string(), v.to_str().unwrap_or_default().to_string())
                }
                tonic::metadata::KeyAndValueRef::Binary(k, v) => (
                    k.to_string(),
                    String::from_utf8_lossy(&v.to_bytes().unwrap_or_default()).into_owned(),
                ),
            })
            .filter(|(k, _)| {
                ![
                    "te",
                    "content-type",
                    "user-agent",
                    "grpc-accept-encoding",
                    "grpc-timeout",
                ]
                .contains(&k.as_str())
            })
            .collect();
        // Sorted: grpc-go sends them in a Go map's random order.
        let mut metadata: Vec<(String, String)> = metadata;
        metadata.sort();
        let (responder, responses) = tokio::sync::mpsc::unbounded_channel();
        let index = {
            let mut state = self.0.lock().unwrap();
            state.streams.push(XdsStream {
                metadata,
                requests: Vec::new(),
            });
            state.responder = Some(responder);
            state.streams.len() - 1
        };
        let mut requests = request.into_inner();
        let state = self.0.clone();
        tokio::spawn(async move {
            while let Ok(Some(request)) = requests.message().await {
                state.lock().unwrap().streams[index].requests.push(request);
            }
        });
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::UnboundedReceiverStream::new(responses),
        )))
    }
}

impl XdsServer {
    /// Serves on `port` (over TLS with `tls`) for the rest of the test.
    async fn start(port: u16, tls: Option<tonic::transport::ServerTlsConfig>) -> Self {
        let server = Self::default();
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = tls {
            builder = builder.tls_config(tls).unwrap();
        }
        let router = builder.add_service(AggregatedDiscoveryServiceServer::new(server.clone()));
        tokio::spawn(router.serve_with_incoming(TcpListenerStream::new(listener)));
        server
    }

    fn streams(&self) -> Vec<XdsStream> {
        self.0.lock().unwrap().streams.clone()
    }

    /// Waits until the streams hold `count` requests in all.
    async fn wait_requests(&self, count: usize) -> Vec<XdsStream> {
        for _ in 0..400 {
            let streams = self.streams();
            if streams.iter().map(|s| s.requests.len()).sum::<usize>() >= count {
                return streams;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("fewer than {count} xDS requests: {:?}", self.streams());
    }

    /// Sends `response` (or ends with an error) on the latest stream.
    fn respond(&self, response: Result<DiscoveryResponse, tonic::Status>) {
        let state = self.0.lock().unwrap();
        let _ = state.responder.as_ref().expect("a stream").send(response);
    }

    /// Ends the latest stream with `status`, or with OK when `None`.
    fn end(&self, status: Option<tonic::Status>) {
        let mut state = self.0.lock().unwrap();
        let responder = state.responder.take().expect("a stream");
        if let Some(status) = status {
            let _ = responder.send(Err(status));
        }
    }
}

/// A response carrying `configs`, at `version` with `nonce`.
fn xds_response(
    version: &str,
    nonce: &str,
    configs: &[rls_conf::RateLimitConfig],
) -> DiscoveryResponse {
    DiscoveryResponse {
        version_info: version.into(),
        resources: configs
            .iter()
            .map(|c| prost_types::Any {
                type_url: RATE_LIMIT_CONFIG_TYPE.into(),
                value: prost::Message::encode_to_vec(c),
            })
            .collect(),
        type_url: RATE_LIMIT_CONFIG_TYPE.into(),
        nonce: nonce.into(),
        ..Default::default()
    }
}

/// A config of `domain` limiting `key` to `requests` per minute.
fn xds_config(name: &str, domain: &str, key: &str, requests: u32) -> rls_conf::RateLimitConfig {
    rls_conf::RateLimitConfig {
        name: name.into(),
        domain: domain.into(),
        descriptors: vec![rls_conf::RateLimitDescriptor {
            key: key.into(),
            rate_limit: Some(rls_conf::RateLimitPolicy {
                unit: rls_conf::RateLimitUnit::Minute as i32,
                requests_per_unit: requests,
                ..Default::default()
            }),
            ..Default::default()
        }],
    }
}

/// The ports of one xDS run of a binary.
#[derive(Clone)]
struct XdsRun {
    xds_port: u16,
    grpc: u16,
    http: u16,
}

impl XdsRun {
    /// Whether the HTTP port is open (gRPC listens).
    async fn serves(&self) -> bool {
        tokio::net::TcpStream::connect(("127.0.0.1", self.http))
            .await
            .is_ok()
    }

    async fn serving(&self) {
        for _ in 0..300 {
            if self.serves().await {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("the service does not serve");
    }

    async fn client(&self) -> RateLimitServiceClient<Channel> {
        RateLimitServiceClient::connect(format!("http://127.0.0.1:{}", self.grpc))
            .await
            .unwrap()
    }
}

/// What a run of `binary` with `environment` (and an xDS client of a server the scenario
/// starts) shows: the scenario's observations, the streams the server saw (protobuf-go's
/// random non-breaking space in its errors made a space), and the log lines (sorted, the
/// ports and `masked` paths masked). The service stops on SIGTERM unless `exits`.
async fn xds_run<F, Fut>(
    binary: &std::path::Path,
    environment: &[(&'static str, String)],
    masked: &[&std::path::Path],
    scenario: F,
) -> (Vec<String>, Vec<XdsStream>, Vec<String>)
where
    F: FnOnce(XdsRun) -> Fut,
    Fut: std::future::Future<Output = (Vec<String>, Option<XdsServer>, bool)>,
{
    let redis = Redis::start().await;
    let run = XdsRun {
        xds_port: reserved_port(),
        grpc: reserved_port(),
        http: reserved_port(),
    };
    let debug = reserved_port();
    let mut full = vec![
        ("CONFIG_TYPE", "GRPC_XDS_SOTW".to_string()),
        (
            "CONFIG_GRPC_XDS_SERVER_URL",
            format!("127.0.0.1:{}", run.xds_port),
        ),
        ("XDS_CLIENT_BACKOFF_INITIAL_INTERVAL", "100ms".into()),
        ("REDIS_SOCKET_TYPE", "tcp".into()),
        ("REDIS_URL", redis.address.clone()),
        ("USE_STATSD", "false".into()),
        ("LOG_LEVEL", "debug".into()),
        ("GRPC_HOST", "127.0.0.1".into()),
        ("GRPC_PORT", run.grpc.to_string()),
        ("HOST", "127.0.0.1".into()),
        ("PORT", run.http.to_string()),
        ("DEBUG_HOST", "127.0.0.1".into()),
        ("DEBUG_PORT", debug.to_string()),
    ];
    for (key, value) in environment {
        full.retain(|(k, _)| k != key);
        full.push((key, value.clone()));
    }
    let logged = Logged::spawn(binary, &full);
    let (observed, xds, exits) = scenario(run.clone()).await;
    let stderr = match exits {
        false => logged.stop().await,
        true => logged.exited().await,
    };
    let mut replace = vec![
        (format!("127.0.0.1:{}", run.xds_port), "XDS"),
        (redis.address.clone(), "REDIS"),
        (format!("127.0.0.1:{}", run.grpc), "GRPC"),
        (format!("127.0.0.1:{}", run.http), "HTTP"),
        (format!("127.0.0.1:{debug}"), "DEBUG"),
        ("\\u00a0".to_string(), " "),
        // prototext's random spaces.
        ("  ".to_string(), " "),
    ];
    for path in masked {
        replace.push((path.to_string_lossy().into_owned(), "DIR"));
    }
    let streams = xds
        .map(|xds| xds.streams())
        .unwrap_or_default()
        .into_iter()
        .map(|mut stream| {
            for request in &mut stream.requests {
                if let Some(detail) = &mut request.error_detail {
                    detail.message = detail.message.replace('\u{a0}', " ");
                }
            }
            stream
        })
        .collect();
    // The time until the reset.
    let resets = regex::Regex::new(r"seconds:\d+").unwrap();
    let mut lines: Vec<String> = logrus_lines(&stderr, &replace)
        .into_iter()
        .map(|l| resets.replace_all(&l, "seconds:N").into_owned())
        .collect();
    lines.sort();
    (observed, streams, lines)
}

/// Both binaries' [`xds_run`], which must agree.
async fn xds_runs<F, Fut>(
    environment: &[(&'static str, String)],
    masked: &[&std::path::Path],
    scenario: F,
) where
    F: Fn(XdsRun) -> Fut,
    Fut: std::future::Future<Output = (Vec<String>, Option<XdsServer>, bool)>,
{
    let upstream = xds_run(&upstream_binary(), environment, masked, &scenario).await;
    let ours = xds_run(
        std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
        environment,
        masked,
        &scenario,
    )
    .await;
    assert_eq!(ours.0, upstream.0, "observations");
    assert_eq!(ours.1, upstream.1, "streams");
    // Upstream's process may exit before its provider, or its debug server, logs that it
    // stops: those lines, when upstream has them, ours has, but are compared no further.
    let stopping = |l: &String| {
        l.contains("Stopping xDS client watch")
            || l.contains("grpc: the client connection is closing")
            || l.contains("Failed to start debug server")
    };
    let (mut ours, mut upstream) = (ours, upstream);
    let upstream_stopping: Vec<String> =
        upstream.2.iter().filter(|l| stopping(l)).cloned().collect();
    let ours_stopping: Vec<String> = ours.2.iter().filter(|l| stopping(l)).cloned().collect();
    for line in &upstream_stopping {
        assert!(ours_stopping.contains(line), "{line} only upstream's");
    }
    ours.2.retain(|l| !stopping(l));
    upstream.2.retain(|l| !stopping(l));
    let only = |a: &[String], b: &[String]| {
        let mut b = b.to_vec();
        a.iter()
            .filter(|l| match b.iter().position(|m| m == *l) {
                Some(i) => {
                    b.remove(i);
                    false
                }
                None => true,
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        (only(&ours.2, &upstream.2), only(&upstream.2, &ours.2)),
        (vec![], vec![]),
        "log lines only ours, only upstream's"
    );
}

/// The responses to `requests`, without their (time dependent) resets.
async fn xds_calls(run: &XdsRun, requests: &[RateLimitRequest]) -> Vec<String> {
    let mut client = run.client().await;
    let mut observed = Vec::new();
    for request in requests {
        let mut response = client
            .should_rate_limit(request.clone())
            .await
            .unwrap()
            .into_inner();
        for status in &mut response.statuses {
            status.duration_until_reset = None;
        }
        observed.push(format!("{response:?}"));
    }
    observed
}

/// A `Value` of `kind`.
fn pb_value(kind: Option<prost_types::value::Kind>) -> prost_types::Value {
    prost_types::Value { kind }
}

/// The xDS SotW provider as upstream's: no service until the server is up and has sent a
/// config; each response loaded (its metadata too) and ACKed, or NACKed with the loader's or
/// protobuf's error; a failed stream opened again after the backoff, the NACKed version
/// then reported as acknowledged; the node, its metadata and the headers sent; the stream
/// stopped on SIGTERM.
#[tokio::test]
async fn xds_configs_are_acked_like_upstream() {
    if !tools_available() {
        return;
    }
    use prost_types::value::Kind;
    let environment = [
        ("CONFIG_GRPC_XDS_NODE_ID", "node-1".to_string()),
        ("CONFIG_GRPC_XDS_NODE_METADATA", "a=b,c,d=e=f".into()),
        (
            "CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS",
            "X-One:1,two:2,token-bin:abc".into(),
        ),
        ("RESPONSE_DYNAMIC_METADATA", "true".into()),
    ];
    let mut first = xds_config("one", "llm", "user", 2);
    first.descriptors[0].rate_limit.as_mut().unwrap().unit = rls_conf::RateLimitUnit::Hour as i32;
    let nested = prost_types::Struct {
        fields: [
            (
                "list".to_string(),
                pb_value(Some(Kind::ListValue(prost_types::ListValue {
                    values: vec![pb_value(None), pb_value(Some(Kind::BoolValue(true)))],
                }))),
            ),
            ("unset".to_string(), pb_value(None)),
        ]
        .into(),
    };
    first.descriptors.push(rls_conf::RateLimitDescriptor {
        key: "meta".into(),
        rate_limit: Some(rls_conf::RateLimitPolicy {
            unit: rls_conf::RateLimitUnit::Hour as i32,
            requests_per_unit: 10,
            name: "named".into(),
            ..Default::default()
        }),
        metadata: Some(prost_types::Struct {
            fields: [
                (
                    "s".to_string(),
                    pb_value(Some(Kind::StringValue("x".into()))),
                ),
                ("n".to_string(), pb_value(Some(Kind::NumberValue(1.5)))),
                (
                    "nested".to_string(),
                    pb_value(Some(Kind::StructValue(nested))),
                ),
                (
                    "empty".to_string(),
                    pb_value(Some(Kind::StructValue(Default::default()))),
                ),
            ]
            .into(),
        }),
        ..Default::default()
    });
    first.descriptors.push(rls_conf::RateLimitDescriptor {
        key: "no_metadata".into(),
        rate_limit: Some(rls_conf::RateLimitPolicy {
            unit: rls_conf::RateLimitUnit::Hour as i32,
            requests_per_unit: 10,
            ..Default::default()
        }),
        metadata: Some(Default::default()),
        ..Default::default()
    });
    let mut bad_unit = xds_config("two", "llm", "user", 2);
    bad_unit.descriptors[0].rate_limit.as_mut().unwrap().unit = 17;
    let mut second = xds_config("three", "llm", "user", 1);
    second.descriptors[0].rate_limit.as_mut().unwrap().unit = rls_conf::RateLimitUnit::Hour as i32;
    let scenario = |run: XdsRun| {
        let (first, bad_unit, second) = (first.clone(), bad_unit.clone(), second.clone());
        async move {
            // The server is down at first: no service.
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            let mut observed = vec![format!("served early: {}", run.serves().await)];
            let xds = XdsServer::start(run.xds_port, None).await;
            xds.wait_requests(1).await;
            xds.respond(Ok(xds_response("v1", "n1", &[first])));
            run.serving().await;
            let user = request("llm", &[&[("user", "a")]]);
            let meta = request("llm", &[&[("meta", "x")], &[("no_metadata", "y")]]);
            observed
                .extend(xds_calls(&run, &[user.clone(), user.clone(), user.clone(), meta]).await);
            xds.respond(Ok(xds_response("v2", "n2", &[bad_unit])));
            xds.wait_requests(3).await;
            xds.respond(Ok(DiscoveryResponse {
                version_info: "v3".into(),
                nonce: "n3".into(),
                resources: vec![prost_types::Any {
                    type_url: "type.googleapis.com/foo.Bar".into(),
                    value: vec![],
                }],
                ..Default::default()
            }));
            xds.wait_requests(4).await;
            xds.respond(Ok(xds_response(
                "v4",
                "n4",
                &[
                    xds_config("a", "llm", "user", 1),
                    xds_config("b", "llm", "user", 1),
                ],
            )));
            xds.wait_requests(5).await;
            xds.end(Some(tonic::Status::unavailable("going away")));
            xds.wait_requests(6).await;
            xds.end(None);
            xds.wait_requests(7).await;
            xds.respond(Ok(xds_response("v5", "n5", &[second])));
            xds.wait_requests(8).await;
            // The ACK goes out once the service has taken the config, maybe not yet applied.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            observed.extend(xds_calls(&run, &[user.clone(), user]).await);
            (observed, Some(xds), false)
        }
    };
    xds_runs(&environment, &[], scenario).await;
}

/// A stream fails like upstream's: on a response over `XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES`
/// (opened again, nothing received), and before it opens on headers grpc-go refuses; the
/// service serves meanwhile with `FORCE_START_WITHOUT_INITIAL_CONFIG`.
#[tokio::test]
async fn xds_streams_fail_like_upstream() {
    if !tools_available() {
        return;
    }
    let environment = [
        ("XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES", "200".to_string()),
        ("FORCE_START_WITHOUT_INITIAL_CONFIG", "true".into()),
    ];
    xds_runs(&environment, &[], |run: XdsRun| async move {
        let xds = XdsServer::start(run.xds_port, None).await;
        run.serving().await;
        xds.wait_requests(1).await;
        let big = xds_config(&"n".repeat(300), "llm", "user", 1);
        xds.respond(Ok(xds_response("v1", "n1", &[big])));
        xds.wait_requests(2).await;
        xds.respond(Ok(xds_response(
            "v2",
            "n2",
            &[xds_config("small", "llm", "user", 1)],
        )));
        xds.wait_requests(3).await;
        (vec![], Some(xds), false)
    })
    .await;
    for headers in ["Bad Key:1", "x:caf\u{e9}"] {
        let environment = [
            (
                "CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS",
                headers.to_string(),
            ),
            ("FORCE_START_WITHOUT_INITIAL_CONFIG", "true".into()),
            // One failure, then a backoff the stop abandons.
            ("XDS_CLIENT_BACKOFF_INITIAL_INTERVAL", "1h".into()),
            ("XDS_CLIENT_BACKOFF_MAX_INTERVAL", "2h".into()),
        ];
        xds_runs(&environment, &[], |run: XdsRun| async move {
            let xds = XdsServer::start(run.xds_port, None).await;
            run.serving().await;
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            (vec![], Some(xds), false)
        })
        .await;
    }
}

/// `CONFIG_GRPC_XDS_SERVER_USE_TLS` as upstream's: the server verified against the system's
/// roots plus `CONFIG_GRPC_XDS_SERVER_TLS_CACERT`, by `CONFIG_GRPC_XDS_SERVER_TLS_SAN` if set
/// (else the URL's host), the client's key pair offered; a server failing the handshake is
/// dialled again silently.
#[tokio::test]
async fn xds_over_tls_like_upstream() {
    if !tools_available() {
        return;
    }
    let dir = temp_dir("xds-tls");
    let test_pki = pki(&dir);
    let (cert, key) = &test_pki.server;
    let ca_pem = std::fs::read_to_string(&test_pki.ca).unwrap();
    let ca = test_pki.ca.to_string_lossy().into_owned();
    let server_tls = |client_ca: bool| {
        let tls = tonic::transport::ServerTlsConfig::new()
            .identity(tonic::transport::Identity::from_pem(cert, key));
        match client_ca {
            true => tls.client_ca_root(tonic::transport::Certificate::from_pem(&ca_pem)),
            false => tls,
        }
    };
    let client_pair = [
        (
            "CONFIG_GRPC_XDS_CLIENT_TLS_CERT",
            test_pki.client_cert.to_string_lossy().into_owned(),
        ),
        (
            "CONFIG_GRPC_XDS_CLIENT_TLS_KEY",
            test_pki.client_key.to_string_lossy().into_owned(),
        ),
    ];
    // (variables, whether the server wants a client certificate, whether it is reached)
    type Variables = Vec<(&'static str, String)>;
    let cases: Vec<(Variables, bool, bool)> = vec![
        (
            vec![("CONFIG_GRPC_XDS_SERVER_TLS_CACERT", ca.clone())],
            false,
            true,
        ),
        // Not the server's name.
        (
            vec![
                ("CONFIG_GRPC_XDS_SERVER_TLS_CACERT", ca.clone()),
                ("CONFIG_GRPC_XDS_SERVER_TLS_SAN", "localhost".into()),
            ],
            false,
            false,
        ),
        (
            vec![
                ("CONFIG_GRPC_XDS_SERVER_TLS_CACERT", ca.clone()),
                ("CONFIG_GRPC_XDS_SERVER_TLS_SAN", "127.0.0.1".into()),
            ],
            false,
            true,
        ),
        // No root for the server's CA.
        (vec![], false, false),
        // No client certificate for a server wanting one, then one.
        (
            vec![("CONFIG_GRPC_XDS_SERVER_TLS_CACERT", ca.clone())],
            true,
            false,
        ),
        (
            [
                vec![("CONFIG_GRPC_XDS_SERVER_TLS_CACERT", ca.clone())],
                client_pair.to_vec(),
            ]
            .concat(),
            true,
            true,
        ),
    ];
    for (variables, client_ca, reached) in cases {
        let mut environment = vec![
            ("CONFIG_GRPC_XDS_SERVER_USE_TLS", "true".to_string()),
            ("FORCE_START_WITHOUT_INITIAL_CONFIG", "true".into()),
        ];
        environment.extend(variables);
        let tls = server_tls(client_ca);
        xds_runs(&environment, &[&dir], |run: XdsRun| {
            let tls = tls.clone();
            async move {
                let xds = XdsServer::start(run.xds_port, Some(tls)).await;
                run.serving().await;
                match reached {
                    true => {
                        xds.wait_requests(1).await;
                        xds.respond(Ok(xds_response(
                            "v1",
                            "n1",
                            &[xds_config("one", "llm", "user", 1)],
                        )));
                        xds.wait_requests(2).await;
                    }
                    false => tokio::time::sleep(std::time::Duration::from_millis(2500)).await,
                }
                (vec![], Some(xds), false)
            }
        })
        .await;
    }
}

/// Each response makes upstream read the settings' TLS files again (`settings.NewSettings`):
/// one gone, the service exits with its error.
#[tokio::test]
async fn xds_responses_reread_the_tls_files_like_upstream() {
    if !tools_available() {
        return;
    }
    let dir = temp_dir("xds-reread");
    let test_pki = pki(&dir);
    let (cert, key) = server_files(&dir.join("certs"), &test_pki.server);
    let ca = test_pki.ca.clone();
    let ca_pem = std::fs::read(&ca).unwrap();
    let message = format!(
        "failed to read file: {0}: open {0}: no such file or directory",
        ca.display()
    );
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        std::fs::write(&ca, &ca_pem).unwrap();
        let environment = [
            ("GRPC_SERVER_USE_TLS", "true".to_string()),
            ("GRPC_SERVER_TLS_CERT", cert.clone()),
            ("GRPC_SERVER_TLS_KEY", key.clone()),
            ("GRPC_CLIENT_TLS_CACERT", ca.to_string_lossy().into_owned()),
        ];
        let (ca, ca_pem) = (ca.clone(), ca_pem.clone());
        let (_, _, lines) = xds_run(&binary, &environment, &[], |run: XdsRun| async move {
            let xds = XdsServer::start(run.xds_port, None).await;
            xds.wait_requests(1).await;
            xds.respond(Ok(xds_response(
                "v1",
                "n1",
                &[xds_config("one", "llm", "user", 1)],
            )));
            run.serving().await;
            std::fs::remove_file(&ca).unwrap();
            xds.respond(Ok(xds_response(
                "v2",
                "n2",
                &[xds_config("two", "llm", "user", 1)],
            )));
            let _ = ca_pem;
            (vec![], Some(xds), true)
        })
        .await;
        assert!(
            lines.iter().any(|l| l.contains(&message)),
            "{}: {lines:#?}",
            binary.display()
        );
    }
}

/// `redis-cli` (or `valkey-cli`) for the server: its name with `-cli` for `-server`.
fn redis_cli() -> String {
    let server = redis_server();
    match server.strip_suffix("-server") {
        Some(prefix) => format!("{prefix}-cli"),
        None => "redis-cli".into(),
    }
}

/// A Redis cluster of three primaries, each with a third of the slots.
struct RedisCluster {
    nodes: Vec<Redis>,
    /// With TLS, each node's TLS port (the harness uses the plain ones).
    tls_ports: Vec<u16>,
    _dir: TempDir,
}

impl RedisCluster {
    async fn start() -> Self {
        Self::start_with(None, false).await
    }

    /// Over TLS too with `pki`: each node also on a TLS port (its certificate, for 127.0.0.1),
    /// its cluster bus over TLS, so that it announces its TLS port to TLS clients.
    /// With `replicas`, each primary also has a replica (the last three nodes).
    async fn start_with(pki: Option<&Pki>, replicas: bool) -> Self {
        let dir = temp_dir("cluster");
        let mut nodes = Vec::new();
        let mut tls_ports = Vec::new();
        while nodes.len() < if replicas { 6 } else { 3 } {
            // Every port reserved, the cluster bus's too (by default 10000 above the node's).
            let port = reserved_port();
            let config = dir.join(format!("nodes-{port}.conf"));
            let config = config.to_string_lossy().into_owned();
            let mut args = vec![
                "--cluster-enabled".to_string(),
                "yes".into(),
                "--cluster-config-file".into(),
                config,
                "--cluster-port".into(),
                reserved_port().to_string(),
            ];
            let tls_port = reserved_port();
            if let Some(pki) = pki {
                let (cert, key) = server_files(&dir.join(format!("tls-{port}")), &pki.node);
                args.extend(
                    [
                        "--tls-port",
                        &tls_port.to_string(),
                        "--tls-cert-file",
                        &cert,
                        "--tls-key-file",
                        &key,
                        "--tls-ca-cert-file",
                        &pki.ca.to_string_lossy(),
                        "--tls-cluster",
                        "yes",
                        "--tls-auth-clients",
                        "no",
                    ]
                    .map(String::from),
                );
            }
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            if let Some(node) = Redis::start_with(port, &args).await {
                nodes.push(node);
                tls_ports.push(tls_port);
            }
        }
        let addresses: Vec<String> = nodes.iter().map(|n| n.address.clone()).collect();
        let created = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            Command::new(redis_cli())
                .arg("--cluster")
                .arg("create")
                .args(&addresses)
                .args(["--cluster-replicas", if replicas { "1" } else { "0" }])
                .arg("--cluster-yes")
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .output(),
        )
        .await
        .expect("the cluster is created")
        .unwrap();
        assert!(
            created.status.success(),
            "{}",
            String::from_utf8_lossy(&created.stderr)
        );
        // Every node sees the cluster as up.
        for node in &nodes {
            for attempt in 0.. {
                let info: String = node
                    .client
                    .custom(
                        CustomCommand::new_static("CLUSTER", ClusterHash::Random, false),
                        vec!["INFO"],
                    )
                    .await
                    .unwrap();
                if info.contains("cluster_state:ok") {
                    break;
                }
                assert!(attempt < 500, "the cluster is not up: {info}");
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
        // Every node's slot ranges list their replica (the topology radix and fred read): the
        // nodes learn of the replicas seconds apart, and radix closes a replica's pool when its
        // refresh asks a node that hides it.
        if replicas {
            for node in &nodes {
                for attempt in 0.. {
                    let slots: Value = node
                        .client
                        .custom(
                            CustomCommand::new_static("CLUSTER", ClusterHash::Random, false),
                            vec!["SLOTS"],
                        )
                        .await
                        .unwrap();
                    let listed = match &slots {
                        Value::Array(sets) => sets
                            .iter()
                            .all(|set| matches!(set, Value::Array(set) if set.len() >= 4)),
                        _ => false,
                    };
                    if listed {
                        break;
                    }
                    // Up to 30 s: slow under the full suite's load.
                    assert!(
                        attempt < 1500,
                        "the replicas are not in the topology: {slots:?}"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            }
            // And each replica is in sync.
            for replica in &nodes[3..] {
                for attempt in 0.. {
                    let info = resp_command(&replica.address, &["INFO", "replication"]).await;
                    if info.contains("master_link_status:up") {
                        break;
                    }
                    assert!(attempt < 1500, "a replica does not sync: {info}");
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            }
        }
        Self {
            nodes,
            tls_ports,
            _dir: dir,
        }
    }

    /// [`seeds`](Self::seeds) on the TLS ports.
    fn tls_seeds(&self) -> String {
        format!(
            "127.0.0.1:{},127.0.0.1:{}",
            self.tls_ports[0], self.tls_ports[1]
        )
    }

    /// `REDIS_URL` for two of the nodes: the cluster finds the third.
    fn seeds(&self) -> String {
        format!("{},{}", self.nodes[0].address, self.nodes[1].address)
    }

    /// The primaries (the first three nodes).
    fn primaries(&self) -> &[Redis] {
        &self.nodes[..3]
    }

    async fn flush(&self) {
        for node in self.primaries() {
            let _: String = node
                .client
                .custom(
                    CustomCommand::new_static("FLUSHALL", ClusterHash::Random, false),
                    Vec::<String>::new(),
                )
                .await
                .unwrap();
        }
    }

    /// Every primary's keys, by node.
    async fn dump(&self) -> Vec<BTreeMap<String, (String, i64)>> {
        let mut dumps = Vec::new();
        for node in self.primaries() {
            dumps.push(node.dump().await);
        }
        dumps
    }
}

/// Waits for the next `window` (whole seconds: a minute, an hour) when less than `margin` is
/// left of this one: a test whose counters of that window must stay in one window, on both
/// binaries.
async fn clear_of_a_window_boundary(window: std::time::Duration, margin: std::time::Duration) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let left = std::time::Duration::from_secs(window.as_secs() - now.as_secs() % window.as_secs())
        - std::time::Duration::from_nanos(now.subsec_nanos().into());
    if left < margin {
        tokio::time::sleep(left + std::time::Duration::from_millis(5)).await;
    }
}

/// Waits for the next minute when less than 2 s is left of this one: requests whose hits on
/// `LLM_CONFIG`'s limit (2 a minute) must be counted in one window, on both binaries.
async fn clear_of_a_minute_boundary() {
    clear_of_a_window_boundary(
        std::time::Duration::from_secs(60),
        std::time::Duration::from_secs(2),
    )
    .await;
}

/// Waits for the start of the next second of the system's clock.
async fn at_a_new_second() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let left = std::time::Duration::from_secs(1)
        - std::time::Duration::from_nanos(now.subsec_nanos().into());
    tokio::time::sleep(left + std::time::Duration::from_millis(5)).await;
}

/// What a run of `binary` on `cluster` shows: three requests' answers (for keys spread over the
/// nodes), the hour's counters on each node, the pools' stats and the log lines (`masked` and
/// the ports masked).
async fn cluster_run(
    binary: &std::path::Path,
    cluster: &RedisCluster,
    variables: &[(&'static str, String)],
    masked: &[(String, &str)],
) -> (
    Vec<String>,
    Vec<Vec<String>>,
    BTreeMap<String, u64>,
    Vec<String>,
) {
    const CONFIG: &[u8] = b"domain: llm\ndescriptors:\n  - key: user\n    rate_limit: {unit: hour, requests_per_unit: 2}\n  - key: burst\n    rate_limit: {unit: second, requests_per_unit: 1000}\n";
    let runtime = temp_dir("cluster-runtime");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", CONFIG),
            ("current", b"->v1"),
        ],
    );
    let users = [
        "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi",
    ];
    cluster.flush().await;
    let statsd = StatsdServer::start().await;
    let (grpc, http, debug) = (reserved_port(), reserved_port(), reserved_port());
    let mut environment = [
        runtime_environment(&runtime, true),
        vec![
            ("REDIS_SOCKET_TYPE", "tcp".to_string()),
            ("REDIS_TYPE", "cluster".into()),
            ("EXPIRATION_JITTER_MAX_SECONDS", "0".into()),
            ("STATSD_HOST", "127.0.0.1".into()),
            ("STATSD_PORT", statsd.address.port().to_string()),
            ("STATS_FLUSH_INTERVAL", "100ms".into()),
            ("LOG_LEVEL", "warn".into()),
            ("GRPC_HOST", "127.0.0.1".into()),
            ("GRPC_PORT", grpc.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", http.to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", debug.to_string()),
        ],
    ]
    .concat();
    environment.extend(variables.iter().cloned());
    let logged = Logged::start(binary, &environment, http).await;
    let mut client = RateLimitServiceClient::connect(format!("http://127.0.0.1:{grpc}"))
        .await
        .unwrap();
    let descriptors: Vec<[(&str, &str); 1]> = users
        .iter()
        .flat_map(|u| [[("user", *u)], [("burst", *u)]])
        .collect();
    let descriptors: Vec<&[(&str, &str)]> = descriptors.iter().map(|d| d.as_slice()).collect();
    // The three requests within one second, so that each side counts the per-second limit
    // in a single window.
    at_a_new_second().await;
    let mut answers = Vec::new();
    for _ in 0..3 {
        let mut response = client
            .should_rate_limit(request("llm", &descriptors))
            .await
            .unwrap()
            .into_inner();
        for status in &mut response.statuses {
            status.duration_until_reset = None;
        }
        answers.push(format!("{response:?}"));
    }
    // The hour's counters only: the second's expire.
    let keys = cluster
        .dump()
        .await
        .into_iter()
        .map(|node| {
            node.into_iter()
                .filter(|(k, _)| k.starts_with("llm_user_"))
                .map(|(k, (v, _))| format!("{k}={v}"))
                .collect()
        })
        .collect();
    // Each pool full: a connection per node and per pool slot. radix opens a replica's pool
    // once its topology, refreshed every 5 s, holds it.
    let size = |name| {
        variables
            .iter()
            .rev()
            .find(|(k, _)| *k == name)
            .and_then(|(_, v)| v.parse::<u64>().ok())
    };
    let nodes = cluster.nodes.len() as u64;
    let full = [
        ("ratelimit.redis_pool.cx_active|g", size("REDIS_POOL_SIZE")),
        (
            "ratelimit.redis_per_second_pool.cx_active|g",
            size("REDIS_PERSECOND_POOL_SIZE"),
        ),
    ];
    let mut pools = BTreeMap::new();
    // Up to 30 s: a loaded machine can take several topology refreshes.
    for _ in 0..60 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        pools = stat_values(&statsd.lines.lock().unwrap(), |name| {
            name.contains("_pool.")
        });
        if full.iter().all(|(gauge, size)| {
            !pools.contains_key(*gauge) || size.is_none_or(|size| pools[*gauge] == size * nodes)
        }) {
            break;
        }
    }
    let stderr = logged.stop().await;
    let mut masked = masked.to_vec();
    masked.extend([
        (format!("127.0.0.1:{grpc}"), "GRPC"),
        (format!("127.0.0.1:{http}"), "HTTP"),
        (format!("127.0.0.1:{debug}"), "DEBUG"),
    ]);
    (answers, keys, pools, logrus_lines(&stderr, &masked))
}

/// [`cluster_run`] of both binaries, which must agree; the limit applies and the keys spread
/// over the nodes.
async fn cluster_runs(
    cluster: &RedisCluster,
    variables: &[(&'static str, String)],
    masked: &[(String, &str)],
) {
    // Both binaries' counters of an hour, their pools filling: a minute or two.
    clear_of_a_window_boundary(
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(180),
    )
    .await;
    let upstream = cluster_run(&upstream_binary(), cluster, variables, masked).await;
    let ours = cluster_run(
        std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
        cluster,
        variables,
        masked,
    )
    .await;
    assert!(upstream.0[2].contains("OverLimit"), "{:?}", upstream.0);
    assert!(
        upstream.1.iter().filter(|node| !node.is_empty()).count() > 1,
        "the keys spread over the nodes: {:?}",
        upstream.1
    );
    assert_eq!(ours.0, upstream.0, "answers");
    assert_eq!(ours.1, upstream.1, "keys by node");
    assert_eq!(ours.2, upstream.2, "pool stats");
    assert_eq!(ours.3, upstream.3, "log lines");
}

/// `REDIS_TYPE=CLUSTER` (and `REDIS_PERSECOND_TYPE`) as upstream's: the cluster found from two
/// of its nodes, each request's keys on their slots' nodes, a pool per node, replicas included
/// (`cx_*` counting every node's connections), and upstream's startup lines (the pipeline window, the
/// parallelism, the URLs).
#[tokio::test]
async fn a_redis_cluster_serves_like_upstream() {
    if !tools_available() {
        return;
    }
    // With replicas, which radix pools too.
    let cluster = RedisCluster::start_with(None, true).await;
    let variables = [
        ("REDIS_URL", cluster.seeds()),
        ("REDIS_POOL_SIZE", "3".into()),
        ("REDIS_PIPELINE_WINDOW", "1ms".into()),
        ("REDIS_CLUSTER_PIPELINE_PARALLELISM", "0".into()),
        ("REDIS_PERSECOND", "true".into()),
        ("REDIS_PERSECOND_SOCKET_TYPE", "tcp".into()),
        ("REDIS_PERSECOND_TYPE", "CLUSTER".into()),
        ("REDIS_PERSECOND_URL", cluster.nodes[2].address.clone()),
        ("REDIS_PERSECOND_POOL_SIZE", "2".into()),
        ("REDIS_PERSECOND_CLUSTER_PIPELINE_PARALLELISM", "5".into()),
    ];
    let masked = [
        (cluster.seeds(), "SEEDS"),
        (cluster.seeds().replace(',', " "), "SEED SEED"),
        (cluster.nodes[2].address.clone(), "NODE"),
    ];
    cluster_runs(&cluster, &variables, &masked).await;
}

/// A cluster over TLS (`REDIS_TLS`): every node, found from the seeds, dialled over TLS and
/// verified by its address, as upstream's.
#[tokio::test]
async fn a_redis_cluster_over_tls_serves_like_upstream() {
    if !tools_available() {
        return;
    }
    let dir = temp_dir("cluster-pki");
    let test_pki = pki(&dir);
    let cluster = RedisCluster::start_with(Some(&test_pki), false).await;
    let variables = [
        ("REDIS_URL", cluster.tls_seeds()),
        ("REDIS_TLS", "true".into()),
        (
            "REDIS_TLS_CACERT",
            test_pki.ca.to_string_lossy().into_owned(),
        ),
        ("REDIS_POOL_SIZE", "2".into()),
    ];
    let masked = [
        (cluster.tls_seeds(), "SEEDS"),
        (cluster.tls_seeds().replace(',', " "), "SEED SEED"),
    ];
    cluster_runs(&cluster, &variables, &masked).await;
}

/// A cluster node down at startup (in the topology, but refusing connections): its dial's
/// error, then the retry naming it, as upstream logs them.
#[tokio::test]
async fn a_cluster_node_down_at_startup_is_logged_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("cluster-down");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let mut cluster = RedisCluster::start().await;
    let down = cluster.nodes.remove(2);
    let down_address = down.address.clone();
    drop(down);
    let environment = vec![
        (
            "RUNTIME_ROOT",
            runtime.join("current").to_string_lossy().into_owned(),
        ),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("REDIS_SOCKET_TYPE", "tcp".into()),
        ("REDIS_TYPE", "cluster".into()),
        ("REDIS_URL", cluster.seeds()),
        ("REDIS_POOL_SIZE", "2".into()),
        ("REDIS_STARTUP_INITIAL_INTERVAL", "50ms".into()),
        ("REDIS_STARTUP_MAX_INTERVAL", "100ms".into()),
        ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into()),
        ("DISABLE_STATS", "true".into()),
        ("LOG_LEVEL", "warn".into()),
    ];
    let waits = regex::Regex::new(r"in [^ ]+ \(elapsed: [^)]+\)").unwrap();
    let lines = |stderr: &str| {
        let mut lines: Vec<String> = logrus_lines(stderr, &[(down_address.clone(), "DOWN")])
            .into_iter()
            // The panic (our fatal line, §7) and the dials radix cancels (§7) aside.
            .filter(|l| l.starts_with("time=") && !l.contains("level=fatal"))
            .filter(|l| !l.contains("operation was canceled") && !l.contains("context canceled"))
            .map(|l| waits.replace(&l, "in WAIT (elapsed: ELAPSED)").into_owned())
            .collect();
        lines.dedup();
        lines
    };
    let upstream = lines(&startup_failure(&upstream_binary(), &environment).await);
    let ours = lines(
        &startup_failure(
            std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
            &environment,
        )
        .await,
    );
    assert!(
        upstream
            .iter()
            .any(|l| l.contains("error creating client for \"DOWN\"")),
        "{upstream:?}"
    );
    assert_eq!(ours, upstream);
}

/// A primary, its replica and a sentinel watching them (`mymaster`, quorum 1).
struct RedisSentinel {
    nodes: Vec<Redis>,
    _sentinel: Child,
    sentinel_address: String,
    /// The sentinel's `requirepass`, if any.
    password: Option<String>,
    _dir: TempDir,
}

impl RedisSentinel {
    async fn start() -> Self {
        Self::start_with(None).await
    }

    /// With `password`, the sentinel requires it.
    async fn start_with(password: Option<&str>) -> Self {
        let dir = temp_dir("sentinel");
        let mut primary = None;
        for _ in 0..5 {
            primary = Redis::start_with(reserved_port(), &[]).await;
            if primary.is_some() {
                break;
            }
        }
        let primary = primary.expect("the primary starts");
        let (host, port) = primary.address.split_once(':').unwrap();
        let mut replica = None;
        for _ in 0..5 {
            replica = Redis::start_with(reserved_port(), &["--replicaof", host, port]).await;
            if replica.is_some() {
                break;
            }
        }
        let replica = replica.expect("the replica starts");
        let port = reserved_port();
        let config = dir.join("sentinel.conf");
        let (host, primary_port) = primary.address.split_once(':').unwrap();
        std::fs::write(
            &config,
            format!(
                "port {port}\nsentinel monitor mymaster {host} {primary_port} 1\nsentinel down-after-milliseconds mymaster 1000\nsentinel failover-timeout mymaster 3000\n{}",
                password.map_or(String::new(), |p| format!("requirepass {p}\n")),
            ),
        )
        .unwrap();
        let sentinel = Command::new(redis_server())
            .arg(&config)
            .arg("--sentinel")
            .stdout(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start the sentinel");
        let sentinel = Self {
            nodes: vec![primary, replica],
            _sentinel: sentinel,
            sentinel_address: format!("127.0.0.1:{port}"),
            password: password.map(String::from),
            _dir: dir,
        };
        // The replica is in sync.
        for attempt in 0.. {
            let info = resp_command(&sentinel.nodes[1].address, &["INFO", "replication"]).await;
            if info.contains("master_link_status:up") {
                break;
            }
            assert!(attempt < 300, "the replica does not sync: {info}");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        // The sentinel knows the replica.
        for attempt in 0.. {
            let replicas = sentinel.ask(&["SENTINEL", "REPLICAS", "mymaster"]).await;
            if replicas.contains(
                &sentinel.nodes[1]
                    .address
                    .split(':')
                    .nth(1)
                    .unwrap()
                    .to_string(),
            ) {
                break;
            }
            assert!(
                attempt < 300,
                "the sentinel does not see the replica: {replicas}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        sentinel
    }

    /// A command to the sentinel (authenticated, if it wants a password): its reply.
    async fn ask(&self, args: &[&str]) -> String {
        match &self.password {
            None => resp_command(&self.sentinel_address, args).await,
            Some(password) => resp_commands(&self.sentinel_address, &[&["AUTH", password], args])
                .await
                .pop()
                .unwrap(),
        }
    }

    /// The primary's address, as the sentinel names it.
    async fn primary(&self) -> String {
        let reply = self
            .ask(&["SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"])
            .await;
        let parts: Vec<&str> = reply
            .lines()
            .filter(|l| !l.starts_with(['*', '$']))
            .collect();
        format!("{}:{}", parts[0], parts[1])
    }

    /// A failover, done once the sentinel names the other node and it takes writes. Tried
    /// again while the sentinel refuses it, or gives it up (`-failover-abort-no-good-slave`:
    /// the last failover's old primary is not yet a replica, which takes it about 10 s).
    async fn failover(&self) {
        let before = self.primary().await;
        for attempt in 0..30 {
            let reply = self.ask(&["SENTINEL", "FAILOVER", "mymaster"]).await;
            if reply.starts_with("+OK") {
                for _ in 0..50 {
                    let primary = self.primary().await;
                    if primary != before
                        && resp_command(&primary, &["ROLE"]).await.contains("master")
                    {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
            assert!(attempt < 29, "no failover: {reply}");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}

/// A command to the server (a sentinel, or a node) at `address`, over a connection of its own:
/// its raw RESP reply, read whole.
async fn resp_command(address: &str, args: &[&str]) -> String {
    resp_commands(address, &[args]).await.pop().unwrap()
}

/// [`resp_command`] for several commands on one connection: each reply.
async fn resp_commands(address: &str, commands: &[&[&str]]) -> Vec<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut command = String::new();
    for args in commands {
        command.push_str(&format!("*{}\r\n", args.len()));
        for arg in *args {
            command.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
    }
    // It may still be starting.
    let mut stream = None;
    for _ in 0..250 {
        let connect = tokio::net::TcpStream::connect(address);
        if let Ok(Ok(connected)) =
            tokio::time::timeout(std::time::Duration::from_secs(1), connect).await
        {
            stream = Some(connected);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let mut stream = stream.expect("the server listens");
    stream.write_all(command.as_bytes()).await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut reply = Vec::new();
    // The replies, each whole.
    let lengths = |bytes: &[u8]| {
        let mut lengths = Vec::new();
        let mut at = 0;
        while lengths.len() < commands.len() {
            let length = resp_length(&bytes[at..])?;
            lengths.push(length);
            at += length;
        }
        Some(lengths)
    };
    let lengths = loop {
        if let Some(lengths) = lengths(&reply) {
            break lengths;
        }
        let mut chunk = [0; 65536];
        let read = tokio::time::timeout_at(deadline, stream.read(&mut chunk))
            .await
            .expect("whole replies")
            .unwrap();
        assert!(read > 0, "the connection closed mid-reply");
        reply.extend_from_slice(&chunk[..read]);
    };
    let mut at = 0;
    lengths
        .into_iter()
        .map(|length| {
            let text = String::from_utf8_lossy(&reply[at..at + length]).into_owned();
            at += length;
            text
        })
        .collect()
}

/// The length of the RESP value at the start of `bytes`, once it is all there.
fn resp_length(bytes: &[u8]) -> Option<usize> {
    let end = bytes.windows(2).position(|w| w == b"\r\n")?;
    let line = std::str::from_utf8(&bytes[1..end]).ok()?;
    let after = end + 2;
    match bytes.first()? {
        b'$' => match line.parse::<i64>().ok()? {
            -1 => Some(after),
            n => (bytes.len() >= after + n as usize + 2).then_some(after + n as usize + 2),
        },
        b'*' => {
            let mut at = after;
            for _ in 0..line.parse::<i64>().ok()?.max(0) {
                at += resp_length(&bytes[at..])?;
            }
            Some(at)
        }
        _ => Some(after),
    }
}

/// `REDIS_TYPE=SENTINEL` as upstream's: the primary the sentinel names, a pool on it and on
/// its replica (both in `cx_active`), and after a failover, the new primary within radix's
/// 5 s, the counters going on.
#[test]
fn a_redis_sentinel_serves_like_upstream() {
    if !tools_available() {
        return;
    }
    // A test thread's 2 MiB stack is too small for it in a debug build.
    let run = std::thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(sentinel_serves_like_upstream())
        })
        .unwrap();
    if let Err(panic) = run.join() {
        std::panic::resume_unwind(panic);
    }
}

async fn sentinel_serves_like_upstream() {
    // Both binaries' counters of an hour, a failover each: a few minutes at most.
    clear_of_a_window_boundary(
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(300),
    )
    .await;
    let runtime = temp_dir("sentinel-runtime");
    write_tree(
        &runtime,
        &[
            (
                "v1/ratelimit/config/llm.yaml",
                b"domain: llm\ndescriptors:\n  - key: user\n    rate_limit: {unit: hour, requests_per_unit: 4}\n  - key: burst\n    rate_limit: {unit: second, requests_per_unit: 1000}\n",
            ),
            ("current", b"->v1"),
        ],
    );
    let topology = RedisSentinel::start().await;
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        // Over a connection of its own: a failover drops the harness's.
        resp_command(&topology.primary().await, &["FLUSHALL"]).await;
        let statsd = StatsdServer::start().await;
        let (grpc, http, debug) = (reserved_port(), reserved_port(), reserved_port());
        let environment = [
            runtime_environment(&runtime, true),
            vec![
                ("REDIS_SOCKET_TYPE", "tcp".to_string()),
                ("REDIS_TYPE", "sentinel".into()),
                (
                    "REDIS_URL",
                    format!("mymaster,{}", topology.sentinel_address),
                ),
                ("REDIS_POOL_SIZE", "2".into()),
                // The per-second pool too, on the same sentinel.
                ("REDIS_PERSECOND", "true".into()),
                ("REDIS_PERSECOND_SOCKET_TYPE", "tcp".into()),
                ("REDIS_PERSECOND_TYPE", "SENTINEL".into()),
                (
                    "REDIS_PERSECOND_URL",
                    format!("mymaster,{}", topology.sentinel_address),
                ),
                ("REDIS_PERSECOND_POOL_SIZE", "1".into()),
                ("EXPIRATION_JITTER_MAX_SECONDS", "0".into()),
                ("STATSD_HOST", "127.0.0.1".into()),
                ("STATSD_PORT", statsd.address.port().to_string()),
                ("STATS_FLUSH_INTERVAL", "100ms".into()),
                ("LOG_LEVEL", "warn".into()),
                ("GRPC_HOST", "127.0.0.1".into()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", http.to_string()),
                ("DEBUG_HOST", "127.0.0.1".into()),
                ("DEBUG_PORT", debug.to_string()),
            ],
        ]
        .concat();
        let logged = Logged::start(&binary, &environment, http).await;
        let mut client = RateLimitServiceClient::connect(format!("http://127.0.0.1:{grpc}"))
            .await
            .unwrap();
        let mut answers = Vec::new();
        let mut ask = async |answers: &mut Vec<String>, user: &str| {
            let call =
                client.should_rate_limit(request("llm", &[&[("user", user)], &[("burst", user)]]));
            let mut response =
                match tokio::time::timeout(std::time::Duration::from_secs(5), call).await {
                    Ok(answer) => answer.map(|r| r.into_inner()),
                    Err(_) => Err(tonic::Status::deadline_exceeded("no answer in 5 s")),
                };
            if let Ok(response) = &mut response {
                for status in &mut response.statuses {
                    status.duration_until_reset = None;
                }
            }
            answers.push(format!("{response:?}"));
        };
        // Each group of answers in one second: the burst limit's remaining counts it.
        at_a_new_second().await;
        for _ in 0..2 {
            ask(&mut answers, "a").await;
        }
        topology.failover().await;
        // radix asks the sentinels every 5 s.
        tokio::time::sleep(std::time::Duration::from_secs(7)).await;
        // The failover closed the new primary's connections: radix fails a command on each one
        // its PING has not found yet (docs/parity.md §7), so other counters warm the pools up.
        // Twice, a ping cycle apart (5 s for the one-connection pool): three answers in a row
        // can all go over one connection while another is still dead.
        for round in 0..2 {
            if round == 1 {
                tokio::time::sleep(std::time::Duration::from_secs(6)).await;
            }
            let mut in_a_row = 0;
            for _ in 0..20 {
                let mut warm = Vec::new();
                ask(&mut warm, "warm").await;
                in_a_row = if warm[0].starts_with("Ok") {
                    in_a_row + 1
                } else {
                    0
                };
                if in_a_row == 3 {
                    break;
                }
            }
            assert_eq!(in_a_row, 3, "no answer from the new primary");
        }
        at_a_new_second().await;
        for _ in 0..3 {
            ask(&mut answers, "a").await;
        }
        // Over a connection of its own: a failover drops the harness's.
        let primary = topology.primary().await;
        let mut keys = Vec::new();
        // User a's hour counters only: the second's are named after their second.
        for key in resp_bulk_strings(&resp_command(&primary, &["KEYS", "llm_user_a_*"]).await) {
            let value = resp_bulk_strings(&resp_command(&primary, &["GET", &key]).await);
            keys.push(format!("{key}={}", value.join("")));
        }
        keys.sort();
        let active = stat_values(&statsd.lines.lock().unwrap(), |name| {
            name.ends_with("_pool.cx_active")
        });
        let stderr = logged.stop().await;
        let lines = logrus_lines(
            &stderr,
            &[
                (topology.sentinel_address.clone(), "SENTINEL"),
                (format!("127.0.0.1:{grpc}"), "GRPC"),
                (format!("127.0.0.1:{http}"), "HTTP"),
                (format!("127.0.0.1:{debug}"), "DEBUG"),
            ],
        );
        runs.push((answers, keys, active, lines));
    }
    assert!(runs[0].0[4].contains("OverLimit"), "{:?}", runs[0].0);
    assert_eq!(runs[1].0, runs[0].0, "answers");
    assert_eq!(runs[1].1, runs[0].1, "keys");
    assert_eq!(runs[1].2, runs[0].2, "connections");
    assert_eq!(runs[1].3, runs[0].3, "log lines");
}

/// `SENTINEL` startup failures, retried and logged as upstream: an unknown primary, an
/// unreachable sentinel, the sentinel auth refused (its dialer's line at each attempt); a URL
/// without sentinels is refused at once.
#[tokio::test]
async fn sentinel_startup_is_logged_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("sentinel-logs");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let topology = RedisSentinel::start().await;
    let sentinel = &topology.sentinel_address;
    let waits = regex::Regex::new(r"in [^ ]+ \(elapsed: [^)]+\)").unwrap();
    let given_up = regex::Regex::new(r"(Expected master name|timed out waiting).*").unwrap();
    let after = regex::Regex::new(r"after [^:]+:").unwrap();
    let cases: Vec<(String, &str)> = vec![
        (format!("nosuch,{sentinel}"), ""),
        ("mymaster,127.0.0.1:1".into(), ""),
        (format!("mymaster,{sentinel}"), "u:pw"),
        (format!("mymaster,{sentinel}"), "pw"),
        ("mymaster".into(), ""),
    ];
    for (url, sentinel_auth) in cases {
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_TYPE", "SENTINEL".into()),
            ("REDIS_URL", url.clone()),
            ("REDIS_SENTINEL_AUTH", sentinel_auth.into()),
            ("REDIS_STARTUP_INITIAL_INTERVAL", "50ms".into()),
            ("REDIS_STARTUP_MAX_INTERVAL", "100ms".into()),
            ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into()),
            ("DISABLE_STATS", "true".into()),
            ("LOG_LEVEL", "warn".into()),
        ];
        let lines = |stderr: &str| {
            let mut lines: Vec<String> = logrus_lines(stderr, &[(sentinel.clone(), "SENTINEL")])
                .into_iter()
                .filter(|l| l.starts_with("time=") && !l.contains("level=fatal"))
                .map(|l| waits.replace(&l, "in WAIT (elapsed: ELAPSED)").into_owned())
                .collect();
            lines.dedup();
            lines
        };
        let upstream = startup_failure(&upstream_binary(), &environment).await;
        let ours = startup_failure(
            std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
            &environment,
        )
        .await;
        assert_eq!(lines(&ours), lines(&upstream), "{url} {sentinel_auth}");
        // The reason it gives up: upstream's panic, our fatal line.
        let reason = |stderr: &str| {
            given_up
                .find(stderr)
                .map(|m| {
                    let text = m.as_str().trim_end_matches('"').to_string();
                    after.replace(&text, "after ELAPSED:").into_owned()
                })
                .unwrap_or_default()
        };
        assert!(!reason(&upstream).is_empty(), "{upstream}");
        assert_eq!(reason(&ours), reason(&upstream), "{url} {sentinel_auth}");
    }
}

/// The bulk strings of a raw RESP reply, in order.
fn resp_bulk_strings(reply: &str) -> Vec<String> {
    let mut lines = reply.split("\r\n");
    let mut strings = Vec::new();
    while let Some(line) = lines.next() {
        if line.starts_with('$') && line != "$-1" {
            strings.extend(lines.next().map(String::from));
        }
    }
    strings
}

/// A sentinel with a password: `REDIS_SENTINEL_AUTH` reaches it while `REDIS_AUTH_FILE` stays
/// the nodes' (upstream's credential provider wraps only their dialer); and the first sentinel
/// URL's credentials come before another sentinel's own, as radix reads them.
#[tokio::test]
async fn a_password_protected_sentinel_is_used_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("sentinel-auth");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    // The nodes' default user takes any password.
    let auth_file = runtime.join("redis-auth");
    std::fs::write(&auth_file, "default:anything\n").unwrap();
    let topology = RedisSentinel::start_with(Some("sentpw")).await;
    let sentinel = topology.sentinel_address.clone();
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let (grpc, http) = (reserved_port(), reserved_port());
        let environment = [
            runtime_environment(&runtime, true),
            vec![
                ("REDIS_SOCKET_TYPE", "tcp".to_string()),
                ("REDIS_TYPE", "sentinel".into()),
                ("REDIS_URL", format!("mymaster,{sentinel}")),
                ("REDIS_SENTINEL_AUTH", "sentpw".into()),
                ("REDIS_AUTH_FILE", auth_file.to_string_lossy().into_owned()),
                ("DISABLE_STATS", "true".into()),
                ("LOG_LEVEL", "warn".into()),
                ("GRPC_HOST", "127.0.0.1".into()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", http.to_string()),
                ("DEBUG_HOST", "127.0.0.1".into()),
                ("DEBUG_PORT", reserved_port().to_string()),
            ],
        ]
        .concat();
        let logged = Logged::start(&binary, &environment, http).await;
        let mut client = RateLimitServiceClient::connect(format!("http://127.0.0.1:{grpc}"))
            .await
            .unwrap();
        let call = client.should_rate_limit(request("llm", &[&[("user", "a")]]));
        let answer = tokio::time::timeout(std::time::Duration::from_secs(5), call)
            .await
            .map(|r| r.map(|r| r.into_inner().overall_code));
        let stderr = logged.stop().await;
        let lines = logrus_lines(
            &stderr,
            &[
                (sentinel.clone(), "SENTINEL"),
                (auth_file.to_string_lossy().into_owned(), "AUTH_FILE"),
                (format!("127.0.0.1:{grpc}"), "GRPC"),
                (format!("127.0.0.1:{http}"), "HTTP"),
            ],
        );
        let lines: Vec<String> = lines.into_iter().filter(|l| !l.contains("debug")).collect();
        runs.push((format!("{answer:?}"), lines));
    }
    assert!(runs[0].0.contains("Ok(Ok(1))"), "{:?}", runs[0]);
    assert_eq!(runs[1], runs[0]);

    // The first URL's (wrong) password is the one sent, the other sentinel's own unused. Both
    // URLs name the same sentinel: radix tries them in Go's map order and reports the last
    // one's error, so each must fail the same way.
    let environment = vec![
        (
            "RUNTIME_ROOT",
            runtime.join("current").to_string_lossy().into_owned(),
        ),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("REDIS_SOCKET_TYPE", "tcp".into()),
        ("REDIS_TYPE", "sentinel".into()),
        (
            "REDIS_URL",
            format!(
                "mymaster,redis://:wrong@{sentinel},redis://:sentpw@{}",
                sentinel.replace("127.0.0.1", "localhost")
            ),
        ),
        ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into()),
        ("DISABLE_STATS", "true".into()),
        ("LOG_LEVEL", "warn".into()),
    ];
    let waits = regex::Regex::new(r"in [^ ]+ \(elapsed: [^)]+\)").unwrap();
    let lines = |stderr: &str| {
        let mut lines: Vec<String> = logrus_lines(stderr, &[(sentinel.clone(), "SENTINEL")])
            .into_iter()
            .filter(|l| l.starts_with("time=") && !l.contains("level=fatal"))
            .map(|l| waits.replace(&l, "in WAIT (elapsed: ELAPSED)").into_owned())
            .collect();
        lines.dedup();
        lines
    };
    let upstream = lines(&startup_failure(&upstream_binary(), &environment).await);
    let ours = lines(
        &startup_failure(
            std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
            &environment,
        )
        .await,
    );
    assert!(
        upstream.iter().any(|l| l.contains("WRONGPASS")),
        "{upstream:?}"
    );
    assert_eq!(ours, upstream);
}

/// A TCP proxy to a Redis server that records the commands its clients send.
struct RecordingProxy {
    address: String,
    commands: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
    accept: tokio::task::JoinHandle<()>,
}

impl RecordingProxy {
    async fn start(target: &str) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (target, sink) = (target.to_string(), commands.clone());
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let (target, sink) = (target.clone(), sink.clone());
                tokio::spawn(async move {
                    let Ok(server) = tokio::net::TcpStream::connect(&target).await else {
                        return;
                    };
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let replies = tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut server_read, &mut client_write).await;
                    });
                    let (mut pending, mut chunk) = (Vec::new(), [0u8; 4096]);
                    loop {
                        let n = match client_read.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        if server_write.write_all(&chunk[..n]).await.is_err() {
                            break;
                        }
                        pending.extend_from_slice(&chunk[..n]);
                        while let Some(length) = resp_length(&pending) {
                            let frame: Vec<u8> = pending.drain(..length).collect();
                            let command = resp_bulk_strings(&String::from_utf8_lossy(&frame));
                            sink.lock().unwrap().push(command);
                        }
                    }
                    replies.abort();
                });
            }
        });
        Self {
            address,
            commands,
            accept,
        }
    }

    /// The arguments of each `AUTH` so far.
    fn auths(&self) -> Vec<Vec<String>> {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .filter(|c| {
                c.first()
                    .is_some_and(|name| name.eq_ignore_ascii_case("AUTH"))
            })
            .map(|c| c[1..].to_vec())
            .collect()
    }

    /// Waits until `count` `AUTH`s have been sent.
    async fn wait_for_auths(&self, count: usize) {
        for _ in 0..200 {
            if self.auths().len() >= count {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("fewer than {count} AUTH: {:?}", self.auths());
    }
}

impl Drop for RecordingProxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// An ElastiCache IAM token without what changes with the time: the date and the signature.
fn timeless_token(token: &str) -> String {
    let date = regex::Regex::new(r"\d{8}T\d{6}Z").unwrap();
    let day = regex::Regex::new(r"%2F\d{8}%2F").unwrap();
    let signature = regex::Regex::new(r"X-Amz-Signature=[0-9a-f]{64}").unwrap();
    let token = date.replace_all(token, "DATE");
    let token = day.replace_all(&token, "%2FDAY%2F");
    signature
        .replace_all(&token, "X-Amz-Signature=SIGNATURE")
        .into_owned()
}

/// A fake of the AWS services credentials come from: STS (`AssumeRole`,
/// `AssumeRoleWithWebIdentity`), a container's endpoint and IMDSv2; it records each request.
struct FakeAws {
    url: String,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    accept: tokio::task::JoinHandle<()>,
}

impl FakeAws {
    async fn start() -> Self {
        Self::start_with(None).await
    }

    /// Over TLS with `pki`'s server certificate when given.
    async fn start_with(pki: Option<&Pki>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let scheme = if pki.is_some() { "https" } else { "http" };
        let url = format!("{scheme}://{}", listener.local_addr().unwrap());
        let acceptor = pki.map(|pki| {
            use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let config = rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    CertificateDer::pem_slice_iter(pki.server.0.as_bytes())
                        .map(Result::unwrap)
                        .collect(),
                    PrivateKeyDer::from_pem_slice(pki.server.1.as_bytes()).unwrap(),
                )
                .unwrap();
            tokio_rustls::TlsAcceptor::from(Arc::new(config))
        });
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = requests.clone();
        let accept = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (sink, acceptor) = (sink.clone(), acceptor.clone());
                tokio::spawn(async move {
                    match acceptor {
                        Some(acceptor) => {
                            if let Ok(stream) = acceptor.accept(stream).await {
                                Self::serve(stream, sink).await;
                            }
                        }
                        None => Self::serve(stream, sink).await,
                    }
                });
            }
        });
        Self {
            url,
            requests,
            accept,
        }
    }

    /// Answers one request on `stream`, and records it.
    async fn serve(
        mut stream: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
        sink: Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut request = Vec::new();
        let mut chunk = [0u8; 4096];
        let (head, body) = loop {
            let Ok(n) = stream.read(&mut chunk).await else {
                return;
            };
            if n == 0 {
                return;
            }
            request.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&request).into_owned();
            let Some((head, body)) = text.split_once("\r\n\r\n") else {
                continue;
            };
            let length = head
                .lines()
                .find_map(|l| {
                    let (name, value) = l.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if body.len() >= length {
                break (head.to_string(), body.to_string());
            }
        };
        let (status, response) = Self::answer(&head, &body);
        sink.lock().unwrap().push(format!("{head}\n\n{body}"));
        let reply = format!(
            // IMDS gives its tokens' lifetime.
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nX-Aws-Ec2-Metadata-Token-Ttl-Seconds: 21600\r\nConnection: close\r\n\r\n{response}",
            response.len()
        );
        let _ = stream.write_all(reply.as_bytes()).await;
        let _ = stream.shutdown().await;
    }

    fn answer(head: &str, body: &str) -> (&'static str, String) {
        let line = head.lines().next().unwrap_or_default();
        let sts = |action: &str, key: &str| {
            format!(
                "<{action}Response xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><{action}Result>\
                 <Credentials><AccessKeyId>{key}</AccessKeyId><SecretAccessKey>{key}-secret</SecretAccessKey>\
                 <SessionToken>{key}-session</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>\
                 <AssumedRoleUser><Arn>arn:aws:sts::1:assumed-role/r/s</Arn><AssumedRoleId>AROA:s</AssumedRoleId></AssumedRoleUser>\
                 </{action}Result><ResponseMetadata><RequestId>1</RequestId></ResponseMetadata></{action}Response>"
            )
        };
        let json = |key: &str| {
            format!(
                "{{\"Code\":\"Success\",\"Type\":\"AWS-HMAC\",\"LastUpdated\":\"2026-01-01T00:00:00Z\",\
                 \"AccessKeyId\":\"{key}\",\"SecretAccessKey\":\"{key}-secret\",\"Token\":\"{key}-session\",\
                 \"Expiration\":\"2099-01-01T00:00:00Z\"}}"
            )
        };
        match line.split(' ').take(2).collect::<Vec<_>>()[..] {
            ["POST", _] if body.contains("Action=AssumeRoleWithWebIdentity") => {
                ("200 OK", sts("AssumeRoleWithWebIdentity", "ASIAWEB"))
            }
            ["POST", _] if body.contains("Action=AssumeRole") => {
                ("200 OK", sts("AssumeRole", "ASIAROLE"))
            }
            ["GET", "/ecs-credentials"] => ("200 OK", json("ASIAECS")),
            // Credentials expiring in `/ecs-expiring/<seconds>`.
            ["GET", path] if path.starts_with("/ecs-expiring/") => {
                let seconds: i64 = path["/ecs-expiring/".len()..].parse().unwrap();
                let expires = jiff::Timestamp::now() + jiff::SignedDuration::from_secs(seconds);
                let expiration = expires.strftime("%Y-%m-%dT%H:%M:%SZ").to_string();
                (
                    "200 OK",
                    json("ASIAEXPIRING").replace("2099-01-01T00:00:00Z", &expiration),
                )
            }
            ["PUT", "/latest/api/token"] => ("200 OK", "imds-token".into()),
            ["GET", "/latest/meta-data/iam/security-credentials/"] => {
                ("200 OK", "imds-role".into())
            }
            [
                "GET",
                "/latest/meta-data/iam/security-credentials/imds-role",
            ] => ("200 OK", json("ASIAIMDS")),
            _ => ("404 Not Found", String::new()),
        }
    }

    /// The requests so far for `what` (a path or an STS action): the headers and parameters
    /// that the credentials depend on.
    fn requests(&self, what: &str) -> Vec<String> {
        let parameter = regex::Regex::new(
            r"(Action|RoleArn|RoleSessionName|WebIdentityToken|ExternalId|DurationSeconds)=[^&]*",
        )
        .unwrap();
        let header =
            regex::Regex::new(r"(?im)^(authorization|x-aws-ec2-metadata-token):.*$").unwrap();
        let key = regex::Regex::new(r"(?i)credential=([a-z0-9]+)/").unwrap();
        let digits = regex::Regex::new(r"\d{10,}").unwrap();
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.contains(what))
            .map(|r| {
                let mut seen: Vec<String> = parameter
                    .find_iter(r)
                    .map(|m| m.as_str().to_string())
                    // A session name by default is the time.
                    .map(|p| digits.replace_all(&p, "N").into_owned())
                    .collect();
                for h in header.find_iter(r) {
                    // Of a signature, only the key that signs.
                    seen.push(match key.captures(h.as_str()) {
                        Some(k) => format!("signed by {}", &k[1]),
                        None => h.as_str().trim().to_lowercase(),
                    });
                }
                seen.sort();
                seen.join(" ")
            })
            .collect()
    }
}

impl Drop for FakeAws {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// The variables of a service whose pools authenticate with ElastiCache IAM through `proxy`,
/// in `home`, with no IMDS unless the case sets it.
fn iam_environment(
    runtime: &std::path::Path,
    proxy: &RecordingProxy,
    home: &std::path::Path,
) -> Vec<(&'static str, String)> {
    let (grpc, http, debug) = (reserved_port(), reserved_port(), reserved_port());
    vec![
        (
            "RUNTIME_ROOT",
            runtime.join("current").to_string_lossy().into_owned(),
        ),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("REDIS_SOCKET_TYPE", "tcp".into()),
        ("REDIS_URL", proxy.address.clone()),
        ("REDIS_POOL_SIZE", "1".into()),
        ("REDIS_AWS_IAM_AUTH", "true".into()),
        ("REDIS_AWS_IAM_CACHE_NAME", "MyCache".into()),
        ("REDIS_AWS_IAM_USER_ID", "app".into()),
        ("HOME", home.to_string_lossy().into_owned()),
        ("AWS_EC2_METADATA_DISABLED", "true".into()),
        ("DISABLE_STATS", "true".into()),
        ("EXPIRATION_JITTER_MAX_SECONDS", "0".into()),
        ("LOG_LEVEL", "warn".into()),
        ("GRPC_HOST", "127.0.0.1".into()),
        ("GRPC_PORT", grpc.to_string()),
        ("HOST", "127.0.0.1".into()),
        ("PORT", http.to_string()),
        ("DEBUG_HOST", "127.0.0.1".into()),
        ("DEBUG_PORT", debug.to_string()),
    ]
}

/// The value of `name` in `environment` (the last one, which wins).
fn variable(environment: &[(&str, String)], name: &str) -> String {
    environment
        .iter()
        .rev()
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

/// A Redis server whose users `app` and `app2` take any password (any token).
async fn redis_with_iam_users() -> Redis {
    let redis = Redis::start().await;
    for user in ["app", "app2"] {
        resp_command(
            &redis.address,
            &["ACL", "SETUSER", user, "on", "nopass", "~*", "&*", "+@all"],
        )
        .await;
    }
    redis
}

/// `REDIS_AWS_IAM_AUTH` as upstream's: each pool authenticates as its user with a token
/// presigned for its cache (lowercased), its region (`*_AWS_IAM_REGION`, else the AWS
/// configuration's) and the AWS credentials (a session token included), serverless or not;
/// the same lines logged; a connection made again reuses the token.
#[tokio::test]
async fn elasticache_iam_tokens_authenticate_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("iam");
    write_tree(
        &runtime,
        &[
            (
                "v1/ratelimit/config/llm.yaml",
                b"domain: llm\ndescriptors:\n  - key: user\n    rate_limit: {unit: hour, requests_per_unit: 2}\n  - key: burst\n    rate_limit: {unit: second, requests_per_unit: 100}\n",
            ),
            ("current", b"->v1"),
        ],
    );
    let home = runtime.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let redis = redis_with_iam_users().await;
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        resp_command(&redis.address, &["FLUSHALL"]).await;
        let proxy = RecordingProxy::start(&redis.address).await;
        let mut environment = iam_environment(&runtime, &proxy, &home);
        environment.extend([
            ("AWS_REGION", "eu-west-3".to_string()),
            ("AWS_ACCESS_KEY_ID", "AKIDENV".into()),
            ("AWS_SECRET_ACCESS_KEY", "env-secret".into()),
            ("AWS_SESSION_TOKEN", "env/session+token=".into()),
            // The per-second pool: serverless, another user and region.
            ("REDIS_PERSECOND", "true".into()),
            ("REDIS_PERSECOND_SOCKET_TYPE", "tcp".into()),
            ("REDIS_PERSECOND_URL", proxy.address.clone()),
            ("REDIS_PERSECOND_POOL_SIZE", "1".into()),
            ("REDIS_PERSECOND_AWS_IAM_AUTH", "true".into()),
            ("REDIS_PERSECOND_AWS_IAM_CACHE_NAME", "Serverless-1".into()),
            ("REDIS_PERSECOND_AWS_IAM_USER_ID", "app2".into()),
            ("REDIS_PERSECOND_AWS_IAM_SERVERLESS", "true".into()),
            ("REDIS_PERSECOND_AWS_IAM_REGION", "us-east-2".into()),
        ]);
        let http: u16 = variable(&environment, "PORT").parse().unwrap();
        let logged = Logged::start(&binary, &environment, http).await;
        let mut client = RateLimitServiceClient::connect(format!(
            "http://127.0.0.1:{}",
            variable(&environment, "GRPC_PORT")
        ))
        .await
        .unwrap();
        let mut answers = Vec::new();
        for _ in 0..3 {
            let response = client
                .should_rate_limit(request("llm", &[&[("user", "a")], &[("burst", "b")]]))
                .await
                .map(|r| r.into_inner().overall_code);
            answers.push(format!("{response:?}"));
        }
        // Each user's tokens, then its connections made again.
        let tokens = |user: &str| -> Vec<String> {
            proxy
                .auths()
                .iter()
                .filter(|a| a.len() == 2 && a[0] == user)
                .map(|a| a[1].clone())
                .collect()
        };
        // A second later: a token made again would differ (its date).
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let before = proxy.auths().len();
        resp_command(&redis.address, &["CLIENT", "KILL", "TYPE", "normal"]).await;
        proxy.wait_for_auths(before + 2).await;
        let mut observed = Vec::new();
        for user in ["app", "app2"] {
            let tokens = tokens(user);
            let mut distinct = tokens.clone();
            distinct.dedup();
            observed.push(format!(
                "{user}: {} (one token: {})",
                timeless_token(&tokens[0]),
                distinct.len() == 1
            ));
        }
        let port = |name| format!("127.0.0.1:{}", variable(&environment, name));
        let lines = logrus_lines(
            &logged.stop().await,
            &[
                (proxy.address.clone(), "PROXY"),
                (port("GRPC_PORT"), "GRPC"),
                (port("PORT"), "HTTP"),
                (port("DEBUG_PORT"), "DEBUG"),
            ],
        );
        runs.push((answers, observed, lines));
    }
    // OVER_LIMIT at the third.
    assert_eq!(runs[0].0[2], "Ok(2)", "{:?}", runs[0].0);
    assert!(
        runs[0].1[0].starts_with("app: mycache/?Action=connect&User=app&X-Amz-Algorithm"),
        "{:?}",
        runs[0].1
    );
    assert_eq!(runs[1].0, runs[0].0, "answers");
    assert_eq!(runs[1].1, runs[0].1, "tokens");
    assert_eq!(runs[1].2, runs[0].2, "log lines");
}

/// Where the AWS credentials come from, as `config.LoadDefaultConfig` picks them: the
/// environment's keys (their aliases too), then a web identity, then the profile (`AWS_PROFILE`,
/// else `AWS_DEFAULT_PROFILE`, else `default`; from the config or the credentials file): its
/// keys, a role assumed from another profile's or from the container's credentials, a web
/// identity, a process; then the container's endpoint, then IMDS. The same token (its access
/// key, session token and region) and the same calls to STS and the container.
#[tokio::test]
async fn aws_credentials_are_found_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("iam-sources");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let home = runtime.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let path = |name: &str| runtime.join(name).to_string_lossy().into_owned();
    std::fs::write(path("web-token"), "web-identity-jwt").unwrap();
    std::fs::write(
        path("process.sh"),
        "#!/bin/sh\necho '{\"Version\": 1, \"AccessKeyId\": \"AKIDPROCESS\", \"SecretAccessKey\": \"process-secret\", \"SessionToken\": \"process-session\"}'\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path("process.sh"), std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(
        path("aws-credentials"),
        "[default]\naws_access_key_id = AKIDDEFAULT\naws_secret_access_key = default-secret\n\
         [keys2]\naws_access_key_id = AKIDKEYS2\naws_secret_access_key = keys2-secret\n",
    )
    .unwrap();
    std::fs::write(
        path("aws-config"),
        format!(
            "[profile keys]\naws_access_key_id = AKIDKEYS\naws_secret_access_key = keys-secret\nregion = ap-south-1\n\
             [profile process]\ncredential_process = {}\n\
             [profile role]\nrole_arn = arn:aws:iam::1:role/r\nsource_profile = keys\nrole_session_name = my-session\nexternal_id = ext-1\nduration_seconds = 3600\n\
             [profile web]\nrole_arn = arn:aws:iam::1:role/web\nweb_identity_token_file = {}\nrole_session_name = web-session\n\
             [profile from-container]\nrole_arn = arn:aws:iam::1:role/c\ncredential_source = EcsContainer\n",
            path("process.sh"),
            path("web-token")
        ),
    )
    .unwrap();
    let web = [
        ("AWS_WEB_IDENTITY_TOKEN_FILE", path("web-token")),
        ("AWS_ROLE_ARN", "arn:aws:iam::1:role/env-web".into()),
        ("AWS_ROLE_SESSION_NAME", "env-session".into()),
    ];
    let keys = [
        ("AWS_ACCESS_KEY_ID", "AKIDENV".to_string()),
        ("AWS_SECRET_ACCESS_KEY", "env-secret".into()),
    ];
    let cases: Vec<(&str, Vec<(&'static str, String)>)> = vec![
        (
            "the environment's aliases",
            vec![
                ("AWS_ACCESS_KEY", "AKIDALIAS".into()),
                ("AWS_SECRET_KEY", "alias-secret".into()),
            ],
        ),
        (
            "the environment before a web identity",
            [&keys[..], &web[..]].concat(),
        ),
        ("a web identity before the default profile", web.to_vec()),
        ("the default profile", vec![]),
        (
            "AWS_PROFILE, and its region",
            vec![
                ("AWS_PROFILE", "keys".into()),
                ("AWS_REGION", String::new()),
            ],
        ),
        (
            "AWS_DEFAULT_PROFILE",
            vec![("AWS_DEFAULT_PROFILE", "keys2".into())],
        ),
        // Run by `sh`, found on the `PATH`.
        (
            "a process",
            vec![
                ("AWS_PROFILE", "process".into()),
                ("PATH", "/bin:/usr/bin".into()),
            ],
        ),
        (
            "a role assumed from another profile",
            vec![("AWS_PROFILE", "role".into())],
        ),
        (
            "a profile's web identity",
            vec![("AWS_PROFILE", "web".into())],
        ),
        (
            "a role assumed from the container's",
            vec![
                ("AWS_PROFILE", "from-container".into()),
                (
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                    "FAKE/ecs-credentials".into(),
                ),
            ],
        ),
        (
            "the container's",
            vec![
                ("AWS_SHARED_CREDENTIALS_FILE", path("missing")),
                (
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                    "FAKE/ecs-credentials".into(),
                ),
                ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "container-auth".into()),
            ],
        ),
        (
            "IMDS",
            vec![
                ("AWS_SHARED_CREDENTIALS_FILE", path("missing")),
                ("AWS_EC2_METADATA_DISABLED", "false".into()),
                ("AWS_EC2_METADATA_SERVICE_ENDPOINT", "FAKE".into()),
            ],
        ),
    ];
    let redis = redis_with_iam_users().await;
    for (case, variables) in cases {
        let mut runs = Vec::new();
        for binary in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ] {
            let proxy = RecordingProxy::start(&redis.address).await;
            let aws = FakeAws::start().await;
            let mut environment = iam_environment(&runtime, &proxy, &home);
            environment.extend([
                ("AWS_REGION", "eu-west-3".to_string()),
                ("AWS_CONFIG_FILE", path("aws-config")),
                ("AWS_SHARED_CREDENTIALS_FILE", path("aws-credentials")),
                ("AWS_ENDPOINT_URL_STS", aws.url.clone()),
            ]);
            environment.extend(
                variables
                    .iter()
                    .map(|(k, v)| (*k, v.replace("FAKE", &aws.url))),
            );
            let http: u16 = variable(&environment, "PORT").parse().unwrap();
            let logged = Logged::start(&binary, &environment, http).await;
            proxy.wait_for_auths(1).await;
            let token = timeless_token(&proxy.auths()[0][1]);
            logged.stop().await;
            runs.push((
                token,
                aws.requests("Action="),
                aws.requests("/ecs-credentials"),
            ));
        }
        assert_eq!(runs[1], runs[0], "{case}");
    }
}

/// Credentials that expire: the container's are taken as expiring 5 minutes early, and a token
/// is not reused past a minute before they expire. Expiring in 4 minutes, each connection made
/// again asks the container and signs a new token; in 10, neither.
#[tokio::test]
async fn expiring_aws_credentials_are_refreshed_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("iam-expiring");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let home = runtime.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let redis = redis_with_iam_users().await;
    for seconds in [240, 600] {
        let mut runs = Vec::new();
        for binary in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ] {
            let proxy = RecordingProxy::start(&redis.address).await;
            let aws = FakeAws::start().await;
            let mut environment = iam_environment(&runtime, &proxy, &home);
            environment.extend([
                ("AWS_REGION", "eu-west-3".to_string()),
                (
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                    format!("{}/ecs-expiring/{seconds}", aws.url),
                ),
            ]);
            let http: u16 = variable(&environment, "PORT").parse().unwrap();
            let logged = Logged::start(&binary, &environment, http).await;
            proxy.wait_for_auths(1).await;
            let asked = aws.requests("/ecs-expiring").len();
            // A second later: a token made again would differ (its date).
            tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
            let before = proxy.auths().len();
            resp_command(&redis.address, &["CLIENT", "KILL", "TYPE", "normal"]).await;
            proxy.wait_for_auths(before + 1).await;
            // Tokens signed in the same second are the same: only the reconnection's tells.
            let auths = proxy.auths();
            let (earlier, again) = auths.split_at(before);
            runs.push(format!(
                "container asked again: {}, new token: {}",
                aws.requests("/ecs-expiring").len() > asked,
                again.iter().any(|a| earlier.iter().all(|e| e[1] != a[1]))
            ));
            logged.stop().await;
        }
        assert_eq!(runs[1], runs[0], "expiring in {seconds} s");
        assert_eq!(
            runs[0],
            match seconds {
                240 => "container asked again: true, new token: true",
                _ => "container asked again: false, new token: false",
            },
            "expiring in {seconds} s"
        );
    }
}

/// `AWS_CA_BUNDLE`: STS over HTTPS, its certificate from the bundle's CA, gives the
/// credentials of a web identity; without the bundle, the certificate is refused at each
/// connection. The container's endpoint is asked without the bundle, as Go's.
#[tokio::test]
async fn an_aws_ca_bundle_is_trusted_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("iam-ca-bundle");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let home = runtime.join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(runtime.join("web-token"), "web-identity-jwt").unwrap();
    let pki = pki(&runtime.join("pki"));
    let redis = redis_with_iam_users().await;
    let mut tokens = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let proxy = RecordingProxy::start(&redis.address).await;
        let aws = FakeAws::start_with(Some(&pki)).await;
        let base = iam_environment(&runtime, &proxy, &home);
        let bundle = ("AWS_CA_BUNDLE", pki.ca.to_string_lossy().into_owned());
        let web = [
            ("AWS_REGION", "eu-west-3".to_string()),
            ("AWS_ENDPOINT_URL_STS", aws.url.clone()),
            (
                "AWS_WEB_IDENTITY_TOKEN_FILE",
                runtime.join("web-token").to_string_lossy().into_owned(),
            ),
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/web".into()),
        ];
        let environment = [&base[..], &web[..], std::slice::from_ref(&bundle)].concat();
        let http: u16 = variable(&environment, "PORT").parse().unwrap();
        let logged = Logged::start(&binary, &environment, http).await;
        proxy.wait_for_auths(1).await;
        tokens.push(timeless_token(&proxy.auths()[0][1]));
        logged.stop().await;
        // Refused: STS without the bundle (the system's roots do not hold the test CA), and
        // the container's endpoint even with it.
        let gives_up = ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".to_string());
        let container = [
            ("AWS_REGION", "eu-west-3".to_string()),
            (
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                format!("{}/ecs-credentials", aws.url),
            ),
        ];
        for environment in [
            [&base[..], &web[..], std::slice::from_ref(&gives_up)].concat(),
            [
                &base[..],
                &container[..],
                &[bundle.clone(), gives_up.clone()],
            ]
            .concat(),
        ] {
            let failure = startup_failure(&binary, &environment).await;
            assert!(
                failure.contains("retrieving AWS credentials for ElastiCache IAM authentication: failed to refresh cached credentials, "),
                "{}: {failure}",
                binary.display()
            );
        }
    }
    assert!(tokens[0].contains("ASIAWEB"), "{tokens:?}");
    assert_eq!(tokens[1], tokens[0]);
}

/// The memcached server to run.
fn memcached_server() -> String {
    std::env::var("RATELIMIT_MEMCACHED_SERVER").unwrap_or_else(|_| "memcached".to_string())
}

/// Whether the Memcached scenarios can run: the differential tools and `memcached`.
fn memcached_available() -> bool {
    if !tools_available() {
        return false;
    }
    let found = std::process::Command::new(memcached_server())
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if !found {
        assert!(
            std::env::var_os("RATELIMIT_REQUIRE_DIFFERENTIAL").is_none(),
            "Memcached scenarios need `{}`",
            memcached_server()
        );
        eprintln!(
            "skipping: Memcached scenarios need `{}`",
            memcached_server()
        );
    }
    found
}

/// A memcached server on a port of its own.
struct Memcached {
    address: String,
    _process: Option<Child>,
}

impl Memcached {
    /// The server, once it answers and is the one started here (its port may be taken first by
    /// another test's server).
    async fn start() -> Self {
        for _ in 0..5 {
            let port = reserved_port();
            let mut process = Command::new(memcached_server())
                // memcached refuses to run as root without `-u`, which it ignores otherwise.
                .args([
                    "-p",
                    &port.to_string(),
                    "-l",
                    "127.0.0.1",
                    "-U",
                    "0",
                    "-u",
                    "root",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .expect("start memcached");
            let pid = format!("STAT pid {}\r\n", process.id().unwrap());
            let mut memcached = Self {
                address: format!("127.0.0.1:{port}"),
                _process: None,
            };
            for _ in 0..250 {
                if process.try_wait().unwrap().is_some() {
                    break;
                }
                if let Some(stats) = memcached.try_ask("stats\r\n").await {
                    if stats.contains(&pid) {
                        memcached._process = Some(process);
                        return memcached;
                    }
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
        panic!("could not start {}", memcached_server());
    }

    /// A text-protocol command, and its answer up to its last line (`END`, or a single one).
    async fn ask(&self, command: &str) -> String {
        self.try_ask(command).await.expect("memcached answers")
    }

    /// [`ask`](Self::ask), `None` if the server does not answer.
    async fn try_ask(&self, command: &str) -> Option<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(&self.address).await.ok()?;
        stream.write_all(command.as_bytes()).await.ok()?;
        let multi = ["get", "lru_crawler", "stats"]
            .iter()
            .any(|c| command.starts_with(c));
        let mut answer = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let read = stream.read(&mut chunk);
            let n = tokio::time::timeout(std::time::Duration::from_secs(5), read)
                .await
                .ok()?
                .ok()?;
            if n == 0 {
                break;
            }
            answer.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&answer);
            // A crawl already running answers `BUSY`; errors are one line.
            let refused = ["BUSY", "ERROR", "CLIENT_ERROR", "SERVER_ERROR"]
                .iter()
                .any(|p| text.starts_with(p));
            if text.ends_with("\r\n") && (!multi || refused || text.ends_with("END\r\n")) {
                break;
            }
        }
        Some(String::from_utf8_lossy(&answer).into_owned())
    }

    /// Each key with its value and its time to live, to two significant digits (`-` for none),
    /// sorted: a window's end in a run a few seconds after another reads the same.
    /// The LRU crawler's dump skips an item in use: dumps until they hold every item.
    async fn dump(&self) -> Vec<String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let items: usize = self
            .ask("stats\r\n")
            .await
            .lines()
            .find_map(|l| l.strip_prefix("STAT curr_items "))
            .and_then(|n| n.trim().parse().ok())
            .unwrap_or(0);
        let mut expirations = BTreeMap::new();
        for _ in 0..20 {
            if expirations.len() >= items {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            for line in self.ask("lru_crawler metadump all\r\n").await.lines() {
                let fields: BTreeMap<&str, &str> = line
                    .split(' ')
                    .filter_map(|field| field.split_once('='))
                    .collect();
                if let (Some(key), Some(exp)) = (fields.get("key"), fields.get("exp")) {
                    expirations.insert(key.to_string(), exp.parse::<i64>().unwrap());
                }
            }
        }
        assert!(
            expirations.len() >= items,
            "{items} items, dumped: {expirations:?}"
        );
        let mut entries = Vec::new();
        for (key, exp) in expirations {
            let answer = self.ask(&format!("get {key}\r\n")).await;
            let Some(value) = answer
                .lines()
                .nth(1)
                .filter(|_| answer.starts_with("VALUE"))
            else {
                continue;
            };
            let ttl = match exp {
                -1 => "-".to_string(),
                exp => significant(exp - now).to_string(),
            };
            entries.push(format!("{key}={value} ttl={ttl}"));
        }
        entries
    }
}

/// `n` rounded to two significant digits.
fn significant(n: i64) -> i64 {
    let mut scale = 1;
    while n.abs() / scale >= 100 {
        scale *= 10;
    }
    (n + n.signum() * scale / 2) / scale * scale
}

/// The variables of a service on Memcached at `servers` (comma-separated), its stats sent to
/// `statsd`.
fn memcached_environment(
    runtime: &std::path::Path,
    servers: &str,
    statsd: &StatsdServer,
) -> Vec<(&'static str, String)> {
    let (grpc, http, debug) = (reserved_port(), reserved_port(), reserved_port());
    vec![
        (
            "RUNTIME_ROOT",
            runtime.join("current").to_string_lossy().into_owned(),
        ),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("BACKEND_TYPE", "memcache".into()),
        ("MEMCACHE_HOST_PORT", servers.into()),
        ("EXPIRATION_JITTER_MAX_SECONDS", "0".into()),
        ("STATSD_HOST", "127.0.0.1".into()),
        ("STATSD_PORT", statsd.address.port().to_string()),
        ("STATS_FLUSH_INTERVAL", "100ms".into()),
        ("LOG_LEVEL", "warn".into()),
        ("GRPC_HOST", "127.0.0.1".into()),
        ("GRPC_PORT", grpc.to_string()),
        ("HOST", "127.0.0.1".into()),
        ("PORT", http.to_string()),
        ("DEBUG_HOST", "127.0.0.1".into()),
        ("DEBUG_PORT", debug.to_string()),
    ]
}

const MEMCACHED_CONFIG: &[u8] = b"domain: llm
descriptors:
  - key: user
    rate_limit: {unit: hour, requests_per_unit: 3}
  - key: plan
    rate_limit: {unit: month, requests_per_unit: 5}
  - key: shadow
    shadow_mode: true
    rate_limit: {unit: hour, requests_per_unit: 1}
  - key: yearly
    rate_limit: {unit: year, requests_per_unit: 10}
";

/// What a run on Memcached shows: the answers, then memcached's keys, the stats, the
/// healthcheck's status and the log lines (windows' starts and servers masked).
#[derive(Debug, PartialEq)]
struct MemcachedRun {
    answers: Vec<String>,
    keys: Vec<String>,
    stats: BTreeMap<String, u64>,
    health: u16,
    lines: Vec<String>,
}

/// The same run but for the healthcheck: upstream's is never healthy on Memcached (its Redis
/// component starts unhealthy and only Redis marks it); ours is (docs/parity.md §7).
fn assert_same_memcached_runs(upstream: &MemcachedRun, ours: &MemcachedRun, context: &str) {
    assert_eq!((upstream.health, ours.health), (500, 200), "{context}");
    let ours = MemcachedRun {
        health: upstream.health,
        answers: ours.answers.clone(),
        keys: ours.keys.clone(),
        stats: ours.stats.clone(),
        lines: ours.lines.clone(),
    };
    assert_eq!(&ours, upstream, "{context}");
}

/// `requests` sent one at a time to `binary` on the Memcached `servers` (comma-separated;
/// `memcached` those running, dumped after), with `extra` variables, each after the last one's
/// background increments.
async fn memcached_run(
    binary: &std::path::Path,
    runtime: &std::path::Path,
    servers: &str,
    memcached: &[&Memcached],
    extra: &[(&'static str, String)],
    requests: &[RateLimitRequest],
) -> MemcachedRun {
    let statsd = StatsdServer::start().await;
    let mut environment = memcached_environment(runtime, servers, &statsd);
    environment.extend(extra.iter().cloned());
    let http: u16 = variable(&environment, "PORT").parse().unwrap();
    let logged = Logged::start(binary, &environment, http).await;
    let mut client = RateLimitServiceClient::connect(format!(
        "http://127.0.0.1:{}",
        variable(&environment, "GRPC_PORT")
    ))
    .await
    .unwrap();
    let mut answers = Vec::new();
    for request in requests {
        let mut response = client
            .should_rate_limit(request.clone())
            .await
            .map(|r| r.into_inner());
        if let Ok(response) = &mut response {
            for status in &mut response.statuses {
                status.duration_until_reset = None;
            }
        }
        answers.push(format!("{response:?}"));
        // The increments run after the answer.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let masked: Vec<(String, &str)> = servers
        .split(',')
        .enumerate()
        .map(|(i, s)| (s.to_string(), ["MEMCACHED0", "MEMCACHED1"][i.min(1)]))
        .collect();
    // The windows' starts: a run may begin in the next window.
    let bucket = regex::Regex::new(r"_\d{10}\b").unwrap();
    let mask = |text: &str| {
        let mut text = bucket.replace_all(text, "_START").into_owned();
        for (from, to) in &masked {
            text = text.replace(from, to);
        }
        text
    };
    let mut keys = Vec::new();
    for m in memcached {
        for key in m.dump().await {
            keys.push(mask(&format!("{}: {key}", m.address)));
        }
    }
    let health = http_request(&format!("127.0.0.1:{http}"), "GET", "/healthcheck", &[])
        .await
        .status;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let stats = stat_values(&statsd.lines.lock().unwrap(), |name| {
        name.contains(".memcache.") || name.contains(".rate_limit.")
    });
    let port = |name| format!("127.0.0.1:{}", variable(&environment, name));
    let replace = [
        (port("GRPC_PORT"), "GRPC"),
        (port("PORT"), "HTTP"),
        (port("DEBUG_PORT"), "DEBUG"),
    ];
    let lines = logrus_lines(&logged.stop().await, &replace)
        .iter()
        .map(|l| mask(l))
        .collect();
    MemcachedRun {
        answers,
        keys,
        stats,
        health,
        lines,
    }
}

/// On Memcached, a list limit's key past 250 − 64 bytes without its window's start and the
/// request's values is warned about at load (ADR 0001): `{p%25llm_plan_<value>_}_fx3600_n` is
/// 25 bytes and the value, and 3 more under `a` with no value. A `rate_limit:` key keeps upstream's silence, and so does Redis.
#[tokio::test]
async fn long_memcached_keys_are_warned_about_at_load() {
    if !memcached_available() {
        return;
    }
    let (fits, past, legacy) = ("v".repeat(161), "w".repeat(162), "x".repeat(300));
    let (nested_fits, nested_past) = ("y".repeat(158), "z".repeat(159));
    let list = |value: &str| {
        format!(
            "  - key: plan\n    value: {value}\n    rate_limits:\n      - {{name: n, requests_per_unit: 1, window: {{type: fixed, duration: 1h}}}}\n"
        )
    };
    let config = format!(
        "domain: llm\ndescriptors:\n{}{}  - key: plan\n    value: {legacy}\n    rate_limit: {{unit: hour, requests_per_unit: 1}}\n  - key: a\n    descriptors:\n{}{}",
        list(&fits),
        list(&past),
        list(&nested_fits)
            .replace("\n  ", "\n    ")
            .replacen("  -", "    -", 1),
        list(&nested_past)
            .replace("\n  ", "\n    ")
            .replacen("  -", "    -", 1)
    );
    let runtime = temp_dir("memcached-long-keys");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", config.as_bytes()),
            ("current", b"->v1"),
        ],
    );
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit"));
    let memcached = Memcached::start().await;
    let statsd = StatsdServer::start().await;
    let mut environment = memcached_environment(&runtime, &memcached.address, &statsd);
    environment.push(("CACHE_KEY_PREFIX", "p%".into()));
    let grpc = environment
        .iter()
        .find(|(k, _)| *k == "GRPC_PORT")
        .unwrap()
        .1
        .clone();
    let stderr = logged_run(&binary, &environment, &format!("http://127.0.0.1:{grpc}")).await;
    assert!(stderr.contains(&format!("llm.plan_{past}~n: ")), "{stderr}");
    assert!(
        stderr.contains(&format!("llm.a.plan_{nested_past}~n: ")),
        "{stderr}"
    );
    assert_eq!(stderr.matches("takes 187 bytes").count(), 2, "{stderr}");
    assert!(!stderr.contains(&fits), "{stderr}");
    assert!(!stderr.contains(&nested_fits), "{stderr}");
    assert!(!stderr.contains(&legacy), "{stderr}");
    let redis = Redis::start().await;
    environment.push(("BACKEND_TYPE", "redis".into()));
    environment.push(("REDIS_SOCKET_TYPE", "tcp".into()));
    environment.push(("REDIS_URL", redis.address.clone()));
    let stderr = logged_run(&binary, &environment, &format!("http://127.0.0.1:{grpc}")).await;
    assert!(!stderr.contains(&past), "{stderr}");
}

/// `BACKEND_TYPE=memcache` as upstream's: the counters read, the answer, then the increments
/// (`incr`, else `add` with the window's expiration: past 30 days, a year's, a Unix time; a
/// calendar month's, until its end); a refund's `decr`; the local cache skipping memcached for a key
/// over its limit; shadow mode; the `memcache.*` and limits' stats, and the log lines.
#[tokio::test]
async fn memcached_serves_like_upstream() {
    if !memcached_available() {
        return;
    }
    let runtime = temp_dir("memcached");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", MEMCACHED_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let user = |u: &str| request("llm", &[&[("user", u)]]);
    let mut requests = vec![user("a"), user("a"), user("a"), user("a"), user("a")];
    // A refund, then more for that user, with the plan's month and the shadow rule.
    requests.push(refund(user("b"), 2));
    requests.push(user("b"));
    requests.push(with_hits(user("b"), 0, &[Some(3)]));
    requests.push(request("llm", &[&[("plan", "p")], &[("shadow", "s")]]));
    requests.push(request("llm", &[&[("plan", "p")], &[("shadow", "s")]]));
    requests.push(request("llm", &[&[("unknown", "x")]]));
    // A year is over 30 days: its expiration is a Unix time.
    requests.push(request("llm", &[&[("yearly", "y")]]));
    let extra = [
        ("LOCAL_CACHE_SIZE_IN_BYTES", "1048576".to_string()),
        ("USE_CALENDAR_MONTH_RATE_LIMIT", "true".into()),
    ];
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let memcached = Memcached::start().await;
        let servers = memcached.address.clone();
        runs.push(
            memcached_run(
                &binary,
                &runtime,
                &servers,
                &[&memcached],
                &extra,
                &requests,
            )
            .await,
        );
    }
    assert!(
        runs[0].answers[3].contains("OverLimit"),
        "{:?}",
        runs[0].answers
    );
    assert_same_memcached_runs(&runs[0], &runs[1], "");
}

/// Memcached's failures, logged as upstream logs them and answered from what was read: a key
/// gomemcache refuses (a space in it), a value that is not a 32-bit number (`incr` refused for
/// one that is not a number), a server down, a host that does not resolve (no server at all).
/// Upstream's healthcheck fails throughout (it starts with Redis unhealthy, and only Redis makes
/// it healthy); ours does not.
#[tokio::test]
async fn memcached_failures_are_logged_like_upstream() {
    if !memcached_available() {
        return;
    }
    let runtime = temp_dir("memcached-failures");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", MEMCACHED_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let user = |u: &str| request("llm", &[&[("user", u)]]);
    let requests = [user("a b"), user("c"), user("d"), user("c")];
    let down = format!("127.0.0.1:{}", reserved_port());
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let memcached = Memcached::start().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let start = now - now % 3600;
        for (key, value) in [("c", "abc"), ("d", "3000000000")] {
            let set = format!(
                "set llm_user_{key}_{start} 0 3600 {}\r\n{value}\r\n",
                value.len()
            );
            assert_eq!(memcached.ask(&set).await, "STORED\r\n");
        }
        let servers = memcached.address.clone();
        let mut run =
            vec![memcached_run(&binary, &runtime, &servers, &[&memcached], &[], &requests).await];
        for servers in [down.as_str(), "nosuch.invalid:11211"] {
            run.push(memcached_run(&binary, &runtime, servers, &[], &[], &[user("a")]).await);
        }
        runs.push(run);
    }
    assert!(
        runs[0][0]
            .lines
            .iter()
            .any(|l| l.contains("Unexpected non-numeric value")),
        "{:?}",
        runs[0][0].lines
    );
    for (upstream, ours) in runs[0].iter().zip(&runs[1]) {
        assert_same_memcached_runs(upstream, ours, "");
    }
}

/// A TLS server's acceptor with `certificate`, asking for a client certificate issued by
/// `client_ca` when given.
fn tls_acceptor(
    certificate: &(String, String),
    client_ca: Option<&std::path::Path>,
) -> tokio_rustls::TlsAcceptor {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap();
    let builder = match client_ca {
        Some(ca) => {
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_file_iter(ca).unwrap() {
                roots.add(cert.unwrap()).unwrap();
            }
            builder.with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider,
                )
                .build()
                .unwrap(),
            )
        }
        None => builder.with_no_client_auth(),
    };
    let config = builder
        .with_single_cert(
            CertificateDer::pem_slice_iter(certificate.0.as_bytes())
                .map(Result::unwrap)
                .collect(),
            PrivateKeyDer::from_pem_slice(certificate.1.as_bytes()).unwrap(),
        )
        .unwrap();
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}

/// A TLS proxy in front of `target` (memcached) with `acceptor`, on `host`; its address.
async fn tls_proxy(
    target: &str,
    acceptor: tokio_rustls::TlsAcceptor,
    host: &str,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(format!("{host}:0")).await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let target = target.to_string();
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let (acceptor, target) = (acceptor.clone(), target.clone());
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(stream).await else {
                    return;
                };
                let Ok(mut server) = tokio::net::TcpStream::connect(&target).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut tls, &mut server).await;
            });
        }
    });
    (address, task)
}

/// `MEMCACHE_TLS`: memcached over TLS, verified against the system's roots and
/// `MEMCACHE_TLS_CACERT`, with `MEMCACHE_TLS_CLIENT_CERT` and `MEMCACHE_TLS_CLIENT_KEY` when the
/// server asks for one; a certificate refused (an unknown authority, expired) fails each
/// operation, as upstream logs it. On `[::1]`, an address the certificate does not name, with
/// `MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION` (when the host has IPv6).
#[tokio::test]
async fn memcached_over_tls_is_used_like_upstream() {
    if !memcached_available() {
        return;
    }
    let runtime = temp_dir("memcached-tls");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", MEMCACHED_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let pki = pki(&runtime.join("pki"));
    let path = |p: &std::path::Path| p.to_string_lossy().into_owned();
    let user = |u: &str| request("llm", &[&[("user", u)]]);
    let requests = [user("a"), user("a")];
    // Go on macOS asks the system about an unknown authority, in its own words.
    let untrusted = regex::Regex::new(
        r"x509: (“[^”]*” certificate is not trusted|certificate signed by unknown authority)",
    )
    .unwrap();
    let now = regex::Regex::new(r"current time \S+ is after").unwrap();
    let ipv6 = std::net::TcpListener::bind("[::1]:0").is_ok();
    // Each case: a CA given, a client certificate asked (and given), the server's certificate
    // expired, on `[::1]` with the hostname not verified.
    let cases = [
        ("the CA given", true, false, false, false),
        ("no CA", false, false, false, false),
        ("a client certificate asked", true, true, false, false),
        ("expired", true, false, true, false),
        ("[::1]", true, false, false, true),
    ];
    for (name, ca, client_certificate, expired, on_ipv6) in cases {
        if on_ipv6 && !ipv6 {
            continue;
        }
        let mut runs = Vec::new();
        for binary in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
        ] {
            let memcached = Memcached::start().await;
            let certificate = if expired {
                &pki.expired_server
            } else {
                &pki.server
            };
            let asked = client_certificate.then_some(pki.ca.as_path());
            let host = if on_ipv6 { "[::1]" } else { "127.0.0.1" };
            let (proxy, _task) =
                tls_proxy(&memcached.address, tls_acceptor(certificate, asked), host).await;
            let mut extra = vec![("MEMCACHE_TLS", "true".to_string())];
            if ca {
                extra.push(("MEMCACHE_TLS_CACERT", path(&pki.ca)));
            }
            if client_certificate {
                extra.push(("MEMCACHE_TLS_CLIENT_CERT", path(&pki.client_cert)));
                extra.push(("MEMCACHE_TLS_CLIENT_KEY", path(&pki.client_key)));
            }
            if on_ipv6 {
                extra.push(("MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION", "true".into()));
            }
            let mut run =
                memcached_run(&binary, &runtime, &proxy, &[&memcached], &extra, &requests).await;
            run.keys = run
                .keys
                .iter()
                .map(|k| k.replace(&memcached.address, "MEMCACHED0"))
                .collect();
            run.lines = run
                .lines
                .iter()
                .map(|l| {
                    let l = untrusted.replace_all(l, "x509: UNTRUSTED");
                    now.replace_all(&l, "current time NOW is after")
                        .into_owned()
                })
                .collect();
            runs.push(run);
        }
        assert_same_memcached_runs(&runs[0], &runs[1], name);
        let works = ca && !expired;
        assert_eq!(runs[0].keys.is_empty(), !works, "{name}: {:?}", runs[0]);
    }
}

/// Several `MEMCACHE_HOST_PORT` servers: each key on the one gomemcache picks (the CRC-32 of the
/// key), its operations there.
#[tokio::test]
async fn memcached_keys_are_sharded_like_upstream() {
    if !memcached_available() {
        return;
    }
    let runtime = temp_dir("memcached-shards");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", MEMCACHED_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let users = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let requests: Vec<RateLimitRequest> = users
        .iter()
        .map(|u| request("llm", &[&[("user", u)], &[("plan", u)]]))
        .collect();
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let (first, second) = (Memcached::start().await, Memcached::start().await);
        let servers = format!("{},{}", first.address, second.address);
        runs.push(
            memcached_run(
                &binary,
                &runtime,
                &servers,
                &[&first, &second],
                &[],
                &requests,
            )
            .await,
        );
    }
    assert!(
        runs[0].keys.iter().any(|k| k.starts_with("MEMCACHED0"))
            && runs[0].keys.iter().any(|k| k.starts_with("MEMCACHED1")),
        "{:?}",
        runs[0].keys
    );
    assert_same_memcached_runs(&runs[0], &runs[1], "");
}

/// `MEMCACHE_MAX_IDLE_CONNS`, as gomemcache keeps connections: each operation takes an idle one
/// or dials a new one, and at most that many are kept once done; the rest are closed.
#[tokio::test]
async fn memcached_keeps_idle_connections_like_gomemcache() {
    if !memcached_available() {
        return;
    }
    use llm_ratelimit::memcached::client::{Address, Client, Error};
    let memcached = Memcached::start().await;
    let total = || async {
        memcached
            .ask("stats\r\n")
            .await
            .lines()
            .find_map(|l| {
                l.strip_prefix("STAT total_connections ")?
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .unwrap()
    };
    let client = Client::new(
        vec![Address::Tcp(memcached.address.parse().unwrap())],
        1,
        None,
    );
    let before = total().await;
    // Three at once: three connections, one kept.
    let (a, b, c) = tokio::join!(
        client.add("a", b"1", 60),
        client.add("b", b"1", 60),
        client.add("c", b"1", 60)
    );
    assert_eq!((a, b, c), (Ok(()), Ok(()), Ok(())));
    // Each `ask` is a connection of its own.
    assert_eq!(total().await - before, 3 + 1);
    // One after the other: the kept one, every time.
    for key in ["a", "b", "c"] {
        assert_eq!(client.increment(key, 1).await, Ok(2));
    }
    assert_eq!(total().await - before, 3 + 2);
    // A miss keeps the connection; another error closes it: the next operation dials.
    assert_eq!(client.increment("missing", 1).await, Err(Error::CacheMiss));
    assert_eq!(client.add("text", b"abc", 60).await, Ok(()));
    assert!(client.increment("text", 1).await.is_err());
    assert_eq!(client.increment("a", 1).await, Ok(3));
    assert_eq!(total().await - before, 3 + 3 + 1);
}

/// With Memcached, a call's spans: the service's, and `Memcached Fetch Execution` with the
/// number of keys it gets.
#[tokio::test]
async fn memcached_calls_are_traced_like_upstream() {
    if !memcached_available() {
        return;
    }
    let runtime = temp_dir("memcached-traces");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", MEMCACHED_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit")),
    ] {
        let memcached = Memcached::start().await;
        let statsd = StatsdServer::start().await;
        let (collected, collector_port) = otlp_collector(true, None).await;
        let mut environment = memcached_environment(&runtime, &memcached.address, &statsd);
        environment.extend([
            ("TRACING_ENABLED", "true".to_string()),
            ("TRACING_EXPORTER_PROTOCOL", "grpc".into()),
            // Random otherwise.
            ("TRACING_SERVICE_INSTANCE_ID", "instance-1".into()),
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                format!("http://127.0.0.1:{collector_port}"),
            ),
        ]);
        let http: u16 = variable(&environment, "PORT").parse().unwrap();
        let logged = Logged::start(&binary, &environment, http).await;
        let mut client = RateLimitServiceClient::connect(format!(
            "http://127.0.0.1:{}",
            variable(&environment, "GRPC_PORT")
        ))
        .await
        .unwrap();
        client
            .should_rate_limit(request("llm", &[&[("user", "a")], &[("plan", "p")]]))
            .await
            .unwrap();
        logged.stop().await;
        runs.push(received_spans(&collected.lock().unwrap(), &[]));
    }
    assert!(
        runs[0]
            .iter()
            .any(|s| s.contains("Memcached Fetch Execution")),
        "{:#?}",
        runs[0]
    );
    assert_eq!(runs[1], runs[0]);
}

// `rate_limits:` with `fixed` windows (docs/design.md D7, D8, ADR 0001, ADR 0002): an
// extension, with no upstream to compare with, counted on a real Redis.

/// The service on its own Redis (and per-second Redis), on a manual clock.
struct Extended {
    subject: Subject,
    redis: Redis,
    per_second: Option<Redis>,
}

impl Extended {
    async fn start(yaml: &str, settings: Settings) -> Self {
        let redis = Redis::start().await;
        let per_second = match settings.per_second {
            true => Some(Redis::start().await),
            false => None,
        };
        let subject =
            Subject::start(&redis, per_second.as_ref(), &settings, None, None, None).await;
        let file = ConfigFile {
            name: "config.yaml".into(),
            content: yaml.into(),
        };
        subject
            .service
            .set_config(config::load(&[file], &settings.load).unwrap());
        Self {
            subject,
            redis,
            per_second,
        }
    }

    fn at(&self, now: i64) {
        self.subject.clock.0.store(now, Ordering::SeqCst);
    }

    async fn call(&self, request: RateLimitRequest) -> RateLimitResponse {
        self.subject
            .service
            .should_rate_limit(&request)
            .await
            .unwrap()
    }

    /// The code, limit name, remaining hits and reset of each descriptor's status.
    async fn statuses(&self, request: RateLimitRequest) -> Vec<(String, String, u32, i64)> {
        self.call(request)
            .await
            .statuses
            .iter()
            .map(|s| {
                (
                    format!("{:?}", s.code()),
                    s.current_limit
                        .as_ref()
                        .map_or(String::new(), |l| l.name.clone()),
                    s.limit_remaining,
                    s.duration_until_reset.as_ref().map_or(-1, |d| d.seconds),
                )
            })
            .collect()
    }

    /// The counters and the seconds left of their TTL (to the nearest minute, the test
    /// taking some seconds of real time), in `redis`.
    async fn keys_of(redis: &Redis) -> BTreeMap<String, (String, i64)> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        redis
            .dump()
            .await
            .into_iter()
            .map(|(k, (v, expiry))| (k, (v, (expiry - now + 30) / 60 * 60)))
            .collect()
    }

    async fn keys(&self) -> BTreeMap<String, (String, i64)> {
        Self::keys_of(&self.redis).await
    }

    /// The counters of the list limits' stats.
    fn stats(&self) -> BTreeMap<String, u64> {
        self.subject
            .stats
            .snapshot()
            .into_iter()
            .filter(|(name, value)| name.contains('~') && *value > 0)
            .collect()
    }
}

/// [`Extended::statuses`] rows, from literals.
fn owned<const N: usize>(rows: [(&str, &str, u32, i64); N]) -> Vec<(String, String, u32, i64)> {
    rows.iter()
        .map(|(c, n, r, t)| (c.to_string(), n.to_string(), *r, *t))
        .collect()
}

const WEEK_AND_FIVE_HOURS: &str = "
domain: d
descriptors:
  - key: user
    rate_limits:
      - name: five_hour
        requests_per_unit: 3
        window: {type: fixed, duration: 5h}
      - name: weekly
        requests_per_unit: 5
        window: {type: fixed, duration: 7d, offset: 4d}
";

/// A request for `alice` with a client limit of 1 an hour × `multiplier` (D23).
fn client_limit(multiplier: Option<u32>) -> RateLimitRequest {
    use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride;
    use llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::rate_limit::Unit;
    let mut request = request("d", &[&[("user", "alice")]]);
    request.descriptors[0].limit = Some(RateLimitOverride {
        requests_per_unit: 1,
        unit: Unit::Hour as i32,
        unit_multiplier: multiplier,
    });
    request
}

/// A client limit's `unit_multiplier` is one fixed window of that many units, in a key with its
/// duration, and echoed (D23).
#[tokio::test]
async fn client_limits_apply_their_unit_multiplier() {
    use llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::rate_limit::Unit;
    if !tools_available() {
        return;
    }
    let h = Extended::start(WEEK_AND_FIVE_HOURS, Settings::default()).await;
    let answer = async |now: i64| {
        h.at(now);
        let response = h.call(client_limit(Some(2))).await;
        let status = response.statuses[0].clone();
        let limit = status.current_limit.clone().unwrap();
        (
            format!("{:?}", status.code()),
            limit.unit(),
            limit.unit_multiplier,
            status.limit_remaining,
            status.duration_until_reset.unwrap().seconds,
        )
    };
    // The 2 h windows start at multiples of 7200: 999993600, 1000000800.
    assert_eq!(
        answer(999_994_000).await,
        ("Ok".to_string(), Unit::Hour, Some(2), 0, 6800)
    );
    // Past the hour, in the same window.
    assert_eq!(
        answer(999_998_000).await,
        ("OverLimit".to_string(), Unit::Hour, Some(2), 0, 2800)
    );
    assert_eq!(
        h.keys().await,
        BTreeMap::from([(
            "{d_user_alice_}_fx7200_999993600_".to_string(),
            ("2".to_string(), 7200)
        )])
    );
    assert_eq!(
        answer(1_000_000_800).await,
        ("Ok".to_string(), Unit::Hour, Some(2), 0, 7200)
    );
    // The quota lookup reads the same window.
    let lookup = h
        .subject
        .service
        .lookup(&client_limit(Some(2)))
        .await
        .unwrap();
    let rendered: serde_json::Value =
        serde_json::from_str(&llm_ratelimit::quota::render(&lookup)).unwrap();
    assert_eq!(
        rendered["descriptors"][0]["limits"][0]["window"],
        serde_json::json!({"type": "fixed", "duration_s": 7200})
    );
    assert_eq!(rendered["descriptors"][0]["limits"][0]["used"], 1);
}

/// A multiplier of 1 is no multiplier: upstream's key, and none echoed (D13, D23).
#[tokio::test]
async fn a_client_limit_multiplied_by_one_is_a_plain_one() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(WEEK_AND_FIVE_HOURS, Settings::default()).await;
    h.at(1_000_000_000);
    let response = h.call(client_limit(Some(1))).await;
    let limit = response.statuses[0].current_limit.clone().unwrap();
    assert_eq!(limit.unit_multiplier, None);
    assert_eq!(
        h.keys().await.into_keys().collect::<Vec<_>>(),
        ["d_user_alice_999997200"]
    );
}

/// A multiplier of 0, or a window over `u32::MAX` seconds as in `rate_limits:`, is a bad
/// request, also for the quota lookup; on an unknown domain no limit applies, as for any
/// client limit.
#[tokio::test]
async fn client_limits_with_an_impossible_multiplier_are_refused() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(WEEK_AND_FIVE_HOURS, Settings::default()).await;
    h.at(1_000_000_000);
    for multiplier in [0, 1_193_047] {
        let request = client_limit(Some(multiplier));
        let error = h.subject.service.should_rate_limit(&request).await;
        assert_eq!(
            error.err().map(|e| e.to_string()),
            Some(format!("invalid rate limit unit_multiplier {multiplier}"))
        );
        assert!(h.subject.service.lookup(&request).await.is_err());
        let mut elsewhere = request;
        elsewhere.domain = "unknown".into();
        assert!(
            h.subject
                .service
                .should_rate_limit(&elsewhere)
                .await
                .is_ok()
        );
    }
    // The longest: 1193046 hours is under 2^32 seconds.
    let response = h.call(client_limit(Some(1_193_046))).await;
    assert_eq!(
        response.statuses[0]
            .current_limit
            .as_ref()
            .unwrap()
            .unit_multiplier,
        Some(1_193_046)
    );
    assert_eq!(h.keys().await.len(), 1);
}

/// On Memcached too (D23, D24): the key with its duration. A window ending past what memcached's
/// `int32` expiration can say ends at its largest: a wrapped one would be already expired.
#[tokio::test]
async fn client_limits_apply_their_unit_multiplier_on_memcached() {
    if !memcached_available() {
        return;
    }
    let memcached = Memcached::start().await;
    let servers =
        llm_ratelimit::memcached::client::resolve(std::slice::from_ref(&memcached.address))
            .await
            .unwrap();
    let client = Arc::new(llm_ratelimit::memcached::client::Client::new(
        servers, 2, None,
    ));
    let clock = Arc::new(ManualClock::default());
    let stats = Arc::new(Stats::default());
    let cache = llm_ratelimit::memcached::MemcacheCache::new(
        client,
        clock.clone(),
        stats.clone(),
        &stats.scope_with_tags("ratelimit"),
        CacheSettings::default(),
        None,
    );
    let service = RateLimitService::new(cache, clock.clone(), stats, ServiceSettings::default());
    let file = ConfigFile {
        name: "config.yaml".into(),
        content: WEEK_AND_FIVE_HOURS.into(),
    };
    service.set_config(config::load(&[file], &LoadOptions::default()).unwrap());
    clock.0.store(999_994_000, Ordering::SeqCst);
    let response = service
        .should_rate_limit(&client_limit(Some(2)))
        .await
        .unwrap();
    assert_eq!(
        response.statuses[0]
            .duration_until_reset
            .as_ref()
            .unwrap()
            .seconds,
        6800
    );
    // The increment goes after the answer.
    let mut answer = String::new();
    for _ in 0..100 {
        answer = memcached
            .ask("get {d_user_alice_}_fx7200_999993600_\r\n")
            .await;
        if answer.starts_with("VALUE") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(answer.starts_with("VALUE"), "{answer}");
    // 105000 h from 2027 is past 2038.
    clock.0.store(1_800_000_000, Ordering::SeqCst);
    service
        .should_rate_limit(&client_limit(Some(105_000)))
        .await
        .unwrap();
    for _ in 0..100 {
        answer = memcached
            .ask("get {d_user_alice_}_fx378000000_1512000000_\r\n")
            .await;
        if answer.starts_with("VALUE") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(answer.starts_with("VALUE"), "{answer}");
}

/// A client limit's window of several units is its own unit in the headers (D13, D23).
#[tokio::test]
async fn client_limits_with_a_multiplier_have_headers_of_their_own() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        service: ServiceSettings {
            per_unit_headers: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(WEEK_AND_FIVE_HOURS, settings).await;
    h.at(1_000_000_000);
    let response = h.call(client_limit(Some(2))).await;
    let headers: BTreeMap<String, String> = response
        .response_headers_to_add
        .into_iter()
        .map(|header| (header.key, header.value))
        .collect();
    assert_eq!(
        headers,
        BTreeMap::from([
            ("RateLimit-Limit-2Hours".to_string(), "1".to_string()),
            ("RateLimit-Remaining-2Hours".to_string(), "0".to_string()),
        ])
    );
}

/// Each limit of a list counts in its own key (ADR 0001), its window from the epoch or its
/// offset; the descriptor's status is the representative one (D17): of those over, the
/// longest to reset, else the fewest remaining. Each limit has its stats, named `~name` (D29).
#[tokio::test]
async fn list_limits_count_in_their_own_windows() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(WEEK_AND_FIVE_HOURS, Settings::default()).await;
    let alice = || request("d", &[&[("user", "alice")]]);
    // 2001-09-09 01:46:40 UTC, a Sunday: the 5 h window started at 00:00, the week (from
    // Monday 00:00 UTC, the epoch being a Thursday) on 2001-09-03.
    h.at(1_000_000_000);
    let mut seen = Vec::new();
    for _ in 0..4 {
        seen.extend(h.statuses(alice()).await);
    }
    assert_eq!(
        seen,
        owned([
            ("Ok", "five_hour", 2, 8000),
            ("Ok", "five_hour", 1, 8000),
            ("Ok", "five_hour", 0, 8000),
            ("OverLimit", "five_hour", 0, 8000),
        ])
    );
    // The next 5 h window: the week is the one left with the fewest hits, then over.
    h.at(1_000_008_000);
    let mut seen = Vec::new();
    for _ in 0..4 {
        seen.extend(h.statuses(alice()).await);
    }
    assert_eq!(
        seen,
        owned([
            ("Ok", "weekly", 0, 72000),
            ("OverLimit", "weekly", 0, 72000),
            ("OverLimit", "weekly", 0, 72000),
            // Both over: the week ends last.
            ("OverLimit", "weekly", 0, 72000),
        ])
    );
    let current = h.call(alice()).await.statuses[0]
        .current_limit
        .clone()
        .unwrap();
    assert_eq!(
        (current.unit(), current.unit_multiplier, current.requests_per_unit),
        (
            llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::rate_limit::Unit::Week,
            None,
            5
        )
    );
    assert_eq!(
        h.keys().await,
        BTreeMap::from([
            (
                "{d_user_alice_}_fx18000_999990000_five_hour".to_string(),
                ("4".to_string(), 18000)
            ),
            (
                "{d_user_alice_}_fx18000_1000008000_five_hour".to_string(),
                ("5".to_string(), 18000)
            ),
            (
                "{d_user_alice_}_fx604800o345600_999475200_weekly".to_string(),
                ("9".to_string(), 604800)
            ),
        ])
    );
    let limit =
        |name: &str, stat: &str| format!("ratelimit.service.rate_limit.d.user~{name}.{stat}");
    assert_eq!(
        h.stats(),
        BTreeMap::from([
            (limit("five_hour", "total_hits"), 9),
            (limit("five_hour", "within_limit"), 6),
            (limit("five_hour", "near_limit"), 2),
            (limit("five_hour", "over_limit"), 3),
            (limit("weekly", "total_hits"), 9),
            (limit("weekly", "within_limit"), 5),
            (limit("weekly", "near_limit"), 1),
            (limit("weekly", "over_limit"), 4),
        ])
    );
}

/// `offset: hashed` (ADR 0002): each descriptor's window starts at its own phase, hashed from
/// its entries after `share_threshold`; the hash tag escapes `%`, `{` and `}` (ADR 0001).
/// Expected phases from an independent FNV-1a.
#[tokio::test]
async fn hashed_offsets_and_escaped_keys_are_stored_as_adr_0001_says() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(
        "
domain: \"d{1}%\"
descriptors:
  - key: user
    rate_limits:
      - {name: weekly, requests_per_unit: 5, window: {type: fixed, duration: 7d, offset: hashed}}
  - key: file
    value: \"a*\"
    share_threshold: true
    rate_limits:
      - {name: daily, requests_per_unit: 5, window: {type: fixed, duration: 1d, offset: hashed, offset_rounding: 6h}}
",
        Settings::default(),
    )
    .await;
    h.at(1_000_000_000);
    let descriptors: &[&[(&str, &str)]] = &[
        &[("user", "alice")],
        &[("user", "b}o{b")],
        &[("file", "abc")],
        &[("file", "axe")],
    ];
    assert_eq!(
        h.statuses(request("d{1}%", descriptors)).await,
        owned([
            // Phase 428 400 s: the week started at 999 558 000.
            ("Ok", "weekly", 4, 162_800),
            // Phase 115 200 s.
            ("Ok", "weekly", 4, 454_400),
            // Both files share `file=a*`: phase 21 600 s, one counter.
            ("Ok", "daily", 4, 15_200),
            ("Ok", "daily", 3, 15_200),
        ])
    );
    let keys: Vec<String> = h.keys().await.into_keys().collect();
    assert_eq!(
        keys,
        [
            "{d%7B1%7D%25_file_a*_}_fx86400oh21600_999928800_daily",
            "{d%7B1%7D%25_user_alice_}_fx604800oh3600_999558000_weekly",
            "{d%7B1%7D%25_user_b%7Do%7Bb_}_fx604800oh3600_999849600_weekly",
        ]
    );
}

/// The descriptor's `shadow_mode` (D17) applies to the representative status: OK where it is
/// over, its remaining hits 0; each limit counts its `shadow_mode` stat.
#[tokio::test]
async fn shadow_mode_applies_to_a_list_after_its_reduction() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: user
    shadow_mode: true
    rate_limits:
      - {name: minute, requests_per_unit: 1, window: {type: fixed, duration: 1m}}
      - {name: hour, requests_per_unit: 1, window: {type: fixed, duration: 1h}}
",
        Settings::default(),
    )
    .await;
    h.at(1_000_000_000);
    let call = || h.statuses(request("d", &[&[("user", "a")]]));
    assert_eq!(call().await, owned([("Ok", "minute", 0, 20)]));
    // Both over: the hour ends last.
    assert_eq!(call().await, owned([("Ok", "hour", 0, 800)]));
    let response = h.call(request("d", &[&[("user", "a")]])).await;
    assert_eq!(
        response.overall_code(),
        llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::Code::Ok
    );
    let stats = h.stats();
    assert_eq!(
        stats["ratelimit.service.rate_limit.d.user~minute.shadow_mode"],
        2
    );
    assert_eq!(
        stats["ratelimit.service.rate_limit.d.user~hour.shadow_mode"],
        2
    );
}

/// `replaces` (D16) on list limits: a list limit replaces another descriptor's limit by name,
/// and is replaced by name; a descriptor whose limits are all replaced is OK, with no limit.
#[tokio::test]
async fn list_limits_replace_and_are_replaced_by_name() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: hourly, requests_per_unit: 10, window: {type: fixed, duration: 1h}}
      - {name: daily, requests_per_unit: 100, window: {type: fixed, duration: 1d}, replaces: [{name: legacy}]}
  - key: org
    rate_limit: {unit: minute, requests_per_unit: 1, name: legacy}
  - key: team
    rate_limit: {unit: minute, requests_per_unit: 50, replaces: [{name: hourly}]}
  - key: all
    rate_limit: {unit: minute, requests_per_unit: 50, replaces: [{name: hourly}, {name: daily}]}
",
        Settings::default(),
    )
    .await;
    h.at(1_000_000_000);
    assert_eq!(
        h.statuses(request("d", &[&[("user", "a")], &[("org", "x")]]))
            .await,
        owned([("Ok", "hourly", 9, 800), ("Ok", "", 0, -1)])
    );
    assert_eq!(
        h.statuses(request("d", &[&[("user", "a")], &[("team", "x")]]))
            .await,
        owned([("Ok", "daily", 98, 80000), ("Ok", "", 49, 20)])
    );
    assert_eq!(
        h.statuses(request("d", &[&[("user", "a")], &[("all", "x")]]))
            .await,
        owned([("Ok", "", 0, -1), ("Ok", "", 49, 20)])
    );
}

/// Refunds (`is_negative_hits`, D21) take hits back from every limit of the list, floored
/// at 0, and answer OK with what remains.
#[tokio::test]
async fn refunds_take_hits_back_from_every_list_limit() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(WEEK_AND_FIVE_HOURS, Settings::default()).await;
    h.at(1_000_000_000);
    let hits = |n: u64, refund: bool| {
        let mut r = request("d", &[&[("user", "alice")]]);
        r.descriptors[0].hits_addend = Some(n);
        r.descriptors[0].is_negative_hits = refund;
        r
    };
    assert_eq!(
        h.statuses(hits(4, false)).await,
        owned([("OverLimit", "five_hour", 0, 8000)])
    );
    assert_eq!(
        h.statuses(hits(2, true)).await,
        owned([("Ok", "five_hour", 1, 8000)])
    );
    assert_eq!(
        h.statuses(hits(9, true)).await,
        owned([("Ok", "five_hour", 3, 8000)])
    );
    let values: Vec<String> = h.keys().await.into_values().map(|(v, _)| v).collect();
    assert_eq!(values, ["0", "0"]);
}

/// The local cache (D18) keeps a list key over its limit for its window: the next requests
/// are answered without Redis, and counted in `over_limit_with_local_cache`.
#[tokio::test]
async fn list_keys_over_their_limit_are_kept_in_the_local_cache() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: CacheSettings {
            local_cache_size_in_bytes: 1 << 20,
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(WEEK_AND_FIVE_HOURS, settings).await;
    h.at(1_000_000_000);
    let alice = || request("d", &[&[("user", "alice")]]);
    for _ in 0..6 {
        h.call(alice()).await;
    }
    assert_eq!(
        h.statuses(alice()).await,
        owned([("OverLimit", "weekly", 0, 80000)])
    );
    // The 5 h key stopped at 4, the week's at 6: Redis was not asked again.
    let values: BTreeMap<String, String> = h
        .keys()
        .await
        .into_iter()
        .map(|(k, (v, _))| (k, v))
        .collect();
    assert_eq!(
        values,
        BTreeMap::from([
            (
                "{d_user_alice_}_fx18000_999990000_five_hour".to_string(),
                "4".to_string()
            ),
            (
                "{d_user_alice_}_fx604800o345600_999475200_weekly".to_string(),
                "6".to_string()
            ),
        ])
    );
    let stats = h.stats();
    assert_eq!(
        stats["ratelimit.service.rate_limit.d.user~five_hour.over_limit_with_local_cache"],
        3
    );
    assert_eq!(
        stats["ratelimit.service.rate_limit.d.user~weekly.over_limit_with_local_cache"],
        1
    );
}

/// The local cache never holds an anchored window (D18): over its limit, each request still
/// reaches Redis, so an amendment is counted.
#[tokio::test]
async fn anchored_windows_stay_out_of_the_local_cache() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: CacheSettings {
            local_cache_size_in_bytes: 1 << 20,
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(SESSION, settings).await;
    let alice: &[&[(&str, &str)]] = &[&[("user", "alice")]];
    h.at(1_000_000_000);
    h.call(hits(alice, 4)).await;
    h.call(hits(alice, 100)).await;
    let lookup = h
        .subject
        .service
        .lookup(&request("d", alice))
        .await
        .unwrap();
    let rendered: serde_json::Value =
        serde_json::from_str(&llm_ratelimit::quota::render(&lookup)).unwrap();
    let session = &rendered["descriptors"][0]["limits"][0];
    assert_eq!(session["name"], "five_hour");
    assert_eq!(session["used"], 104);
    assert!(
        !h.stats().contains_key(
            "ratelimit.service.rate_limit.d.user~five_hour.over_limit_with_local_cache"
        )
    );
}

const CONCURRENT: &str = "
domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: session, requests_per_unit: 3, window: {type: anchored, duration: 5h}}
  - key: inflight
    descriptors:
      - key: model
        rate_limits:
          - {name: slots, requests_per_unit: 2, window: {type: concurrency, lease: 10m, retry_after: 2s}}
  - key: pool
    rate_limits:
      - {name: all, requests_per_unit: 1, window: {type: concurrency, lease: 10m}}
";

/// The descriptor of `user`'s slots on model `m`.
const ALICE_SLOTS: &[&[(&str, &str)]] = &[&[("inflight", "alice"), ("model", "m")]];
const ALICE_KEY: &str = "{d_inflight_alice_model_m_}_cc_slots";

/// How many slots a concurrency window's `key` holds, its expired ones included.
async fn slots_held(redis: &Redis, key: &str) -> String {
    let reply = resp_command(&redis.address, &["ZCARD", key]).await;
    reply.trim().trim_start_matches(':').to_string()
}

/// A check takes a slot up to the limit, then `OVER_LIMIT` with `retry_after`; an amendment or
/// a refund gives slots back, floored at 0 (D35).
#[tokio::test]
async fn concurrency_windows_take_and_give_back_slots() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(CONCURRENT, Settings::default()).await;
    h.at(1_000_000_000);
    let check = || hits(ALICE_SLOTS, 0);
    assert_eq!(h.statuses(check()).await, owned([("Ok", "slots", 1, 2)]));
    assert_eq!(h.statuses(check()).await, owned([("Ok", "slots", 0, 2)]));
    assert_eq!(
        h.statuses(check()).await,
        owned([("OverLimit", "slots", 0, 2)])
    );
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "2");
    // An amendment of cost 1 frees one, which the next check takes.
    assert_eq!(
        h.statuses(hits(ALICE_SLOTS, 1)).await,
        owned([("Ok", "slots", 1, 2)])
    );
    assert_eq!(h.statuses(check()).await, owned([("Ok", "slots", 0, 2)]));
    // A refund is a release; of nothing, it changes nothing.
    let mut refund = hits(ALICE_SLOTS, 1);
    refund.descriptors[0].is_negative_hits = true;
    assert_eq!(
        h.statuses(refund.clone()).await,
        owned([("Ok", "slots", 1, 2)])
    );
    refund.descriptors[0].hits_addend = Some(0);
    assert_eq!(h.statuses(refund).await, owned([("Ok", "slots", 1, 2)]));
    // More than are held: floored, and counted as a client not following the protocol.
    assert_eq!(
        h.statuses(hits(ALICE_SLOTS, 5)).await,
        owned([("Ok", "slots", 2, 2)])
    );
    let stats = h.stats();
    let stat = |name: &str| {
        stats
            .get(&format!(
                "ratelimit.service.rate_limit.d.inflight.model~slots.{name}"
            ))
            .copied()
    };
    assert_eq!(stat("slots_acquired"), Some(3));
    assert_eq!(stat("slots_released"), Some(3));
    assert_eq!(stat("releases_without_slot"), Some(4));
    assert_eq!(stat("slots_expired"), None);
    assert_eq!(stat("total_hits"), Some(4));
    assert_eq!(stat("within_limit"), Some(3));
    assert_eq!(stat("over_limit"), Some(1));
    // The set is gone with its last slot, and it expires after its lease otherwise.
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "0");
    h.call(check()).await;
    let ttl = resp_command(&h.redis.address, &["TTL", ALICE_KEY]).await;
    let ttl: i64 = ttl.trim().trim_start_matches(':').parse().unwrap();
    assert!((655..=660).contains(&ttl), "{ttl}");
}

/// A slot ends with its lease, whichever request's operation finds it (D35). A release takes
/// the newest slot, so that an orphan keeps its age: with the oldest taken, each release would
/// leave a fresher stamp, and under steady traffic the excess would never expire.
#[tokio::test]
async fn slots_end_with_their_lease_and_releases_take_the_newest() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(CONCURRENT, Settings::default()).await;
    let check = || hits(ALICE_SLOTS, 0);
    // An orphan, then a request that gets its amendment.
    h.at(1_000_000_000);
    h.call(check()).await;
    h.at(1_000_000_100);
    h.call(check()).await;
    h.call(hits(ALICE_SLOTS, 1)).await;
    // The orphan's lease is not over a second before its end.
    h.at(1_000_000_599);
    assert_eq!(h.statuses(check()).await, owned([("Ok", "slots", 0, 2)]));
    h.call(hits(ALICE_SLOTS, 1)).await;
    h.at(1_000_000_600);
    assert_eq!(h.statuses(check()).await, owned([("Ok", "slots", 1, 2)]));
    let stats = h.stats();
    assert_eq!(
        stats["ratelimit.service.rate_limit.d.inflight.model~slots.slots_expired"],
        1
    );
}

/// No slot stays held for a request that another limit denies: no amendment will come for it
/// (ADR 0004).
#[tokio::test]
async fn a_request_denied_by_another_limit_holds_no_slot() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(CONCURRENT, Settings::default()).await;
    h.at(1_000_000_000);
    let both: &[&[(&str, &str)]] = &[
        &[("user", "alice")],
        &[("inflight", "alice"), ("model", "m")],
    ];
    // The session is over.
    h.call(hits(&both[..1], 4)).await;
    let response = h.call(hits(both, 0)).await;
    assert_eq!(response.overall_code(), Code::OverLimit);
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "0");
    assert!(
        !h.stats()
            .contains_key("ratelimit.service.rate_limit.d.inflight.model~slots.slots_acquired")
    );
    // Its status says what is free, without taking anything.
    assert_eq!(response.statuses[1].code(), Code::Ok);
    assert_eq!(response.statuses[1].limit_remaining, 2);
    // The amendment of an earlier request still releases, whatever the session says.
    h.call(hits(ALICE_SLOTS, 0)).await;
    let amendment = h.call(hits(both, 1)).await;
    assert_eq!(amendment.overall_code(), Code::OverLimit);
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "0");
}

/// Slots taken for a request that another concurrency limit denies are given back, with their
/// own identifiers: another request's slot is left alone (ADR 0004).
#[tokio::test]
async fn slots_are_given_back_when_another_concurrency_limit_denies() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(CONCURRENT, Settings::default()).await;
    h.at(1_000_000_000);
    let pool: &[&[(&str, &str)]] = &[&[("pool", "gpu")]];
    let both: &[&[(&str, &str)]] = &[&[("inflight", "alice"), ("model", "m")], &[("pool", "gpu")]];
    // One slot of alice's another request holds, and the pool full.
    h.call(hits(ALICE_SLOTS, 0)).await;
    h.call(hits(pool, 0)).await;
    let response = h.call(hits(both, 0)).await;
    assert_eq!(response.overall_code(), Code::OverLimit);
    assert_eq!(response.statuses[0].code(), Code::Ok);
    assert_eq!(response.statuses[1].code(), Code::OverLimit);
    // What it would have left, had it not given its slot back.
    assert_eq!(response.statuses[0].limit_remaining, 1);
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "1");
    assert_eq!(slots_held(&h.redis, "{d_pool_gpu_}_cc_all").await, "1");
    let stats = h.stats();
    assert_eq!(
        stats["ratelimit.service.rate_limit.d.inflight.model~slots.slots_released"],
        1
    );
    // Once the pool has room, both are taken.
    h.call(hits(pool, 1)).await;
    assert_eq!(h.call(hits(both, 0)).await.overall_code(), Code::Ok);
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "2");
}

/// The other limits decide as the final decision will: a limit in shadow mode denies nothing,
/// so the slot is taken; a quota group over denies only when every group is (ADR 0004).
#[tokio::test]
async fn slots_follow_shadow_and_quota_modes_of_other_limits() {
    if !tools_available() {
        return;
    }
    let session = "  - key: user\n    rate_limits:";
    let both: &[&[(&str, &str)]] = &[
        &[("user", "alice")],
        &[("inflight", "alice"), ("model", "m")],
    ];
    let shadow = CONCURRENT.replacen(
        session,
        "  - key: user\n    shadow_mode: true\n    rate_limits:",
        1,
    );
    let h = Extended::start(&shadow, Settings::default()).await;
    h.at(1_000_000_000);
    h.call(hits(&both[..1], 4)).await;
    assert_eq!(h.call(hits(both, 0)).await.overall_code(), Code::Ok);
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "1");

    let quota = CONCURRENT.replacen(
        session,
        "  - key: user\n    quota_mode: true\n    rate_limits:",
        1,
    );
    let h = Extended::start(&quota, Settings::default()).await;
    h.at(1_000_000_000);
    h.call(hits(&both[..1], 4)).await;
    // Its only group is over: denied, no slot.
    assert_eq!(h.call(hits(both, 0)).await.overall_code(), Code::OverLimit);
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "0");
    // Another group is within its limit: the request passes, and takes its slot.
    let groups: &[&[(&str, &str)]] = &[
        &[("user", "alice")],
        &[("user", "bob")],
        &[("inflight", "alice"), ("model", "m")],
    ];
    assert_eq!(h.call(hits(groups, 0)).await.overall_code(), Code::Ok);
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "1");
}

/// A pipeline of checks failing on one key gives its request an error, so no amendment will
/// come: what it took on the others is released.
#[tokio::test]
async fn a_failed_check_holds_no_slot() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(CONCURRENT, Settings::default()).await;
    h.at(1_000_000_000);
    resp_command(
        &h.redis.address,
        &["SET", "{d_pool_gpu_}_cc_all", "not a set"],
    )
    .await;
    let both: &[&[(&str, &str)]] = &[&[("inflight", "alice"), ("model", "m")], &[("pool", "gpu")]];
    let answer = h.subject.service.should_rate_limit(&hits(both, 0)).await;
    assert!(answer.is_err(), "{answer:?}");
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "0");
}

/// In shadow mode a limit takes slots past its maximum and refuses nothing, as global shadow
/// mode does (ADR 0004).
#[tokio::test]
async fn shadow_mode_takes_slots_past_the_limit() {
    if !tools_available() {
        return;
    }
    let yaml = CONCURRENT.replace(
        "      - key: model\n",
        "      - key: model\n        shadow_mode: true\n",
    );
    let h = Extended::start(&yaml, Settings::default()).await;
    h.at(1_000_000_000);
    for _ in 0..3 {
        assert_eq!(h.call(hits(ALICE_SLOTS, 0)).await.overall_code(), Code::Ok);
    }
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "3");
    let shadowed = "ratelimit.service.rate_limit.d.inflight.model~slots.shadow_mode";
    assert_eq!(h.stats()[shadowed], 1);

    let settings = Settings {
        service: ServiceSettings {
            global_shadow_mode: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(CONCURRENT, settings).await;
    h.at(1_000_000_000);
    let both: &[&[(&str, &str)]] = &[
        &[("user", "alice")],
        &[("inflight", "alice"), ("model", "m")],
    ];
    // What global shadow mode lets through gets its slot, the session being over or not.
    h.call(hits(&both[..1], 4)).await;
    for _ in 0..3 {
        assert_eq!(h.call(hits(both, 0)).await.overall_code(), Code::Ok);
    }
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "3");
}

/// A concurrency limit denies the request whatever `QUOTA_MODE` says, and is refused at load
/// with a `quota_mode` of its own (ADR 0004). `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`, the
/// local cache and `REDIS_PERSECOND` change nothing for it.
#[tokio::test]
async fn other_settings_leave_concurrency_limits_alone() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: CacheSettings {
            stop_cache_key_increment_when_overlimit: true,
            local_cache_size_in_bytes: 1 << 20,
            ..Default::default()
        },
        service: ServiceSettings {
            global_quota_mode: true,
            ..Default::default()
        },
        per_second: true,
        ..Default::default()
    };
    let short = CONCURRENT.replace("lease: 10m", "lease: 30s");
    let h = Extended::start(&short, settings).await;
    h.at(1_000_000_000);
    for _ in 0..2 {
        assert_eq!(h.call(hits(ALICE_SLOTS, 0)).await.overall_code(), Code::Ok);
    }
    assert_eq!(
        h.call(hits(ALICE_SLOTS, 0)).await.overall_code(),
        Code::OverLimit
    );
    // On the main Redis, its short lease notwithstanding.
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "2");
    // Refused, but not remembered in the local cache: a released slot is free at once.
    h.call(hits(ALICE_SLOTS, 1)).await;
    assert_eq!(h.call(hits(ALICE_SLOTS, 0)).await.overall_code(), Code::Ok);
}

/// A model's name with `/`, `.`, `:` and what ADR 0001 escapes gives one key per model, so that
/// being limited on a model leaves the others free.
#[tokio::test]
async fn models_with_any_name_have_slots_of_their_own() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(CONCURRENT, Settings::default()).await;
    h.at(1_000_000_000);
    let on = |model: &'static str| [("inflight", "alice"), ("model", model)];
    let qwen = on("Qwen/Qwen3-32B:v1.5");
    let escaped = on("org/m%{x}");
    for _ in 0..3 {
        h.call(hits(&[&qwen], 0)).await;
    }
    assert_eq!(
        h.call(hits(&[&qwen], 0)).await.overall_code(),
        Code::OverLimit
    );
    assert_eq!(h.call(hits(&[&escaped], 0)).await.overall_code(), Code::Ok);
    assert_eq!(
        slots_held(
            &h.redis,
            "{d_inflight_alice_model_Qwen/Qwen3-32B:v1.5_}_cc_slots"
        )
        .await,
        "2"
    );
    assert_eq!(
        slots_held(
            &h.redis,
            "{d_inflight_alice_model_org/m%25%7Bx%7D_}_cc_slots"
        )
        .await,
        "1"
    );
}

/// The quota lookup shows what is in flight, leaving out what has expired, and changes
/// nothing.
#[tokio::test]
async fn quota_lookups_show_slots_in_flight() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(CONCURRENT, Settings::default()).await;
    h.at(1_000_000_000);
    h.call(hits(ALICE_SLOTS, 0)).await;
    let limit = async || {
        let lookup = h
            .subject
            .service
            .lookup(&request("d", ALICE_SLOTS))
            .await
            .unwrap();
        let rendered: serde_json::Value =
            serde_json::from_str(&llm_ratelimit::quota::render(&lookup)).unwrap();
        rendered["descriptors"][0]["limits"][0].clone()
    };
    let shown = limit().await;
    assert_eq!(shown["name"], "slots");
    assert_eq!(shown["in_flight"], 1);
    assert_eq!(shown["limit"], 2);
    assert_eq!(shown["remaining"], 1);
    assert_eq!(shown["used_pct"], 50.0);
    assert_eq!(shown["resets_at"], serde_json::Value::Null);
    assert_eq!(
        shown["window"],
        serde_json::json!({"type": "concurrency", "lease_s": 600, "retry_after_s": 2})
    );
    h.at(1_000_000_600);
    assert_eq!(limit().await["in_flight"], 0);
    // Not purged by the reading: only an operation does.
    assert_eq!(slots_held(&h.redis, ALICE_KEY).await, "1");
}

/// A service on a Redis Cluster's `nodes`, with its own clock.
async fn clustered_service(
    nodes: &[Redis],
    yaml: &str,
) -> (Arc<RateLimitService<RedisCache>>, Arc<ManualClock>) {
    let hosts: Vec<String> = nodes[1..]
        .iter()
        .map(|n| format!("node={}", n.address))
        .collect();
    let url = format!("redis-cluster://{}?{}", nodes[0].address, hosts.join("&"));
    let pool = Builder::from_config(Config::from_url(&url).unwrap())
        .build_pool(1)
        .unwrap();
    pool.init().await.unwrap();
    let clock = Arc::new(ManualClock::default());
    let stats = Arc::new(Stats::default());
    let cache = RedisCache::new(pool, clock.clone(), stats.clone(), CacheSettings::default());
    let service = Arc::new(RateLimitService::new(
        cache,
        clock.clone(),
        stats,
        ServiceSettings::default(),
    ));
    let file = ConfigFile {
        name: "config.yaml".into(),
        content: yaml.into(),
    };
    service.set_config(config::load(&[file], &LoadOptions::default()).unwrap());
    (service, clock)
}

/// The script decides as [`ConcurrencyWindow::step`] does, over random sequences through three
/// instances of the service on a Redis Cluster, for keys on several nodes (D32.4). Then, the
/// clock still, checks and amendments race across the instances: however they interleave,
/// the maximum holds and no slot is lost.
#[tokio::test]
async fn the_concurrency_script_steps_like_the_rust_window_on_a_cluster() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit::config::window::{ConcurrencyWindow, SlotOp};
    let cluster = RedisCluster::start().await;
    let instances = [
        clustered_service(&cluster.nodes, CONCURRENT).await,
        clustered_service(&cluster.nodes, CONCURRENT).await,
        clustered_service(&cluster.nodes, CONCURRENT).await,
    ];
    let at = |now: i64| {
        for (_, clock) in &instances {
            clock.0.store(now, Ordering::SeqCst);
        }
    };
    let window = ConcurrencyWindow {
        lease: 600,
        retry_after: 2,
    };
    // A fixed-seed LCG: the same sequence each run.
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = |n: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % n
    };
    let models = ["a/b:c", "d.e", "f", "g", "h", "i"];
    let mut now: i64 = 1_000_000_000;
    let mut states: Vec<Vec<i64>> = vec![Vec::new(); models.len()];
    for step in 0..600 {
        now += match next(10) {
            0 => 400 + next(400) as i64,
            1 => -(next(30) as i64),
            _ => next(120) as i64,
        };
        at(now);
        let which = next(models.len() as u64) as usize;
        let (service, _) = &instances[next(3) as usize];
        let (added, refund) = match next(5) {
            0 | 1 => (0, false),
            2 | 3 => (1 + next(3), false),
            _ => (next(3), true),
        };
        let mut request = hits(&[&[("inflight", "alice"), ("model", models[which])]], added);
        request.descriptors[0].is_negative_hits = refund;
        let op = match added == 0 && !refund {
            true => SlotOp::Acquire {
                max: 2,
                force: false,
            },
            false => SlotOp::Release(added),
        };
        let expected = window.step(&mut states[which], op, now);
        let status = service.should_rate_limit(&request).await.unwrap().statuses[0].clone();
        let refused = matches!(op, SlotOp::Acquire { .. }) && !expected.acquired;
        assert_eq!(
            (
                status.code() == Code::OverLimit,
                u64::from(status.limit_remaining)
            ),
            (
                refused,
                match refused {
                    true => 0,
                    false => 2u64.saturating_sub(expected.in_flight),
                }
            ),
            "step {step} at {now}: {op:?} on {}",
            models[which]
        );
    }
    // Far past every lease, so that only the race counts.
    now += 10_000;
    at(now);
    let racing: &[&[(&str, &str)]] = &[&[("inflight", "bob"), ("model", "m")]];
    let in_flight = async || {
        let lookup = instances[0]
            .0
            .lookup(&request("d", &[&[("inflight", "bob"), ("model", "m")]]))
            .await
            .unwrap();
        let rendered: serde_json::Value =
            serde_json::from_str(&llm_ratelimit::quota::render(&lookup)).unwrap();
        rendered["descriptors"][0]["limits"][0]["in_flight"]
            .as_u64()
            .unwrap()
    };
    let mut held: u64 = 0;
    for _ in 0..30 {
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..12 {
            let service = instances[i % 3].0.clone();
            tasks.spawn(async move {
                service
                    .should_rate_limit(&hits(&[&[("inflight", "bob"), ("model", "m")]], 0))
                    .await
                    .unwrap()
                    .overall_code()
                    == Code::Ok
            });
        }
        let mut granted = 0;
        while let Some(ok) = tasks.join_next().await {
            granted += u64::from(ok.unwrap());
        }
        held += granted;
        assert!(held <= 2, "{held} slots of 2");
        assert_eq!(in_flight().await, held);
        // A release through another instance than the one that acquired.
        let release = next(3).min(held);
        for i in 0..release {
            instances[((i + 1) % 3) as usize]
                .0
                .should_rate_limit(&hits(racing, 1))
                .await
                .unwrap();
        }
        held -= release;
        assert_eq!(in_flight().await, held);
    }
}

/// Three real services on one Redis: acquisitions and releases through any of them, with
/// nothing held in an instance's memory, and the misuse guard's stat there too.
#[tokio::test]
async fn several_instances_share_one_servers_slots() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("concurrency-instances");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/d.yaml", CONCURRENT.as_bytes()),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_llm-ratelimit"));
    let environment = binary_environment(&runtime, &redis);
    let mut services = Vec::new();
    let mut clients = Vec::new();
    for _ in 0..3 {
        let service = Service::start(&binary, &environment).await;
        clients.push(service.client().await);
        services.push(service);
    }
    let mut ask = async |i: usize, added: u64| {
        clients[i % 3]
            .should_rate_limit(hits(ALICE_SLOTS, added))
            .await
            .unwrap()
            .into_inner()
            .overall_code()
    };
    assert_eq!(ask(0, 0).await, Code::Ok);
    assert_eq!(ask(1, 0).await, Code::Ok);
    assert_eq!(ask(2, 0).await, Code::OverLimit);
    // Released through an instance that acquired nothing.
    assert_eq!(ask(2, 1).await, Code::Ok);
    assert_eq!(slots_held(&redis, ALICE_KEY).await, "1");
    assert_eq!(ask(0, 0).await, Code::Ok);
    assert_eq!(ask(1, 0).await, Code::OverLimit);
}

/// A stand-in for vLLM, by the request's model: `ok` answers, `unavailable` answers 503 as a flow
/// control TTL eviction does, `cut` starts a stream and closes it, `held` starts a stream
/// and keeps it open until the client leaves, `queued` answers nothing until it does.
async fn llm_stand_in() -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                let body_start = loop {
                    let Ok(n) = stream.read(&mut buffer).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..n]);
                    let text = String::from_utf8_lossy(&request);
                    if let Some(at) = text.find("\r\n\r\n") {
                        let length = text
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if request.len() >= at + 4 + length {
                            break at + 4;
                        }
                    }
                };
                let body = String::from_utf8_lossy(&request[body_start..]).into_owned();
                let usage = r#"{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;
                let chunk = "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n";
                let stream_head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{chunk}\r\n",
                    chunk.len()
                );
                if body.contains("\"model\":\"unavailable\"") {
                    let error = r#"{"error":{"message":"evicted","type":"unavailable"}}"#;
                    let answer = format!(
                        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{error}",
                        error.len()
                    );
                    let _ = stream.write_all(answer.as_bytes()).await;
                } else if body.contains("\"model\":\"cut\"") {
                    let _ = stream.write_all(stream_head.as_bytes()).await;
                    let _ = stream.flush().await;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                } else if body.contains("\"model\":\"queued\"") {
                    let _ = stream.read(&mut buffer).await;
                } else if body.contains("\"model\":\"held\"") {
                    let _ = stream.write_all(stream_head.as_bytes()).await;
                    let _ = stream.flush().await;
                    let _ = stream.read(&mut buffer).await;
                } else {
                    let answer = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{usage}",
                        usage.len()
                    );
                    let _ = stream.write_all(answer.as_bytes()).await;
                }
            });
        }
    });
    port
}

/// With a real agentgateway (`RATELIMIT_AGENTGATEWAY=<path>`, skipped otherwise), a slot is
/// taken by the check and released by the amendment after a response, a 503, a cut stream and
/// a client leaving a stream; the refused request gets its `retry-after` and no amendment.
/// A transport error or a client leaving before the headers sends no amendment, so the slot
/// waits for its lease: what `docs/upstream/agentgateway-amend-on-drop.md` proposes to change. Should this test fail on
/// that assertion, the docs on orphans can be relaxed.
#[tokio::test]
async fn a_real_agentgateway_takes_and_releases_slots() {
    let Ok(agentgateway) = std::env::var("RATELIMIT_AGENTGATEWAY") else {
        return;
    };
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("agentgateway-slots");
    let limits = "domain: llm
descriptors:
  - key: inflight
    descriptors:
      - key: model
        rate_limits:
          - {name: inflight, requests_per_unit: 2, window: {type: concurrency, lease: 10m, retry_after: 7s}}
";
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", limits.as_bytes()),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    let service = Service::start(
        std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
        &binary_environment(&runtime, &redis),
    )
    .await;
    service.client().await;
    let rls = service.grpc.trim_start_matches("http://").to_string();
    let llm = llm_stand_in().await;
    // Nothing listens there.
    let gone = reserved_port();
    let (llm_port, admin, stats, ready) = (
        reserved_port(),
        reserved_port(),
        reserved_port(),
        reserved_port(),
    );
    let model = |name: &str, port: u16| {
        format!(
            "  - name: {name}
    provider:
      custom:
        formats:
        - type: completions
    params:
      baseUrl: http://127.0.0.1:{port}/v1
      model: {name}
"
        )
    };
    let config = format!(
        "config:
  adminAddr: 127.0.0.1:{admin}
  statsAddr: 127.0.0.1:{stats}
  readinessAddr: 127.0.0.1:{ready}
llm:
  port: {llm_port}
  policies:
    remoteRateLimit:
      domain: llm
      host: {rls}
      descriptors:
      - entries:
        - key: inflight
          value: '\"alice\"'
        - key: model
          value: llm.requestModel
        type: tokens
        cost: '1'
  models:
{}{}{}{}{}{}",
        model("ok", llm),
        model("unavailable", llm),
        model("cut", llm),
        model("held", llm),
        model("queued", llm),
        model("gone", gone)
    );
    let path = runtime.join("agentgateway.yaml");
    std::fs::write(&path, config).unwrap();
    let mut gateway = Command::new(&agentgateway)
        .arg("-f")
        .arg(&path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("start agentgateway");
    let chat = |name: &str, stream: bool| {
        let body = format!(
            r#"{{"model":"{name}","stream":{stream},"messages":[{{"role":"user","content":"hi"}}]}}"#
        );
        format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: llm\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    };
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(("127.0.0.1", llm_port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(gateway.try_wait().unwrap().is_none(), "agentgateway exited");
    let address = format!("127.0.0.1:{llm_port}");
    let key = |name: &str| format!("{{llm_inflight_alice_model_{name}_}}_cc_inflight");
    // The amendment goes after the response: wait for it.
    let settles_at = async |name: &str, held: &str| {
        for _ in 0..100 {
            if slots_held(&redis, &key(name)).await == held {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!(
            "{name}: {} slots, not {held}",
            slots_held(&redis, &key(name)).await
        );
    };
    let status = |answer: &str| answer.split(' ').nth(1).unwrap_or_default().to_string();
    // A response, then a 503 such as a flow control eviction answers: both release.
    let answer = send_raw(&address, &chat("ok", false)).await;
    assert_eq!(status(&answer), "200", "{answer}");
    settles_at("ok", "0").await;
    let answer = send_raw(&address, &chat("unavailable", false)).await;
    assert_eq!(status(&answer), "503", "{answer}");
    settles_at("unavailable", "0").await;
    // A stream the model cuts releases.
    let answer = send_raw(&address, &chat("cut", true)).await;
    assert_eq!(status(&answer), "200", "{answer}");
    settles_at("cut", "0").await;
    // Two streams held take the two slots; the third is refused, with no amendment after it.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut held = Vec::new();
    for _ in 0..2 {
        let mut stream = tokio::net::TcpStream::connect(&address).await.unwrap();
        stream
            .write_all(chat("held", true).as_bytes())
            .await
            .unwrap();
        let mut first = [0u8; 64];
        let n = stream.read(&mut first).await.unwrap();
        assert!(first[..n].starts_with(b"HTTP/1.1 200"));
        held.push(stream);
    }
    settles_at("held", "2").await;
    let answer = send_raw(&address, &chat("held", true)).await;
    assert_eq!(status(&answer), "429", "{answer}");
    assert!(
        answer.to_ascii_lowercase().contains("retry-after: 7"),
        "{answer}"
    );
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(slots_held(&redis, &key("held")).await, "2");
    // Clients leaving their streams release.
    held.clear();
    settles_at("held", "0").await;
    // A transport error sends no amendment: an orphan, until its lease.
    let answer = send_raw(&address, &chat("gone", false)).await;
    assert!(status(&answer).starts_with('5'), "{answer}");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(slots_held(&redis, &key("gone")).await, "1");
    // So does a client leaving before the response headers, as when cancelled in a queue.
    let mut queued = tokio::net::TcpStream::connect(&address).await.unwrap();
    queued
        .write_all(chat("queued", true).as_bytes())
        .await
        .unwrap();
    settles_at("queued", "1").await;
    drop(queued);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(slots_held(&redis, &key("queued")).await, "1");
}

const BUCKET: &str = "
domain: d
descriptors:
  - key: minute
    rate_limits:
      - {name: b, requests_per_unit: 60, window: {type: token_bucket, duration: 60s}}
  - key: small
    rate_limits:
      - {name: b, requests_per_unit: 60, window: {type: token_bucket, duration: 60s, burst: 10, max_debt: 0}}
  - key: free
    rate_limits:
      - {name: b, requests_per_unit: 60, window: {type: token_bucket, duration: 60s, max_debt: unlimited}}
  - key: slow
    rate_limits:
      - {name: b, requests_per_unit: 7, window: {type: token_bucket, duration: 1h, burst: 3, max_debt: 10m}}
  - key: shadow
    shadow_mode: true
    rate_limits:
      - {name: b, requests_per_unit: 60, window: {type: token_bucket, duration: 60s, burst: 1}}
";

/// A bucket that is not empty admits any request, charged in full, down to `max_debt`, its
/// default a whole bucket; empty, it denies until it is not. Allowed, the reset is when it is
/// full; denied, when it admits again (D11, D12). A check takes nothing.
#[tokio::test]
async fn token_buckets_admit_until_empty_and_charge_in_full() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(BUCKET, Settings::default()).await;
    let alice: &[&[(&str, &str)]] = &[&[("minute", "alice")]];
    h.at(1_000_000_000);
    // A full bucket has no key to write.
    assert_eq!(
        h.statuses(hits(alice, 0)).await,
        owned([("Ok", "b", 60, 0)])
    );
    assert!(h.keys().await.is_empty());
    assert_eq!(
        h.statuses(hits(alice, 50)).await,
        owned([("Ok", "b", 10, 50)])
    );
    // 10 tokens left: admitted, in debt by 20.
    assert_eq!(
        h.statuses(hits(alice, 30)).await,
        owned([("Ok", "b", 0, 80)])
    );
    assert_eq!(
        h.statuses(hits(alice, 0)).await,
        owned([("OverLimit", "b", 0, 21)])
    );
    // At 0 tokens it still denies, a second before it admits.
    h.at(1_000_000_020);
    assert_eq!(
        h.statuses(hits(alice, 0)).await,
        owned([("OverLimit", "b", 0, 1)])
    );
    h.at(1_000_000_021);
    assert_eq!(
        h.statuses(hits(alice, 0)).await,
        owned([("Ok", "b", 1, 59)])
    );
    // The key is the deadline in microseconds, kept until it is full, and a margin.
    let keys = h.keys().await;
    let (value, ttl) = &keys["{d_minute_alice_}_tb60_b"];
    assert_eq!(value, "1000000080000000");
    assert_eq!(*ttl, 120);
    // Past the debt, the rest is truncated: the deadline stops at 2 buckets from now.
    assert_eq!(
        h.statuses(hits(alice, 1000)).await,
        owned([("Ok", "b", 0, 120)])
    );
    let stat = "ratelimit.service.rate_limit.d.minute~b.total_capped_hits";
    assert_eq!(h.stats()[stat], 939);
    // Denied hits are charged as well, as `overflow: envoy` says, up to the debt.
    assert_eq!(
        h.statuses(hits(alice, 5)).await,
        owned([("OverLimit", "b", 0, 61)])
    );
    assert_eq!(h.stats()[stat], 944);
}

/// `burst` is the capacity, whatever the rate; with `max_debt: 0` it stops at empty; with
/// `max_debt: unlimited`, nothing is truncated; as a duration, it is what refills in it.
#[tokio::test]
async fn token_buckets_follow_their_burst_and_max_debt() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(BUCKET, Settings::default()).await;
    h.at(1_000_000_000);
    let small: &[&[(&str, &str)]] = &[&[("small", "a")]];
    assert_eq!(
        h.statuses(hits(small, 0)).await,
        owned([("Ok", "b", 10, 0)])
    );
    assert_eq!(
        h.statuses(hits(small, 25)).await,
        owned([("Ok", "b", 0, 10)])
    );
    assert_eq!(
        h.statuses(hits(small, 0)).await,
        owned([("OverLimit", "b", 0, 1)])
    );
    let capped = "ratelimit.service.rate_limit.d.small~b.total_capped_hits";
    assert_eq!(h.stats()[capped], 15);

    let free: &[&[(&str, &str)]] = &[&[("free", "a")]];
    assert_eq!(
        h.statuses(hits(free, 1000)).await,
        owned([("Ok", "b", 0, 1000)])
    );
    assert_eq!(
        h.statuses(hits(free, 0)).await,
        owned([("OverLimit", "b", 0, 941)])
    );

    // 7 tokens an hour, whose 10 min of debt are 1.17 tokens, on a capacity of 3.
    let slow: &[&[(&str, &str)]] = &[&[("slow", "a")]];
    h.call(hits(slow, 100)).await;
    let keys = h.keys().await;
    let limit = (3.0 + 600.0 * 7.0 / 3600.0) * (3600.0 * 1e6 / 7.0);
    let expected = (1e15_f64 + limit).ceil();
    assert_eq!(keys["{d_slow_a_}_tb3600_b"].0, format!("{expected:.0}"));
}

/// A refund moves the deadline back, never before now, and is always OK, whatever it finds
/// (D21). Back at full, the key is gone.
#[tokio::test]
async fn refunds_move_a_token_buckets_deadline_back() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(BUCKET, Settings::default()).await;
    let alice: &[&[(&str, &str)]] = &[&[("minute", "alice")]];
    let refund = |n: u64| {
        let mut request = hits(alice, n);
        request.descriptors[0].is_negative_hits = true;
        request
    };
    h.at(1_000_000_000);
    h.call(hits(alice, 90)).await;
    assert_eq!(
        h.statuses(hits(alice, 0)).await,
        owned([("OverLimit", "b", 0, 31)])
    );
    assert_eq!(h.statuses(refund(40)).await, owned([("Ok", "b", 10, 50)]));
    assert_eq!(
        h.statuses(hits(alice, 0)).await,
        owned([("Ok", "b", 10, 50)])
    );
    assert_eq!(h.statuses(refund(500)).await, owned([("Ok", "b", 60, 0)]));
    assert!(h.keys().await.is_empty());
    assert_eq!(
        h.subject
            .stats
            .snapshot()
            .get("ratelimit.service.rate_limit.d.minute~b.total_negative_hits"),
        Some(&540)
    );
}

/// `within_limit` is what is admitted; `near_limit` and `over_limit` split usage as the legacy
/// does, usage being capacity minus what remains (D22).
#[tokio::test]
async fn token_bucket_stats_count_as_d22_says() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(BUCKET, Settings::default()).await;
    let alice: &[&[(&str, &str)]] = &[&[("minute", "alice")]];
    h.at(1_000_000_000);
    h.call(hits(alice, 40)).await;
    // 80 % of 60 is 48: 40 to 50 goes 2 past it.
    h.call(hits(alice, 10)).await;
    h.call(hits(alice, 20)).await;
    h.call(hits(alice, 20)).await;
    let snapshot = h.subject.stats.snapshot();
    let stat = |name: &str| {
        snapshot
            .get(&format!("ratelimit.service.rate_limit.d.minute~b.{name}"))
            .copied()
    };
    assert_eq!(stat("total_hits"), Some(90));
    assert_eq!(stat("within_limit"), Some(70));
    // 60 is the capacity: the third goes 10 past it, and 10 near it, as the legacy splits them.
    assert_eq!(stat("near_limit"), Some(12));
    assert_eq!(stat("over_limit"), Some(30));
    // In shadow mode, what is over counts there too: of 3, 1 fits, 1 goes over, 1 is truncated.
    let shadow: &[&[(&str, &str)]] = &[&[("shadow", "a")]];
    h.call(hits(shadow, 3)).await;
    assert_eq!(
        h.subject
            .stats
            .snapshot()
            .get("ratelimit.service.rate_limit.d.shadow~b.shadow_mode"),
        Some(&1)
    );
}

/// A bucket is charged whatever `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` does to the request's
/// other keys (D20).
#[tokio::test]
async fn stopping_increments_leaves_token_buckets_alone() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: CacheSettings {
            stop_cache_key_increment_when_overlimit: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let yaml = "
domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: b, requests_per_unit: 60, window: {type: token_bucket, duration: 60s}}
  - key: org
    rate_limit: {unit: hour, requests_per_unit: 1}
";
    let h = Extended::start(yaml, settings).await;
    h.at(1_000_000_000);
    // From the second request on, `org` is about to go over.
    for _ in 0..3 {
        h.call(request("d", &[&[("user", "a")], &[("org", "x")]]))
            .await;
    }
    assert_eq!(h.keys().await["{d_user_a_}_tb60_b"].0, "1000000003000000");
}

/// What the script stores and truncates is what [`TokenBucketWindow::step`] says, huge hits
/// included, up to where a double stops being exact.
#[tokio::test]
async fn the_token_bucket_script_stores_what_the_rust_window_says() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit::config::window::{BucketOp, MaxDebt, TokenBucketWindow};
    let h = Extended::start(BUCKET, Settings::default()).await;
    let buckets = [
        (
            "small",
            60,
            TokenBucketWindow {
                duration: 60,
                burst: Some(10),
                max_debt: Some(MaxDebt::Tokens(0)),
            },
        ),
        (
            "slow",
            7,
            TokenBucketWindow {
                duration: 3600,
                burst: Some(3),
                max_debt: Some(MaxDebt::Seconds(600)),
            },
        ),
        (
            "free",
            60,
            TokenBucketWindow {
                duration: 60,
                burst: None,
                max_debt: Some(MaxDebt::Unlimited),
            },
        ),
    ];
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = |n: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % n
    };
    for (key, requests, window) in buckets {
        let shape = window.duration;
        let stored_key = format!("{{d_{key}_k_}}_tb{shape}_b");
        let mut tat: Option<f64> = None;
        let mut now: i64 = 1_000_000_000;
        let mut capped: u64 = 0;
        for step in 0..120 {
            now += next(500) as i64;
            h.at(now);
            let op = BucketOp {
                now,
                hits: match next(5) {
                    0 => u64::MAX,
                    1 => next(1 << 40),
                    _ => next(300),
                },
                refund: next(6) == 0,
            };
            let expected = window.step(tat, op, requests);
            if expected.after != expected.before {
                tat = (expected.after > now as f64 * 1e6).then_some(expected.after);
            }
            // Counters wrap, as gostats' do.
            capped = capped.wrapping_add(expected.capped);
            let mut request = hits(&[&[(key, "k")]], op.hits);
            request.descriptors[0].is_negative_hits = op.refund;
            h.call(request).await;
            let reply = resp_command(&h.redis.address, &["GET", &stored_key]).await;
            let stored: Option<f64> = reply.lines().nth(1).map(|v| v.parse().unwrap());
            assert_eq!(stored, tat, "{key} step {step} at {now}: {op:?}");
        }
        let counted = h
            .subject
            .stats
            .snapshot()
            .get(&format!(
                "ratelimit.service.rate_limit.d.{key}~b.total_capped_hits"
            ))
            .copied()
            .unwrap_or(0);
        assert_eq!(counted, capped, "{key}");
    }
}

/// A denied bucket is kept in the local cache until it would admit, a refund clearing it; never
/// in shadow mode, its own or global (D18).
#[tokio::test]
async fn denied_token_buckets_are_kept_in_the_local_cache() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: CacheSettings {
            local_cache_size_in_bytes: 1 << 20,
            stop_cache_key_increment_when_overlimit: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(BUCKET, settings).await;
    let alice: &[&[(&str, &str)]] = &[&[("minute", "alice")]];
    let scripts = async || {
        let commands = h.redis.commands().await;
        commands.iter().filter(|c| c.contains("EVAL")).count()
    };
    h.at(1_000_000_000);
    h.call(hits(alice, 90)).await;
    // Denied, then answered from the entry, with what it has left.
    assert_eq!(
        h.statuses(hits(alice, 0)).await,
        owned([("OverLimit", "b", 0, 31)])
    );
    let before = scripts().await;
    h.at(1_000_000_010);
    assert_eq!(
        h.statuses(hits(alice, 0)).await,
        owned([("OverLimit", "b", 0, 21)])
    );
    assert_eq!(scripts().await, before);
    // Its entry ends when the bucket admits.
    h.at(1_000_000_031);
    assert_eq!(
        h.statuses(hits(alice, 0)).await,
        owned([("Ok", "b", 1, 59)])
    );
    assert!(scripts().await > before);
    // A refund clears it at once.
    h.call(hits(alice, 100)).await;
    h.call(hits(alice, 0)).await;
    let mut refund = hits(alice, 100);
    refund.descriptors[0].is_negative_hits = true;
    h.call(refund).await;
    assert_eq!(h.call(hits(alice, 0)).await.overall_code(), Code::Ok);
    // In shadow mode, nothing is cached.
    let shadow: &[&[(&str, &str)]] = &[&[("shadow", "a")]];
    h.call(hits(shadow, 2)).await;
    assert_eq!(h.call(hits(shadow, 0)).await.overall_code(), Code::Ok);
    let before = scripts().await;
    assert_eq!(h.call(hits(shadow, 0)).await.overall_code(), Code::Ok);
    assert!(scripts().await > before);

    let settings = Settings {
        cache: CacheSettings {
            local_cache_size_in_bytes: 1 << 20,
            global_shadow_mode: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(BUCKET, settings).await;
    h.at(1_000_000_000);
    h.call(hits(alice, 90)).await;
    // Denied, were it not for global shadow mode: the next amendment still counts.
    h.call(hits(alice, 0)).await;
    h.call(hits(alice, 5)).await;
    assert_eq!(
        h.keys().await["{d_minute_alice_}_tb60_b"].0,
        "1000000095000000"
    );
}

/// The quota lookup shows the bucket's level, in debt too, and when it is full, changing
/// nothing.
#[tokio::test]
async fn quota_lookups_show_a_token_buckets_level() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(BUCKET, Settings::default()).await;
    let alice: &[&[(&str, &str)]] = &[&[("minute", "alice")]];
    h.at(1_000_000_000);
    let limit = async || {
        let lookup = h
            .subject
            .service
            .lookup(&request("d", alice))
            .await
            .unwrap();
        let rendered: serde_json::Value =
            serde_json::from_str(&llm_ratelimit::quota::render(&lookup)).unwrap();
        rendered["descriptors"][0]["limits"][0].clone()
    };
    let full = limit().await;
    assert_eq!(full["level"], 60);
    assert_eq!(full["remaining"], 60);
    assert_eq!(full["used"], 0);
    assert_eq!(full["resets_in_s"], 0);
    assert_eq!(
        full["window"],
        serde_json::json!({"type": "token_bucket", "duration_s": 60, "burst": 60})
    );
    assert!(h.keys().await.is_empty());
    h.call(hits(alice, 90)).await;
    let debt = limit().await;
    assert_eq!(debt["level"], -30);
    assert_eq!(debt["remaining"], 0);
    assert_eq!(debt["used"], 90);
    assert_eq!(debt["used_pct"], 150.0);
    assert_eq!(debt["resets_in_s"], 90);
    assert_eq!(debt["limit"], 60);
}

/// `RateLimit-Reset` is the bucket's, from the status: the clock's re-read could not know it.
/// A bucket shares a list with other limits, each in its own key (D7).
#[tokio::test]
async fn token_buckets_answer_headers_and_share_lists() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        service: ServiceSettings {
            response_headers: Some(LimitHeaders::default()),
            ..Default::default()
        },
        ..Default::default()
    };
    let yaml = "
domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: rate, requests_per_unit: 60, window: {type: token_bucket, duration: 60s}}
      - {name: week, requests_per_unit: 1000, window: {type: fixed, duration: 7d}}
";
    let h = Extended::start(yaml, settings).await;
    let alice: &[&[(&str, &str)]] = &[&[("user", "alice")]];
    h.at(1_000_000_000);
    let response = h.call(hits(alice, 50)).await;
    let headers: BTreeMap<String, String> = response
        .response_headers_to_add
        .into_iter()
        .map(|header| (header.key, header.value))
        .collect();
    assert_eq!(headers["RateLimit-Limit"], "60");
    assert_eq!(headers["RateLimit-Remaining"], "10");
    assert_eq!(headers["RateLimit-Reset"], "50");
    assert_eq!(h.keys().await.len(), 2);
}

/// The script decides as [`TokenBucketWindow::step`] does, over random sequences through three
/// instances on a Redis Cluster, intervals that are not whole included (D32.4). Then charges
/// race across the instances: none is lost, since the deadline ends up the same.
#[tokio::test]
async fn the_token_bucket_script_steps_like_the_rust_window_on_a_cluster() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit::config::window::{BucketOp, MaxDebt, TokenBucketWindow};
    let cluster = RedisCluster::start().await;
    let instances = [
        clustered_service(&cluster.nodes, BUCKET).await,
        clustered_service(&cluster.nodes, BUCKET).await,
        clustered_service(&cluster.nodes, BUCKET).await,
    ];
    let at = |now: i64| {
        for (_, clock) in &instances {
            clock.0.store(now, Ordering::SeqCst);
        }
    };
    let buckets = [
        (
            "minute",
            60,
            TokenBucketWindow {
                duration: 60,
                burst: None,
                max_debt: None,
            },
        ),
        (
            "small",
            60,
            TokenBucketWindow {
                duration: 60,
                burst: Some(10),
                max_debt: Some(MaxDebt::Tokens(0)),
            },
        ),
        (
            "free",
            60,
            TokenBucketWindow {
                duration: 60,
                burst: None,
                max_debt: Some(MaxDebt::Unlimited),
            },
        ),
        (
            "slow",
            7,
            TokenBucketWindow {
                duration: 3600,
                burst: Some(3),
                max_debt: Some(MaxDebt::Seconds(600)),
            },
        ),
    ];
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = |n: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % n
    };
    for (key, requests, window) in buckets {
        let mut tat: Option<f64> = None;
        let mut now: i64 = 1_000_000_000;
        for step in 0..300 {
            now += match next(10) {
                0 => 200 + next(4000) as i64,
                1 => -(next(30) as i64),
                _ => next(90) as i64,
            };
            at(now);
            let op = BucketOp {
                now,
                hits: match next(4) {
                    0 => 0,
                    _ => next(40),
                },
                refund: next(6) == 0,
            };
            let expected = window.step(tat, op, requests);
            // The script writes only a change, and a full bucket has no key, which matters
            // when the clock goes back.
            if expected.after != expected.before {
                tat = (expected.after > now as f64 * 1e6).then_some(expected.after);
            }
            let mut request = hits(&[&[(key, "k")]], op.hits);
            request.descriptors[0].is_negative_hits = op.refund;
            let (service, _) = &instances[next(3) as usize];
            let status = service.should_rate_limit(&request).await.unwrap().statuses[0].clone();
            let admitted = op.refund || window.admits(expected.before, now, requests);
            let wanted = match admitted {
                true => (
                    Code::Ok,
                    window.remaining(expected.after, now, requests),
                    window.until_full(expected.after, now),
                ),
                false => (
                    Code::OverLimit,
                    0,
                    window.until_admitted(expected.after, now, requests),
                ),
            };
            assert_eq!(
                (
                    status.code(),
                    status.limit_remaining,
                    status.duration_until_reset.unwrap().seconds
                ),
                wanted,
                "{key} step {step} at {now}: {op:?}"
            );
        }
    }
    let now = 1_100_000_000;
    at(now);
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..30 {
        let service = instances[i % 3].0.clone();
        tasks.spawn(async move {
            service
                .should_rate_limit(&hits(&[&[("free", "race")]], 1))
                .await
                .unwrap();
        });
    }
    while tasks.join_next().await.is_some() {}
    let status = instances[0]
        .0
        .should_rate_limit(&hits(&[&[("free", "race")]], 0))
        .await
        .unwrap()
        .statuses[0]
        .clone();
    assert_eq!(status.limit_remaining, 30);
    assert_eq!(status.duration_until_reset.unwrap().seconds, 30);
}

/// What a bucket's settings may not do well is warned about at load: one taking a day to fill
/// (D11), and `quota_mode` with `max_debt: unlimited`, which a gauge reports too (D19).
#[tokio::test]
async fn token_buckets_warn_of_what_does_not_suit_them() {
    if !tools_available() {
        return;
    }
    let yaml = "domain: llm
descriptors:
  - key: day
    rate_limits:
      - {name: b, requests_per_unit: 1000, window: {type: token_bucket, duration: 1d}}
  - key: hours
    rate_limits:
      - {name: b, requests_per_unit: 1000, window: {type: token_bucket, duration: 23h}}
  - key: burst
    rate_limits:
      - {name: b, requests_per_unit: 269, window: {type: token_bucket, duration: 1h, burst: 6456}}
  - key: quota
    quota_mode: true
    rate_limits:
      - {name: b, requests_per_unit: 10, window: {type: token_bucket, duration: 1m, max_debt: unlimited}}
";
    let runtime = temp_dir("token-bucket-warnings");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", yaml.as_bytes()),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    let grpc = reserved_port();
    let mut environment = binary_environment(&runtime, &redis);
    environment.extend([
        ("LOG_LEVEL", "warn".to_string()),
        ("GRPC_HOST", "127.0.0.1".to_string()),
        ("GRPC_PORT", grpc.to_string()),
        ("HOST", "127.0.0.1".to_string()),
        ("PORT", reserved_port().to_string()),
        ("DEBUG_HOST", "127.0.0.1".to_string()),
        ("DEBUG_PORT", reserved_port().to_string()),
    ]);
    let stderr = logged_run(
        std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
        &environment,
        &format!("http://127.0.0.1:{grpc}"),
    )
    .await;
    assert!(
        stderr.contains("llm.day~b: this token_bucket takes a day"),
        "{stderr}"
    );
    assert!(!stderr.contains("llm.hours~b"), "{stderr}");
    assert!(
        stderr.contains("llm.burst~b: this token_bucket takes a day"),
        "{stderr}"
    );
    assert!(
        stderr.contains("llm.quota~b: quota_mode with max_debt: unlimited"),
        "{stderr}"
    );

    let h = Extended::start(
        &yaml.replace("domain: llm", "domain: d"),
        Settings::default(),
    )
    .await;
    assert_eq!(
        h.subject
            .stats
            .gauges()
            .get("ratelimit.service.config_lasting_lockouts"),
        Some(&1)
    );
    let h = Extended::start(BUCKET, Settings::default()).await;
    assert_eq!(
        h.subject
            .stats
            .gauges()
            .get("ratelimit.service.config_lasting_lockouts"),
        None
    );
    // On a config without any, the gauge goes back to 0; `QUOTA_MODE` counts as `quota_mode`.
    let h = Extended::start(
        &yaml.replace("domain: llm", "domain: d"),
        Settings::default(),
    )
    .await;
    let file = |content: &str| ConfigFile {
        name: "config.yaml".into(),
        content: content.into(),
    };
    h.subject
        .service
        .set_config(config::load(&[file(BUCKET)], &LoadOptions::default()).unwrap());
    let gauge = "ratelimit.service.config_lasting_lockouts";
    assert_eq!(h.subject.stats.gauges().get(gauge), Some(&0));
    let global = LoadOptions {
        global_quota_mode: true,
        ..Default::default()
    };
    h.subject
        .service
        .set_config(config::load(&[file(BUCKET)], &global).unwrap());
    assert_eq!(h.subject.stats.gauges().get(gauge), Some(&1));
}

/// A stand-in for vLLM answering with usage by model: `cached` reports cached tokens, `null` a
/// null count as some versions do, `plain` no details, and `streamed` the same as `cached` in
/// the stream's last chunk.
async fn llm_usage_stand_in() -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                let body_start = loop {
                    let Ok(n) = stream.read(&mut buffer).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..n]);
                    let text = String::from_utf8_lossy(&request);
                    if let Some(at) = text.find("\r\n\r\n") {
                        let length = text
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if request.len() >= at + 4 + length {
                            break at + 4;
                        }
                    }
                };
                let body = String::from_utf8_lossy(&request[body_start..]).into_owned();
                let details = |cached: &str| {
                    format!(
                        r#""usage":{{"prompt_tokens":1000,"completion_tokens":100,"total_tokens":1100{cached}}}"#
                    )
                };
                let with_cache = details(r#","prompt_tokens_details":{"cached_tokens":800}"#);
                if body.contains("\"model\":\"streamed\"") {
                    let chunks = format!(
                        "data: {{\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"hi\"}}}}]}}\n\ndata: {{\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[],{with_cache}}}\n\ndata: [DONE]\n\n"
                    );
                    let answer = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{chunks}",
                        chunks.len()
                    );
                    let _ = stream.write_all(answer.as_bytes()).await;
                    return;
                }
                let usage = if body.contains("\"model\":\"null\"") {
                    details(r#","prompt_tokens_details":{"cached_tokens":null}"#)
                } else if body.contains("\"model\":\"plain\"") {
                    details("")
                } else {
                    with_cache
                };
                let json = format!(
                    r#"{{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{{"index":0,"message":{{"role":"assistant","content":"hi"}},"finish_reason":"stop"}}],{usage}}}"#
                );
                let answer = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{json}",
                    json.len()
                );
                let _ = stream.write_all(answer.as_bytes()).await;
            });
        }
    });
    port
}

/// With a real agentgateway (`RATELIMIT_AGENTGATEWAY=<path>`), the `cost` of
/// `docs/examples/agentgateway.md` counts cached input at a tenth and output at 4, a null or
/// absent cached count as none, in a stream too; an expression that fails skips its
/// descriptor, so nothing is counted for it.
#[tokio::test]
async fn a_real_agentgateway_weighs_tokens_by_the_cache() {
    let Ok(agentgateway) = std::env::var("RATELIMIT_AGENTGATEWAY") else {
        return;
    };
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("agentgateway-cost");
    let limits = "domain: llm
descriptors:
  - key: weighted
    rate_limits:
      - {name: week, requests_per_unit: 1000000, window: {type: fixed, duration: 7d}}
  - key: failing
    rate_limits:
      - {name: week, requests_per_unit: 1000000, window: {type: fixed, duration: 7d}}
  - key: negative
    rate_limits:
      - {name: week, requests_per_unit: 1000000, window: {type: fixed, duration: 7d}}
";
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", limits.as_bytes()),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    let service = Service::start(
        std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
        &binary_environment(&runtime, &redis),
    )
    .await;
    service.client().await;
    let rls = service.grpc.trim_start_matches("http://").to_string();
    let llm = llm_usage_stand_in().await;
    let (llm_port, admin, stats, ready) = (
        reserved_port(),
        reserved_port(),
        reserved_port(),
        reserved_port(),
    );
    let model = |name: &str| {
        format!(
            "  - name: {name}
    provider:
      custom:
        formats:
        - type: completions
    params:
      baseUrl: http://127.0.0.1:{llm}/v1
      model: {name}
"
        )
    };
    let config = format!(
        "config:
  adminAddr: 127.0.0.1:{admin}
  statsAddr: 127.0.0.1:{stats}
  readinessAddr: 127.0.0.1:{ready}
llm:
  port: {llm_port}
  policies:
    remoteRateLimit:
      domain: llm
      host: {rls}
      descriptors:
      - entries:
        - key: weighted
          value: llm.requestModel
        type: tokens
        cost: >-
          int(default(llm.inputTokens, 0)) - int(default(llm.cachedInputTokens, 0))
          + int(default(llm.cachedInputTokens, 0)) / 10
          + 4 * int(default(llm.outputTokens, 0))
      - entries:
        - key: failing
          value: llm.requestModel
        type: tokens
        cost: llm.noSuchField + 1
      - entries:
        - key: negative
          value: llm.requestModel
        type: tokens
        cost: 0 - 1
  models:
{}{}{}{}",
        model("cached"),
        model("null"),
        model("plain"),
        model("streamed")
    );
    let path = runtime.join("agentgateway.yaml");
    std::fs::write(&path, config).unwrap();
    let mut gateway = Command::new(&agentgateway)
        .arg("-f")
        .arg(&path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("start agentgateway");
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(("127.0.0.1", llm_port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(gateway.try_wait().unwrap().is_none(), "agentgateway exited");
    let address = format!("127.0.0.1:{llm_port}");
    let chat = |name: &str, stream: bool| {
        let options = if stream {
            r#","stream_options":{"include_usage":true}"#
        } else {
            ""
        };
        let body = format!(
            r#"{{"model":"{name}","stream":{stream}{options},"messages":[{{"role":"user","content":"hi"}}]}}"#
        );
        format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: llm\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    };
    for (name, stream) in [
        ("cached", false),
        ("null", false),
        ("plain", false),
        ("streamed", true),
    ] {
        let answer = send_raw(&address, &chat(name, stream)).await;
        assert!(answer.starts_with("HTTP/1.1 200"), "{name}: {answer}");
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let counted: BTreeMap<String, String> = redis
        .dump()
        .await
        .into_iter()
        .map(|(key, (value, _))| (key, value))
        .collect();
    let of = |prefix: &str, name: &str| {
        let wanted = format!("{{llm_{prefix}_{name}_}}");
        counted
            .iter()
            .find(|(key, _)| key.starts_with(&wanted))
            .map(|(_, value)| value.as_str())
    };
    // 1000 of input, 800 of them cached, and 100 of output: 200 + 80 + 400.
    assert_eq!(of("weighted", "cached"), Some("680"), "{counted:?}");
    assert_eq!(of("weighted", "streamed"), Some("680"), "{counted:?}");
    // No count of cached tokens: all of it counts as fresh, 1000 + 400.
    assert_eq!(of("weighted", "null"), Some("1400"), "{counted:?}");
    assert_eq!(of("weighted", "plain"), Some("1400"), "{counted:?}");
    // The check's 0 hits create the key; an expression that fails adds nothing to it.
    assert_eq!(of("failing", "cached"), Some("0"), "{counted:?}");
    assert_eq!(of("negative", "cached"), Some("0"), "{counted:?}");
}

/// A loaded config's fingerprint is in a gauge per domain, the previous one going to 0 on a
/// reload, and in a log line at each load (D36).
#[tokio::test]
async fn config_fingerprints_are_reported_at_each_load() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(WEEK_AND_FIVE_HOURS, Settings::default()).await;
    let first = h.subject.service.current_config().unwrap().fingerprints()["d"].clone();
    let gauge = |fingerprint: &str| format!("ratelimit.service.config_fingerprint.d.{fingerprint}");
    assert_eq!(h.subject.stats.gauges().get(&gauge(&first)), Some(&1));
    let file = ConfigFile {
        name: "config.yaml".into(),
        content: WEEK_AND_FIVE_HOURS.replace("requests_per_unit: 3", "requests_per_unit: 4"),
    };
    h.subject
        .service
        .set_config(config::load(&[file], &LoadOptions::default()).unwrap());
    let second = h.subject.service.current_config().unwrap().fingerprints()["d"].clone();
    assert_ne!(second, first);
    let gauges = h.subject.stats.gauges();
    assert_eq!(gauges.get(&gauge(&first)), Some(&0));
    assert_eq!(gauges.get(&gauge(&second)), Some(&1));
    // A config without a list reports nothing, and what it reported goes to 0.
    let file = ConfigFile {
        name: "config.yaml".into(),
        content: "domain: d\ndescriptors:\n  - key: k\n    rate_limit: {unit: hour, requests_per_unit: 1}\n".into(),
    };
    h.subject
        .service
        .set_config(config::load(&[file], &LoadOptions::default()).unwrap());
    assert_eq!(h.subject.stats.gauges().get(&gauge(&second)), Some(&0));

    let runtime = temp_dir("config-fingerprint");
    let yaml = WEEK_AND_FIVE_HOURS.replace("domain: d", "domain: llm");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", yaml.as_bytes()),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    let grpc = reserved_port();
    let mut environment = binary_environment(&runtime, &redis);
    environment.extend([
        ("LOG_LEVEL", "info".to_string()),
        ("GRPC_HOST", "127.0.0.1".to_string()),
        ("GRPC_PORT", grpc.to_string()),
        ("HOST", "127.0.0.1".to_string()),
        ("PORT", reserved_port().to_string()),
        ("DEBUG_HOST", "127.0.0.1".to_string()),
        ("DEBUG_PORT", reserved_port().to_string()),
    ]);
    let hash = config::load(
        &[ConfigFile {
            name: "llm.yaml".into(),
            content: yaml.clone(),
        }],
        &LoadOptions::default(),
    )
    .unwrap()
    .fingerprints()["llm"]
        .clone();
    let stderr = logged_run(
        std::path::Path::new(env!("CARGO_BIN_EXE_llm-ratelimit")),
        &environment,
        &format!("http://127.0.0.1:{grpc}"),
    )
    .await;
    assert!(
        stderr.contains(&format!("Config fingerprint of domain llm: {hash}")),
        "{stderr}"
    );
}

/// The storage keys of every kind of limit, written out: they are what a deployment's counters
/// live under, so changing one resets them. Nothing of the project's name is in them.
#[tokio::test]
async fn storage_keys_stay_as_they_are() {
    if !tools_available() {
        return;
    }
    let yaml = "
domain: d
descriptors:
  - key: legacy
    rate_limit: {unit: hour, requests_per_unit: 100}
  - key: fixed
    rate_limits:
      - {name: week, requests_per_unit: 100, window: {type: fixed, duration: 7d, offset: 4d}}
  - key: anchored
    rate_limits:
      - {name: session, requests_per_unit: 100, window: {type: anchored, duration: 5h, start_rounding: 1h}}
  - key: bucket
    rate_limits:
      - {name: rate, requests_per_unit: 100, window: {type: token_bucket, duration: 60s}}
  - key: inflight
    descriptors:
      - key: model
        rate_limits:
          - {name: slots, requests_per_unit: 100, window: {type: concurrency, lease: 10m}}
";
    let settings = Settings {
        cache: CacheSettings {
            cache_key_prefix: "prefix_".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(yaml, settings).await;
    h.at(1_000_000_000);
    for descriptor in [
        &[("legacy", "a")][..],
        &[("fixed", "a")][..],
        &[("anchored", "a")][..],
        &[("bucket", "a")][..],
    ] {
        h.call(hits(&[descriptor], 1)).await;
    }
    h.call(hits(&[&[("inflight", "a"), ("model", "org/m:v1.5")]], 0))
        .await;
    let reply = resp_command(&h.redis.address, &["KEYS", "*"]).await;
    let mut keys: Vec<&str> = reply
        .lines()
        .filter(|line| !line.starts_with(['*', '$']))
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "prefix_d_legacy_a_999997200",
            "{prefix_d_anchored_a_}_an18000r3600_session",
            "{prefix_d_bucket_a_}_tb60_rate",
            "{prefix_d_fixed_a_}_fx604800o345600_999475200_week",
            "{prefix_d_inflight_a_model_org/m:v1.5_}_cc_slots",
        ]
    );
}

/// The stats of every kind of limit, and of the service, written out, with what the shipped
/// mapper makes of them: dashboards and alerts depend on these names.
#[tokio::test]
async fn stat_and_metric_names_stay_as_they_are() {
    use llm_ratelimit::stats::prometheus::{self, MetricType, PrometheusSettings};
    if !tools_available() {
        return;
    }
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: legacy
    rate_limit: {unit: hour, requests_per_unit: 1}
  - key: fixed
    rate_limits:
      - {name: week, requests_per_unit: 1, window: {type: fixed, duration: 7d}}
  - key: bucket
    rate_limits:
      - {name: rate, requests_per_unit: 1, window: {type: token_bucket, duration: 60s, max_debt: 0}}
  - key: slots
    rate_limits:
      - {name: inflight, requests_per_unit: 1, window: {type: concurrency, lease: 10m}}
",
        Settings::default(),
    )
    .await;
    h.at(1_000_000_000);
    for descriptor in [
        &[("legacy", "a")][..],
        &[("fixed", "a")][..],
        &[("bucket", "a")][..],
    ] {
        h.call(hits(&[descriptor], 3)).await;
    }
    let slots: &[&[(&str, &str)]] = &[&[("slots", "a")]];
    for added in [0, 0, 2] {
        h.call(hits(slots, added)).await;
    }
    let mut refund = hits(&[&[("fixed", "a")]], 1);
    refund.descriptors[0].is_negative_hits = true;
    h.call(refund).await;
    let fingerprint = h.subject.service.current_config().unwrap().fingerprints()["d"].clone();
    let mut stats: Vec<String> = h.subject.stats.snapshot().into_keys().collect();
    stats.extend(h.subject.stats.gauges().into_keys());
    stats.sort();
    let stats: Vec<String> = stats
        .into_iter()
        .map(|name| name.replace(&fingerprint, "FINGERPRINT"))
        .collect();
    assert_eq!(stats, GOLDEN_STATS);
    let mapper = prometheus::load_mapper(&PrometheusSettings::default()).unwrap();
    let mut metrics: Vec<String> = stats
        .iter()
        .chain(&GOLDEN_OTHER_STATS.map(str::to_string))
        .map(|stat| {
            let stat = stat.replace("FINGERPRINT", &fingerprint);
            let mapped = [MetricType::Counter, MetricType::Gauge, MetricType::Observer]
                .into_iter()
                .find_map(|kind| mapper.get_mapping(&stat, kind));
            mapped.map_or(format!("{stat} unmapped"), |m| m.name.clone())
        })
        .collect();
    metrics.sort();
    metrics.dedup();
    assert_eq!(metrics, GOLDEN_METRICS);
}

const GOLDEN_STATS: [&str; 19] = [
    "ratelimit.service.config_fingerprint.d.FINGERPRINT",
    "ratelimit.service.config_load_success",
    "ratelimit.service.rate_limit.d.bucket~rate.near_limit",
    "ratelimit.service.rate_limit.d.bucket~rate.total_capped_hits",
    "ratelimit.service.rate_limit.d.bucket~rate.total_hits",
    "ratelimit.service.rate_limit.d.bucket~rate.within_limit",
    "ratelimit.service.rate_limit.d.fixed~week.near_limit",
    "ratelimit.service.rate_limit.d.fixed~week.over_limit",
    "ratelimit.service.rate_limit.d.fixed~week.total_hits",
    "ratelimit.service.rate_limit.d.fixed~week.total_negative_hits",
    "ratelimit.service.rate_limit.d.legacy.near_limit",
    "ratelimit.service.rate_limit.d.legacy.over_limit",
    "ratelimit.service.rate_limit.d.legacy.total_hits",
    "ratelimit.service.rate_limit.d.slots~inflight.over_limit",
    "ratelimit.service.rate_limit.d.slots~inflight.releases_without_slot",
    "ratelimit.service.rate_limit.d.slots~inflight.slots_acquired",
    "ratelimit.service.rate_limit.d.slots~inflight.slots_released",
    "ratelimit.service.rate_limit.d.slots~inflight.total_hits",
    "ratelimit.service.rate_limit.d.slots~inflight.within_limit",
];
/// Stats no decision above produces.
const GOLDEN_OTHER_STATS: [&str; 7] = [
    "ratelimit_server.ShouldRateLimit.total_requests",
    "ratelimit_server.ShouldRateLimit.response_time",
    "ratelimit.service.call.should_rate_limit.redis_error",
    "ratelimit.quota_lookup.total_requests",
    "ratelimit.quota_lookup.error.backend",
    "ratelimit.quota_lookup.response_time",
    "ratelimit.redis_pool.cx_active",
];
const GOLDEN_METRICS: [&str; 18] = [
    "ratelimit.quota_lookup.total_requests unmapped",
    "ratelimit.redis_pool.cx_active unmapped",
    "ratelimit.service.rate_limit.d.fixed~week.total_negative_hits unmapped",
    "ratelimit_quota_lookup_errors",
    "ratelimit_quota_lookup_response_time_seconds",
    "ratelimit_service_config_fingerprint",
    "ratelimit_service_config_load_success",
    "ratelimit_service_rate_limit_near_limit",
    "ratelimit_service_rate_limit_over_limit",
    "ratelimit_service_rate_limit_releases_without_slot",
    "ratelimit_service_rate_limit_slots_acquired",
    "ratelimit_service_rate_limit_slots_released",
    "ratelimit_service_rate_limit_total_capped_hits",
    "ratelimit_service_rate_limit_total_hits",
    "ratelimit_service_rate_limit_within_limit",
    "ratelimit_service_response_time_seconds",
    "ratelimit_service_should_rate_limit_error",
    "ratelimit_service_total_requests",
];

/// `LIMIT_PER_UNIT_HEADERS_ENABLED` describes every limit of every list, a window of several
/// units under its own name (D13); `RateLimit-Reset` is the representative's window's,
/// offset included, re-read from the clock.
#[tokio::test]
async fn headers_describe_list_limits() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        service: ServiceSettings {
            response_headers: Some(LimitHeaders::default()),
            per_unit_headers: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: five_hour, requests_per_unit: 3, window: {type: fixed, duration: 5h}}
      - {name: weekly, requests_per_unit: 5, window: {type: fixed, duration: 7d, offset: 4d}}
  - key: org
    rate_limit: {unit: hour, requests_per_unit: 9}
",
        settings,
    )
    .await;
    h.at(1_000_000_000);
    let headers: Vec<(String, String)> = h
        .call(request("d", &[&[("user", "a")], &[("org", "x")]]))
        .await
        .response_headers_to_add
        .into_iter()
        .map(|h| (h.key, h.value))
        .collect();
    let pairs = |rows: &[(&str, &str)]| -> Vec<(String, String)> {
        rows.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    assert_eq!(
        headers,
        pairs(&[
            ("RateLimit-Limit-5Hours", "3"),
            ("RateLimit-Remaining-5Hours", "2"),
            ("RateLimit-Limit-Weeks", "5"),
            ("RateLimit-Remaining-Weeks", "4"),
            ("RateLimit-Limit-Hours", "9"),
            ("RateLimit-Remaining-Hours", "8"),
            ("RateLimit-Limit", "3"),
            ("RateLimit-Remaining", "2"),
            ("RateLimit-Reset", "8000"),
        ])
    );
}

/// `REDIS_PERSECOND` (D15): a list with a window shorter than a minute goes, whole, to the
/// per-second Redis; others stay on the main one.
#[tokio::test]
async fn lists_with_a_short_window_go_to_the_per_second_redis() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        per_second: true,
        ..Default::default()
    };
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: burst
    rate_limits:
      - {name: ten_seconds, requests_per_unit: 5, window: {type: fixed, duration: 10s}}
      - {name: daily, requests_per_unit: 50, window: {type: fixed, duration: 1d}}
  - key: steady
    rate_limits:
      - {name: minute, requests_per_unit: 5, window: {type: fixed, duration: 1m}}
",
        settings,
    )
    .await;
    h.at(1_000_000_000);
    h.call(request("d", &[&[("burst", "a")], &[("steady", "a")]]))
        .await;
    let main: Vec<String> = h.keys().await.into_keys().collect();
    let per_second: Vec<String> = Extended::keys_of(h.per_second.as_ref().unwrap())
        .await
        .into_keys()
        .collect();
    assert_eq!(main, ["{d_steady_a_}_fx60_999999960_minute"]);
    assert_eq!(
        per_second,
        [
            "{d_burst_a_}_fx10_1000000000_ten_seconds",
            "{d_burst_a_}_fx86400_999993600_daily",
        ]
    );
}

/// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` leaves lists alone (D20): their hits are always
/// counted, whatever the request's other keys, and a list going over does not stop them.
#[tokio::test]
async fn stopping_increments_over_the_limit_leaves_lists_alone() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: CacheSettings {
            stop_cache_key_increment_when_overlimit: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: hourly, requests_per_unit: 1, window: {type: fixed, duration: 1h}}
  - key: org
    rate_limit: {unit: hour, requests_per_unit: 1}
",
        settings,
    )
    .await;
    h.at(1_000_000_000);
    // From the second request on, `org` is about to go over: only keys going over count,
    // but the list's always does.
    for _ in 0..3 {
        h.call(request("d", &[&[("user", "a")], &[("org", "x")]]))
            .await;
    }
    let values: BTreeMap<String, String> = h
        .keys()
        .await
        .into_iter()
        .map(|(k, (v, _))| (k, v))
        .collect();
    assert_eq!(
        values,
        BTreeMap::from([
            ("d_org_x_999997200".to_string(), "3".to_string()),
            (
                "{d_user_a_}_fx3600_999997200_hourly".to_string(),
                "3".to_string()
            ),
        ])
    );
    // A client limit with a multiplier counts like a list limit, under its limit too.
    let mut multiplied = request("d", &[&[("user", "alice")], &[("org", "y")]]);
    multiplied.descriptors[0].limit = client_limit(Some(2)).descriptors[0].limit.take();
    multiplied.descriptors[0]
        .limit
        .as_mut()
        .unwrap()
        .requests_per_unit = 10;
    for _ in 0..3 {
        h.call(multiplied.clone()).await;
    }
    assert_eq!(h.keys().await["{d_user_alice_}_fx7200_999993600_"].0, "3");
}

/// On Memcached (D24), list limits count in ADR 0001's keys too, decided on what `GET` reads
/// and increased after the answer.
#[tokio::test]
async fn list_limits_count_on_memcached() {
    if !memcached_available() {
        return;
    }
    let memcached = Memcached::start().await;
    let servers =
        llm_ratelimit::memcached::client::resolve(std::slice::from_ref(&memcached.address))
            .await
            .unwrap();
    let client = Arc::new(llm_ratelimit::memcached::client::Client::new(
        servers, 2, None,
    ));
    let clock = Arc::new(ManualClock::default());
    let stats = Arc::new(Stats::default());
    let cache = llm_ratelimit::memcached::MemcacheCache::new(
        client,
        clock.clone(),
        stats.clone(),
        &stats.scope_with_tags("ratelimit"),
        CacheSettings::default(),
        None,
    );
    let service = RateLimitService::new(cache, clock.clone(), stats, ServiceSettings::default());
    let file = ConfigFile {
        name: "config.yaml".into(),
        content: WEEK_AND_FIVE_HOURS.into(),
    };
    service.set_config(config::load(&[file], &LoadOptions::default()).unwrap());
    clock.0.store(1_000_000_000, Ordering::SeqCst);
    let five_hour = "{d_user_alice_}_fx18000_999990000_five_hour";
    let weekly = "{d_user_alice_}_fx604800o345600_999475200_weekly";
    let value = async |key: &str| {
        let answer = memcached.ask(&format!("get {key}\r\n")).await;
        answer
            .lines()
            .nth(1)
            .filter(|_| answer.starts_with("VALUE"))
            .map(str::to_string)
    };
    let mut seen = Vec::new();
    for n in 1..=4 {
        let response = service
            .should_rate_limit(&request("d", &[&[("user", "alice")]]))
            .await
            .unwrap();
        let s = &response.statuses[0];
        seen.push((
            format!("{:?}", s.code()),
            s.current_limit.as_ref().unwrap().name.clone(),
            s.limit_remaining,
            s.duration_until_reset.as_ref().unwrap().seconds,
        ));
        // The increments are sent after the answer.
        for _ in 0..100 {
            if value(weekly).await == Some(n.to_string()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    assert_eq!(
        seen,
        owned([
            ("Ok", "five_hour", 2, 8000),
            ("Ok", "five_hour", 1, 8000),
            ("Ok", "five_hour", 0, 8000),
            ("OverLimit", "five_hour", 0, 8000),
        ])
    );
    assert_eq!(value(five_hour).await.as_deref(), Some("4"));
    assert_eq!(value(weekly).await.as_deref(), Some("4"));
}

/// On a tie (D17), the first limit of the list stands for it: among those with the fewest
/// remaining hits, and among those over with the same reset.
#[tokio::test]
async fn ties_are_won_by_the_first_list_limit() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: a, requests_per_unit: 1, window: {type: fixed, duration: 1h}}
      - {name: b, requests_per_unit: 1, window: {type: fixed, duration: 1h}}
      - {name: c, requests_per_unit: 9, window: {type: fixed, duration: 1d}}
",
        Settings::default(),
    )
    .await;
    h.at(1_000_000_000);
    let call = || h.statuses(request("d", &[&[("user", "x")]]));
    assert_eq!(call().await, owned([("Ok", "a", 0, 800)]));
    // `a` and `b` over with the same reset; `c` has the longest reset but is not over.
    assert_eq!(call().await, owned([("OverLimit", "a", 0, 800)]));
}

/// `quota_mode` (D17): a list's descriptor is exhausted when one of its limits is, and the
/// request over only when every group is; a descriptor whose limits are all replaced leaves
/// its group but is still among `passedBackends`.
#[tokio::test]
async fn quota_groups_see_a_list_exhausted_when_one_limit_is() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        service: ServiceSettings {
            response_dynamic_metadata: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: model_name_override
    quota_mode: true
    rate_limits:
      - {name: minute, requests_per_unit: 1, window: {type: fixed, duration: 1m}}
      - {name: day, requests_per_unit: 100, window: {type: fixed, duration: 1d}}
  - key: backend_name
    quota_mode: true
    rate_limits:
      - {name: backend_hour, requests_per_unit: 1, window: {type: fixed, duration: 1h}}
  - key: override
    rate_limit: {unit: hour, requests_per_unit: 100, replaces: [{name: backend_hour}]}
",
        settings,
    )
    .await;
    h.at(1_000_000_000);
    use llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::Code;
    let models: &[&[(&str, &str)]] = &[
        &[("model_name_override", "a")],
        &[("model_name_override", "b")],
    ];
    h.call(request("d", &models[..1])).await;
    // `a`'s minute is over, `b` is not: the request passes, through `b`.
    let response = h.call(request("d", models)).await;
    assert_eq!(response.overall_code(), Code::Ok);
    let backends = |response: &RateLimitResponse| {
        let metadata = response.dynamic_metadata.as_ref().unwrap();
        format!("{:?}", metadata.fields.get("passedBackends"))
    };
    assert!(
        backends(&response).contains("\"b\""),
        "{}",
        backends(&response)
    );
    assert!(
        !backends(&response).contains("\"a\""),
        "{}",
        backends(&response)
    );
    // Both exhausted.
    let response = h.call(request("d", models)).await;
    assert_eq!(response.overall_code(), Code::OverLimit);
    // `backend_name`'s only limit is replaced: out of the groups, but a passed backend
    // (listed when the request has a quota group, here `c`'s).
    let replaced: &[&[(&str, &str)]] = &[
        &[("backend_name", "x")],
        &[("override", "y")],
        &[("model_name_override", "c")],
    ];
    for _ in 0..2 {
        let response = h.call(request("d", replaced)).await;
        assert_eq!(response.overall_code(), Code::Ok);
        let passed = backends(&response);
        assert!(
            passed.contains("\"x\"") && passed.contains("\"c\""),
            "{passed}"
        );
        h.at(1_000_000_000 + 60);
    }
}

const SESSION: &str = "
domain: d
descriptors:
  - key: user
    rate_limits:
      - name: five_hour
        requests_per_unit: 3
        window: {type: anchored, duration: 5h, start_rounding: 1h}
      - name: weekly
        requests_per_unit: 50
        window: {type: fixed, duration: 7d, offset: 4d}
  - key: first
    rate_limits:
      - name: session
        requests_per_unit: 3
        window: {type: anchored, duration: 1h, open_on: first_hit}
  - key: org
    rate_limit: {unit: hour, requests_per_unit: 0}
  - key: solo
    rate_limits:
      - name: five_hour
        requests_per_unit: 3
        window: {type: anchored, duration: 5h, start_rounding: 1h}
";

/// A hash's fields and values (`HGETALL`).
async fn hash_fields(redis: &Redis, key: &str) -> BTreeMap<String, String> {
    let flat: Vec<String> = redis
        .client
        .custom(
            CustomCommand::new_static("HGETALL", ClusterHash::FirstKey, false),
            vec![key],
        )
        .await
        .unwrap();
    flat.chunks(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect()
}

/// `request` with each descriptor's `hits_addend` set to `n`.
fn hits(descriptors: &[&[(&str, &str)]], n: u64) -> RateLimitRequest {
    let mut request = request("d", descriptors);
    for descriptor in &mut request.descriptors {
        descriptor.hits_addend = Some(n);
    }
    request
}

/// An anchored window (D9) opens with the first request, at the rounded hour, lasts its
/// duration from there whatever comes, and the next request after it opens the next; its key
/// holds the anchor and the count, until the window's end and a margin.
#[tokio::test]
async fn anchored_windows_open_on_a_request_and_last_their_duration() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        service: ServiceSettings {
            response_headers: Some(LimitHeaders::default()),
            ..Default::default()
        },
        ..Default::default()
    };
    let h = Extended::start(SESSION, settings).await;
    let alice: &[&[(&str, &str)]] = &[&[("user", "alice")]];
    // 01:46:40 UTC: the window starts at 01:00 and ends at 06:00, in 15 200 s.
    h.at(1_000_000_000);
    assert_eq!(
        h.statuses(hits(alice, 0)).await,
        owned([("Ok", "five_hour", 3, 15_200)])
    );
    h.at(1_000_010_000);
    assert_eq!(
        h.statuses(hits(alice, 2)).await,
        owned([("Ok", "five_hour", 1, 5_200)])
    );
    let response = h.call(hits(alice, 2)).await;
    let headers: Vec<(String, String)> = response
        .response_headers_to_add
        .iter()
        .map(|h| (h.key.clone(), h.value.clone()))
        .collect();
    assert_eq!(
        headers,
        [
            ("RateLimit-Limit".to_string(), "3".to_string()),
            ("RateLimit-Remaining".to_string(), "0".to_string()),
            ("RateLimit-Reset".to_string(), "5200".to_string()),
        ]
    );
    let key = "{d_user_alice_}_an18000r3600_five_hour";
    let fields = hash_fields(&h.redis, key).await;
    assert_eq!(
        fields,
        BTreeMap::from([("a".into(), "999997200".into()), ("c".into(), "4".into())])
    );
    let ttl: i64 = h.redis.client.ttl(key).await.unwrap();
    // Set when the window opened: its end, 15 200 s later, and the margin.
    assert!((15_200 + 60 - 5..=15_200 + 60).contains(&ttl), "{ttl}");
    // 06:00: closed; this request opens the next window, at 06:00, and counts in it.
    h.at(999_997_200 + 18_000);
    assert_eq!(
        h.statuses(hits(alice, 1)).await,
        owned([("Ok", "five_hour", 2, 18_000)])
    );
    let fields = hash_fields(&h.redis, key).await;
    assert_eq!(fields["a"], "1000015200");
    // Hits past 2^53 saturate there, still over the limit, never wrapping below it.
    for _ in 0..2 {
        assert_eq!(
            h.statuses(hits(alice, u64::MAX)).await,
            owned([("OverLimit", "five_hour", 0, 18_000)])
        );
    }
    assert_eq!(
        hash_fields(&h.redis, key).await["c"],
        (1u64 << 53).to_string()
    );
}

/// `open_on: first_hit`: a 0-hit check leaves the window closed (and answers for the full
/// budget and duration); the first counted hit opens it.
#[tokio::test]
async fn first_hit_windows_wait_for_counted_hits() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(SESSION, Settings::default()).await;
    let first: &[&[(&str, &str)]] = &[&[("first", "x")]];
    h.at(1_000_000_000);
    assert_eq!(
        h.statuses(hits(first, 0)).await,
        owned([("Ok", "session", 3, 3_600)])
    );
    assert!(h.keys().await.is_empty(), "{:?}", h.keys().await);
    h.at(1_000_000_100);
    assert_eq!(
        h.statuses(hits(first, 1)).await,
        owned([("Ok", "session", 2, 3_600)])
    );
    h.at(1_000_000_200);
    assert_eq!(
        h.statuses(hits(first, 0)).await,
        owned([("Ok", "session", 2, 3_500)])
    );
}

/// A window a denied request opens without hits stays open with its whole budget, for hits
/// that will not come: counted in `opened_without_hits` (D9). One opened by a request that
/// passes is not: its hits follow.
#[tokio::test]
async fn windows_opened_without_hits_are_counted() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::Code;
    let h = Extended::start(SESSION, Settings::default()).await;
    h.at(1_000_000_000);
    // A 0-hit check of `alice` and `org`, which allows nothing: it passes (0 is not over 0).
    let response = h
        .call(hits(&[&[("user", "alice")], &[("org", "o")]], 0))
        .await;
    assert_eq!(response.overall_code(), Code::Ok);
    // `bob`'s 0-hit check with a hit on `org`: denied.
    let mut denied = hits(&[&[("user", "bob")], &[("org", "o")]], 0);
    denied.descriptors[1].hits_addend = Some(1);
    assert_eq!(h.call(denied.clone()).await.overall_code(), Code::OverLimit);
    // Again: `bob`'s window is already open.
    assert_eq!(h.call(denied).await.overall_code(), Code::OverLimit);
    assert_eq!(
        h.stats()
            .get("ratelimit.service.rate_limit.d.user~five_hour.opened_without_hits"),
        Some(&1)
    );
    // Upstream's limits never show it.
    assert!(
        !h.subject
            .stats
            .snapshot()
            .keys()
            .any(|k| k.contains("org") && k.contains("opened"))
    );
}

/// D32.4: the Redis script decides every request as [`AnchoredWindow::step`] does, over a
/// random sequence of checks, hits and refunds, both windows' kinds.
#[tokio::test]
async fn the_anchored_script_steps_like_the_rust_window() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit::config::window::{Anchored, AnchoredOp, AnchoredWindow, OpenOn};
    let h = Extended::start(SESSION, Settings::default()).await;
    let windows = [
        (
            "solo",
            "five_hour",
            AnchoredWindow {
                duration: 18_000,
                rounding: Some(3_600),
                open_on: OpenOn::Check,
            },
        ),
        (
            "first",
            "session",
            AnchoredWindow {
                duration: 3_600,
                rounding: None,
                open_on: OpenOn::FirstHit,
            },
        ),
    ];
    // A fixed-seed LCG: the same sequence each run.
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = |n: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % n
    };
    for (key, name, window) in windows {
        let mut state: Option<Anchored> = None;
        let mut now: i64 = 1_000_000_000;
        for step in 0..400 {
            // Mostly small steps, sometimes past the window, sometimes back in time.
            now += match next(10) {
                0 => window.duration + next(7_200) as i64,
                1 => -(next(600) as i64),
                _ => next(900) as i64,
            };
            let op = AnchoredOp {
                now,
                hits: match next(4) {
                    0 => 0,
                    _ => next(5),
                },
                refund: next(6) == 0,
            };
            let expected = window.step(state, op);
            state = expected.state;
            h.at(now);
            let mut request = hits(&[&[(key, "k")]], op.hits);
            request.descriptors[0].is_negative_hits = op.refund;
            let status = h.call(request).await.statuses[0].clone();
            let limit = 3u64;
            let count = expected.window.map_or(0, |w| w.count);
            let remaining = match op.refund {
                true => limit.saturating_sub(count),
                false if count > limit => 0,
                false => limit - count,
            };
            assert_eq!(
                (
                    status.limit_remaining as u64,
                    status.duration_until_reset.unwrap().seconds
                ),
                (remaining, window.reset(expected.window, now)),
                "{name}, step {step}: {op:?} after {state:?}"
            );
        }
    }
}

// The quota lookup (ADR 0003): every limit's usage and reset, read without changing anything.

const QUOTAS: &str = "
domain: llm-quota
descriptors:
  - key: workspace
    descriptors:
      - key: user
        rate_limits:
          - name: five_hour
            requests_per_unit: 100
            window: {type: anchored, duration: 5h, start_rounding: 1h}
          - name: weekly
            requests_per_unit: 1000
            window: {type: fixed, duration: 7d, offset: hashed}
  - key: workspace_total
    rate_limits:
      - name: weekly
        requests_per_unit: 10000
        window: {type: fixed, duration: 7d, offset: 4d}
  - key: legacy
    rate_limit: {unit: hour, requests_per_unit: 10, name: hourly}
  - key: free
    rate_limit: {unlimited: true, name: free}
";

/// Every key with its value (a hash's fields, for an anchored window) and expiry time.
async fn snapshot(redis: &Redis) -> BTreeMap<String, (String, i64)> {
    let custom = |name| CustomCommand::new_static(name, ClusterHash::FirstKey, false);
    let keys: Vec<String> = redis
        .client
        .custom(
            CustomCommand::new_static("KEYS", ClusterHash::Random, false),
            vec!["*"],
        )
        .await
        .unwrap();
    let mut all = BTreeMap::new();
    for key in keys {
        let kind: String = redis
            .client
            .custom(custom("TYPE"), vec![key.as_str()])
            .await
            .unwrap();
        let value = match kind.as_str() {
            "hash" => format!("{:?}", hash_fields(redis, &key).await),
            _ => redis
                .client
                .custom(custom("GET"), vec![key.as_str()])
                .await
                .unwrap(),
        };
        let expiry: i64 = redis
            .client
            .custom(custom("EXPIRETIME"), vec![key.as_str()])
            .await
            .unwrap();
        all.insert(key, (value, expiry));
    }
    all
}

/// The lookup's answer, as JSON.
async fn quota_json(h: &Extended, request: RateLimitRequest) -> serde_json::Value {
    let lookup = h.subject.service.lookup(&request).await.unwrap();
    serde_json::from_str(&llm_ratelimit::quota::render(&lookup)).unwrap()
}

/// Every limit of every descriptor: an anchored session closed until a request opens it (no
/// reset), then open from its anchor; fixed windows from their phase; a `rate_limit:` by
/// its unit; an unlimited one; none for a descriptor no rule matches. Expected times from an
/// independent computation (alice's weekly phase: 100 800 s).
#[tokio::test]
async fn quota_lookups_show_every_limit() {
    if !tools_available() {
        return;
    }
    use serde_json::json;
    let h = Extended::start(QUOTAS, Settings::default()).await;
    let descriptors: &[&[(&str, &str)]] = &[
        &[("workspace", "w"), ("user", "alice")],
        &[("workspace_total", "w")],
        &[("legacy", "x")],
        &[("free", "y")],
        &[("nothing", "z")],
    ];
    let request = || {
        let mut r = request("llm-quota", descriptors);
        // Ignored by a lookup.
        r.descriptors[0].hits_addend = Some(7);
        r
    };
    let flags = json!({"shadow_mode": false, "quota_mode": false});
    let entry = |fields: serde_json::Value| {
        let mut entry = flags.clone();
        entry
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        entry
    };
    let expected = |five_hour: serde_json::Value, used: u64, now: &str, at: [(&str, i64); 3]| {
        json!({
            "now": now,
            "descriptors": [
                {"limits": [
                    entry(five_hour),
                    entry(json!({"name": "weekly", "window": {"type": "fixed", "duration_s": 604800},
                        "limit": 1000, "used": used, "remaining": 1000 - used,
                        "used_pct": used as f64 / 10.0,
                        "resets_at": at[0].0, "resets_in_s": at[0].1})),
                ]},
                {"limits": [
                    entry(json!({"name": "weekly", "window": {"type": "fixed", "duration_s": 604800},
                        "limit": 10000, "used": used, "remaining": 10000 - used,
                        "used_pct": used as f64 / 100.0,
                        "resets_at": at[1].0, "resets_in_s": at[1].1})),
                ]},
                {"limits": [
                    entry(json!({"name": "hourly", "window": {"type": "unit", "unit": "HOUR"},
                        "limit": 10, "used": used, "remaining": 10u64.saturating_sub(used),
                        "used_pct": used as f64 * 10.0,
                        "resets_at": at[2].0, "resets_in_s": at[2].1})),
                ]},
                {"limits": [{"name": "free", "unlimited": true}]},
                {"limits": []},
            ]
        })
    };
    let closed = json!({"name": "five_hour", "window": {"type": "anchored", "duration_s": 18000},
        "limit": 100, "used": 0, "remaining": 100, "used_pct": 0.0, "open": false,
        "resets_at": null, "resets_in_s": null});
    h.at(1_000_000_000);
    assert_eq!(
        quota_json(&h, request()).await,
        expected(
            closed.clone(),
            0,
            "2001-09-09T01:46:40Z",
            [
                ("2001-09-14T04:00:00Z", 440_000),
                ("2001-09-10T00:00:00Z", 80_000),
                ("2001-09-09T02:00:00Z", 800),
            ]
        )
    );
    let mut counted = hits(descriptors, 40);
    counted.domain = "llm-quota".into();
    h.call(counted).await;
    h.at(1_000_000_100);
    assert_eq!(
        quota_json(&h, request()).await,
        expected(
            json!({"name": "five_hour", "window": {"type": "anchored", "duration_s": 18000},
                "limit": 100, "used": 40, "remaining": 60, "used_pct": 40.0, "open": true,
                "resets_at": "2001-09-09T06:00:00Z", "resets_in_s": 15_100}),
            40,
            "2001-09-09T01:48:20Z",
            [
                ("2001-09-14T04:00:00Z", 439_900),
                ("2001-09-10T00:00:00Z", 79_900),
                ("2001-09-09T02:00:00Z", 700),
            ]
        )
    );
    // 06:00:05: the session is over (its key outlives it a minute), the hour is a new one.
    h.at(1_000_015_205);
    let after = quota_json(&h, request()).await;
    assert_eq!(after["descriptors"][0]["limits"][0], entry(closed));
    assert_eq!(after["descriptors"][2]["limits"][0]["used"], 0);
    assert_eq!(after["descriptors"][0]["limits"][1]["used"], 40);
}

/// A lookup changes nothing: it only reads (`GET`, `HMGET`), and leaves the keys, their TTLs
/// and the stats as they were.
#[tokio::test]
async fn quota_lookups_change_nothing() {
    if !tools_available() {
        return;
    }
    let h = Extended::start(QUOTAS, Settings::default()).await;
    let descriptors: &[&[(&str, &str)]] = &[
        &[("workspace", "w"), ("user", "alice")],
        &[("workspace", "w"), ("user", "bob")],
        &[("workspace_total", "w")],
        &[("legacy", "x")],
        &[("unknown", "u")],
    ];
    h.at(1_000_000_000);
    let mut counted = hits(&descriptors[..1], 5);
    counted.domain = "llm-quota".into();
    h.call(counted).await;
    let keys = snapshot(&h.redis).await;
    let stats = h.subject.stats.snapshot();
    let before = h.redis.commands().await.len();
    for now in [1_000_000_000, 1_000_020_000] {
        h.at(now);
        h.subject
            .service
            .lookup(&request("llm-quota", descriptors))
            .await
            .unwrap();
    }
    let commands = h.redis.commands().await[before..].to_vec();
    assert!(
        commands
            .iter()
            .all(|c| c.starts_with("\"GET\"") || c.starts_with("\"HMGET\"")),
        "{commands:?}"
    );
    // A read per limit: two each for alice and bob, one for the workspace and the legacy.
    assert_eq!(commands.len(), 2 * 6, "{commands:?}");
    assert_eq!(snapshot(&h.redis).await, keys);
    assert_eq!(h.subject.stats.snapshot(), stats);
}

/// The rules resolve as for a decision: `replaces` across the request's descriptors, a
/// client-sent `limit`; an unknown domain or an invalid request is an error.
#[tokio::test]
async fn quota_lookups_resolve_rules_as_decisions_do() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit::service::LookupError;
    use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride;
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: hourly, requests_per_unit: 10, window: {type: fixed, duration: 1h}}
      - {name: daily, requests_per_unit: 100, window: {type: fixed, duration: 1d}}
  - key: plan
    rate_limit: {unit: minute, requests_per_unit: 5, replaces: [{name: hourly}]}
",
        Settings::default(),
    )
    .await;
    h.at(1_000_000_000);
    let names = |value: &serde_json::Value, i: usize| -> Vec<String> {
        value["descriptors"][i]["limits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l["name"].as_str().unwrap().to_string())
            .collect()
    };
    let alone = quota_json(&h, request("d", &[&[("user", "a")]])).await;
    assert_eq!(names(&alone, 0), ["hourly", "daily"]);
    let with_plan = quota_json(&h, request("d", &[&[("user", "a")], &[("plan", "p")]])).await;
    assert_eq!(names(&with_plan, 0), ["daily"]);
    let mut client = request("d", &[&[("user", "a")]]);
    client.descriptors[0].limit = Some(RateLimitOverride {
        requests_per_unit: 3,
        unit: llm_ratelimit_proto::envoy::r#type::v3::RateLimitUnit::Second as i32,
        ..Default::default()
    });
    let overridden = quota_json(&h, client).await;
    assert_eq!(
        overridden["descriptors"][0]["limits"][0]["window"],
        serde_json::json!({"type": "unit", "unit": "SECOND"})
    );
    assert_eq!(overridden["descriptors"][0]["limits"][0]["limit"], 3);
    assert_eq!(
        h.subject
            .service
            .lookup(&request("other", &[&[("user", "a")]]))
            .await,
        Err(LookupError::UnknownDomain("other".into()))
    );
    assert_eq!(
        h.subject.service.lookup(&request("d", &[])).await,
        Err(LookupError::InvalidRequest(
            "rate limit descriptor list must not be empty".into()
        ))
    );
    // Before any configuration.
    let fresh = RateLimitService::new(
        RedisCache::new(
            h.redis.connect().await,
            h.subject.clock.clone(),
            Arc::new(Stats::default()),
            CacheSettings::default(),
        ),
        h.subject.clock.clone(),
        Arc::new(Stats::default()),
        ServiceSettings::default(),
    );
    assert_eq!(
        fresh.lookup(&request("d", &[&[("user", "a")]])).await,
        Err(LookupError::InvalidRequest(
            "no rate limit configuration loaded".into()
        ))
    );
}

/// ADR 0003's consistency property: at any `now`, a lookup then a decision with N hits per
/// descriptor; each descriptor's status is OK ⇔ `used + N ≤ limit` for each of its limits.
/// Over random sequences of fixed windows (with and without an offset), an anchored one, a
/// `rate_limit:`, `replaces` and a client `limit`; no shadow mode, quota mode or local cache.
#[tokio::test]
async fn quota_lookups_announce_what_the_next_decision_applies() {
    if !tools_available() {
        return;
    }
    use llm_ratelimit::service::LimitQuota;
    use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride;
    use llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::Code;
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: minute, requests_per_unit: 5, window: {type: fixed, duration: 1m}}
      - {name: hour, requests_per_unit: 9, window: {type: fixed, duration: 1h, offset: 15m}}
      - {name: session, requests_per_unit: 7, window: {type: anchored, duration: 10m, start_rounding: 1m}}
  - key: legacy
    rate_limit: {unit: minute, requests_per_unit: 4}
  - key: plan
    rate_limit: {unit: hour, requests_per_unit: 50, replaces: [{name: minute}]}
  - key: client
    rate_limit: {unit: hour, requests_per_unit: 50}
",
        Settings::default(),
    )
    .await;
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = |n: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % n
    };
    let mut now: i64 = 1_000_000_000;
    let mut over = 0;
    for step in 0..300 {
        now += match next(8) {
            0 => 600 + next(1_200) as i64,
            _ => next(40) as i64,
        };
        h.at(now);
        let mut descriptors: Vec<&[(&str, &str)]> =
            vec![&[("user", "u")], &[("legacy", "l")], &[("client", "c")]];
        if next(2) == 0 {
            descriptors.push(&[("plan", "p")]);
        }
        let n = next(4);
        let mut request = request("d", &descriptors);
        for descriptor in &mut request.descriptors {
            descriptor.hits_addend = Some(n);
        }
        request.descriptors[2].limit = Some(RateLimitOverride {
            requests_per_unit: 6,
            unit: llm_ratelimit_proto::envoy::r#type::v3::RateLimitUnit::Minute as i32,
            ..Default::default()
        });
        let lookup = h.subject.service.lookup(&request).await.unwrap();
        let expected: Vec<Code> = lookup
            .descriptors
            .iter()
            .map(|limits| {
                let fits = limits.iter().all(|quota| match quota {
                    LimitQuota::Limited(limit, reading) => {
                        reading.used + n <= u64::from(limit.limit.requests_per_unit)
                    }
                    LimitQuota::Unlimited(_) => true,
                });
                if fits { Code::Ok } else { Code::OverLimit }
            })
            .collect();
        let decided: Vec<Code> = h
            .call(request)
            .await
            .statuses
            .iter()
            .map(|s| s.code())
            .collect();
        assert_eq!(
            decided, expected,
            "step {step} at {now}, {n} hits: {lookup:?}"
        );
        over += decided.iter().filter(|c| **c == Code::OverLimit).count();
    }
    // The sequence goes over the limits as well as under them.
    assert!(over > 50, "{over}");
}

/// `POST /quota` on the HTTP port (ADR 0003): JSON answers, `{"error": …}` with the cause's
/// status, other methods refused; `ratelimit.quota_lookup.*` counts them.
#[tokio::test]
async fn quota_lookups_are_served_over_http() {
    if !tools_available() {
        return;
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let h = Extended::start(QUOTAS, Settings::default()).await;
    h.at(1_000_000_000);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let health = Arc::new(llm_ratelimit::health::HealthChecker::new(
        llm_ratelimit::health::HealthService::default(),
        false,
    ));
    tokio::spawn(llm_ratelimit::http::serve(
        listener,
        health,
        h.subject.service.clone(),
        std::future::pending(),
    ));
    let send = async |method: &str, body: &str| {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let request = format!(
            "{method} /quota HTTP/1.0\r\nHost: h\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        let status: u16 = head[9..12].parse().unwrap();
        let headers = head.to_ascii_lowercase();
        (status, headers, body.to_string())
    };
    let (status, headers, body) = send(
        "POST",
        r#"{"domain":"llm-quota","descriptors":[{"entries":[{"key":"legacy","value":"x"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(
        headers.contains("content-type: application/json"),
        "{headers}"
    );
    let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(answer["descriptors"][0]["limits"][0]["name"], "hourly");
    let (status, headers, _) = send("GET", "").await;
    assert_eq!(status, 405);
    assert!(headers.contains("allow: post"), "{headers}");
    let error =
        |body: &str| serde_json::from_str::<serde_json::Value>(body).unwrap()["error"].clone();
    let (status, _, body) = send("POST", "not json").await;
    assert_eq!(status, 400);
    assert!(error(&body).is_string(), "{body}");
    let (status, _, body) = send(
        "POST",
        r#"{"domain":"nope","descriptors":[{"entries":[{"key":"a","value":"b"}]}]}"#,
    )
    .await;
    assert_eq!(
        (status, error(&body)),
        (404, "unknown domain 'nope'".into())
    );
    // A key a decision cannot read either (an anchored window's, not a hash): the backend's.
    let key = "{llm-quota_workspace_w_user_alice_}_an18000r3600_five_hour";
    let () = h
        .redis
        .client
        .set(key, "x", None, None, false)
        .await
        .unwrap();
    let (status, _, body) = send(
        "POST",
        r#"{"domain":"llm-quota","descriptors":[{"entries":[{"key":"workspace","value":"w"},{"key":"user","value":"alice"}]}]}"#,
    )
    .await;
    assert_eq!(status, 500, "{body}");
    assert!(
        error(&body).as_str().unwrap().contains("WRONGTYPE"),
        "{body}"
    );
    let stats: BTreeMap<String, u64> = h
        .subject
        .stats
        .snapshot()
        .into_iter()
        .filter(|(name, _)| name.starts_with("ratelimit.quota_lookup"))
        .collect();
    assert_eq!(
        stats,
        BTreeMap::from([
            ("ratelimit.quota_lookup.total_requests".to_string(), 4),
            ("ratelimit.quota_lookup.error.backend".to_string(), 1),
            (
                "ratelimit.quota_lookup.error.invalid_request".to_string(),
                1
            ),
            ("ratelimit.quota_lookup.error.unknown_domain".to_string(), 1),
        ])
    );
}

/// On Memcached (D24), a lookup reads the fixed windows' keys with one `GetMulti`, and
/// counts nothing in `memcache.*`.
#[tokio::test]
async fn quota_lookups_read_memcached() {
    if !memcached_available() {
        return;
    }
    let memcached = Memcached::start().await;
    let servers =
        llm_ratelimit::memcached::client::resolve(std::slice::from_ref(&memcached.address))
            .await
            .unwrap();
    let client = Arc::new(llm_ratelimit::memcached::client::Client::new(
        servers, 2, None,
    ));
    let clock = Arc::new(ManualClock::default());
    let stats = Arc::new(Stats::default());
    let cache = llm_ratelimit::memcached::MemcacheCache::new(
        client,
        clock.clone(),
        stats.clone(),
        &stats.scope_with_tags("ratelimit"),
        CacheSettings::default(),
        None,
    );
    let service = RateLimitService::new(
        cache,
        clock.clone(),
        stats.clone(),
        ServiceSettings::default(),
    );
    let file = ConfigFile {
        name: "config.yaml".into(),
        content: WEEK_AND_FIVE_HOURS.into(),
    };
    service.set_config(config::load(&[file], &LoadOptions::default()).unwrap());
    clock.0.store(1_000_000_000, Ordering::SeqCst);
    let alice = || request("d", &[&[("user", "alice")]]);
    service.should_rate_limit(&alice()).await.unwrap();
    // The increments go after the answer, each counted once memcached has answered it: a key
    // it shows may not be counted yet.
    for _ in 0..100 {
        if stats
            .snapshot()
            .get("ratelimit.memcache.add.__code=success")
            == Some(&2)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let counted = stats.snapshot();
    let lookup = service.lookup(&alice()).await.unwrap();
    let used: Vec<u64> = lookup.descriptors[0]
        .iter()
        .map(|quota| match quota {
            llm_ratelimit::service::LimitQuota::Limited(_, reading) => reading.used,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(used, [1, 1]);
    assert_eq!(stats.snapshot(), counted);
}

/// With `REDIS_PERSECOND`, a lookup reads a list with a short window where decisions count
/// it: on the per-second Redis.
#[tokio::test]
async fn quota_lookups_read_the_per_second_redis() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        per_second: true,
        ..Default::default()
    };
    let h = Extended::start(
        "
domain: d
descriptors:
  - key: burst
    rate_limits:
      - {name: ten_seconds, requests_per_unit: 5, window: {type: fixed, duration: 10s}}
",
        settings,
    )
    .await;
    h.at(1_000_000_000);
    let burst = || request("d", &[&[("burst", "a")]]);
    h.call(hits(&[&[("burst", "a")]], 2)).await;
    let lookup = quota_json(&h, burst()).await;
    assert_eq!(lookup["descriptors"][0]["limits"][0]["used"], 2);
}
