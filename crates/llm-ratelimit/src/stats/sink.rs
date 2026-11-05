//! Where flushed stats go, chosen like envoyproxy/ratelimit's runner
//! (`src/service_cmd/runner/runner.go`).

use super::{
    DogStatsdSettings, DogStatsdSink, LoggingSink, PrometheusSettings, PrometheusSink, StatsdSink,
};

/// A destination for flushed stats.
pub enum Sink {
    /// `DISABLE_STATS`: stats are dropped.
    Null,
    /// The default when no other sink is enabled: JSON lines on stderr.
    Logging(LoggingSink),
    /// `USE_STATSD`: statsd lines over TCP or UDP.
    Statsd(StatsdSink),
    /// `USE_DOG_STATSD`: DogStatsD lines over UDP.
    Dogstatsd(DogStatsdSink),
    /// `USE_PROMETHEUS`: a registry served over HTTP.
    Prometheus(PrometheusSink),
}

impl Sink {
    pub fn flush_counter(&self, name: &str, value: u64) {
        match self {
            Sink::Null => {}
            Sink::Logging(sink) => sink.flush_counter(name, value),
            Sink::Statsd(sink) => sink.flush_counter(name, value),
            Sink::Dogstatsd(sink) => sink.flush_counter(name, value),
            Sink::Prometheus(sink) => sink.flush_counter(name, value),
        }
    }

    pub fn flush_gauge(&self, name: &str, value: u64) {
        match self {
            Sink::Null => {}
            Sink::Logging(sink) => sink.flush_gauge(name, value),
            Sink::Statsd(sink) => sink.flush_gauge(name, value),
            Sink::Dogstatsd(sink) => sink.flush_gauge(name, value),
            Sink::Prometheus(sink) => sink.flush_gauge(name, value),
        }
    }

    pub fn flush_timer(&self, name: &str, value: f64) {
        match self {
            Sink::Null => {}
            Sink::Logging(sink) => sink.flush_timer(name, value),
            Sink::Statsd(sink) => sink.flush_timer(name, value),
            Sink::Dogstatsd(sink) => sink.flush_timer(name, value),
            Sink::Prometheus(sink) => sink.flush_timer(name, value),
        }
    }

    /// Delivers what was flushed so far.
    pub async fn flush(&self) {
        match self {
            Sink::Null => {}
            Sink::Logging(sink) => sink.flush(),
            Sink::Statsd(sink) => sink.flush().await,
            // Upstream's DogStatsD sink cannot be flushed: its client sends every 100 ms.
            Sink::Dogstatsd(_) => {}
            // Each stat is in the registry as soon as it is flushed.
            Sink::Prometheus(_) => {}
        }
    }
}

/// gostats' `STATSD_PROTOCOL`, which upstream leaves to gostats.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StatsdProtocol {
    #[default]
    Tcp,
    Udp,
}

/// The settings that choose and configure the sink.
#[derive(Debug, Clone)]
pub struct SinkSettings {
    /// `DISABLE_STATS`: wins over every other setting.
    pub disable_stats: bool,
    /// `USE_STATSD`, on by default.
    pub use_statsd: bool,
    /// `USE_DOG_STATSD`.
    pub use_dog_statsd: bool,
    /// `USE_PROMETHEUS`.
    pub use_prometheus: bool,
    /// `STATSD_HOST`.
    pub statsd_host: String,
    /// `STATSD_PORT`.
    pub statsd_port: u16,
    pub statsd_protocol: StatsdProtocol,
    pub dogstatsd: DogStatsdSettings,
    pub prometheus: PrometheusSettings,
}

impl Default for SinkSettings {
    fn default() -> Self {
        Self {
            disable_stats: false,
            use_statsd: true,
            use_dog_statsd: false,
            use_prometheus: false,
            statsd_host: "localhost".into(),
            statsd_port: 8125,
            statsd_protocol: StatsdProtocol::Tcp,
            dogstatsd: DogStatsdSettings::default(),
            prometheus: PrometheusSettings::default(),
        }
    }
}

const SEVERAL_SINKS: &str = "Error: unable to use more than one stats sink at the same time. Set one of USE_DOG_STATSD, USE_STATSD, USE_PROMETHEUS.";

/// The sink `settings` select, or why the process cannot start. As upstream, `DISABLE_STATS`
/// wins, then DogStatsD, statsd and Prometheus each refuse the others, and stats are logged when
/// none is enabled. Since `USE_STATSD` is on by default, enabling another sink alone is an error.
pub fn select_sink(settings: &SinkSettings) -> Result<Sink, String> {
    let enabled = [
        settings.use_dog_statsd,
        settings.use_statsd,
        settings.use_prometheus,
    ];
    if settings.disable_stats {
        crate::log::info("Stats disabled");
        return Ok(Sink::Null);
    }
    if enabled.iter().filter(|on| **on).count() > 1 {
        return Err(SEVERAL_SINKS.into());
    }
    if settings.use_dog_statsd {
        let sink = DogStatsdSink::new(
            &settings.statsd_host,
            settings.statsd_port,
            settings.dogstatsd.clone(),
        )
        .map_err(|e| format!("{DOGSTATSD_FAILED}{e}"))?;
        crate::log::info("Stats initialized for dogstatsd");
        return Ok(Sink::Dogstatsd(sink));
    }
    if settings.use_prometheus {
        crate::log::info("Stats initialized for Prometheus");
        return PrometheusSink::start(&settings.prometheus).map(Sink::Prometheus);
    }
    if settings.use_statsd {
        crate::log::info("Stats initialized for statsd");
        return Ok(Sink::Statsd(StatsdSink::new(
            &settings.statsd_host,
            settings.statsd_port,
            settings.statsd_protocol,
        )));
    }
    crate::log::info("Stats initialized for stdout");
    Ok(Sink::Logging(LoggingSink::stderr()))
}

const DOGSTATSD_FAILED: &str = "Failed to create dogstatsd sink: ";

/// How a `select_sink` error stops the process: the runner's own checks are `logger.Fatal`,
/// the sinks' failures panics.
pub fn sink_failure(message: String) -> crate::log::Failure {
    match message == SEVERAL_SINKS || message.starts_with(DOGSTATSD_FAILED) {
        true => crate::log::Failure::Fatal(message),
        false => crate::log::Failure::Panic(message),
    }
}
