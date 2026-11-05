//! The OTLP exporter (`otlptracegrpc`, `otlptracehttp`) behind the SDK's batch span
//! processor: spans queue up (2048 at most, `OTEL_BSP_MAX_QUEUE_SIZE`) and leave in batches
//! of 512 (`OTEL_BSP_MAX_EXPORT_BATCH_SIZE`) every 5 s (`OTEL_BSP_SCHEDULE_DELAY`), to the
//! collector the `OTEL_EXPORTER_OTLP_*` variables configure.

use super::{Kind, Scope, SpanContext, Value};
use llm_ratelimit_proto::opentelemetry::proto::{
    collector::trace::v1::{ExportTraceServiceRequest, trace_service_client::TraceServiceClient},
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span::SpanKind, status::StatusCode},
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// The exporter's protocol (`TRACING_EXPORTER_PROTOCOL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Grpc,
    Http,
}

/// A span ended, waiting to be exported.
#[derive(Debug, Clone)]
pub(super) struct SpanData {
    pub scope: Scope,
    pub context: SpanContext,
    pub parent_span_id: Option<[u8; 8]>,
    pub parent_remote: bool,
    pub name: String,
    pub kind: Kind,
    pub start: u64,
    pub end: u64,
    pub attributes: Vec<(String, Value)>,
    /// Attributes beyond the span limit.
    pub dropped_attributes: u32,
    /// `Error` with this message; unset otherwise.
    pub status: Option<String>,
}

/// The semantic conventions' schema of the resource (v1.7.0).
const RESOURCE_SCHEMA: &str = "https://opentelemetry.io/schemas/1.7.0";

/// `resource.NewWithAttributes` with the three service attributes, merged over
/// `resource.Environment()` (`OTEL_RESOURCE_ATTRIBUTES`, whose values are percent-decoded and
/// whose last value of a key wins, then `OTEL_SERVICE_NAME`): sorted by key, the service
/// attributes winning.
pub(super) fn resource(name: &str, namespace: &str, instance_id: &str) -> Resource {
    let mut attributes = std::collections::BTreeMap::new();
    let pairs = std::env::var("OTEL_RESOURCE_ATTRIBUTES").unwrap_or_default();
    let pairs = pairs.trim();
    let mut invalid = Vec::new();
    if !pairs.is_empty() {
        for pair in pairs.split(',') {
            let Some((key, value)) = pair.split_once('=') else {
                invalid.push(pair);
                continue;
            };
            let value = crate::go::unescape(value.trim(), false).unwrap_or_else(|| {
                otel_error(&format!(
                    "invalid URL escape in the value of {}",
                    key.trim()
                ));
                value.to_string()
            });
            // An empty key is not a valid attribute.
            if !key.trim().is_empty() {
                attributes.insert(key.trim().to_string(), value);
            }
        }
    }
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_default();
    let service_name = service_name.trim();
    if !service_name.is_empty() {
        attributes.insert("service.name".to_string(), service_name.to_string());
    }
    if !invalid.is_empty() {
        otel_error(&format!(
            "partial resource: missing value: [{}]",
            invalid.join(" ")
        ));
    }
    for (key, value) in [
        ("service.name", name),
        ("service.namespace", namespace),
        ("service.instance.id", instance_id),
    ] {
        attributes.insert(key.to_string(), value.to_string());
    }
    Resource {
        attributes: attributes
            .into_iter()
            .map(|(k, v)| key_value(k, Value::Str(v)))
            .collect(),
        ..Default::default()
    }
}

fn key_value(key: String, value: Value) -> KeyValue {
    let value = match value {
        Value::Str(s) => any_value::Value::StringValue(s),
        Value::Int(i) => any_value::Value::IntValue(i),
    };
    KeyValue {
        key,
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

/// What the otel package's default error handler writes: Go's standard logger.
fn otel_error(message: &str) {
    crate::log::stdlib(message);
}

/// The batch span processor's queue.
pub(super) struct Queue {
    /// None when the queue has no room at all.
    spans: Option<mpsc::Sender<SpanData>>,
    shutdown: mpsc::Sender<oneshot::Sender<Result<(), String>>>,
}

impl Queue {
    /// Queues a span, dropped when the queue is full.
    pub fn push(&self, span: SpanData) {
        if let Some(spans) = &self.spans {
            let _ = spans.try_send(span);
        }
    }

    /// Exports what is queued and stops.
    pub async fn shutdown(&self) -> Result<(), String> {
        let (done, flushed) = oneshot::channel();
        if self.shutdown.send(done).await.is_err() {
            return Ok(());
        }
        flushed.await.unwrap_or(Ok(()))
    }
}

/// The batch span processor's settings: `OTEL_BSP_*`, read with `strconv.Atoi` (a value
/// that is not a number is the default; 0 and negative values are kept).
struct BatchSettings {
    /// Spans past it are dropped; none fit at 0 or less.
    queue_size: i64,
    /// Larger than the queue, it is the smaller of 512 and the queue.
    batch_size: i64,
    /// At 0 or less, each span leaves at once.
    delay: Duration,
    /// At 0 or less, none.
    timeout: Option<Duration>,
}

impl BatchSettings {
    fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let int = |name: &str, default: i64| match lookup(name) {
            Some(v) if !v.is_empty() => crate::go::atoi(&v).unwrap_or(default),
            _ => default,
        };
        let queue_size = int("OTEL_BSP_MAX_QUEUE_SIZE", 2048);
        let mut batch_size = int("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", 512);
        if batch_size > queue_size {
            batch_size = queue_size.min(512);
        }
        let millis = |v: i64| Duration::from_millis(v.max(0) as u64);
        let timeout = int("OTEL_BSP_EXPORT_TIMEOUT", 30000);
        BatchSettings {
            queue_size,
            batch_size,
            delay: millis(int("OTEL_BSP_SCHEDULE_DELAY", 5000)),
            timeout: (timeout > 0).then(|| millis(timeout)),
        }
    }
}

/// Starts the batch span processor and its exporter.
pub(super) fn start(protocol: Protocol, config: Config, resource: Resource) -> Queue {
    let settings = BatchSettings::from_env();
    let batch_size = settings.batch_size.max(1) as usize;
    let timeout = settings.timeout;
    // A queue of 0 drops every span: a channel needs room for one.
    let (spans, mut queued) = mpsc::channel::<SpanData>(settings.queue_size.max(1) as usize);
    let (shutdown, mut stop) = mpsc::channel::<oneshot::Sender<Result<(), String>>>(1);
    let exporter = Exporter::new(protocol, config);
    let delay = settings.delay.max(Duration::from_millis(1));
    tokio::spawn(async move {
        // Export errors go to the error handler (also while shutting down).
        let export = async |spans: Vec<SpanData>| {
            if let Err(e) = exporter.export(&resource, spans, timeout).await {
                otel_error(&e);
            }
        };
        let mut batch = Vec::new();
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + delay, delay);
        loop {
            tokio::select! {
                span = queued.recv() => {
                    let Some(span) = span else { break };
                    batch.push(span);
                    if batch.len() >= batch_size {
                        export(std::mem::take(&mut batch)).await;
                    }
                }
                _ = ticks.tick() => {
                    if !batch.is_empty() {
                        export(std::mem::take(&mut batch)).await;
                    }
                }
                done = stop.recv() => {
                    while let Ok(span) = queued.try_recv() {
                        batch.push(span);
                    }
                    for spans in std::mem::take(&mut batch).chunks(batch_size) {
                        export(spans.to_vec()).await;
                    }
                    if let Some(done) = done {
                        let _ = done.send(Ok(()));
                    }
                    break;
                }
            }
        }
    });
    Queue {
        spans: (settings.queue_size > 0).then_some(spans),
        shutdown,
    }
}

/// The OTLP request for `spans`: one resource, the spans grouped by scope in the order the
/// scopes first come.
fn request(resource: &Resource, spans: Vec<SpanData>) -> ExportTraceServiceRequest {
    let mut scopes: Vec<ScopeSpans> = Vec::new();
    for span in spans {
        let scope = InstrumentationScope {
            name: span.scope.name.to_string(),
            version: span.scope.version.to_string(),
            ..Default::default()
        };
        let index = match scopes.iter().position(|s| s.scope.as_ref() == Some(&scope)) {
            Some(index) => index,
            None => {
                scopes.push(ScopeSpans {
                    scope: Some(scope),
                    ..Default::default()
                });
                scopes.len() - 1
            }
        };
        scopes[index].spans.push(otlp_span(span));
    }
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource.clone()),
            scope_spans: scopes,
            schema_url: RESOURCE_SCHEMA.to_string(),
        }],
    }
}

/// `tracetransform.span`: the status always present (unset, or error with its message); the
/// flags are the trace flags, with "is remote" known (`0x100`) and set (`0x200`) for a
/// remote parent.
fn otlp_span(span: SpanData) -> Span {
    let (code, message) = match span.status {
        Some(message) => (StatusCode::Error, message),
        None => (StatusCode::Unset, String::new()),
    };
    let kind = match span.kind {
        Kind::Internal => SpanKind::Internal,
        Kind::Server => SpanKind::Server,
    };
    Span {
        trace_id: span.context.trace_id.to_vec(),
        span_id: span.context.span_id.to_vec(),
        trace_state: span.context.trace_state,
        parent_span_id: span.parent_span_id.map_or_else(Vec::new, |id| id.to_vec()),
        flags: u32::from(span.context.flags) | 0x100 | if span.parent_remote { 0x200 } else { 0 },
        name: span.name,
        kind: kind as i32,
        start_time_unix_nano: span.start,
        end_time_unix_nano: span.end,
        attributes: span
            .attributes
            .into_iter()
            .map(|(k, v)| key_value(k, v))
            .collect(),
        dropped_attributes_count: span.dropped_attributes,
        status: Some(Status {
            message,
            code: code as i32,
        }),
        ..Default::default()
    }
}

/// The exporter's configuration, from `OTEL_EXPORTER_OTLP_*` then `OTEL_EXPORTER_OTLP_TRACES_*`
/// (which win), applied in the order Go applies them.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Config {
    /// `host:port` (gRPC: the endpoint's host and path joined).
    pub endpoint: String,
    /// HTTP's path, `/v1/traces` by default.
    pub path: String,
    pub insecure: bool,
    pub headers: Vec<(String, String)>,
    pub gzip: bool,
    pub timeout: Duration,
    /// PEM certificates to verify the collector with, instead of the system's.
    pub ca: Option<Vec<u8>>,
    /// A PEM client certificate and key.
    pub identity: Option<(Vec<u8>, Vec<u8>)>,
}

impl Config {
    /// Whether a CA or a client certificate is configured.
    pub fn has_tls_settings(&self) -> bool {
        self.ca.is_some() || self.identity.is_some()
    }

    pub fn from_env(protocol: Protocol) -> Self {
        Self::from_lookup(protocol, |name| std::env::var(name).ok())
    }

    /// The configuration from the variables `lookup` gives.
    fn from_lookup(protocol: Protocol, lookup: impl Fn(&str) -> Option<String>) -> Self {
        // The value of `OTEL_EXPORTER_OTLP_<name>`, trimmed, if not empty.
        let env = |name: &str| {
            let value = lookup(&format!("OTEL_EXPORTER_OTLP_{name}"))?;
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_string())
        };
        let mut config = Config {
            endpoint: match protocol {
                Protocol::Grpc => "localhost:4317".into(),
                Protocol::Http => "localhost:4318".into(),
            },
            path: "/v1/traces".into(),
            insecure: false,
            headers: Vec::new(),
            gzip: false,
            timeout: Duration::from_secs(10),
            ca: None,
            identity: None,
        };
        for (name, signal) in [("ENDPOINT", false), ("TRACES_ENDPOINT", true)] {
            let Some(value) = env(name) else { continue };
            let Some(url) = parse_url(&value) else {
                // Credentials can be in a URL: it is not written.
                otel_error(&format!("parse url: invalid OTEL_EXPORTER_OTLP_{name}"));
                continue;
            };
            config.insecure = matches!(url.scheme.to_lowercase().as_str(), "http" | "unix");
            match protocol {
                Protocol::Grpc => config.endpoint = crate::go::join(&[&url.host, &url.path]),
                Protocol::Http => {
                    config.endpoint = url.host;
                    config.path = match signal {
                        false => crate::go::join(&[&url.path, "/v1/traces"]),
                        true if url.path.is_empty() => "/".into(),
                        true => url.path,
                    };
                }
            }
        }
        for name in ["CERTIFICATE", "TRACES_CERTIFICATE"] {
            if let Some(file) = env(name) {
                // A file with no certificate leaves the configuration as it is.
                match std::fs::read(&file) {
                    Ok(pem) if !certificates(&pem).is_empty() => config.ca = Some(pem),
                    Ok(_) => otel_error(
                        "create tls cert pool: failed to append certificate to the cert pool",
                    ),
                    Err(e) => otel_error(&format!("read tls ca cert file: {e}")),
                }
            }
        }
        for (cert, key) in [
            ("CLIENT_CERTIFICATE", "CLIENT_KEY"),
            ("TRACES_CLIENT_CERTIFICATE", "TRACES_CLIENT_KEY"),
        ] {
            if let (Some(cert), Some(key)) = (env(cert), env(key)) {
                match (std::fs::read(&cert), std::fs::read(&key)) {
                    (Ok(cert), Ok(key)) if key_pair(&cert, &key).is_some() => {
                        config.identity = Some((cert, key))
                    }
                    (Ok(_), Ok(_)) => otel_error("create tls client key pair: invalid PEM"),
                    (Err(e), _) | (_, Err(e)) => otel_error(&format!("read tls client cert: {e}")),
                }
            }
        }
        for name in ["INSECURE", "TRACES_INSECURE"] {
            if let Some(value) = env(name) {
                config.insecure = value.eq_ignore_ascii_case("true");
            }
        }
        for name in ["HEADERS", "TRACES_HEADERS"] {
            if let Some(value) = env(name) {
                config.headers = headers(&value);
            }
        }
        for name in ["COMPRESSION", "TRACES_COMPRESSION"] {
            if let Some(value) = env(name) {
                config.gzip = value == "gzip";
            }
        }
        for name in ["TIMEOUT", "TRACES_TIMEOUT"] {
            if let Some(value) = env(name) {
                match crate::go::atoi(&value) {
                    Ok(ms) => config.timeout = Duration::from_millis(ms.max(0) as u64),
                    Err(e) => otel_error(&format!("parse duration: {e}")),
                }
            }
        }
        if protocol == Protocol::Http {
            config.path = clean_path(&config.path);
        }
        config
    }
}

/// `cleanPath`: an empty path (or `.`) is the default, a relative one made absolute.
fn clean_path(path: &str) -> String {
    match path.trim() {
        "" | "." => "/v1/traces".into(),
        path if path.starts_with('/') => path.into(),
        path => format!("/{path}"),
    }
}

/// `stringToHeader`: `key=value` pairs separated by commas, the key a token, the value
/// percent-decoded; trimmed. A later key replaces an earlier one. Values, which can be
/// credentials, are never written in errors.
fn headers(value: &str) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = Vec::new();
    for (index, pair) in value.split(',').enumerate() {
        let Some((key, value)) = pair.split_once('=') else {
            otel_error(&format!(
                "parse headers: missing '=' in entry {}",
                index + 1
            ));
            continue;
        };
        let key = key.trim();
        let token = |c: char| c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c);
        if key.is_empty() || !key.chars().all(token) {
            otel_error(&format!("parse headers: invalid header key: {key}"));
            continue;
        }
        let Some(value) = crate::go::unescape(value, false) else {
            otel_error(&format!("escape header value: invalid escape in {key}"));
            continue;
        };
        super::upsert(&mut headers, key.to_string(), value.trim().to_string());
    }
    headers
}

/// The parts of a URL `url.Parse` gives that the exporter uses.
#[derive(Debug, Clone, PartialEq)]
struct Url {
    scheme: String,
    host: String,
    path: String,
}

/// `url.Parse`, for an endpoint: `scheme://host/path` (a query and fragment left out); a
/// value with no scheme is a path, which cannot have a colon in its first segment.
fn parse_url(value: &str) -> Option<Url> {
    let value = value.split('#').next().unwrap_or_default();
    let value = value.split('?').next().unwrap_or_default();
    let (scheme, rest) = match value.find(':') {
        Some(i)
            if i > 0
                && value[..i].starts_with(|c: char| c.is_ascii_alphabetic())
                && value[..i]
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c)) =>
        {
            (&value[..i], &value[i + 1..])
        }
        Some(0) => return None,
        _ => ("", value),
    };
    match rest.strip_prefix("//") {
        Some(authority_path) => {
            let (authority, path) = match authority_path.find('/') {
                Some(i) => (&authority_path[..i], &authority_path[i..]),
                None => (authority_path, ""),
            };
            let host = authority.rsplit('@').next().unwrap_or_default();
            // A port must be digits; no control character anywhere.
            let port = match host.rsplit_once(']') {
                Some((_, rest)) => rest.strip_prefix(':'),
                None => host.rsplit_once(':').map(|(_, p)| p),
            };
            if port.is_some_and(|p| !p.bytes().all(|b| b.is_ascii_digit()))
                || value.bytes().any(|b| b < 0x20 || b == 0x7f)
            {
                return None;
            }
            Some(Url {
                scheme: scheme.into(),
                host: host.into(),
                path: crate::go::unescape(path, false)?,
            })
        }
        // `scheme:opaque`: no host.
        None if !scheme.is_empty() => Some(Url {
            scheme: scheme.into(),
            host: String::new(),
            path: String::new(),
        }),
        None => {
            let first = rest.split('/').next().unwrap_or_default();
            if first.contains(':') {
                return None;
            }
            Some(Url {
                scheme: String::new(),
                host: String::new(),
                path: crate::go::unescape(rest, false)?,
            })
        }
    }
}

/// Sends batches to the collector.
enum Exporter {
    Grpc {
        config: Config,
        channel: tokio::sync::OnceCell<Result<tonic::transport::Channel, String>>,
    },
    Http {
        config: Config,
        client: Box<HttpClient>,
    },
}

type HttpClient = hyper_util::client::legacy::Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    http_body_util::Full<bytes::Bytes>,
>;

impl Exporter {
    fn new(protocol: Protocol, config: Config) -> Self {
        match protocol {
            Protocol::Grpc => Exporter::Grpc {
                config,
                channel: tokio::sync::OnceCell::new(),
            },
            Protocol::Http => Exporter::Http {
                client: Box::new(http_client(&config)),
                config,
            },
        }
    }

    /// Sends `spans`, within the exporter's timeout and the batch processor's.
    async fn export(
        &self,
        resource: &Resource,
        spans: Vec<SpanData>,
        batch_timeout: Option<Duration>,
    ) -> Result<(), String> {
        let request = request(resource, spans);
        let (config, sent) = match self {
            Exporter::Grpc { config, channel } => (
                config,
                Box::pin(export_grpc(config, channel, request)) as Sending<'_>,
            ),
            Exporter::Http { config, client } => (
                config,
                Box::pin(export_http(config, client, request)) as Sending<'_>,
            ),
        };
        let timeout = batch_timeout.map_or(config.timeout, |t| t.min(config.timeout));
        tokio::time::timeout(timeout, sent)
            .await
            .unwrap_or_else(|_| Err("traces export: context deadline exceeded".into()))
    }
}

type Sending<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;

async fn export_grpc(
    config: &Config,
    channel: &tokio::sync::OnceCell<Result<tonic::transport::Channel, String>>,
    request: ExportTraceServiceRequest,
) -> Result<(), String> {
    let channel = channel
        .get_or_init(|| async { grpc_channel(config) })
        .await
        .clone()?;
    let mut client = TraceServiceClient::new(channel);
    if config.gzip {
        client = client.send_compressed(tonic::codec::CompressionEncoding::Gzip);
    }
    let mut request = tonic::Request::new(request);
    request.set_timeout(config.timeout);
    for (key, value) in &config.headers {
        let (Ok(key), Ok(value)) = (
            tonic::metadata::MetadataKey::from_bytes(key.to_lowercase().as_bytes()),
            tonic::metadata::MetadataValue::try_from(value.as_str()),
        ) else {
            continue;
        };
        request.metadata_mut().insert(key, value);
    }
    client
        .export(request)
        .await
        .map(drop)
        .map_err(|status| format!("traces export: {}", status.message()))
}

async fn export_http(
    config: &Config,
    client: &HttpClient,
    request: ExportTraceServiceRequest,
) -> Result<(), String> {
    let scheme = if config.insecure { "http" } else { "https" };
    let url = format!("{scheme}://{}{}", config.endpoint, config.path);
    let body = prost::Message::encode_to_vec(&request);
    let mut builder = hyper::Request::post(&url)
        .header("User-Agent", USER_AGENT)
        .header("Content-Type", "application/x-protobuf");
    for (key, value) in &config.headers {
        builder = builder.header(key.as_str(), value.as_str());
    }
    let body = if config.gzip {
        use std::io::Write;
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let _ = gz.write_all(&body);
        builder = builder.header("Content-Encoding", "gzip");
        gz.finish().unwrap_or_default()
    } else {
        body
    };
    let request = builder
        .body(http_body_util::Full::new(bytes::Bytes::from(body)))
        .map_err(|e| format!("traces export: {e}"))?;
    let response = client
        .request(request)
        .await
        .map_err(|e| format!("traces export: Post \"{url}\": {e}"))?;
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let body = http_body_util::BodyExt::collect(response.into_body())
        .await
        .map(|b| String::from_utf8_lossy(&b.to_bytes()).trim().to_string())
        .unwrap_or_default();
    let body = if body.is_empty() {
        "(empty)".into()
    } else {
        body
    };
    Err(format!(
        "traces export: failed to send to {url}: {status} (body: {body})"
    ))
}

/// The PEM certificates of `pem` (none if it has none or one does not parse).
fn certificates(pem: &[u8]) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    use rustls::pki_types::pem::PemObject;
    rustls::pki_types::CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_default()
}

/// A client certificate chain and its key, if both parse.
fn key_pair(
    cert: &[u8],
    key: &[u8],
) -> Option<(
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
)> {
    use rustls::pki_types::pem::PemObject;
    let certs = certificates(cert);
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(key).ok()?;
    (!certs.is_empty()).then_some((certs, key))
}

/// This exporter's `User-Agent`.
const USER_AGENT: &str = concat!("ratelimit/", env!("CARGO_PKG_VERSION"));

/// The TLS configuration: the system's roots or `CERTIFICATE`, and the client certificate.
fn tls_config(config: &Config) -> Result<rustls::ClientConfig, String> {
    let mut roots = rustls::RootCertStore::empty();
    match &config.ca {
        Some(pem) => {
            for cert in certificates(pem) {
                roots.add(cert).map_err(|e| e.to_string())?;
            }
        }
        None => {
            for cert in rustls_native_certs::load_native_certs().certs {
                let _ = roots.add(cert);
            }
        }
    }
    let builder = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| e.to_string())?
    .with_root_certificates(roots);
    match config.identity.as_ref().and_then(|(c, k)| key_pair(c, k)) {
        Some((certs, key)) => builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| e.to_string()),
        None => Ok(builder.with_no_client_auth()),
    }
}

fn http_client(config: &Config) -> HttpClient {
    let tls = tls_config(config).unwrap_or_else(|e| {
        otel_error(&format!("create tls config: {e}"));
        tls_config(&Config {
            ca: None,
            identity: None,
            ..config.clone()
        })
        .expect("a TLS configuration with the system's roots")
    });
    // Go's TCP connections are `TCP_NODELAY`.
    let mut http = hyper_util::client::legacy::connect::HttpConnector::new();
    http.enforce_http(false);
    http.set_nodelay(true);
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_or_http()
        .enable_http1()
        .wrap_connector(http);
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build(connector)
}

/// Whether gRPC goes over TLS: unless insecure, and always with a certificate configured
/// (Go's TLS credentials win over `INSECURE` and an `http` endpoint).
fn grpc_tls(config: &Config) -> bool {
    !config.insecure || config.has_tls_settings()
}

/// A lazily connected channel to the collector, over TLS as [`grpc_tls`] says.
fn grpc_channel(config: &Config) -> Result<tonic::transport::Channel, String> {
    let tls = grpc_tls(config);
    let scheme = if tls { "https" } else { "http" };
    let mut endpoint =
        tonic::transport::Endpoint::from_shared(format!("{scheme}://{}", config.endpoint))
            .map_err(|e| format!("traces export: {e}"))?
            .user_agent(USER_AGENT)
            .map_err(|e| format!("traces export: {e}"))?;
    if tls {
        let mut tls = tonic::transport::ClientTlsConfig::new();
        tls = match &config.ca {
            Some(pem) => tls.ca_certificate(tonic::transport::Certificate::from_pem(pem)),
            None => tls.with_native_roots(),
        };
        if let Some((cert, key)) = &config.identity {
            tls = tls.identity(tonic::transport::Identity::from_pem(cert, key));
        }
        endpoint = endpoint
            .tls_config(tls)
            .map_err(|e| format!("traces export: {e}"))?;
    }
    Ok(endpoint.connect_lazy())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_parse_like_go() {
        let url = |s| parse_url(s).map(|u| (u.scheme, u.host, u.path));
        assert_eq!(
            url("http://h:1/p?q#f"),
            Some(("http".into(), "h:1".into(), "/p".into()))
        );
        assert_eq!(
            url("localhost:4318"),
            Some(("localhost".into(), String::new(), String::new()))
        );
        assert_eq!(url("127.0.0.1:4318"), None);
        assert_eq!(url(":x"), None);
    }

    #[test]
    fn a_certificate_makes_grpc_use_tls() {
        let insecure = Config {
            insecure: true,
            ..Config::from_env(Protocol::Grpc)
        };
        assert!(!grpc_tls(&insecure));
        assert!(grpc_tls(&Config {
            ca: Some(Vec::new()),
            ..insecure.clone()
        }));
        assert!(grpc_tls(&Config {
            identity: Some((Vec::new(), Vec::new())),
            ..insecure
        }));
    }

    #[test]
    fn variables_apply_in_gos_order() {
        let config = |protocol, vars: &[(&str, &str)]| {
            let vars: Vec<(String, String)> = vars
                .iter()
                .map(|(k, v)| (format!("OTEL_EXPORTER_OTLP_{k}"), v.to_string()))
                .collect();
            Config::from_lookup(protocol, |name| {
                vars.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone())
            })
        };
        let default = config(Protocol::Http, &[]);
        assert_eq!(
            (
                default.endpoint.as_str(),
                default.path.as_str(),
                default.insecure
            ),
            ("localhost:4318", "/v1/traces", false)
        );
        assert_eq!(config(Protocol::Grpc, &[]).endpoint, "localhost:4317");
        // The signal's endpoint wins, its path as given; `INSECURE` decides over the scheme.
        let http = config(
            Protocol::Http,
            &[
                ("ENDPOINT", "http://a:1/base"),
                ("TRACES_ENDPOINT", "https://b:2/custom"),
                ("INSECURE", " TRUE "),
                ("TIMEOUT", "250"),
                ("TRACES_TIMEOUT", "x"),
                ("HEADERS", "a=1"),
                ("TRACES_HEADERS", "b=2"),
                ("COMPRESSION", "gzip"),
                ("TRACES_COMPRESSION", "GZIP"),
            ],
        );
        assert_eq!(
            (http.endpoint.as_str(), http.path.as_str()),
            ("b:2", "/custom")
        );
        assert!(http.insecure);
        assert_eq!(http.timeout, Duration::from_millis(250));
        assert_eq!(http.headers, vec![("b".to_string(), "2".to_string())]);
        assert!(!http.gzip);
        let base = config(Protocol::Http, &[("ENDPOINT", "https://a:1/base/")]);
        assert_eq!(
            (base.path.as_str(), base.insecure),
            ("/base/v1/traces", false)
        );
        let grpc = config(
            Protocol::Grpc,
            &[("ENDPOINT", "unix://h:3/p"), ("TRACES_INSECURE", "false")],
        );
        assert_eq!((grpc.endpoint.as_str(), grpc.insecure), ("h:3/p", false));
        assert!(config(Protocol::Grpc, &[("ENDPOINT", "unix://h:3")]).insecure);
    }

    #[test]
    fn batches_are_set_up_like_go() {
        let settings = |vars: &[(&str, &str)]| {
            BatchSettings::from_lookup(|name| {
                vars.iter()
                    .find(|(k, _)| format!("OTEL_BSP_{k}") == name)
                    .map(|(_, v)| v.to_string())
            })
        };
        let default = settings(&[]);
        assert_eq!(
            (
                default.queue_size,
                default.batch_size,
                default.delay,
                default.timeout
            ),
            (
                2048,
                512,
                Duration::from_secs(5),
                Some(Duration::from_secs(30))
            )
        );
        // Not trimmed: not a number, so the default; 0 and negative values are kept.
        let odd = settings(&[
            ("MAX_QUEUE_SIZE", "100"),
            ("MAX_EXPORT_BATCH_SIZE", "1000"),
            ("SCHEDULE_DELAY", " 10"),
            ("EXPORT_TIMEOUT", "0"),
        ]);
        assert_eq!(
            (odd.queue_size, odd.batch_size, odd.delay, odd.timeout),
            (100, 100, Duration::from_secs(5), None)
        );
        assert_eq!(settings(&[("MAX_QUEUE_SIZE", "1000")]).batch_size, 512);
        assert_eq!(settings(&[("MAX_QUEUE_SIZE", "-1")]).batch_size, -1);
    }

    #[test]
    fn paths_are_cleaned_like_go() {
        assert_eq!(clean_path(" "), "/v1/traces");
        assert_eq!(clean_path("."), "/v1/traces");
        assert_eq!(clean_path("x/y"), "/x/y");
        assert_eq!(clean_path("/"), "/");
    }

    #[test]
    fn headers_parse_like_go() {
        assert_eq!(
            headers(" a = b%20c ,bad,d=e,a=f"),
            vec![("a".into(), "f".into()), ("d".into(), "e".into())]
        );
    }
}
