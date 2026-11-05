//! envoyproxy/ratelimit's `XdsGrpcSotwProvider` (`src/provider/xds_grpc_sotw_provider.go`),
//! with go-control-plane's SotW ADS client (`pkg/client/sotw/v3`): the `RateLimitConfig`
//! resources of an xDS management server, each response loaded as one config, then ACKed,
//! or NACKed with the error. A failed stream is opened again after a backoff, on a new
//! connection.

use crate::config::{self, ConfigError, LoadOptions, RateLimitConfig};
use crate::log;
use crate::settings::Settings;
use llm_ratelimit_proto::envoy::config::core::v3::Node;
use llm_ratelimit_proto::envoy::service::discovery::v3::{DiscoveryRequest, DiscoveryResponse};
use llm_ratelimit_proto::ratelimit::config::ratelimit::v3 as pb;
use prost::Message;
use rustls::pki_types::ServerName;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// `resource.RateLimitConfigType`.
const TYPE_URL: &str = "type.googleapis.com/ratelimit.config.ratelimit.v3.RateLimitConfig";
const CONFIG_NAME: &str = "ratelimit.config.ratelimit.v3.RateLimitConfig";
const STREAM_PATH: &str =
    "/envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources";
/// grpc-go's default `MaxCallRecvMsgSize`.
const DEFAULT_MAX_RECEIVE: i64 = 4 * 1024 * 1024;
/// `sotw.ErrInit`: what `Fetch` returns when the stream could not be opened.
const ERR_INIT: &str = "ads client: grpc connection is not initialized (use InitConnect() method to initialize connection)";

/// What the provider sends the service.
enum Event {
    Config(Result<RateLimitConfig, ConfigError>),
    /// A panic `sendConfigs` recovered that is no config error: the service panics with it.
    Panic(String),
}

/// An event, and who to tell once the service has taken it (the channel is unbuffered).
type Delivery = (Event, oneshot::Sender<()>);

/// The provider, running from [`XdsProvider::start`] until [`stop`](XdsProvider::stop).
pub struct XdsProvider {
    events: Option<mpsc::Receiver<Delivery>>,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl XdsProvider {
    /// `NewXdsGrpcSotwProvider`: connects and watches in the background. `tls` is the
    /// `CONFIG_GRPC_XDS_SERVER_USE_TLS` configuration loaded with the settings; `reload_tls`
    /// loads every TLS file of the settings again, as `sendConfigs` does for each response.
    pub fn start(
        settings: &Settings,
        tls: Option<rustls::ClientConfig>,
        enforce_alpn: bool,
        reload_tls: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        let client = Client::from_settings(settings, tls, enforce_alpn, Box::new(reload_tls));
        let (events, received) = mpsc::channel(1);
        let (stop, stopping) = oneshot::channel();
        let task = tokio::spawn(client.run(events, stopping));
        Self {
            events: Some(received),
            stop,
            task,
        }
    }

    /// Hands each event to `update` from now on (a panic exits), as `NewService`'s loop takes
    /// them.
    pub fn deliver(
        &mut self,
        update: impl Fn(Result<RateLimitConfig, ConfigError>) + Send + 'static,
    ) {
        let Some(mut events) = self.events.take() else {
            return;
        };
        tokio::spawn(async move {
            while let Some((event, taken)) = events.recv().await {
                let _ = taken.send(());
                match event {
                    Event::Config(config) => update(config),
                    Event::Panic(message) => log::panic_exit(&message),
                }
            }
        });
    }

    /// `Stop`: a watch ends (logged as upstream logs it); a dial or a backoff in progress is
    /// abandoned silently, upstream's process exiting before it would notice.
    pub async fn stop(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}

/// Where to dial: grpc-go's view of `CONFIG_GRPC_XDS_SERVER_URL`.
#[derive(Debug, Clone, PartialEq)]
enum Address {
    Tcp(String),
    Unix(String),
}

#[derive(Debug, Clone, PartialEq)]
struct Target {
    address: Address,
    /// The `:authority`, and the TLS server name's source.
    authority: String,
}

/// `grpc.Dial`'s target: a `dns`, `unix` or `passthrough` URL, or else an address
/// (`passthrough`), which the dialer reads as `unix:PATH` or `HOST:PORT`.
fn target(url: &str) -> Target {
    let scheme_end = url.find(':').filter(|&i| {
        let scheme = &url[..i];
        scheme.starts_with(|c: char| c.is_ascii_alphabetic())
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
    });
    if let Some(i) = scheme_end {
        let (host, path) = host_and_path(&url[i + 1..]);
        match url[..i].to_ascii_lowercase().as_str() {
            "unix" if host.is_empty() => {
                return Target {
                    address: Address::Unix(path.to_string()),
                    authority: "localhost".into(),
                };
            }
            "dns" => {
                let endpoint = path.strip_prefix('/').unwrap_or(path);
                // The dns resolver's default port.
                let address = match split_host_port(endpoint) {
                    Some(_) => endpoint.to_string(),
                    None => format!("{}:443", endpoint),
                };
                return Target {
                    address: Address::Tcp(address),
                    authority: endpoint.into(),
                };
            }
            "passthrough" => {
                let endpoint = path.strip_prefix('/').unwrap_or(path);
                return passthrough(endpoint);
            }
            _ => {}
        }
    }
    passthrough(url)
}

/// A URL's host and path (or opaque part), after its scheme's `:`.
fn host_and_path(rest: &str) -> (&str, &str) {
    match rest.strip_prefix("//") {
        Some(rest) => match rest.find('/') {
            Some(j) => (&rest[..j], &rest[j..]),
            None => (rest, ""),
        },
        None => ("", rest),
    }
}

/// The passthrough resolver's address, as grpc-go's dialer (`parseDialTarget`) reads it; Go
/// dials the local system for an empty host.
fn passthrough(endpoint: &str) -> Target {
    let authority = match endpoint.starts_with(':') {
        true => format!("localhost{endpoint}"),
        false => endpoint.to_string(),
    };
    let address = match (endpoint.find(':'), endpoint.find(":/")) {
        (Some(i), None) if &endpoint[..i] == "unix" => Address::Unix(endpoint[i + 1..].into()),
        (_, Some(i)) if &endpoint[..i] == "unix" => {
            let (host, path) = host_and_path(&endpoint[i + 1..]);
            Address::Unix(if path.is_empty() { host } else { path }.into())
        }
        _ => Address::Tcp(authority.clone()),
    };
    Target { address, authority }
}

/// `net.SplitHostPort`'s host and port, if `address` has a port.
fn split_host_port(address: &str) -> Option<(&str, &str)> {
    if let Some(rest) = address.strip_prefix('[') {
        let end = rest.find(']')?;
        let port = rest[end + 1..].strip_prefix(':')?;
        return Some((&rest[..end], port));
    }
    let (host, port) = address.rsplit_once(':')?;
    (!host.contains(':')).then_some((host, port))
}

/// jpillora/backoff's `Backoff`, never reset.
#[derive(Debug, Clone)]
struct Backoff {
    min: i64,
    max: i64,
    factor: f64,
    jitter: bool,
    attempt: u64,
}

impl Backoff {
    fn duration(&mut self) -> i64 {
        let attempt = self.attempt;
        self.attempt += 1;
        self.for_attempt(attempt as f64)
    }

    fn for_attempt(&self, attempt: f64) -> i64 {
        let min = if self.min <= 0 { 100_000_000 } else { self.min };
        let max = if self.max <= 0 {
            10_000_000_000
        } else {
            self.max
        };
        if min >= max {
            return max;
        }
        let factor = if self.factor <= 0.0 { 2.0 } else { self.factor };
        let minf = min as f64;
        let mut durf = minf * factor.powf(attempt);
        if self.jitter {
            durf = crate::random::float64() * (durf - minf) + minf;
        }
        if durf > (i64::MAX - 512) as f64 {
            return max;
        }
        (durf as i64).clamp(min, max)
    }
}

/// grpc-go's connection backoff (`backoff.DefaultConfig`) after `retries` failed attempts.
fn connect_backoff(retries: u32) -> Duration {
    const BASE: f64 = 1e9;
    const MAX: f64 = 120e9;
    if retries == 0 {
        return Duration::from_secs(1);
    }
    let mut backoff = BASE;
    let mut retries = retries;
    while backoff < MAX && retries > 0 {
        backoff *= 1.6;
        retries -= 1;
    }
    backoff = backoff.min(MAX) * (1.0 + 0.2 * (crate::random::float64() * 2.0 - 1.0));
    Duration::from_nanos(backoff.max(0.0) as u64)
}

/// go-control-plane's `adsClient` state, kept across streams.
#[derive(Debug, Default)]
struct Ads {
    acked_version: String,
    /// The last response's version and nonce.
    received: Option<(String, String)>,
}

impl Ads {
    /// `Ack`: the last response received is the last acknowledged.
    fn ack(&mut self, node: &Node) -> DiscoveryRequest {
        self.acked_version = self
            .received
            .as_ref()
            .map(|(version, _)| version.clone())
            .unwrap_or_default();
        self.request(node, None)
    }

    fn nack(&self, node: &Node, message: String) -> DiscoveryRequest {
        self.request(
            node,
            Some(llm_ratelimit_proto::google::rpc::Status {
                message,
                ..Default::default()
            }),
        )
    }

    fn request(
        &self,
        node: &Node,
        error_detail: Option<llm_ratelimit_proto::google::rpc::Status>,
    ) -> DiscoveryRequest {
        DiscoveryRequest {
            version_info: self.acked_version.clone(),
            node: Some(node.clone()),
            type_url: TYPE_URL.into(),
            response_nonce: self
                .received
                .as_ref()
                .map(|(_, nonce)| nonce.clone())
                .unwrap_or_default(),
            error_detail,
            ..Default::default()
        }
    }
}

/// `getClientNode`: the node ID, and the `KEY=VALUE` pairs of the metadata (others skipped).
fn node(id: &str, metadata: &str) -> Node {
    let fields = metadata
        .split(',')
        .filter_map(|entry| entry.split_once('='))
        .map(|(key, value)| {
            (
                key.to_string(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue(value.into())),
                },
            )
        })
        .collect();
    Node {
        id: id.into(),
        metadata: Some(prost_types::Struct { fields }),
        ..Default::default()
    }
}

/// grpc-go's reserved headers, which it leaves out of a call's metadata.
const RESERVED_HEADERS: &[&str] = &[
    "content-type",
    "user-agent",
    "grpc-message-type",
    "grpc-encoding",
    "grpc-message",
    "grpc-status",
    "grpc-timeout",
    "te",
];

/// `metadata.New` of `CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS` (keys lowercased), without
/// grpc-go's reserved headers; `None` if grpc-go refuses them for a call's metadata
/// (`imetadata.Validate`: an empty key, a key not in `[0-9a-z-_.]` unless a pseudo-header, a
/// value not printable ASCII unless `-bin`), which fails the stream before it opens.
fn headers(headers: &BTreeMap<String, String>) -> Option<Vec<(String, String)>> {
    let headers: Vec<(String, String)> = headers
        .iter()
        .map(|(key, value)| (key.to_lowercase(), value.clone()))
        .collect();
    for (key, value) in &headers {
        let key_valid = key.starts_with(':')
            || key
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b));
        let value_valid =
            key.ends_with("-bin") || value.bytes().all(|b| (0x20..=0x7e).contains(&b));
        if key.is_empty() || !key_valid || !value_valid {
            return None;
        }
    }
    Some(
        headers
            .into_iter()
            .filter(|(key, _)| !key.starts_with(':') && !RESERVED_HEADERS.contains(&key.as_str()))
            .collect(),
    )
}

/// The TLS configuration (offering `h2`) and the server name to verify.
type TlsTarget = (Arc<rustls::ClientConfig>, ServerName<'static>);

/// The client's settings, read once.
struct Client {
    url: String,
    target: Target,
    tls: Option<TlsTarget>,
    san: String,
    enforce_alpn: bool,
    /// `XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES`, if set.
    max_receive: Option<i64>,
    /// `None` when grpc-go refuses them.
    headers: Option<Vec<(String, String)>>,
    node: Node,
    options: LoadOptions,
    backoff: Backoff,
    reload_tls: Box<dyn Fn() -> Result<(), String> + Send + Sync>,
}

impl Client {
    fn from_settings(
        settings: &Settings,
        tls: Option<rustls::ClientConfig>,
        enforce_alpn: bool,
        reload_tls: Box<dyn Fn() -> Result<(), String> + Send + Sync>,
    ) -> Self {
        let url = settings.config_grpc_xds_server_url.clone();
        let target = target(&url);
        let san = settings.config_grpc_xds_server_tls_san.clone();
        let tls = tls.map(|mut config| {
            // `credentials.NewTLS` offers `h2`.
            config.alpn_protocols = vec![b"h2".to_vec()];
            // The SAN, or else the authority's host.
            let name = match san.is_empty() {
                false => san.clone(),
                true => split_host_port(&target.authority)
                    .map_or(target.authority.clone(), |(host, _)| host.to_string()),
            };
            let name = ServerName::try_from(name)
                .unwrap_or_else(|_| ServerName::try_from("invalid.").expect("a valid name"));
            (Arc::new(config), name)
        });
        let max = settings.xds_client_max_msg_size_in_bytes;
        Self {
            url,
            target,
            tls,
            san,
            enforce_alpn,
            max_receive: (max != 0).then_some(max),
            headers: headers(&settings.config_grpc_xds_client_additional_headers),
            node: node(
                &settings.config_grpc_xds_node_id,
                &settings.config_grpc_xds_node_metadata,
            ),
            options: settings.load_options(),
            backoff: Backoff {
                min: settings.xds_client_backoff_initial_interval,
                max: settings.xds_client_backoff_max_interval,
                factor: settings.xds_client_backoff_random_factor,
                jitter: settings.xds_client_backoff_jitter,
                attempt: 0,
            },
            reload_tls,
        }
    }

    /// `initXdsClient`: connect and watch, again after each failure.
    async fn run(mut self, events: mpsc::Sender<Delivery>, mut stop: oneshot::Receiver<()>) {
        log::info("Starting xDS client connection for rate limit configurations");
        let mut ads = Ads::default();
        loop {
            let channel = tokio::select! {
                channel = self.dial() => channel,
                _ = &mut stop => return,
            };
            log::info("Connection to xDS Management Server is successful");
            let failure = tokio::select! {
                failure = self.watch(channel, &mut ads, &events) => failure,
                _ = &mut stop => {
                    log::info("Stopping xDS client watch for rate limit configurations");
                    log::error("Failed to receive configuration from xDS Management Server: rpc error: code = Canceled desc = grpc: the client connection is closing");
                    return;
                }
            };
            log::error(&format!(
                "Failed to receive configuration from xDS Management Server: {failure}"
            ));
            crate::debug!("Retry attempt# {:.6}", self.backoff.attempt as f64);
            let wait = self.backoff.duration();
            crate::debug!(
                "Sleeping for {} using exponential backoff\n",
                crate::go::format_duration(wait)
            );
            tokio::select! {
                _ = tokio::time::sleep(crate::settings::duration(wait)) => {}
                _ = &mut stop => return,
            }
        }
    }

    /// `getGrpcConnection`: a blocking `grpc.Dial`, retried with grpc-go's backoff until the
    /// connection is up.
    async fn dial(&self) -> tonic::transport::Channel {
        log::info(&format!("Dialing xDS Management Server: '{}'", self.url));
        if self.tls.is_some() && !self.san.is_empty() {
            log::info(&format!(
                "ServerName used for xDS Management Service hostname verification is {}",
                self.san
            ));
        }
        if let Some(max) = self.max_receive {
            log::info(&format!(
                "Setting xDS gRPC max receive message size to {max} bytes"
            ));
        }
        // The `:authority`: the SAN with TLS, else the target's.
        let authority = match (&self.tls, self.san.is_empty()) {
            (Some(_), false) => self.san.clone(),
            _ => self.target.authority.clone(),
        };
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{authority}"))
            .unwrap_or_else(|_| tonic::transport::Endpoint::from_static("http://localhost"));
        let (target, tls, enforce_alpn) = (
            Arc::new(self.target.clone()),
            self.tls.clone(),
            self.enforce_alpn,
        );
        // Who to tell when the attempt's server first speaks.
        let spoken: Arc<std::sync::Mutex<Option<oneshot::Sender<bool>>>> = Default::default();
        let speaks = spoken.clone();
        let connector = tower::service_fn(move |_: http::Uri| {
            let spoken = speaks.lock().unwrap().take();
            connect(target.clone(), tls.clone(), enforce_alpn, spoken)
        });
        let mut retries = 0;
        loop {
            let backoff = connect_backoff(retries);
            let (speaks, server_spoke) = oneshot::channel();
            *spoken.lock().unwrap() = Some(speaks);
            // Ready once the server has spoken (grpc-go waits for its settings); within
            // grpc-go's `minConnectTimeout`, or the backoff if longer.
            let attempt = tokio::time::timeout(backoff.max(Duration::from_secs(20)), async {
                let channel = endpoint
                    .connect_with_connector(connector.clone())
                    .await
                    .ok()?;
                server_spoke.await.ok()?.then_some(channel)
            });
            if let Ok(Some(channel)) = attempt.await {
                return channel;
            }
            tokio::time::sleep(backoff).await;
            retries += 1;
        }
    }

    /// `InitConnect`, then `watchConfigs`: the responses of one stream, until it fails (the
    /// error as `Fetch` returns it).
    async fn watch(
        &self,
        channel: tonic::transport::Channel,
        ads: &mut Ads,
        events: &mpsc::Sender<Delivery>,
    ) -> String {
        let Some(headers) = &self.headers else {
            return ERR_INIT.into();
        };
        let (requests, outgoing) = mpsc::unbounded_channel();
        let _ = requests.send(ads.ack(&self.node));
        let mut request = tonic::Request::new(
            tokio_stream::wrappers::UnboundedReceiverStream::new(outgoing),
        );
        for (key, value) in headers {
            let metadata = request.metadata_mut();
            match key.ends_with("-bin") {
                true => {
                    if let Ok(key) = tonic::metadata::BinaryMetadataKey::from_bytes(key.as_bytes())
                    {
                        metadata.append_bin(
                            key,
                            tonic::metadata::BinaryMetadataValue::from_bytes(value.as_bytes()),
                        );
                    }
                }
                false => {
                    if let (Ok(key), Ok(value)) = (
                        tonic::metadata::AsciiMetadataKey::from_bytes(key.as_bytes()),
                        tonic::metadata::AsciiMetadataValue::try_from(value.as_str()),
                    ) {
                        metadata.append(key, value);
                    }
                }
            }
        }
        let mut grpc = tonic::client::Grpc::new(channel).max_decoding_message_size(usize::MAX);
        if grpc.ready().await.is_err() {
            return TRANSPORT_ERROR.into();
        }
        let codec = Codec {
            max_receive: self.max_receive.unwrap_or(DEFAULT_MAX_RECEIVE),
        };
        let path = http::uri::PathAndQuery::from_static(STREAM_PATH);
        let mut stream = match grpc.streaming(request, path, codec).await {
            Ok(response) => response.into_inner(),
            Err(status) => return go_status(&status),
        };
        loop {
            let response = match stream.message().await {
                Ok(Some(response)) => response,
                Ok(None) => return "EOF".into(),
                Err(status) => return go_status(&status),
            };
            ads.received = Some((response.version_info.clone(), response.nonce.clone()));
            let request = match self.send_configs(&response.resources, events).await {
                Ok(()) => ads.ack(&self.node),
                Err(message) => ads.nack(&self.node, message),
            };
            let _ = requests.send(request);
        }
    }

    /// `sendConfigs`: the resources loaded as one config and handed to the service; the
    /// error to NACK with, if any.
    async fn send_configs(
        &self,
        resources: &[prost_types::Any],
        events: &mpsc::Sender<Delivery>,
    ) -> Result<(), String> {
        let mut configs = Vec::with_capacity(resources.len());
        for resource in resources {
            match unmarshal(resource) {
                Ok(config) => configs.push(config),
                Err(e) => {
                    log::error(&format!(
                        "Error while unmarshalling config from xDS Management Server: {e}"
                    ));
                    return Err(e);
                }
            }
        }
        // `settings.NewSettings()` reads the TLS files again: its panic is no config error.
        if let Err(e) = (self.reload_tls)() {
            deliver(events, Event::Panic(e.clone())).await;
            return Err(e);
        }
        let config = config::load_xds(&configs, &self.options);
        let nack = config.as_ref().err().map(ToString::to_string);
        deliver(events, Event::Config(config)).await;
        nack.map_or(Ok(()), Err)
    }
}

/// Sends `event` and waits until the service takes it.
async fn deliver(events: &mpsc::Sender<Delivery>, event: Event) {
    let (taken, was_taken) = oneshot::channel();
    if events.send((event, taken)).await.is_ok() {
        let _ = was_taken.await;
    }
}

/// `anypb.UnmarshalTo` into a `RateLimitConfig`, with protobuf-go's error texts.
fn unmarshal(resource: &prost_types::Any) -> Result<pb::RateLimitConfig, String> {
    let url = resource.type_url.as_str();
    // `MessageIs`.
    let is = url
        .strip_suffix(CONFIG_NAME)
        .is_some_and(|rest| rest.is_empty() || rest.ends_with('/'));
    if !is {
        // `MessageName`: after the last `/`, if a valid full name.
        let name = url.rsplit('/').next().unwrap_or_default();
        let valid = name.split('.').all(|ident| {
            ident.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                && ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        });
        let name = if valid { name } else { "" };
        return Err(format!(
            "proto: mismatched message type: got {CONFIG_NAME:?}, want {name:?}"
        ));
    }
    pb::RateLimitConfig::decode(resource.value.as_slice()).map_err(|e| proto_error(&e))
}

/// protobuf-go's text for a decoding failure.
fn proto_error(e: &prost::DecodeError) -> String {
    match e.to_string().contains("not UTF-8") {
        true => "string field contains invalid UTF-8".into(),
        false => "proto: cannot parse invalid wire-format data".into(),
    }
}

/// grpc-go's `rpc error` text of a status a stream ended with.
fn go_status(status: &tonic::Status) -> String {
    // A transport failure, not the server's status.
    if std::error::Error::source(status).is_some() {
        return TRANSPORT_ERROR.into();
    }
    use tonic::Code;
    let code = match status.code() {
        Code::Ok => "OK",
        Code::Cancelled => "Canceled",
        Code::Unknown => "Unknown",
        Code::InvalidArgument => "InvalidArgument",
        Code::DeadlineExceeded => "DeadlineExceeded",
        Code::NotFound => "NotFound",
        Code::AlreadyExists => "AlreadyExists",
        Code::PermissionDenied => "PermissionDenied",
        Code::ResourceExhausted => "ResourceExhausted",
        Code::FailedPrecondition => "FailedPrecondition",
        Code::Aborted => "Aborted",
        Code::OutOfRange => "OutOfRange",
        Code::Unimplemented => "Unimplemented",
        Code::Internal => "Internal",
        Code::Unavailable => "Unavailable",
        Code::DataLoss => "DataLoss",
        Code::Unauthenticated => "Unauthenticated",
    };
    format!("rpc error: code = {code} desc = {}", status.message())
}

/// What grpc-go reports when the connection under a stream is lost.
const TRANSPORT_ERROR: &str = "rpc error: code = Unavailable desc = error reading from server: EOF";

/// The stream's codec: grpc-go's receive size limit, and protobuf-go's error texts.
#[derive(Clone, Copy)]
struct Codec {
    max_receive: i64,
}

impl tonic::codec::Codec for Codec {
    type Encode = DiscoveryRequest;
    type Decode = DiscoveryResponse;
    type Encoder = Codec;
    type Decoder = Codec;

    fn encoder(&mut self) -> Self::Encoder {
        *self
    }

    fn decoder(&mut self) -> Self::Decoder {
        *self
    }
}

impl tonic::codec::Encoder for Codec {
    type Item = DiscoveryRequest;
    type Error = tonic::Status;

    fn encode(
        &mut self,
        item: Self::Item,
        buf: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        item.encode(buf)
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl tonic::codec::Decoder for Codec {
    type Item = DiscoveryResponse;
    type Error = tonic::Status;

    fn decode(
        &mut self,
        buf: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        use bytes::Buf;
        let length = buf.remaining();
        if length as i64 > self.max_receive {
            return Err(tonic::Status::resource_exhausted(format!(
                "grpc: received message larger than max ({length} vs. {})",
                self.max_receive
            )));
        }
        DiscoveryResponse::decode(buf).map(Some).map_err(|e| {
            tonic::Status::internal(format!(
                "grpc: failed to unmarshal the received message: {}",
                proto_error(&e)
            ))
        })
    }
}

/// A connection's stream, plain or over TLS.
trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}

impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Io for T {}

/// One connection attempt: TCP or a Unix socket, then the TLS handshake (`h2` negotiated,
/// unless ALPN is not enforced).
async fn connect(
    target: Arc<Target>,
    tls: Option<TlsTarget>,
    enforce_alpn: bool,
    spoken: Option<oneshot::Sender<bool>>,
) -> std::io::Result<hyper_util::rt::TokioIo<FirstRead>> {
    let io: Box<dyn Io> = match &target.address {
        Address::Tcp(address) => {
            let stream = tokio::net::TcpStream::connect(address.as_str()).await?;
            crate::net::nodelay(&stream);
            Box::new(stream)
        }
        Address::Unix(path) => Box::new(tokio::net::UnixStream::connect(path).await?),
    };
    let io: Box<dyn Io> = match tls {
        None => io,
        Some((config, name)) => {
            let stream = tokio_rustls::TlsConnector::from(config)
                .connect(name, io)
                .await?;
            if enforce_alpn && stream.get_ref().1.alpn_protocol() != Some(b"h2") {
                return Err(std::io::Error::other(
                    "credentials: cannot check peer: missing selected ALPN property",
                ));
            }
            Box::new(stream)
        }
    };
    Ok(hyper_util::rt::TokioIo::new(FirstRead { io, spoken }))
}

/// A connection that tells, at its first read, whether the server spoke (or failed).
struct FirstRead {
    io: Box<dyn Io>,
    spoken: Option<oneshot::Sender<bool>>,
}

impl tokio::io::AsyncRead for FirstRead {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = std::pin::Pin::new(&mut self.io).poll_read(cx, buf);
        if let std::task::Poll::Ready(read) = &polled {
            let spoke = read.is_ok() && buf.filled().len() > before;
            if let Some(spoken) = self.spoken.take() {
                let _ = spoken.send(spoke);
            }
        }
        polled
    }
}

impl tokio::io::AsyncWrite for FirstRead {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_are_read_like_grpc_go() {
        let tcp = |address: &str, authority: &str| Target {
            address: Address::Tcp(address.into()),
            authority: authority.into(),
        };
        let unix = |path: &str| Target {
            address: Address::Unix(path.into()),
            authority: "localhost".into(),
        };
        assert_eq!(
            target("localhost:18000"),
            tcp("localhost:18000", "localhost:18000")
        );
        assert_eq!(
            target("127.0.0.1:18000"),
            tcp("127.0.0.1:18000", "127.0.0.1:18000")
        );
        assert_eq!(target("[::1]:18000"), tcp("[::1]:18000", "[::1]:18000"));
        assert_eq!(target(":18000"), tcp("localhost:18000", "localhost:18000"));
        assert_eq!(target("dns:///xds:18000"), tcp("xds:18000", "xds:18000"));
        assert_eq!(target("DNS:///xds"), tcp("xds:443", "xds"));
        assert_eq!(target("passthrough:///xds:1"), tcp("xds:1", "xds:1"));
        assert_eq!(target("unix:///run/xds.sock"), unix("/run/xds.sock"));
        assert_eq!(target("unix:xds.sock"), unix("xds.sock"));
    }

    #[test]
    fn headers_are_checked_like_grpc_go() {
        let of = |pairs: &[(&str, &str)]| {
            headers(
                &pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            )
        };
        let pair = |k: &str, v: &str| (k.to_string(), v.to_string());
        assert_eq!(
            of(&[("X-A", "1"), ("b.c_d-9", "2")]),
            Some(vec![pair("x-a", "1"), pair("b.c_d-9", "2")])
        );
        // Reserved headers are left out, pseudo-headers too (their values still checked).
        assert_eq!(
            of(&[
                ("te", "x"),
                ("user-agent", "x"),
                ("grpc-status-details-bin", "x"),
                (":p", "x")
            ]),
            Some(vec![pair("grpc-status-details-bin", "x")])
        );
        assert_eq!(of(&[(":p", "\u{1}")]), None);
        assert_eq!(of(&[("", "x")]), None);
        assert_eq!(of(&[("a b", "x")]), None);
        assert_eq!(of(&[("a", "caf\u{e9}")]), None);
        assert_eq!(
            of(&[("a-bin", "caf\u{e9}")]),
            Some(vec![pair("a-bin", "caf\u{e9}")])
        );
    }

    #[test]
    fn backoffs_are_drawn_like_jpillora() {
        let backoff = |min: i64, max: i64, factor: f64| Backoff {
            min,
            max,
            factor,
            jitter: false,
            attempt: 0,
        };
        let s = 1_000_000_000;
        // Upstream's defaults: a factor under 1 never grows.
        let mut defaults = backoff(10 * s, 60 * s, 0.5);
        assert_eq!(
            (0..4).map(|_| defaults.duration()).collect::<Vec<_>>(),
            [10 * s; 4]
        );
        let mut doubling = backoff(s, 5 * s, 2.0);
        assert_eq!(
            (0..5).map(|_| doubling.duration()).collect::<Vec<_>>(),
            [s, 2 * s, 4 * s, 5 * s, 5 * s]
        );
        assert_eq!(backoff(0, 0, 0.0).for_attempt(1.0), 200_000_000);
        assert_eq!(backoff(5 * s, s, 2.0).for_attempt(0.0), s);
        assert_eq!(backoff(s, 5 * s, f64::NAN).for_attempt(3.0), s);
        assert_eq!(backoff(s, 5 * s, f64::INFINITY).for_attempt(3.0), 5 * s);
        let jittered = Backoff {
            jitter: true,
            ..backoff(s, 100 * s, 2.0)
        };
        for _ in 0..100 {
            assert!((s..=8 * s).contains(&jittered.for_attempt(3.0)));
        }
    }
}
