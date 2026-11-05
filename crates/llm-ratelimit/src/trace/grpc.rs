//! otelgrpc's server handler (v0.70.0): a server span per gRPC call, named after the method,
//! under the caller's trace, with the semantic conventions `OTEL_SEMCONV_STABILITY_OPT_IN`
//! chooses, the server's address, and the call's final status.

use super::{Kind, Scope, Span, Value};
use std::pin::Pin;
use std::task::{Context, Poll};

/// otelgrpc's tracer.
pub const SCOPE: Scope = Scope {
    name: "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
    version: "0.70.0",
};

/// The attributes of the method: the current conventions, the previous ones (`rpc/old`), or
/// both (`rpc/dup`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Conventions {
    Current,
    Previous,
    Both,
}

/// `parseSemconvMode`: the first of `rpc/dup` or `rpc/old` in the comma-separated list.
fn conventions() -> Conventions {
    let value = std::env::var("OTEL_SEMCONV_STABILITY_OPT_IN").unwrap_or_default();
    for part in value.split(',') {
        match part.trim() {
            "rpc/dup" => return Conventions::Both,
            "rpc/old" => return Conventions::Previous,
            _ => {}
        }
    }
    Conventions::Current
}

/// The layer, for the gRPC server; `uds` is the socket path the server listens on, if any.
/// With tracing off, calls go through untouched.
#[derive(Debug, Clone)]
pub struct Layer {
    conventions: Conventions,
    uds: Option<String>,
}

impl Layer {
    pub fn new(uds: Option<String>) -> Self {
        Layer {
            conventions: conventions(),
            uds,
        }
    }
}

impl<S> tower::Layer<S> for Layer {
    type Service = Traced<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Traced {
            inner,
            layer: self.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Traced<S> {
    inner: S,
    layer: Layer,
}

/// The methods the server registers: grpc-go starts a span only for them (an unknown one
/// gets a span that never ends, so is never exported).
const METHODS: [&str; 4] = [
    "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit",
    "/grpc.health.v1.Health/Check",
    "/grpc.health.v1.Health/List",
    "/grpc.health.v1.Health/Watch",
];

/// The span's name and the method's attributes (`ParseFullMethod`, `ParseFullMethodOld`).
fn method(full: &str, conventions: Conventions) -> (String, Vec<(String, Value)>) {
    let Some(name) = full.strip_prefix('/') else {
        return (full.to_string(), Vec::new());
    };
    let string = |k: &str, v: &str| (k.to_string(), Value::Str(v.to_string()));
    let current = string("rpc.method", name);
    let system_name = string("rpc.system.name", "grpc");
    let mut previous = vec![string("rpc.system", "grpc")];
    if let Some((service, method)) = name.split_once('/') {
        let method = method.split('/').next().unwrap_or_default();
        previous.extend([string("rpc.service", service), string("rpc.method", method)]);
    }
    let attributes = match conventions {
        Conventions::Current => vec![current, system_name],
        Conventions::Previous => previous,
        // The current `rpc.method` replaces the previous one.
        Conventions::Both => {
            let mut attributes = previous;
            super::upsert(&mut attributes, current.0, current.1);
            attributes.push(system_name);
            attributes
        }
    };
    (name.to_string(), attributes)
}

/// `serverAddrAttrs` of the connection's local address: the host and the port, or the
/// socket path.
fn server_address(address: &str) -> Vec<(String, Value)> {
    let Ok((host, port)) = crate::go::split_host_port(address) else {
        return vec![("server.address".into(), Value::Str(address.into()))];
    };
    let mut attributes = vec![("server.address".into(), Value::Str(host))];
    if let Ok(port) = port.parse::<i64>() {
        attributes.push(("server.port".into(), Value::Int(port)));
    }
    attributes
}

/// The connection's local address as Go prints it: an IPv4-mapped address as IPv4.
fn local_address<B>(request: &http::Request<B>) -> Option<String> {
    let address = request
        .extensions()
        .get::<tonic::transport::server::TcpConnectInfo>()?
        .local_addr()?;
    let ip = match address.ip() {
        std::net::IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or(address.ip(), Into::into),
        ip => ip,
    };
    Some(std::net::SocketAddr::from((ip, address.port())).to_string())
}

impl<S, B, R> tower::Service<http::Request<B>> for Traced<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<R>>,
    R: http_body::Body + Unpin,
{
    type Response = http::Response<TracedBody<R>>;
    type Error = S::Error;
    type Future = TracedFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        let path = request.uri().path();
        if !super::enabled() || !METHODS.contains(&path) {
            return TracedFuture {
                response: self.inner.call(request),
                span: None,
            };
        }
        let parent = super::propagation::extract(request.headers());
        let conventions = self.layer.conventions;
        let full = path.to_string();
        let name = full.strip_prefix('/').unwrap_or(&full).to_string();
        let mut span = super::start(SCOPE, &name, Kind::Server, parent.as_ref(), || {
            method(&full, conventions).1
        });
        if span.is_recording() {
            let local = match &self.layer.uds {
                Some(path) => Some(path.clone()),
                None => local_address(&request),
            };
            for (key, value) in local.as_deref().map(server_address).unwrap_or_default() {
                span.set_attribute(&key, value);
            }
        }
        if let Some(context) = span.context() {
            request.extensions_mut().insert(context.clone());
        }
        TracedFuture {
            response: self.inner.call(request),
            span: Some(span),
        }
    }
}

pin_project_lite::pin_project! {
    /// The inner service's answer, its body wrapped to end the span.
    pub struct TracedFuture<F> {
        #[pin]
        response: F,
        span: Option<Span>,
    }
}

impl<F, R, E> std::future::Future for TracedFuture<F>
where
    F: std::future::Future<Output = Result<http::Response<R>, E>>,
{
    type Output = Result<http::Response<TracedBody<R>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = std::task::ready!(this.response.poll(cx))?;
        let (parts, body) = response.into_parts();
        // A trailers-only answer has its status in the headers.
        let status = this.span.as_ref().and_then(|_| grpc_status(&parts.headers));
        Poll::Ready(Ok(http::Response::from_parts(
            parts,
            TracedBody {
                body,
                span: this.span.take(),
                status,
            },
        )))
    }
}

/// The `grpc-status` and `grpc-message` of headers or trailers.
fn grpc_status(headers: &http::HeaderMap) -> Option<(i32, String)> {
    let code = headers.get("grpc-status")?.to_str().ok()?.parse().ok()?;
    let message = headers
        .get("grpc-message")
        .and_then(|m| m.to_str().ok())
        .map(|m| crate::go::unescape(m, false).unwrap_or_else(|| m.to_string()))
        .unwrap_or_default();
    Some((code, message))
}

/// gRPC's status codes, by number.
const CODES: [&str; 17] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];

/// `canonicalString` of a gRPC status code.
fn canonical(code: i32) -> String {
    usize::try_from(code)
        .ok()
        .and_then(|c| CODES.get(c))
        .map_or_else(|| format!("CODE({code})"), |name| name.to_string())
}

/// `serverStatus`: the codes that are the server's fault make the span an error.
fn server_fault(code: i32) -> bool {
    matches!(
        canonical(code).as_str(),
        "UNKNOWN"
            | "DEADLINE_EXCEEDED"
            | "UNIMPLEMENTED"
            | "INTERNAL"
            | "UNAVAILABLE"
            | "DATA_LOSS"
    )
}

/// The answer's body, which ends the span once the call's status is known.
pub struct TracedBody<B> {
    body: B,
    span: Option<Span>,
    status: Option<(i32, String)>,
}

impl<B> TracedBody<B> {
    /// `stats.End`: the status code attribute, and an error status for the server's faults.
    /// With no status, `code`: the call stopped before its end.
    fn end(&mut self, code: i32) {
        let Some(mut span) = self.span.take() else {
            return;
        };
        let (code, message) = self.status.take().unwrap_or((code, String::new()));
        if server_fault(code) {
            span.set_error(&message);
        }
        span.set_attribute("rpc.response.status_code", Value::Str(canonical(code)));
    }
}

impl<B> Drop for TracedBody<B> {
    /// A call dropped before its trailers was cancelled.
    fn drop(&mut self) {
        self.end(1);
    }
}

impl<B: http_body::Body + Unpin> http_body::Body for TracedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let polled = Pin::new(&mut self.body).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(status) = frame.trailers_ref().and_then(grpc_status) {
                    self.status = Some(status);
                    self.end(0);
                }
            }
            Poll::Ready(None) => self.end(0),
            Poll::Ready(Some(Err(_))) => self.end(2),
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn methods_are_named_like_otelgrpc() {
        let method_name = "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit";
        let (name, attributes) = method(method_name, Conventions::Both);
        assert_eq!(name, &method_name[1..]);
        let keys: Vec<&str> = attributes.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            ["rpc.system", "rpc.service", "rpc.method", "rpc.system.name"]
        );
        assert_eq!(attributes[2].1, Value::Str(method_name[1..].into()));
        assert_eq!(canonical(14), "UNAVAILABLE");
        assert_eq!(canonical(42), "CODE(42)");
    }
}
