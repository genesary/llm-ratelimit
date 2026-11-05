//! The `envoy.service.ratelimit.v3.RateLimitService` gRPC endpoint.

use crate::service::{RateLimitCache, RateLimitService, ServiceError};
use crate::stats::ServerStats;
use llm_ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse,
    rate_limit_service_server::{self, RateLimitServiceServer},
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct GrpcService<C> {
    service: Arc<RateLimitService<C>>,
    stats: ServerStats,
}

/// The gRPC server for `service`, its calls counted in `stats`.
pub fn server<C: RateLimitCache>(
    service: Arc<RateLimitService<C>>,
    stats: ServerStats,
) -> RateLimitServiceServer<GrpcService<C>> {
    RateLimitServiceServer::new(GrpcService { service, stats })
}

#[tonic::async_trait]
impl<C: RateLimitCache> rate_limit_service_server::RateLimitService for GrpcService<C> {
    async fn should_rate_limit(
        &self,
        request: Request<RateLimitRequest>,
    ) -> Result<Response<RateLimitResponse>, Status> {
        // Upstream returns plain Go errors, which grpc-go sends as UNKNOWN with the error text.
        // The server span is the parent of the request's spans.
        let parent = request
            .extensions()
            .get::<crate::trace::SpanContext>()
            .cloned();
        let call =
            crate::trace::with_parent(parent, self.service.should_rate_limit(request.get_ref()));
        self.stats
            .record("ShouldRateLimit", call)
            .await
            .map(Response::new)
            .map_err(|e| match e {
                ServiceError::InvalidArgument(message) => Status::invalid_argument(message),
                e => Status::unknown(e.to_string()),
            })
    }
}

/// grpc-go's checks of a request's headers before any handler: a content type other than
/// `application/grpc` (alone or followed by `+` or `;`) gets `415` with `INVALID_ARGUMENT`,
/// then a method other than `POST` gets `405` with `INTERNAL`.
#[derive(Clone, Copy, Default)]
pub struct Requests;

impl<S> tower::Layer<S> for Requests {
    type Service = Checked<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Checked(inner)
    }
}

#[derive(Clone)]
pub struct Checked<S>(S);

impl<S, B, R> tower::Service<http::Request<B>> for Checked<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<R>>,
    S::Future: Send + 'static,
    R: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    R::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, S::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), S::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        // The last content type, as grpc-go reads each in turn.
        let content_type = request
            .headers()
            .get_all(http::header::CONTENT_TYPE)
            .iter()
            .next_back()
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
            .unwrap_or_default();
        // `ContentSubtype`: what follows `+` or `;`, if a gRPC content type.
        let subtype = content_type
            .strip_prefix("application/grpc")
            .and_then(|rest| match rest.chars().next() {
                None => Some(""),
                Some('+' | ';') => Some(&rest[1..]),
                Some(_) => None,
            });
        let abort = if subtype.is_none() {
            Some((
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                3,
                format!(
                    "invalid gRPC request content-type {}",
                    crate::go::quote(&content_type)
                ),
            ))
        } else if request.method() != http::Method::POST {
            Some((
                http::StatusCode::METHOD_NOT_ALLOWED,
                13,
                format!(
                    "Received a HEADERS frame with :method {} which should be POST",
                    crate::go::quote(request.method().as_str())
                ),
            ))
        } else {
            None
        };
        if let Some((status, code, message)) = abort {
            // `writeEarlyAbort`: headers alone, the content type with the request's subtype.
            let answered = match subtype.unwrap_or_default() {
                "" => "application/grpc".to_string(),
                subtype => format!("application/grpc+{subtype}"),
            };
            let response = http::Response::builder()
                .status(status)
                .header(http::header::CONTENT_TYPE, answered)
                .header("grpc-status", code.to_string())
                .header("grpc-message", encode_grpc_message(&message))
                .body(tonic::body::Body::empty())
                .expect("valid headers");
            return Box::pin(async move { Ok(response) });
        }
        let response = self.0.call(request);
        Box::pin(async move { Ok(response.await?.map(tonic::body::Body::new)) })
    }
}

/// `encodeGrpcMessage`: bytes outside printable ASCII, and `%`, percent-encoded.
fn encode_grpc_message(message: &str) -> String {
    let mut encoded = String::with_capacity(message.len());
    for byte in message.bytes() {
        match byte {
            b' '..=b'~' if byte != b'%' => encoded.push(byte as char),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}
