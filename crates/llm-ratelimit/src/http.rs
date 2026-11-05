//! The service's HTTP server on `HOST:PORT` (`src/server/server_impl.go`): a gorilla/mux
//! router with `/healthcheck` and `/json`, and the quota lookup's `/quota` (ADR 0003).

use crate::goserver::{Answer, Request};
use crate::health::HealthChecker;
use crate::service::{LookupError, RateLimitCache, RateLimitService};
use bytes::Bytes;
use http_body_util::BodyExt;
use llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::Code;
use std::sync::Arc;

/// The router on the decoded path (gorilla/mux matches it, not the escaped one). Go's
/// strings hold any bytes: each decoded byte is kept as one char, which `clean` and matching
/// treat alike, and turned back into its byte when escaped.
fn respond(health: &HealthChecker, request: &Request) -> Routed {
    let uri = request.uri();
    // A bad escape was refused before routing (`goserver`).
    let path = crate::go::unescape_bytes(uri.path(), false).unwrap_or_default();
    let path: String = path.into_iter().map(char::from).collect();
    route(health, uri, &path)
}

/// The largest `/json` body read: grpc-go's default largest message.
const MAX_JSON_BODY: usize = 4 << 20;

/// Where a request goes: answered at once, or to `/json` or `/quota`, which read its body.
enum Routed {
    Answer(Answer),
    Json,
    Quota,
}

/// The router on the decoded `path`: a path that is not clean is redirected (301, no body)
/// to the URL with the clean path, then `/healthcheck` and `/json` answer, and anything else
/// is not found. Any method is served.
fn route(health: &HealthChecker, uri: &hyper::Uri, path: &str) -> Routed {
    let clean = crate::mux::clean_path(path);
    if clean != path {
        // `url.URL.String`: the scheme and host of an absolute-form target are kept.
        let mut location = match (uri.scheme_str(), uri.authority()) {
            (Some(scheme), Some(authority)) => format!("{scheme}://{authority}"),
            _ => String::new(),
        };
        let bytes: Vec<u8> = clean.chars().map(|c| c as u8).collect();
        location += &crate::go::escape_path(&bytes);
        if let Some(query) = uri.query() {
            location = format!("{location}?{query}");
        }
        return Routed::Answer(Answer::new(301, vec![("location", location)], b""));
    }
    let text = || vec![("content-type", "text/plain; charset=utf-8".to_string())];
    Routed::Answer(match path {
        "/healthcheck" if health.is_ok() => Answer::new(200, text(), b"OK"),
        "/healthcheck" => Answer::new(500, Vec::new(), b""),
        "/json" => return Routed::Json,
        "/quota" => return Routed::Quota,
        _ => Answer::error(404, "404 page not found"),
    })
}

/// `NewJsonHandler`: the body as a `RateLimitRequest` in protojson, answered by the service
/// (400 for any error) in protojson: 429 when over the limit, 500 for an unknown code or a
/// response protojson cannot write.
async fn json<C: RateLimitCache>(
    service: &RateLimitService<C>,
    body: Result<Bytes, String>,
) -> Answer {
    // Each failure is logged, then answered alike.
    let bad_request = |error: String| {
        crate::log::warn(&format!("error: {error}"));
        Answer::error(400, "Bad Request")
    };
    let request = match body.and_then(|body| crate::protojson::decode_request(&body)) {
        Ok(request) => request,
        Err(error) => return bad_request(error),
    };
    let response = match service.should_rate_limit(&request).await {
        Ok(response) => response,
        Err(error) => return bad_request(error.to_string()),
    };
    // A trace of its own: `/json` passes no context on.
    let _span = crate::trace::start(
        crate::trace::SERVER,
        "NewJsonHandler Remaining Execution",
        crate::trace::Kind::Internal,
        None,
        || {
            vec![(
                "response".into(),
                crate::trace::Value::Str(crate::prototext::response(&response)),
            )]
        },
    );
    crate::debug!("resp:{}", crate::prototext::response(&response));
    let body = match crate::protojson::encode_response(&response) {
        Ok(body) => body,
        Err(error) => {
            crate::log::error(&format!("error marshaling proto3 to json: {error}"));
            return Answer::error(500, "Internal Server Error");
        }
    };
    let status = match response.overall_code() {
        Code::Unknown => 500,
        Code::OverLimit => 429,
        Code::Ok => 200,
    };
    let headers = vec![("content-type", "application/json".to_string())];
    Answer::new(status, headers, body.as_bytes())
}

/// The quota lookup (ADR 0003): the body as a `RateLimitRequest` in protojson, read without
/// counting; its answer or `{"error": …}` in JSON, its causes counted in
/// `ratelimit.quota_lookup.*`.
async fn quota<C: RateLimitCache>(
    service: &RateLimitService<C>,
    body: Result<Bytes, String>,
) -> Answer {
    let stats = service.stats();
    stats.quota_lookup_counter("total_requests").inc();
    let start = std::time::Instant::now();
    let answered = match body.and_then(|body| crate::protojson::decode_request(&body)) {
        Ok(request) => service.lookup(&request).await,
        Err(error) => Err(LookupError::InvalidRequest(error)),
    };
    let json = vec![("content-type", "application/json".to_string())];
    let answer = match answered {
        Ok(lookup) => Answer::new(200, json, crate::quota::render(&lookup).as_bytes()),
        Err(error) => {
            let (status, cause) = error.status_and_cause();
            stats.quota_lookup_counter(&format!("error.{cause}")).inc();
            crate::log::warn(&format!("quota lookup error: {error}"));
            let body = serde_json::json!({"error": error.to_string()}).to_string();
            Answer::new(status, json, body.as_bytes())
        }
    };
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    stats.quota_lookup_timer().add_value(elapsed);
    answer
}

/// Serves HTTP/1 requests from `listener` until `stop` resolves, then closes at once (Go's
/// `http.Server.Close`).
pub async fn serve<C: RateLimitCache>(
    listener: tokio::net::TcpListener,
    health: Arc<HealthChecker>,
    service: Arc<RateLimitService<C>>,
    stop: impl std::future::Future<Output = ()>,
) {
    let handler = move |request: Request| {
        let (health, service) = (health.clone(), service.clone());
        async move {
            match respond(&health, &request) {
                Routed::Answer(answer) => answer,
                Routed::Json => {
                    // Upstream reads any length; past the size gRPC refuses, it is refused.
                    let body = http_body_util::Limited::new(request.into_body(), MAX_JSON_BODY);
                    let body = body.collect().await.map_err(|e| e.to_string());
                    json(&service, body.map(|body| body.to_bytes())).await
                }
                Routed::Quota if request.method() != hyper::Method::POST => {
                    let allow = vec![("allow", "POST".to_string())];
                    Answer::new(405, allow, b"")
                }
                Routed::Quota => {
                    let body = http_body_util::Limited::new(request.into_body(), MAX_JSON_BODY);
                    let body = body.collect().await.map_err(|e| e.to_string());
                    quota(&service, body.map(|body| body.to_bytes())).await
                }
            }
        }
    };
    crate::goserver::serve(listener, handler, stop).await;
}
