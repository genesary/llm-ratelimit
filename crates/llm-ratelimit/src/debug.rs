//! The debug port (`DEBUG_HOST:DEBUG_PORT`, `src/server/server_impl.go`): a Go `ServeMux`
//! with the endpoints' list at `/`, `/rlconfig`, `/stats` and pprof's paths.

use crate::goserver::{Answer, Request};
use crate::mux::{Route, ServeMux};
use crate::service::{RateLimitCache, RateLimitService};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Endpoint {
    Root,
    PprofIndex,
    Profile,
    Stats,
    Trace,
    RlConfig,
}

/// `AddDebugHttpEndpoint`'s paths and help, as `/` lists them (sorted).
const ENDPOINTS: &[(&str, &str, Endpoint)] = &[
    (
        "/debug/pprof/",
        "root of various pprof endpoints. hit for help.",
        Endpoint::PprofIndex,
    ),
    (
        "/debug/pprof/profile",
        "CPU profiling endpoint",
        Endpoint::Profile,
    ),
    ("/debug/pprof/trace", "trace endpoint", Endpoint::Trace),
    (
        "/rlconfig",
        "print out the currently loaded configuration for debugging",
        Endpoint::RlConfig,
    ),
    ("/stats", "print out stats", Endpoint::Stats),
];

fn mux() -> ServeMux<Endpoint> {
    let mut mux = ServeMux::default();
    for (path, _, endpoint) in ENDPOINTS {
        mux.handle(path, *endpoint).unwrap();
    }
    mux.handle("/", Endpoint::Root).unwrap();
    mux
}

/// pprof's `serveError`.
fn pprof_error(status: u16, text: &str) -> Answer {
    let headers = vec![
        ("x-content-type-options", "nosniff".into()),
        ("content-type", "text/plain; charset=utf-8".into()),
        ("x-go-pprof", "1".into()),
    ];
    Answer::new(status, headers, format!("{text}\n").as_bytes())
}

fn answer<C: RateLimitCache>(endpoint: Endpoint, service: &RateLimitService<C>) -> Answer {
    match endpoint {
        Endpoint::Root => {
            let list: String = ENDPOINTS
                .iter()
                .map(|(path, help, _)| format!("{path}: {help}\n"))
                .collect();
            Answer::sniffed(200, Vec::new(), list.as_bytes())
        }
        Endpoint::RlConfig => {
            let dump = service
                .current_config()
                .map(|c| c.dump())
                .unwrap_or_default();
            Answer::sniffed(200, Vec::new(), dump.as_bytes())
        }
        // expvar's variables, sorted: `cmdline`, and `memstats`, which has no equivalent here.
        Endpoint::Stats => {
            let args: Vec<String> = std::env::args_os()
                .map(|arg| crate::go::json_string(arg.as_encoded_bytes()))
                .collect();
            let body = format!("cmdline: [{}]\n", args.join(","));
            Answer::sniffed(200, Vec::new(), body.as_bytes())
        }
        // No Go runtime to profile: what pprof answers when it cannot start, or for a profile
        // it does not have (the index included).
        Endpoint::Profile => pprof_error(500, "Could not enable CPU profiling: not available"),
        Endpoint::Trace => pprof_error(500, "Could not enable tracing: not available"),
        Endpoint::PprofIndex => pprof_error(404, "Unknown profile"),
    }
}

/// Serves the debug port from `listener` until `stop` resolves.
pub async fn serve<C: RateLimitCache>(
    listener: tokio::net::TcpListener,
    service: Arc<RateLimitService<C>>,
    stop: impl std::future::Future<Output = ()>,
) {
    let mux = Arc::new(mux());
    let handler = move |request: Request| {
        let (mux, service) = (mux.clone(), service.clone());
        async move {
            let uri = request.uri();
            // A bad escape was refused before routing (`goserver`).
            let path = crate::goserver::request_path(uri).unwrap_or_default();
            let connect = request.method() == hyper::Method::CONNECT;
            match mux.route(&path, uri.query(), connect) {
                Route::Handler(endpoint) => answer(*endpoint, &service),
                Route::BadRequest => Answer::star(request.version()),
                Route::Redirect(location) => Answer::redirect(&location, request.method()),
                Route::NotFound => Answer::error(404, "404 page not found"),
            }
        }
    };
    crate::goserver::serve(listener, handler, stop).await;
}
