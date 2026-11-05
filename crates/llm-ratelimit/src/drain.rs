//! grpc-go's `GracefulStop` drain, which tonic's lacks: each connection gets a `GOAWAY` and a
//! ping, and is closed once its calls are done, at the ping's answer or 5 s after. Without an
//! answer (a client gone silent), tonic would wait forever: here the server stops 5 s after
//! the shutdown once no call is in flight (its answer's body sent, streams included).

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

/// How long grpc-go waits for the ping's answer.
pub const PING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The calls in flight on the server.
#[derive(Clone, Default)]
pub struct InFlight(Arc<Calls>);

#[derive(Default)]
struct Calls {
    count: AtomicUsize,
    none_left: tokio::sync::Notify,
}

impl InFlight {
    /// Resolves once no call is in flight.
    pub async fn idle(&self) {
        loop {
            let notified = self.0.none_left.notified();
            if self.0.count.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }

    fn start(&self) -> Call {
        self.0.count.fetch_add(1, Ordering::SeqCst);
        Call(self.clone())
    }
}

/// A call in flight until dropped.
struct Call(InFlight);

impl Drop for Call {
    fn drop(&mut self) {
        let calls = &self.0.0;
        if calls.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            calls.none_left.notify_waiters();
        }
    }
}

impl<S> tower::Layer<S> for InFlight {
    type Service = Counted<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Counted {
            inner,
            in_flight: self.clone(),
        }
    }
}

/// A service whose calls are counted until their answer's body ends.
#[derive(Clone)]
pub struct Counted<S> {
    inner: S,
    in_flight: InFlight,
}

impl<S, B, R> tower::Service<http::Request<B>> for Counted<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<R>>,
{
    type Response = http::Response<CountedBody<R>>;
    type Error = S::Error;
    type Future = CountedFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        CountedFuture {
            response: self.inner.call(request),
            call: Some(self.in_flight.start()),
        }
    }
}

pin_project_lite::pin_project! {
    /// The inner service's answer, its body keeping the call in flight.
    pub struct CountedFuture<F> {
        #[pin]
        response: F,
        call: Option<Call>,
    }
}

impl<F, R, E> std::future::Future for CountedFuture<F>
where
    F: std::future::Future<Output = Result<http::Response<R>, E>>,
{
    type Output = Result<http::Response<CountedBody<R>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = std::task::ready!(this.response.poll(cx))?;
        let (parts, body) = response.into_parts();
        Poll::Ready(Ok(http::Response::from_parts(
            parts,
            CountedBody {
                body,
                _call: this.call.take(),
            },
        )))
    }
}

pin_project_lite::pin_project! {
    /// An answer's body: the call is in flight until it is dropped.
    pub struct CountedBody<B> {
        #[pin]
        body: B,
        _call: Option<Call>,
    }
}

impl<B: http_body::Body> http_body::Body for CountedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.project().body.poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
