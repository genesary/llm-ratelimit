//! envoyproxy/ratelimit's health checker (`src/server/health.go`): the service is healthy when
//! all its components are, as `/healthcheck` and the gRPC health service report it. The gRPC
//! health service is a port of grpc-go's (`health/server.go`), which serves `""` as serving.

use crate::stats::ServerStats;
use llm_ratelimit_proto::grpc::health::v1::{
    HealthCheckRequest, HealthCheckResponse, HealthListRequest, HealthListResponse,
    health_check_response::ServingStatus,
    health_server::{Health, HealthServer},
};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};

/// Redis: unhealthy until its connection is confirmed at startup, and with
/// `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION`, whenever no connection is left.
pub const REDIS: &str = "redis";
/// The config: with `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED`, unhealthy until a config with
/// domains loads.
pub const CONFIG: &str = "config";
/// Unhealthy once the process got SIGTERM.
pub const SIGTERM: &str = "sigterm";

/// The service name upstream reports health for.
const SERVICE: &str = "ratelimit";

/// grpc-go's health server: a status per service name, and the streams watching them.
#[derive(Clone)]
pub struct HealthService {
    statuses: Arc<Mutex<HashMap<String, ServingStatus>>>,
    updates: broadcast::Sender<(String, ServingStatus)>,
    /// Upstream's interceptor counts the unary calls (`Check`, `List`).
    stats: Option<ServerStats>,
}

impl Default for HealthService {
    fn default() -> Self {
        Self {
            statuses: Arc::new(Mutex::new(HashMap::from([(
                String::new(),
                ServingStatus::Serving,
            )]))),
            updates: broadcast::channel(64).0,
            stats: None,
        }
    }
}

impl HealthService {
    /// The service with its unary calls counted in `stats`.
    pub fn with_stats(self, stats: ServerStats) -> Self {
        Self {
            stats: Some(stats),
            ..self
        }
    }

    /// `call`, counted as `method` when stats are kept.
    async fn record<T>(&self, method: &str, call: impl std::future::Future<Output = T>) -> T {
        match &self.stats {
            Some(stats) => stats.record(method, call).await,
            None => call.await,
        }
    }

    /// `SetServingStatus`.
    pub fn set_serving_status(&self, service: &str, status: ServingStatus) {
        self.statuses
            .lock()
            .unwrap()
            .insert(service.to_string(), status);
        let _ = self.updates.send((service.to_string(), status));
    }

    pub fn server(&self) -> HealthServer<Self> {
        HealthServer::new(self.clone())
    }
}

type WatchStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<HealthCheckResponse, Status>> + Send>>;

#[tonic::async_trait]
impl Health for HealthService {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let call = async {
            let statuses = self.statuses.lock().unwrap();
            match statuses.get(&request.get_ref().service) {
                Some(status) => Ok(Response::new(HealthCheckResponse {
                    status: *status as i32,
                })),
                None => Err(Status::not_found("unknown service")),
            }
        };
        self.record("Check", call).await
    }

    async fn list(
        &self,
        _: Request<HealthListRequest>,
    ) -> Result<Response<HealthListResponse>, Status> {
        let call = async {
            let statuses = self.statuses.lock().unwrap();
            Ok(Response::new(HealthListResponse {
                statuses: statuses
                    .iter()
                    .map(|(service, status)| {
                        let response = HealthCheckResponse {
                            status: *status as i32,
                        };
                        (service.clone(), response)
                    })
                    .collect(),
            }))
        };
        self.record("List", call).await
    }

    type WatchStream = WatchStream;

    /// The current status (`SERVICE_UNKNOWN` for a service never set), then each change;
    /// a status equal to the last one sent is skipped. The stream ends as soon as its client
    /// goes away.
    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<WatchStream>, Status> {
        let service = request.into_inner().service;
        let (mut updates, initial) = {
            let statuses = self.statuses.lock().unwrap();
            let initial = statuses
                .get(&service)
                .copied()
                .unwrap_or(ServingStatus::ServiceUnknown);
            (self.updates.subscribe(), initial)
        };
        let stream = async_stream(move |sender| async move {
            let mut last = None;
            let mut next = Some(initial);
            loop {
                if let Some(status) = next.take()
                    && last != Some(status)
                {
                    last = Some(status);
                    let response = HealthCheckResponse {
                        status: status as i32,
                    };
                    if sender.send(Ok(response)).await.is_err() {
                        return;
                    }
                }
                let update = tokio::select! {
                    update = updates.recv() => update,
                    () = sender.closed() => return,
                };
                match update {
                    Ok((name, status)) if name == service => next = Some(status),
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

/// A stream fed by `produce` through a channel.
fn async_stream<F, Fut>(
    produce: F,
) -> tokio_stream::wrappers::ReceiverStream<Result<HealthCheckResponse, Status>>
where
    F: FnOnce(tokio::sync::mpsc::Sender<Result<HealthCheckResponse, Status>>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    tokio::spawn(produce(sender));
    tokio_stream::wrappers::ReceiverStream::new(receiver)
}

/// The components' health, reported to `/healthcheck` and to the gRPC health service.
pub struct HealthChecker {
    components: Mutex<BTreeMap<&'static str, bool>>,
    ok: AtomicBool,
    grpc: HealthService,
}

impl HealthChecker {
    /// `NewHealthChecker`.
    pub fn new(grpc: HealthService, healthy_with_at_least_one_config_loaded: bool) -> Self {
        let mut components = BTreeMap::from([(REDIS, false), (SIGTERM, true)]);
        if healthy_with_at_least_one_config_loaded {
            components.insert(CONFIG, false);
        }
        grpc.set_serving_status(SERVICE, ServingStatus::NotServing);
        Self {
            components: Mutex::new(components),
            ok: AtomicBool::new(false),
            grpc,
        }
    }

    /// Whether every component is healthy (`/healthcheck`).
    pub fn is_ok(&self) -> bool {
        self.ok.load(Ordering::SeqCst)
    }

    /// `Fail`: the component is unhealthy, and so is the service.
    pub fn fail(&self, component: &str) -> Result<(), String> {
        let mut components = self.components.lock().unwrap();
        let Some(healthy) = components.get_mut(component) else {
            return Err(format!("Invalid component: {component}"));
        };
        *healthy = false;
        self.ok.store(false, Ordering::SeqCst);
        self.grpc
            .set_serving_status(SERVICE, ServingStatus::NotServing);
        Ok(())
    }

    /// `Ok`: the component is healthy, and the service too if every component is.
    pub fn ok(&self, component: &str) -> Result<(), String> {
        let mut components = self.components.lock().unwrap();
        let Some(healthy) = components.get_mut(component) else {
            return Err(format!("Invalid component: {component}"));
        };
        *healthy = true;
        let all_healthy = components.values().all(|healthy| *healthy);
        crate::debug!(
            "Health status of components: {}, all healthy: {all_healthy}",
            {
                // Go prints a map sorted by key, as the components are.
                let entries: Vec<String> =
                    components.iter().map(|(k, v)| format!("{k}:{v}")).collect();
                format!("map[{}]", entries.join(" "))
            }
        );
        if all_healthy {
            self.ok.store(true, Ordering::SeqCst);
            self.grpc
                .set_serving_status(SERVICE, ServingStatus::Serving);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Watch whose client went away stops listening at once, not at the next change.
    #[tokio::test]
    async fn watch_ends_with_its_client() {
        let service = HealthService::default();
        let request = Request::new(HealthCheckRequest {
            service: SERVICE.into(),
        });
        let mut stream = service.watch(request).await.unwrap().into_inner();
        // Once the first status is read, the task waits for the next change.
        tokio_stream::StreamExt::next(&mut stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(service.updates.receiver_count(), 1);
        drop(stream);
        for _ in 0..100 {
            if service.updates.receiver_count() == 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the Watch task outlived its client");
    }
}
