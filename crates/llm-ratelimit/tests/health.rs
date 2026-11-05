//! The gRPC health service's rules (grpc-go's) and the health checker's components (upstream's
//! `health.go`); the binaries are compared in `differential.rs`.

use llm_ratelimit::health::{CONFIG, HealthChecker, HealthService, REDIS, SIGTERM};
use llm_ratelimit_proto::grpc::health::v1::{
    HealthCheckRequest, health_check_response::ServingStatus, health_server::Health,
};
use tokio_stream::StreamExt;

fn request(service: &str) -> tonic::Request<HealthCheckRequest> {
    tonic::Request::new(HealthCheckRequest {
        service: service.into(),
    })
}

/// Watch sends the current status, then each change, but never the same status twice in a
/// row; an unknown service is SERVICE_UNKNOWN until it is set.
#[tokio::test]
async fn watch_sends_changes_only() {
    let service = HealthService::default();
    let mut known = service.watch(request("svc")).await.unwrap().into_inner();
    service.set_serving_status("svc", ServingStatus::NotServing);
    service.set_serving_status("svc", ServingStatus::NotServing);
    service.set_serving_status("other", ServingStatus::Serving);
    service.set_serving_status("svc", ServingStatus::Serving);
    let mut statuses = Vec::new();
    for _ in 0..3 {
        statuses.push(known.next().await.unwrap().unwrap().status);
    }
    let expected = [
        ServingStatus::ServiceUnknown,
        ServingStatus::NotServing,
        ServingStatus::Serving,
    ];
    assert_eq!(statuses, expected.map(|s| s as i32));
    let nothing = tokio::time::timeout(std::time::Duration::from_millis(100), known.next()).await;
    assert!(nothing.is_err(), "{nothing:?}");
}

#[tokio::test]
async fn check_knows_only_set_services() {
    let service = HealthService::default();
    let empty = service
        .check(request(""))
        .await
        .unwrap()
        .into_inner()
        .status;
    assert_eq!(empty, ServingStatus::Serving as i32);
    let unknown = service.check(request("x")).await.unwrap_err();
    assert_eq!(
        (unknown.code(), unknown.message()),
        (tonic::Code::NotFound, "unknown service")
    );
}

/// Healthy only once every component is: Redis once connected, the config if required;
/// any failure makes it unhealthy again.
#[tokio::test]
async fn health_needs_every_component() {
    let service = HealthService::default();
    let checker = HealthChecker::new(service.clone(), true);
    let status = |s: &HealthService| {
        let s = s.clone();
        async move {
            s.check(request("ratelimit"))
                .await
                .unwrap()
                .into_inner()
                .status
        }
    };
    assert!(!checker.is_ok());
    assert_eq!(status(&service).await, ServingStatus::NotServing as i32);
    checker.ok(REDIS).unwrap();
    assert!(!checker.is_ok());
    checker.ok(CONFIG).unwrap();
    assert!(checker.is_ok());
    assert_eq!(status(&service).await, ServingStatus::Serving as i32);
    checker.fail(SIGTERM).unwrap();
    assert!(!checker.is_ok());
    checker.ok(REDIS).unwrap();
    assert!(!checker.is_ok());
    assert_eq!(checker.ok("bogus").unwrap_err(), "Invalid component: bogus");
    let without_config = HealthChecker::new(HealthService::default(), false);
    without_config.ok(REDIS).unwrap();
    assert!(without_config.is_ok());
}
