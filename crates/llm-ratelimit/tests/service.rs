//! The rate limit service's decision logic, ported from envoyproxy/ratelimit@f4e7669
//! `test/service/ratelimit_test.go`. The counting backend is a fake standing in for Redis.

use llm_ratelimit::config::{self, ConfigFile, LoadOptions, RateLimit};
use llm_ratelimit::limiter::SystemClock;
use llm_ratelimit::service::{
    CacheError, LimitOutcome, RateLimitCache, RateLimitService, ServiceError, ServiceSettings,
};
use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor,
    rate_limit_descriptor::{Entry, RateLimitOverride},
};
use llm_ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse,
    rate_limit_response::{Code, DescriptorStatus, rate_limit::Unit},
};
use std::sync::{Arc, Mutex};

/// A counting backend that returns preset statuses and records the limits it was asked to check.
#[derive(Default)]
struct FakeCache {
    statuses: Mutex<Vec<DescriptorStatus>>,
    received: Mutex<Vec<Vec<Option<RateLimit>>>>,
    error: Option<String>,
}

impl FakeCache {
    fn returning(statuses: Vec<DescriptorStatus>) -> Self {
        Self {
            statuses: Mutex::new(statuses),
            ..Default::default()
        }
    }

    fn received(&self) -> Vec<Vec<Option<RateLimit>>> {
        self.received.lock().unwrap().clone()
    }
}

impl RateLimitCache for FakeCache {
    async fn do_limit(
        &self,
        _request: &RateLimitRequest,
        limits: &[Vec<RateLimit>],
    ) -> Result<Vec<Vec<LimitOutcome>>, CacheError> {
        // Each descriptor's `rate_limit:`, if any.
        let received = limits.iter().map(|l| l.first().cloned()).collect();
        self.received.lock().unwrap().push(received);
        if let Some(error) = &self.error {
            return Err(CacheError(error.clone()));
        }
        // A preset status per descriptor with a limit; presets beyond the descriptors are
        // answers for descriptors the request does not have.
        let statuses = self.statuses.lock().unwrap().clone();
        let extra = statuses.len().saturating_sub(limits.len());
        let mut answer: Vec<Vec<LimitOutcome>> = limits
            .iter()
            .zip(statuses.iter().cloned())
            .map(|(limits, status)| match limits.is_empty() {
                true => Vec::new(),
                false => vec![status.into()],
            })
            .collect();
        answer.extend(
            statuses[statuses.len() - extra..]
                .iter()
                .map(|s| vec![s.clone().into()]),
        );
        Ok(answer)
    }
}

/// The service over `cache`, on the system clock.
fn service<C: RateLimitCache>(cache: C, settings: ServiceSettings) -> RateLimitService<C> {
    RateLimitService::new(cache, Arc::new(SystemClock), Arc::default(), settings)
}

fn load(yaml: &str) -> config::RateLimitConfig {
    let file = ConfigFile {
        name: "inline.yaml".into(),
        content: yaml.into(),
    };
    config::load(&[file], &LoadOptions::default()).unwrap()
}

fn request(domain: &str, descriptors: &[&[(&str, &str)]]) -> RateLimitRequest {
    RateLimitRequest {
        domain: domain.into(),
        descriptors: descriptors
            .iter()
            .map(|entries| RateLimitDescriptor {
                entries: entries
                    .iter()
                    .map(|(key, value)| Entry {
                        key: key.to_string(),
                        value: value.to_string(),
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        hits_addend: 1,
    }
}

fn status(code: Code, limit_remaining: u32) -> DescriptorStatus {
    DescriptorStatus {
        code: code as i32,
        limit_remaining,
        ..Default::default()
    }
}

const CONFIG: &str = "
domain: test-domain
descriptors:
  - key: foo
    rate_limit: {unit: minute, requests_per_unit: 10, name: key_name}
";

#[tokio::test]
async fn unmatched_descriptor_is_passed_to_the_cache_without_a_limit() {
    let cache = Arc::new(FakeCache::returning(vec![status(Code::Ok, 0)]));
    let service = service(cache.clone(), ServiceSettings::default());
    service.set_config(load(CONFIG));

    let response = service
        .should_rate_limit(&request("test-domain", &[&[("hello", "world")]]))
        .await
        .unwrap();

    assert_eq!(
        response,
        RateLimitResponse {
            overall_code: Code::Ok as i32,
            statuses: vec![status(Code::Ok, 0)],
            ..Default::default()
        }
    );
    assert_eq!(cache.received(), [vec![None]]);
}

#[tokio::test]
async fn any_descriptor_over_limit_makes_the_request_over_limit() {
    let cache = Arc::new(FakeCache::returning(vec![
        status(Code::OverLimit, 0),
        status(Code::Ok, 0),
    ]));
    let service = service(cache.clone(), ServiceSettings::default());
    service.set_config(load(CONFIG));

    let response = service
        .should_rate_limit(&request(
            "test-domain",
            &[&[("foo", "bar")], &[("hello", "world")]],
        ))
        .await
        .unwrap();

    assert_eq!(response.overall_code(), Code::OverLimit);
    assert_eq!(
        response.statuses,
        [status(Code::OverLimit, 0), status(Code::Ok, 0)]
    );
    let received = cache.received();
    let [limits] = received.as_slice() else {
        panic!("one cache call")
    };
    assert_eq!(
        limits[0]
            .as_ref()
            .map(|l| (l.name.as_str(), l.limit.requests_per_unit)),
        Some(("key_name", 10))
    );
    assert!(limits[1].is_none());
}

#[tokio::test]
async fn invalid_requests_and_missing_config_are_service_errors() {
    let service = service(Arc::new(FakeCache::default()), ServiceSettings::default());

    let error = service
        .should_rate_limit(&request("test-domain", &[&[("hello", "world")]]))
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ServiceError::Service("no rate limit configuration loaded".into())
    );

    service.set_config(load(CONFIG));
    let error = service
        .should_rate_limit(&request("", &[&[("hello", "world")]]))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "rate limit domain must not be empty");
    let error = service
        .should_rate_limit(&request("test-domain", &[]))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "rate limit descriptor list must not be empty"
    );
}

#[tokio::test]
async fn cache_errors_are_reported_as_is() {
    let cache = FakeCache {
        error: Some("cache error".into()),
        ..Default::default()
    };
    let service = service(Arc::new(cache), ServiceSettings::default());
    service.set_config(load(CONFIG));

    let error = service
        .should_rate_limit(&request("test-domain", &[&[("foo", "bar")]]))
        .await
        .unwrap_err();
    assert_eq!(error, ServiceError::Cache(CacheError("cache error".into())));
    assert_eq!(error.to_string(), "cache error");
}

#[tokio::test]
async fn unlimited_descriptors_skip_the_cache_and_report_max_remaining() {
    let cache = Arc::new(FakeCache::returning(vec![
        status(Code::Ok, 9),
        status(Code::Ok, 0),
        status(Code::Ok, 0),
    ]));
    let service = service(cache.clone(), ServiceSettings::default());
    service.set_config(load(
        "
domain: some-domain
descriptors:
  - key: foo
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: baz
    rate_limit: {unlimited: true}
",
    ));

    let response = service
        .should_rate_limit(&request(
            "some-domain",
            &[&[("foo", "bar")], &[("hello", "world")], &[("baz", "qux")]],
        ))
        .await
        .unwrap();

    assert_eq!(response.overall_code(), Code::Ok);
    assert_eq!(
        response.statuses,
        [
            status(Code::Ok, 9),
            status(Code::Ok, 0),
            status(Code::Ok, u32::MAX)
        ]
    );
    let received = cache.received();
    assert_eq!(
        received[0].iter().map(Option::is_some).collect::<Vec<_>>(),
        [true, false, false]
    );
}

const REPLACES: &str = "
domain: d
descriptors:
  - key: specific
    rate_limit: {unit: minute, requests_per_unit: 1, name: specific, replaces: [{name: generic}]}
  - key: generic
    rate_limit: {unit: minute, requests_per_unit: 2, name: generic}
  - key: unnamed
    rate_limit: {unit: minute, requests_per_unit: 3}
  - key: a
    rate_limit: {unit: minute, requests_per_unit: 4, name: a, replaces: [{name: b}]}
  - key: b
    rate_limit: {unit: minute, requests_per_unit: 5, name: b, replaces: [{name: a}]}
  - key: open
    rate_limit: {unlimited: true, name: open, replaces: [{name: generic}]}
";

async fn checked(descriptors: &[&[(&str, &str)]]) -> Vec<Option<u32>> {
    let cache = Arc::new(FakeCache::returning(vec![
        status(Code::Ok, 0);
        descriptors.len()
    ]));
    let service = service(cache.clone(), ServiceSettings::default());
    service.set_config(load(REPLACES));
    service
        .should_rate_limit(&request("d", descriptors))
        .await
        .unwrap();
    cache.received()[0]
        .iter()
        .map(|l| l.as_ref().map(|l| l.limit.requests_per_unit))
        .collect()
}

#[tokio::test]
async fn replaced_limits_are_not_checked() {
    assert_eq!(
        checked(&[&[("generic", "x")], &[("specific", "x")]]).await,
        [None, Some(1)]
    );
    assert_eq!(
        checked(&[&[("generic", "x")], &[("unnamed", "x")]]).await,
        [Some(2), Some(3)]
    );
    // Not transitive: both sides of a cycle are dropped.
    assert_eq!(checked(&[&[("a", "x")], &[("b", "x")]]).await, [None, None]);
    // An unlimited limit still replaces others.
    assert_eq!(
        checked(&[&[("generic", "x")], &[("open", "x")]]).await,
        [None, None]
    );
}

async fn overall(
    settings: ServiceSettings,
    config: &str,
    cases: &[(&[(&str, &str)], Code)],
) -> Code {
    let descriptors: Vec<_> = cases.iter().map(|(d, _)| *d).collect();
    let statuses = cases.iter().map(|(_, code)| status(*code, 0)).collect();
    let service = service(Arc::new(FakeCache::returning(statuses)), settings);
    service.set_config(load(config));
    service
        .should_rate_limit(&request("d", &descriptors))
        .await
        .unwrap()
        .overall_code()
}

const QUOTA: &str = "
domain: d
descriptors:
  - key: regular
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: backend_name
    quota_mode: true
    rate_limit: {unit: minute, requests_per_unit: 100}
    descriptors:
      - key: tenant
        quota_mode: true
        rate_limit: {unit: minute, requests_per_unit: 10}
";

#[tokio::test]
async fn global_shadow_mode_always_answers_ok() {
    let shadow = ServiceSettings {
        global_shadow_mode: true,
        ..Default::default()
    };
    let cases: &[(&[(&str, &str)], Code)] = &[
        (&[("regular", "x")], Code::OverLimit),
        (&[("other", "x")], Code::Ok),
    ];
    assert_eq!(overall(shadow.clone(), QUOTA, cases).await, Code::Ok);
    assert_eq!(
        overall(ServiceSettings::default(), QUOTA, cases).await,
        Code::OverLimit
    );
    // Also over quota groups.
    let cases: &[(&[(&str, &str)], Code)] = &[(&[("backend_name", "a")], Code::OverLimit)];
    assert_eq!(overall(shadow, QUOTA, cases).await, Code::Ok);
}

#[tokio::test]
async fn quota_groups_are_over_only_when_every_group_is_exhausted() {
    let default = ServiceSettings::default();
    let a_tenant: &[(&str, &str)] = &[("backend_name", "a"), ("tenant", "t1")];
    let a_model: &[(&str, &str)] = &[("backend_name", "a")];
    let b_model: &[(&str, &str)] = &[("backend_name", "b")];

    // Within a group (same backend), any exhausted bucket exhausts the group.
    assert_eq!(
        overall(
            default.clone(),
            QUOTA,
            &[(a_tenant, Code::OverLimit), (a_model, Code::Ok)]
        )
        .await,
        Code::OverLimit
    );
    assert_eq!(
        overall(
            default.clone(),
            QUOTA,
            &[(a_tenant, Code::Ok), (a_model, Code::OverLimit)]
        )
        .await,
        Code::OverLimit
    );
    // Across groups, one group with quota left lets the request through (failover).
    assert_eq!(
        overall(
            default.clone(),
            QUOTA,
            &[
                (a_tenant, Code::OverLimit),
                (a_model, Code::OverLimit),
                (b_model, Code::Ok)
            ]
        )
        .await,
        Code::Ok
    );
    assert_eq!(
        overall(
            default.clone(),
            QUOTA,
            &[(a_model, Code::OverLimit), (b_model, Code::OverLimit)]
        )
        .await,
        Code::OverLimit
    );
    // A regular descriptor over its limit is over regardless of quota groups.
    assert_eq!(
        overall(
            default.clone(),
            QUOTA,
            &[(&[("regular", "x")], Code::OverLimit), (b_model, Code::Ok)]
        )
        .await,
        Code::OverLimit
    );
    assert_eq!(
        overall(
            default,
            QUOTA,
            &[(&[("regular", "x")], Code::Ok), (b_model, Code::OverLimit)]
        )
        .await,
        Code::OverLimit
    );
}

#[tokio::test]
async fn global_quota_mode_puts_every_descriptor_in_a_quota_group() {
    let quota = ServiceSettings {
        global_quota_mode: true,
        ..Default::default()
    };
    // Without backend entries, each descriptor is its own group: one with quota left is enough.
    let cases: &[(&[(&str, &str)], Code)] = &[
        (&[("regular", "x")], Code::OverLimit),
        (&[("other", "y")], Code::Ok),
    ];
    assert_eq!(overall(quota.clone(), QUOTA, cases).await, Code::Ok);
    let cases: &[(&[(&str, &str)], Code)] = &[
        (&[("regular", "x")], Code::OverLimit),
        (&[("regular", "y")], Code::OverLimit),
    ];
    assert_eq!(overall(quota, QUOTA, cases).await, Code::OverLimit);
}

#[tokio::test]
async fn a_cache_answer_of_the_wrong_length_is_an_error() {
    // Upstream asserts and crashes the process; per D1 this becomes a clean error.
    for statuses in [vec![status(Code::Ok, 0)], vec![status(Code::Ok, 0); 3]] {
        let service = service(
            Arc::new(FakeCache::returning(statuses)),
            ServiceSettings::default(),
        );
        service.set_config(load(CONFIG));

        let error = service
            .should_rate_limit(&request(
                "test-domain",
                &[&[("foo", "bar")], &[("hello", "world")]],
            ))
            .await
            .unwrap_err();

        assert!(matches!(error, ServiceError::Cache(_)), "{error:?}");
    }
}

#[tokio::test]
async fn a_limit_with_an_unknown_unit_is_an_invalid_argument() {
    // Upstream panics and the process dies (D1: becomes INVALID_ARGUMENT, see docs/parity.md).
    let cache = Arc::new(FakeCache::returning(vec![status(Code::Ok, 0)]));
    let service = service(cache.clone(), ServiceSettings::default());
    service.set_config(load(CONFIG));
    let mut request = request("test-domain", &[&[("foo", "bar")]]);
    request.descriptors[0].limit = Some(
        llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride {
            requests_per_unit: 1,
            unit: 0, // UNKNOWN
            unit_multiplier: None,
        },
    );

    let error = service.should_rate_limit(&request).await.unwrap_err();

    assert_eq!(
        error,
        ServiceError::InvalidArgument("invalid rate limit unit UNKNOWN".into())
    );
    assert!(cache.received().is_empty(), "nothing is counted");
}

/// The `descriptors[].limit` metadata of a request, as a string.
async fn limit_text(limit: RateLimitOverride) -> String {
    use prost_types::value::Kind;
    let cache = Arc::new(FakeCache::returning(vec![status(Code::Ok, 0)]));
    let settings = ServiceSettings {
        response_dynamic_metadata: true,
        ..Default::default()
    };
    let service = service(cache, settings);
    service.set_config(load(CONFIG));
    let mut request = request("test-domain", &[&[("foo", "x")]]);
    request.descriptors[0].limit = Some(limit);
    let metadata = service
        .should_rate_limit(&request)
        .await
        .unwrap()
        .dynamic_metadata
        .unwrap();
    let Some(Kind::ListValue(descriptors)) = &metadata.fields["descriptors"].kind else {
        panic!("descriptors: {metadata:?}");
    };
    let Some(Kind::StructValue(descriptor)) = &descriptors.values[0].kind else {
        panic!("descriptor: {descriptors:?}");
    };
    let Some(Kind::StringValue(limit)) = &descriptor.fields["limit"].kind else {
        panic!("limit: {descriptor:?}");
    };
    limit.clone()
}

/// `unit_multiplier` is an extension upstream does not know (docs/parity.md section 7); it is
/// written as protobuf's text format writes a wrapper message.
#[tokio::test]
async fn client_limit_metadata_shows_the_unit_multiplier() {
    let limit = |unit_multiplier| RateLimitOverride {
        requests_per_unit: 10,
        unit: Unit::Hour as i32,
        unit_multiplier,
    };
    assert_eq!(
        limit_text(limit(Some(5))).await,
        "requests_per_unit:10 unit:HOUR unit_multiplier:{value:5}"
    );
    assert_eq!(
        limit_text(limit(None)).await,
        "requests_per_unit:10 unit:HOUR"
    );
}

/// Upstream merges into the configuration's own structs, so `a`'s metadata would keep `z` from
/// the first request; ours does not (docs/parity.md section 7).
#[tokio::test]
async fn merging_metadata_leaves_the_configuration_unchanged() {
    use prost_types::value::Kind;
    let cache = Arc::new(FakeCache::returning(vec![
        status(Code::Ok, 0),
        status(Code::Ok, 0),
    ]));
    let settings = ServiceSettings {
        response_dynamic_metadata: true,
        ..Default::default()
    };
    let service = service(cache.clone(), settings);
    service.set_config(load(
        "
domain: d
descriptors:
  - key: a
    rate_limit: {unit: minute, requests_per_unit: 10}
    metadata: {shared: {x: 1}}
  - key: b
    rate_limit: {unit: minute, requests_per_unit: 10}
    metadata: {shared: {z: 2}}
",
    ));
    let shared = |response: RateLimitResponse| {
        let metadata = response.dynamic_metadata.unwrap();
        let Some(Kind::StructValue(merged)) = &metadata.fields["metadata"].kind else {
            panic!("metadata: {metadata:?}");
        };
        let Some(Kind::StructValue(shared)) = &merged.fields["shared"].kind else {
            panic!("shared: {merged:?}");
        };
        shared.fields.keys().cloned().collect::<Vec<_>>()
    };

    let both = service
        .should_rate_limit(&request("d", &[&[("a", "x")], &[("b", "x")]]))
        .await
        .unwrap();
    assert_eq!(shared(both), ["x", "z"]);

    *cache.statuses.lock().unwrap() = vec![status(Code::Ok, 0)];
    let alone = service
        .should_rate_limit(&request("d", &[&[("a", "x")]]))
        .await
        .unwrap();
    assert_eq!(shared(alone), ["x"]);
}
