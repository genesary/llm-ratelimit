//! What each part of the service takes from the environment. Reading the variables themselves
//! is proven against upstream in `differential.rs`.

use llm_ratelimit::settings::Settings;
use llm_ratelimit::stats::StatsdProtocol;

fn settings(environment: &[(&str, &str)]) -> Settings {
    Settings::from_lookup(&|key| {
        environment
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.to_string())
    })
    .unwrap()
}

#[test]
fn the_cache_takes_its_settings() {
    let s = settings(&[
        ("CACHE_KEY_PREFIX", "p_"),
        ("LOCAL_CACHE_SIZE_IN_BYTES", "1048576"),
        ("STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT", "true"),
        ("NEAR_LIMIT_RATIO", "0.5"),
    ]);
    let cache = s.cache();
    assert_eq!(cache.cache_key_prefix, "p_");
    assert_eq!(cache.local_cache_size_in_bytes, 1_048_576);
    assert!(cache.stop_cache_key_increment_when_overlimit);
    assert_eq!(cache.near_limit_ratio, 0.5);
    // freecache raises a negative size to its minimum: the cache still exists.
    let negative = settings(&[("LOCAL_CACHE_SIZE_IN_BYTES", "-1")]).cache();
    assert!(negative.local_cache_size_in_bytes > 0);
    assert_eq!(Settings::default().cache().local_cache_size_in_bytes, 0);
}

#[test]
fn the_service_takes_its_settings() {
    let s = settings(&[
        ("SHADOW_MODE", "1"),
        ("QUOTA_MODE", "t"),
        ("LIMIT_RESPONSE_HEADERS_ENABLED", "true"),
        ("LIMIT_RESET_HEADER", "X-Reset"),
        ("LIMIT_PER_UNIT_HEADERS_ENABLED", "true"),
        ("RESPONSE_DYNAMIC_METADATA", "true"),
    ]);
    let service = s.service();
    assert!(service.global_shadow_mode && service.global_quota_mode);
    let response = service.response_headers.unwrap();
    assert_eq!(
        (
            response.limit.as_str(),
            response.remaining.as_str(),
            response.reset.as_str()
        ),
        ("RateLimit-Limit", "RateLimit-Remaining", "X-Reset")
    );
    assert!(service.request_headers.is_none());
    assert!(service.per_unit_headers && service.response_dynamic_metadata);
    let request = settings(&[
        ("LIMIT_REQUEST_HEADERS_ENABLED", "true"),
        ("LIMIT_REQUEST_LIMIT_HEADER", "X-Limit"),
    ])
    .service()
    .request_headers
    .unwrap();
    assert_eq!(request.limit, "X-Limit");
}

#[test]
fn loading_takes_its_settings() {
    let options = settings(&[
        ("MERGE_DOMAIN_CONFIG", "true"),
        ("SANITIZE_DESCRIPTOR_METRIC_DOTS", "true"),
    ])
    .load_options();
    assert!(options.merge_domain_configs && options.sanitize_descriptor_metric_dots);
}

/// Go's ticker refuses an interval that is not positive.
#[test]
fn the_flush_interval_must_be_positive() {
    let interval = |value| settings(&[("STATS_FLUSH_INTERVAL", value)]).stats_flush_interval();
    assert_eq!(interval("1.5s"), Ok(std::time::Duration::from_millis(1500)));
    assert!(interval("0").is_err());
    assert!(interval("-1s").is_err());
    assert_eq!(
        Settings::default().stats_flush_interval(),
        Ok(std::time::Duration::from_secs(10))
    );
}

/// gostats reads `STATSD_PROTOCOL` itself (an empty value is its default, TCP); datadog-go
/// reads the `DD_*` variables, with origin detection on unless set to a false value.
#[test]
fn the_sink_libraries_read_their_own_variables() {
    let environment = [
        ("STATSD_PROTOCOL", "udp6"),
        ("DD_ENV", "prod"),
        ("DD_ORIGIN_DETECTION_ENABLED", "F"),
        ("STATSD_HOST", "h"),
        ("STATSD_PORT", "9125"),
    ];
    let lookup = |key: &str| {
        environment
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.to_string())
    };
    let sinks = Settings::from_lookup(&lookup)
        .unwrap()
        .sinks(&lookup)
        .unwrap();
    assert_eq!(sinks.statsd_protocol, StatsdProtocol::Udp);
    assert_eq!((sinks.statsd_host.as_str(), sinks.statsd_port), ("h", 9125));
    assert_eq!(sinks.dogstatsd.env, "prod");
    assert!(!sinks.dogstatsd.origin_detection);
    for (protocol, origin, expected_origin) in [("", "", true), ("tcp4", "maybe", true)] {
        let env = [
            ("STATSD_PROTOCOL", protocol),
            ("DD_ORIGIN_DETECTION_ENABLED", origin),
        ];
        let lookup = |key: &str| {
            env.iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        };
        let sinks = Settings::default().sinks(&lookup).unwrap();
        assert_eq!(sinks.statsd_protocol, StatsdProtocol::Tcp);
        assert_eq!(sinks.dogstatsd.origin_detection, expected_origin);
    }
}

/// `docs/environment.md` documents every variable once, with its default: a row
/// ``| `NAME` | `default` | … |``, or `(empty)` for an empty default.
#[test]
fn every_variable_is_documented_with_its_default() {
    let doc = include_str!("../../../docs/environment.md");
    let mut documented = std::collections::BTreeMap::new();
    for line in doc.lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.len() < 4 || !cells[1].starts_with('`') {
            continue;
        }
        let name = cells[1].trim_matches('`');
        if !name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        {
            continue;
        }
        let default = match cells[2] {
            "(empty)" => "",
            other => other.trim_matches('`'),
        };
        assert!(
            documented.insert(name, default).is_none(),
            "{name} is documented twice"
        );
    }
    for (name, default) in Settings::variables() {
        assert_eq!(
            documented.remove(name),
            Some(default),
            "{name}: its row in docs/environment.md, with default `{default}`"
        );
    }
    // Other rows are the AWS SDK's and OpenTelemetry's own variables, with no default here.
    for (name, default) in documented {
        assert!(
            name.starts_with("AWS_") || name.starts_with("OTEL_") || name.starts_with("GRPC_"),
            "{name} is documented but not a setting"
        );
        assert_eq!(default, "", "{name}");
    }
}
