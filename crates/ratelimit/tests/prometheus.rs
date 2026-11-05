//! The Prometheus sink where upstream panics (`docs/parity.md` §7): those inputs become
//! errors at startup. Everything else is proven against upstream in `differential.rs`.

use ratelimit::stats::prometheus::{self, Mapper, MapperError, PrometheusSettings};
use ratelimit::stats::{PrometheusSink, Sink, SinkSettings, select_sink};

fn is_panic(yaml: &str) -> bool {
    matches!(Mapper::from_yaml(yaml), Err(MapperError::Panic(_)))
}

/// Upstream's mapper has no logger, so a deprecation warning panics; values for its untagged
/// interface fields cannot be decoded and panic too.
#[test]
fn mapper_files_that_panic_upstream_are_errors() {
    assert!(is_panic("mappings:\n- {match: a, name: n, buckets: [1]}"));
    assert!(is_panic("mappings:\n- {match: a, name: n, quantiles: []}"));
    assert!(is_panic("registerer: x"));
    assert!(is_panic("logger: {a: b}"));
    assert!(!is_panic("registerer: ~"));
    // Not when the newer options say the same.
    let both = "mappings:\n- {match: a, name: n, buckets: [1], histogram_options: {x: 1}}";
    assert!(Mapper::from_yaml(both).is_ok());

    let file = std::env::temp_dir().join(format!("panicking-mapper-{}.yaml", std::process::id()));
    std::fs::write(&file, "registerer: x").unwrap();
    let settings = PrometheusSettings {
        mapper_yaml: file.to_string_lossy().into_owned(),
        ..Default::default()
    };
    assert!(prometheus::load_mapper(&settings).is_err());
    std::fs::remove_file(file).unwrap();
}

/// Go's mux panics on a `PROMETHEUS_PATH` that is not a clean absolute path, or reads a method,
/// a host or wildcards in it.
#[test]
fn unsupported_paths_are_errors() {
    for path in [
        "metrics",
        "GET /metrics",
        "/a/{x}",
        "/a/../b",
        "//m",
        "/a/./b",
    ] {
        let sink = PrometheusSink::new(Mapper::default(), path, prometheus::system_clock());
        assert!(sink.is_err(), "{path}");
    }
    for path in ["/", "/metrics", "/stats/", "/a-b/c_d"] {
        let sink = PrometheusSink::new(Mapper::default(), path, prometheus::system_clock());
        assert!(sink.is_ok(), "{path}");
    }
}

#[tokio::test]
async fn prometheus_is_selected_alone() {
    let settings = SinkSettings {
        use_statsd: false,
        use_prometheus: true,
        prometheus: PrometheusSettings {
            addr: "127.0.0.1:0".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(matches!(select_sink(&settings), Ok(Sink::Prometheus(_))));
}

/// Like Go's `net.Listen`: a wildcard host listens on IPv4 and IPv6, a host name on its first
/// IPv4 address.
#[tokio::test]
async fn the_listener_binds_like_go() {
    let free_port = || {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    };
    let (wildcard, named) = (free_port(), free_port());
    let cases = [
        (
            format!(":{wildcard}"),
            vec![format!("127.0.0.1:{wildcard}"), format!("[::1]:{wildcard}")],
        ),
        (
            format!("localhost:{named}"),
            vec![format!("127.0.0.1:{named}")],
        ),
    ];
    for (addr, reachable) in cases {
        let settings = PrometheusSettings {
            addr,
            ..Default::default()
        };
        let _sink = PrometheusSink::start(&settings).unwrap();
        for target in reachable {
            let mut connected = false;
            for _ in 0..100 {
                if tokio::net::TcpStream::connect(&target).await.is_ok() {
                    connected = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(connected, "{target}");
        }
    }
}

/// The shipped mapper puts the quota lookup's error cause in a label (ADR 0003), and its
/// response time in seconds, unless `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS`.
#[test]
fn the_default_mapper_maps_quota_lookup_stats() {
    use ratelimit::stats::prometheus::MetricType;
    let mapper = prometheus::load_mapper(&PrometheusSettings::default()).unwrap();
    let errors = mapper
        .get_mapping("ratelimit.quota_lookup.error.backend", MetricType::Counter)
        .unwrap();
    assert_eq!(errors.name, "ratelimit_quota_lookup_errors");
    assert_eq!(
        errors.labels,
        [("cause".to_string(), "backend".to_string())].into()
    );
    let time = mapper
        .get_mapping("ratelimit.quota_lookup.response_time", MetricType::Observer)
        .unwrap();
    assert_eq!(time.name, "ratelimit_quota_lookup_response_time_seconds");
    assert_eq!(time.scale, Some(0.001));
    let milliseconds = prometheus::load_mapper(&PrometheusSettings {
        response_time_as_milliseconds: true,
        ..Default::default()
    })
    .unwrap();
    let time = milliseconds
        .get_mapping("ratelimit.quota_lookup.response_time", MetricType::Observer)
        .unwrap();
    assert_eq!(time.scale, None);
}

/// The shipped mapper splits a list limit's `~name` into a `limit` label (D29), at one key
/// and deeper, for every counter a list limit emits, `opened_without_hits` included.
#[test]
fn the_default_mapper_labels_list_limits() {
    use ratelimit::stats::prometheus::MetricType;
    let mapper = prometheus::load_mapper(&PrometheusSettings::default()).unwrap();
    let labels = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    for stat in [
        "near_limit",
        "over_limit",
        "total_hits",
        "within_limit",
        "shadow_mode",
        "opened_without_hits",
        "slots_acquired",
        "slots_released",
        "slots_expired",
        "releases_without_slot",
    ] {
        let cases = [
            (
                format!("ratelimit.service.rate_limit.llm.user~five_hour.{stat}"),
                labels(&[("domain", "llm"), ("key1", "user"), ("limit", "five_hour")]),
            ),
            (
                format!("ratelimit.service.rate_limit.llm.user_a~b~week-1.{stat}"),
                labels(&[("domain", "llm"), ("key1", "user_a~b"), ("limit", "week-1")]),
            ),
            (
                format!("ratelimit.service.rate_limit.llm.org_acme.model~weekly.{stat}"),
                labels(&[
                    ("domain", "llm"),
                    ("key1", "org_acme"),
                    ("key2", "model"),
                    ("limit", "weekly"),
                ]),
            ),
            (
                format!("ratelimit.service.rate_limit.llm.org.team.user~daily.{stat}"),
                labels(&[
                    ("domain", "llm"),
                    ("key1", "org"),
                    ("key2", "team"),
                    ("limit", "daily"),
                ]),
            ),
        ];
        for (name, expected) in cases {
            let mapping = mapper
                .get_mapping(&name, MetricType::Counter)
                .unwrap_or_else(|| panic!("{name} unmapped"));
            assert_eq!(mapping.name, format!("ratelimit_service_rate_limit_{stat}"));
            assert_eq!(mapping.labels, expected, "{name}");
        }
    }
    // Deeper, as upstream's deeper rules do, the local cache's denials count as `over_limit`.
    let cached =
        "ratelimit.service.rate_limit.llm.org_acme.model~weekly.over_limit_with_local_cache";
    let mapping = mapper.get_mapping(cached, MetricType::Counter).unwrap();
    assert_eq!(mapping.name, "ratelimit_service_rate_limit_over_limit");
    assert_eq!(
        mapping.labels,
        labels(&[
            ("domain", "llm"),
            ("key1", "org_acme"),
            ("key2", "model"),
            ("limit", "weekly"),
        ])
    );
}
