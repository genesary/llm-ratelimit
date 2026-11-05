//! Config loading, ported from envoyproxy/ratelimit@f4e7669 `test/config/config_test.go`.
//! The YAML fixtures in `tests/fixtures/config` are copied unchanged from upstream `test/config`.

use llm_ratelimit::config::{
    self, ConfigFile, LimitLookup, LoadOptions, RateLimit, RateLimitConfig,
};
use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor,
    rate_limit_descriptor::{Entry, RateLimitOverride},
};
use llm_ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::rate_limit::Unit;
use llm_ratelimit_proto::envoy::r#type::v3::RateLimitUnit;

fn fixture(name: &str) -> ConfigFile {
    let path = format!(
        "{}/tests/fixtures/config/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    ConfigFile {
        name: name.to_string(),
        content: std::fs::read_to_string(path).unwrap(),
    }
}

fn load(names: &[&str]) -> RateLimitConfig {
    let files: Vec<_> = names.iter().map(|n| fixture(n)).collect();
    config::load(&files, &LoadOptions::default()).unwrap()
}

fn descriptor(entries: &[(&str, &str)]) -> RateLimitDescriptor {
    RateLimitDescriptor {
        entries: entries
            .iter()
            .map(|(key, value)| Entry {
                key: key.to_string(),
                value: value.to_string(),
            })
            .collect(),
        ..Default::default()
    }
}

fn limit(config: &RateLimitConfig, domain: &str, entries: &[(&str, &str)]) -> Option<RateLimit> {
    match config.get_limit(domain, &descriptor(entries)) {
        LimitLookup::Limit(limit) => Some(limit),
        LimitLookup::NoLimit | LimitLookup::UnknownDomain | LimitLookup::List(_) => None,
    }
}

#[test]
fn nested_key_without_value_matches_any_value() {
    let config = load(&["basic_config.yaml"]);

    let rl = limit(
        &config,
        "test-domain",
        &[("key1", "value1"), ("subkey1", "something")],
    )
    .unwrap();

    assert_eq!(rl.limit.requests_per_unit, 5);
    assert_eq!(rl.limit.unit(), Unit::Second);
    assert_eq!(rl.full_key, "test-domain.key1_value1.subkey1");
}

#[test]
fn unknown_domain_is_distinguished_from_unmatched_descriptor() {
    let config = load(&["basic_config.yaml"]);

    assert_eq!(
        config.get_limit("foo_domain", &descriptor(&[])),
        LimitLookup::UnknownDomain
    );
    assert_eq!(
        config.get_limit("test-domain", &descriptor(&[])),
        LimitLookup::NoLimit
    );
    for entries in [
        &[("key1", "something")][..],
        &[("key1", "value1")],
        &[("key2", "value2"), ("subkey", "subvalue")],
        &[("key5", "value5"), ("subkey5", "subvalue")],
        &[("key2", "value3")],
    ] {
        assert_eq!(
            config.get_limit("test-domain", &descriptor(entries)),
            LimitLookup::NoLimit,
            "{entries:?}"
        );
    }
}

#[test]
fn specific_value_overrides_key_default() {
    let config = load(&["basic_config.yaml"]);

    let rl = limit(
        &config,
        "test-domain",
        &[("key1", "value1"), ("subkey1", "subvalue1")],
    )
    .unwrap();
    assert_eq!(
        (rl.limit.requests_per_unit, rl.limit.unit()),
        (10, Unit::Second)
    );
    assert_eq!(rl.full_key, "test-domain.key1_value1.subkey1_subvalue1");

    let rl = limit(&config, "test-domain", &[("key2", "something")]).unwrap();
    assert_eq!(
        (rl.limit.requests_per_unit, rl.limit.unit()),
        (20, Unit::Minute)
    );
    assert_eq!(rl.full_key, "test-domain.key2");

    let rl = limit(&config, "test-domain", &[("key2", "value2")]).unwrap();
    assert_eq!(
        (rl.limit.requests_per_unit, rl.limit.unit()),
        (30, Unit::Minute)
    );
    assert_eq!(rl.full_key, "test-domain.key2_value2");
}

#[test]
fn limit_name_and_unlimited_are_carried() {
    let config = load(&["basic_config.yaml"]);

    let rl = limit(&config, "test-domain", &[("key3", "foo")]).unwrap();
    assert_eq!(
        (rl.limit.requests_per_unit, rl.limit.unit()),
        (1, Unit::Hour)
    );
    assert_eq!(rl.limit.name, "");

    let rl = limit(&config, "test-domain", &[("key4", "foo")]).unwrap();
    assert_eq!(
        (rl.limit.requests_per_unit, rl.limit.unit()),
        (1, Unit::Day)
    );
    assert_eq!(rl.limit.name, "key4_rate_limit");
    assert_eq!(rl.name, "key4_rate_limit");
    assert!(!rl.unlimited);

    let rl = limit(&config, "test-domain", &[("key6", "foo")]).unwrap();
    assert!(rl.unlimited);
    assert_eq!(rl.full_key, "test-domain.key6");

    let rl = limit(&config, "test-domain", &[("key8", "foo")]).unwrap();
    assert_eq!(
        (rl.limit.requests_per_unit, rl.limit.unit()),
        (200, Unit::Month)
    );
}

#[test]
fn detailed_metric_names_stats_after_request_values() {
    let config = load(&["basic_config.yaml"]);

    let rl = limit(&config, "test-domain", &[("key7", "unspecified_value")]).unwrap();
    assert_eq!(
        (rl.limit.requests_per_unit, rl.limit.unit()),
        (70, Unit::Minute)
    );
    assert_eq!(rl.full_key, "test-domain.key7_unspecified_value");

    let rl = limit(&config, "test-domain", &[("key7", "another_value")]).unwrap();
    assert_eq!(rl.full_key, "test-domain.key7_another_value");
}

fn load_error(names: &[&str], options: &LoadOptions) -> String {
    let files: Vec<_> = names.iter().map(|n| fixture(n)).collect();
    config::load(&files, options).unwrap_err().to_string()
}

#[test]
fn invalid_configs_are_rejected_with_upstream_messages() {
    let cases: &[(&[&str], &str)] = &[
        (
            &["empty_domain.yaml"],
            "empty_domain.yaml: config file cannot have empty domain",
        ),
        (
            &["basic_config.yaml", "duplicate_domain.yaml"],
            "duplicate_domain.yaml: duplicate domain 'test-domain' in config file",
        ),
        (
            &["empty_key.yaml"],
            "empty_key.yaml: descriptor has empty key",
        ),
        (
            &["duplicate_key.yaml"],
            "duplicate_key.yaml: duplicate descriptor composite key 'test-domain.key1_value1'",
        ),
        (
            &["bad_limit_unit.yaml"],
            "bad_limit_unit.yaml: invalid rate limit unit 'foo'",
        ),
        (
            &["replaces_self.yaml"],
            "replaces_self.yaml: replaces should not contain name of same descriptor",
        ),
        (
            &["replaces_empty.yaml"],
            "replaces_empty.yaml: should not have an empty replaces entry",
        ),
        (
            &["misspelled_key.yaml"],
            "misspelled_key.yaml: config error, unknown key 'ratelimit'",
        ),
        (
            &["misspelled_key2.yaml"],
            "misspelled_key2.yaml: config error, unknown key 'requestsperunit'",
        ),
        (
            &["non_string_key.yaml"],
            "non_string_key.yaml: config error, key is not of type string: 0.25",
        ),
        (
            &["non_map_list.yaml"],
            "non_map_list.yaml: config error, yaml file contains list of type other than map: a",
        ),
        (
            &["unlimited_with_unit.yaml"],
            "unlimited_with_unit.yaml: should not specify rate limit unit when unlimited",
        ),
    ];
    for (files, expected) in cases {
        assert_eq!(load_error(files, &LoadOptions::default()), *expected);
    }
}

#[test]
fn yaml_syntax_errors_are_config_errors() {
    // Upstream: "bad_yaml.yaml: error loading config file: yaml: line 2: found unexpected end of stream".
    // The parser's wording and reported position differ from Go's libyaml port (documented
    // divergence); only the message structure is kept.
    let error = load_error(&["bad_yaml.yaml"], &LoadOptions::default());
    assert!(
        error.starts_with("bad_yaml.yaml: error loading config file: yaml: line "),
        "{error}"
    );
}

#[test]
fn merged_domains_combine_descriptors() {
    let files: Vec<_> = ["merge_domain_key1.yaml", "merge_domain_key2.yaml"]
        .map(fixture)
        .to_vec();
    let options = LoadOptions {
        merge_domain_configs: true,
        ..Default::default()
    };
    let config = config::load(&files, &options).unwrap();

    assert_eq!(
        limit(&config, "test-domain", &[("key1", "value1")])
            .unwrap()
            .limit
            .requests_per_unit,
        10
    );
    assert_eq!(
        limit(&config, "test-domain", &[("key2", "value2")])
            .unwrap()
            .limit
            .requests_per_unit,
        20
    );
    assert_eq!(
        load_error(
            &["merge_domain_key1.yaml", "merge_domain_key1.yaml"],
            &options
        ),
        "merge_domain_key1.yaml: duplicate descriptor composite key 'test-domain.key1_value1'"
    );
}

fn with_override(
    entries: &[(&str, &str)],
    requests_per_unit: u32,
    unit: RateLimitUnit,
) -> RateLimitDescriptor {
    RateLimitDescriptor {
        limit: Some(RateLimitOverride {
            requests_per_unit,
            unit: unit as i32,
            unit_multiplier: None,
        }),
        ..descriptor(entries)
    }
}

#[test]
fn request_limit_override_replaces_configured_limit() {
    let config = load(&["basic_config.yaml"]);

    assert_eq!(
        config.get_limit("foo_domain", &with_override(&[], 10, RateLimitUnit::Day)),
        LimitLookup::UnknownDomain
    );

    let entries = [("key1", "value1"), ("subkey1", "something")];
    let LimitLookup::Limit(rl) = config.get_limit(
        "test-domain",
        &with_override(&entries, 10, RateLimitUnit::Day),
    ) else {
        panic!("override should apply");
    };
    assert_eq!(rl.full_key, "test-domain.key1_value1.subkey1_something");
    assert_eq!(
        (
            rl.limit.requests_per_unit,
            rl.limit.unit(),
            rl.limit.name.as_str()
        ),
        (10, Unit::Day, "")
    );

    let entries = [("key1", "value1"), ("subkey1", "something_else")];
    let LimitLookup::Limit(rl) = config.get_limit(
        "test-domain",
        &with_override(&entries, 42, RateLimitUnit::Hour),
    ) else {
        panic!("override should apply");
    };
    assert_eq!(
        rl.full_key,
        "test-domain.key1_value1.subkey1_something_else"
    );
    assert_eq!(
        (rl.limit.requests_per_unit, rl.limit.unit()),
        (42, Unit::Hour)
    );

    // Applies even when no configured rule matches the descriptor.
    let LimitLookup::Limit(rl) = config.get_limit(
        "test-domain",
        &with_override(&[("nope", "")], 1, RateLimitUnit::Second),
    ) else {
        panic!("override should apply");
    };
    assert_eq!(rl.full_key, "test-domain.nope");
}

#[test]
fn shadow_mode_is_per_descriptor() {
    let config = load(&["shadowmode_config.yaml"]);

    type Case<'a> = (&'a [(&'a str, &'a str)], u32, bool);
    let cases: &[Case] = &[
        (&[("key1", "value1"), ("subkey1", "something")], 5, false),
        (&[("key1", "value1"), ("subkey1", "subvalue1")], 10, true),
        (&[("key2", "something")], 20, true),
        (&[("key2", "value2")], 30, false),
    ];
    for (entries, requests_per_unit, shadow_mode) in cases {
        let rl = limit(&config, "test-domain", entries).unwrap();
        assert_eq!(
            (rl.limit.requests_per_unit, rl.shadow_mode),
            (*requests_per_unit, *shadow_mode)
        );
    }
}

#[test]
fn wildcard_values_share_one_limit_named_after_the_pattern() {
    let config = load(&["wildcard.yaml"]);
    let key =
        |entries: &[(&str, &str)]| limit(&config, "test-domain", entries).map(|rl| rl.full_key);

    assert_eq!(
        key(&[("noVal", "foo1")]).as_deref(),
        Some("test-domain.noVal")
    );
    assert_eq!(
        key(&[("wild", "foo1")]).as_deref(),
        Some("test-domain.wild_foo*")
    );
    assert_eq!(
        key(&[("wild", "foo2")]).as_deref(),
        Some("test-domain.wild_foo*")
    );
    assert_eq!(
        key(&[("nestedWild", "val1"), ("wild", "goo2")]).as_deref(),
        Some("test-domain.nestedWild_val1.wild_goo*")
    );
    assert_eq!(key(&[("wild", "bar")]), None);
    assert_eq!(
        key(&[("noWild", "foo1")]),
        None,
        "plain values don't match eagerly"
    );

    for value in ["barab", "bar123b", "barb"] {
        assert_eq!(
            key(&[("midWild", value)]).as_deref(),
            Some("test-domain.midWild_bar*b"),
            "{value}"
        );
    }
    for value in ["bara", "xbarb"] {
        assert_eq!(key(&[("midWild", value)]), None, "{value}");
    }
    for value in ["foo123bar456baz", "foobarbaz"] {
        assert_eq!(
            key(&[("multiWild", value)]).as_deref(),
            Some("test-domain.multiWild_foo*bar*baz"),
            "{value}"
        );
    }
    for value in ["foo123baz", "bar456baz"] {
        assert_eq!(key(&[("multiWild", value)]), None, "{value}");
    }
}

fn inline_file(content: &str) -> ConfigFile {
    ConfigFile {
        name: "inline.yaml".to_string(),
        content: content.to_string(),
    }
}

fn load_yaml(content: &str) -> RateLimitConfig {
    config::load(&[inline_file(content)], &LoadOptions::default()).unwrap()
}

fn full_key(config: &RateLimitConfig, domain: &str, entries: &[(&str, &str)]) -> String {
    limit(config, domain, entries)
        .expect("a limit should match")
        .full_key
}

const NESTED_REALISTIC: &str = "
domain: nested
descriptors:
  - key: key_1
    rate_limit: {requests_per_unit: 500, unit: minute}
    detailed_metric: true
  - key: key_1
    value: some-value-for-key-1
    rate_limit: {requests_per_unit: 500, unit: minute}
  - key: key_2
    rate_limit: {requests_per_unit: 5000, unit: minute}
    detailed_metric: true
    descriptors:
      - key: key_3
        detailed_metric: true
      - key: key_3
        value: requested-key3-value
        rate_limit: {requests_per_unit: 5, unit: minute}
        detailed_metric: true
  - key: key_2
    value: specific-id
    rate_limit: {requests_per_unit: 50000, unit: minute}
    detailed_metric: true
    descriptors:
      - key: key_3
        detailed_metric: true
      - key: key_3
        value: requested-key3-value
        rate_limit: {requests_per_unit: 100, unit: minute}
        detailed_metric: true
";

fn two_level(top_detailed: bool, leaf_detailed: bool) -> String {
    format!(
        "
domain: nested
descriptors:
  - key: key-1
    rate_limit: {{requests_per_unit: 100, unit: minute}}
    detailed_metric: {top_detailed}
    descriptors:
      - key: key-2
        rate_limit: {{requests_per_unit: 5, unit: minute}}
        detailed_metric: {leaf_detailed}
"
    )
}

#[test]
fn detailed_metric_keys_follow_the_matched_leaf() {
    let config = load_yaml(&two_level(true, false));
    assert_eq!(
        full_key(&config, "nested", &[("key-1", "value-1")]),
        "nested.key-1_value-1"
    );
    let config = load_yaml(&two_level(false, false));
    assert_eq!(
        full_key(&config, "nested", &[("key-1", "value-1")]),
        "nested.key-1"
    );
    let config = load_yaml(&two_level(true, true));
    assert_eq!(
        full_key(
            &config,
            "nested",
            &[("key-1", "value-1"), ("key-2", "value-2")]
        ),
        "nested.key-1_value-1.key-2_value-2"
    );
    let config = load_yaml(&two_level(false, false));
    assert_eq!(
        full_key(
            &config,
            "nested",
            &[("key-1", "value-1"), ("key-2", "value-2")]
        ),
        "nested.key-1.key-2"
    );

    let config = load_yaml(NESTED_REALISTIC);
    assert_eq!(
        full_key(&config, "nested", &[("key_1", "value-for-key-1")]),
        "nested.key_1_value-for-key-1"
    );
    assert_eq!(
        full_key(&config, "nested", &[("key_2", "key-2-value")]),
        "nested.key_2_key-2-value"
    );
    assert_eq!(
        full_key(
            &config,
            "nested",
            &[("key_2", "key-2-value"), ("key_3", "requested-key3-value")]
        ),
        "nested.key_2_key-2-value.key_3_requested-key3-value"
    );
}

const VALUE_TO_METRIC: &str = "
domain: domain
descriptors:
  - key: route
    value_to_metric: true
    descriptors:
      - key: http_method
        value_to_metric: true
        descriptors:
          - key: subject_id
            rate_limit: {requests_per_unit: 60, unit: minute}
";

#[test]
fn value_to_metric_puts_request_values_in_the_stats_key() {
    let config = load_yaml(VALUE_TO_METRIC);
    let key = |entries: &[(&str, &str)]| full_key(&config, "domain", entries);
    assert_eq!(
        key(&[
            ("route", "draw"),
            ("http_method", "GET"),
            ("subject_id", "123")
        ]),
        "domain.route_draw.http_method_GET.subject_id"
    );
    // Empty values add no `_` suffix.
    assert_eq!(
        key(&[("route", ""), ("http_method", "GET"), ("subject_id", "123")]),
        "domain.route.http_method_GET.subject_id"
    );
    assert_eq!(
        key(&[
            ("route", "draw"),
            ("http_method", ""),
            ("subject_id", "123")
        ]),
        "domain.route_draw.http_method.subject_id"
    );
    assert_eq!(
        key(&[("route", ""), ("http_method", ""), ("subject_id", "123")]),
        "domain.route.http_method.subject_id"
    );

    let config = load_yaml(
        "
domain: d
descriptors:
  - key: k1
    value_to_metric: true
    descriptors:
      - key: k2
        rate_limit: {requests_per_unit: 1, unit: second}
",
    );
    assert_eq!(
        full_key(&config, "d", &[("k1", "A"), ("k2", "foo")]),
        "d.k1_A.k2"
    );

    let config = load_yaml(
        "
domain: d
descriptors:
  - key: k1
    descriptors:
      - key: k2
        value_to_metric: true
        descriptors:
          - key: k3
            rate_limit: {requests_per_unit: 1, unit: second}
",
    );
    assert_eq!(
        full_key(&config, "d", &[("k1", "X"), ("k2", "Y"), ("k3", "Z")]),
        "d.k1.k2_Y.k3"
    );

    let config = load_yaml(
        "
domain: d
descriptors:
  - key: k1
    descriptors:
      - key: k2
        rate_limit: {requests_per_unit: 1, unit: second}
",
    );
    assert_eq!(
        full_key(&config, "d", &[("k1", "X"), ("k2", "Y")]),
        "d.k1.k2"
    );
}

#[test]
fn value_to_metric_with_configured_and_wildcard_values() {
    let config = load_yaml(
        "
domain: test-domain
descriptors:
  - key: route
    value_to_metric: true
    descriptors:
      - key: http_method
        value: GET
        value_to_metric: true
        descriptors:
          - key: subject_id
            rate_limit: {requests_per_unit: 60, unit: minute}
      - key: http_method
        value: POST
        value_to_metric: true
        descriptors:
          - key: subject_id
            rate_limit: {requests_per_unit: 30, unit: minute}
",
    );
    let rl = limit(
        &config,
        "test-domain",
        &[
            ("route", "api"),
            ("http_method", "GET"),
            ("subject_id", "user123"),
        ],
    )
    .unwrap();
    assert_eq!(
        (rl.limit.requests_per_unit, rl.full_key.as_str()),
        (60, "test-domain.route_api.http_method_GET.subject_id")
    );
    let rl = limit(
        &config,
        "test-domain",
        &[
            ("route", "api"),
            ("http_method", "POST"),
            ("subject_id", "user456"),
        ],
    )
    .unwrap();
    assert_eq!(
        (rl.limit.requests_per_unit, rl.full_key.as_str()),
        (30, "test-domain.route_api.http_method_POST.subject_id")
    );

    let config = load_yaml(
        "
domain: domain
descriptors:
  - key: user
    value_to_metric: true
    descriptors:
      - key: action
        value: read*
        value_to_metric: true
        descriptors:
          - key: resource
            rate_limit: {requests_per_unit: 100, unit: minute}
",
    );
    assert_eq!(
        full_key(
            &config,
            "domain",
            &[
                ("user", "alice"),
                ("action", "readfile"),
                ("resource", "documents")
            ]
        ),
        "domain.user_alice.action_readfile.resource"
    );
    assert_eq!(
        full_key(
            &config,
            "domain",
            &[
                ("user", "bob"),
                ("action", "readdata"),
                ("resource", "database")
            ]
        ),
        "domain.user_bob.action_readdata.resource"
    );
}

#[test]
fn detailed_metric_takes_precedence_over_value_to_metric() {
    let config = load_yaml(
        "
domain: domain
descriptors:
  - key: route
    value_to_metric: true
    descriptors:
      - key: http_method
        value_to_metric: true
        descriptors:
          - key: subject_id
            detailed_metric: true
            rate_limit: {requests_per_unit: 60, unit: minute}
",
    );
    assert_eq!(
        full_key(
            &config,
            "domain",
            &[
                ("route", "draw"),
                ("http_method", "GET"),
                ("subject_id", "123")
            ]
        ),
        "domain.route_draw.http_method_GET.subject_id_123"
    );
}

fn shared(
    config: &RateLimitConfig,
    entries: &[(&str, &str)],
) -> (String, Option<Vec<String>>, u32) {
    let rl = limit(config, "test-domain", entries).expect("a limit should match");
    (
        rl.full_key,
        rl.share_threshold_key_pattern,
        rl.limit.requests_per_unit,
    )
}

fn patterns(p: &[&str]) -> Option<Vec<String>> {
    Some(p.iter().map(|s| s.to_string()).collect())
}

#[test]
fn share_threshold_makes_wildcard_matches_share_one_key() {
    let config = load(&["share_threshold.yaml"]);

    for value in ["files/a.pdf", "files/b.csv", "files/c.txt"] {
        assert_eq!(
            shared(&config, &[("files", value)]),
            (
                "test-domain.files_files/*".into(),
                patterns(&["files/*"]),
                100
            )
        );
    }
    // share_threshold takes priority over value_to_metric and detailed_metric.
    assert_eq!(
        shared(&config, &[("route", "api/v1"), ("method", "GET")]),
        (
            "test-domain.route_api/*.method".into(),
            patterns(&["api/*", ""]),
            60
        )
    );
    let rl = limit(
        &config,
        "test-domain",
        &[("service", "svc/user"), ("endpoint", "get")],
    )
    .unwrap();
    assert!(rl.detailed_metric);
    assert_eq!(rl.full_key, "test-domain.service_svc/*.endpoint_get");
    assert_eq!(rl.share_threshold_key_pattern, patterns(&["svc/*", ""]));
    // Nested wildcards.
    assert_eq!(
        shared(
            &config,
            &[("nested", "parent"), ("files", "nested/file1.txt")]
        ),
        (
            "test-domain.nested_parent.files_nested/*".into(),
            patterns(&["", "nested/*"]),
            200
        )
    );
    assert_eq!(
        shared(
            &config,
            &[("files", "top/file1.txt"), ("files", "nested/file1.txt")]
        ),
        (
            "test-domain.files_top/*.files_nested/*".into(),
            patterns(&["top/*", "nested/*"]),
            300
        )
    );
    assert_eq!(
        shared(
            &config,
            &[("path", "path/foo"), ("file", "file/doc1"), ("type", "pdf")]
        ),
        (
            "test-domain.path_path/*.file_file/*.type".into(),
            patterns(&["path/*", "", ""]),
            120
        )
    );
    assert_eq!(
        shared(
            &config,
            &[
                ("user", "user/alice"),
                ("resource", "res/file1"),
                ("action", "read")
            ]
        ),
        (
            "test-domain.user_user/*.resource_res/*.action".into(),
            patterns(&["user/*", "res/*", ""]),
            150
        )
    );
}

#[test]
fn explicit_value_takes_precedence_over_shared_wildcard() {
    let config = load(&["share_threshold.yaml"]);

    assert_eq!(
        shared(&config, &[("file", "test1")]),
        ("test-domain.file_test1".into(), None, 50)
    );
    for value in ["test2", "test23", "test123"] {
        assert_eq!(
            shared(&config, &[("file", value)]),
            ("test-domain.file_test*".into(), patterns(&["test*"]), 100)
        );
    }
}

#[test]
fn share_threshold_with_middle_and_multiple_wildcards() {
    let config = load(&["share_threshold.yaml"]);

    for value in ["/api/v1", "/api-beta-v1", "/apiXYZv1"] {
        assert_eq!(
            shared(&config, &[("midpath", value)]),
            (
                "test-domain.midpath_/api*v1".into(),
                patterns(&["/api*v1"]),
                100
            )
        );
    }
    assert!(limit(&config, "test-domain", &[("midpath", "/api/v2")]).is_none());
    for value in ["svcAepBrpc", "svcXXXepYYYrpc", "svceprpc"] {
        assert_eq!(
            shared(&config, &[("multiroute", value)]),
            (
                "test-domain.multiroute_svc*ep*rpc".into(),
                patterns(&["svc*ep*rpc"]),
                50
            )
        );
    }
    assert!(limit(&config, "test-domain", &[("multiroute", "svcABCrpc")]).is_none());
    for value in ["/api/v2", "/api-beta-v2"] {
        assert_eq!(
            shared(&config, &[("midroute", value)]).0,
            "test-domain.midroute_/api*v2"
        );
    }
    for (tenant, resource) in [("t1prod", "resAv2"), ("t99prod", "resBBBv2")] {
        assert_eq!(
            shared(&config, &[("tenant", tenant), ("resource", resource)]),
            (
                "test-domain.tenant_t*prod.resource_res*v2".into(),
                patterns(&["t*prod", "res*v2"]),
                60
            )
        );
    }
}

#[test]
fn share_threshold_requires_a_wildcard() {
    let file = ConfigFile {
        name: "inline.yaml".to_string(),
        content: "
domain: d
descriptors:
  - key: k
    value: v
    share_threshold: true
    rate_limit: {requests_per_unit: 1, unit: second}
"
        .to_string(),
    };
    assert_eq!(
        config::load(&[file], &LoadOptions::default())
            .unwrap_err()
            .to_string(),
        "inline.yaml: share_threshold can only be used with wildcard values (containing '*'), but found key 'k_v'"
    );
}

#[test]
fn metadata_is_attached_to_the_limit() {
    use prost_types::value::Kind;
    let config = load(&["metadata.yaml"]);

    let rl = limit(
        &config,
        "test-domain",
        &[("key1", "value1"), ("subkey1", "something")],
    )
    .unwrap();
    let metadata = rl.metadata.expect("metadata");
    assert_eq!(
        metadata.fields["name"].kind,
        Some(Kind::StringValue("service_1".into()))
    );

    let rl = limit(&config, "test-domain", &[("key2", "something")]).unwrap();
    let metadata = rl.metadata.expect("metadata");
    let Some(Kind::StructValue(service)) = &metadata.fields["service_with_quota"].kind else {
        panic!("nested metadata should be a struct");
    };
    assert_eq!(
        service.fields["name"].kind,
        Some(Kind::StringValue("service_2".into()))
    );

    let rl = limit(
        &config,
        "test-domain",
        &[("key1", "value1"), ("subkey1", "subvalue1")],
    )
    .unwrap();
    assert_eq!(rl.metadata, None);
}

fn load_sanitized(content: &str, sanitize: bool) -> RateLimitConfig {
    let file = inline_file(content);
    let options = LoadOptions {
        sanitize_descriptor_metric_dots: sanitize,
        ..Default::default()
    };
    config::load(&[file], &options).unwrap()
}

#[test]
fn sanitize_descriptor_metric_dots_only_affects_stats_keys() {
    let ip = "
domain: domain
descriptors:
  - key: source_ip
    value: 10.0.0.1
    rate_limit: {requests_per_unit: 5, unit: minute}
";
    let key = |config: &RateLimitConfig, k: &str, v: &str| full_key(config, "domain", &[(k, v)]);

    assert_eq!(
        key(&load_sanitized(ip, false), "source_ip", "10.0.0.1"),
        "domain.source_ip_10.0.0.1"
    );
    let config = load_sanitized(ip, true);
    assert_eq!(
        key(&config, "source_ip", "10.0.0.1"),
        "domain.source_ip_10_0_0_1"
    );
    assert_eq!(
        limit(&config, "domain", &[("source_ip", "10.0.0.1")])
            .unwrap()
            .limit
            .requests_per_unit,
        5
    );

    let host = "
domain: domain
descriptors:
  - key: host
    value: foo.bar
    rate_limit: {requests_per_unit: 5, unit: minute}
";
    assert_eq!(
        key(&load_sanitized(host, true), "host", "foo.bar"),
        "domain.host_foo_bar"
    );

    let dotted_key = "
domain: domain
descriptors:
  - key: a.b
    value: v
    rate_limit: {requests_per_unit: 5, unit: minute}
";
    assert_eq!(
        key(&load_sanitized(dotted_key, true), "a.b", "v"),
        "domain.a_b_v"
    );

    for flag in ["detailed_metric", "value_to_metric"] {
        let config = load_sanitized(
            &format!(
                "
domain: domain
descriptors:
  - key: source_ip
    {flag}: true
    rate_limit: {{requests_per_unit: 5, unit: minute}}
"
            ),
            true,
        );
        assert_eq!(
            key(&config, "source_ip", "10.0.0.1"),
            "domain.source_ip_10_0_0_1",
            "{flag}"
        );
    }
}

#[test]
fn metadata_is_dropped_when_upstream_rebuilds_the_limit() {
    // envoyproxy/ratelimit rebuilds the limit with `NewRateLimit` (which has no metadata)
    // for a detailed limit matched at a leaf, and when value_to_metric changes the stats key.
    let config = load_yaml(
        "
domain: d
descriptors:
  - key: detailed
    detailed_metric: true
    metadata: {a: 1}
    rate_limit: {requests_per_unit: 1, unit: second}
  - key: plain
    value_to_metric: true
    metadata: {a: 1}
    rate_limit: {requests_per_unit: 1, unit: second}
  - key: exact
    value: v
    value_to_metric: true
    metadata: {a: 1}
    rate_limit: {requests_per_unit: 1, unit: second}
",
    );
    assert_eq!(
        limit(&config, "d", &[("detailed", "x")]).unwrap().metadata,
        None
    );
    assert_eq!(
        limit(&config, "d", &[("plain", "x")]).unwrap().metadata,
        None
    );
    assert!(
        limit(&config, "d", &[("exact", "v")])
            .unwrap()
            .metadata
            .is_some()
    );
}

fn try_load_yaml(content: &str) -> Result<RateLimitConfig, String> {
    config::load(&[inline_file(content)], &LoadOptions::default()).map_err(|e| e.to_string())
}

#[test]
fn yaml_is_read_with_go_yaml_v2_rules() {
    // YAML 1.1 booleans, and any scalar's source text read into a string field.
    let config = try_load_yaml(
        "
domain: d
descriptors:
  - key: port
    value: 017
    shadow_mode: yes
    rate_limit: {requests_per_unit: 0x10, unit: SECOND}
",
    )
    .unwrap();
    let rl = limit(&config, "d", &[("port", "017")]).unwrap();
    assert!(rl.shadow_mode);
    assert_eq!(
        (rl.limit.requests_per_unit, rl.limit.unit()),
        (16, Unit::Second)
    );

    // yaml.v2 type errors, collected and reported together.
    assert_eq!(
        try_load_yaml(
            "
domain: d
descriptors:
  - key: k
    shadow_mode: \"true\"
    rate_limit: {requests_per_unit: -1, unit: second}
"
        )
        .unwrap_err(),
        "inline.yaml: error loading config file: yaml: unmarshal errors:\n  \
         line 5: cannot unmarshal !!str `true` into bool\n  \
         line 6: cannot unmarshal !!int `-1` into uint32"
    );

    // Floats are rejected by key validation before decoding; `0080` is not octal, so it is one.
    assert_eq!(
        try_load_yaml("domain: d\ndescriptors:\n  - key: k\n    value: 0080\n").unwrap_err(),
        "inline.yaml: error checking config"
    );
    assert_eq!(
        try_load_yaml("domain: d\ndescriptors:\n  - key: k\n    rate_limit: {requests_per_unit: 1.5, unit: second}\n")
            .unwrap_err(),
        "inline.yaml: error checking config"
    );
    // A boolean key is not a string key.
    assert_eq!(
        try_load_yaml("domain: d\non: 1\n").unwrap_err(),
        "inline.yaml: config error, key is not of type string: true"
    );
}

#[test]
fn quota_mode_and_replaces_are_carried() {
    let config = load_yaml(
        "
domain: d
descriptors:
  - key: model
    quota_mode: true
    rate_limit:
      name: specific
      unit: minute
      requests_per_unit: 5
      replaces: [{name: generic}, {name: other}]
",
    );
    let rl = limit(&config, "d", &[("model", "x")]).unwrap();
    assert!(rl.quota_mode);
    assert_eq!(rl.replaces, ["generic", "other"]);
}

#[test]
fn every_file_is_validated_before_any_is_loaded() {
    // Upstream parses and key-validates all files first, then loads domains in order.
    let empty_domain = fixture("empty_domain.yaml");
    let unknown_key = fixture("misspelled_key.yaml");
    assert_eq!(
        config::load(&[empty_domain, unknown_key], &LoadOptions::default())
            .unwrap_err()
            .to_string(),
        "misspelled_key.yaml: config error, unknown key 'ratelimit'"
    );
}

#[test]
fn yaml_merge_keys_are_expanded() {
    let config = try_load_yaml(
        "
domain: d
descriptors:
  - &base
    key: a
    rate_limit: {unit: second, requests_per_unit: 5}
  - <<: *base
    key: b
  - <<: [{key: ignored, value: first}, {key: c, value: second}]
    rate_limit: {unit: minute, requests_per_unit: 7}
",
    )
    .unwrap();
    assert_eq!(
        limit(&config, "d", &[("b", "x")])
            .unwrap()
            .limit
            .requests_per_unit,
        5
    );
    // In a merged sequence, earlier maps take precedence over later ones.
    assert_eq!(
        limit(&config, "d", &[("ignored", "first")])
            .unwrap()
            .limit
            .requests_per_unit,
        7
    );
}

#[test]
fn only_null_tilde_and_empty_leave_a_rate_limit_unset() {
    // yaml.v2 allocates the struct pointer for any other null spelling, then zeroes it.
    for spelling in ["NULL", "Null"] {
        assert_eq!(
            try_load_yaml(&format!(
                "domain: d\ndescriptors:\n  - key: k\n    rate_limit: {spelling}\n"
            ))
            .unwrap_err(),
            "inline.yaml: invalid rate limit unit ''",
            "{spelling}"
        );
    }
    for spelling in ["null", "~", ""] {
        let config = try_load_yaml(&format!(
            "domain: d\ndescriptors:\n  - key: k\n    rate_limit: {spelling}\n"
        ))
        .unwrap();
        assert!(limit(&config, "d", &[("k", "v")]).is_none(), "{spelling}");
    }
}

#[test]
fn duplicate_struct_and_metadata_keys_are_merged_like_yaml_v2() {
    let config = try_load_yaml(
        "
domain: d
descriptors:
  - key: k
    rate_limit: {unit: second, requests_per_unit: 5}
    rate_limit: {name: foo}
    metadata: {a: 1}
    metadata: {b: 2}
",
    )
    .unwrap();
    let rl = limit(&config, "d", &[("k", "v")]).unwrap();
    assert_eq!((rl.limit.requests_per_unit, rl.name.as_str()), (5, "foo"));
    let mut keys: Vec<_> = rl.metadata.unwrap().fields.into_keys().collect();
    keys.sort();
    assert_eq!(keys, ["a", "b"]);
}

#[test]
fn collection_keys_are_invalid_map_keys() {
    assert_eq!(
        try_load_yaml("domain: d\n? [a, 1]\n: b\n").unwrap_err(),
        "inline.yaml: error loading config file: yaml: invalid map key: []interface {}{\"a\", 1}"
    );
}

#[test]
fn timestamps_binary_and_null_keys_follow_yaml_v2() {
    assert_eq!(
        try_load_yaml("domain: d\ndescriptors:\n  - key: k\n    shadow_mode: 2001-12-14\n")
            .unwrap_err(),
        "inline.yaml: error loading config file: yaml: unmarshal errors:\n  \
         line 4: cannot unmarshal !!timestamp `2001-12-14` into bool"
    );

    let config = try_load_yaml(
        "domain: !!binary ZA==\ndescriptors:\n  - key: k\n    metadata: {~: x}\n    rate_limit: {unit: second, requests_per_unit: 1}\n",
    )
    .unwrap();
    let rl = limit(&config, "d", &[("k", "v")]).expect("domain decoded from base64");
    assert_eq!(
        rl.metadata.unwrap().fields.into_keys().collect::<Vec<_>>(),
        [""]
    );
}
