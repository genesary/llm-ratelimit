//! `rate_limits:`, several named limits per descriptor (docs/design.md D7), with `fixed`
//! windows of any duration and origin (D8, ADR 0002): how they load and what is refused.
//! Counting them is in `differential.rs` (which runs a Redis), next to upstream's limits.

use ratelimit::config::window::{
    Anchored, AnchoredOp, AnchoredResult, AnchoredWindow, ConcurrencyWindow, FixedWindow, OpenOn,
    Origin, SlotOp, SlotResult, Window,
};
use ratelimit::config::{self, ConfigFile, LimitLookup, LoadOptions, RateLimit};
use ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor, rate_limit_descriptor::Entry,
};
use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::rate_limit::Unit;

fn load(yaml: &str) -> Result<config::RateLimitConfig, String> {
    let file = ConfigFile {
        name: "config.yaml".into(),
        content: yaml.into(),
    };
    config::load(&[file], &LoadOptions::default()).map_err(|e| e.to_string())
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

/// The limits a descriptor's lookup finds in a `rate_limits:` list.
fn list(yaml: &str, entries: &[(&str, &str)]) -> Vec<RateLimit> {
    match load(yaml).unwrap().get_limit("d", &descriptor(entries)) {
        LimitLookup::List(limits) => limits,
        other => panic!("not a list: {other:?}"),
    }
}

/// A descriptor's list with one limit of this `window`.
fn with_window(window: &str) -> String {
    format!(
        "domain: d\ndescriptors:\n  - key: user\n    rate_limits:\n      - name: w\n        requests_per_unit: 1\n        window: {window}\n"
    )
}

#[test]
fn a_list_gives_each_limit_its_name_window_and_stats_key() {
    let limits = list(
        "
domain: d
descriptors:
  - key: user
    shadow_mode: true
    quota_mode: true
    metadata: {tier: free}
    rate_limits:
      - name: five_hour
        requests_per_unit: 500
        window: {type: fixed, duration: 5h}
      - name: weekly
        requests_per_unit: 5000
        window: {type: fixed, duration: 7d, offset: 4d}
",
        &[("user", "alice")],
    );
    let shown: Vec<_> = limits
        .iter()
        .map(|l| {
            (
                l.full_key.as_str(),
                l.limit.name.as_str(),
                l.limit.requests_per_unit,
                l.limit.unit(),
                l.limit.unit_multiplier,
                l.window,
            )
        })
        .collect();
    assert_eq!(
        shown,
        [
            (
                "d.user~five_hour",
                "five_hour",
                500,
                Unit::Hour,
                Some(5),
                Some(Window::Fixed(FixedWindow {
                    duration: 5 * 3600,
                    origin: Origin::Epoch
                }))
            ),
            (
                "d.user~weekly",
                "weekly",
                5000,
                Unit::Week,
                None,
                Some(Window::Fixed(FixedWindow {
                    duration: 7 * 86_400,
                    origin: Origin::Offset(4 * 86_400)
                }))
            ),
        ]
    );
    // The descriptor's settings, for every limit of the list (D7).
    for limit in &limits {
        assert!(limit.shadow_mode && limit.quota_mode, "{limit:?}");
        assert!(limit.metadata.is_some(), "{limit:?}");
    }
}

/// D13: the largest unit whose length divides the duration, and how many of them.
#[test]
fn windows_are_reported_in_the_largest_unit_dividing_them() {
    let reported = |duration: &str| {
        let limits = list(
            &with_window(&format!("{{type: fixed, duration: {duration}}}")),
            &[("user", "a")],
        );
        (limits[0].limit.unit(), limits[0].limit.unit_multiplier)
    };
    assert_eq!(reported("1s"), (Unit::Second, None));
    assert_eq!(reported("90s"), (Unit::Second, Some(90)));
    assert_eq!(reported("1h30m"), (Unit::Minute, Some(90)));
    assert_eq!(reported("60m"), (Unit::Hour, None));
    assert_eq!(reported("48h"), (Unit::Day, Some(2)));
    assert_eq!(reported("2w"), (Unit::Week, Some(2)));
    assert_eq!(reported("30d"), (Unit::Month, None));
    assert_eq!(reported("365d"), (Unit::Year, None));
    assert_eq!(reported("730d"), (Unit::Year, Some(2)));
}

/// ADR 0002: `offset: hashed`, with the default rounding (the largest of 1h, 1m, 1s that
/// divides the duration) or a given one; `offset: 0` is the epoch's.
#[test]
fn offsets_are_read_as_adr_0002_writes_them() {
    let origin = |window: &str| match list(&with_window(window), &[("user", "a")])[0].window {
        Some(Window::Fixed(fixed)) => fixed.origin,
        other => panic!("not a fixed window: {other:?}"),
    };
    assert_eq!(
        origin("{type: fixed, duration: 7d, offset: hashed}"),
        Origin::Hashed { rounding: 3600 }
    );
    assert_eq!(
        origin("{type: fixed, duration: 90s, offset: hashed}"),
        Origin::Hashed { rounding: 1 }
    );
    assert_eq!(
        origin("{type: fixed, duration: 1d, offset: hashed, offset_rounding: 6h}"),
        Origin::Hashed { rounding: 6 * 3600 }
    );
    assert_eq!(
        origin("{type: fixed, duration: 1d, offset: 0}"),
        Origin::Epoch
    );
    assert_eq!(
        origin("{type: fixed, duration: 1d, offset: 0s}"),
        Origin::Epoch
    );
    assert_eq!(
        origin("{type: fixed, duration: 1d, offset: 23h59m59s}"),
        Origin::Offset(86_399)
    );
}

/// The hashed phase: FNV-1a 64 over `key=value` joined by NUL, modulo the steps of the
/// rounding (expected values from an independent implementation).
#[test]
fn hashed_phases_follow_the_descriptor() {
    let window = FixedWindow {
        duration: 7 * 86_400,
        origin: Origin::Hashed { rounding: 3600 },
    };
    assert_eq!(window.phase([("user", "alice")]), 428_400);
    assert_eq!(window.phase([("user", "bob")]), 504_000);
    assert_eq!(window.phase([("org", "acme"), ("user", "alice")]), 475_200);
    // At 1_000_000_000, alice's week started at 999_558_000 and ends in 162_800 s.
    let phase = window.phase([("user", "alice")]);
    assert_eq!(window.start(1_000_000_000, phase), 999_558_000);
    assert_eq!(window.reset(1_000_000_000, phase), 162_800);
}

/// Configs a list makes invalid, each with its message.
#[test]
fn invalid_lists_are_refused() {
    let descriptor = |body: &str| format!("domain: d\ndescriptors:\n  - key: user\n{body}");
    let entry = |fields: &str| descriptor(&format!("    rate_limits:\n      - {{{fields}}}\n"));
    let fixed = "window: {type: fixed, duration: 1h}";
    let cases: Vec<(String, &str)> = vec![
        (
            descriptor(&format!(
                "    rate_limit: {{unit: hour, requests_per_unit: 1}}\n    rate_limits:\n      - {{name: a, {fixed}}}\n"
            )),
            "config.yaml: d.user: should not specify both rate_limit and rate_limits",
        ),
        (
            descriptor("    rate_limits: []\n"),
            "config.yaml: d.user: rate_limits should not be empty",
        ),
        (
            entry(&format!("requests_per_unit: 1, {fixed}")),
            "config.yaml: d.user: invalid rate_limits name '': 1 to 64 letters, digits, '_' or '-', starting with a letter or digit",
        ),
        (
            entry(&format!("name: a.b, {fixed}")),
            "config.yaml: d.user: invalid rate_limits name 'a.b': 1 to 64 letters, digits, '_' or '-', starting with a letter or digit",
        ),
        (
            entry(&format!("name: _a, {fixed}")),
            "config.yaml: d.user: invalid rate_limits name '_a': 1 to 64 letters, digits, '_' or '-', starting with a letter or digit",
        ),
        (
            entry(&format!("name: {}, {fixed}", "a".repeat(65))),
            "config.yaml: d.user: invalid rate_limits name 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa': 1 to 64 letters, digits, '_' or '-', starting with a letter or digit",
        ),
        (
            descriptor(&format!(
                "    rate_limits:\n      - {{name: a, {fixed}}}\n      - {{name: a, {fixed}}}\n"
            )),
            "config.yaml: d.user~a: duplicate rate_limits name",
        ),
        (
            entry(&format!("name: a, unit: hour, {fixed}")),
            "config.yaml: d.user~a: rate_limits entries take a window, not a unit",
        ),
        (
            entry(&format!("name: a, unlimited: true, {fixed}")),
            "config.yaml: d.user~a: rate_limits entries cannot be unlimited: use rate_limit: {unlimited: true}",
        ),
        (
            descriptor(&format!(
                "    rate_limits:\n      - {{name: a, {fixed}, replaces: [{{name: b}}]}}\n      - {{name: b, {fixed}}}\n"
            )),
            "config.yaml: d.user~a: replaces should not contain a name of the same rate_limits",
        ),
        (
            entry(&format!("name: a, {fixed}, replaces: [{{name: a}}]")),
            "config.yaml: d.user~a: replaces should not contain a name of the same rate_limits",
        ),
        (
            entry(&format!("name: a, {fixed}, replaces: [{{}}]")),
            "config.yaml: d.user~a: should not have an empty replaces entry",
        ),
        (
            entry("name: a"),
            "config.yaml: d.user~a: window is required",
        ),
        (
            entry("name: a, window: {duration: 1h}"),
            "config.yaml: d.user~a: window type is required",
        ),
        (
            entry("name: a, window: {type: sliding, duration: 1h}"),
            "config.yaml: d.user~a: window type 'sliding' is not supported yet",
        ),
        (
            entry("name: a, window: {type: rolling, duration: 1h}"),
            "config.yaml: d.user~a: unknown window type 'rolling'",
        ),
        (
            entry("name: a, window: {type: fixed}"),
            "config.yaml: d.user~a: window duration is required",
        ),
        (
            entry("name: a, window: {type: fixed, duration: 60}"),
            "config.yaml: d.user~a: invalid window duration '60'",
        ),
        (
            entry("name: a, window: {type: fixed, duration: 1.5h}"),
            "config.yaml: d.user~a: invalid window duration '1.5h'",
        ),
        (
            entry("name: a, window: {type: fixed, duration: 500ms}"),
            "config.yaml: d.user~a: invalid window duration '500ms'",
        ),
        (
            entry("name: a, window: {type: fixed, duration: 0s}"),
            "config.yaml: d.user~a: invalid window duration '0s'",
        ),
        (
            entry("name: a, window: {type: fixed, duration: 30m1h}"),
            "config.yaml: d.user~a: invalid window duration '30m1h'",
        ),
        (
            entry("name: a, window: {type: fixed, duration: 1h1h}"),
            "config.yaml: d.user~a: invalid window duration '1h1h'",
        ),
        (
            entry("name: a, window: {type: fixed, duration: 1d, offset: 1d}"),
            "config.yaml: d.user~a: offset '1d' must be shorter than the window duration '1d'",
        ),
        (
            entry("name: a, window: {type: fixed, duration: 1d, offset: soon}"),
            "config.yaml: d.user~a: invalid offset 'soon'",
        ),
        (
            entry("name: a, window: {type: fixed, duration: 1d, offset_rounding: 1h}"),
            "config.yaml: d.user~a: offset_rounding needs offset: hashed",
        ),
        (
            entry("name: a, window: {type: fixed, duration: 1d, offset: 1h, offset_rounding: 1h}"),
            "config.yaml: d.user~a: offset_rounding needs offset: hashed",
        ),
        (
            entry(
                "name: a, window: {type: fixed, duration: 1d, offset: hashed, offset_rounding: 7h}",
            ),
            "config.yaml: d.user~a: offset_rounding '7h' must divide the window duration '1d'",
        ),
        (
            entry(
                "name: a, window: {type: fixed, duration: 1d, offset: hashed, offset_rounding: 2d}",
            ),
            "config.yaml: d.user~a: offset_rounding '2d' must divide the window duration '1d'",
        ),
        (
            descriptor(&format!(
                "    overflow: strict\n    rate_limits:\n      - {{name: a, {fixed}}}\n"
            )),
            "config.yaml: d.user: overflow: strict is not supported yet",
        ),
        (
            descriptor("    overflow: lax\n    rate_limit: {unit: hour, requests_per_unit: 1}\n"),
            "config.yaml: d.user: invalid overflow 'lax': envoy or strict",
        ),
    ];
    for (yaml, expected) in cases {
        assert_eq!(load(&yaml).err().as_deref(), Some(expected), "{yaml}");
    }
}

/// The list's keys are only valid in their place: elsewhere they are unknown, as upstream
/// says of any key it does not know.
#[test]
fn list_keys_elsewhere_are_unknown_keys() {
    let cases = [
        ("domain: d\nrate_limits: []\n", "rate_limits"),
        (
            "domain: d\ndescriptors:\n  - key: a\n    rate_limit: {unit: hour, requests_per_unit: 1, window: {}}\n",
            "window",
        ),
        (
            "domain: d\ndescriptors:\n  - key: a\n    duration: 1h\n",
            "duration",
        ),
        (
            "domain: d\ndescriptors:\n  - key: a\n    rate_limits:\n      - {name: a, shadow_mode: true, window: {type: fixed, duration: 1h}}\n",
            "shadow_mode",
        ),
        (
            "domain: d\ndescriptors:\n  - key: a\n    rate_limits:\n      - {name: a, window: {type: fixed, duration: 1h, unit: hour}}\n",
            "unit",
        ),
        (
            "domain: d\ndescriptors:\n  - key: a\n    rate_limit:\n      overflow: envoy\n",
            "overflow",
        ),
        // Under a `descriptors` that is not a descriptor's.
        (
            "domain: d\ndescriptors:\n  - key: a\n    rate_limit:\n      descriptors:\n        - {rate_limits: []}\n",
            "rate_limits",
        ),
    ];
    for (yaml, key) in cases {
        assert_eq!(
            load(yaml).err(),
            Some(format!("config.yaml: config error, unknown key '{key}'")),
            "{yaml}"
        );
    }
}

/// `overflow: envoy` is the default, on either form of limit.
#[test]
fn overflow_envoy_is_accepted() {
    load("domain: d\ndescriptors:\n  - key: a\n    overflow: envoy\n    rate_limit: {unit: hour, requests_per_unit: 1}\n")
        .unwrap();
    list(
        "domain: d\ndescriptors:\n  - key: user\n    overflow: envoy\n    rate_limits:\n      - {name: a, window: {type: fixed, duration: 1h}}\n",
        &[("user", "x")],
    );
}

/// `/rlconfig`: a line per limit of a list, with its stats key and its window.
#[test]
fn lists_are_dumped_limit_by_limit() {
    let config = load(
        "
domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: a, requests_per_unit: 1, window: {type: fixed, duration: 5h}}
      - {name: b, requests_per_unit: 2, window: {type: fixed, duration: 7d, offset: 4d}}
      - {name: c, requests_per_unit: 3, window: {type: fixed, duration: 1d, offset: hashed}}
",
    )
    .unwrap();
    assert_eq!(
        config.dump(),
        "d.user~a: window=fixed 18000s requests_per_unit=1, shadow_mode: false, quota_mode: false
d.user~b: window=fixed 604800s offset=345600s requests_per_unit=2, shadow_mode: false, quota_mode: false
d.user~c: window=fixed 86400s offset=hashed/3600s requests_per_unit=3, shadow_mode: false, quota_mode: false
"
    );
}

/// `detailed_metric`, `value_to_metric` and `share_threshold` name the stats as for a
/// `rate_limit:`, the limit's name last.
#[test]
fn list_stats_keys_follow_the_descriptor_settings() {
    let key = |body: &str, entries: &[(&str, &str)]| {
        list(&format!("domain: d\ndescriptors:\n{body}"), entries)[0]
            .full_key
            .clone()
    };
    let limits = "    rate_limits:\n      - {name: n, window: {type: fixed, duration: 1h}}\n";
    assert_eq!(
        key(
            &format!("  - key: user\n    detailed_metric: true\n{limits}"),
            &[("user", "alice")]
        ),
        "d.user_alice~n"
    );
    assert_eq!(
        key(
            &format!("  - key: user\n    value_to_metric: true\n{limits}"),
            &[("user", "alice")]
        ),
        "d.user_alice~n"
    );
    assert_eq!(
        key(
            &format!(
                "  - key: file\n    value: \"a*\"\n    share_threshold: true\n    detailed_metric: true\n{limits}"
            ),
            &[("file", "abc")]
        ),
        "d.file_a*~n"
    );
}

/// `anchored` windows (D9): their duration, `start_rounding` and `open_on`.
#[test]
fn anchored_windows_load_with_their_rounding_and_opening() {
    let window = |w: &str| list(&with_window(w), &[("user", "a")])[0].window;
    assert_eq!(
        window("{type: anchored, duration: 5h}"),
        Some(Window::Anchored(AnchoredWindow {
            duration: 18_000,
            rounding: None,
            open_on: OpenOn::Check
        }))
    );
    assert_eq!(
        window("{type: anchored, duration: 5h, start_rounding: 1h, open_on: first_hit}"),
        Some(Window::Anchored(AnchoredWindow {
            duration: 18_000,
            rounding: Some(3_600),
            open_on: OpenOn::FirstHit
        }))
    );
    let limits = list(
        &with_window("{type: anchored, duration: 5h, start_rounding: 1h}"),
        &[("user", "a")],
    );
    assert_eq!(
        (limits[0].limit.unit(), limits[0].limit.unit_multiplier),
        (Unit::Hour, Some(5))
    );
}

#[test]
fn invalid_anchored_windows_are_refused() {
    let refused = |w: &str| load(&with_window(w)).err();
    let error = |message: &str| Some(format!("config.yaml: d.user~w: {message}"));
    assert_eq!(
        refused("{type: anchored, duration: 5h, offset: 1h}"),
        error("offset is for fixed windows")
    );
    assert_eq!(
        refused("{type: anchored, duration: 5h, offset_rounding: 1h}"),
        error("offset_rounding is for fixed windows")
    );
    assert_eq!(
        refused("{type: anchored, duration: 5h, start_rounding: 6h}"),
        error("start_rounding '6h' must not be longer than the window duration '5h'")
    );
    assert_eq!(
        refused("{type: anchored, duration: 5h, start_rounding: soon}"),
        error("invalid start_rounding 'soon'")
    );
    assert_eq!(
        refused("{type: anchored, duration: 5h, open_on: request}"),
        error("invalid open_on 'request': check or first_hit")
    );
    assert_eq!(
        refused("{type: fixed, duration: 5h, start_rounding: 1h}"),
        error("start_rounding is for anchored windows")
    );
    assert_eq!(
        refused("{type: fixed, duration: 5h, open_on: check}"),
        error("open_on is for anchored windows")
    );
    // D24: Memcached counts fixed windows only.
    let file = ConfigFile {
        name: "config.yaml".into(),
        content: with_window("{type: anchored, duration: 5h}"),
    };
    let memcached = LoadOptions {
        memcached: true,
        ..Default::default()
    };
    assert_eq!(
        config::load(&[file], &memcached)
            .err()
            .map(|e| e.to_string()),
        error("only fixed windows can be counted on Memcached")
    );
}

/// A `concurrency` window takes a lease, required, and a `retry_after` of at least a second; it
/// is alone in its list, not in quota mode, and refused on Memcached (D35, ADR 0004).
#[test]
fn concurrency_windows_load_as_adr_0004_says() {
    let limits = list(
        &with_window("{type: concurrency, lease: 10m, retry_after: 2s}"),
        &[("user", "alice")],
    );
    assert_eq!(
        limits[0].window,
        Some(Window::Concurrency(ConcurrencyWindow {
            lease: 600,
            retry_after: 2
        }))
    );
    assert_eq!(limits[0].window.unwrap().shape(), "cc");
    let limits = list(
        &with_window("{type: concurrency, lease: 30s}"),
        &[("user", "alice")],
    );
    assert_eq!(
        limits[0].window,
        Some(Window::Concurrency(ConcurrencyWindow {
            lease: 30,
            retry_after: 1
        }))
    );
    let refused = |w: &str| load(&with_window(w)).err();
    let error = |message: &str| Some(format!("config.yaml: d.user~w: {message}"));
    for (window, message) in [
        ("{type: concurrency}", "lease is required"),
        ("{type: concurrency, lease: 0s}", "invalid lease '0s'"),
        ("{type: concurrency, lease: soon}", "invalid lease 'soon'"),
        (
            "{type: concurrency, lease: 1m, retry_after: 0s}",
            "invalid retry_after '0s'",
        ),
        (
            "{type: concurrency, lease: 1m, duration: 1h}",
            "duration is not for concurrency windows",
        ),
        (
            "{type: concurrency, lease: 1m, offset: 1h}",
            "offset is not for concurrency windows",
        ),
        (
            "{type: fixed, duration: 1h, lease: 1m}",
            "lease is for concurrency windows",
        ),
        (
            "{type: anchored, duration: 1h, retry_after: 1s}",
            "retry_after is for concurrency windows",
        ),
    ] {
        assert_eq!(refused(window), error(message), "{window}");
    }
    let alone = "domain: d
descriptors:
  - key: user
    rate_limits:
      - {name: slots, requests_per_unit: 1, window: {type: concurrency, lease: 1m}}
      - {name: w, requests_per_unit: 1, window: {type: fixed, duration: 1h}}
";
    assert_eq!(
        load(alone).err(),
        Some(
            "config.yaml: d.user~slots: a concurrency limit must be alone in its rate_limits: hits release its slots"
                .to_string()
        )
    );
    let quota = with_window("{type: concurrency, lease: 1m}").replacen(
        "rate_limits:",
        "quota_mode: true\n    rate_limits:",
        1,
    );
    assert_eq!(
        load(&quota).err(),
        error("a concurrency limit cannot be in quota_mode")
    );
    let file = ConfigFile {
        name: "config.yaml".into(),
        content: with_window("{type: concurrency, lease: 1m}"),
    };
    let memcached = LoadOptions {
        memcached: true,
        ..Default::default()
    };
    assert_eq!(
        config::load(&[file], &memcached)
            .err()
            .map(|e| e.to_string()),
        error("only fixed windows can be counted on Memcached")
    );
}

/// A slot ends at its lease, whatever else happens; a release takes the newest and stops at
/// nothing left; a forced acquisition goes past the maximum.
#[test]
fn slots_step_as_d35_says() {
    let window = ConcurrencyWindow {
        lease: 100,
        retry_after: 1,
    };
    let mut slots = Vec::new();
    let acquire = SlotOp::Acquire {
        max: 2,
        force: false,
    };
    let result = |acquired, released, expired, in_flight| SlotResult {
        acquired,
        released,
        expired,
        in_flight,
    };
    assert_eq!(
        window.step(&mut slots, acquire, 1000),
        result(true, 0, 0, 1)
    );
    assert_eq!(
        window.step(&mut slots, acquire, 1050),
        result(true, 0, 0, 2)
    );
    assert_eq!(
        window.step(&mut slots, acquire, 1060),
        result(false, 0, 0, 2)
    );
    assert_eq!(
        window.step(
            &mut slots,
            SlotOp::Acquire {
                max: 2,
                force: true
            },
            1060
        ),
        result(true, 0, 0, 3)
    );
    // The newest go first: the one acquired at 1000 stays.
    assert_eq!(
        window.step(&mut slots, SlotOp::Release(2), 1070),
        result(false, 2, 0, 1)
    );
    assert_eq!(slots, [1000]);
    // Its lease ends when its age reaches it, before the operation counts.
    assert_eq!(
        window.step(&mut slots, SlotOp::Release(1), 1099),
        result(false, 1, 0, 0)
    );
    window.step(&mut slots, acquire, 1100);
    assert_eq!(
        window.step(&mut slots, SlotOp::Release(5), 1200),
        result(false, 0, 1, 0)
    );
    // A slot of its own, by when it was acquired.
    window.step(&mut slots, acquire, 1300);
    assert_eq!(
        window.step(&mut slots, SlotOp::Remove(1300), 1301),
        result(false, 1, 0, 0)
    );
    assert_eq!(
        window.step(&mut slots, SlotOp::Remove(1300), 1301),
        result(false, 0, 0, 0)
    );
}

/// D9's opening and counting, in `envoy` mode: what [`AnchoredWindow::step`] does to a state.
#[test]
fn anchored_windows_open_count_and_close_as_d9_says() {
    let five_hours = AnchoredWindow {
        duration: 18_000,
        rounding: Some(3_600),
        open_on: OpenOn::Check,
    };
    let op = |now, hits, refund| AnchoredOp { now, hits, refund };
    let window = |anchor, count| Some(Anchored { anchor, count });
    let opened = |anchor, count| AnchoredResult {
        state: window(anchor, count),
        window: window(anchor, count),
        opened: true,
    };
    let kept = |w| AnchoredResult {
        state: w,
        window: w,
        opened: false,
    };
    // No window open after the request; what is stored stays.
    let closed = |stored| AnchoredResult {
        state: stored,
        window: None,
        opened: false,
    };
    // A 0-hit check opens it, at the hour (2001-09-09 01:00 UTC).
    let now = 1_000_000_000;
    assert_eq!(
        five_hours.step(None, op(now, 0, false)),
        opened(999_997_200, 0)
    );
    assert_eq!(five_hours.reset(window(999_997_200, 0), now), 15_200);
    // Counted while open; the anchor never moves, a clock behind it counting as no time.
    let open = window(999_997_200, 3);
    assert_eq!(
        five_hours.step(open, op(now, 4, false)),
        kept(window(999_997_200, 7))
    );
    assert_eq!(
        five_hours.step(open, op(999_997_200 + 17_999, 1, false)),
        kept(window(999_997_200, 4))
    );
    assert_eq!(
        five_hours.step(open, op(999_990_000, 1, false)),
        kept(window(999_997_200, 4))
    );
    // Its reset then is the whole window, not more (D27).
    assert_eq!(five_hours.reset(open, 999_990_000), 18_000);
    // Closed at anchor + duration: the next request opens the next one, counted in it.
    assert_eq!(
        five_hours.step(open, op(999_997_200 + 18_000, 2, false)),
        opened(1_000_015_200, 2)
    );
    // Refunds (D21) floor at 0, and never open or close a window.
    assert_eq!(
        five_hours.step(open, op(now, 5, true)),
        kept(window(999_997_200, 0))
    );
    assert_eq!(five_hours.step(None, op(now, 5, true)), closed(None));
    assert_eq!(
        five_hours.step(open, op(999_997_200 + 18_000, 1, true)),
        closed(open)
    );
    // `first_hit`: only counted hits open it.
    let first_hit = AnchoredWindow {
        open_on: OpenOn::FirstHit,
        rounding: None,
        ..five_hours
    };
    assert_eq!(first_hit.step(None, op(now, 0, false)), closed(None));
    let old = window(now - 18_000, 2);
    assert_eq!(first_hit.step(old, op(now, 0, false)), closed(old));
    assert_eq!(first_hit.reset(None, now), 18_000);
    assert_eq!(first_hit.step(None, op(now, 2, false)), opened(now, 2));
    // Hits and counts saturate at 2^53, the script's exact integers.
    let most = 1u64 << 53;
    assert_eq!(
        first_hit.step(None, op(now, u64::MAX, false)),
        opened(now, most)
    );
    assert_eq!(
        first_hit.step(window(now, most - 1), op(now, 2, false)),
        kept(window(now, most))
    );
}
