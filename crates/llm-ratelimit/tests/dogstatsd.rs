//! The DogStatsD sink, against envoyproxy/ratelimit@f4e7669's `src/godogstats` and datadog-go
//! v5.5.0, where the oracle cannot reach: what it is compared on is in `tests/differential.rs`.

use llm_ratelimit::stats::origin::{self, Sources};
use llm_ratelimit::stats::{
    DogStatsdSettings, DogStatsdSink, Mogrifier, Sink, SinkSettings, select_sink,
};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::net::UdpSocket;

async fn server() -> (UdpSocket, u16) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    (socket, port)
}

/// Every datagram until none comes for 300 ms.
async fn datagrams(socket: &UdpSocket) -> Vec<String> {
    let mut received = Vec::new();
    let mut buffer = vec![0; 65536];
    while let Ok(Ok(n)) =
        tokio::time::timeout(Duration::from_millis(300), socket.recv(&mut buffer)).await
    {
        received.push(String::from_utf8(buffer[..n].to_vec()).unwrap());
    }
    received
}

fn tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Values go out as `int64`, global tags lose their newlines, the container ID comes last.
#[tokio::test]
async fn lines_carry_int64_values_tags_and_the_container_id() {
    let (socket, port) = server().await;
    let settings = DogStatsdSettings {
        env: "pr\nod".into(),
        ..Default::default()
    };
    let sink =
        DogStatsdSink::with_container_id("127.0.0.1", port, settings, Some("abc".into())).unwrap();
    sink.flush_counter("big.__k=v", u64::MAX);
    sink.flush_counter("small", 1);
    assert_eq!(
        datagrams(&socket).await.concat(),
        "big:-1|c|#env:prod,k:v|c:abc\nsmall:1|c|#env:prod|c:abc\n"
    );
}

/// Lines are packed whole into datagrams of at most 1432 bytes; a longer line is dropped.
#[tokio::test]
async fn datagrams_hold_whole_lines_and_longer_lines_are_dropped() {
    let (socket, port) = server().await;
    let sink =
        DogStatsdSink::with_container_id("127.0.0.1", port, DogStatsdSettings::default(), None)
            .unwrap();
    let mut expected = String::new();
    for i in 0..100 {
        let name = format!("ratelimit.service.rate_limit.domain.key_{i:03}.total_hits");
        sink.flush_counter(&name, i);
        expected += &format!("{name}:{i}|c\n");
        if i == 50 {
            sink.flush_counter(&"x".repeat(1430), 1);
        }
    }
    let received = datagrams(&socket).await;
    for datagram in &received {
        assert!(
            datagram.len() <= 1432 && datagram.ends_with('\n'),
            "{datagram:?}"
        );
    }
    assert!(received.len() > 1);
    assert_eq!(received.concat(), expected);
}

fn mogrifier(pattern: &str, name: &str, tag_pairs: &[(&str, &str)]) -> Mogrifier {
    Mogrifier::new("M", pattern, name, tags(tag_pairs)).unwrap()
}

async fn mogrified(mogrifiers: Vec<Mogrifier>, name: &str) -> String {
    let (socket, port) = server().await;
    let settings = DogStatsdSettings {
        mogrifiers,
        ..Default::default()
    };
    let sink = DogStatsdSink::with_container_id("127.0.0.1", port, settings, None).unwrap();
    sink.flush_counter(name, 1);
    datagrams(&socket).await.concat()
}

/// `$<digits>` is replaced by that group, or kept when there is none; the mogrifier's tags come
/// sorted by key (upstream's order is random).
#[tokio::test]
async fn mogrifiers_substitute_groups_like_upstream() {
    let m = mogrifier(
        r"^a\.(b)\.(c)$",
        "x.$2.$1a.$12.$.$$0",
        &[("z", "$1"), ("y", "$0")],
    );
    assert_eq!(
        mogrified(vec![m], "a.b.c").await,
        "x.c.ba.$12.$.$a.b.c:1|c|#y:a.b.c,z:b\n"
    );
}

/// As in Go, `\d`, `\w`, `\s` and `\b` only know ASCII.
#[tokio::test]
async fn mogrifier_perl_classes_are_ascii() {
    for (pattern, name) in [
        (r"^\d+$", "٣"),
        (r"^\w+$", "é"),
        (r"^a\sb$", "a\u{a0}b"),
        (r"^[\d]+$", "٣"),
    ] {
        let m = mogrifier(pattern, "matched", &[]);
        assert_eq!(
            mogrified(vec![m], name).await,
            format!("{name}:1|c\n"),
            "{pattern}"
        );
    }
    // `é` is not a word character in ASCII, so `\b` sits between it and `a`.
    for (pattern, name) in [
        (r"^\D\W\S$", "é é"),
        (r"^[^\d]\b", "x"),
        (r"^\w\s\d$", "a 1"),
        (r"^é\b", "éa"),
    ] {
        let m = mogrifier(pattern, "matched", &[]);
        assert_eq!(mogrified(vec![m], name).await, "matched:1|c\n", "{pattern}");
    }
}

#[test]
fn invalid_mogrifiers_are_refused_like_upstream() {
    let error = |pattern: &str, name: &str, tag_pairs: &[(&str, &str)]| {
        Mogrifier::new("M", pattern, name, tags(tag_pairs)).unwrap_err()
    };
    assert_eq!(error("", "n", &[]), "no PATTERN specified for mogrifier M");
    assert!(error("(", "n", &[]).starts_with("failed to compile pattern for M: (: "));
    assert_eq!(error("a", "", &[]), "no NAME specified for mogrifier M");
    assert_eq!(
        error("a", "n", &[("t", "")]),
        "no value specified for tag t for mogrifier M"
    );
}

#[tokio::test]
async fn the_dogstatsd_sink_is_selected_like_upstream() {
    let settings = SinkSettings {
        use_statsd: false,
        use_dog_statsd: true,
        statsd_host: "127.0.0.1".into(),
        ..Default::default()
    };
    assert!(matches!(select_sink(&settings), Ok(Sink::Dogstatsd(_))));
    let unresolvable = SinkSettings {
        statsd_host: "no-such-host.invalid".into(),
        ..settings
    };
    let error = select_sink(&unresolvable).err().unwrap();
    assert!(
        error.starts_with("Failed to create dogstatsd sink: "),
        "{error}"
    );
}

/// Files laid out like `/proc/self` and `/sys/fs/cgroup`.
fn sources(cgroup: &str, mountinfo: &str) -> (tempdir::Dir, Sources) {
    let dir = tempdir::Dir::new();
    std::fs::write(dir.path().join("cgroup"), cgroup).unwrap();
    std::fs::write(dir.path().join("mountinfo"), mountinfo).unwrap();
    let sources = Sources {
        cgroup: dir.path().join("cgroup"),
        mountinfo: dir.path().join("mountinfo"),
        cgroup_mount: dir.path().join("fs"),
    };
    (dir, sources)
}

const DOCKER_ID: &str = "3726184226f5d3147c25fdeab5b60097e378e8a720503a5e19ecfdf29f869860";

#[test]
fn in_the_host_cgroup_namespace_the_id_comes_from_the_cgroup_file() {
    for (cgroup, expected) in [
        (format!("12:memory:/docker/{DOCKER_ID}\n"), Some(DOCKER_ID.to_string())),
        (
            format!("0::/system.slice/docker-{DOCKER_ID}.scope\n"),
            Some(DOCKER_ID.to_string()),
        ),
        (
            "1:name=systemd:/ecs/34dc0b5e626f2c5c4c5170e34b10e765-1234567890\n".to_string(),
            Some("34dc0b5e626f2c5c4c5170e34b10e765-1234567890".to_string()),
        ),
        (
            "1:cpu:/garden/6f265890-5165-7fab-6b52-18d1\n9:x:/kubepods/pod3d274242-8ee0-11e9-a8a6-1e68d864ef1a\n".to_string(),
            Some("3d274242-8ee0-11e9-a8a6-1e68d864ef1a".to_string()),
        ),
        ("0::/user.slice/user-1000.slice\n".to_string(), None),
    ] {
        let (_dir, sources) = sources(&cgroup, "");
        assert_eq!(origin::detect(true, &sources), expected, "{cgroup}");
    }
}

#[cfg(unix)]
#[test]
fn elsewhere_a_container_in_mountinfo_means_the_cgroup_inode() {
    use std::os::unix::fs::MetadataExt;
    let mount = |kind: &str| {
        format!("2 1 0:1 /var/lib/docker/containers/{DOCKER_ID}/hostname /etc/hostname rw - ext4 /dev/sda1 rw\n")
            .replace("containers", kind)
    };
    // cgroup v1: the memory controller's node; cgroup v2: the unified node.
    for (cgroup, node) in [
        ("4:memory:/kubepods/x\n0::/other\n", "memory/kubepods/x"),
        ("0::/kubepods/y\n", "kubepods/y"),
    ] {
        let (dir, sources) = sources(cgroup, &mount("containers"));
        // Both nodes exist: the memory controller's wins.
        std::fs::create_dir_all(dir.path().join("fs/other")).unwrap();
        let path = dir.path().join("fs").join(node);
        std::fs::create_dir_all(&path).unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        assert_eq!(
            origin::detect(false, &sources),
            Some(format!("in-{inode}")),
            "{cgroup}"
        );
    }
    // A containerd sandbox, no container, or no cgroup node: nothing.
    for (cgroup, mountinfo, create) in [
        ("0::/kubepods/y\n", mount("sandboxes"), true),
        (
            "0::/kubepods/y\n",
            "2 1 0:1 / / rw - ext4 /dev/sda1 rw\n".to_string(),
            true,
        ),
        ("0::/kubepods/y\n", mount("containers"), false),
    ] {
        let (dir, sources) = sources(cgroup, &mountinfo);
        if create {
            std::fs::create_dir_all(dir.path().join("fs/kubepods/y")).unwrap();
        }
        assert_eq!(origin::detect(false, &sources), None, "{mountinfo}");
    }
}

/// A throwaway directory.
mod tempdir {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub struct Dir(PathBuf);

    impl Dir {
        pub fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "ratelimit-dogstatsd-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        pub fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// The address is `host:port` as Go reads it: a name resolves to IPv4 first (the agent usually
/// listens on 127.0.0.1), a bare IPv6 host is refused, a bracketed one is accepted.
#[tokio::test]
async fn the_address_is_resolved_like_go() {
    let (socket, port) = server().await;
    let settings = DogStatsdSettings::default;
    let sink = DogStatsdSink::with_container_id("localhost", port, settings(), None).unwrap();
    sink.flush_counter("a", 1);
    assert_eq!(datagrams(&socket).await.concat(), "a:1|c\n");

    let error = DogStatsdSink::with_container_id("::1", port, settings(), None)
        .err()
        .unwrap();
    assert!(error.contains("too many colons"), "{error}");
    assert!(DogStatsdSink::with_container_id("[::1]", port, settings(), None).is_ok());
}

/// A timer is `Timing(name, time.Duration(milliseconds) * time.Millisecond)`: whole
/// milliseconds with six decimals, then the tags and the container ID.
#[tokio::test]
async fn timers_are_whole_milliseconds() {
    let (socket, port) = server().await;
    let settings = DogStatsdSettings {
        env: "prod".into(),
        ..Default::default()
    };
    let sink =
        DogStatsdSink::with_container_id("127.0.0.1", port, settings, Some("abc".into())).unwrap();
    sink.flush_timer("t.__k=v", 2.7);
    sink.flush_timer("u", 0.0);
    assert_eq!(
        datagrams(&socket).await.concat(),
        "t:2.000000|ms|#env:prod,k:v|c:abc\nu:0.000000|ms|#env:prod|c:abc\n"
    );
}

/// A gauge is `Gauge(name, float64(value))`: the float's shortest form, never an exponent.
#[tokio::test]
async fn gauges_are_floats() {
    let (socket, port) = server().await;
    let sink = DogStatsdSink::new("127.0.0.1", port, DogStatsdSettings::default()).unwrap();
    sink.flush_gauge("small.__k=v", 3);
    sink.flush_gauge("big", u64::MAX);
    assert_eq!(
        datagrams(&socket).await.concat(),
        "small:3|g|#k:v\nbig:18446744073709552000|g\n"
    );
}
