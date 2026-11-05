//! The image (docs/design.md D33) against upstream's: run as any user on a read-only file
//! system with no capabilities, with upstream's defaults (ports, runtime root), serving on
//! IPv4 and, when the container has it, IPv6; the same answers and log lines.
//!
//! Needs `RATELIMIT_IMAGE` (the image under test) and a container CLI
//! (`RATELIMIT_CONTAINER_CLI`, `docker` by default); skipped without `RATELIMIT_IMAGE`.

use llm_ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor, rate_limit_descriptor::Entry,
};
use llm_ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, rate_limit_service_client::RateLimitServiceClient,
};
use std::process::Command;

/// envoyproxy/ratelimit's image at the pinned commit.
const UPSTREAM_IMAGE: &str = "docker.io/envoyproxy/ratelimit:f4e76690@sha256:83b1664a194ff9fc1ff30b0868573115a2d01a1b6e2a3f8961724f9a79bf1cd0";
const REDIS_IMAGE: &str = "docker.io/library/redis:8.2.2-alpine@sha256:59b6e694653476de2c992937ebe1c64182af4728e54bb49e9b7a6c26614d8933";
const CURL_IMAGE: &str = "docker.io/curlimages/curl:8.16.0@sha256:463eaf6072688fe96ac64fa623fe73e1dbe25d8ad6c34404a669ad3ce1f104b6";

const CONFIG: &str = "
domain: llm
descriptors:
  - key: user
    rate_limit:
      unit: day
      requests_per_unit: 2
";

fn cli() -> String {
    std::env::var("RATELIMIT_CONTAINER_CLI").unwrap_or_else(|_| "docker".into())
}

/// The CLI's output for `args`, which must succeed.
fn container(args: &[&str]) -> String {
    let output = Command::new(cli())
        .args(args)
        .output()
        .expect("run the container CLI");
    assert!(
        output.status.success(),
        "{} {args:?}: {}",
        cli(),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// What a test creates, removed when dropped (on failure too).
enum Created {
    Container(String),
    Network(String),
    Directory(std::path::PathBuf),
}

impl Drop for Created {
    fn drop(&mut self) {
        let remove = |args: &[&str]| {
            let _ = Command::new(cli()).args(args).output();
        };
        match self {
            Created::Container(id) => remove(&["rm", "-f", id]),
            Created::Network(name) => remove(&["network", "rm", name]),
            Created::Directory(path) => {
                let _ = std::fs::remove_dir_all(path);
            }
        }
    }
}

/// The host port a container's `port` is published on.
fn host_port(id: &str, port: u16) -> u16 {
    let published = container(&["port", id, &format!("{port}/tcp")]);
    let first = published.lines().next().unwrap_or_default();
    first.rsplit(':').next().unwrap().parse().unwrap()
}

/// The HTTP status of `url` seen from inside the container `id` (`000` when unreachable).
fn status_inside(id: &str, url: &str) -> String {
    let output = Command::new(cli())
        .args([
            "run",
            "--rm",
            "--network",
            &format!("container:{id}"),
            CURL_IMAGE,
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--max-time",
            "3",
            if url.contains(":8081") {
                "--http2-prior-knowledge"
            } else {
                "--http1.1"
            },
            url,
        ])
        .output()
        .expect("run curl");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn request(user: &str) -> RateLimitRequest {
    RateLimitRequest {
        domain: "llm".into(),
        descriptors: vec![RateLimitDescriptor {
            entries: vec![Entry {
                key: "user".into(),
                value: user.into(),
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// What one image shows, with IPv6 enabled in its container or not.
async fn observe(image: &str, network: &str, redis: &str, ipv6: bool) -> Vec<String> {
    let config =
        std::env::temp_dir().join(format!("ratelimit-image-{}-{}", std::process::id(), ipv6));
    std::fs::create_dir_all(&config).unwrap();
    let _config = Created::Directory(config.clone());
    std::fs::write(config.join("llm.yaml"), CONFIG).unwrap();
    // Readable by the container's user.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(
        config.join("llm.yaml"),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    let id = container(&[
        "run",
        "-d",
        "--network",
        network,
        "--read-only",
        "--user",
        "65534:65534",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--sysctl",
        &format!(
            "net.ipv6.conf.all.disable_ipv6={}",
            if ipv6 { 0 } else { 1 }
        ),
        "-e",
        "REDIS_SOCKET_TYPE=tcp",
        "-e",
        &format!("REDIS_URL={redis}:6379"),
        "-e",
        "USE_STATSD=false",
        "-e",
        "RUNTIME_SUBDIRECTORY=ratelimit",
        "-e",
        "EXPIRATION_JITTER_MAX_SECONDS=0",
        "-v",
        &format!(
            "{}:/srv/runtime_data/current/ratelimit/config:ro",
            config.display()
        ),
        "-p",
        "127.0.0.1::8080",
        "-p",
        "127.0.0.1::8081",
        "-p",
        "127.0.0.1::6070",
        image,
    ]);
    let _container = Created::Container(id.clone());
    let (http, grpc, debug) = (
        host_port(&id, 8080),
        host_port(&id, 8081),
        host_port(&id, 6070),
    );
    let healthcheck = format!("http://127.0.0.1:{http}/healthcheck");
    let mut healthy = false;
    for _ in 0..200 {
        if http_status(&healthcheck).await == Some(200) {
            healthy = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(healthy, "{image}: {}", container(&["logs", &id]));
    // No shell: only the binary runs.
    let shell = Command::new(cli())
        .args([
            "run",
            "--rm",
            "--entrypoint",
            "/bin/sh",
            image,
            "-c",
            "true",
        ])
        .output()
        .unwrap();
    let mut observed = vec![format!("shell runs: {}", shell.status.success())];
    let mut client = RateLimitServiceClient::connect(format!("http://127.0.0.1:{grpc}"))
        .await
        .unwrap();
    for _ in 0..3 {
        let mut response = client
            .should_rate_limit(request("a"))
            .await
            .unwrap()
            .into_inner();
        for status in &mut response.statuses {
            status.duration_until_reset = None;
        }
        observed.push(format!("{response:?}"));
    }
    observed.push(http_body(&format!("http://127.0.0.1:{debug}/rlconfig")).await);
    for url in [
        "http://127.0.0.1:8080/healthcheck",
        "http://[::1]:8080/healthcheck",
        "http://127.0.0.1:6070/rlconfig",
        "http://[::1]:6070/rlconfig",
        // gRPC's port answers HTTP/2 there too.
        "http://127.0.0.1:8081/",
        "http://[::1]:8081/",
    ] {
        observed.push(format!("{url}: {}", status_inside(&id, url)));
    }
    container(&["stop", "-t", "30", &id]);
    observed.push(format!(
        "exit code {}",
        container(&["inspect", "-f", "{{.State.ExitCode}}", &id])
    ));
    // The logrus lines (not gostats' own, flushed on a timer), without their times.
    let output = Command::new(cli()).args(["logs", &id]).output().unwrap();
    let time = regex::Regex::new(r#"time="[^"]+""#).unwrap();
    let mut lines: Vec<String> = String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter(|l| !l.contains("gostats.loggingsink"))
        .map(|l| time.replace(l, "time=TIME").into_owned())
        .collect();
    lines.sort();
    observed.extend(lines);
    observed
}

/// A minimal HTTP/1.1 GET: the status, if the server answers.
async fn http_status(url: &str) -> Option<u16> {
    let response = http_get(url).await?;
    response.split(' ').nth(1)?.parse().ok()
}

/// The body of a GET.
async fn http_body(url: &str) -> String {
    let response = http_get(url).await.unwrap_or_default();
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default()
}

async fn http_get(url: &str) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rest = url.strip_prefix("http://")?;
    let (address, path) = rest.split_once('/')?;
    let mut stream = tokio::net::TcpStream::connect(address).await.ok()?;
    let request = format!("GET /{path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.ok()?;
    Some(String::from_utf8_lossy(&response).into_owned())
}

/// Our image serves as upstream's with only the image changed: as UID 65534 on a read-only
/// file system without capabilities, the config read from upstream's default runtime root,
/// on upstream's default ports, on IPv4 and IPv6 (IPv4 only when the container has no
/// IPv6); the same answers, debug page and log lines, and a clean stop.
#[tokio::test]
async fn the_image_serves_like_upstreams() {
    let Ok(image) = std::env::var("RATELIMIT_IMAGE") else {
        return;
    };
    let suffix = std::process::id();
    let network = format!("ratelimit-image-{suffix}");
    container(&["network", "create", &network]);
    let _network = Created::Network(network.clone());
    let redis = format!("ratelimit-redis-{suffix}");
    let redis_id = container(&[
        "run",
        "-d",
        "--network",
        &network,
        "--name",
        &redis,
        REDIS_IMAGE,
        "redis-server",
        "--save",
        "",
    ]);
    let _redis = Created::Container(redis_id);
    let flush = || container(&["exec", &redis, "redis-cli", "FLUSHALL"]);
    for ipv6 in [false, true] {
        flush();
        let upstream = observe(UPSTREAM_IMAGE, &network, &redis, ipv6).await;
        flush();
        // What upstream shows: the limit applies, IPv6 answers when enabled, a clean stop.
        let calls: Vec<_> = upstream
            .iter()
            .filter(|o| o.starts_with("RateLimitResponse"))
            .collect();
        assert!(calls[2].contains("OverLimit"), "{upstream:?}");
        let ipv6_status = if ipv6 { "200" } else { "000" };
        assert!(
            upstream.contains(&format!("http://[::1]:8080/healthcheck: {ipv6_status}")),
            "{upstream:?}"
        );
        assert!(
            upstream.contains(&"exit code 0".to_string()),
            "{upstream:?}"
        );
        let ours = observe(&image, &network, &redis, ipv6).await;
        assert_eq!(ours, upstream, "IPv6 enabled: {ipv6}");
    }
}

/// The new name is the entrypoint, and upstream's manifests' `/bin/ratelimit` still runs the
/// same binary: each refuses a bad setting the same way.
#[test]
fn both_binary_paths_run() {
    let Ok(image) = std::env::var("RATELIMIT_IMAGE") else {
        return;
    };
    assert_eq!(
        container(&[
            "image",
            "inspect",
            "--format",
            "{{json .Config.Entrypoint}}",
            &image
        ]),
        r#"["/bin/llm-ratelimit"]"#
    );
    for path in ["/bin/llm-ratelimit", "/bin/ratelimit"] {
        let output = Command::new(cli())
            .args([
                "run",
                "--rm",
                "--env",
                "BACKEND_TYPE=bogus",
                "--entrypoint",
                path,
                &image,
            ])
            .output()
            .expect("run the container CLI");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("Invalid setting for BackendType: bogus"),
            "{path}: {stderr}"
        );
    }
}

/// LICENSE, NOTICE and the crates' licenses (cargo-about) in `/usr/share/doc/llm-ratelimit`
/// (docs/design.md D34).
#[test]
fn the_image_ships_its_licenses() {
    let Ok(image) = std::env::var("RATELIMIT_IMAGE") else {
        return;
    };
    let id = container(&["create", &image]);
    let _created = Created::Container(id.clone());
    let dir = std::env::temp_dir().join(format!("ratelimit-licenses-{}", std::process::id()));
    let _dir = Created::Directory(dir.clone());
    container(&[
        "cp",
        &format!("{id}:/usr/share/doc/llm-ratelimit"),
        &dir.to_string_lossy(),
    ]);
    let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for name in ["LICENSE", "NOTICE"] {
        let expected = std::fs::read_to_string(root.join(name)).unwrap();
        assert_eq!(read(name), expected, "{name}");
    }
    let crates = read("THIRD-PARTY-LICENSES.txt");
    for used in ["- tokio ", "- tonic ", "- fred ", "- rustls "] {
        assert!(crates.contains(used), "{used} missing");
    }
}
