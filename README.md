# ratelimit

[![CI](https://github.com/genesary/ratelimit/actions/workflows/ci.yml/badge.svg)](https://github.com/genesary/ratelimit/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/genesary/ratelimit?sort=semver)](https://github.com/genesary/ratelimit/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/genesary/ratelimit/badge)](https://scorecard.dev/viewer/?uri=github.com/genesary/ratelimit)
[![MSRV](https://img.shields.io/badge/rust-1.99-orange)](#building-and-testing)

A Rust rewrite of [envoyproxy/ratelimit](https://github.com/envoyproxy/ratelimit), compatible with it at commit [`f4e7669`](https://github.com/envoyproxy/ratelimit/commit/f4e7669), and extended with several windows per descriptor for LLM quotas.

To replace envoy ratelimit behind Envoy, Envoy Gateway or agentgateway, you only change the image. The environment variables, the YAML configuration, the gRPC and HTTP API, the metrics, the log lines and the image's layout stay the same.

## Why

- **Proven compatible.** Every upstream behaviour in [`docs/parity.md`](docs/parity.md) §1–6 has a test, most of them differential: upstream's binary and this one run side by side, and their answers, Redis and Memcached keys, stats and logs are compared, on Redis and on Valkey.
- **Quotas the way LLM providers count them.** Several named limits per descriptor: a five-hour session that the first request opens, plus a weekly window, each with its own name in headers and metrics.
- **A quota lookup.** `POST /quota` shows every limit's usage and reset, without counting anything, so users can see "Session 40 % · resets in 2 h 05".
- **A small, hardened image.** It is distroless, runs as any UID on a read-only file system, and is signed with cosign, with an SBOM and provenance.

### Why not

- It is young (v0.1.0) and has a single maintainer. The upstream project has years of production use.
- `sliding` and `token_bucket` windows and `overflow: strict` are designed but not built yet, and are refused at load.
- Where this service differs on purpose (where upstream panics, an error at startup or on the request; a stricter TLS check; …), the difference is listed in [`docs/parity.md`](docs/parity.md) §7.

## Quick start

A configuration with two limits per user: a five-hour session opened by the user's first request, and a weekly window whose start is spread across users:

```yaml
# runtime/ratelimit/config/llm.yaml
domain: llm
descriptors:
  - key: user
    rate_limits:
      - name: session
        requests_per_unit: 4000000
        window: {type: anchored, duration: 5h}
      - name: weekly
        requests_per_unit: 40000000
        window: {type: fixed, duration: 7d, offset: hashed}
```

```sh
docker network create rl
docker run -d --name redis --network rl redis:8
docker run -d --name ratelimit --network rl -p 8080:8080 -p 8081:8081 \
  -v "$PWD/runtime:/srv/runtime_data/current:ro" \
  -e RUNTIME_SUBDIRECTORY=ratelimit -e RUNTIME_WATCH_ROOT=false -e USE_STATSD=false \
  -e REDIS_SOCKET_TYPE=tcp -e REDIS_URL=redis:6379 \
  ghcr.io/genesary/ratelimit:0.1.0
```

gRPC (`envoy.service.ratelimit.v3.RateLimitService`) listens on 8081, and HTTP on 8080. A decision counting 1200 tokens:

```sh
curl -s localhost:8080/json -d '{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"alice"}]}],"hits_addend":1200}'
```
```json
{"overallCode":"OK","statuses":[{"code":"OK","currentLimit":{"name":"session","requestsPerUnit":4000000,"unit":"HOUR","unitMultiplier":5},"limitRemaining":3998800,"durationUntilReset":"18000s"}]}
```

And the quota lookup, which counts nothing:

```sh
curl -s localhost:8080/quota -d '{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"alice"}]}]}'
```
```json
{"descriptors":[{"limits":[
  {"limit":4000000,"name":"session","open":true,"quota_mode":false,"remaining":3998800,"resets_at":"2026-10-30T00:01:29Z","resets_in_s":18000,"shadow_mode":false,"used":1200,"used_pct":0.0,"window":{"duration_s":18000,"type":"anchored"}},
  {"limit":40000000,"name":"weekly","quota_mode":false,"remaining":39998800,"resets_at":"2026-11-02T23:00:00Z","resets_in_s":359911,"shadow_mode":false,"used":1200,"used_pct":0.0,"window":{"duration_s":604800,"type":"fixed"}}]}],
 "now":"2026-10-29T19:01:29Z"}
```

## Migrating from envoyproxy/ratelimit

Swap the image for `ghcr.io/genesary/ratelimit` and keep your configuration and environment variables. The binary is at `/bin/ratelimit`, as upstream's is. Upstream configurations load and count exactly as before. The extensions are opt-in, through `rate_limits:` lists.

## Features

Every upstream feature is supported:

- **Backends:** Redis (single, sentinel, cluster, TLS, AWS IAM, per-second pool) and Memcached (with SRV discovery).
- **Configuration:** files with hot reload, or xDS.
- **Limiting:** the local cache, `quota_mode` and `share_threshold`.
- **API:** the `LIMIT_*` headers, dynamic metadata, `/json`, the debug port and `grpc.health.v1`.
- **Stats sinks:** statsd, DogStatsD and Prometheus (statsd_exporter's mapper).
- **Tracing:** OTLP traces.
- **Network:** gRPC over TLS with certificate reload, and gRPC on a Unix socket.

The extensions are listed in [`docs/parity.md`](docs/parity.md) §8:

- several named limits per descriptor (`rate_limits:`);
- `fixed` windows of any duration, with an `offset` (constant, or hashed from the descriptor);
- `anchored` windows, opened by a request and lasting their duration;
- `token_bucket` windows, refilling continuously;
- `concurrency` windows, limiting the requests in flight;
- the quota lookup, `POST /quota` ([ADR 0003](docs/adr/0003-quota-lookup.md)).

## Documentation

The full documentation is in [`docs/`](docs/README.md):

- **Reference:** [configuration](docs/configuration.md), [environment variables](docs/environment.md), [API](docs/api.md), [quota lookup](docs/quota-lookup.md), [backends](docs/backends.md), [observability](docs/observability.md), [deployment](docs/deployment.md).
- **Examples:** [Envoy](docs/examples/envoy.md), [agentgateway LLM quotas](docs/examples/agentgateway.md).
- **Design:** [design decisions](docs/design.md), [parity with upstream](docs/parity.md), [ADRs](docs/adr), [glossary](GLOSSARY.md), [changelog](CHANGELOG.md).

## Releases and verification

Images are published as `ghcr.io/genesary/ratelimit:<version>` for linux/amd64 and linux/arm64, with an SBOM and provenance, signed with cosign (keyless):

```sh
cosign verify ghcr.io/genesary/ratelimit:0.1.0 \
  --certificate-identity https://github.com/genesary/ratelimit/.github/workflows/release.yml@refs/tags/v0.1.0 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Versions follow [Semantic Versioning](https://semver.org/). Before 1.0, a minor version may change behaviour; each change is in the changelog.

## Building and testing

The supported Rust version is 1.99 (`rust-version` in `Cargo.toml`), which is what CI builds with.

```sh
cargo build --release -p ratelimit       # target/release/ratelimit
docker build -t ratelimit .               # the image: /bin/ratelimit on distroless
```

The differential tests build upstream's service from [`tools/oracle`](tools/oracle) and need `go`, `redis-server` and `memcached` on the `PATH`. Without them, those tests are skipped:

```sh
RATELIMIT_REQUIRE_DIFFERENTIAL=1 cargo test --workspace
RATELIMIT_REDIS_SERVER=/path/to/valkey-server cargo test --workspace   # against Valkey
```

## Contributing and support

- **Contributing:** see [`CONTRIBUTING.md`](CONTRIBUTING.md); everyone taking part follows the [code of conduct](CODE_OF_CONDUCT.md).
- **Questions and bugs:** [`SUPPORT.md`](SUPPORT.md) says where each goes.
- **Security:** report vulnerabilities privately, as [`SECURITY.md`](SECURITY.md) explains, never in a public issue.

## License

[Apache-2.0](LICENSE). [`NOTICE`](NOTICE) credits envoyproxy/ratelimit, the Envoy API and the other projects this one ports. The image ships both files, with the licenses of the crates the binary is built from, in `/usr/share/doc/ratelimit/`.
