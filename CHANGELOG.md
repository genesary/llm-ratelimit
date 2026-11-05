# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- The image is also tagged with a `v`, like the git tag: `v0.1.0` and `v0.1` next to `0.1.0` and `0.1`.
- `token_bucket` windows refill continuously, with `burst` and `max_debt`.
- `concurrency` windows limit the requests in flight per descriptor, with a required `lease`: a check takes a slot and an amendment releases it. See ADR 0004.
- On Memcached, a warning at load names a `rate_limits:` limit whose key leaves under 64 of memcached's 250 bytes for its window's start and the request's values.

### Changed

- A client-sent `limit`'s `unit_multiplier` is now applied, where it was ignored: one fixed window of that many units. A multiplier of 0, or a window over 2^32 − 1 seconds, is refused.

### Fixed

- On Memcached, a window ending after January 2038 was expired on arrival and never counted: its expiration wrapped its 32 bits. It now stops at their largest value.

## [0.1.0] - 2026-10-30

The first release.

### Added

- **Upstream parity:** a drop-in replacement for envoyproxy/ratelimit at commit `f4e7669`. Every upstream behaviour has a test, most of them differential (upstream's binary and this one side by side, on Redis and Valkey); the details are in `docs/parity.md` §1–6.
  - **Configuration:** the same environment variables and YAML configuration, from files with hot reload or from xDS.
  - **API:** the gRPC and HTTP API, the `LIMIT_*` headers and dynamic metadata.
  - **Backends:** Redis (single, sentinel, cluster, TLS, AWS IAM, per-second pool) and Memcached.
  - **Limiting:** the local cache, `quota_mode` and `share_threshold`.
  - **Observability:** statsd, DogStatsD and Prometheus stats, and OTLP traces.
  - **Image:** distroless for amd64 and arm64, signed with cosign, with an SBOM and provenance.
- **`rate_limits:` lists:** several named limits per descriptor, each with its own window, stats key (`<descriptor>~<name>`) and per-unit headers.
- **`fixed` windows** of any duration (`5h`, `7d`, …), with an `offset`, constant or hashed from the descriptor (ADR 0001, ADR 0002).
- **`anchored` windows:** a window that a request opens (`open_on: check`) or its first counted hit opens (`first_hit`), and that lasts its duration. Redis only.
- **The quota lookup:** `POST /quota` reports every limit's usage and reset without counting anything (ADR 0003).
- **Prometheus labels:** the shipped Prometheus mapper puts a list limit's name in a `limit` label.

### Deliberate divergences

- **Redis down:** a decision fails at once while every connection is down, so that callers without a deadline (agentgateway) apply their failure mode; upstream holds decisions until the caller's deadline.
- **Errors instead of panics:** where upstream panics, this service returns an error, at startup or on the request, and TLS checks are stricter. The full list is in `docs/parity.md` §7.

[Unreleased]: https://github.com/genesary/ratelimit/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/genesary/ratelimit/releases/tag/v0.1.0
