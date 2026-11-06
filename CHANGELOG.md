# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-11-06

Soft limits: a budget past its limit lowers a request's priority instead of refusing it.

### Added

- `soft` limits answer `OK` past their limit and set request headers, for a scheduler to lower the request's priority rather than refuse it. See ADR 0006.

## [0.2.0] - 2026-11-05

The project is now `llm-ratelimit`, at `ghcr.io/genesary/llm-ratelimit`. To replace envoy ratelimit, you still only change the image.

### Added

- The image is also tagged with a `v`, like the git tag: `v0.1.0` and `v0.1` next to `0.1.0` and `0.1`.
- ADR 0005 proposes limits adapting to load; nothing is built.
- A fingerprint of each loaded config, in a gauge, `/rlconfig` and the log, to alert on instances that diverge.
- The agentgateway guide shows how to weigh tokens by the cache, in "Counting like Claude".
- `token_bucket` windows refill continuously, with `burst` and `max_debt`.
- `concurrency` windows limit the requests in flight per descriptor, with a required `lease`: a check takes a slot and an amendment releases it. See ADR 0004.
- On Memcached, a warning at load names a `rate_limits:` limit whose key leaves under 64 of memcached's 250 bytes for its window's start and the request's values.

### Changed

- The project is renamed `llm-ratelimit`: the repository, `genesary/llm-ratelimit`, the image, `ghcr.io/genesary/llm-ratelimit`, and the crates. The image's entrypoint is `/bin/llm-ratelimit`. What makes it a drop-in replacement does not change: the environment variables, the APIs, the Redis and Memcached keys, the stat and metric names, and `/bin/ratelimit`, which stays as a link.
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

[Unreleased]: https://github.com/genesary/llm-ratelimit/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/genesary/llm-ratelimit/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/genesary/llm-ratelimit/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/genesary/llm-ratelimit/releases/tag/v0.1.0
