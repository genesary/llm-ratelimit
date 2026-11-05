# Design — `ratelimit` (Rust)

Reference document for the design decisions, from the design grilling of 2026-10-16. A decision marked *(amended)* was revised after it was first agreed. Format details are fixed in the ADRs:

- [ADR 0001 — Storage keys of `rate_limits:` entries](adr/0001-storage-keys-for-rate-limits-entries.md)
- [ADR 0002 — Origin of fixed windows (`offset`)](adr/0002-origin-of-fixed-windows.md)

Compatibility progress is tracked in [`docs/parity.md`](parity.md).

---

## Goal

A Rust rewrite of [envoyproxy/ratelimit](https://github.com/envoyproxy/ratelimit) (reference: commit `f4e7669`), **100% compatible** and extended: free window durations, new window types, several limits per descriptor, counting modes. Replacing envoy ratelimit behind Envoy, Envoy Gateway, Istio or agentgateway only takes changing the image. The first version is released only at **full parity**.

---

## A. Scope and compatibility

**D1. Strict superset** *(amended)*. Every behaviour of the reference is reproduced exactly for any input it handles **without panicking**. Its panics (e.g. a client `limit` with `unit: UNKNOWN`, `utilities.go:38`; an invalid environment variable re-read on reload) become proper errors, listed as deliberate divergences in `docs/parity.md`. Additions never change the behaviour of an existing Envoy config.

**D2. Full parity before v1.** Every feature of the reference is included: Memcached, xDS config, `quota_mode`, `share_threshold`, local cache, `REDIS_PERSECOND`, Redis sentinel / cluster / TLS / AWS IAM, DogStatsD, OTLP, `LIMIT_*` headers, dynamic metadata, `/json`, debug port, `grpc.health.v1`, `GRPC_UDS`, certificate hot reload, etc. The exhaustive list is `docs/parity.md`.

**D3. Parity inventory** *(amended)*. `docs/parity.md`, whose environment variable section is **generated** from `src/settings/settings.go` (134 variables). It also covers YAML keys, the API, metric names, logs, traces and the image. v1 ships when every line is proven by a test or marked as a justified divergence. Logs: the format (logrus' text and JSON) and every line at info, warning, error and fatal level are exact, since alerts and log pipelines match on them; debug lines are best effort: written at the same points as upstream's, but free to differ in text and order, and to describe the extensions. A debug line never justifies keeping code shaped like upstream's.

**D4. gRPC API.** Only `envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit` (v2 was removed upstream). To reproduce precisely:

- request-level `hits_addend`: 0 means 1, **unless** a per-descriptor `hits_addend` is present, even 0 (`utilities.go:102-118`). agentgateway always sends 0 at request level: the per-descriptor path is the critical one;
- `is_negative_hits` (D21) and the client-sent `limit` (D23);
- `current_limit.name` filled in; `limit_remaining = 0` on `OVER_LIMIT`; `quota` and `raw_body` never filled in; errors returned as `codes.Unknown` with the error text;
- `overall_code` is never `UNKNOWN` (agentgateway treats any code other than `OK` as a denial).

**D5. Invalid config** *(amended)*.

- On reload: `config_load_error` incremented, previous config kept.
- At startup without a valid config: the service does not exit, but **every call returns a gRPC error** ("no rate limit configuration loaded"), not `OK`. The client decides (Envoy: `failure_mode_deny`; agentgateway: `failClosed` by default, i.e. 500s).
- With `FORCE_START_WITHOUT_INITIAL_CONFIG=false` and xDS unreachable, the servers do not start (waiting for the first event), as upstream.
- `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED` checks "at least one domain", not "valid config".
- Unknown YAML keys are rejected (upstream's recursive allow-list, `metadata` excluded), with the list extended to our additions. The xDS proto silently ignores unknown fields.

**D6. Legacy Redis keys identical byte for byte.** `CACHE_KEY_PREFIX + domain + "_" + (key "_" value "_")* + bucketStart`, epoch-aligned, with three nuances: the `share_threshold` pattern replaces the value, start of the UTC month with `USE_CALENDAR_MONTH_RATE_LIMIT`, empty value (`key__`). TTL = unit length + jitter, not the remaining time. During a migration, both services must use the same `EXPIRATION_JITTER_MAX_SECONDS`.

---

## B. New window types (YAML only)

**D7. Several limits per descriptor** *(amended)*. New `rate_limits:` list, exclusive with `rate_limit:`. In the list, `unit` is replaced by `window`, and **`name` is required** (ADR 0001). `shadow_mode`, `quota_mode` and `detailed_metric` stay at **descriptor** level and apply to the whole list. `unlimited` is not allowed in the list: the legacy form `rate_limit: {unlimited: true}` is kept.

```yaml
- key: user_id
  overflow: envoy                  # or strict (D14)
  rate_limits:
    - name: five_hour
      requests_per_unit: 500000
      window: { type: anchored, duration: 5h, start_rounding: 1h }
    - name: weekly
      requests_per_unit: 5000000
      window: { type: fixed, duration: 7d, offset: hashed }
```

**D8. `fixed`** *(amended)*. Any duration (`90s`, `5h`, `7d`), aligned on the epoch by default, with an optional origin: `offset: <duration>` (UTC) or `offset: hashed` (a phase per "account", stateless). This is what reproduces Claude's weekly limit. Details, rules and formula: ADR 0002. A list window always lasts exactly its duration: `USE_CALENDAR_MONTH_RATE_LIMIT` stays a `rate_limit: {unit: month}` option, even for a window reported as `MONTH`. IANA time zones are deferred.

**D9. `anchored`** *(amended)*. A fixed-length window whose origin is the first request. **Session** mode only: the `recurring` mode is removed (replaced by `fixed` + `offset`, ADR 0002).

- **Opening**: `open_on: check | first_hit`, default `check`.
  - `check`: the anchor is set by the first request **presented** while no window is open, whatever `hits_addend` (0 included) and the verdict, **except in `strict`, where a denied request changes no state, anchor included**. This places the anchor at the first message with agentgateway, which sends a 0-hit check and then the tokens after the response.
  - `first_hit`: the anchor is set by the first request whose hits are **actually counted** under `overflow` (in `envoy`, a denied request larger than the limit therefore opens the window; in `strict`, it does not).
- A set anchor is never moved or extended. It is replaced by the first request presented after `anchor + duration`. A late amendment (stream started before the end, finished after) is an ordinary request on a closed window: it opens the next window and is counted in it, like ccusage.
- **`start_rounding`**: optional, no rounding by default, in UTC. Rounding applies to the anchor; the window ends at `anchor + duration`.
- **Response to a check that opens**: `OK`, `limit_remaining = limit − counted hits`, `duration_until_reset = ceil(anchor + duration − now)`, so less than the full duration with rounding.
- **Windows without hits**: with `open_on: check` in `envoy`, a request denied by another limit (same list or another descriptor) still opens the window, with a full budget. So does a request that fails upstream (agentgateway then amends 0 tokens). These windows, opened with no hits by a request the response denies, are counted in `opened_without_hits`; one opened with no hits by a request that passes is not, its hits being on their way. If that is unwanted, use `open_on: first_hit`. Claude's actual behaviour in this case is not documented.
- **Script**: anchor and counter in the same key, read-decide-write in a single script; `elapsed` clamped at 0; closing decided by `now ≥ anchor + duration` in the script, not by the key's absence; TTL = duration + margin.
- **Reference test matrix** (closed window, list `[5h anchored open_on: check, 7d fixed]`; list verdict / hits / mode → opens? / counts?):

  | List verdict | Hits | Mode | Opens | Counts |
  |---|---|---|---|---|
  | weekly OK | 0 | any | yes | 0 |
  | weekly OK | > 0 | `envoy` | yes | yes |
  | 5h self-denial (hits > limit) | > 0 | `strict` | no | no |
  | weekly OVER | 0 | `envoy` | yes | 0 |
  | weekly OVER | 0 | `strict` | no | 0 |
  | weekly OVER | > 0 | `envoy` | yes | yes |
  | weekly OVER | > 0 | `strict` | no | no |
  | weekly OVER in shadow | > 0 | `strict` | yes | yes |
  | weekly replaced by `replaces` | — | — | as weekly OK | |
  | org OVER on another descriptor | 0 | any | yes (`opened_without_hits` +1) | 0 |
  | window already open | — | — | anchor never moved | per `overflow` |

**D10. `sliding` and `sliding_approx`** *(amended)*.

- `sliding` (default): N sub-buckets **without interpolation**, in **one Redis hash per (descriptor, limit)** (one script, one key, one slot; network cost independent of N). Never exceeds the limit; capacity comes back at most D/N late. `buckets: 12` by default (12 divides 60 and 3600), with a sub-bucket of at least 1 s; the docs recommend 56 or 84 for 7 days.
- `sliding_approx`: Cloudflare's two-window approximation. Its 0.003% error figure (400M requests, 270,000 sources) does not generalise to bursty LLM tokens or long durations.
- In `envoy` mode, a client that keeps sending above the limit can stay blocked; it recovers at most 2 × D after it stops. To document.

**D11. `token_bucket`** *(amended)*.

- GCRA, a single deadline (TAT) stored in microseconds (exact in a Lua double until 2255).
- Rate = `requests_per_unit / duration`; capacity = `burst` (default `requests_per_unit`).
- In `envoy`: admitted if the bucket is not empty, charged in full, debt allowed **up to `max_debt`**, whose default is **`burst`** (at most one bucket of debt, so at most `duration` of blocking). `max_debt` is given in tokens or as a duration (`10m` ⇒ `r × 600` tokens); `0` and `unlimited` are accepted. Truncated tokens are counted in the `total_capped_hits` stat.
- Warning at load when `burst / r ≥ 24 h`: `token_bucket` does not suit a daily or weekly budget; use `fixed` + `offset`.
- `limit_remaining` = whole number of tokens left, never below 0. Allowed: `duration_until_reset` = time until the bucket is full (as Anthropic does). Denied: time until there are enough tokens, at least 1 s.

**D12. `duration_until_reset`** *(amended)*. Computed per type and rounded **up** to the second, because agentgateway truncates `x-ratelimit-reset` (`remoteratelimit.rs:606-610`). Never 0 on a denial. Legacy: reproduce `sec − now % sec`, including the possible 0 at the last second of a calendar month. `fixed` with an offset: `D − (now − φ).rem_euclid(D)`.

**D13. Reporting the window duration** *(amended)*. The largest standard unit that divides the duration is returned, plus `unit_multiplier` (5h → HOUR×5). This is an **extension**: the Go reference does not know this field, nor does agentgateway (flattened proto, field ignored). Per-unit headers are computed on the **per-limit** expansion, not on the reduced statuses; for a legacy config the output is unchanged (`RateLimit-Limit-Hours`), and the `5Hours` suffix only appears when the multiplier ≠ 1.

---

## C. Counting semantics

**D14. Counting mode per descriptor** *(amended)*: `overflow: envoy | strict`.

- `envoy` (default, for `rate_limit:` and `rate_limits:`): everything is counted, including denied requests. It is the only mode that never loses agentgateway's token amendments, sent after the response without reading the result, unless the local cache answers for a `fixed` limit (D18).
- `strict`: all-or-nothing over the list. **A descriptor whose list is exhausted is not counted**, and a denied request changes no state (anchor included, D9).
- 0-hit request: `envoy` uses `>` like the reference, `strict` uses `>=`.
- To document: in `strict`, a denied agentgateway amendment is lost without a signal; with a `cost` expression, an amendment larger than the remaining tokens is denied and the bucket is not touched, so `strict` + `cost` limits almost nothing.
- `strict` × shadow: the effective verdict is `OK`, so the list is counted (the reference counts, then masks).

**D15. Several descriptors in one request** *(amended)*. Independent, as in the reference. `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` is reproduced exactly for legacy, following its actual truth table (`fixed_cache_impl.go:65-93`): a key over the limit in the local cache ⇒ no key incremented (but `INCRBY 0` + `EXPIRE` create ghost keys); otherwise only the keys **about to go over** are incremented and the others get 0. Redis only. With `REDIS_PERSECOND`, a list with a window shorter than a minute goes entirely to the per-second client (a `fixed` window reported in seconds can be a day and a second long, so the reported unit does not decide).

**D16. `replaces`** *(amended)*. Allowed on each entry of a list, with the reference's exact semantics: two passes over the whole request, not transitive; replaced or `unlimited` limits still replace others; a cycle removes both sides; an unnamed limit cannot be replaced; an `unlimited` limit that is "replaced" stays `OK` with `limit_remaining = MaxUint32`. Names need not be unique across descriptors (replacement groups); two identical names in one list, or a limit replacing a sibling in its list, are errors. Filtering happens **before** `strict`; if every limit of a descriptor is replaced, `OK` is returned without `current_limit`.

**D17. Reference features on the new windows** *(amended)*.

- **Shadow**: the verdict used for opening and counting is the **post-shadow** verdict. The descriptor's shadow and the **global** shadow (`SHADOW_MODE`) are passed to the script.
- **`quota_mode`**: reproduced as is (`ratelimit.go:233-355`): groups by `backend_name` / `model_name_override`, `OVER_LIMIT` only when every group is exhausted, `passedBackends` **deduplicated per group** and filtered to the groups not exhausted. With several limits, a descriptor is exhausted if **at least one** of its limits is.
- **Representative status of a descriptor with several limits** (total order): if there are `OVER_LIMIT` statuses, the one with the **largest `duration_until_reset`**; otherwise the one with the **smallest `limit_remaining`**; on a tie, the first in the list. `current_limit`, `limit_remaining` and `duration_until_reset` come from that single status; `statuses[i]` stays 1:1 with the descriptors. The backends return a status per limit; the reduction happens in the service's shared decision layer (D31), then the descriptor's shadow applies and only changes the code.
- A descriptor whose limits are all replaced leaves the quota group (except under global `QUOTA_MODE`) but appears in `passedBackends` if it carries a group key.
- **`share_threshold`**: the pattern replaces the value in the `{…}` part of keys (ADR 0001) and in the input of the `offset: hashed` hash (ADR 0002). **`detailed_metric`**, **`value_to_metric`**: stats only, every type.

**D18. Local cache** *(amended)*. Legacy and `fixed`: the reference's exact behaviour (entry set before the shadow check, TTL = full unit length, a limit in shadow mode is no longer counted when the cache is enabled, `over_limit_with_local_cache` counts `hitsAddend`), so hits it answers are not counted, amendments included. `anchored`: never cached, Redis decides every time. When its decision can change is unknowable across instances (a refund on another one, a reload), an entry would lose amendments, and it saves little, since a list's other limits still reach Redis; to revisit if a load is reported, by caching 0-hit checks only. Other new types: an entry lives until the earliest moment the decision can change; cached only when less than one unit remains; a refund clears the local entry (deliberate divergence, new types only); no caching for limits in shadow mode.

**D19. `quota_mode` and lasting lockout.** Warning at load (log and gauge) when `quota_mode` is combined with `sliding`, or with `token_bucket` with `max_debt: unlimited`, in `envoy` mode: a group can stay blocked as long as traffic exceeds the rate.

**D20. `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` on the new types** *(amended)*: **no effect**. To avoid counting denied requests, use `overflow: strict`.

**D21. Refunds (`is_negative_hits`).** Reference properties for every type: always answers `OK`, `limit_remaining = max(0, limit − counter)`, `total_negative_hits` stat, TTL reset to the full duration, no effect on the local cache for legacy; Memcached decrements asynchronously after the response. Per type: `fixed` decrements the current window (floor 0); `anchored` decrements without ever opening or closing the window, and its key keeps expiring with the window rather than a full duration after the refund; `sliding` decrements from the most recent sub-bucket to the oldest; `sliding_approx`, current window then previous; `token_bucket` moves the deadline back, never before `now`.

**D22. `near_limit` and `over_limit` stats.** Exact legacy arithmetic: `nearLimitThreshold = floor(float64(float32(limit) × float32(ratio)))`, split of `hitsAddend` between `near_limit` and `over_limit`, `within_limit` only when `OK`. Usage before and after the request, per type: `token_bucket`, capacity − remaining (can exceed capacity while in debt); `sliding`, raw sum of the sub-buckets; `anchored`, 0 before opening.

**D23. Client-sent `limit`** *(amended)*. If the domain is known: **one** fixed window of `unit × unit_multiplier`, which **replaces all** configured limits of the descriptor; no inherited `shadow_mode`, `unlimited` or `replaces`, global shadow applied; counting mode of the matching rule if there is one, otherwise `envoy`; legacy key if the multiplier is 1, a key including the duration otherwise; a window over `2^32 − 1` seconds refused, as in `rate_limits:`. Reading, echoing as sent and rejecting a multiplier of 0 are **extensions** (the reference ignores the field), as is looking up the rule for the mode.

---

## D. Storage, keys and clock

**D24. Backends.** Redis (Lua scripts for the new types), Memcached (legacy and `fixed`, offset included; `anchored`, `sliding`, `token_bucket` and `strict` are config errors there) and in-memory. Memcached is not atomic (decision on `GET`, asynchronous increment): tests tolerate it. Memcached keys ≤ 250 bytes, without spaces or control characters; otherwise the error is logged and `OK` returned, as upstream.

**D25. Keys of the new types** *(amended)*: ADR 0001.

**D26. Identity of a limit** *(amended)*: type + duration + rounding + opening mode + `buckets` + `offset` + **name** (ADR 0001). Changing any of them starts from zero. Changing `requests_per_unit`, `burst` or `max_debt` keeps the counters; a cap lowered below the counter blocks until the end of the window, tokens above a lowered `burst` are capped on read. During a rolling deployment, the effective limit can briefly double (documented).

**D27. Clock.** A single `now` per request, read from an injectable clock and passed as ARGV to scripts; never Redis `TIME`. Scripts clamp a negative elapsed time at 0. Exception to reproduce for legacy: the reference re-reads the clock for the `LIMIT_*` headers (`ratelimit.go:594, 615`).

---

## E. Configuration and observability

**D28. Reload and xDS** *(amended)*. Reload from `RUNTIME_ROOT/RUNTIME_SUBDIRECTORY/RUNTIME_APPDIRECTORY` with the same logic (`SymlinkRefresher` if `RUNTIME_WATCH_ROOT`, otherwise `DirectoryRefresher`; `RUNTIME_IGNOREDOTFILES`; `runtime` stats scope). xDS SotW is supported for what the upstream proto carries, which already excludes `value_to_metric` and `share_threshold` upstream. Our additions are YAML only.

**D29. Metrics** *(amended)*.

- Same names as the reference. Exclusive sinks, fatal error if two are enabled: statsd over **TCP** (`USE_STATSD=true` by default, so `USE_PROMETHEUS=true` alone is fatal), DogStatsD with mogrifiers, native Prometheus; `DISABLE_STATS` wins; otherwise log.
- Prometheus *(amended)*: a port of statsd_exporter v0.26.1's mapper (every key, decoded with yaml.v2's rules and validated alike; the glob FSM with its ordering quirks, then the regexes in order; `$n` templates; the escaping of unmapped names), its exporter and registry (conflicts, TTLs, self-metrics), and client_golang's Gather checks and text format, served like promhttp. A mapper file that cannot be read or does not load maps nothing, silently, as upstream; one that makes upstream panic is an error at startup (D1). Summaries come with the timers that need them.
- Several limits: a `~name` identifier appended to the **last segment** (`user_alice~weekly`), emitted **even for a single-limit list**. The shipped default mapper adds a `limit=` label and maps the new `total_capped_hits` (D11) and `opened_without_hits` (D9) stats.
- Go stats (`go.*`) *(amended)*: not sent; no Rust equivalent would mean the same (documented divergence). *(amended)* The debug port's `/stats` keeps expvar's `cmdline`, without `memstats`; pprof's paths answer as pprof does when it has no such profile or cannot start one.

---

## F. Code, tests and delivery

**D30. Stack** *(amended)*. tokio, tonic + prost, axum, serde, fred. TLS through `rustls`, for a binary without OpenSSL. The Prometheus listener is served by `hyper` directly, behind a port of Go's mux rules for its one path, rather than axum's router.

**D31. Code layout** *(amended)*. A workspace of two crates: `ratelimit-proto` (flattened protos, compiled with `protox`, no `protoc`) and `ratelimit`. Seam by operations: `Store::apply(batch of ops {key, algo, limit, hits, now})`. Each algorithm of the new types is a pure function `step(state, op) → (state, result)` in Rust (in-memory backend), rewritten in Lua for Redis. **The legacy path stays an `INCRBY` + `EXPIRE` pipeline with the decision on the service side**, not a script, so the commands seen by Redis are identical. The decision layer (thresholds, shadow, quota, status reduction, response) is shared.

**D32. Tests** *(amended)*.

1. **Exact comparison in CI on every PR** with a Go program that imports the reference at a pinned commit, with an injectable `TimeSource` and a seeded or zero jitter; responses, headers, keys, values, TTLs and the sequence of Redis commands (`MONITOR`) compared.
2. **Nightly test** against the official image pinned by digest, with `EXPIRATION_JITTER_MAX_SECONDS=0` and a tolerance of about 1 s.
3. Upstream `integration-test/*.sh` scripts replayed against our image: a smoke test, not parity.
4. **Rust ↔ Lua property tests** on a real Redis **in cluster mode**, fuzzing concurrent `check(0)` / `amend(n)` / `refund(n)` sequences.
5. Differential test of the mapper against a real `statsd_exporter`, and of the raw statsd stream against the reference.
6. TDD with the injectable clock for the new types, including the D9 opening matrix.

**D33. Image** *(amended)*. Binary exactly at `/bin/ratelimit`, distroless without a shell, amd64 + arm64 (`cargo-zigbuild`). Any UID (Envoy Gateway forces 65534), read-only file system, no capabilities. Ports 8080 / 8081 / 6070, `RUNTIME_ROOT=/srv/runtime_data/current`. Explicit dual-stack listening (`[::]` with `IPV6_V6ONLY=0`) and `SO_REUSEPORT` on all three ports. `/healthcheck` served only after the initial Redis connection, as upstream. Every variable injected by Envoy Gateway is supported, except `GRPC_SERVER_TLS_CA_CERT`, **ignored as upstream** (no code reads it). Image signed with cosign, with SBOM and provenance; CI test on `kind` with a real Envoy Gateway where only the image is changed. No Helm chart in v1.

**D34. License.** Apache-2.0, a NOTICE file crediting envoy ratelimit and the Envoy API, vendored proto headers kept, dependency licenses generated with `cargo-about`.

---

## Build order

1. Protos, gRPC server, YAML loader, legacy Redis, **Go comparison harness** (D32.1).
2. Local cache, `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`, headers, dynamic metadata, `quota_mode` groups.
3. Stats sinks and Prometheus mapper.
4. xDS client, image, Envoy Gateway test.
5. Memcached.
6. New types, only through `rate_limits:`: `fixed` with `offset`, `anchored`, `token_bucket`, `sliding`.

## Main risks

1. Byte-for-byte parity of the legacy Redis path (command sequence, ghost keys, `float32`, clock re-read for headers).
2. `quota_mode` groups and dynamic metadata (recent semantics, Go prototext not reproducible).
3. Lua scripts of the new types under Redis Cluster.
4. statsd_exporter mapper and gostats format.
5. xDS SotW client and Envoy Gateway deployment.
