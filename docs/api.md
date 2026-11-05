# API

The service listens on three ports:

| Port | Default | Variables | Serves |
|---|---|---|---|
| gRPC | `0.0.0.0:8081` | `GRPC_HOST`, `GRPC_PORT`, `GRPC_UDS` | [`ShouldRateLimit`](#shouldratelimit), [gRPC health](#grpc-health) |
| HTTP | `0.0.0.0:8080` | `HOST`, `PORT` | [`/json`](#post-json), [`/healthcheck`](#healthcheck), [`/quota`](#quota) |
| Debug | `0.0.0.0:6070` | `DEBUG_HOST`, `DEBUG_PORT` | [`/rlconfig`, `/stats`, pprof's paths](#the-debug-port) |

The variables are described in [environment.md](environment.md), the YAML in [configuration.md](configuration.md), and the stats in [observability.md](observability.md).

- [`ShouldRateLimit`](#shouldratelimit): [the request](#the-request), [how hits are counted](#how-hits-are-counted), [the response](#the-response), [headers](#headers), [dynamic metadata](#dynamic-metadata), [errors](#errors)
- [gRPC health](#grpc-health)
- [gRPC over TLS](#grpc-over-tls), [on a Unix socket](#grpc-on-a-unix-socket), [connection age](#connection-age)
- [The HTTP port](#the-http-port): [`POST /json`](#post-json), [`/healthcheck`](#healthcheck), [`/quota`](#quota)
- [The debug port](#the-debug-port)

## `ShouldRateLimit`

The service implements Envoy's rate limit service, version 3:

```text
envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit
```

This is the API Envoy, Envoy Gateway and agentgateway call. The v2 API is not served. Each call is a **decision**: it counts the request's hits and answers OK or OVER_LIMIT.

The server has no gRPC reflection. To call it with `grpcurl`, give it the protos: Envoy's API (`envoy/service/ratelimit/v3/rls.proto` and its imports), or the trimmed copies in this repository under `crates/ratelimit-proto/proto`.

```sh
grpcurl -plaintext \
  -import-path crates/ratelimit-proto/proto \
  -proto envoy/service/ratelimit/v3/rls.proto \
  -d '{"domain": "llm", "descriptors": [{"entries": [{"key": "user", "value": "alice"}]}]}' \
  localhost:8081 envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit
```

### The request

`RateLimitRequest`:

| Field | Type | Meaning |
|---|---|---|
| `domain` | string | The configuration's `domain`. Required. |
| `descriptors` | list of `RateLimitDescriptor` | The descriptors to decide on. At least one. |
| `hits_addend` | uint32 | The hits each descriptor adds, unless it sets its own. 0, or unset, means 1. |

`RateLimitDescriptor`:

| Field | Type | Meaning |
|---|---|---|
| `entries` | list of `{key, value}` | Matched against the configuration's `descriptors`, as [configuration.md](configuration.md#how-a-request-matches) describes. |
| `hits_addend` | uint64, optional | This descriptor's hits. When set, it replaces the request's `hits_addend`, and 0 means 0: a **check**. |
| `is_negative_hits` | bool | A **refund**: the hits are taken back instead of added. |
| `limit` | `{requests_per_unit, unit, unit_multiplier}` | A limit sent by the client. It replaces the configured limits of this descriptor: see [configuration.md](configuration.md#a-limit-sent-by-the-client). `unit_multiplier` makes the window that many `unit`s: an extension of Envoy's API that upstream ignores. |

Each descriptor is decided on its own, with its own limits. The request's answer combines them, [below](#the-response).

### How hits are counted

| The descriptor sends | Its hits |
|---|---|
| nothing, and the request's `hits_addend` is 0 or unset | 1 |
| nothing, and the request's `hits_addend` is N | N |
| its own `hits_addend: N` | N, whatever the request's |
| its own `hits_addend: 0` | 0: a check |

- **Every hit is counted, denied requests included.** A request that is over the limit still adds its hits, as upstream does, unless the [local cache](backends.md#the-local-cache) answers it. A descriptor whose limit is already reached stays over until its window ends.
- **A descriptor is over its limit** when its counter, once this request's hits are added, is greater than `requests_per_unit`. A check (0 hits) is therefore OK while the counter is at most the limit, and OVER_LIMIT once it has gone past it.
- **A `rate_limits:` list** counts the hits in every one of its limits.
- **A refund** (`is_negative_hits: true`) subtracts the hits:
  - The counter never goes below 0.
  - The answer is always OK, with what remains of the limit, even if the counter is still above it.
  - On Redis, the key of a `rate_limit:` limit or a `fixed` window gets a full window's TTL again.
  - The hits are counted in the `total_negative_hits` stat, not in `total_hits`, and in no over-limit stat.
  - In an [anchored window](configuration.md#anchored), a refund never opens or closes a session.

agentgateway, for example, sends a check before calling a model, then an **amendment** with the tokens the model used. A refund, which agentgateway does not send, gives hits back.

### The response

`RateLimitResponse`:

| Field | Meaning |
|---|---|
| `overall_code` | `OK` or `OVER_LIMIT`, [below](#overall_code). |
| `statuses` | One `DescriptorStatus` per descriptor, in the request's order. |
| `response_headers_to_add` | Headers for the client's response, with `LIMIT_RESPONSE_HEADERS_ENABLED` or `LIMIT_PER_UNIT_HEADERS_ENABLED`: see [Headers](#headers). |
| `request_headers_to_add` | Headers for the request sent upstream, with `LIMIT_REQUEST_HEADERS_ENABLED`. |
| `dynamic_metadata` | With `RESPONSE_DYNAMIC_METADATA=true`: see [Dynamic metadata](#dynamic-metadata). |

`raw_body` and `quota` are never set.

`DescriptorStatus`:

| Field | Meaning |
|---|---|
| `code` | `OK` or `OVER_LIMIT`, after the descriptor's `shadow_mode`. |
| `current_limit` | The limit that decided: `requests_per_unit`, `unit`, for a `rate_limits:` limit its `name` and `unit_multiplier`, and for a client-sent limit the `unit_multiplier` it sent, unless it is 1. Absent when no limit applies. |
| `limit_remaining` | Hits left in the window, 0 when over the limit. `4294967295` (the largest uint32) for an `unlimited` descriptor. |
| `duration_until_reset` | Time until the window ends, in whole seconds. Absent when no limit applies. |

**A `rate_limits:` list** has several limits but one status. The status is the list's **representative status**: of the limits over, the one whose reset is furthest away; otherwise the one with the fewest hits remaining; on a tie, the first in the list. `current_limit`, `limit_remaining` and `duration_until_reset` all come from that limit. See [configuration.md](configuration.md#decisions-with-several-limits).

A window is reported as the largest unit dividing its duration, with `unit_multiplier` when the multiplier is not 1: a `5h` window is `HOUR` with `unit_multiplier: 5`, a `7d` window is `WEEK`. `unit_multiplier` is an extension of Envoy's API that clients may ignore. A `rate_limit:` limit has no `name` unless the YAML gives one, and never a `unit_multiplier`. A client-sent limit is reported as it was sent: `DAY` × 7 stays `DAY` with `unit_multiplier: 7`.

```json
{
  "code": "OVER_LIMIT",
  "currentLimit": {"requestsPerUnit": 100, "unit": "HOUR", "name": "session", "unitMultiplier": 5},
  "durationUntilReset": "18000s"
}
```

A descriptor that matches no limit, or names a domain that is not configured, is OK with no `current_limit`. An unknown domain also counts in the `domain_not_found` stat.

#### `overall_code`

- **`OVER_LIMIT`** when a regular descriptor is over its limit.
- **With `quota_mode`**, quota descriptors are grouped by model, and the request is OVER_LIMIT only when every group is exhausted. See [configuration.md](configuration.md#quota_mode).
- **`OK`** otherwise.
- **`SHADOW_MODE=true`** (global shadow mode) turns an OVER_LIMIT answer into OK, counted in the `global_shadow_mode` stat. The descriptors' statuses keep their codes.

Being over the limit is not an error: the gRPC call succeeds with `overall_code: OVER_LIMIT`.

### Headers

The three headers describe the descriptor closest to its limit: the one with the fewest hits remaining, the first on a tie, except that a regular descriptor over its limit always wins (the last one, when several are).

| Variable | Headers | Added to |
|---|---|---|
| `LIMIT_RESPONSE_HEADERS_ENABLED=true` | `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset` | `response_headers_to_add` |
| `LIMIT_REQUEST_HEADERS_ENABLED=true` | the same three | `request_headers_to_add` |
| `LIMIT_PER_UNIT_HEADERS_ENABLED=true` | `RateLimit-Limit-<Unit>s`, `RateLimit-Remaining-<Unit>s` | `response_headers_to_add`, before the three |

- **Names.** `LIMIT_LIMIT_HEADER`, `LIMIT_REMAINING_HEADER` and `LIMIT_RESET_HEADER` rename the response headers; `LIMIT_REQUEST_LIMIT_HEADER`, `LIMIT_REQUEST_REMAINING_HEADER` and `LIMIT_REQUEST_RESET_HEADER` rename the request headers.
- **`RateLimit-Reset`** is in seconds. For an anchored window, it is the time left in the session.
- **Per-unit headers** describe every limit of every descriptor, not only the representative ones: one pair per unit and multiplier, for the limit of that unit with the fewest hits remaining. A multiplier other than 1 is in the name: `RateLimit-Limit-5Hours`.

```text
RateLimit-Limit-5Hours: 100      RateLimit-Remaining-5Hours: 99
RateLimit-Limit-Weeks: 1000      RateLimit-Remaining-Weeks: 999
RateLimit-Limit-Minutes: 2       RateLimit-Remaining-Minutes: 1
RateLimit-Limit: 2               RateLimit-Remaining: 1           RateLimit-Reset: 35
```

### Dynamic metadata

With `RESPONSE_DYNAMIC_METADATA=true`, `dynamic_metadata` describes the request. Envoy can then log it or pass it on.

| Key | Value |
|---|---|
| `domain` | The request's domain. |
| `descriptors` | One object per descriptor: `entries` as `"key=value"` strings; `hitsAddend` when the descriptor sets it, even to 0; `limit` when the client sent one, as protobuf text (`requests_per_unit:5 unit:HOUR`). |
| `hitsAddend` | The request's `hits_addend`, when it is not 0. |
| `metadata` | The YAML `metadata` of the limits of the descriptors that passed, merged: a key already present is kept, objects present in both are merged. Absent when empty. |
| `passedBackends` | With `quota_mode`: one `{backend_name, model_name_override}` per quota group that is not exhausted. Absent when empty. |

```json
"dynamicMetadata": {
  "domain": "q",
  "descriptors": [
    {"entries": ["backend_name=a", "model_name_override=m1"], "hitsAddend": 0},
    {"entries": ["backend_name=c", "model_name_override=m3"]}
  ],
  "passedBackends": [{"backend_name": "c", "model_name_override": "m3"}]
}
```

### Errors

| Case | gRPC status | Message | Stat |
|---|---|---|---|
| Empty `domain` | `UNKNOWN` | `rate limit domain must not be empty` | `service_error` |
| No descriptors | `UNKNOWN` | `rate limit descriptor list must not be empty` | `service_error` |
| No configuration loaded yet | `UNKNOWN` | `no rate limit configuration loaded` | `service_error` |
| The backend failed (Redis) | `UNKNOWN` | the backend's error, e.g. `command ["INCRBY" …] in pipeline returned error: …` | `redis_error` |
| A client `limit` with `unit: UNKNOWN` | `INVALID_ARGUMENT` | `invalid rate limit unit UNKNOWN` | none |
| An HTTP/2 request that is not gRPC: another content type | `INVALID_ARGUMENT`, HTTP 415 | `invalid gRPC request content-type …` | none |
| The same, with another method than `POST` | `INTERNAL`, HTTP 405 | `Received a HEADERS frame with :method … which should be POST` | none |

The stats are `ratelimit.service.call.should_rate_limit.service_error` and `.redis_error`: see [observability.md](observability.md).

- **Upstream** answers the same errors with the same texts, as `UNKNOWN`. On a client limit of unit `UNKNOWN`, upstream's process crashes; here the call fails with `INVALID_ARGUMENT`.
- **Memcached** failures do not fail the call: they are logged, and the request is answered from what was read. See [backends.md](backends.md#memcached).

## gRPC health

The gRPC port also serves `grpc.health.v1.Health`: `Check`, `Watch` and `List`.

| Service name | Status |
|---|---|
| `ratelimit` | `SERVING` when every component below is healthy, `NOT_SERVING` otherwise. |
| `""` (empty) | Always `SERVING`. |
| any other | `Check`: `NOT_FOUND` (`unknown service`). `Watch`: `SERVICE_UNKNOWN`. |

```sh
grpcurl -plaintext \
  -import-path crates/ratelimit-proto/proto -proto grpc/health/v1/health.proto \
  -d '{"service": "ratelimit"}' localhost:8081 grpc.health.v1.Health/Check
```

The components:

| Component | Healthy |
|---|---|
| Backend | Redis: from the first successful connection at startup. With `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION=true`, unhealthy again while no connection to Redis is left. Memcached: once its client is made. |
| Configuration | Only with `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED=true`: unhealthy until a configuration with at least one domain loads, and again if a configuration with no domain loads. |
| Shutdown | Until the process gets SIGTERM. |

The same health answers [`/healthcheck`](#healthcheck).

On Memcached, upstream never reports healthy, because only its Redis client marks the backend healthy. Here a service on Memcached reports healthy, so it can run behind health probes.

On SIGTERM, the service turns unhealthy at once, closes the gRPC listener, lets the calls in flight finish, and exits with status 0. SIGINT and SIGHUP shut it down the same way, without turning it unhealthy first.

## gRPC over TLS

| Variable | Meaning |
|---|---|
| `GRPC_SERVER_USE_TLS=true` | Serve gRPC over TLS only. Plaintext clients are closed. |
| `GRPC_SERVER_TLS_CERT`, `GRPC_SERVER_TLS_KEY` | The server's certificate chain and private key, in PEM. Both files must be in the same directory. |
| `GRPC_CLIENT_TLS_CACERT` | Require a client certificate issued by this CA (mutual TLS). |
| `GRPC_CLIENT_TLS_SAN` | With a CA: the client's certificate must also be valid for this DNS name or IP. A mismatch is refused and logged (`error validating client: …`). |
| `GRPC_ENFORCE_ALPN_ENABLED=false` | Also serve TLS clients that negotiate no ALPN protocol. By default, clients must negotiate `h2`, as grpc-go requires. |

- **Reload.** The key pair is reloaded when a file in its directory changes, without a restart (`CertProvider reloaded cert from …` in the log). A new pair that fails to load is logged, and the previous one stays in use.
- **Startup.** A key pair or CA file that does not load stops the service at startup.
- **Client CA.** Only certificates `GRPC_CLIENT_TLS_CACERT` issued are accepted. Upstream also accepts any certificate the system's roots vouch for; this service does not, because those roots vouch for servers, not for clients.
- **Keys.** RSA keys of 2048 bits or more, and ECDSA keys on P-256 or P-384. Go also accepts 1024-bit RSA and P-521.
- **The rest of the handshake** (cipher suites, alerts) is rustls', not Go's.
- **`GRPC_SERVER_TLS_CA_CERT`**, which Envoy Gateway sets, is ignored, as upstream ignores it.

Envoy reaches the service over mutual TLS with a `transport_socket` on its rate limit cluster:

```yaml
transport_socket:
  name: envoy.transport_sockets.tls
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
    common_tls_context:
      tls_certificates:
        - certificate_chain: {filename: /etc/envoy/tls/ratelimit-client-cert.pem}
          private_key: {filename: /etc/envoy/tls/ratelimit-client-key.pem}
      validation_context:
        trusted_ca: {filename: /etc/envoy/tls/ratelimit-server-ca.pem}
        match_typed_subject_alt_names:
          - san_type: DNS
            matcher: {exact: ratelimit.example.internal}
```

## gRPC on a Unix socket

`GRPC_UDS=/run/ratelimit/grpc.sock` serves gRPC on that Unix socket instead of `GRPC_HOST:GRPC_PORT`. TLS applies to it too. The socket's file is removed when the service stops.

```sh
grpcurl -plaintext -unix \
  -import-path crates/ratelimit-proto/proto -proto envoy/service/ratelimit/v3/rls.proto \
  -d '{"domain": "llm", "descriptors": [{"entries": [{"key": "user", "value": "alice"}]}]}' \
  /run/ratelimit/grpc.sock envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit
```

## Connection age

| Variable | Default | Meaning |
|---|---|---|
| `GRPC_MAX_CONNECTION_AGE` | `24h` | After this, a client connection is sent a `GOAWAY`, so clients reconnect and re-resolve the service's address, which helps when it scales. |
| `GRPC_MAX_CONNECTION_AGE_GRACE` | `1h` | How long a connection may still finish its calls after the `GOAWAY` before it is closed. |

grpc-go adds ±10% of jitter to the age; this service applies it as given.

## The HTTP port

The HTTP port speaks HTTP/1.1. A path that is not clean (`//json`, `/a/../json`) is redirected with `301` to its clean form; an unknown path gets `404`.

### `POST /json`

The JSON form of `ShouldRateLimit`: the body is a `RateLimitRequest` in protobuf's JSON mapping, and the answer a `RateLimitResponse` in the same mapping.

- **Field names** are accepted in camelCase (`hitsAddend`) or as in the proto (`hits_addend`). Enums are accepted by name (`"HOUR"`) or number.
- **The answer** uses camelCase, and leaves out fields at their default: a `limitRemaining` of 0 does not appear. Durations are strings: `"18000s"`.
- **Any method** is served, not only `POST`.

```sh
curl -s localhost:8080/json -d '{
  "domain": "llm",
  "descriptors": [
    {"entries": [{"key": "user", "value": "alice"}], "hitsAddend": 1200}
  ]
}'
```

```json
{"overallCode":"OK","statuses":[{"code":"OK","currentLimit":{"name":"session","requestsPerUnit":4000000,"unit":"HOUR","unitMultiplier":5},"limitRemaining":3998800,"durationUntilReset":"18000s"}]}
```

| Status | When |
|---|---|
| `200` | `overallCode` is `OK`. |
| `429` | `overallCode` is `OVER_LIMIT`. |
| `400` | The body is not a valid request, or is over 4 MiB, or the call fails for any reason in [Errors](#errors), the backend's failures included. The body is `Bad Request`. |
| `500` | The answer has an unknown code, or cannot be written as JSON. |

Unlike upstream, `/json` accepts `unitMultiplier` in a client limit (Go refuses it with `400`), and refuses with `400` a client limit with an unknown unit (Go's process crashes).

### `/healthcheck`

`200` with the body `OK` when the service is healthy, `500` with no body otherwise. It reports the same components as [gRPC health](#grpc-health). The HTTP port only opens once the first connection to Redis has succeeded.

```sh
curl -i localhost:8080/healthcheck
```

### `/quota`

`POST /quota` takes the same body as `/json` and answers, without counting anything, what each limit of the descriptors has used and when it resets. See [the quota lookup](quota-lookup.md).

## The debug port

| Path | Answer |
|---|---|
| `/` | The list of the paths below. Any path not listed answers it too. |
| `/rlconfig` | The configuration in use, one line per limit. Empty before a configuration loads. |
| `/stats` | `cmdline`: the process's command line, as JSON. |
| `/debug/pprof/` | `404 Unknown profile`. |
| `/debug/pprof/<name>` (`heap`, `goroutine`, …) | `404 Unknown profile`. |
| `/debug/pprof/profile` | `500 Could not enable CPU profiling: not available`. |
| `/debug/pprof/trace` | `500 Could not enable tracing: not available`. |

```sh
curl -s localhost:6070/rlconfig
```

```text
llm.api: unit=MINUTE requests_per_unit=2, shadow_mode: false, quota_mode: false
llm.user~session: window=anchored 18000s open_on=check requests_per_unit=100, shadow_mode: false, quota_mode: false
llm.user~weekly: window=fixed 604800s offset=hashed/3600s requests_per_unit=1000, shadow_mode: false, quota_mode: false
```

- **`/rlconfig`** lists the limits sorted by domain, then by key at each level. Upstream lists them in a random order. A `rate_limits:` limit is named `<descriptor>~<name>` and shows its window.
- **No Go runtime.** Upstream's `/stats` also prints Go's memory statistics (`memstats`), and its pprof paths serve the Go runtime's profiles. This service has no Go runtime: it leaves `memstats` out, and its pprof paths give pprof's own error answers. A tool that probes them reads them as profiles that are not available.
- **A port it cannot listen on** is not fatal: the failure is logged, and the service runs without the debug port.
