# Observability

The service reports what it does in three ways:

- **Stats:** counters, gauges and timers, flushed to one sink: statsd, DogStatsD, Prometheus, or stderr.
- **Logs:** lines on stderr, as text or JSON.
- **Traces:** OpenTelemetry spans, exported over OTLP.

The stat names, the log lines and the spans are envoyproxy/ratelimit's, so existing dashboards and alerts keep working. The few differences are listed in [`parity.md`](parity.md) §7. Every variable on this page is also in [`environment.md`](environment.md).

- [Stats](#stats): [per limit](#per-limit), [service](#service), [gRPC server](#grpc-server), [quota lookup](#quota-lookup), [backends](#backends), [runtime](#runtime-and-certificates)
- [Stats keys](#stats-keys): how descriptors become stat names
- [Sinks](#sinks): [statsd](#statsd), [DogStatsD](#dogstatsd), [Prometheus](#prometheus), [stderr](#stderr-the-logging-sink), [`EXTRA_TAGS`](#extra_tags)
- [Logs](#logs)
- [Traces](#traces)

## Stats

Stats are kept in memory and flushed every `STATS_FLUSH_INTERVAL` (default `10s`), the first time one interval after startup:

- **Counters:** each flush sends the increase since the previous flush. A counter that did not move is not sent.
- **Gauges:** each flush sends the current value.
- **Timers:** each value is sent as it is recorded, in milliseconds.

A counter exists from the first time it is counted. So `over_limit` for a limit appears only once a request goes over it.

| Variable | Default | Effect |
|---|---|---|
| `STATS_FLUSH_INTERVAL` | `10s` | How often counters and gauges are flushed. A Go duration: `500ms`, `10s`, `1m`. |
| `DISABLE_STATS` | `false` | `true` drops every stat. It wins over every sink setting. |
| `NEAR_LIMIT_RATIO` | `0.8` | The share of a limit past which hits count in `near_limit`. |
| `EXTRA_TAGS` | empty | Tags added to every stat, [below](#extra_tags). |
| `SANITIZE_DESCRIPTOR_METRIC_DOTS` | `false` | `true` turns `.` into `_` in descriptor keys and values, in stat names only. |

### Per limit

Each limit has its counters under `ratelimit.service.rate_limit.<stats key>.`, where the [stats key](#stats-keys) is the domain followed by the descriptor:

```text
ratelimit.service.rate_limit.edge.generic_key_api.total_hits
ratelimit.service.rate_limit.edge.user.method_DELETE.over_limit
ratelimit.service.rate_limit.llm.user~session.opened_without_hits
```

All of them count **hits**, not requests. A request counts 1 hit unless it sends `hits_addend` (for example the tokens an LLM call used).

| Counter | What it counts |
|---|---|
| `total_hits` | Every hit counted against the limit. |
| `within_limit` | Hits of requests that stayed within the limit. |
| `near_limit` | Hits past `NEAR_LIMIT_RATIO` of the limit (80 % by default) and up to the limit. They are also in `within_limit`, except in the request that crosses the limit. |
| `over_limit` | Hits past the limit, including those the local cache refused. |
| `over_limit_with_local_cache` | Hits the [local cache](backends.md) refused without asking the backend. |
| `shadow_mode` | Over-limit hits let through because the descriptor has `shadow_mode: true`. |
| `total_negative_hits` | Hits given back by refunds (`is_negative_hits`). |
| `total_capped_hits` | Only for `token_bucket` windows: hits past `max_debt`, truncated rather than charged. |
| `slots_acquired`, `slots_released`, `slots_expired`, `releases_without_slot` | Only for `concurrency` windows. See [below](#concurrency-windows). |
| `opened_without_hits` | Only for `anchored` windows: sessions that a denied request opened without counting hits in them. See [configuration.md](configuration.md#anchored). |

An example, for a limit of 3 per minute (near threshold: 2) and four requests of 1 hit:

| Request | Answer | `total_hits` | `within_limit` | `near_limit` | `over_limit` |
|---|---|---|---|---|---|
| 1 | OK | +1 | +1 | | |
| 2 | OK | +1 | +1 | | |
| 3 | OK | +1 | +1 | +1 | |
| 4 | OVER_LIMIT | +1 | | | +1 |

A `rate_limits:` list has a set of counters for each of its limits, named after the descriptor with `~<name>` appended. In the example above, `user~minute` and `user~hour` each have their own `total_hits`.


### Concurrency windows

A [`concurrency` window](configuration.md#concurrency) counts slots. `total_hits`, `within_limit` and `over_limit` count one per slot asked for, since its hits are 0, and `near_limit` is not used.

| Counter | What it counts | What to watch |
|---|---|---|
| `slots_acquired` | Slots taken. | |
| `slots_released` | Slots released: amendments, refunds, and slots taken for a request that ended up denied. | |
| `slots_expired` | Slots removed because their lease ended, by whichever request found them. A slot still there when its key expires is not counted. | Lost amendments. Compare it to `slots_acquired`: a high share means orphans, or a lease under your long requests. |
| `releases_without_slot` | Releases with nothing left to release. | It should stay near 0. Occasional ones come from a request that outlived its lease. Many mean a client sends hits on its checks, or `failOpen` released slots never taken. |

They are counters, not a gauge, because a stats key names the descriptor rather than the user, and they sum across instances. What is in flight, for a limit, across instances:

```promql
sum(ratelimit_service_rate_limit_slots_acquired{limit="inflight"})
  - sum(ratelimit_service_rate_limit_slots_released{limit="inflight"})
  - sum(ratelimit_service_rate_limit_slots_expired{limit="inflight"})
```

This is an estimate that drifts upward: a restart resets an instance's counters, and an orphan whose user goes idle disappears with its key, uncounted. [`/quota`](quota-lookup.md) gives one user's exact figure.

### Configs that diverge

Instances sharing one Redis must apply [the same limits](deployment.md#several-instances-one-valkey). Each domain with a `rate_limits:` list reports a fingerprint of what it loaded, the same for the same keys, windows and thresholds, however they are written. Reordering descriptors leaves it alone; reordering a list's limits, or a descriptor's wildcards, changes it, because that can change an answer:

- in a gauge, `ratelimit.service.config_fingerprint.<domain>.<fingerprint>`, at 1, and at 0 for one replaced by a reload;
- in `/rlconfig`, as `fingerprint <domain> <fingerprint>`;
- in a log line at each load, at info level: `Config fingerprint of domain <domain>: <fingerprint>`.

An alert for two instances applying different limits to the same keys:

```yaml
- alert: RateLimitConfigDiverges
  expr: count by (domain) (count by (domain, fingerprint) (ratelimit_service_config_fingerprint == 1)) > 1
  for: 10m     # a rollout goes through this for a moment
  annotations:
    summary: "Instances apply different limits to domain {{ $labels.domain }}"
```

- **Silence is not agreement.** A domain without a list reports nothing, xDS configs included, so an instance that lost its list raises no alert.
- **Not in the fingerprint:** `metadata`, and the environment: `CACHE_KEY_PREFIX`, `QUOTA_MODE`, `SHADOW_MODE`.
- **Group by what shares a server.** The rule assumes one Redis per Prometheus scope. With several, add the label that tells them apart, such as `cluster`, or two environments would alert forever.
- **`EXTRA_TAGS`** puts tags in the stat's name, which the shipped mapper does not match. With tags, map it yourself.
- **A release changing how a fingerprint is computed** says so in its notes, since instances of two releases then differ.


### Service

| Stat | Type | What it counts |
|---|---|---|
| `ratelimit.service.config_load_success` | counter | Configurations loaded. |
| `ratelimit.service.config_load_error` | counter | Configurations refused. The previous one stays in use. |
| `ratelimit.service.call.should_rate_limit.service_error` | counter | Requests refused as invalid, or received while no configuration is loaded. |
| `ratelimit.service.call.should_rate_limit.redis_error` | counter | Requests that failed in Redis. Memcached failures are logged, not counted here. |
| `ratelimit.service.rate_limit.<domain>.domain_not_found` | counter | Requests for a domain that no configuration has. |
| `ratelimit.service.global_shadow_mode` | counter | Over-limit answers turned into OK by `SHADOW_MODE=true`. |

### gRPC server

| Stat | Type | What it counts |
|---|---|---|
| `ratelimit_server.<method>.total_requests` | counter | gRPC calls of `<method>`. |
| `ratelimit_server.<method>.response_time` | timer | Their duration, in whole milliseconds. |

`<method>` is `ShouldRateLimit`, or `Check` and `List` for the `grpc.health.v1` service. The HTTP endpoints (`/json`, `/quota`, `/healthcheck`) are not counted here.

### Quota lookup

Not in upstream: the stats of [`POST /quota`](quota-lookup.md). A lookup never changes the per-limit counters above.

| Stat | Type | What it counts |
|---|---|---|
| `ratelimit.quota_lookup.total_requests` | counter | Lookups received. |
| `ratelimit.quota_lookup.error.<cause>` | counter | Lookups that failed: `invalid_request` (400), `unknown_domain` (404), `backend` (500). |
| `ratelimit.quota_lookup.response_time` | timer | Their duration, in milliseconds (with a fraction). |

### Backends

**Redis**, for the main pool and, with `REDIS_PERSECOND=true`, the per-second pool:

| Stat | Type | Meaning |
|---|---|---|
| `ratelimit.redis_pool.cx_active` | gauge | Open connections. |
| `ratelimit.redis_pool.cx_total` | counter | Connections opened. |
| `ratelimit.redis_pool.cx_local_close` | counter | Connections closed or lost. |
| `ratelimit.redis_per_second_pool.cx_*` | | The same, for the per-second pool. |

**Memcached** (`BACKEND_TYPE=memcache`): operations by result, as a `code` tag.

| Stat | Type | `code` |
|---|---|---|
| `ratelimit.memcache.multiget` | counter | `success`, `error` |
| `ratelimit.memcache.increment` | counter | `success`, `miss`, `error` |
| `ratelimit.memcache.decrement` | counter | `success`, `miss`, `error` |
| `ratelimit.memcache.add` | counter | `success`, `not_stored`, `error` |
| `ratelimit.memcache.keys_requested` | counter | |
| `ratelimit.memcache.keys_found` | counter | |

In statsd's form, the tag is part of the name: `ratelimit.memcache.multiget.__code=success`.

**Local cache** (`LOCAL_CACHE_SIZE_IN_BYTES` above 0): gauges, read at each flush, under `ratelimit.localcache.`: `entryCount`, `hitCount`, `missCount`, `lookupCount`, `overwriteCount`, `evacuateCount`, `expiredCount`, `averageAccessTime`.

### Runtime and certificates

| Stat | Type | Meaning |
|---|---|---|
| `runtime.load_attempts` | counter | Times the configuration directory was read. |
| `runtime.load_failures` | counter | Reads in which a file could not be read. |
| `runtime.num_values` | gauge | Files in the last read. |
| `certs.load_attempts`, `certs.load_failures`, `certs.num_values` | | The same, for the gRPC server's certificate directory, with `GRPC_SERVER_USE_TLS=true`. |
| `reserved_tag` | counter | `EXTRA_TAGS` keys that are reserved words (`host`, `region`, `az`, …), counted again for each tagged scope. |

Upstream also sends Go runtime stats (`ratelimit.go.*`). This service does not: it has no Go runtime to report.

## Stats keys

The stats key of a limit is the domain, then one segment per descriptor entry, joined with `.`. Each segment is the entry's key, followed by `_<value>` when the value is part of the key:

| Configuration | Request entry | Stats key |
|---|---|---|
| `key: generic_key`, `value: api` | `generic_key=api` | `edge.generic_key_api` |
| `key: client` (no value) | `client=c1` | `edge.client` |
| `key: client`, `value_to_metric: true` | `client=c1` | `edge.client_c1` |
| `key: client`, `detailed_metric: true` | `client=c1` | `edge.client_c1` |
| `key: path`, `value: "/api/*"` (wildcard) | `path=/api/users` | `edge.path_/api/*` |
| `key: path`, `value: "/api/*"`, `value_to_metric: true` | `path=/api/users` | `edge.path_/api/users` |
| nested `user`, then `method: DELETE` | `user=bob`, `method=DELETE` | `edge.user.method_DELETE` |
| `rate_limits:` entry `minute` on `key: user` | `user=bob` | `edge.user~minute` |

The rules:

- **A value in the configuration** is always in the key. A wildcard puts its pattern there, not the request's value.
- **A descriptor without a value** leaves the value out, so all values share one set of counters.
- **`value_to_metric: true`** puts the request's value in the key, for that entry.
- **`detailed_metric: true`** puts the request's values of every entry in the key. It wins over `value_to_metric`. Each distinct value makes new stats: use it only where the number of values is small.
- **List limits** append `~<name>`, after the rules above.

Some characters are replaced in stats keys (never in matching):

- `:` and `|` become `_`.
- The dots of anything shaped like an IPv4 address become `_`: `host=10.0.0.1:80` gives `host_10_0_0_1_80`.
- With `SANITIZE_DESCRIPTOR_METRIC_DOTS=true`, every `.` in keys and values becomes `_`: `host=api.example.com` gives `host_api_example_com` instead of `host_api.example.com`. Without it, a dotted value adds levels to the name, which shifts the labels of the Prometheus mapper.

## Sinks

One sink at a time. The choice:

1. `DISABLE_STATS=true`: no stats.
2. Otherwise, at most one of `USE_STATSD`, `USE_DOG_STATSD` and `USE_PROMETHEUS` may be `true`. `USE_STATSD` is `true` by default, so **set `USE_STATSD=false` when enabling another sink**. With two, the service stops at startup with `Error: unable to use more than one stats sink at the same time. Set one of USE_DOG_STATSD, USE_STATSD, USE_PROMETHEUS.`
3. With none, stats are written to stderr.

| Sink | Settings |
|---|---|
| statsd | `USE_STATSD=true` (default), `STATSD_HOST` (`localhost`), `STATSD_PORT` (`8125`), `STATSD_PROTOCOL` (`tcp`, or `udp`) |
| DogStatsD | `USE_DOG_STATSD=true`, `STATSD_HOST`, `STATSD_PORT`, over UDP |
| Prometheus | `USE_PROMETHEUS=true`, `PROMETHEUS_ADDR` (`:9090`), `PROMETHEUS_PATH` (`/metrics`) |
| stderr | none of the above |

### statsd

The default sink sends plain statsd lines to `STATSD_HOST:STATSD_PORT`, over **TCP** unless `STATSD_PROTOCOL=udp`:

```text
ratelimit.service.rate_limit.edge.user~minute.total_hits:4|c
ratelimit.redis_pool.cx_active:10|g
ratelimit_server.ShouldRateLimit.response_time:1|ms
```

- **Unreachable server:** the service keeps running and logs `connection error: …`. Lines wait in a bounded queue (about 4 MiB). Once it is full, new lines are dropped, and the drop is logged.
- **Tags:** `EXTRA_TAGS` are appended to the name (`….total_hits.__env=prod:4|c`), since plain statsd has no tags.

To get Prometheus metrics through [statsd_exporter](https://github.com/prometheus/statsd_exporter), send to its statsd port. The [Prometheus sink](#prometheus) does the same in-process.

### DogStatsD

```sh
USE_STATSD=false
USE_DOG_STATSD=true
STATSD_HOST=datadog-agent
STATSD_PORT=8125
DD_ENV=prod
DD_SERVICE=ratelimit
```

Lines go over UDP, packed into datagrams of at most 1432 bytes and sent every 100 ms:

```text
ratelimit.service.rate_limit.edge.user.total_hits:4|c|#env:prod,service:ratelimit
```

The tags of a line, in order:

1. **Global tags:** `env:`, `service:` and `version:` from `DD_ENV`, `DD_SERVICE` and `DD_VERSION`. `DD_ENTITY_ID` adds `dd.internal.entity_id:` and turns origin detection off.
2. **`EXTRA_TAGS`**, taken out of the name.
3. **The tags of the first matching mogrifier.**

Origin detection (on unless `DD_ORIGIN_DETECTION_ENABLED=false`) adds the container ID as `|c:<id>` when one is found.

**Mogrifiers** rewrite a name into a shorter name plus tags. List them in `USE_DOG_STATSD_MOGRIFIERS`, then define each one:

| Variable | Meaning |
|---|---|
| `DOG_STATSD_MOGRIFIER_<NAME>_PATTERN` | A regular expression on the stat's name, as Go's `regexp` reads it ([environment](environment.md) lists the differences). |
| `DOG_STATSD_MOGRIFIER_<NAME>_NAME` | The new name. `$1`, `$2`, … are the pattern's groups. |
| `DOG_STATSD_MOGRIFIER_<NAME>_TAGS` | `key:value` pairs, comma-separated, which may use the groups. |

This one turns every per-limit counter into one metric per counter, tagged with the domain and the descriptor:

```sh
USE_DOG_STATSD_MOGRIFIERS=HITS
DOG_STATSD_MOGRIFIER_HITS_PATTERN='^ratelimit\.service\.rate_limit\.([^.]+)\.(.*)\.([^.]+)$'
DOG_STATSD_MOGRIFIER_HITS_NAME='ratelimit.service.rate_limit.$3'
DOG_STATSD_MOGRIFIER_HITS_TAGS='domain:$1,descriptor:$2'
```

```text
ratelimit.service.rate_limit.total_hits:4|c|#domain:edge,descriptor:user~minute
```

The first mogrifier whose pattern matches applies; the others are skipped. A mogrifier without a pattern or a name, or with an invalid pattern, stops the service at startup.

### Prometheus

```sh
USE_STATSD=false
USE_PROMETHEUS=true
PROMETHEUS_ADDR=:9090     # default
PROMETHEUS_PATH=/metrics  # default
```

Each stat goes through a statsd_exporter mapper, which turns its name into a Prometheus name and labels. The registry is served in Prometheus' text format at `PROMETHEUS_ADDR` + `PROMETHEUS_PATH`.

**The shipped mapper** is used unless `PROMETHEUS_MAPPER_YAML` names a file. It maps upstream's stats as upstream's default mapper does, and adds the list limits and the quota lookup:

| Prometheus name | Labels | From |
|---|---|---|
| `ratelimit_service_rate_limit_total_hits` | `domain`, `key1`, `key2`, `limit` | `total_hits` of each limit |
| `ratelimit_service_rate_limit_within_limit` | same | `within_limit` |
| `ratelimit_service_rate_limit_near_limit` | same | `near_limit` |
| `ratelimit_service_rate_limit_over_limit` | same | `over_limit` |
| `ratelimit_service_rate_limit_shadow_mode` | same | `shadow_mode` |
| `ratelimit_service_rate_limit_opened_without_hits` | same | `opened_without_hits` (list limits) |
| `ratelimit_service_rate_limit_total_capped_hits` | same | `total_capped_hits` |
| `ratelimit_service_config_fingerprint` | `domain`, `fingerprint` | 1 for what each domain has loaded, 0 for what it had. See [below](#configs-that-diverge). |
| `ratelimit_service_config_lasting_lockouts` | | Limits in `quota_mode` with `max_debt: unlimited`; absent if there are none |
| `ratelimit_service_rate_limit_slots_acquired`, `…_slots_released`, `…_slots_expired`, `…_releases_without_slot` | same | The [`concurrency`](#concurrency-windows) counters |
| `ratelimit_service_should_rate_limit_error` | `err_type` (`redis_error`, `service_error`) | `call.should_rate_limit.*` |
| `ratelimit_service_total_requests` | `grpc_method` | `ratelimit_server.<method>.total_requests` |
| `ratelimit_service_response_time_seconds` | `grpc_method` | `ratelimit_server.<method>.response_time`, a histogram in seconds |
| `ratelimit_service_config_load_success`, `ratelimit_service_config_load_error` | | `config_load_*` |
| `ratelimit_quota_lookup_errors` | `cause` | `ratelimit.quota_lookup.error.<cause>` |
| `ratelimit_quota_lookup_response_time_seconds` | | `ratelimit.quota_lookup.response_time`, a histogram in seconds |

The per-limit labels:

- **`domain`** is the domain, and **`key1`** the first descriptor segment.
- **`key2`** is the second segment, for descriptors two levels deep or more. Deeper segments are not in the labels.
- **`limit`** is the name of a list limit (`~<name>`). Upstream's limits have no `limit` label.

A scrape after a few requests on the configuration of [the Envoy guide](examples/envoy.md):

```text
ratelimit_service_rate_limit_total_hits{domain="edge",key1="generic_key_api"} 4
ratelimit_service_rate_limit_total_hits{domain="edge",key1="user",limit="minute"} 4
ratelimit_service_rate_limit_total_hits{domain="edge",key1="user",limit="hour"} 4
ratelimit_service_rate_limit_over_limit{domain="edge",key1="user",limit="minute"} 1
ratelimit_service_rate_limit_total_hits{domain="edge",key1="user",key2="method_DELETE"} 2
ratelimit_service_rate_limit_over_limit{domain="edge",key1="user",key2="method_DELETE"} 1
ratelimit_service_config_load_success 1
ratelimit_redis_pool_cx_active 10
runtime_load_attempts 1
```

**Stats no rule maps** keep their statsd name, with every character other than a letter, a digit or `_` turned into `_`: `ratelimit_redis_pool_cx_active`, `runtime_num_values`, `ratelimit_quota_lookup_total_requests`. The same goes for `over_limit_with_local_cache` of one-level descriptors and for every `total_negative_hits`. For descriptors two levels deep or more, `over_limit_with_local_cache` is added into `ratelimit_service_rate_limit_over_limit`, as upstream's mapper does. Counters keep their name as they are, without a `_total` suffix, as statsd_exporter does. The registry also holds statsd_exporter's own `statsd_exporter_*` metrics and `promhttp_metric_handler_*`. It has no `go_*` or `process_*` metrics.

**Variables:**

| Variable | Default | Effect |
|---|---|---|
| `PROMETHEUS_MAPPER_YAML` | empty | A statsd_exporter mapping file to use instead of the shipped mapper. |
| `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS` | `false` | With the shipped mapper only: `true` drops the conversion to seconds of the two `response_time` histograms. They keep their `_seconds` names but hold milliseconds, as upstream's legacy behaviour. |

`PROMETHEUS_MAPPER_YAML` takes [statsd_exporter's mapping format](https://github.com/prometheus/statsd_exporter#metric-mapping-and-configuration): `mappings` with `match` (glob or `match_type: regex`), `name`, `labels`, `match_metric_type`, `timer_type`, `buckets`, `quantiles`, `ttl`, `action: drop`, and `defaults`. Two cautions:

- **A file that cannot be read, or is not a valid mapping, is ignored silently.** No stat is then mapped; the shipped mapper is not used instead. Check a new file by scraping once.
- **The shipped mapper's rules do not match tagged names.** With `EXTRA_TAGS` set, every stat is unmapped, and timers become summaries. Avoid `EXTRA_TAGS` with Prometheus; use the scrape's own labels (target, pod) instead.

The shipped mapper is the file [`default_mapper.yaml`](../crates/llm-ratelimit/src/stats/prometheus/default_mapper.yaml): copy it as a starting point for your own.

### stderr: the logging sink

With `USE_STATSD=false` and no other sink, each flush writes one JSON line per stat on stderr, whatever `LOG_LEVEL` is:

```json
{"level":"debug","ts":1793303136.809801,"logger":"gostats.loggingsink","msg":"flushing counter","json":{"name":"ratelimit.service.config_load_success","type":"counter","value":"1.000000"}}
{"level":"debug","ts":1793303136.809806,"logger":"gostats.loggingsink","msg":"flushing gauge","json":{"name":"ratelimit.redis_pool.cx_active","type":"gauge","value":"10.000000"}}
{"level":"debug","ts":1793303136.809813,"logger":"gostats.loggingsink","msg":"flushing all stats","json":{"type":"all stats","value":"0.000000"}}
```

Gauges are written at every flush, so stderr gets a few lines every `STATS_FLUSH_INTERVAL` even when idle. Set `DISABLE_STATS=true` to have no stats at all.

### `EXTRA_TAGS`

`EXTRA_TAGS=env:prod,region:eu` tags every stat:

- **statsd and stderr:** appended to the name, sorted by key: `ratelimit.service.rate_limit.edge.user.total_hits.__env=prod.__region=eu`.
- **DogStatsD:** sent as tags: `|#env:prod,region:eu`.
- **Prometheus:** appended to the name, which the shipped mapper then no longer matches ([above](#prometheus)).

In values, `.`, `:` and `|` become `_`. Empty keys and values are left out. Keys that are reserved words (`asg`, `az`, `backend`, `canary`, `host`, `period`, `region`, `shard`, `window`, `source`, `project`, `facet`, `envoyservice`) still apply, but are counted in the `reserved_tag` counter.

## Logs

Logs go to stderr.

| Variable | Default | Values |
|---|---|---|
| `LOG_LEVEL` | `warn` | `panic`, `fatal`, `error`, `warn` (or `warning`), `info`, `debug`, `trace`, in any case |
| `LOG_FORMAT` | `text` | `text`, or `json` in any case |

**Text**, when stderr is not a terminal (a container, a file):

```text
time="2026-10-29T20:45:35+01:00" level=warning msg="Listening for gRPC on '0.0.0.0:8081'"
time="2026-10-29T20:47:49+01:00" level=error msg="Error loading new configuration: edge.yaml: duplicate domain 'edge' in config file"
```

On a terminal, the same messages are shorter and coloured: the level's first four letters, the seconds since start, then the message.

**JSON** (`LOG_FORMAT=json`), one object per line, its time with nanoseconds:

```json
{"@message":"connecting to redis on redis:6379 with pool size 10","@timestamp":"2026-10-29T20:46:37.766309+01:00","level":"warning"}
```

- **Time zone:** times are in the local time zone: `TZ`, else `/etc/localtime`, else UTC.
- **Levels:** levels are written as `warning`, `error`, and so on. A few startup lines are at `warning` on purpose, as upstream's, so the default level shows where the service listens.
- **Fatal errors:** a startup error writes a `level=fatal` line, and the process exits with status 1.
- **The trace exporter:** its errors are written in Go's standard logger format (`2026/10/29 20:45:35 <message>`), not as above.
- **Debug and trace:** `LOG_LEVEL=debug` writes each lookup, request and response. These lines match upstream's today, but are best effort and may change. Do not parse them.

## Traces

With `TRACING_ENABLED=true`, the service exports OpenTelemetry spans over OTLP.

| Variable | Default | Meaning |
|---|---|---|
| `TRACING_ENABLED` | `false` | `true` to export spans. |
| `TRACING_EXPORTER_PROTOCOL` | `http` | `http` (OTLP/HTTP, protobuf) or `grpc` (OTLP/gRPC). Anything else stops the service at startup. |
| `TRACING_SERVICE_NAME` | `RateLimit` | The resource's `service.name`. |
| `TRACING_SERVICE_NAMESPACE` | empty | `service.namespace`. |
| `TRACING_SERVICE_INSTANCE_ID` | a random UUID | `service.instance.id`. The pod name is a good choice. |
| `TRACING_SAMPLING_RATE` | `1` | The share of new traces sampled, from `0` to `1`. |

**Sampling** is parent-based: a request that carries a trace context follows its parent's sampled flag. Other requests start a trace, sampled at `TRACING_SAMPLING_RATE`.

**Where spans go** is set by the standard OpenTelemetry variables:

| Variable | Default | Meaning |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4318` (http), `localhost:4317` (grpc) | The collector, as a URL. Over HTTP, `/v1/traces` is appended to its path. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | | The same, for traces only. Over HTTP, its path is used as it is. |
| `OTEL_EXPORTER_OTLP_INSECURE` | `false` | `true` for no TLS. An `http://` endpoint is insecure too; otherwise TLS is used. |
| `OTEL_EXPORTER_OTLP_HEADERS` | | `key=value` pairs, comma-separated, sent with each export. |
| `OTEL_EXPORTER_OTLP_COMPRESSION` | | `gzip` to compress exports. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `10000` | The export timeout, in milliseconds. |
| `OTEL_EXPORTER_OTLP_CERTIFICATE` | | A PEM file of CAs to verify the collector with. |
| `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE`, `OTEL_EXPORTER_OTLP_CLIENT_KEY` | | A client certificate and its key, for mutual TLS. |

Each `OTEL_EXPORTER_OTLP_*` variable also has an `OTEL_EXPORTER_OTLP_TRACES_*` form, which wins. Over HTTP, an insecure endpoint with a CA or a client certificate set stops the service at startup.

**Other variables:**

| Variable | Default | Meaning |
|---|---|---|
| `OTEL_RESOURCE_ATTRIBUTES` | | More resource attributes, as `key=value,…`. The `TRACING_SERVICE_*` attributes win over these, and over `OTEL_SERVICE_NAME`. |
| `OTEL_BSP_SCHEDULE_DELAY` | `5000` | Milliseconds between exports. |
| `OTEL_BSP_MAX_QUEUE_SIZE` | `2048` | Spans waiting to be exported; more are dropped. |
| `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` | `512` | Spans per export. |
| `OTEL_BSP_EXPORT_TIMEOUT` | `30000` | Milliseconds an export may take. |
| `OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT` (or `OTEL_ATTRIBUTE_COUNT_LIMIT`) | `128` | Attributes kept per span. |
| `OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT` (or `OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT`) | no limit | Characters kept per string attribute. |
| `OTEL_SEMCONV_STABILITY_OPT_IN` | | The gRPC span attributes. By default, the stable RPC conventions (`rpc.system.name`, a full `rpc.method`, a string `rpc.response.status_code`). `rpc/old` for the old ones (`rpc.system`, `rpc.service`, a short `rpc.method`); `rpc/dup` for both. |

**The spans:**

- **gRPC:** a server span for each call (`envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit`, and the health service's), then `ShouldRateLimit Execution` and `Redis Pipeline Execution` (or `Memcached Fetch Execution`) within it.
- **`/json`:** its requests are traced too.
- **Propagation:** the incoming context is read from W3C `traceparent` and `tracestate`, or from B3 headers (single `b3` or `x-b3-*`).
- **Failed exports** are not retried: that batch is dropped, and the next ones go out as usual.

A collector over OTLP/gRPC, without TLS:

```sh
TRACING_ENABLED=true
TRACING_EXPORTER_PROTOCOL=grpc
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
TRACING_SERVICE_INSTANCE_ID=ratelimit-0
TRACING_SAMPLING_RATE=0.1
```

At startup, the service logs its parameters at `info` (`TracerProvider initialized with following parameters: …`), or `Tracing disabled`.
