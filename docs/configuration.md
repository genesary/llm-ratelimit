# Configuration

The rate limits are YAML files. Their format is envoyproxy/ratelimit's, as of upstream commit `f4e7669`: a file written for it loads here unchanged and applies the same limits. This service adds one key to it, `rate_limits:`, a list of named limits with windows of any duration.

This page is the reference of the YAML. The environment variables, `RUNTIME_ROOT` and `CONFIG_TYPE` among them, are in [environment.md](environment.md).

- [Files and reloading](#files-and-reloading)
- [How the YAML is read](#how-the-yaml-is-read)
- [The format at a glance](#the-format-at-a-glance)
- [`domain`](#domain)
- [`descriptors`](#descriptors)
- [How a request matches](#how-a-request-matches): [order](#matching-order), [depth](#depth), [wildcards](#wildcards)
- [`rate_limit`](#rate_limit): [units](#units), [`unlimited`](#unlimited), [`name`](#name)
- [`replaces`](#replaces)
- [`shadow_mode`](#shadow_mode)
- [`quota_mode`](#quota_mode)
- [`detailed_metric` and `value_to_metric`](#detailed_metric-and-value_to_metric)
- [`share_threshold`](#share_threshold)
- [`metadata`](#metadata)
- [A limit sent by the client](#a-limit-sent-by-the-client)
- Extensions: [`rate_limits:`](#rate_limits), [windows](#windows) ([`fixed`](#fixed), [`anchored`](#anchored), [durations](#durations)), [decisions with several limits](#decisions-with-several-limits), [headers, stats and metrics](#headers-stats-and-metrics), [moving an existing limit to a list](#moving-an-existing-limit-to-a-list)
- [Errors at load](#errors-at-load)

To see the remaining usage and reset of every limit, see the [quota lookup](quota-lookup.md). To put the service behind agentgateway for LLM token quotas, see [the agentgateway guide](examples/agentgateway.md). The format and its examples are adapted from upstream's README (Apache-2.0).

## Files and reloading

### Where the files live

With the default `CONFIG_TYPE=FILE`, the service reads the files of one directory:

```text
$RUNTIME_ROOT/$RUNTIME_SUBDIRECTORY/$RUNTIME_APPDIRECTORY/
```

`RUNTIME_ROOT` defaults to `/srv/runtime_data/current` and `RUNTIME_APPDIRECTORY` to `config`. `RUNTIME_SUBDIRECTORY` has no default. For example, with `RUNTIME_SUBDIRECTORY=ratelimit`:

```text
/srv/runtime_data/current/
└── ratelimit/
    └── config/
        ├── messaging.yaml      domain: messaging
        └── llm.yaml            domain: llm
```

How the directory is watched depends on `RUNTIME_WATCH_ROOT`:

| `RUNTIME_WATCH_ROOT` | What reloads the files | Use it for |
|---|---|---|
| `true` (default) | A change of `RUNTIME_ROOT` itself, such as a symlink swapped to a new directory. Edits in place are not seen. | Deployments that swap a symlink. |
| `false` | A write, creation, removal or `chmod` of a file in the directory. | Editing files in place; Kubernetes ConfigMaps, with `RUNTIME_IGNOREDOTFILES=true` ([deployment](deployment.md#kubernetes)). |

With `RUNTIME_WATCH_ROOT=true`, set `RUNTIME_SUBDIRECTORY`: when it is empty, no file is read, and the service logs `no runtime configuration. using nil loader.`

### What is loaded

- **Every file**, whatever its extension, in subdirectories too. A file that is not YAML is a load error.
- **Dotfiles and dot-directories too**, unless `RUNTIME_IGNOREDOTFILES=true`, which skips them without reading inside.
- **In name order**, which decides the merge order and which error is reported first.
- **One domain per file.** Two files with the same `domain` are refused, unless `MERGE_DOMAIN_CONFIG=true`. With it, the descriptors of both are combined into one domain; a descriptor defined in both is still refused (`duplicate descriptor composite key`).

Errors name a file by its path, with `/` written as `.`: under `$RUNTIME_ROOT/$RUNTIME_SUBDIRECTORY` with `RUNTIME_WATCH_ROOT=true` (`config.llm.yaml`), under the `config` directory itself with `false` (`llm.yaml`).

### Reloading

Each reload reads every file again and builds a new configuration:

- **It loads:** the new configuration applies to the next requests. It is counted in `ratelimit.service.config_load_success` and logged (`Successfully loaded new configuration`).
- **It does not:** the configuration in use stays in place. The error is logged (`Error loading new configuration: …`) and counted in `ratelimit.service.config_load_error`. See [errors at load](#errors-at-load).
- **No configuration has loaded yet:** every request gets the gRPC error `no rate limit configuration loaded`, until one loads.

A configuration with no file loads, with no domain.

### xDS

With `CONFIG_TYPE=GRPC_XDS_SOTW`, the configuration comes from an xDS management server instead of files: `RateLimitConfig` resources (`type.googleapis.com/ratelimit.config.ratelimit.v3.RateLimitConfig`), each one domain, like a file. All the resources of a response are loaded as one configuration; one that does not load is refused (NACK) and the last one accepted stays in use. Errors name a resource by its `name`.

The resource carries the same fields as the YAML, except `value_to_metric`, `share_threshold` and the [extensions](#rate_limits), which are YAML only. The connection settings are in [environment.md](environment.md).

## How the YAML is read

The files are read as upstream reads them, with YAML 1.1's rules (Go's `yaml.v2`). Most files never notice; these cases do:

| Written | Read as |
|---|---|
| `shadow_mode: yes`, `on`, `y` | `true` (and `no`, `off`, `n` as `false`) |
| `shadow_mode: "true"` | a string, refused: `cannot unmarshal !!str` into `bool` |
| `value: 010`, `value: yes` | the text as written, `010` and `yes`: values keep their source text |
| `value: ~`, `value: null` | no value, as if `value` were left out |
| `requests_per_unit: 0x10` | 16 (`010` would be 8, in octal) |
| `domain: 1.5`, or any unquoted decimal number, anywhere outside `metadata` | refused: `error checking config`. Quote it: `"1.5"`. |
| a key twice in one mapping | the last one |

Anchors, aliases and merge keys (`<<: *defaults`) work. Only the first YAML document of a file (before a second `---`) is read.

## The format at a glance

```yaml
domain: <name, required>
descriptors:
  - key: <required>
    value: <optional>
    rate_limit:                    # optional; or rate_limits: (an extension)
      unit: <second | minute | hour | day | week | month | year>
      requests_per_unit: <0 to 4294967295>
      unlimited: <bool>            # instead of unit and requests_per_unit
      name: <optional>
      replaces:                    # optional
        - name: <the name of another limit>
    shadow_mode: <bool>
    quota_mode: <bool>
    detailed_metric: <bool>
    value_to_metric: <bool>
    share_threshold: <bool>
    metadata: <any mapping>
    descriptors:                   # optional, nested
      - key: ...
```

Every key is optional except `domain` and each descriptor's `key`. A key that is not in this list is refused at load (`config error, unknown key '…'`), so a typo never goes unnoticed. `metadata` is not checked.

## `domain`

A domain is a set of limits. Each request names one (`RateLimitRequest.domain`), and only that domain's descriptors apply to it. Domains let teams keep configurations that do not conflict.

- It is required and must not be empty.
- One file declares one domain. Two files declaring the same one need `MERGE_DOMAIN_CONFIG=true` (see [above](#what-is-loaded)).
- A request for a domain that no file declares gets OK for every descriptor, and is counted in `ratelimit.service.rate_limit.<domain>.domain_not_found`.

## `descriptors`

A request carries one or more **descriptors**. Each is an ordered list of entries, a key and a value each: `("message_type", "marketing"), ("to_number", "2061234567")`. Keys and values are case-sensitive.

The configuration is a tree of descriptors that those entries are matched against:

- **`key`** (required): the entry's key.
- **`value`** (optional): the entry's value. Without it, the descriptor matches any value of that key, and each value is still counted on its own.
- **`descriptors`** (optional): nested descriptors, matched against the request's next entry.
- **`rate_limit`** (optional): the limit of the requests that end here. A descriptor with neither a limit nor nested descriptors limits nothing.

```yaml
domain: messaging
descriptors:
  # 5 marketing messages a day to each number
  - key: message_type
    value: marketing
    descriptors:
      - key: to_number
        rate_limit:
          unit: day
          requests_per_unit: 5

  # 100 messages a day to each number
  - key: to_number
    rate_limit:
      unit: day
      requests_per_unit: 100
```

`("message_type", "marketing")` has no limit: it is only a level to nest under. `to_number` has no value, so `("to_number", "2061111111")` and `("to_number", "2062222222")` each get their own 5, and their own 100, messages a day.

A request can carry several descriptors, and every one of them is checked:

```text
domain: messaging
descriptor: ("message_type", "marketing"), ("to_number", "2061111111")
descriptor: ("to_number", "2061111111")
```

The answer is OVER_LIMIT when any descriptor is over its limit (but see [`quota_mode`](#quota_mode)), and each descriptor gets its own status.

The pair of `key` and `value` must be unique among the descriptors of one level: two `key: to_number` without a value, side by side, are refused (`duplicate descriptor composite key`).

## How a request matches

A request's descriptor walks the tree one entry at a time, from the top level of its domain.

### Matching order

At each level, the entry `(key, value)` is matched against the descriptors of that level, in this order:

1. a descriptor with that `key` and exactly that `value`;
2. a descriptor with that `key` and a [wildcard](#wildcards) `value` matching it (the first in the file, if several match);
3. a descriptor with that `key` and no `value`.

The most specific match wins. Here an address gets 10 requests a second, except `50.0.0.5`, which gets none:

```yaml
domain: edge_proxy_per_ip
descriptors:
  - key: remote_address
    rate_limit:
      unit: second
      requests_per_unit: 10

  - key: remote_address
    value: 50.0.0.5
    rate_limit:
      unit: second
      requests_per_unit: 0
```

`requests_per_unit: 0` is valid: every request is over the limit.

When an entry matches nothing, the walk stops and no limit applies to that descriptor.

### Depth

A limit applies only to a descriptor that ends where the limit is: the request's descriptor must have as many entries as the limit's depth in the tree. A descriptor with more entries does not match a shallower limit, and one with fewer does not reach a deeper one.

With the `messaging` configuration above:

| The request's descriptor | Limit | Why |
|---|---|---|
| `(message_type, marketing), (to_number, 2061111111)` | 5 a day | exact `message_type`, then any `to_number` |
| `(to_number, 2061111111)` | 100 a day | any `to_number`, at the top level |
| `(message_type, marketing)` | none | that descriptor has no limit of its own |
| `(message_type, transactional), (to_number, 2061111111)` | none | no `message_type` matches `transactional` |
| `(to_number, 2061111111), (message_type, marketing)` | none | the order of entries matters, and `to_number` has no nested descriptors |

A descriptor can have a limit and nested descriptors at once: a request ending there gets its limit, a longer one goes on to the nested ones.

### Wildcards

A `value` containing `*` is a wildcard. Each `*` matches any run of characters, empty included, and can stand anywhere:

| `value` | Matches | Does not match |
|---|---|---|
| `value*` | `value`, `value1`, `valueXYZ` | `myvalue` |
| `/api/*/action` | `/api/123/action`, `/api/user-id/action` | `/api/123/other` |
| `/api/*/resource/*/action` | `/api/v1/resource/123/action` | `/api/v1/resource/123` |

An exact `value` is tried before the wildcards. Each value a wildcard matches has its own counter, unless [`share_threshold`](#share_threshold) is set.

## `rate_limit`

```yaml
rate_limit:
  unit: minute
  requests_per_unit: 100
```

| Key | Meaning |
|---|---|
| `unit` | The window: `second`, `minute`, `hour`, `day`, `week`, `month` or `year`, in any case. Required unless `unlimited`. |
| `requests_per_unit` | The hits allowed per window, from 0 to 4294967295: requests, or more when the client sends `hits_addend` (see [api.md](api.md)). |
| `unlimited` | `true` for a descriptor [without a limit](#unlimited). |
| `name` | A name that [`replaces`](#replaces) can refer to. |
| `replaces` | The limits this one [replaces](#replaces). |

### Units

The windows of a unit follow each other from the Unix epoch (1970-01-01 00:00 UTC), and a window's counter starts again from 0 when the next one begins.

| `unit` | Window | Windows start |
|---|---|---|
| `second` | 1 s | every second |
| `minute` | 60 s | every minute, at `:00` |
| `hour` | 3600 s | every hour, at `:00` UTC |
| `day` | 24 h | at 00:00 UTC |
| `week` | 7 days | on Thursdays at 00:00 UTC (the epoch was a Thursday) |
| `month` | 30 days | every 30 days from the epoch; on the 1st of each UTC month with `USE_CALENDAR_MONTH_RATE_LIMIT=true` |
| `year` | 365 days | every 365 days from the epoch |

For a window of another length, such as five hours, or one that starts when a user first comes, use [`rate_limits:`](#rate_limits).

### `unlimited`

```yaml
domain: internal
descriptors:
  - key: ldap
    rate_limit:
      unlimited: true

  - key: azure
    rate_limit:
      unit: minute
      requests_per_unit: 100
```

An unlimited descriptor is always OK, with `limit_remaining` at 4294967295. It never reaches Redis or Memcached. It tells the client that the descriptor is known, unlike one that matches nothing. `unlimited: true` with a `unit` is refused.

### `name`

A limit's `name` is what [`replaces`](#replaces) refers to. It is also sent back in the response's `current_limit.name`. It is optional for `rate_limit:`, need not be unique, and is free text.

## `replaces`

`replaces` lets one limit take another's place when both match the same request. Use it for an exception: users have a limit on every read endpoint, and one user needs more on one of them.

```yaml
domain: example7
descriptors:
  - key: key_1
    value: value_1
    descriptors:
      - key: user
        value: bkthomps
        rate_limit:
          name: specific_limit
          requests_per_unit: 5
          unit: second
  - key: key_2
    value: value_2
    descriptors:
      - key: user
        value: bkthomps
        rate_limit:
          replaces:
            - name: specific_limit
          requests_per_unit: 10
          unit: second
```

| The request's descriptors | Limits checked |
|---|---|
| `(key_1, value_1), (user, bkthomps)` | 5 a second |
| `(key_2, value_2), (user, bkthomps)` | 10 a second |
| both descriptors in one request | 10 a second only: `specific_limit` is replaced |

- **Within one request:** the replacements of every limit the request matches are collected first. Then every limit whose `name` is among them is left out: it is not counted, and its descriptor is OK with no `current_limit`.
- **Not transitive:** a limit that is replaced still replaces the ones it names.
- **By name:** every limit with that name is replaced, in any descriptor of the request. A limit without a name is never replaced.
- **Refused at load:** an empty `name` in `replaces`, or a limit replacing its own name.

## `shadow_mode`

```yaml
domain: example6
descriptors:
  - key: service
    descriptors:
      - key: user
        value: user-a
        shadow_mode: true
        rate_limit:
          requests_per_unit: 10
          unit: second
      - key: user
        value: user-b
        rate_limit:
          requests_per_unit: 20
          unit: second
```

`shadow_mode: true` counts hits as usual, but a descriptor over its limit is answered OK. `user-a` is never limited; `user-b` is limited to 20 a second. The hits a shadow limit lets through are counted in its `shadow_mode` stat, so a limit can be tried on real traffic before it is enforced.

`shadow_mode` belongs to the descriptor, not to `rate_limit`. `SHADOW_MODE=true` puts the whole service in shadow mode ([environment.md](environment.md)).

## `quota_mode`

`quota_mode: true` makes a descriptor a **quota descriptor**. Being over its limit does not deny the request by itself. It is meant for a gateway choosing among models: the request is denied only when every model it could use is out of quota.

- **Groups.** Quota descriptors are grouped by the model they name: the values of their `backend_name` and `model_name_override` entries (the last of each). A quota descriptor with neither is a group of its own.
- **Exhausted.** A group is exhausted when any of its descriptors is over its limit.
- **The answer.** The request is OVER_LIMIT when a regular descriptor is over its limit, or when every quota group is exhausted. Each descriptor's own status still says OVER_LIMIT when it is over.
- **The models left.** With `RESPONSE_DYNAMIC_METADATA=true`, the response's dynamic metadata lists in `passedBackends` the `backend_name` and `model_name_override` of each group that is not exhausted, once per group (see [api.md](api.md)).

```yaml
domain: llm
descriptors:
  - key: backend_name
    descriptors:
      - key: model_name_override
        quota_mode: true
        rate_limit:
          unit: day
          requests_per_unit: 1000000
```

A request with the descriptors `(backend_name, a), (model_name_override, m1)` and `(backend_name, b), (model_name_override, m2)` is OK while either model has quota left, and `passedBackends` names the ones that do. It is OVER_LIMIT once both are out.

`QUOTA_MODE=true` makes every descriptor a quota descriptor ([environment.md](environment.md)).

## `detailed_metric` and `value_to_metric`

A descriptor's stats are named after its path in the configuration, not after the request's values: every `to_number` above counts in the stats of `messaging.to_number`. These two keys put the request's values in the stats names. Each value then has stats of its own, so use them only where the number of values is bounded. They change the stats, never the counters or the decisions. The stats are described in [observability.md](observability.md).

**`detailed_metric: true`** names the stats after every entry of the request, from the domain down:

```yaml
domain: example8
descriptors:
  - key: key1
    detailed_metric: true
    rate_limit:
      unit: minute
      requests_per_unit: 10
  - key: key1
    value: value1
    rate_limit:
      unit: minute
      requests_per_unit: 20
```

| The request's descriptor | Stats key |
|---|---|
| `(key1, unspecified_value)` | `example8.key1_unspecified_value` |
| `(key1, unspecified_value2)` | `example8.key1_unspecified_value2` |
| `(key1, value1)` | `example8.key1_value1` (exact match, no `detailed_metric`) |

Without it, the first two would both count in `example8.key1`.

**`value_to_metric: true`** adds the request's value to the stats name at the levels that set it, where the configuration gives no value. A level with a `value` always has it in the name. A wildcard's level gets the request's whole value.

```yaml
domain: example10
descriptors:
  - key: route
    value_to_metric: true
    descriptors:
      - key: http_method
        value_to_metric: true
        descriptors:
          - key: subject_id
            rate_limit:
              unit: minute
              requests_per_unit: 60
```

| The request's descriptor | Stats key |
|---|---|
| `(route, api), (http_method, GET), (subject_id, 123)` | `example10.route_api.http_method_GET.subject_id` |
| `(route, web), (http_method, POST), (subject_id, 456)` | `example10.route_web.http_method_POST.subject_id` |

Without `value_to_metric`, both would count in `example10.route.http_method.subject_id`. When a limit has `detailed_metric`, `value_to_metric` has no effect on it.

## `share_threshold`

By default, each value a [wildcard](#wildcards) matches has its own counter. With `share_threshold: true`, every value it matches shares one counter, and one limit.

```yaml
domain: example11
descriptors:
  - key: files
    value: files/*
    share_threshold: true
    rate_limit:
      unit: hour
      requests_per_unit: 10

  - key: files_no_share
    value: files_no_share/*
    rate_limit:
      unit: hour
      requests_per_unit: 10
```

- 5 requests for `files/a.pdf` and 5 for `files/b.csv` exhaust the shared 10: the next one for `files/c.txt` is OVER_LIMIT.
- 10 requests for `files_no_share/a.pdf` exhaust its own limit only: `files_no_share/b.csv` still has 10.

`share_threshold` needs a `value` containing `*`; anywhere else it is refused at load. In the stats names, the wildcard's level shows the pattern, not the request's value, even with `value_to_metric` or `detailed_metric`:

```yaml
domain: example11_metrics
descriptors:
  - key: route
    value: api/*
    share_threshold: true
    value_to_metric: true
    descriptors:
      - key: method
        rate_limit:
          unit: minute
          requests_per_unit: 60
```

`(route, api/v1), (method, GET)` counts in the stats of `example11_metrics.route_api/*.method`.

## `metadata`

`metadata` is a mapping of anything, which the service does not read:

```yaml
- key: user
  metadata:
    tier: free
    owner: {team: platform}
  rate_limit:
    unit: hour
    requests_per_unit: 100
```

With `RESPONSE_DYNAMIC_METADATA=true`, the response's dynamic metadata holds, under `metadata`, the `metadata` of the limits of the descriptors that were not over their limit. Several are merged: the first descriptor's field wins, and nested mappings are merged field by field. The dynamic metadata is described in [api.md](api.md).

As upstream does, a limit's `metadata` is left out of the response when the descriptor has `detailed_metric: true` and no nested descriptors, or when `value_to_metric` (or `SANITIZE_DESCRIPTOR_METRIC_DOTS`) changes its stats name.

## A limit sent by the client

A request's descriptor can carry its own limit (`RateLimitDescriptor.limit`: `requests_per_unit`, `unit` and `unit_multiplier`), which Envoy builds from its route configuration. It replaces what the configuration says for that descriptor:

- It applies whenever the domain is configured, even if the descriptor matches no configured descriptor. A request for an unknown domain still gets no limit.
- It replaces a `rate_limit:` and a [`rate_limits:`](#rate_limits) list alike, by one limit of the unit sent, times its `unit_multiplier`, even for a `concurrency` window.
- None of the configured descriptor's keys apply: no `shadow_mode`, `quota_mode`, `detailed_metric`, `metadata` or `replaces`. It has no name, so it is never replaced.
- Its stats are named after the request's entries: `domain.key1_value1.key2`, an entry without a value by its key alone.
- A `unit` of `UNKNOWN` is a gRPC `INVALID_ARGUMENT` error (upstream's process crashes there).
- A `unit_multiplier` other than 1 makes it one `fixed` window of that many units, from the epoch, with its own key, counted like a [`rate_limits:`](#rate_limits) limit: `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` does not apply. A multiplier of 0, or a window over `4294967295` seconds, is a gRPC `INVALID_ARGUMENT` error and a `400` on `/json` and `/quota`. Upstream ignores the field.

The fields are described in [api.md](api.md).

## `rate_limits:`

A descriptor takes either `rate_limit:` (upstream's single limit) or `rate_limits:`, a list of named limits. Each limit in the list has its own window, and every one of them applies to every request that matches the descriptor.

```yaml
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

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | 1 to 64 letters, digits, `_` or `-`, starting with a letter or digit, unique in the list. It names the limit in responses, headers, stats and the quota lookup. Renaming it starts the limit's counters again. |
| `requests_per_unit` | yes | The number of hits allowed per window: requests, or tokens when the caller sends `hits_addend`. |
| `window` | yes | The window, [below](#windows). |
| `replaces` | no | The names of other descriptors' limits this one replaces, with upstream's semantics: a list limit can replace, and be replaced by, any named limit. It cannot replace a limit of its own list. |

Some keys stay at the **descriptor** level and apply to every limit of its list:

- `shadow_mode` and `quota_mode`;
- `detailed_metric` and `value_to_metric`;
- `share_threshold`;
- `metadata`.

`overflow: envoy`, the default, counts every hit, denied requests included, as upstream does, except those the [local cache](backends.md#the-local-cache) answers. `overflow: strict` is designed but not built yet, and is refused at load.

A list entry takes a `window`, not a `unit`, and cannot be `unlimited`. For an unlimited descriptor, keep `rate_limit: {unlimited: true}`.

The value of each descriptor entry is part of every limit's storage key, as upstream's is. A list on `key: user` therefore counts each user separately: `user=alice` and `user=bob` have their own windows and counters.

`rate_limits:` and its keys are YAML only: xDS cannot send them. They are valid only in their place: `window` or `type` elsewhere is an unknown key.

## Windows

### `fixed`

A window of fixed length. Windows follow each other without gaps; when one ends, its counter starts again from 0.

```yaml
window: {type: fixed, duration: 5h}                                       # 00:00, 05:00, 10:00, … UTC
window: {type: fixed, duration: 7d, offset: 4d}                           # Monday 00:00 UTC to Monday
window: {type: fixed, duration: 7d, offset: hashed}                       # a start of its own per descriptor
window: {type: fixed, duration: 7d, offset: hashed, offset_rounding: 1d}  # …at a whole day
```

The windows start at multiples of `duration` since the Unix epoch (1970-01-01 00:00 UTC, a Thursday), shifted by the `offset`:

- **No `offset`, or `offset: 0`:** the epoch's windows. A `7d` window starts on Thursdays at 00:00 UTC.
- **`offset: <duration>`:** shifted by a constant, in UTC; `0 ≤ offset < duration`. `offset: 4d` on a `7d` window starts it on Mondays at 00:00 UTC. There is no time-zone option: a constant offset cannot follow daylight saving time.
- **`offset: hashed`:** a start of its own for each descriptor, hashed from its entries (`user=alice`). This spreads the weekly resets of users over the week, as Claude's weekly limit does. It is stateless: no key is stored, it is the same on every replica and survives a Redis flush.
  - `offset_rounding` rounds the start to a multiple of it, and must divide `duration`. The default is the largest of `1h`, `1m` and `1s` that divides `duration`.
  - The start moves when the descriptor's entries change (adding, removing or renaming one), or when `duration` or `offset_rounding` changes. It does not move when the domain or `CACHE_KEY_PREFIX` changes.

A window always lasts exactly its duration. `USE_CALENDAR_MONTH_RATE_LIMIT` only applies to `rate_limit: {unit: month}`. A duration that does not divide 24 h, such as `5h`, cannot be aligned on the day by any offset.

`fixed` windows are counted on Redis and on Memcached.

### `anchored`

A window that a request opens and that lasts its duration from then: a session, like Claude's five hours. Each descriptor value has its own session: Alice's first request at 04:52 opens hers until 09:52, Bob's at 05:32 opens his until 10:32.

```yaml
window: {type: anchored, duration: 5h}
window: {type: anchored, duration: 5h, start_rounding: 1h}   # opened at 04:52, anchored at 04:00, ends at 09:00
window: {type: anchored, duration: 5h, open_on: first_hit}
```

- **Opening:** when no session is open, a request opens one. Its **anchor** is the request's time, rounded down to `start_rounding` if set (in UTC), and the session ends at `anchor + duration`. The anchor never moves while the session is open.
- **`open_on`:**
  - `check` (the default): any request opens it, even one with 0 hits, and even one that another limit denies. agentgateway sends a check before calling the model (0 hits, unless its tokenization estimates input tokens), so the session starts at the user's first message.
  - `first_hit`: only a request whose hits are counted opens it.
- **After the end:** the session closes at `anchor + duration`, and nothing happens until the next request, which opens the next session. A user who comes back at 14:10 gets a session from 14:10 to 19:10.
  - A late amendment, such as a stream that started before the end and finished after, is an ordinary request on a closed window. It opens the next session and is counted in it.
- **Refunds** (`is_negative_hits`) take hits back, but never open or close a session.
- **The reset** (`duration_until_reset`, `RateLimit-Reset`) is the time left until `anchor + duration`.
- **Windows opened without hits:** a request that another limit denies still opens a session with `open_on: check`. Those sessions are counted in the `opened_without_hits` stat. Use `open_on: first_hit` if that is unwanted.

`anchored` windows are counted on Redis only, in one script per request: they are refused at load with `BACKEND_TYPE=memcache`. The local cache never holds them, so Redis is asked every time.

### `token_bucket`

A bucket that refills continuously at `requests_per_unit` tokens per `duration`: usage is smoothed, with no reset to wait for and no cliff at a window's end.

```yaml
window: {type: token_bucket, duration: 5h}                                   # holds what 5 h refill
window: {type: token_bucket, duration: 1m, burst: 500}                       # holds 500, whatever the rate
window: {type: token_bucket, duration: 1m, max_debt: 10m}                    # debt of up to 10 min of refill
```

- **`burst`** (a whole number of tokens above 0, by default `requests_per_unit`): how many tokens it holds. A bucket starts full.
- **A decision:** a bucket with any token in it admits the request, whatever its size, and charges it in full. An empty one denies it, and charges it too, as `overflow: envoy` counts denied hits, unless the local cache answers. A check (0 hits) charges nothing.
- **`max_debt`** (by default a whole `burst`): how far below empty a charge can take the bucket. Tokens, as `500`; or a duration, as `10m`, for what refills in it; or `0` to stop at empty; or `unlimited`. Hits past it are truncated, and counted in `total_capped_hits`.
- **`limit_remaining`** is the whole tokens left, at least 0. `current_limit` and the `RateLimit-Limit` headers report `requests_per_unit`, so with a larger `burst`, what remains can exceed them; [`/quota`](quota-lookup.md) reports the capacity.
- **`duration_until_reset`:** allowed, the time until the bucket is full; denied, the time until it admits again, at least 1 s.
- **Refunds** give tokens back, up to full. They never let it hold more than `burst`.

`requests_per_unit` must be above 0. A bucket taking 24 h or more to fill gets a warning at load: it does not suit a daily or weekly budget, for which use `fixed` with an offset. `quota_mode` with `max_debt: unlimited` gets one too, and counts in the `config_lasting_lockouts` gauge, since a group can stay blocked as long as traffic exceeds the rate.

**With other settings:** it may share a list with other windows, and `shadow_mode` works as for any list limit. `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` has no effect. It is refused at load on Memcached. The [local cache](backends.md#the-local-cache) holds a denied bucket until it would admit again.

### `concurrency`

A limit on requests in flight rather than on hits over time, to keep one user from flooding a model. `requests_per_unit` is the most requests in flight:

```yaml
- key: inflight_user
  descriptors:
    - key: model
      rate_limits:
        - name: inflight
          requests_per_unit: 8
          window: {type: concurrency, lease: 10m, retry_after: 2s}
```

| The request | Effect |
|---|---|
| 0 hits | Takes a slot if fewer than `requests_per_unit` are held; otherwise `OVER_LIMIT`. |
| N > 0 hits | Releases N slots, stopping at none. Always OK. |
| A refund of N | The same release. |

> **This only works on a descriptor of its own, with `type: tokens` and `cost: "1"` on agentgateway's side,** so that the check has 0 hits and the amendment 1. A client sending hits on its checks (agentgateway's tokenization without a `cost`, or Envoy, which sends 1 hit by default) never takes a slot: its requests release instead, and are counted in `releases_without_slot`. That counter should stay near 0.

- **`lease`** (required, no default): how long a slot lasts without a release. Every operation on the window removes the slots this old. Set it around the p99 of your request durations, and keep it short:
  - A slot's release can be lost, and the slot then lasts until its lease ([when](examples/agentgateway.md#limiting-requests-in-flight)).
  - Too long, and a few lost releases lock a user out of the model.
  - Too short, and a request outlives its slot, which admits up to two too many: one when the slot ends, and one when its late amendment releases another request's slot. With a lease at the p99, that concerns about 1% of requests.
  - A slot's lease cannot be renewed: nothing is sent during a request.
- **`retry_after`** (default `1s`, at least `1s`): `duration_until_reset` on every answer, so the `retry-after` of a refusal when no longer limit also refuses.
- **The limit** is a broad safeguard, not a tight limiter. Leave room above what a user needs, so that a few lost releases refuse nobody. Sharing a pool fairly belongs to the scheduler in front of the model.
- **`limit_remaining`** is the limit minus the slots held. `current_limit` reports the lease as a unit: `10m` is `MINUTE` with `unit_multiplier: 10`.

**With other limits in the request:** the others decide first, and a request they deny takes no slot, since no release would follow. With several `concurrency` limits, slots taken for a request that one of them refuses are released at once. During that instant, one may refuse another request: a false refusal is possible, an excess is not. A release applies whatever the other limits say.

**With other settings:**

| Setting | Behaviour |
|---|---|
| `shadow_mode`, `SHADOW_MODE` | Takes slots past the limit and refuses nothing. The excess counts in `shadow_mode`. |
| `quota_mode` | Refused at load. `QUOTA_MODE` leaves the limit out of quota groups: its refusal denies the request. |
| `replaces` | As any list limit: a replaced limit takes and releases nothing. |
| Other limits in the list | Refused at load: an amendment's tokens would release that many slots. |
| `overflow` | `envoy` has no effect. |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` | No effect. |
| `REDIS_PERSECOND` | Always on the main Redis. |
| `BACKEND_TYPE=memcache` | Refused at load: `only fixed windows can be counted on Memcached`. |
| The local cache | Never holds it. |
| A client-sent `limit` | Replaces the window, as for any limit: do not send one on this descriptor. |

Several instances can share one Redis: a slot taken through one can be released through another, and nothing is kept in an instance's memory. See [deployment.md](deployment.md#several-instances-one-valkey).

The format is fixed in [ADR 0004](adr/0004-concurrency-windows.md).

### Durations

A duration is whole numbers of weeks, days, hours, minutes then seconds, each at most once and in that order, with nothing between them:

- `90s`, `5h`, `7d`, `2w`, `1h30m` and `1d12h` are valid;
- `30m1h` (out of order), `1.5h`, `5 h` and `0s` are not.

The longest window is 2³² − 1 seconds, about 136 years.

## Decisions with several limits

A decision counts the request's hits in every limit of the descriptor's list, then answers one status for that descriptor (Envoy's API has one per descriptor):

- **If some limits are over:** of those, the one whose reset is furthest away.
- **Otherwise:** the one with the fewest hits remaining.
- **On a tie:** the first in the list.

`current_limit` carries that limit's `name`, its `requests_per_unit`, and its window as a unit and a multiplier: a `5h` window is `HOUR` × 5 (`unit_multiplier: 5`), and a `7d` window is `WEEK`. `limit_remaining` and `duration_until_reset` come from the same limit.

`shadow_mode` and `quota_mode` then apply to that status as they do upstream. With `quota_mode`, a descriptor counts as exhausted when at least one of its limits is.

## Headers, stats and metrics

**Headers.** With `LIMIT_RESPONSE_HEADERS_ENABLED=true`, `RateLimit-Limit`, `RateLimit-Remaining` and `RateLimit-Reset` describe the request's most limiting status, as upstream's do. With `LIMIT_PER_UNIT_HEADERS_ENABLED=true` as well, every limit is described:

- **One pair per unit:** a pair of headers for each unit, with the multiplier in its name when it is not 1.
- **Grouping:** limits of the same unit and multiplier are grouped as upstream groups its units, and legacy limits keep upstream's headers.

```text
RateLimit-Limit-5Hours: 3        RateLimit-Remaining-5Hours: 2     (a 5h window)
RateLimit-Limit-Weeks: 5         RateLimit-Remaining-Weeks: 4      (a 7d window)
RateLimit-Limit-Hours: 9         RateLimit-Remaining-Hours: 8      (rate_limit: {unit: hour})
```

The headers are described in [api.md](api.md).

**Stats.** Each list limit has its own stats, named after its descriptor with `~<name>` appended, even when the list has one limit:

```text
ratelimit.service.rate_limit.llm.user~session.total_hits
ratelimit.service.rate_limit.llm.user~session.over_limit
ratelimit.service.rate_limit.llm.user~session.opened_without_hits
```

`detailed_metric: true` puts the value in the name, as upstream does: `llm.user_alice~session`.

**Prometheus.** With `USE_PROMETHEUS=true` and the shipped mapper, the limit's name becomes a `limit` label:

```text
ratelimit_service_rate_limit_total_hits{domain="llm",key1="user",limit="session"}
```

The stats and metrics are described in [observability.md](observability.md).

**`/rlconfig`** on the debug port prints a line per limit, with its window.

## Moving an existing limit to a list

A limit's storage key includes its window, so moving `rate_limit: {unit: hour, requests_per_unit: 100}` to a `rate_limits:` entry with `window: {type: fixed, duration: 1h}` starts its counters from 0. Changing a limit's `name`, `duration` or `offset` does the same. The keys of the old limit expire on their own.

## Errors at load

A configuration with a mistake is refused, as upstream refuses its own. The error is logged (`Error loading new configuration: …`) and counted in `ratelimit.service.config_load_error`, and the configuration in use stays in place. Each message starts with the file's name, as the loader names it ([above](#what-is-loaded)):

```text
Error loading new configuration: config.messaging.yaml: invalid rate limit unit 'minutes'
```

**Reading the file.** The YAML parser's wording and line numbers can differ from upstream's.

- `error loading config file: yaml: line 4: …`: a YAML syntax error.
- `error loading config file: yaml: unmarshal errors:` then ``line 6: cannot unmarshal !!str `abc` into uint32``: a value of the wrong type.
- `config error, unknown key 'ratelimit'`: a key the format does not have, often a typo.
- `config error, key is not of type string: 0.25`
- `config error, yaml file contains list of type other than map: a`
- `error checking config`: a decimal number, or an integer over 2⁶³ − 1, outside `metadata`. Quote it.

**The format.**

- `config file cannot have empty domain`
- `duplicate domain 'messaging' in config file`: set `MERGE_DOMAIN_CONFIG=true` to combine them.
- `descriptor has empty key`
- `duplicate descriptor composite key 'messaging.to_number'`
- `invalid rate limit unit 'minutes'`; `invalid rate limit unit ''` when `unit` is missing
- `should not specify rate limit unit when unlimited`
- `should not have an empty replaces entry`
- `replaces should not contain name of same descriptor`
- `share_threshold can only be used with wildcard values (containing '*'), but found key 'files_a'`

**`rate_limits:`.** These messages name the descriptor, from its domain, and the limit (`llm.user~session: …`), for example:

- `should not specify both rate_limit and rate_limits`
- `rate_limits should not be empty`
- `invalid rate_limits name 'a b': 1 to 64 letters, digits, '_' or '-', starting with a letter or digit`
- `duplicate rate_limits name`
- `rate_limits entries take a window, not a unit`
- `rate_limits entries cannot be unlimited: use rate_limit: {unlimited: true}`
- `replaces should not contain a name of the same rate_limits`
- `window is required`, `window type is required`, `unknown window type 'x'`
- `window type 'sliding' is not supported yet`, and the same for `sliding_approx`
- `invalid burst 'x'`, `invalid max_debt 'x'`, `burst is for token_bucket windows`, `offset is not for token_bucket windows`, `a token_bucket needs requests_per_unit above 0`, and the same for the other keys
- `lease is required`, `invalid lease 'x'`, `invalid retry_after 'x'`, `lease is for concurrency windows`, `duration is not for concurrency windows`, and the same for the other keys
- `a concurrency limit must be alone in its rate_limits: hits release its slots`, `a concurrency limit cannot be in quota_mode`
- `window duration is required`, `invalid window duration '1.5h'`
- `offset '7d' must be shorter than the window duration '7d'`
- `offset_rounding '5h' must divide the window duration '7d'`, `offset_rounding needs offset: hashed`
- `start_rounding '6h' must not be longer than the window duration '5h'`
- `invalid open_on 'x': check or first_hit`
- `start_rounding is for anchored windows`, `offset is for fixed windows`
- `only fixed windows can be counted on Memcached`
- `overflow: strict is not supported yet`, `invalid overflow 'x': envoy or strict`
