# Backends

The counters live in Redis (the default) or in Memcached, chosen with `BACKEND_TYPE`. A local cache in the service's memory can sit in front of either.

| `BACKEND_TYPE` | Backend | Windows counted |
|---|---|---|
| `redis` (default) | Redis or Valkey: [single](#single), [sentinel](#sentinel), [cluster](#cluster) | `rate_limit:` units, `fixed` and `anchored` windows |
| `memcache` | [Memcached](#memcached) | `rate_limit:` units and `fixed` windows |

Every variable on this page is listed with its default in [environment.md](environment.md). The windows are described in [configuration.md](configuration.md#windows), and the stats in [observability.md](observability.md).

- [Redis](#redis): [deployments](#deployments), [authentication](#authentication), [TLS](#tls), [AWS ElastiCache IAM](#aws-elasticache-iam), [the pool](#the-pool), [startup](#startup), [a second Redis for short windows](#a-second-redis-for-short-windows), [failover](#failover), [Valkey](#valkey)
- [Storage keys](#storage-keys) and [TTLs](#ttls)
- [Memcached](#memcached)
- [The local cache](#the-local-cache)
- [`STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`](#stop_cache_key_increment_when_overlimit)
- [`NEAR_LIMIT_RATIO`](#near_limit_ratio)

## Redis

Each decision is one pipeline on one connection:

- **`rate_limit:` limits and `fixed` windows:** `INCRBY` then `EXPIRE` for each key.
- **`anchored` windows:** one Lua script (`EVAL`) per key, which opens, counts and closes the session.
- **Refunds** of `rate_limit:` limits and `fixed` windows: one Lua script per key, which subtracts, floors at 0 and sets the TTL again.

The server must therefore run Lua scripts.

### Deployments

`REDIS_TYPE` (any case) says how to reach Redis, and `REDIS_URL` where:

| `REDIS_TYPE` | `REDIS_URL` | Example |
|---|---|---|
| `SINGLE` (default) | one address, or a Unix socket's path | `redis:6379`, `/var/run/redis.sock`, `redis://user:pass@redis:6379/2` |
| `SENTINEL` | the primary's name, then the sentinels, comma-separated | `mymaster,sentinel-0:26379,sentinel-1:26379` |
| `CLUSTER` | some of the cluster's nodes, comma-separated | `redis-0:6379,redis-1:6379,redis-2:6379` |

#### Single

`REDIS_SOCKET_TYPE` is how to reach `REDIS_URL`:

| `REDIS_SOCKET_TYPE` | `REDIS_URL` |
|---|---|
| `unix` (default) | a socket's path. The default is `/var/run/nutcracker/ratelimit.sock`. |
| `tcp` | `host:port`, or a `redis://` URL. `tcp4` and `tcp6` keep to one address family. |

Most deployments set both:

```sh
REDIS_SOCKET_TYPE=tcp
REDIS_URL=redis:6379
```

A `redis://` URL can carry credentials and a database:

```text
redis://user:password@redis:6379/2
redis://redis:6379?db=2&username=user&password=password
```

The database is selected with `SELECT` on each connection. Credentials in the URL are masked in the logs, including the `username` and `password` query parameters.

#### Sentinel

`REDIS_URL` holds the primary's name, then at least one sentinel. The service asks the first sentinel that answers where the primary is, then connects to it and to its replicas.

- **Credentials:** `REDIS_AUTH` authenticates to Redis, and `REDIS_SENTINEL_AUTH` (`password` or `user:password`) to the sentinels. Without `REDIS_SENTINEL_AUTH`, the sentinels are not authenticated to.
- **Failover:** after a failover, the service finds the new primary within 5 s, when it next asks the sentinels. A connection Redis drops also reconnects through the sentinels, so requests can reach the new primary sooner.
- The sentinels are reached over TCP.

#### Cluster

`REDIS_URL` lists some of the cluster's nodes. The service reads the cluster's topology from the first one that answers (`CLUSTER SLOTS`), then opens a pool on every node, replicas included.

- **Credentials:** the other nodes are dialled by their address, with `REDIS_AUTH`. Credentials in a seed's `redis://` URL are not used for them, so set `REDIS_AUTH`.
- **Keys:** each key goes to the node of its slot. The keys of a `rate_limits:` list share one slot, by their [hash tag](#storage-keys).
- **Topology changes** are followed on `MOVED` and `ASK` replies.
- **A node down at startup** makes the attempt fail; the service tries again, as in [Startup](#startup).
- The nodes are reached over TCP.

### Authentication

| Variable | Meaning |
|---|---|
| `REDIS_AUTH` | `password`, or `user:password` for an ACL user. Sent with `AUTH` on each connection. |
| `REDIS_AUTH_FILE` | A file holding `password` or `user:password`, read again at each connection, surrounding whitespace ignored. A missing or empty file fails the connection. |
| `REDIS_SENTINEL_AUTH` | The sentinels' credentials, in `SENTINEL` mode. |

- **Rotation.** `REDIS_AUTH` is read once, at startup. With `REDIS_AUTH_FILE`, whatever writes the file (a secret manager's agent, a CSI driver, a sidecar) can rotate the credential: the next connection uses the new one, without a restart.
- **Exclusive.** `REDIS_AUTH` and `REDIS_AUTH_FILE` together are refused at startup.
- **Precedence.** `REDIS_AUTH` takes precedence over credentials in a `redis://` URL; what it leaves empty comes from the URL.

### TLS

| Variable | Meaning |
|---|---|
| `REDIS_TLS=true` | Connect to Redis over TLS. |
| `REDIS_TLS_CACERT` | A CA to trust, in addition to the system's roots. |
| `REDIS_TLS_CLIENT_CERT`, `REDIS_TLS_CLIENT_KEY` | A client certificate and key, for a server that requires one. |
| `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION=true` | Verify the server's certificate chain, but not that it names the host. For servers whose certificate does not name the address used, such as Google Cloud Memorystore. |

- **Files.** A file that does not load stops the service at startup. The files are read again with each configuration reload: one that has gone stops the service.
- **Unix socket.** TLS over a Unix socket is refused at startup. Upstream accepts it; this service would have sent the traffic in clear.
- **Keys.** RSA keys of 2048 bits or more, and ECDSA keys on P-256 or P-384.
- **Both pools.** The TLS files apply to both pools; `REDIS_PERSECOND_TLS` turns TLS on for [the second one](#a-second-redis-for-short-windows).

### AWS ElastiCache IAM

With `REDIS_AWS_IAM_AUTH=true`, the service authenticates to ElastiCache with IAM, not with a password. Each connection sends `AUTH <user> <token>`, where the token is a request to the cache presigned with the AWS credentials.

| Variable | Meaning |
|---|---|
| `REDIS_AWS_IAM_AUTH=true` | Turn it on. |
| `REDIS_AWS_IAM_CACHE_NAME` | A serverless cache's name, or a node-based cache's replication group ID. Not the endpoint's host name. Required. |
| `REDIS_AWS_IAM_USER_ID` | The ElastiCache user to connect as. Required. |
| `REDIS_AWS_IAM_SERVERLESS=true` | For a serverless cache, whose tokens are signed differently. |
| `REDIS_AWS_IAM_REGION` | The region. Without it, the AWS configuration's: `AWS_REGION`, `AWS_DEFAULT_REGION`, or the profile's. No region at all is refused at startup. |

- **Credentials.** They are found as the AWS SDK for Go finds them: the environment's keys, a web identity (EKS IRSA), the profile (keys, role, SSO, process), the container's endpoint (ECS, EKS Pod Identity), then the instance's metadata (IMDS). `AWS_CA_BUNDLE` is honoured.
- **Tokens.** A token is signed locally, with no AWS call, and shared by every connection for 14 minutes of its 15, or until a minute before the AWS credentials expire.
- **The AWS side.** ElastiCache needs a user whose authentication mode is `iam`, whose name equals its ID, in a user group attached to the cache. The service's IAM role needs `elasticache:Connect` on the cache and on the user.
- **TLS.** ElastiCache requires in-transit encryption for IAM, so set `REDIS_TLS=true`.
- **Exclusive.** `REDIS_AWS_IAM_AUTH` with `REDIS_AUTH` or `REDIS_AUTH_FILE` is refused at startup.

```sh
REDIS_SOCKET_TYPE=tcp
REDIS_URL=my-cache-abc123.serverless.euw1.cache.amazonaws.com:6379
REDIS_TLS=true
REDIS_AWS_IAM_AUTH=true
REDIS_AWS_IAM_CACHE_NAME=my-cache
REDIS_AWS_IAM_USER_ID=ratelimit
REDIS_AWS_IAM_SERVERLESS=true
```

ElastiCache closes IAM connections after 12 hours; the pool reconnects with a fresh token.

### The pool

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_POOL_SIZE` | `10` | Connections to Redis, all opened at startup. 0 means 4. |
| `REDIS_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | `WAIT` is the only behaviour: when every connection is busy, a request waits for one. `CREATE` and `ERROR` are refused at startup, as upstream refuses them. |
| `REDIS_TIMEOUT` | `10s` | How long connecting may take. 0 is no limit. |
| `REDIS_PIPELINE_WINDOW` | `0` | No effect. Upstream reads it in `CLUSTER` mode only, to buffer writes. |
| `REDIS_PIPELINE_LIMIT` | `0` | No effect; a deprecation warning is logged, as upstream logs it. |
| `REDIS_CLUSTER_PIPELINE_PARALLELISM` | `1` | In `CLUSTER` mode, read, bounded by the pool size and logged as upstream does. A negative value is refused at startup. Commands go to their slot's node in one pipeline per decision, whatever its value. |

The pool sends a `PING` every 5 s divided by the pool size, to one connection after another. The connection count is reported in the `redis_pool.cx_*` gauges.

### Startup

The service connects to Redis before it serves. Until a connection succeeds, it tries again with an exponential backoff:

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_STARTUP_INITIAL_INTERVAL` | `1s` | The first wait. |
| `REDIS_STARTUP_MAX_INTERVAL` | `30s` | The longest wait. |
| `REDIS_STARTUP_MAX_ELAPSED_TIME` | `0` | Give up after this long, and exit. 0 means never give up. |

Each failed attempt is logged: `Retrying Redis connection to … in … (elapsed: …): …`. The HTTP port, and so `/healthcheck`, only opens once Redis answers.

### A second Redis for short windows

With `REDIS_PERSECOND=true`, a second Redis holds the counters of short limits:

- **`rate_limit:` limits** of `unit: second`, and client limits in seconds.
- **`rate_limits:` lists** with a window shorter than a minute. Such a list goes whole to the second Redis, its longer windows included, so its keys stay together.

Everything else stays on the first Redis.

The second Redis has its own variables, each with the `REDIS_PERSECOND_` prefix: `SOCKET_TYPE`, `TYPE`, `URL` (default `/var/run/nutcracker/ratelimitpersecond.sock`), `POOL_SIZE`, `AUTH`, `AUTH_FILE`, `SENTINEL_AUTH`, `TLS`, `TIMEOUT`, `POOL_ON_EMPTY_BEHAVIOR`, `PIPELINE_WINDOW`, `PIPELINE_LIMIT`, `CLUSTER_PIPELINE_PARALLELISM`, and the `AWS_IAM_*` ones. The TLS files (`REDIS_TLS_*`) are shared, and none of the first Redis's other settings carry over.

```sh
REDIS_SOCKET_TYPE=tcp
REDIS_URL=redis:6379
REDIS_PERSECOND=true
REDIS_PERSECOND_SOCKET_TYPE=tcp
REDIS_PERSECOND_URL=redis-persecond:6379
```

### When Redis is down

A connection that is down is skipped while another of the pool is up. When every connection is down, a decision fails at once, with gRPC status `Unknown` and `response returned from Conn: EOF`, rather than waiting for Redis. The caller then applies its failure mode right away: agentgateway's `failureMode`, Envoy's `failure_mode_deny`. `/json` answers `400`.

Each connection reconnects on its own, after 125 ms, then doubling the wait up to 4 s.

In `CLUSTER` mode, a decision on a node that is down still waits for it.

Upstream fails at most one decision per pooled connection, then holds each decision until the caller's deadline. This difference is deliberate ([parity §7](parity.md#7-deliberate-divergences)).

### Failover

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION` | `false` | With `true`, the service is unhealthy while no connection to Redis is left, and healthy again once one is back. |
| `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR` | `false` | In `CLUSTER` mode, close a connection whose command failed with `READONLY`, so the pool reconnects to the current primary. In `SINGLE` mode it only logs a warning, as upstream: upstream's client does not see the errors of pipelined commands, and every decision is a pipeline. |

A lost connection is noticed as soon as its socket closes, and reconnected at once.

### Valkey

Valkey speaks Redis's protocol and runs its Lua scripts: set the same variables. The test suite runs on both Redis and Valkey (9.1.2) in CI.

## Storage keys

`CACHE_KEY_PREFIX` is put in front of every key, on Redis and Memcached. Use it to share one server between several services or environments.

**`rate_limit:` limits** keep upstream's keys, byte for byte, so this service and upstream's can share a Redis during a migration:

```text
<prefix><domain>_<key>_<value>_…_<windowStart>
llm_api_x_1793303040
```

`windowStart` is the Unix time the window started: a multiple of the unit since the epoch, or with `USE_CALENDAR_MONTH_RATE_LIMIT=true`, the first of the UTC calendar month for `unit: month`.

**`rate_limits:` limits** have keys of their own ([ADR 0001](adr/0001-storage-keys-for-rate-limits-entries.md)):

```text
{<prefix><domain>_<key>_<value>_…}_<shape>[_<windowStart>]_<name>
{llm_user_alice_}_fx604800oh3600_1793055600_weekly
{llm_user_alice_}_an18000_session
```

- **The hash tag** `{…}` holds the descriptor's part. On a Redis Cluster, it puts every limit of a descriptor in one slot.
- **Escaping.** Inside it, `%`, `{` and `}` are escaped as `%25`, `%7B` and `%7D`, in the prefix, the domain, the keys and the values. `_` is not escaped, as upstream does not escape it.
- **The shape** names the window: its type and duration in seconds, then its options. `fx604800` is a `7d` fixed window, `fx604800o345600` one with `offset: 4d`, `fx604800oh3600` one with `offset: hashed` rounded to the hour; `an18000` is a `5h` anchored window, `an18000r3600` one with `start_rounding: 1h`, `an3600f` one with `open_on: first_hit`.
- **`windowStart`** is only in a `fixed` window's key: the start of the current window, offset included.
- **The name** comes last.
- **`share_threshold`.** With it, the matched wildcard pattern takes the value's place in both kinds of key.

A `fixed` window's key holds a counter. An `anchored` window's key is a hash: its anchor (`a`, Unix seconds) and its count (`c`).

```text
$ redis-cli hgetall '{llm_user_alice_}_an18000_session'
1) "a"
2) "1793303065"
3) "c"
4) "1"
```

Changing a limit's window (type, duration, offset, rounding, opening) or its name changes its key, so its counter starts again from 0. Changing `requests_per_unit` keeps it. The old keys expire on their own.

## TTLs

Every key expires:

| Key | TTL |
|---|---|
| `rate_limit:` limit | The unit's length, plus jitter. A calendar month: until the next month, plus jitter. |
| `fixed` window | The window's duration, plus jitter. |
| `anchored` window | Until the session ends, plus 60 s. Set when the session opens. |

- **`EXPIRATION_JITTER_MAX_SECONDS`** (default `300`) adds a random number of seconds in `[0, max)` to each TTL, so that keys do not all expire at once. 0 adds none.
- **Refunds** on Redis set a `rate_limit:` or `fixed` key's TTL again, to a full window plus jitter. An anchored key keeps expiring with its session.
- **On Memcached**, the TTL is set when the key is first added. Past 30 days, it is sent as a Unix time, as memcached requires.

## Memcached

With `BACKEND_TYPE=memcache`:

| Variable | Default | Meaning |
|---|---|---|
| `MEMCACHE_HOST_PORT` | none | The servers, comma-separated: `host:port`, or a Unix socket's path. |
| `MEMCACHE_SRV` | none | A DNS SRV record to read the servers from, such as `_memcache._tcp.memcached.svc.cluster.local`. |
| `MEMCACHE_SRV_REFRESH` | `0` | Read the SRV record again at this interval (`30s`, `5m`) and use the new servers. 0 reads it once, at startup. |
| `MEMCACHE_MAX_IDLE_CONNS` | `2` | Idle connections kept per server. |
| `MEMCACHE_TLS=true` | `false` | Connect over TLS. |
| `MEMCACHE_TLS_CACERT`, `MEMCACHE_TLS_CLIENT_CERT`, `MEMCACHE_TLS_CLIENT_KEY` | none | As for [Redis](#tls). |
| `MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION=true` | `false` | Verify the chain, not the name. |

- **One source of servers.** `MEMCACHE_HOST_PORT` and `MEMCACHE_SRV` together are refused at startup.
- **Resolution.** The servers of `MEMCACHE_HOST_PORT` are resolved at startup. If one does not resolve, none is used.
- **Sharding.** Each key goes to the server its CRC-32 picks among the list. Give every replica of the service the same list, in the same order, so a key always lands on the same server. The servers of an SRV record are sorted.
- **TLS on a Unix socket** sends `memcached.invalid` as the server name, so it needs `MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION=true`.

**A decision is not atomic.** It reads the counters with one `get` of all its keys, answers from them, then increments each key in the background (`incr`, or `add` for a new key). Requests arriving at the same instant can each see the old counter, so a client can briefly exceed its limit. Refunds are `decr`, which memcached floors at 0, also after the answer.

**Failures do not fail the decision.** A read that fails, a key memcached cannot take (over 250 bytes, or with spaces or control characters), or a value that is not a number is logged, and the request is answered from what was read: an unread counter counts as 0. Keep descriptor values short and free of spaces; a `rate_limits:` key, with its hash tag, shape and name, is longer than a `rate_limit:` one. A warning at load names a `rate_limits:` limit whose key takes over 186 bytes before its window's start and the request's values.

**What Memcached cannot count.** `anchored` windows need a script that reads and writes atomically. A configuration with one is refused at load on Memcached: `only fixed windows can be counted on Memcached`. `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` has no effect on Memcached.

The client's operations are counted in the `memcache.*` stats.

## The local cache

`LOCAL_CACHE_SIZE_IN_BYTES` (default `0`, off) gives the service a cache in its own memory of the keys known to be over their limit. A request for such a key is answered OVER_LIMIT without asking the backend.

```sh
LOCAL_CACHE_SIZE_IN_BYTES=10485760   # 10 MiB
```

- **What it holds.** The key of a `rate_limit:` limit or a `fixed` window, once a decision finds it over the limit. It stays for the window's length.
- **What it never holds.** `anchored` windows: Redis is asked every time.
- **Shadow mode.** A key over its limit is cached even when its limit is in `shadow_mode`. With the cache on, later requests for that key are answered from it, and their hits are not counted in the backend.
- **Lost hits.** Hits answered from it are never counted in the backend, a token amendment included: on that instance, the limit's count stops growing until its window ends. `anchored` windows are never cached.
- **Refunds** always reach the backend. They do not remove a key from the local cache: a key cached as over its limit stays so until its entry expires.
- **Memory.** It is a port of [freecache](https://github.com/coocood/freecache), down to its memory layout, so it evicts the same keys as upstream when full. Below 512 KiB, its size is 512 KiB.
- **Stats.** Requests answered from it count in `over_limit_with_local_cache`; its own figures are in the `localcache.*` gauges.

It works with Redis and with Memcached. Each replica of the service has its own.

## `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`

With `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT=true`, a request with several descriptors does not count its hits everywhere when some descriptor is over. It only applies on Redis, and only to `rate_limit:` limits:

- **A key of the request is over its limit in the local cache:** no key is incremented. The keys still get `INCRBY 0` and `EXPIRE`, so a key that did not exist is created at 0.
- **Otherwise,** the service first reads the keys (`GET`). If the request's hits would take some keys over their limit, only those keys are counted; the request's other keys get 0.
- **Otherwise,** every key is counted as usual.

It has no effect on `rate_limits:` lists, which always count every hit that reaches the backend.

## `NEAR_LIMIT_RATIO`

`NEAR_LIMIT_RATIO` (default `0.8`) only affects stats: hits past that share of a limit count in `near_limit`. It changes no decision.
