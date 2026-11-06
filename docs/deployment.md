# Deployment

The service ships as one container image. It replaces envoyproxy/ratelimit's image as it is: same binary path, ports, defaults and variables.

- [The image](#the-image)
- [Startup, health and shutdown](#startup-health-and-shutdown)
- [Docker Compose](#docker-compose)
- [Kubernetes](#kubernetes)
- [Envoy Gateway](#envoy-gateway)
- [Replacing upstream's image](#replacing-upstreams-image)

Every variable named here is in [`environment.md`](environment.md); the YAML format is in [`configuration.md`](configuration.md).

## The image

```text
ghcr.io/genesary/llm-ratelimit:<version>
```

| | |
|---|---|
| **Tags** | Set by the release workflow: the version (`0.3.0`) and its major and minor (`0.1`), each also with a `v` like the git tag (`v0.1.0`, `v0.1`), and `sha-<commit>` |
| **Platforms** | `linux/amd64`, `linux/arm64` |
| **Binary** | `/bin/llm-ratelimit`, the entrypoint, and `/bin/ratelimit`, a link to it, the path upstream's manifests call. It takes no arguments: everything is set by environment variables. |
| **Base** | `gcr.io/distroless/static-debian12:nonroot`, as upstream's: no shell, no package manager, no `curl`. |
| **User** | Runs as any UID. CI runs it as 65534 with a read-only root file system, every capability dropped and `no-new-privileges`. It writes no file. |
| **Ports** | 8080 HTTP (`/json`, `/quota`, `/healthcheck`), 8081 gRPC, 6070 debug (`/rlconfig`, `/stats`). With `USE_PROMETHEUS=true`, 9090 for `/metrics`. They listen on IPv4 and IPv6 (IPv4 alone where the container has no IPv6). |
| **Configuration** | Files in `$RUNTIME_ROOT/$RUNTIME_SUBDIRECTORY/config/`. `RUNTIME_ROOT` defaults to `/srv/runtime_data/current`. |
| **Licenses** | `/usr/share/doc/llm-ratelimit/`: `LICENSE`, `NOTICE`, and `THIRD-PARTY-LICENSES.txt` for the crates the binary is built from. |

Without a shell, files are read from outside the container. To read the licenses:

```sh
id=$(docker create ghcr.io/genesary/llm-ratelimit:0.3.0)
docker cp "$id:/usr/share/doc/llm-ratelimit" ./llm-ratelimit-licenses
docker rm "$id"
```

### Verifying the image

Each release is signed with cosign (keyless, by the release workflow's GitHub identity), and carries an SBOM and a provenance attestation:

```sh
cosign verify ghcr.io/genesary/llm-ratelimit:0.3.0 \
  --certificate-identity https://github.com/genesary/llm-ratelimit/.github/workflows/release.yml@refs/tags/v0.3.0 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

For another version, change the tag in both places. Pin the digest that `cosign verify` prints in production manifests.

### Defaults to override in a container

The defaults are upstream's, and some of them assume a sidecar setup:

| Variable | Default | In a container |
|---|---|---|
| `REDIS_SOCKET_TYPE` | `unix` | `tcp`, to reach Redis over the network. |
| `REDIS_URL` | `/var/run/nutcracker/ratelimit.sock` | `host:port`, for example `redis:6379`. |
| `RUNTIME_SUBDIRECTORY` | empty | The directory under `RUNTIME_ROOT` holding `config/`, for example `ratelimit`. |
| `RUNTIME_WATCH_ROOT` | `true` | `false` to reload when a file is edited; see below. |
| `USE_STATSD` | `true` | `false` without a statsd server, or to use another sink ([observability.md](observability.md#sinks)). |

**Configuration reloads.** With `RUNTIME_WATCH_ROOT=true`, only a change of `RUNTIME_ROOT` itself reloads the configuration, as when a deployment tool swaps a `current` symlink. With `false`, the service watches the `config/` directory and reloads when a file in it changes. Use `false` for a mounted directory or a ConfigMap. A configuration that fails to load is logged and counted in `config_load_error`, and the previous one stays in use.

## Startup, health and shutdown

**Startup.** The service connects to the backend first, then loads the configuration, then listens:

- **Redis:** connection attempts are retried with backoff (`REDIS_STARTUP_INITIAL_INTERVAL` `1s`, up to `REDIS_STARTUP_MAX_INTERVAL` `30s`), forever unless `REDIS_STARTUP_MAX_ELAPSED_TIME` is set.
- **The ports:** gRPC, HTTP and the debug port are closed until Redis answers (the Prometheus port opens before). A probe fails as connection refused meanwhile.

**Health.** `GET /healthcheck` on port 8080 answers `200 OK` when the service is healthy, `500` otherwise. The gRPC port also serves `grpc.health.v1`, whose service `ratelimit` reports the same health. The service is unhealthy:

- before Redis first answers;
- with `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION=true`, while no Redis connection is left;
- with `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED=true`, until a configuration with at least one domain has loaded;
- from the first SIGTERM on.

Without `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED=true`, a service whose configuration failed to load still reports healthy, and limits nothing.

**Shutdown.** SIGTERM, SIGINT and SIGHUP stop the service. SIGHUP does not reload anything. On the signal:

1. The service logs `Received signal terminated, initiating shutdown` (or `interrupt`, `hangup`), at `info`. After SIGTERM, health turns unhealthy at once.
2. gRPC stops accepting connections, and the calls in flight finish. Connections whose client does not answer the drain are closed 5 s later.
3. The HTTP and debug ports close, and the process exits.

A stop takes up to about 5 s once the calls in flight are done. Kubernetes' default grace period of 30 s is enough.

## Docker Compose

A Redis and the service, with Prometheus metrics. The configuration directory is mounted read-only, and edits to it reload the service.

```text
.
├── compose.yaml
└── ratelimit/config/edge.yaml
```

```yaml
# ratelimit/config/edge.yaml
domain: edge
descriptors:
  - key: user
    rate_limits:
      - name: minute
        requests_per_unit: 3
        window: {type: fixed, duration: 1m}
      - name: hour
        requests_per_unit: 20
        window: {type: fixed, duration: 1h}
```

```yaml
# compose.yaml
services:
  redis:
    image: redis:8
    command: ["redis-server", "--save", ""]

  ratelimit:
    image: ghcr.io/genesary/llm-ratelimit:0.3.0
    depends_on: [redis]
    user: "65534:65534"
    read_only: true
    cap_drop: [ALL]
    security_opt: ["no-new-privileges:true"]
    environment:
      RUNTIME_SUBDIRECTORY: ratelimit
      RUNTIME_WATCH_ROOT: "false"
      REDIS_SOCKET_TYPE: tcp
      REDIS_URL: redis:6379
      USE_STATSD: "false"
      USE_PROMETHEUS: "true"
      LOG_FORMAT: json
    volumes:
      - ./ratelimit/config:/srv/runtime_data/current/ratelimit/config:ro
    ports:
      - "8080:8080"   # /json, /quota, /healthcheck
      - "8081:8081"   # gRPC
      - "6070:6070"   # /rlconfig
      - "9090:9090"   # /metrics
```

```sh
docker compose up -d
curl -s localhost:8080/healthcheck                 # OK
curl -s localhost:6070/rlconfig                    # the limits loaded
curl -s localhost:8080/json -d '{"domain":"edge","descriptors":[{"entries":[{"key":"user","value":"alice"}]}]}'
```

- **Compose healthchecks:** the image has no shell or `curl`, so a Compose `healthcheck` cannot run inside it. Probe `/healthcheck` from outside, or from another container.
- **File permissions:** the files must be readable by the container's user (UID 65534 here).
- **Behind Envoy:** [the Envoy example](examples/envoy.md) adds an Envoy and a backend to this setup.

## Kubernetes

A ConfigMap for the limits, a Deployment and a Service. It assumes a Redis reachable at `redis:6379` in the same namespace. For a test cluster, [`tools/envoy-gateway/redis.yaml`](../tools/envoy-gateway/redis.yaml) is a minimal one (without persistence; adjust `REDIS_URL` to its namespace).

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ratelimit-config
data:
  edge.yaml: |
    domain: edge
    descriptors:
      - key: user
        rate_limits:
          - name: minute
            requests_per_unit: 3
            window: {type: fixed, duration: 1m}
          - name: hour
            requests_per_unit: 20
            window: {type: fixed, duration: 1h}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ratelimit
spec:
  replicas: 2
  selector:
    matchLabels: {app: ratelimit}
  template:
    metadata:
      labels: {app: ratelimit}
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 65534
        runAsGroup: 65534
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: ratelimit
          image: ghcr.io/genesary/llm-ratelimit:0.3.0
          env:
            - {name: RUNTIME_SUBDIRECTORY, value: ratelimit}
            - {name: RUNTIME_WATCH_ROOT, value: "false"}
            - {name: RUNTIME_IGNOREDOTFILES, value: "true"}
            - {name: HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED, value: "true"}
            - {name: REDIS_SOCKET_TYPE, value: tcp}
            - {name: REDIS_URL, value: "redis:6379"}
            - {name: USE_STATSD, value: "false"}
            - {name: USE_PROMETHEUS, value: "true"}
            - {name: LOG_FORMAT, value: json}
          ports:
            - {name: http, containerPort: 8080}
            - {name: grpc, containerPort: 8081}
            - {name: debug, containerPort: 6070}
            - {name: metrics, containerPort: 9090}
          startupProbe:
            httpGet: {path: /healthcheck, port: http}
            periodSeconds: 2
            failureThreshold: 60      # up to 2 minutes for Redis and the config
          readinessProbe:
            httpGet: {path: /healthcheck, port: http}
            periodSeconds: 5
          livenessProbe:
            httpGet: {path: /healthcheck, port: http}
            periodSeconds: 10
            failureThreshold: 3
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities: {drop: [ALL]}
          volumeMounts:
            - name: config
              mountPath: /srv/runtime_data/current/ratelimit/config
              readOnly: true
      volumes:
        - name: config
          configMap: {name: ratelimit-config}
---
apiVersion: v1
kind: Service
metadata:
  name: ratelimit
spec:
  selector: {app: ratelimit}
  ports:
    - {name: grpc, port: 8081, targetPort: grpc}
    - {name: http, port: 8080, targetPort: http}
```

Point Envoy (or your gateway) at `ratelimit:8081` over gRPC.

The settings that matter on Kubernetes:

- **`RUNTIME_IGNOREDOTFILES=true` is required with a ConfigMap.** Kubernetes mounts a ConfigMap as symlinks into a hidden `..<timestamp>` directory. Without the setting, the service reads each file twice and refuses the configuration: `Error loading new configuration: edge.yaml: duplicate domain 'edge' in config file`.
- **`RUNTIME_WATCH_ROOT=false`** makes an update of the ConfigMap reload the configuration, without a restart, once the kubelet has synced it (up to a minute or so).
- **`HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED=true`** keeps a pod out of the Service while its configuration has not loaded, instead of serving with no limits.
- **Probes:** the ports open only once Redis answers. The startup probe gives Redis time before the liveness probe starts.
- **Load balancing:** gRPC connections are long-lived, so a client stays on one pod. `GRPC_MAX_CONNECTION_AGE` (default `24h`) and `GRPC_MAX_CONNECTION_AGE_GRACE` (default `1h`) make clients reconnect, and so spread over new pods. Lower them (for example `5m` and `30s`) if replicas come and go often.
- **Counters:** they live in Redis, so every replica counts in the same windows.
- **Prometheus:** scrape port `metrics` at `/metrics`.

## Envoy Gateway

[Envoy Gateway](https://gateway.envoyproxy.io/) deploys its own rate limit service, upstream's image, when global rate limiting is enabled. It configures it by xDS over TLS and serves gRPC over TLS. To use this service instead, change only the image, through the Helm chart's `global.images.ratelimit.image`.

CI tests exactly this setup (`tools/envoy-gateway/test.sh`). It runs Envoy Gateway v1.9.3 on kind, with Redis, and a `BackendTrafficPolicy` of 3 requests an hour per `x-user-id` header and 1 for the user `one`. The same requests through Envoy then get the same answers and rate limit headers with upstream's image and with this one. Each call fits within Envoy Gateway's 20 ms timeout, and the pod runs under Envoy Gateway's own security context (UID 65534, read-only, no capabilities).

The chart values of that test:

```yaml
# values.yaml
global:
  images:
    ratelimit:
      image: ghcr.io/genesary/llm-ratelimit:0.3.0
      pullPolicy: IfNotPresent
config:
  envoyGateway:
    gateway:
      controllerName: gateway.envoyproxy.io/gatewayclass-controller
    provider:
      type: Kubernetes
    logging:
      level:
        default: info
    rateLimit:
      backend:
        type: Redis
        redis:
          url: redis.redis-system.svc.cluster.local:6379
```

```sh
helm upgrade --install eg oci://docker.io/envoyproxy/gateway-helm --version v1.9.3 \
  -n envoy-gateway-system --create-namespace -f values.yaml
# The controller reads its configuration at start:
kubectl -n envoy-gateway-system rollout restart deployment/envoy-gateway
```

- **Rate limits:** define them as usual, with `BackendTrafficPolicy` `rateLimit.global` rules. Envoy Gateway translates them into this service's configuration. See [`tools/envoy-gateway/gateway.yaml`](../tools/envoy-gateway/gateway.yaml) for the policy of the test.
- **What Envoy Gateway sends:** it generates the configuration from its own API, so `rate_limits:` lists and the quota lookup are not reachable through it. It runs this service as upstream's, with upstream's features.
- **Checking the swap:** the rate limit pod in `envoy-gateway-system` runs the new image, and its debug port's `/rlconfig` lists the policy's limits.

## Several instances, one Valkey

One service per workspace, all on one Redis or Valkey, gives a user the same quotas in every workspace: the counters are in the server, and instances hold nothing a request depends on (the [local cache](backends.md#the-local-cache) excepted). A [`concurrency`](configuration.md#concurrency) slot taken through one instance can be released through another.

**What must be the same on every instance**, since it decides a key or what a key means:

- `CACHE_KEY_PREFIX`;
- the domain and the descriptors: their keys, and which values they match;
- each limit's `name` and window: type, duration, offset, rounding, and `requests_per_unit`, `lease`;
- `EXPIRATION_JITTER_MAX_SECONDS`, `USE_CALENDAR_MONTH_RATE_LIMIT`, `NEAR_LIMIT_RATIO`, `QUOTA_MODE`, `SHADOW_MODE`.

Keep the [local cache](backends.md#the-local-cache) off: it holds what one instance saw, for a window.

**If they differ.**

| What differs | What happens |
|---|---|
| A key's part (`CACHE_KEY_PREFIX`, the domain, a window's duration or offset, a limit's name) | Different keys: the instances count apart, so a user gets each instance's quota. |
| `requests_per_unit` | One key, two thresholds: the same counter is judged differently, depending on the workspace. |
| A `concurrency` limit's `lease` | One key: each operation purges by its own instance's lease, so the shortest lease wins. |
| A `concurrency` limit's `requests_per_unit` | One key: an instance with the higher limit takes slots past the other's, which then refuses. |

To detect a difference in the config files, alert on the [config fingerprint](observability.md#configs-that-diverge) each instance reports. It does not cover the environment variables above, so keep those in one place.

**`failureMode` when Valkey is the single point of failure.** agentgateway decides what happens when the service answers an error.

| | `failClosed` (the default) | `failOpen` |
|---|---|---|
| Valkey down | Every LLM request fails with a 500: an outage for all workspaces. | Requests pass, unlimited. |
| Quotas | Never exceeded. | Not enforced during the outage. |
| Concurrency windows | A failed check never reaches the model, but amendments lost during the outage orphan their slots until their lease. | Amendments lost during the outage orphan their slots too. And the amendment after a failed check still releases a slot that was never acquired. That slot may be another request's, so admission can exceed the limit for a while, and `releases_without_slot` counts what found nothing. |

Choose with what an outage costs in mind. For a shared model pool, `failOpen` keeps users working, and the scheduler in front of the model still protects it.

## Replacing upstream's image

Change only the image, from `envoyproxy/ratelimit:<tag>` to `ghcr.io/genesary/llm-ratelimit:<version>`, and keep the variables, the YAML files, the mounts and the probes:

- **Counters:** Redis and Memcached keys are the same, so the windows in progress carry over.
- **Metrics and logs:** stat names, Prometheus series and log lines are the same, except for the differences in [`parity.md`](parity.md) §7. One of them: no `ratelimit.go.*` stats, and no `go_*` or `process_*` Prometheus metrics.
- **Rolling back:** switch the image back. Upstream's image refuses a configuration that uses `rate_limits:` (an unknown key there), so remove those descriptors first.
