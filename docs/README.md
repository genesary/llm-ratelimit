# Documentation

## Reference

- [Configuration](configuration.md): the YAML format, from `domain` and `descriptors` to `rate_limits:` lists and their `fixed` and `anchored` windows.
- [Environment variables](environment.md): every setting, with its default.
- [API](api.md): gRPC `ShouldRateLimit`, `/json`, health checks, the debug port.
- [The quota lookup](quota-lookup.md): `POST /quota`, every limit's usage and reset without counting.
- [Backends](backends.md): Redis (single, sentinel, cluster, per-second), Memcached, the local cache, storage keys.
- [Observability](observability.md): stats, statsd, DogStatsD and Prometheus, logs, traces.
- [Deployment](deployment.md): the image, Docker Compose, Kubernetes, Envoy Gateway.

## Examples

- [Envoy](examples/envoy.md): rate limiting HTTP requests with Envoy's `ratelimit` filter.
- [agentgateway](examples/agentgateway.md): LLM token quotas per user: a five-hour session, a weekly budget, and a quota page.

## How it is built

- [Design](design.md): the goal and every design decision (D1–D34), with the build order.
- [Parity](parity.md): what is proven against envoyproxy/ratelimit and by which test, the deliberate divergences (§7), and the extensions (§8).
- [Architecture decision records](adr/): storage keys, the origin of fixed windows, the quota lookup, `concurrency` windows, and a proposal for limits adapting to load.
- [Glossary](../GLOSSARY.md): the words used across the docs and the code.
