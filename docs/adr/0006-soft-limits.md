# ADR 0006 — Soft limits

- **Status:** accepted (2026-11-05)
- **Sources:** issue #86; `docs/design.md` D11, D18, D24, D36, D37; ADR 0001, ADR 0004; agentgateway v1.7.0 (`http/remoteratelimit.rs`, `proxy/httpproxy.rs`); llm-d-router v0.10.0 (`pkg/epp/requestcontrol/director.go`, `apix/v1alpha2/inferenceobjective_types.go`)

## Context

A hard volume quota blocks a user even when the GPUs are free, which wastes capacity already paid for. With the llm-d Router's flow control sharing a saturated pool, volume can act as a priority instead, like Slurm's fair share: within budget a request goes at normal priority; past it, it goes best-effort, served when there is room.

What we build on, checked in the sources and measured on a real agentgateway v1.7.0:

- On `OK`, `request_headers_to_add` are set on the request sent upstream, overwriting the client's header. On `OVER_LIMIT` they are ignored.
- The router reads `x-llm-d-inference-objective`, finds the `InferenceObjective` of that name, and takes its `priority`. An unknown name gets priority 0. A higher band is always served first.
- An objective references one pool, and its name is unique in its namespace and must be a Kubernetes name.
- **A `tokens` descriptor's check runs after every transformation**, in `make_backend_call`. Route and backend transformations run before it, and the Gateway's before that. So a transformation can only read the client's header, and a tier header meant for one would be forgeable.

So the service must send the objective's name itself.

## Decision

```yaml
- key: user
  rate_limits:
    - {name: runaway, requests_per_unit: 300000000, window: {type: fixed, duration: 7d, offset: hashed}}
  descriptors:
    - key: model
      value: qwen3
      rate_limits:
        - name: budget
          requests_per_unit: 2000000
          window: {type: token_bucket, duration: 1h, burst: 8000000, max_debt: 1h}
          soft:
            over:  {x-llm-d-inference-objective: best-effort-qwen3}
            under: {x-llm-d-inference-objective: standard}
```

A limit with `soft` never denies. Over, it answers `OK` and adds its `over` headers to `request_headers_to_add`; otherwise its `under` headers. It always adds one or the other, so that the client's header is overwritten.

### Literal values, one block per model

The values are literal. A template such as `best-effort-{model}` was the first idea and was rejected, as `llm.requestModel` is the client's: `best-effort-Qwen/Qwen3` can never be an objective's name, and an unknown name is priority 0, the normal priority. A user past their budget would keep it, silently. There is also no good answer to a value that turns out invalid at run time: agentgateway drops the header, and a gRPC error is a 500 or no limit at all.

`llm.requestModel` is the name the client sent, aliases included, and CEL has no resolved model at the check. So **each accepted name needs its own block, and filtering with `llm.models` is mandatory**: it answers 404 to any other name. Otherwise a request with a name that has no block matches no budget, and the client's header survives.

### When a limit is over

As its window says, on any request:

| Window | Over when |
|---|---|
| `token_bucket` | The bucket is empty: its level is 0 or below, before this request's hits. |
| `fixed`, `anchored` | The count, this request's hits included, is above the limit. |

**`max_debt` is how long a demotion can last after a user stops:** with `1h`, up to an hour of refill. **`max_debt: 0` makes a budget almost useless:** a bucket stopped at empty refills within the second, and its next request is admitted at normal priority, whatever its size.

### Choices

| Subject | Decision | Why |
|---|---|---|
| Status | `OK`, `limit_remaining` 0, as `shadow_mode` per limit | With `OVER_LIMIT`, a denial by another limit would take its `Retry-After` from this one. The client still sees `x-ratelimit-remaining: 0`. |
| Windows | `fixed`, `anchored`, `token_bucket` | A concurrency limit's over means no slot: nothing to demote. Refused at load. |
| Memcached | Refused at load | `token_bucket` and `anchored` are not there, a failure answers OK with no count, and nobody asked. |
| The list | Alone in its `rate_limits:` | The representative status is the longest reset, so a soft limit could hide a hard one's denial. A hard cap gets its own descriptor. |
| The local cache | Never holds it | A cached answer skips the backend, and traffic keeps flowing here, so amendments would be lost and `over` would end early. |
| `shadow_mode`, `SHADOW_MODE` | Sends `under`, counts `soft_over_limit` | Shadow mode observes without effect. |
| `quota_mode` | Refused at load; `QUOTA_MODE` leaves it out of groups | It never denies, so it has no place in a group's decision. |
| `replaces` | As any list limit | A replaced limit adds no header. |
| `overflow` | `envoy` has no further effect | Hits are counted as the window counts them. |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` | No effect, as D20 | |
| `LIMIT_REQUEST_HEADERS_ENABLED` | Soft headers come after, and win | |
| The fingerprint | Covers `soft` | |

### Several soft limits in a request

For one header: `over` beats `under`, then the first in the request's order. This can only be decided per request, as each model's value differs, so it is not refused at load. Within one limit, what is ambiguous is refused: a name twice in a different case, or `over` and `under` not setting the same names, as one header would then survive from the client.

Names and values are validated at load. A name must be an HTTP token, and not a pseudo-header or a hop-by-hop or framing header. A value must be printable ASCII, not empty, with no space around it.

## Consequences

- **A budget is per user and per model.** A user on five models has five budgets, which departs from a single fair share. The header must name the pool anyway.
- **`runaway` needs its own descriptor in agentgateway,** on `user` alone. Its amendment counts tokens over all models.
- **What a client can forge.** Over or under, the service's value overwrites the client's. With `failOpen` and the service unreachable, or a name with no block, the client's header survives. That gains nothing while only negative priorities exist, as an unknown name is priority 0, which is what a client has by default, and `best-effort-*` only lowers it. **The day a positive priority exists,** the route must remove the header first (see the guide).
- **Rotating names.** `x-llm-d-model-name-rewrite` sets the target model at the router, so a client could send a fresh model name for a fresh budget and still reach the real model. The gateway must remove it and its alias, `x-gateway-model-name-rewrite`.
- **Without flow control,** a negative priority is rejected when saturated rather than queued.
- **Priority is strict:** a best-effort request can wait up to flow control's `defaultRequestTTL`, then gets a 429, or a 503 if the pool has no capacity at all, under saturation.
- **It needs two gateways.** The workspace gateway's check must come before the router reads the header. A gateway calling an InferencePool itself asks the router before its own `tokens` check (`httpproxy.rs`), so the router would only see the client's header.
- **A client-sent `limit`** replaces the limit, `soft` included, and can then deny, as it replaces any limit.

## Rejected alternatives

- **A neutral tier header translated by a transformation:** a `tokens` check runs after every transformation, so it would read the client's header.
- **A second `type: requests`, `cost: "0"` descriptor to read the state early:** it works, measured, but adds a call per request. Rejected by the maintainer in favour of no transformation.
- **Value templates:** see above.
- **A transformation after the check, in agentgateway:** proposed in `docs/upstream/agentgateway-transformation-after-check.md`, not depended on.
- **Status `OVER_LIMIT` under an `OK` overall code:** see Choices.
