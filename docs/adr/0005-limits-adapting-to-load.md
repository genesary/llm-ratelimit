# ADR 0005 — Limits adapting to load

- **Status:** proposed (2026-11-05). Nothing is implemented. Arbitrated: wait for measurements (see the end).
- **Sources:** issue #77; `docs/design.md` D11, D24, D35, D36; ADR 0001, ADR 0004; the metrics of vLLM (`vllm:kv_cache_usage_perc`, `vllm:num_requests_waiting`) and of llm-d's router (`llm_d_epp_flow_control_*`), both read on their `main`, so to check against the versions deployed

## Context

`concurrency` and `token_bucket` limits are constants, chosen for a typical load. A GPU node has spare capacity at night and none at peak. A limit set for the peak wastes the night; one set for the night floods the peak.

The question: should an external component, watching the KV cache's occupancy or vLLM's queue, be able to tighten these limits on the fly, by writing a load factor in Redis?

Two things frame the answer:

- ADR 0004 calls `concurrency` a broad safeguard, because orphans hold slots until their lease. Tightening it under load makes orphans refuse more users, at the very moment when flow control, which orphans them, is busiest.
- A rate limit service accepts or refuses. Sharing a saturated pool fairly belongs to the scheduler in front of it (llm-d's flow control).

## Proposal

```yaml
rate_limits:
  - name: inflight
    requests_per_unit: 8
    window: {type: concurrency, lease: 10m, retry_after: 2s}
    load_factor: {key: qwen3, min: 0.25, fallback: 0.5}
```

```text
effective limit = max(1, floor(requests_per_unit × clamp(factor, min, 1)))
```

- **A factor only tightens:** at most 1, so the configured limit stays the ceiling, and the config still says the most a user can get. That ceiling must then be the quiet hours' limit, higher than today's constant.
- **`min`** is required on each limit, above 0 and at most 1. It bounds what a bad writer can do, and the acceptable floor depends on the limit's `requests_per_unit`.
- **`fallback`** is required: the factor used when none can be read, between `min` and 1.
- **Per user, not per pool.** A limit stays per descriptor, and the factor only changes its value. A user under the lowered cap is not touched, so **many light users are not bounded**: the pool's total stays up to the scheduler.
- **Only on `concurrency` and `token_bucket`.** Refused at load elsewhere, and on Memcached.

## Options

### What it modulates

| Option | For | Against |
|---|---|---|
| **A. `concurrency`'s maximum only** | Simple: the script takes the limit as an argument today. Slots in flight finish normally; new ones wait. | The window ADR 0004 asks to keep broad. |
| **B. `token_bucket`, charging hits ÷ factor** | Slows sustained use with no orphans and no format change: the charge is an argument, and the deadline, the level and the capacity keep their meaning. A level seen in `/quota` does not jump when the factor moves. | **It needs the volume quota to be a `token_bucket`:** a deployment on `fixed` or `anchored` windows must move first, which resets the counters and changes what users see. A token costs more under load, which `/quota` must explain. A refund at another factor than its charge is slightly off. |
| **C. `token_bucket`, changing the refill rate** | The literal request. No format change, since the interval is an argument. | Rescales every bucket at once. With the default `burst`, a level follows the factor. With a larger one, a dip *refills* an empty bucket, and the recovery puts it in debt. |
| **D. `token_bucket`'s capacity** | Keeps the rate, so what was charged stays. | Does not reduce sustained load, only bursts. |

### How an instance reads it

| Option | For | Against |
|---|---|---|
| **1. A `GET` in each decision's pipeline** | Always fresh. Same on every instance. | One more command per decision, on the hot path, and a Redis Cluster round trip when the key is on another node. It cannot join the window's script, whose key is in another slot. |
| **2. Each instance polls, every few seconds, and passes the value to the script** | Nothing on the decision path. A Redis down does not change the path. | Instances differ for up to a poll, which only moves the limit by a step. State in an instance's memory, though only a cache of a shared value. |
| **3. Pub/Sub** | Pushes changes. | A missed message lasts until the next one, Pub/Sub in a Redis Cluster adds connections, and it still needs polling to recover. |

### Where the factor is computed

| Option | For | Against |
|---|---|---|
| **I. A small external writer** (a Prometheus query or a sidecar, as the issue asks) | The policy is outside this service. Any signal can feed it. | One more component to run, and to keep alive. |
| **II. This service scrapes vLLM** | Nothing else to deploy. | Couples it to vLLM's metrics and discovery, far from what a rate limit service does. |

## The key

ADR 0001's form does not fit: a factor belongs to a pool, not a descriptor. Proposed: `<CACHE_KEY_PREFIX>load_factor_<key>`, holding a decimal such as `0.5`, and `<key>` from the config, the same for every instance. A legacy key ends in `_<digits>`, and a domain `load` with a key `factor` could produce `load_factor_qwen3_<start>`, so a `<key>` ending in `_<digits>` is refused at load, which rules out a clash. On the main Redis, also under `REDIS_PERSECOND`.

The writer must set a TTL (`SET … EX`), a few times its period.

## When the value is absent or invalid

| The value | Effect |
|---|---|
| Not read yet, at startup | `fallback`, until the first read, which comes before serving when Redis answers. |
| Absent, or expired | `fallback`: the writer is dead. |
| Not a number, NaN, infinite | The last good value, with a log line and a stat: a typo must not move every limit. |
| Above 1 | 1. |
| Below `min`, 0 or negative | `min`. |
| The read fails | The last good value, for a set time counted by the instance, then `fallback`. Only the factor's Cluster node may be down, so decisions can still go on. |

**No value is neutral, so `fallback` has no default.** The ceiling is set for quiet hours, so a `fallback` of 1 gives quiet-hour limits at peak as soon as the writer dies, and a low one turns a monitoring outage into a service outage. Each deployment chooses. Whatever it chooses, the expiry jumps from the last factor to `fallback` at once, bypassing the writer's slow rise.

## Risks

- **Oscillation.** The feedback goes factor → fewer requests → lower occupancy → factor up, with delay at each step. Without care it hunts. The writer needs smoothing, hysteresis and a slow rise (fast down, slow up), which an expired key bypasses. This is the main risk, and tuning it is real work.
- **The signal lags and misleads.** KV cache occupancy can be high with a healthy pool, since a good cache keeps it full. `vllm:num_requests_waiting` is closer to overload, but with flow control the queue moves out of vLLM, and it stops showing saturation. The signal to read is then the router's: the wait in its queue (`llm_d_epp_flow_control_request_queue_duration_seconds`), the queue's size (`llm_d_epp_flow_control_queue_size`) and `llm_d_epp_flow_control_pool_saturation`.
- **Orphans at a low limit.** With `min × requests_per_unit` near 1, one orphan locks a user out of the model for a lease. Hence `min`, and a lease kept short.
- **It may miss the overload.** A per-user cap refuses heavy users only. With many light ones, the pool saturates while no one reaches the cap, and the harnesses it does refuse retry after `retry_after`, adding load.
- **Anyone who can write to Valkey can cut every limit,** bounded by `min`. Shared credentials matter more.
- **Debugging.** "Why am I refused at 4 when the limit says 8?" needs an answer: `/quota` would show the effective limit and the factor, and a stat would count changes. `current_limit` and the headers would report the effective limit.
- **Divergence detection.** The fingerprint (D36) covers `key` and `min`, not the factor's value, by design.
- **Parity.** Opt-in through `rate_limits:`, so nothing changes for upstream's configs.

## Alternatives

- **Do nothing here, and let llm-d's flow control shed load.** It sees the queue, it knows the fairness ID, and it is built to do this. The service keeps a broad safeguard and volume quotas. The maintainer plans to enable it anyway.
- **Scale instead of refusing.** KEDA or an HPA on the same metrics, where GPUs allow. Complementary, but capacity is the constraint here.
- **Switch configs by time of day.** A cron rewriting the runtime directory: a peak config and a night config. No code and no feedback loop, and the fingerprint (D36) detects an instance left behind. It may well answer most of the need, since load follows the clock. It does not react to a surprise, and a `token_bucket`'s rate change rescales its buckets here too (option C).
- **A client-sent `limit` from the gateway.** agentgateway's `limitOverride` is CEL, which can send a limit per request, but it replaces the window entirely and has no load signal to read.
- **Priorities, not limits.** Tighten only low-priority tiers, which needs tiers. A descriptor per tier and this proposal on the lower ones would do it later.

## Recommendation

**Do not build this yet.** Nothing is measured, and flow control, the planned step, addresses the same overload. In order:

1. Enable flow control, and measure.
2. Try the time-of-day configs, which need no code.
3. Only then, if the trigger below holds, build the smallest version.

**What would justify building it,** with values to calibrate: with flow control on, for a week, the p99 wait in the router's queue above your target during peaks, *and* most of that load coming from a few users. If it comes from many light users instead, this does not help: a per-user cap misses them, and the answer is capacity or the scheduler's fairness.

**Where to measure it.** Not in this service: its stats are per descriptor by design, and `/quota` answers for one user at a time.

- **Per user, the router:** `llm_d_epp_flow_control_queue_size` and the queue duration carry a `fairness_id`, which is the user with the planned fairness ID. `sum by (fairness_id) (llm_d_epp_flow_control_queue_size)` shows who fills the queue.
- **Per user, agentgateway:** `config.metrics.fields.add` adds a CEL field such as `jwt.sub` to its metrics, at the cost of a label per user.

The smallest version is **B + 2 + I**: `token_bucket` charged at hits ÷ factor, polled every few seconds, from an external writer. It assumes the volume quota has moved to a `token_bucket`. B slows heavy users with no orphans, where tightening `concurrency` (A) goes against ADR 0004's broad safeguard and makes each orphan weigh more. A remains simple to add if a hard cap under load is wanted.

## Arbitrated (2026-11-05)

The maintainer decided:

1. **Wait for measurements.** This ADR stays `proposed`, to keep the reasoning. Next: flow control, then the time-of-day configs.
2. **If it is ever built: B,** charging hits ÷ factor. A goes against ADR 0004's broad safeguard.
3. **`fallback` is required.** A default of 1, with a ceiling set for quiet hours, gives limits far too wide at peak as soon as the writer dies. Each deployment must choose.
4. **`min` is required, per limit.** The acceptable floor depends on each limit's `requests_per_unit`.
