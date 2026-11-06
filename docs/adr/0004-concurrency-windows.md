# ADR 0004 — `concurrency` windows

- **Status:** accepted (2026-11-05)
- **Sources:** `docs/design.md` D35, D14, D18, D20, D24; agentgateway at `930a32f` (`crates/agentgateway/src/http/remoteratelimit.rs`, `llm/mod.rs`, `proxy/httpproxy.rs`)

## Context

Volume quotas (`anchored`, `fixed`) manage consumption, not the instantaneous load on a GPU node. Agentic harnesses send many requests in a row, and users share one Valkey across workspaces. We want a limit on requests in flight, using the RLS protocol as agentgateway already speaks it, without changing agentgateway.

A `type: tokens` descriptor with a `cost` gives us two signals: a check with 0 hits before the call, and an amendment with the cost after it. What agentgateway does at `930a32f`:

- The check carries 0 hits when the descriptor has a `cost` (`remoteratelimit.rs:283`).
- All `tokens` descriptors of a policy go in one gRPC call, check and amendment alike.
- No amendment follows a denied check (`remoteratelimit.rs:370`).
- It only works on an `ai` backend (`httpproxy.rs:2960`). On a plain HTTP route a `tokens` descriptor is skipped silently.
- **The amendment is not always sent.** It is sent for an HTTP response, 5xx included, and for a stream dropped after it started (`AmendOnDrop`). It is not sent on a transport error (`httpproxy.rs:3330`), a timeout or client disconnect before the response headers (`:1653`), a failed read of a non-streamed body (`llm/mod.rs:2764`) or a response prompt guard's early return. Each retry attempt checks again.
- With `failOpen`, an amendment still follows a check that failed (`remoteratelimit.rs:373`).

So slots will be orphaned, and the protocol carries nothing that could renew a slot. With llm-d's flow control, a request cancelled while queued before the response headers orphans its slot. A flow control TTL eviction answers 429 or 503, so its amendment is sent.

## Decision

### The window

```yaml
- key: inflight_user
  descriptors:
    - key: model
      rate_limits:
        - name: inflight
          requests_per_unit: 8                       # the most in flight
          window: {type: concurrency, lease: 10m, retry_after: 2s}
```

- **`lease`** is required and has no default, because no value suits every model. Set it around the p99 of request duration. Too long a lease locks users out after orphans. Too short a one lets a request outlive its slot, which admits up to two requests too many: one when the slot ends, and one when its late amendment releases another request's slot.
- **`retry_after`**, whole seconds of at least 1, defaults to `1s`. It is `duration_until_reset` on every answer, so never 0 (D12).
- The limit is a **broad safeguard** against one user flooding a pool, not a tight limiter: set `requests_per_unit` above what a user needs, so that a few orphans do not refuse anyone.

### What a request does

| Request | Effect |
|---|---|
| 0 hits | Acquires a slot if `in_flight < max`, else `OVER_LIMIT`. |
| N > 0 hits | Releases N slots, floored at 0. Always `OK`. |
| A refund of N | The same release. A refund of 0 changes nothing. |

Slots are interchangeable. `limit_remaining` is `max − in_flight`, floored at 0.

### Storage

One sorted set per (descriptor, limit), and one script per operation, so each is atomic on its one key, in a Redis Cluster too. The key follows ADR 0001 with the shape `cc`: `{<prefix><domain>_<key>_<value>_…}_cc_<name>`. The lease is not in the shape: changing it must not forget the slots in flight.

A member is a random 128-bit identifier from the instance, its score the acquisition's time. Every operation first removes the slots whose age is at least `lease`, and then refreshes the key's TTL to `lease` + 60 s. No state lives in an instance's memory, so a slot acquired through one instance may be released through another.

### Release the newest slot (`ZPOPMAX`)

With `ZPOPMIN`, each release would remove an orphan and keep the fresher stamp of the request that has just finished. Under steady traffic the excess would then never expire. With `ZPOPMAX`, an orphan keeps its old stamp and expires on time. The cost: a live long request can lose its slot at its lease, and its amendment then releases another's.

### Decide, then acquire

A slot must not stay held when the request is denied, since no amendment will follow. Other limits usually live in other Redis Cluster slots, so one atomic script is impossible. The order is:

1. Count the request's other limits, and apply its releases.
2. If those deny the request, acquire nothing: read the slots in flight, for the answer.
3. Otherwise try every pending slot, in one pipeline.
4. If a slot is refused and that denies the request, remove the slots just acquired (`ZREM`, by their identifiers). The same if the pipeline of step 3 fails, since that request gets an error and no amendment.

Between steps 3 and 4 a compensated slot can refuse another request, which is a false refusal, never an excess. "Acquire then compensate" alone would hold a slot for every request a session denies, the most common denial, so it is only used across several `concurrency` limits.

### Interactions

| Setting | Behaviour |
|---|---|
| `shadow_mode`, global shadow mode | Acquires past the limit and never refuses: `in_flight` can exceed `max`, and the excess counts in `shadow_mode`. |
| `quota_mode` | Refused at load, and `QUOTA_MODE` leaves the limit out of quota groups: a concurrency limit is not a budget a model can fall back from. |
| `replaces` | As any list limit: a replaced limit neither acquires nor releases. |
| Other limits in the list | Refused at load: an amendment's tokens would release that many slots. |
| `overflow` | `envoy` is accepted and has no effect; nothing is counted past a refusal. |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` | No effect, like D20. |
| `REDIS_PERSECOND` | Always on the main Redis, whatever the lease. |
| Memcached | Refused at load (D24). |
| The local cache | Never holds it (D18). |
| A client-sent `limit` | Replaces the window, as for any limit: do not send one on this descriptor. |

### Stats

Counters, not a gauge: `slots_acquired`, `slots_released`, `slots_expired`, `releases_without_slot`. A stats key names the descriptor's config, not the user's value, so a gauge would be the last user seen. Counters sum across instances and users: `acquired − released − expired` is what is in flight. `slots_expired` shows lost amendments; `releases_without_slot` shows a client not following this protocol, or a request that outlived its lease.

`within_limit`, `over_limit` and `total_hits` count one per slot asked for: their hits are 0, which would make refusals invisible.

## Consequences

- **The misuse guard.** This meaning of `hits` only holds on a dedicated descriptor with `type: tokens` and `cost: "1"`. A request with hits on an empty window releases nothing and counts in `releases_without_slot`.
- **`failOpen`.** When the check fails and the request goes on, the amendment releases a slot the request never acquired, possibly another request's. Admission can then exceed `max` for a while. `failClosed` does not have this, at the price of denying while the server is down.
- **A request outliving its lease** admits up to two requests too many, and its amendment counts in `releases_without_slot` if nothing is held. With a lease at the p99, that is about 1% of requests, an accepted cost: the limit is a safeguard.
- **`slots_expired` undercounts:** an orphan whose user goes idle disappears with the key's TTL, and no script sees it.
- **Orphans last up to the lease**, for the cases listed in Context, until agentgateway sends the amendment on those paths too (`docs/upstream/agentgateway-amend-on-drop.md`). A pod killed mid-request or a failed amendment call still needs the lease afterwards.
- **A denied request may still open a session:** the session's script runs before the slot is tried (D9, `opened_without_hits`).

## Rejected alternatives

- **One script over every limit of the request:** the limits live in different cluster slots.
- **A unique request identifier in a descriptor entry.** agentgateway's CEL has `uuid()` (`celx/general.rs:39`), and amendments reuse the check's entries, so it would reach both. But it would need an entry that is left out of the key, and it does not help with orphans, since they are the slots no amendment comes for. It only stops a `failOpen` release from taking another request's slot.
- **A gauge of slots in flight:** see Stats.
- **A renewable lease:** nothing in the protocol is sent during a request.
- **A separate field for the maximum:** `requests_per_unit` is what `rate_limits:` entries already have, and what `current_limit` reports.
