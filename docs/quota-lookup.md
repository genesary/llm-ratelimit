# The quota lookup

`POST /quota` takes a snapshot of every limit that applies to some descriptors: what each has used and when it resets. It changes nothing:

- it counts no hits;
- it opens or closes no window;
- it writes nothing to Redis or Memcached;
- it adds nothing to the rate-limit stats.

It is meant for showing users their quota, for example "Session 40 % · resets in 2 h 05 · Week 62 % · resets Monday 10:00". It reads exactly what the next decision would at the same instant, with the same rules, keys and phases. Why it exists and what was rejected is in [ADR 0003](adr/0003-quota-lookup.md).

## Request

`POST` on the HTTP port (`PORT`, 8080 by default), next to `/json`. The body is the same JSON `RateLimitRequest` as `/json`'s:

```sh
curl -s localhost:8080/quota -d '{
  "domain": "llm",
  "descriptors": [{"entries": [{"key": "user", "value": "alice"}]}]
}'
```

- **Same descriptors:** send the ones your real requests send. `replaces` works across the request's descriptors, so another set can show limits the real requests do not have, or hide ones they do.
- **Ignored fields:** `hits_addend` and `is_negative_hits`.
- **A descriptor's `limit`:** a client-sent limit replaces the configured ones, as in a decision.
- **Authentication:** none. The route is for internal callers, so expose it to users through your gateway, behind its authentication, with the descriptors built from the caller's identity ([agentgateway guide](examples/agentgateway.md)).

## Response

`200`, one entry per descriptor in request order, each with its limits in configuration order:

```json
{
  "now": "2026-10-28T14:00:00Z",
  "descriptors": [
    {
      "limits": [
        {
          "name": "session",
          "window": {"type": "anchored", "duration_s": 18000},
          "limit": 4000000, "used": 1600000, "remaining": 2400000, "used_pct": 40.0,
          "open": true,
          "resets_at": "2026-10-28T16:05:00Z", "resets_in_s": 7500,
          "shadow_mode": false, "quota_mode": false
        },
        {
          "name": "weekly",
          "window": {"type": "fixed", "duration_s": 604800},
          "limit": 40000000, "used": 24800000, "remaining": 15200000, "used_pct": 62.0,
          "resets_at": "2026-11-02T10:00:00Z", "resets_in_s": 417600,
          "shadow_mode": false, "quota_mode": false
        }
      ]
    }
  ]
}
```

The keys come out in alphabetical order; the example is reordered for reading.

| Field | Meaning |
|---|---|
| `now` | The instant of the snapshot, in UTC (RFC 3339, whole seconds). |
| `name` | The limit's name. It is empty for a `rate_limit:` without one. |
| `window` | `{"type": "fixed" \| "anchored", "duration_s": …}`, `{"type": "concurrency", "lease_s": …, "retry_after_s": …}`, `{"type": "token_bucket", "duration_s": …, "burst": …}`, or, for a `rate_limit:`, `{"type": "unit", "unit": "HOUR"}`. |
| `limit` | `requests_per_unit`. |
| `used` | The hits counted in the current window. It can exceed `limit`, since denied hits are counted too (`overflow: envoy`). |
| `remaining` | `limit − used`, never below 0. |
| `used_pct` | `used / limit × 100`, to one decimal, and not capped (`103.5`). A limit of 0 shows 100 once used, else 0. |
| `resets_at`, `resets_in_s` | When the current window ends, and how long from `now`. |
| `open` | For anchored windows only: whether a session is open. |
| `level` | For token buckets only: the whole tokens in the bucket, negative in debt. `limit` is then the capacity, `used` is `limit − level`, and `resets_in_s` the time until it is full, 0 when it is. |
| `in_flight` | For concurrency windows only: the slots held, leaving out those past their lease. `used` and `used_pct` count them too. |
| `shadow_mode`, `quota_mode` | The descriptor's flags. The figures are the same either way. |

The special cases:

- **A closed session:** an `anchored` limit with no open session shows `used: 0`, `remaining: limit`, `open: false`, and `resets_at` and `resets_in_s` as `null`. Nothing is counting until the next request opens one.
- **A concurrency window** has no reset: `resets_at` and `resets_in_s` are `null`.
- **An `unlimited` limit** is `{"name": …, "unlimited": true}`, with no figures.
- **A descriptor that no rule matches** gives `"limits": []`.
- **Limits removed by `replaces`** are left out.

The format is a contract: fields may be added, but none will be renamed or removed before a major version.

## Errors

The body is `{"error": "…"}`:

| Status | When |
|---|---|
| `400` | The body is not a valid request, has no domain or no descriptors, or no configuration has loaded yet. The messages are `/json`'s. |
| `404` | The domain is unknown: `unknown domain 'x'`. |
| `405` | Another method than `POST`, with `Allow: POST`. |
| `500` | Redis or Memcached failed. On Memcached, a failed read is an error rather than zeros: the lookup cannot report what it did not read. |

## Observability

The lookup has its own stats, separate from the rate-limit ones:

| Stat | Prometheus (shipped mapper) |
|---|---|
| `ratelimit.quota_lookup.total_requests` | `ratelimit_quota_lookup_total_requests` |
| `ratelimit.quota_lookup.error.<cause>`, the cause `invalid_request`, `unknown_domain` or `backend` | `ratelimit_quota_lookup_errors{cause="…"}` |
| `ratelimit.quota_lookup.response_time` (ms) | `ratelimit_quota_lookup_response_time_seconds` |

## Good to know

- **Cost:** a lookup costs Redis as much as a decision, a read per limit. Poll at human pace, around once a minute, not on every keystroke.
- **The local cache:** with `LOCAL_CACHE_SIZE_IN_BYTES` set, a decision can deny from the local cache a key that Redis has lost (a flush, an eviction). The lookup reads Redis only, so for the cache entry's lifetime it can show such a limit as available while decisions deny it. The hits the cache answers are not counted either, so that limit's `used` stops growing. `anchored` windows are never cached.
