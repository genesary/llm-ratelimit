# ADR 0001 — Storage keys of `rate_limits:` entries

- **Status:** accepted (2026-10-16)
- **Sources:** `docs/design.md` D6, D25, D26; reference `envoyproxy/ratelimit@f4e7669` `src/limiter/cache_key.go:66-94`

## Context

Legacy limits (`rate_limit:`) must keep upstream's key **byte for byte** so the old and new services can share a Redis during a migration: `CACHE_KEY_PREFIX + domain + "_" + (key "_" value "_")* + bucketStart`.

Limits in the new `rate_limits:` list have other needs:

- all-or-nothing (`overflow: strict`) and multi-key Lua scripts ⇒ all of a descriptor's keys in **the same** Redis Cluster slot;
- several limits per descriptor ⇒ the key must identify the limit;
- window types with different Redis structures (counter, hash of sub-buckets, TAT) ⇒ changing type must never read an old value (`WRONGTYPE`);
- keys readable by an operator (`redis-cli --scan`).

This format is frozen before the first release: any later change is a major version with a documented reset.

## Decision

### General form

```
{<prefix><domain>_<key>_<value>_…}_<shape>[_<bucketStart>]_<name>
```

1. **Hash tag `{…}`**: the descriptor part, built like the legacy key (prefix, domain, `key_value_` pairs, the `share_threshold` pattern replacing the value), without `bucketStart`. It alone determines the slot, so all of a descriptor's limits and windows share the slot.
2. **Escaping inside `{…}`**: `%` → `%25`, `{` → `%7B`, `}` → `%7D`, applied to the prefix, domain, keys and values. Since the escape character is itself escaped, splitting is unambiguous: the descriptor part ends at the first unescaped `}`. `_` is **not** escaped, as upstream (the legacy collision `a_b`/`c` ↔ `a`/`b_c` is reproduced, not fixed).
3. **`<shape>`**: `[a-z]{2}[a-z0-9]*`, a readable encoding of the window's identity: type, duration in seconds, then depending on the type rounding, `buckets`, `offset`, opening mode. Examples: `fx604800`, `fx604800o345600`, `fx604800oh3600` (hashed offset, 1 h rounding), `an18000r3600` (anchored, its start rounded to the hour, opened by a check), `an3600f` (opened by the first hit), `sl3600b12`, `tb60`. `requests_per_unit`, `burst` and `max_debt` are **not** part of it: changing them keeps the counters.
4. **`<bucketStart>`**: for `fixed` only (start of the window, offset included). Other types have a stable key whose state holds the time.
5. **`<name>`**: required in `rate_limits:`, always **last** because it may contain `_`. Character set `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` (forbidden: `:` `|` space `.` `~` `*` `=`, because of the statsd format and the `~` metric separator).

### Related rules

- **Identity of a limit** = type + duration + rounding + opening mode + `buckets` + `offset` + name. Changing any of them starts from zero; renaming a limit therefore resets its counters (an explicit act).
- **Name uniqueness**: within one descriptor's list only. The same name on two descriptors is legal and forms a replacement group (`replaces`), as upstream. A limit cannot `replaces` a name in its own list.
- **TTL**: every key has one (window duration + margin), `anchored` anchors included: an anchored key, a hash of its anchor `a` and count `c`, expires 60 s after its window ends.
- **Memcached** (`fixed` only): the full key must be ≤ 250 bytes and contain no byte ≤ `0x20` or `0x7f`; beyond that, upstream behaviour (error logged, `OK` answered). Warning at load if the static part exceeds 250 − 64 bytes.
- **A client-sent `limit` with a `unit_multiplier`** (D23): the same form with the shape `fx<seconds>` and an empty name, since it has none: `{d_user_alice_}_fx7200_999993600_`.
- **`REDIS_PERSECOND`**: a list with a window shorter than a minute is routed **entirely** to the per-second Redis client, so its keys stay together (upstream never has two limits per descriptor).

## Consequences

- Moving a limit from `rate_limit:` to `rate_limits:` resets its counter **once** (different key). Documented in the migration guide.
- A heavily used descriptor puts all its limits on one cluster node, like a legacy key today.
- Keys stay readable and sortable by shape and name, which helps debugging in production.

## Rejected alternatives

- **Reusing the legacy key for a `fixed` entry in a list**: keeps the counters during a migration, but the legacy key has no hash tag and changes slot every window; mixed with other limits, the list spans several slots and all-or-nothing is impossible in Cluster.
- **Hashing the shape**: opaque keys, impossible debugging, risk of putting the hash on the wrong side of the hash tag.
- **Optional name, added only to tell two same-shape limits apart**: adding a second limit changed the first one's key and silently reset its counters.
- **Auto-generated name (`fixed-7d`)**: the key and the metric would change on every duration tweak, and `replaces` would target an implicit string.
