# llm-ratelimit

A Rust rewrite of envoyproxy/ratelimit: a global rate limit service that Envoy, Envoy Gateway or agentgateway asks whether a request may proceed, extended with several windows per descriptor for LLM quotas.

## Deciding

**Decision**:
The answer to a rate limit request: it counts the request's hits and says OK or OVER_LIMIT.
_Avoid_: rate limit check, enforcement call

**Check**:
A decision with no hits, sent before the work it guards.
_Avoid_: probe, ping

**Amendment**:
A decision sent after the work it guards, carrying the work's real cost.
_Avoid_: correction, post-hit

**Refund**:
A decision taking hits back (`is_negative_hits`).
_Avoid_: credit, negative hit

**Representative status**:
The one status a descriptor's decision answers when several limits apply to it.
_Avoid_: reduced status, main limit

## Limits and windows

**Limit**:
A number of hits allowed per window, for the requests matching a descriptor.
_Avoid_: rule, quota rule

**Window**:
The span of time a limit's hits are counted over.
_Avoid_: bucket (but for a token bucket), period

**Fixed window**:
A window of fixed length starting at the epoch, or at a phase.
_Avoid_: calendar window, tumbling window

**Phase**:
How long after the epoch a fixed window's windows start, constant or hashed from the descriptor.
_Avoid_: offset (the config key), alignment

**Anchored window**:
A window that a request opens and that lasts its duration from its anchor.
_Avoid_: rolling window, session window

**Anchor**:
When an anchored window opened, rounded down to the window's rounding.
_Avoid_: start time, opening time

**Token bucket**:
A window that refills continuously: a request is admitted while it holds any token, and charged in full.
_Avoid_: leaky bucket, rate window

**Deadline**:
When a token bucket is full again, which is all that is stored of it (GCRA's TAT).
_Avoid_: reset time

**Debt**:
How far below empty a token bucket is, within `max_debt`.
_Avoid_: overdraft

**Soft limit**:
A limit that demotes a request past it rather than denying it.
_Avoid_: quota warning, overage

**Demotion**:
A request's lower priority at the scheduler, from a soft limit's headers.
_Avoid_: throttling, downgrade

**Concurrency window**:
A limit on requests in flight rather than on hits over time: a check takes a slot and an amendment gives it back.
_Avoid_: in-flight limit, semaphore

**Slot**:
One request in flight in a concurrency window, until an amendment releases it or its lease ends.
_Avoid_: permit, token

**Lease**:
How long a slot lasts without an amendment.
_Avoid_: slot timeout, TTL

**Orphan**:
A slot whose request ended with no amendment: it lasts until its lease.
_Avoid_: leak, ghost slot

**Session**:
An open anchored window, as a user sees it: Claude's five-hour session.
_Avoid_: conversation

## Reading

**Quota lookup**:
A snapshot of every limit applying to some descriptors at an instant: what each has used and when it resets, with no effect on anything.
_Avoid_: quota check, dry run, status query
