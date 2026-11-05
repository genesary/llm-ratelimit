# Remote rate limit: the token amendment is lost when no upstream response is processed

Text for an issue on [agentgateway/agentgateway](https://github.com/agentgateway/agentgateway). Checked against commit `930a32f`, and against release v1.7.0 by a test (`a_real_agentgateway_takes_and_releases_slots`).

## Summary

A `type: tokens` descriptor of a remote rate limit sends a check before the LLM call and an amendment after it. The amendment is only sent when `process_response` runs, or when a started stream is dropped. On every other path, `LLMResponseAmend` is dropped unsent. A rate limit service holding state between the two calls never hears that the request ended.

## Where the amendment is lost

`LLMResponseAmend` and `LLMResponsePolicies` have no `Drop` implementation, unlike `AmendOnDrop`, which only exists once streaming has started.

| Path | Location | Amendment |
|---|---|---|
| A response, 4xx and 5xx included | `llm/mod.rs`, `process_response` | Sent |
| A stream dropped after it started | `llm/mod.rs`, `AmendOnDrop` | Sent |
| A transport error before the response headers | `proxy/httpproxy.rs`, `resp?` after `upstream.call` | **Lost** |
| `requestTimeout` or `backendRequestTimeout` before the headers | `proxy/httpproxy.rs`, `timeout_at` in `attempt_upstream` | **Lost** |
| A client disconnecting before the headers | The request future is dropped | **Lost** |
| Failing to read a non-streamed body | `llm/mod.rs`, `buffer_response(resp).await?` | **Lost** |
| A response prompt guard's early return | `llm/mod.rs`, `process_chat_or_detect_buffered_response` | **Lost** |
| A failure between the check and the call, such as late backend auth or building the transport | `proxy/httpproxy.rs`, `make_backend_call` | **Lost** |

## Why it matters

Token accounting only loses a little: most of these requests used few tokens.

It matters for any service that treats the check and the amendment as a pair. We use them to limit requests in flight, with a descriptor of `cost: "1"`: the check takes a slot and the amendment releases it. Each lost amendment holds a slot until a server-side lease ends, which can lock a user out of a model. This gets common with a queue in front of the model, such as an inference scheduler's flow control: a request cancelled in the queue has no response headers yet.

## Proposal

Send the amendment when the policies are dropped unsent, with a guard like `AmendOnDrop` covering the whole request, from a successful check on:

- **Disarmed on denial.** A denied check must not amend, as today.
- **Sent once.** `amend_tokens` consumes the guard, so a path that amends normally does not send a second one.
- **A runtime to spawn on.** `Drop` may run outside a Tokio context, as `tokio::runtime::Handle::try_current` would tell.
- **Expressions needing a response.** With no response, `llm.totalTokens` fails to evaluate, and the descriptor is skipped, which is correct: nothing is known to charge. A constant such as `cost: "1"` still goes out.

A pod killed mid-request, or a failed amendment call, still loses the amendment, so a service keeping state needs its own expiry anyway. The proposal only makes that the rare case.

## Related

- With `failureMode: failOpen`, an amendment follows a check that failed (`http/remoteratelimit.rs`, `check_llm`). The server gets a release for something it never granted. A flag on the guard could skip it, if that seems right to you.
- Each retry attempt checks again (`make_backend_call` runs per attempt), so an attempt failing on a transport error loses one amendment per attempt.
