# Remote rate limit: nothing can read what a token check returns

Text for an issue on [agentgateway/agentgateway](https://github.com/agentgateway/agentgateway). Measured on release v1.7.0.

## Summary

A remote rate limit's `requestHeadersToAdd` are set on the request. For a `requests` descriptor this happens in the route's policies, before `transformations`, so a transformation can read them. For a `tokens` descriptor it happens later, in `make_backend_call` (`check_llm`), after the route's, the backend's and the Gateway's transformations. No transformation can read them.

## What we measured

A service returning a request header, a client sending the same header, and a transformation copying it to another:

| Descriptor | Transformation on | The transformation reads | Upstream receives |
|---|---|---|---|
| `tokens` | route, backend or Gateway | the client's value | the service's value |
| `requests` | route or backend | the service's value | the service's value |

## Why it matters

A service can answer with a tier, for a transformation to turn into a routing decision, such as a scheduler's objective name built from `llm.requestModel`. With a `tokens` descriptor, which is the one that gets the token amendment, the transformation reads the client's header instead, which a client can forge. Nothing warns of it.

## Workarounds today

- A second descriptor of `type: requests` and `cost: "0"` on the same entries, which reads the state early, at the price of a call per request.
- Have the service return the final value itself, which needs a literal per model.

## Proposal

Either of:

1. A header transformation stage after the LLM request policies, reading headers and `llm.*`. The name is yours; `finalTransformations` already means body fields.
2. Run `check_llm` before the route's `transformations`, if nothing depends on the order. The token count may not allow it.

At the least, the order deserves a line in the docs of `requestHeadersToAdd` and `transformations`.
