# Security policy

## Supported versions

Only the latest release gets security fixes. Before 1.0, that is the latest `0.x` minor version.

| Version | Supported |
|---|---|
| 0.1.x | ✅ |

## Reporting a vulnerability

Do not open a public issue. Report it privately, in one of two ways:

- **GitHub's private vulnerability reporting** (preferred): [open a draft advisory](https://github.com/genesary/llm-ratelimit/security/advisories/new).
- **Email:** [dev@ayrick.fr](mailto:dev@ayrick.fr).

Include the version or image digest, the configuration and environment needed to reproduce, and the impact you see.

## What happens next

- **Within 7 days:** you get an acknowledgement, and an assessment of whether it is a vulnerability.
- **Within 90 days:** a fix is released, or the remaining plan is agreed with you. A GitHub security advisory is published with the fix, crediting you unless you prefer otherwise.

If the vulnerability is in envoyproxy/ratelimit's own behaviour, which this project reproduces, it is also reported upstream, in coordination with you.

## Scope

- **In scope:** the `llm-ratelimit` binary, its container image, and its release pipeline.
- **Out of scope:** Redis, Memcached and the proxies in front of the service are separate projects. Report their vulnerabilities to them, unless this service makes one exploitable.

Dependencies are checked against RustSec advisories in CI (`cargo deny`) and updated by Dependabot.
