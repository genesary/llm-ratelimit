## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues for genesary/llm-ratelimit (via `gh`). See `docs/agents/issue-tracker.md`.

### Triage labels

Uses the five default triage labels (needs-triage, needs-info, ready-for-agent, ready-for-human, wontfix). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.

## Git conventions

- Commit messages and PR titles follow [Conventional Commits](https://www.conventionalcommits.org/): `<type>(<optional scope>): <description>`, with `type` one of `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, `test`. Checked on every PR by `.github/workflows/conventional-commits.yml`.
- `main` has a linear history: PRs are merged by squash (the PR title becomes the commit subject) or rebase (each commit lands as-is, so each must follow the convention). Merge commits are disabled.
- Commits and PRs carry no AI attribution (no `Co-Authored-By` trailer, no "Generated with" line).
- Everything written into the repo or onto GitHub is in English: commit messages, PR titles and bodies (a PR body becomes the squash commit body), docs, ADRs, code comments. Conversation with the maintainer may be in French.

## Working agreements

The goal and every design decision are in `docs/design.md`; what is already proven against upstream is in `docs/parity.md`.

- **Scope**: create the files the maintainer asks for; propose anything else in one line and wait.
- **Recommendations**: before the maintainer chooses between options, have each recommendation challenged by a subagent against evidence (upstream source, docs) and report what changed.

### Building a slice

1. Agree the slice with the maintainer: the next ⬜/🚧 rows of `docs/parity.md`, in the order of "Build order" in `docs/design.md`. Done when the maintainer confirms the scope.
2. Agree the test seams (`tdd` skill). Done when the maintainer confirms them.
3. Red → green, one behaviour per cycle. Behaviour upstream has is proven by a differential scenario against `tools/oracle` (envoyproxy/ratelimit at the commit pinned in `tools/oracle/go.mod`). Done when every in-scope behaviour has a test that goes red without its code; mutation-check the subtle ones.
4. `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo deny check`, and the full suite with differential tests required, on Redis and on Valkey.
5. Commit on a branch, then run the `code-review` skill (Standards, and Spec against the upstream Go code). Fix the findings, or put them to the maintainer.
6. Mark the `docs/parity.md` rows the slice proves, push, open the PR (`pr` skill), and wait for the required checks: `check`, `rust (redis)`, `rust (valkey)`. The maintainer merges.

### Running tests

- `RATELIMIT_REQUIRE_DIFFERENTIAL=1 cargo test --workspace` needs `go`, `redis-server` and `memcached` on the PATH (`RATELIMIT_MEMCACHED_SERVER=<path>` for another memcached). Without the variable, differential tests are skipped when a tool is missing.
- `RATELIMIT_REDIS_SERVER=<path to valkey-server>` runs them against Valkey (built from source if it conflicts with Homebrew's `redis`).
