# Repository guardrails

These instructions apply to the whole repository. Keep them concise; add a nested `AGENTS.md` only when a subtree needs stricter rules.

## Authorization and reporting

- Treat review, explanation, and diagnosis as read-only. Implement only when the user requests a change, and keep mutations within that request.
- Before finishing, report the exact files changed, commands run, failures encountered, and validation evidence. Do not imply that an unrun check passed.

## Engineering rules

- Prefer explicit control flow, descriptive domain names, and small typed functions. Keep the baseline, Temporal, and Restate implementations understandable on their own.
- Use Python 3.13 or newer. `pyproject.toml` is authoritative for the supported version and tool configuration.
- Keep strict mypy clean across `poc/` and `tests/`. Do not leave unresolved `Any` at shared contracts, serialized models, workflow/activity boundaries, or other durable boundaries. Do not use broad ignores to make the gate pass.
- Established acronyms such as API are acceptable. Reject ambiguous shorthand, and expand uncommon acronyms on first use in prose.
- Do not add emojis to repository artifacts or generated user-facing output.

## Secret, input, and durable-I/O safety

- Never commit, print, or place secrets in workflow payloads or audit records. Validate untrusted external input at the boundary before issuing mutations.
- Bound external I/O with explicit timeouts. Bound retry attempts and backoff, distinguish typed transient failures from permanent failures, and make retried mutations idempotent or give them a stable operation identity.
- Preserve complete audit evidence for every attempted forward and restore operation, including typed failure details.

## Saga invariants

- Preserve the global barriers: snapshot all participants, then pause all, then patch all, then resume all. Do not run the full sequence one family at a time.
- Use engine-native durability in Temporal and Restate. Keep the bare-Python baseline intentionally non-durable and use it as a behavior reference.
- Prove crash recovery does not duplicate side effects whenever a durable step or retried mutation changes.
- Register restore intent from the authoritative pre-saga snapshot, restore in last-in-first-out order, and continue unwinding after an individual restore fails.
- If any restore fails, return `COMPENSATION_INCOMPLETE` with structured errors and complete audit evidence.
- Actor or virtual-object serialization is per resource; it does not prove saga-level ownership or fencing. Do not claim overlapping workflow safety until issue #28 is resolved.
- Incomplete compensation has no reconciliation API yet. Do not invent one in an unrelated change; issue #27 owns the durable `RecoveryPlan` and reconciliation workflow design.

## Skill routing

- For non-trivial project decisions, use Wayfinder and `grilling`; use `domain-modeling` when terminology or architecture decisions change. Follow selected skill instructions when they are more specific than these general rules.
- Use `develop-control-plane-sagas` for changes to sagas, actors, durable steps, mutations, retries, failures, compensation, recovery scenarios, audit ordering, concurrency, or crash behavior.

## Validation

- Run `make check` for every code or test change. It must perform Ruff lint, Ruff format checking, strict mypy, and the fast test suite without rewriting files.
- Run the affected Temporal or Restate crash suite manually for durability, operation-identity, or engine-integration changes. Bare Python has no crash-durability claim.
- Keep audit-order, snapshot-restoration, retry, replay, engine-parity, and crash tests aligned with the behavior changed.
- Run `git diff --check` before handoff.

## Known limitations

- Preferred conversational voice is intentionally deferred to issue #26.
- Saga-level ownership, leases, takeover, fencing, and stale-command rejection are deferred to issue #28.
- Reconciliation after incomplete compensation and the durable `RecoveryPlan` are deferred to issue #27.
