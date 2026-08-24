---
name: develop-control-plane-sagas
description: Develop safe Flink control-plane saga behavior across the bare-Python baseline, Temporal, and Restate. Use when changing a saga, actor, durable step, mutation, retry, failure, compensation, recovery scenario, audit order, concurrency behavior or replay behavior in `poc/` or `tests/`.
---

# Develop Control Plane Sagas

Protect the shared saga contract while keeping every execution-engine adapter independently readable.

## Classify the change

1. Read the root `AGENTS.md`, `POC_SCOPE.md`, and the affected shared and engine-specific code and tests.
2. Classify the change before editing:
   - **Shared-contract:** externally observable phase order, snapshots, mutation semantics, failure taxonomy, verdicts, retries, compensation, audit evidence, or scenario definitions.
   - **Engine-specific:** runtime registration, client or worker integration, serialization mechanics, engine-native retry configuration, replay wiring, or crash harnesses that do not change the shared behavior.
3. For shared-contract work, implement and test behavior parity across the bare-Python baseline, Temporal, and Restate. If an engine is absent or blocked, report the exact gap; do not silently call the work complete.
4. For engine-specific work, preserve the shared contract and make the implementation understandable without reading another engine first.

## Preserve the contract

- Keep the global phase barriers: snapshot all, pause all, patch all, resume all.
- Use authoritative pre-saga snapshots for restore intent. Register compensation before the forward mutation and unwind in last-in-first-out order.
- Continue restoring after an individual restore failure. Surface any failed restore as `COMPENSATION_INCOMPLETE` with structured errors and full audit evidence.
- Bound I/O timeouts, retry counts, and backoff. Distinguish typed transient failures from permanent failures.
- Give retried mutations a stable operation identity or make them idempotent. Durable engines must recover from a crash without duplicated side effects.
- Use Temporal and Restate durability primitives. Do not add a durability claim to the bare-Python baseline.
- Treat actor or virtual-object serialization as resource-local only. Do not claim it provides saga-level ownership, leases, fencing, takeover, or stale-command rejection.
- Do not add a reconciliation or `RecoveryPlan` API unless that design has been explicitly requested and resolved.

## Select tests by behavior

Always run the fast shared suite. Add or update focused evidence according to the changed behavior:

| Changed behavior | Required evidence |
| --- | --- |
| Phase sequencing or compensation order | Exact audit-order assertions in every affected engine |
| Mutation payload or restore behavior | Pre-saga snapshot restoration and lost-response tests |
| Timeout, retry, or failure taxonomy | Transient success, exhausted retry, permanent failure, bounded-backoff, and stable-operation-identity tests |
| Temporal workflow shape | Replay fixtures and replay tests |
| Actor serialization or resource coordination | Concurrent-command tests; identify saga-level ownership as unresolved unless separately designed |
| Durable step or crash handling | Temporal and Restate crash tests proving resume without duplicate side effects |
| Shared scenario or verdict | Baseline, Temporal, and Restate parity assertions |

## Validate and report

1. Run `make check`.
2. Run the affected durable-engine crash targets when the change touches durability, retries, mutation identity, or engine integration.
3. Run `git diff --check`.
4. Report exact commands, pass/fail results, skipped checks, and the files that contain the evidence. Never substitute final-state assertions for required audit-order evidence.
