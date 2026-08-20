# POC Scope — Temporal Baseline Workflow (Sink Layer Control Plane)

Status: agreed 2026-08-12
Source docs: `RFC_Sink_Layer_Control_Plane_Architecture.md`, `TICKET-Create_a_testable_workflow_baseline.txt`

## Purpose

Prove the durable-execution patterns the RFC depends on, on Temporal, locally,
with zero Flink and zero Kubernetes, while retaining a bare-Python reference
implementation for behavioral comparison. The POC is a **lean experiment** —
but the boundary between workflow logic and cluster interaction is kept clean,
so that going to production means replacing one module, not rewriting the saga.

## Decisions

| Question | Decision |
|---|---|
| Engine | Bare-Python baseline plus Temporal, local (`temporal server start-dev`) |
| Saga | **Move Datatype Between Jobs** only — it is the superset of Batch Pause / Batch Resume |
| Infra | Fully mocked. Files in / files out. **Instant state transitions**, no polling, no operator simulation |
| Actor | **Kept.** `FlinkJobFamilyActor` as a real entity workflow — it is one of the things being proven |
| Rigor | Lean runtime scope with typed Pydantic models, strict mypy across `poc/` and `tests/`, and one non-mutating `make check` quality gate |
| Out of scope | Ingestion Config Portal, reconciliation loop, Operational Web API, Restate/DBOS implementations |

## What must be proven

1. **Fail-fast saga compensation** — all 4 RFC scenarios, asserted on the *order of side effects*, not just final state.
2. **Retry semantics** — transient faults retry with backoff; permanent faults abort immediately without burning the retry budget.
3. **Replay determinism** — saved event histories replayed against current code (named explicitly in the ticket).
4. **Worker crash durability** — SIGKILL mid-saga, restart, resume from last completed step with no duplicated side effects.
5. **Actor pattern** — strict single-writer-per-`job_family`; concurrent commands queue rather than interleave.
6. **Ambiguous outcomes** — rollback restores the pre-saga runtime and config
   snapshot even when a mutating operation lands without returning a response.

## The mock cluster

The "cluster" is a directory. One file per job family, one append-only audit log,
one fault-injection file.

```
.cluster/
  families/family_a.json     {"state":"RUNNING","datatypes":["clicks"],"generation":3}
  families/family_b.json     {"state":"RUNNING","datatypes":["views"],"generation":7}
  savepoints/family_a-sp-0001.json
  audit.jsonl                append-only, one line per activity call
  chaos.json                 fault injection rules
```

Every mutating activity: read JSON → mutate → write JSON → append to
`audit.jsonl` → return. No delays.

**`audit.jsonl` is the primary assertion surface.** Compensation correctness is
"the sequence of side effects was exactly this", which is far stronger than checking
final state — a saga that rolls back in the wrong order still ends in the right place.

**`chaos.json`** is the ticket's "explicit toggles/hooks". Keyed by
`(family, activity)`, consumed per attempt so retry behaviour is testable:

```json
{
  "family_b:patch_configmap": { "mode": "transient", "times": 2 },
  "family_a:resume_job":      { "mode": "permanent" }
}
```

`transient` → retryable error, `times` occurrences then success.
`permanent` → non-retryable `ApplicationError`.
`lost_response` → commit and audit the mutation, then raise a retryable error,
`times` occurrences before a response succeeds.

## Components

**Activities** — thin, idempotent, the only code that touches the mock:
`trigger_savepoint`, `suspend_job`, `resume_job`, `patch_configmap`, and
`read_status`. Rollback never derives its payload from a mutating call's return
value.

**`FlinkJobFamilyActor`** — long-lived entity workflow, `workflow_id = "family:<name>"`.
Handlers `pause` / `resume` / `patch_config` / `restore` exposed as Temporal **updates**
(request/response, unlike signals), each serialized behind an in-workflow lock. That
lock is what reproduces the Virtual Object single-writer guarantee Temporal lacks
natively — and makes the comparison against Restate/DBOS concrete. State lives in the
workflow; no external database. Normal commands use cached runtime state. The
compensation-only `restore` update re-reads cluster state under the actor lock so
it can repair a mutation whose response was lost.

**`MoveDatatypeWorkflow`** — the saga. Phase 1 pause all → Phase 2 patch config all →
Phase 3 resume all. Before every forward call, the workflow pushes a restore
intent built from the complete pre-saga snapshot. Any failure unwinds the stack
LIFO, reconciling each intent against fresh cluster state, and aborts.

> The LIFO stack reproduces the RFC's scenario-4 compensation exactly (modulo ordering
> *within* a phase, which is arbitrary). Worth noting because it means the layered
> rollback needs no special-casing — it falls out of the structure.

## Test plan

| Test | Proves | Environment |
|---|---|---|
| Happy path | Baseline | time-skipping |
| Fail in pause | Compensation scenario 2 | time-skipping |
| Fail in update | Compensation scenario 3 | time-skipping |
| Fail in resume | Compensation scenario 4 (layered) | time-skipping |
| Transient × 2 then ok | Retry + backoff | time-skipping |
| Permanent fault | No retry, immediate compensation | time-skipping |
| Lost suspend response | Stale actor cache is refreshed and both families restored | time-skipping |
| Lost config-patch response | Original snapshot config wins over retry results | time-skipping |
| Concurrent commands, one family | Actor serialization | time-skipping |
| History fixture replay | Determinism | `Replayer`, no server |
| SIGKILL mid-saga | Crash durability, no duplicate side effects | real dev server |

Every failure test asserts against `audit.jsonl`, and asserts observable runtime
state and datatype routing equal their pre-saga snapshot. Generation counters and
savepoint artifacts are intentionally irreversible.

## Layout

```
poc/
  cli.py                     shared scenario runner and verdict formatter
  common/
    domain.py                models, states, error taxonomy, saga verdicts
    cluster.py               file-backed mock: audit + chaos
    scenarios.py             shared scenario matrix
  baseline/
    actor.py                 synchronous FamilyActor
    saga.py                  bare-Python MoveDatatypeWorkflow
  temporal/
    activities.py
    actor.py                 FlinkJobFamilyActor
    actor_proxy.py
    saga.py                  durable MoveDatatypeWorkflow
    worker.py
tests/
  baseline/                  baseline parity tests
  temporal/
    histories/               replay fixtures
    worker_process.py        crash-test worker entrypoint
README.md         how to run each success/failure mode
Makefile
pyproject.toml    uv, Python 3.13, runtime and development dependencies, tool configuration
```

## Assumption to confirm

"Move datatype" is modelled as: datatype `D` moves from `family_a` to `family_b`, so
Phase 2 patches *both* configs — removing `D` from A and adding it to B. Both families
must be paused first, and a partial update leaves routing broken, which is precisely
why it needs all-or-nothing semantics.
