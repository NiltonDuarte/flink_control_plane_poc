# POC Scope — Temporal Baseline Workflow (Sink Layer Control Plane)

Status: agreed 2026-08-12
Source docs: `RFC_Sink_Layer_Control_Plane_Architecture.md`, `TICKET-Create_a_testable_workflow_baseline.txt`

## Purpose

Prove the durable-execution patterns the RFC depends on, on Temporal, locally, with
zero Flink and zero Kubernetes. The POC is a **lean experiment** — but the boundary
between workflow logic and cluster interaction is kept clean, so that going to
production means replacing one module, not rewriting the saga.

## Decisions

| Question | Decision |
|---|---|
| Engine | Temporal, local (`temporal server start-dev`) |
| Saga | **Move Datatype Between Jobs** only — it is the superset of Batch Pause / Batch Resume |
| Infra | Fully mocked. Files in / files out. **Instant state transitions**, no polling, no operator simulation |
| Actor | **Kept.** `FlinkJobFamilyActor` as a real entity workflow — it is one of the things being proven |
| Rigor | Lean. Typed and Pydantic-modelled because it costs nothing, but no `mypy --strict` gate, no packaging ceremony |
| Out of scope | Ingestion Config Portal, reconciliation loop, Operational Web API, Restate/DBOS implementations |

## What must be proven

1. **Fail-fast saga compensation** — all 4 RFC scenarios, asserted on the *order of side effects*, not just final state.
2. **Retry semantics** — transient faults retry with backoff; permanent faults abort immediately without burning the retry budget.
3. **Replay determinism** — saved event histories replayed against current code (named explicitly in the ticket).
4. **Worker crash durability** — SIGKILL mid-saga, restart, resume from last completed step with no duplicated side effects.
5. **Actor pattern** — strict single-writer-per-`job_family`; concurrent commands queue rather than interleave.

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

Every activity: read JSON → mutate → write JSON → append to `audit.jsonl` → return. No delays.

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

## Components

**Activities** — thin, idempotent, the only code that touches the mock:
`trigger_savepoint`, `suspend_job`, `resume_job`, `patch_configmap` (returns previous
config, for rollback).

**`FlinkJobFamilyActor`** — long-lived entity workflow, `workflow_id = "family:<name>"`.
Handlers `pause` / `resume` / `patch_config` exposed as Temporal **updates**
(request/response, unlike signals), each serialized behind an in-workflow lock. That
lock is what reproduces the Virtual Object single-writer guarantee Temporal lacks
natively — and makes the comparison against Restate/DBOS concrete. State lives in the
workflow; no external database.

**`MoveDatatypeWorkflow`** — the saga. Phase 1 pause all → Phase 2 patch config all →
Phase 3 resume all. Each successful step pushes its inverse onto a compensation stack;
any failure unwinds the stack LIFO and aborts.

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
| Concurrent commands, one family | Actor serialization | time-skipping |
| History fixture replay | Determinism | `Replayer`, no server |
| SIGKILL mid-saga | Crash durability, no duplicate side effects | real dev server |

Every failure test asserts against `audit.jsonl`, and asserts the cluster files end
byte-identical to their pre-saga state.

## The core / adapter split

> Added 2026-08-14, after the Temporal baseline was validated. Issue #10.

`TICKET_PoC-Restate.txt` asks for this same baseline on Restate, assessed the same
way. Building that as a second project would have forked the saga, the scenario
matrix, and the assertions — and a comparison between two engines is worth
nothing if they are running two different implementations that merely look alike.

So the saga moved into `poc/core/`, which is **engine-free by construction**, and
Temporal became one adapter over it. The core is synchronous: the saga and the
family handlers are generators that `yield` the operation they want performed and
are handed the result — or the failure — back. They cannot await, so they cannot
do I/O, cannot reach an engine, and cannot drift from the version the other
engine runs. Restate implements the same four obligations Temporal does:
execution, idempotency-key derivation, retry configuration, and the single-writer
mechanism for a job family.

**On the generator experiment.** The concern going in was that a
generator/driver would fight the workflow sandbox or the replay determinism
checks. It did not: every recorded history replays unchanged and every
audit-order assertion held without edit, because Temporal only requires that
workflow code decide deterministically what to schedule next. The costs are real
but small — results arriving through `yield` are untyped, and a failed step has
to re-enter the core through `throw()`, so the driver carries a second code path.
What it buys is that `poc/core/` structurally cannot perform I/O, and the whole
compensation matrix is assertable with no engine, no server and no worker
(`tests/test_core_saga.py`, milliseconds).

## Layout

```
poc/
  core/               no engine import, ever - enforced by a test
    domain.py         models, states, error taxonomy
    ports.py          ClusterPort / CommandPort + the values the core yields
    saga.py           move_datatype + compensation stack
    family.py         pause / resume / patch_config semantics
  cluster.py          file-backed mock: audit + chaos
  scenarios.py        the named scenarios, shared by CLI and tests
  adapters/
    driver.py         runs a core generator against an adapter
    temporal/         MoveDatatypeWorkflow, FlinkJobFamilyActor, proxy, activities
    restate/          stub - issue #11
  cli.py              trigger any scenario by name, on any engine
tests/
  test_core_*.py      the saga with no engine underneath it
  test_saga.py        the shared scenario matrix, parameterized by engine
  temporal/           update dedup, replay, worker-kill + replay fixtures
README.md             how to run each success/failure mode
Makefile
pyproject.toml        uv, Python 3.12, temporalio + pydantic + pytest
```

## Assumption to confirm

"Move datatype" is modelled as: datatype `D` moves from `family_a` to `family_b`, so
Phase 2 patches *both* configs — removing `D` from A and adding it to B. Both families
must be paused first, and a partial update leaves routing broken, which is precisely
why it needs all-or-nothing semantics.
