# Flink Sink Layer Control Plane - Temporal POC

A working baseline workflow for evaluating durable execution engines, per
[`TICKET-Create_a_testable_workflow_baseline.txt`](TICKET-Create_a_testable_workflow_baseline.txt)
and the architecture in
[`RFC_Sink_Layer_Control_Plane_Architecture.md`](RFC_Sink_Layer_Control_Plane_Architecture.md).

This implements the **Move Datatype Between Jobs** saga on Temporal, against a
mock cluster. No Flink, no Kubernetes. Agreed scope is in [`POC_SCOPE.md`](POC_SCOPE.md).

## Quick start

```sh
uv sync
uv run pytest                          # 21 tests, ~45s
uv run python -m poc.cli list          # List scenarios
uv run python -m poc.cli run happy --engine None # Run Happy scenario with no engine
```

To watch a scenario run against a real Temporal server:

```sh
temporal server start-dev              # separate terminal
uv run python -m poc.cli run fail-in-resume
```

## Scenarios

Each is a seed cluster plus a set of fault-injection rules, defined once in
[`poc/common/scenarios.py`](poc/common/scenarios.py) and shared by both
implementations, their CLIs, and the tests.

| Scenario | What it exercises |
|---|---|
| `happy` | Success path |
| `fail-in-pause` | RFC compensation scenario 2 |
| `fail-in-update` | RFC compensation scenario 3 |
| `fail-in-resume` | RFC compensation scenario 4 - the layered rollback |
| `compensation-unreachable` | Compensation that cannot succeed |
| `transient-retry` | Retry with backoff, then success |
| `transient-exhausted` | Retry budget spent, then compensation |
| `lost-response-suspend` | Pause lands without a response; fresh-state rollback |
| `lost-response-patch` | Config patch lands without a response; snapshot rollback |
| `permanent-first-step` | Non-retryable failure, restore evaluates to a no-op |

## How it fits together

```
MoveDatatypeWorkflow  (saga: pause -> patch -> resume, LIFO compensation stack)
        |
        |  execute_family_command       <- activity proxy, see "Findings"
        v
FlinkJobFamilyActor   (entity workflow, one per job family, lock-serialized)
        |
        |  trigger_savepoint / suspend_job / resume_job / patch_configmap
        v
MockCluster           (a directory of JSON files)
```

`poc/common/cluster.py` is the only module that touches infrastructure. It sits
alongside the shared domain models and scenario matrix in the engine-neutral
`poc.common` package, which both `poc` and `poc_baseline` import. Swapping the
mock for real Flink Kubernetes Operator calls means reimplementing that file and
nothing else - it imports no Temporal and no workflow code.

### The mock cluster

```
<root>/
  families/<name>.json    state, datatypes, generation, savepoint_uri
  savepoints/             one file per triggered savepoint
  audit.jsonl             append-only, one line per attempted operation
  chaos.json              fault-injection rules
  chaos_hits.json         attempt counters
```

Transitions are instant - an operation reads JSON, mutates it, writes it back.
There is no desired/observed split and no polling, which is a deliberate scope
cut: this POC is about orchestration semantics, not Flink lifecycle fidelity.

`audit.jsonl` is the primary assertion surface, recording failed attempts as well
as successful ones. Asserting on the *sequence* of side effects is what catches a
rollback that compensates in the wrong order; a final-state check would wave that
through, since a wrong-order rollback still lands in the right place.

`chaos.json` is the ticket's "explicit toggles/hooks":

```json
{
  "family_b:patch_configmap": { "mode": "transient", "times": 2 },
  "family_a:resume_job":      { "mode": "permanent" }
}
```

`transient` and `permanent` fail before mutation. `lost_response` commits and
audits a mutating operation, then raises a retryable error. It models the
ambiguous outcome where the caller cannot tell whether the request landed.

## Findings

Five things this POC established that are worth carrying into the RFC.

### 1. Temporal cannot call an actor handler from a workflow

`workflow.ExternalWorkflowHandle` exposes only `signal` and `cancel` - there is no
`update`. A workflow cannot make a request/response call into another running
workflow. Restate and DBOS invoke a Virtual Object handler directly and get a
return value; on Temporal the saga needs an activity that holds a client and
calls the actor from outside the workflow sandbox ([`poc/actor_proxy.py`](poc/actor_proxy.py)).

That hop is the single biggest structural difference to expect when the same
baseline is built on the other two engines.

### 2. Update-id dedup is what makes crash recovery safe - and it must be scoped to the run

The proxy activity can be retried by a crash or a timeout, and a naive retry
would issue the command twice. `execute_update` accepts an `id`, and Temporal
deduplicates on it, so a retry attaches to the original update instead of
re-running it.

The id must include the **run** id, not just the workflow id. Actors outlive the
sagas that call them, so a second execution of the same workflow id would
otherwise dedupe onto the previous execution's updates and silently inherit its
results - including its failures. This bit during development: a re-run of
`fail-in-resume` reported a fault from the previous run against a cluster
directory that had never been touched.

### 3. Replay catches structural divergence, not command divergence

Verified both directions against this suite:

- inserting an extra activity call is **caught** as a `NondeterminismError`;
- reversing the order in which families are resumed is **not caught**.

Temporal compares the *type* of each scheduled command, not its arguments.
Because every actor command travels through the same `execute_family_command`
activity, command-level changes are invisible to replay. That blind spot is a
direct cost of the proxy design in finding 1. The audit-log assertions cover
ordering; replay covers structure. Neither is sufficient alone - worth knowing
before treating replay as the primary regression gate in an engine comparison.

### 4. Fail-fast sagas cannot guarantee all-or-nothing

If the broken action *is* the compensating action, rollback cannot complete. The
`compensation-unreachable` scenario makes this concrete: `resume` is permanently
broken on `family_b`, so compensation reverts both configs but cannot bring
`family_b` back up. The saga reports the problem rather than resolving it.

Compensation failures are collected and reported, never allowed to abort the
unwind - stopping halfway would strand the cluster in a worse state than
finishing the remaining compensations.

The RFC's fail-fast section is silent on this case. It needs an answer: alerting,
a reconciliation loop that retries later, or an explicit operator escalation.

### 5. Compensation must reconcile intent, not invert a response

A mutating call can commit and lose its response. The actor then has stale
cached state, and a retried config patch can report the already-patched value as
its "previous" value. Rollback data therefore comes only from the authoritative
pre-saga snapshot, and each compensation is registered before its forward call.

During unwind, a dedicated restore update takes the actor lock, re-reads the
cluster, synchronizes the runtime-state cache, and applies only the missing
runtime or config change. Successful no-ops create no mutation audit entries.
`SagaFailure` reports applied compensations, no-ops, and errors separately.

## Tests

```sh
uv run pytest                  # everything
uv run pytest -m "not crash"   # fast subset, ~3s
uv run pytest -m crash         # worker-kill durability, ~35s, real server
```

| File | Covers |
|---|---|
| `tests/test_saga.py` | RFC compensation, retry taxonomy, lost responses |
| `tests/test_actor.py` | Single-writer serialization, cached and reconciled state |
| `tests/test_replay.py` | Replay determinism against committed history fixtures |
| `tests/test_crash.py` | SIGKILL mid-saga, restart, resume from last step |

Everything except the crash test runs on Temporal's **time-skipping** test
server, which fast-forwards a virtual clock whenever no work is in flight. That
makes retry backoff free. The crash test needs a real server and real elapsed
time, because the point is that an in-flight activity times out and gets
redelivered to a new worker.

Replay fixtures live in `tests/histories/`. Regenerate them when the saga's shape
changes on purpose, and review the diff as part of the change:

```sh
uv run python -m tests.record_histories
```

## Known limitations

- Instant state transitions. Real Flink suspends asynchronously; activities here
  do not poll, so activity heartbeats and long-poll timeouts are unexercised.
- Only the Move Datatype saga. Batch Pause and Batch Resume are degenerate cases
  of it and are not separately implemented.
- No reconciliation loop, no Ingestion Config Portal, no operational web API.
- The actor's `continue_as_new` rollover is implemented but not covered by a
  test - reaching the history threshold takes longer than a POC test should run.
