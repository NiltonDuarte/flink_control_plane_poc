# Flink Sink Layer Control Plane - durable execution POC

A working baseline workflow for evaluating durable execution engines, per
[`TICKET-Create_a_testable_workflow_baseline.txt`](TICKET-Create_a_testable_workflow_baseline.txt)
and the architecture in
[`RFC_Sink_Layer_Control_Plane_Architecture.md`](RFC_Sink_Layer_Control_Plane_Architecture.md).

This implements the **Move Datatype Between Jobs** saga against a mock cluster.
No Flink, no Kubernetes. Agreed scope is in [`POC_SCOPE.md`](POC_SCOPE.md).

The saga itself lives in [`poc/core/`](poc/core/) and knows nothing about any
engine; **Temporal** is one adapter over it, and Restate will be the second. That
split is the point: a benchmark between two engines only means something if both
are executing the same domain logic rather than two implementations that were
written to look alike.

## Quick start

```sh
uv sync
uv run pytest                          # 31 tests, ~14s
uv run pytest tests/test_core_saga.py  # the saga alone, no engine, instant
uv run python -m poc.cli list          # the scenarios
```

To watch a scenario run against a real Temporal server:

```sh
temporal server start-dev              # separate terminal
uv run python -m poc.cli run fail-in-resume
uv run python -m poc.cli run fail-in-resume --engine restate   # not yet, see #11
```

## Scenarios

Each is a seed cluster plus a set of fault-injection rules, defined once in
[`poc/scenarios.py`](poc/scenarios.py) and shared by the CLI, the tests, and
every engine adapter.

| Scenario | What it exercises |
|---|---|
| `happy` | Success path |
| `fail-in-pause` | RFC compensation scenario 2 |
| `fail-in-update` | RFC compensation scenario 3 |
| `fail-in-resume` | RFC compensation scenario 4 - the layered rollback |
| `compensation-unreachable` | Compensation that cannot succeed |
| `transient-retry` | Retry with backoff, then success |
| `transient-exhausted` | Retry budget spent, then compensation |
| `permanent-first-step` | Non-retryable failure, nothing to compensate |

## How it fits together

```
poc/core/saga.py      move_datatype: pause -> patch -> resume, LIFO compensation
poc/core/family.py    pause / resume / patch_config semantics, no-op checks
        |
        |  yields Command / ClusterOp values; consumes results
        |  ---------------------- the engine boundary ----------------------
        v
poc/adapters/driver.py            runs the generator against an adapter
poc/adapters/temporal/
    saga.py           MoveDatatypeWorkflow      <- drives the core saga
    actor.py          FlinkJobFamilyActor       <- entity workflow, lock-serialized
    actor_proxy.py    execute_family_command    <- the hop, see "Findings"
    ports.py          retry policies, timeouts, the update-id derivation
    activities.py     one activity per cluster operation
        |
        v
poc/cluster.py        MockCluster (a directory of JSON files)
```

Two modules are load-bearing for the comparison:

- **`poc/core/`** is synchronous. Not by preference - the saga and the family
  handlers are generators, so they *cannot* await, open a socket, or reach an
  engine. `tests/test_core_is_engine_free.py` enforces it by parsing the source.
- **`poc/cluster.py`** is the only module that touches infrastructure. Swapping
  the mock for real Flink Kubernetes Operator calls means reimplementing that
  file and nothing else - it imports no engine and no workflow code.

Everything genuinely engine-specific ends up in an adapter because there is
nowhere else it can go: retry policy, idempotency-key derivation, and the
single-writer mechanism for a job family.

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

## Findings

Four things this POC established that are worth carrying into the RFC.

### 1. Temporal cannot call an actor handler from a workflow

`workflow.ExternalWorkflowHandle` exposes only `signal` and `cancel` - there is no
`update`. A workflow cannot make a request/response call into another running
workflow. Restate and DBOS invoke a Virtual Object handler directly and get a
return value; on Temporal the saga needs an activity that holds a client and
calls the actor from outside the workflow sandbox
([`poc/adapters/temporal/actor_proxy.py`](poc/adapters/temporal/actor_proxy.py)).

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

### 5. A generator-based core survives Temporal untouched

The saga is a synchronous generator: it yields the command it wants performed and
is handed the result back, or the *failure* back, via `throw()`
([`poc/adapters/driver.py`](poc/adapters/driver.py)). The obvious worry was that
this would fight the workflow sandbox or the replay determinism checks.

It did not. Every recorded history replays unchanged, and every audit-order
assertion held with no edit. Temporal is indifferent to how workflow code decides
what to schedule next, as long as it decides deterministically - and a generator
driven by a deterministic loop is exactly that.

What it costs: results arriving through `yield` are untyped, and a failure has to
enter the core through `throw()` rather than simply being raised, so the driver
has a second code path. What it buys is worth more for an engine comparison - the
core structurally cannot perform I/O, and the whole compensation matrix can be
asserted with no engine at all, in milliseconds
([`tests/test_core_saga.py`](tests/test_core_saga.py)).

## Tests

```sh
uv run pytest                  # everything
uv run pytest -m "not crash"   # fast subset, ~3s
uv run pytest -m crash         # worker-kill durability, ~11s, real server
make test-core                 # no engine at all, instant
```

| File | Covers | Engine |
|---|---|---|
| `tests/test_core_saga.py` | Phase order, LIFO unwind, compensation payloads | none |
| `tests/test_core_is_engine_free.py` | The core imports no engine and never awaits | none |
| `tests/test_saga.py` | All four RFC compensation scenarios, retry taxonomy | every registered |
| `tests/temporal/test_actor.py` | Single-writer serialization, actor-held state | Temporal |
| `tests/temporal/test_replay.py` | Replay determinism against committed fixtures | Temporal |
| `tests/temporal/test_crash.py` | SIGKILL mid-saga, restart, resume from last step | Temporal |

`tests/test_saga.py` is the shared matrix. It names no engine: the `engine`
fixture runs it against every adapter registered in
[`tests/engines.py`](tests/engines.py), so adding Restate adds a column rather
than a second test suite. The three files under `tests/temporal/` test Temporal
*mechanisms* - update dedup, replay, worker death - which have no like-for-like
counterpart on another engine, and are deliberately not parameterized.

Everything except the crash test runs on Temporal's **time-skipping** test
server, which fast-forwards a virtual clock whenever no work is in flight. That
makes retry backoff free. The crash test needs a real server and real elapsed
time, because the point is that an in-flight activity times out and gets
redelivered to a new worker.

Replay fixtures live in `tests/temporal/histories/`. Regenerate them when the
saga's shape changes on purpose, and review the diff as part of the change:

```sh
uv run python -m tests.temporal.record_histories
```

## Known limitations

- Instant state transitions. Real Flink suspends asynchronously; activities here
  do not poll, so activity heartbeats and long-poll timeouts are unexercised.
- Only the Move Datatype saga. Batch Pause and Batch Resume are degenerate cases
  of it and are not separately implemented.
- No reconciliation loop, no Ingestion Config Portal, no operational web API.
- One engine implemented. `poc/adapters/restate/` is a documented stub and
  `--engine restate` says so rather than pretending; the Restate implementation
  is issue #11, which this split exists to unblock.
- The actor's `continue_as_new` rollover is implemented but not covered by a
  test - reaching the history threshold takes longer than a POC test should run.
