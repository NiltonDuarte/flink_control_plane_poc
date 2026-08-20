# Restate assessment

Measured on 2026-08-20 with Restate Server and CLI 1.7.2 and the Python SDK
1.0.3. The implementation is intentionally independent from the baseline and
Temporal sagas. It imports only the shared domain models, mock cluster, and
scenario definitions.

## Result

Restate fits this actor-plus-saga shape well. The strongest advantage over the
Temporal version is structural: the workflow calls a keyed Virtual Object
handler directly and receives a durable result. That removes the proxy activity,
client escape hatch, in-workflow lock, and explicit update-id construction. The
main tradeoff in this Python version is testing: the harness gives convincing
integration replay and crash recovery, but it needs Docker and does not provide
Temporal-style time skipping or offline history-file replay.

All ten shared scenarios matched the baseline for verdict, completed steps,
normalized audit attempts and order, and final cluster snapshot. Focused tests
also proved one effective mutation under five concurrent pauses, exact mutation
deduplication across four lost-response attempts, K/V state across calls,
stale-cache repair, four-attempt retry exhaustion, and immediate permanent
failure. The ASGI crash test killed Hypercorn after the saga had started,
restarted it at the same endpoint, and required exactly one effective side
effect for each of the eight logical mutations.

## Implementation findings

### Developer ergonomics and compensation

Exclusive `VirtualObject` handlers are the actor primitive the domain wants:
the key is the family identity, handlers are serialized by Restate, and the
runtime cache is one `ctx.get`/`ctx.set` value. There is no application lock or
long-running actor loop. The workflow-to-actor edge is a normal
`await ctx.object_call(handler, key=..., arg=...)`, as described by the official
[services](https://docs.restate.dev/develop/python/services) and
[service communication](https://docs.restate.dev/develop/python/service-communication)
documentation.

Compensation remains explicit application code. The same pre-registered LIFO
stack is still the clearest expression of the rollback contract, including the
layered resume failure. Restate makes the calls simpler, but it does not infer a
saga or make an unreachable inverse operation solvable. The
`compensation-unreachable` verdict therefore remains
`COMPENSATION_INCOMPLETE`.

### Retry and error taxonomy

Restate retries ordinary handler and durable-step failures by default. Each
mock-cluster operation is inside `ctx.run_typed` with 100 ms initial delay,
factor 2, and four attempts, matching Temporal. A `PermanentClusterError` is
translated inside the durable step to `TerminalError`; transient errors escape
and consume that bounded policy. The retry sequence also has a ten-second
maximum duration. When either bound is exhausted, the SDK surfaces a
`TerminalError`, which is the only exception the saga and compensation loop
catch. This follows the official guidance for
[durable steps](https://docs.restate.dev/develop/python/durable-steps) and
[error handling](https://docs.restate.dev/develop/python/error-handling) and
avoids swallowing SDK-internal control-flow exceptions.

Saga verdicts use a versioned terminal-error envelope with visible outcome and
error-type prefixes plus a base64url `SagaFailure` payload. Ingress statuses are
400 for `REJECTED`, 409 for `COMPENSATED`, and 500 for
`COMPENSATION_INCOMPLETE`. The shared CLI decodes the envelope from Restate's
HTTP error body and renders the same step, reason, and rollback counts as the
other engines.

### Idempotency and ambiguous effects

Restate journals workflow-to-object calls. On workflow replay, a completed call
returns its recorded result rather than issuing a new object command; no
Temporal-style explicit update ID is needed. This is the relevant guarantee in
the official
[service-call documentation](https://docs.restate.dev/develop/python/service-communication).

That guarantee does not by itself make an arbitrary external side effect
exactly once. A process can still die after the mock cluster commits but before
the durable step result is journaled. Each handler therefore derives a stable
operation identity from Restate's durable invocation ID and logical mutation.
The shared boundary persists completed results, records a repeated attempt with
`changed=false`, and does not advance generation or create another savepoint.
Compensation remains pre-registered from authoritative pre-saga data because
deduplication does not remove the need to restore a failed saga.

Restate-to-object waiting is engine-durable rather than wrapped in an arbitrary
application timeout. External I/O remains separately bounded: durable-step
retries have four attempts and ten seconds total; the ingress client has
explicit connect/read/write/pool timeouts; deployment registration has connect
and total timeouts. A production Flink or Kubernetes adapter must additionally
enforce a timeout for each individual external attempt.

### Testing, replay, and crash recovery

The Python harness starts the SDK endpoint and a real Restate server through
Testcontainers. It is pinned to `restatedev/restate:1.7.2` and runs with
`always_replay=True`, which forces replay at suspension points. This is an
integration replay check: it exercises current code against the live server
journal. It is not equivalent to Temporal's offline `Replayer` consuming a
committed history JSON file, and the harness has no virtual clock. Retry delays
therefore cost wall time.

The measured fast Restate suite completed 22 tests in about 11 seconds on the
local Docker runtime. The separate service-crash test keeps Restate running,
kills Hypercorn on port 9080, restarts the same deployment endpoint, and
requires the workflow to finish with exactly eight effective mutations. An
in-flight durable step may still be attempted more than once, but the stable
operation identity makes repeated attempts audited no-ops.

## UI, CLI, SQL, and operations — observed locally

These are observations from the running 1.7.2 server after executing the happy
scenario, not documentation-only claims:

- Deployment discovery registered `MoveDatatypeWorkflow` as a Workflow and
  `FlinkJobFamilyActor` as a Virtual Object, included generated Pydantic input
  and output schemas, and reported `restate-sdk-python/1.0.3` with protocol
  versions 5–7.
- The overview showed two services, one deployment, six handlers, seven
  successful invocations, and the exact HTTP deployment endpoint.
- The workflow detail showed two named durable snapshot runs followed by six
  direct actor calls in pause → patch → resume order. Each call linked to its
  child invocation and exposed input, result, timing, pinned deployment, SDK
  version, and retention.
- The journal contained 24 entries: input, two run commands and notifications,
  six call/ID/result triplets, and output. This made the direct
  workflow-to-object topology materially clearer than the Temporal proxy
  activity history.
- The State view showed two `FlinkJobFamilyActor` keys and their persisted
  `runtime_state` values (`"RUNNING"`), with per-value size. The same values were
  returned by `restate state get FlinkJobFamilyActor family_a` and the SQL
  `state` table.
- `restate invocations list --all` displayed the six child calls with
  `Invoked by` pointing to the parent workflow. `restate services list` showed
  flavor, revision, deployment type, and deployment ID.
- The UI exposed processing, in-flight, stuck, and all-invocation filters, a
  Playground entry point, SQL introspection, and a `Restart as new…` action on
  the completed workflow. Destructive cancel/kill/state-edit controls were not
  exercised in this PoC.

The official [introspection documentation](https://docs.restate.dev/services/introspection)
describes the underlying `sys_invocation`, `sys_journal`, `sys_service`,
`sys_deployment`, `sys_idempotency`, and `state` tables and the corresponding
CLI and HTTP query paths.

## Temporal ↔ Restate

| Area | Temporal implementation | Restate implementation |
|---|---|---|
| Actor / single writer | Entity workflow ID plus explicit `asyncio.Lock` around update handlers; resource-local only | Native keyed Virtual Object; exclusive handlers serialize automatically; resource-local only |
| Saga → actor call | Proxy activity holds a client because workflow handles cannot execute updates | Direct journaled `ctx.object_call` with a return value |
| Idempotency / dedup | Deterministic run-scoped `update_id` deduplicates proxy retries; derived external operation IDs deduplicate activity retries | Journaled service call reuses its result on replay; invocation-derived external operation IDs deduplicate durable-step retries |
| Retry taxonomy | Activity `RetryPolicy`, four attempts, permanent exception type excluded | `RunOptions`, four attempts and ten seconds total; permanent cluster faults translated to `TerminalError` |
| Compensation ergonomics | Explicit pre-registered LIFO stack through proxy activities | Same explicit stack with direct restore-handler calls; less transport plumbing |
| State | Actor workflow memory, replayed from history | Virtual Object K/V, inspectable as `runtime_state` |
| Local tests | Time-skipping server; no Docker for most tests | Real server Testcontainer; Docker required; no time skipping |
| Replay | Offline history fixtures with `Replayer` plus live execution | Harness `always_replay=True`; integration replay only, no offline history-file equivalent tested |
| Crash test | Kill worker, start replacement, server retains workflow history | Keep server, kill/restart Hypercorn at the same deployment endpoint, journal resumes execution |
| UI call graph | Proxy activity obscures command arguments behind one activity type | Parent workflow displays each keyed object call and linked child invocation directly |
| Introspection | Temporal UI/CLI and workflow history/query surfaces | UI + CLI + SQL tables for invocations, journals, deployments, idempotency, and K/V state |
| Operational controls | Workflow terminate/cancel/reset and actor lifecycle management | Invocation cancel/kill/restart, deployment management, stuck-object lookup, state inspection/editing |

## Recommendation

For this control-plane shape, Restate is the cleaner application architecture:
Virtual Objects and direct durable calls match the RFC concepts without the
Temporal proxy. Temporal currently has the stronger local regression toolset in
this repository because offline history replay and time skipping are valuable.
The decision therefore turns on priorities: choose Restate for a smaller, more
legible actor/saga implementation and strong built-in state introspection;
choose Temporal if mature offline replay and virtual-time testing outweigh the
extra actor-call plumbing.

Neither engine's per-resource serialization establishes saga-level ownership,
leases, takeover, fencing, or stale-command rejection; issue #28 owns that
design. Incomplete compensation likewise has no reconciliation API or durable
`RecoveryPlan`; issue #27 remains authoritative.
