"""The loop that runs a core generator against an engine.

Engine-agnostic, but `async`, which is why it sits in `poc/adapters/` rather than
in `poc/core/` - the core's whole guarantee is that it contains no `await`.

The contract in three lines:

* the generator yields an operation and is suspended;
* the driver executes it and pushes the result back in with ``send()``;
* if execution *raised*, the driver pushes the exception in with ``throw()``.

That last line is the interesting one. A failed step has to surface at the exact
`yield` that requested it, because the core's `try/except` around the forward
phases is what triggers the compensation unwind. Handing the error back through
`throw()` is how a saga written as straight-line code keeps working when the
execution happens somewhere else entirely.

Exceptions the *core itself* raises - `SagaRejected`, `SagaAborted` - are not
caught here. They propagate out to the adapter, which is obliged to translate
them into whatever its engine calls a terminal failure.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Generator
from typing import Any, TypeVar

from poc.core.ports import (
    ClusterOp,
    ClusterPort,
    Command,
    CommandPort,
    PatchConfigmap,
    ReadStatus,
    ResumeJob,
    SagaOp,
    SuspendJob,
    TriggerSavepoint,
)

T = TypeVar("T")


async def drive_cluster(
    generator: Generator[ClusterOp, Any, T],
    cluster: ClusterPort,
) -> T:
    """Run a generator that only talks to the cluster - the family handlers."""

    async def dispatch(op: ClusterOp) -> Any:
        return await _run_cluster_op(op, cluster)

    return await _drive(generator, dispatch)


async def drive_saga(
    generator: Generator[SagaOp, Any, T],
    *,
    reads: ClusterPort,
    commands: CommandPort,
) -> T:
    """Run the saga, which reads the cluster directly and commands via actors.

    `reads` and the port the actors use are deliberately separate objects: the
    saga's pre-flight read and an actor's mutation do not want the same timeout
    or the same retry budget.
    """

    async def dispatch(op: SagaOp) -> Any:
        if isinstance(op, Command):
            return await commands.execute(op)
        return await _run_cluster_op(op, reads)

    return await _drive(generator, dispatch)


# -- internals -------------------------------------------------------------


async def _drive(
    generator: Generator[Any, Any, T],
    dispatch: Callable[[Any], Awaitable[Any]],
) -> T:
    to_send: Any = None
    to_throw: BaseException | None = None

    while True:
        try:
            if to_throw is not None:
                op = generator.throw(to_throw)
            else:
                op = generator.send(to_send)
        except StopIteration as finished:
            return finished.value  # type: ignore[no-any-return]

        to_send, to_throw = None, None
        try:
            to_send = await dispatch(op)
        except Exception as err:  # noqa: BLE001 - handed back to the generator
            to_throw = err


async def _run_cluster_op(op: ClusterOp, cluster: ClusterPort) -> Any:
    match op:
        case ReadStatus(family):
            return await cluster.read_status(family)
        case TriggerSavepoint(family):
            return await cluster.trigger_savepoint(family)
        case SuspendJob(family):
            return await cluster.suspend_job(family)
        case ResumeJob(family):
            return await cluster.resume_job(family)
        case PatchConfigmap(family, datatypes):
            return await cluster.patch_configmap(family, datatypes)
        case _:
            raise TypeError(f"not a cluster operation: {op!r}")
