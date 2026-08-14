"""FlinkJobFamilyActor - the Virtual Object / Actor of the RFC, on Temporal.

Temporal has no Virtual Object primitive. This module is what building one costs,
and it is deliberately the *only* thing left here - the handlers' semantics moved
to `poc/core/family.py`, so what remains is pure mechanism:

* ``workflow_id = "family:<name>"`` - Temporal's workflow-id uniqueness is what
  makes the actor a singleton per job family.
* Commands arrive as ``@workflow.update`` handlers, which (unlike signals) are
  request/response, so the caller gets a result and an error back.
* Every handler body runs under one ``asyncio.Lock``. **That lock is the
  single-writer guarantee.** Temporal delivers concurrent updates as concurrent
  tasks, so without it two sagas could interleave a pause and a resume on the
  same family. With it, a second command queues behind the first exactly as the
  RFC describes. On Restate this line has no counterpart at all, which is the
  comparison in a nutshell.
* State lives in the workflow, not in an external database - matching the RFC's
  "eliminating external database dependency for state resolution". `FamilyCore`
  holds it; this class only decides when it is safe to touch.
"""

from __future__ import annotations

import asyncio

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from poc.adapters.driver import drive_cluster
    from poc.adapters.temporal.ports import ACTOR_CLUSTER
    from poc.core.domain import FamilyState
    from poc.core.family import FamilyCore

# Entity workflows live forever, so history has to be bounded. Low enough that
# the POC can actually exercise the rollover.
HISTORY_ROLLOVER = 1_000


def actor_id(family: str) -> str:
    """The workflow id for a family's actor. Uniqueness here *is* the actor."""
    return f"family:{family}"


@workflow.defn
class FlinkJobFamilyActor:
    def __init__(self) -> None:
        self._core = FamilyCore("", FamilyState.RUNNING)
        self._ready = False
        self._terminate = False
        # Serializes every command handler. See the module docstring.
        self._lock = asyncio.Lock()

    @workflow.run
    async def run(self, family: str, state: FamilyState | None = None) -> None:
        if state is None:
            # First incarnation: adopt whatever the cluster currently says.
            self._core = await drive_cluster(FamilyCore.adopt(family), ACTOR_CLUSTER)
        else:
            # Carried across a continue_as_new boundary.
            self._core = FamilyCore(family, state)
        self._ready = True

        await workflow.wait_condition(
            lambda: self._terminate
            or workflow.info().get_current_history_length() > HISTORY_ROLLOVER
        )
        if self._terminate:
            return

        # Take the lock before rolling over: continue_as_new ends this run, and
        # any command still in flight would be aborted mid-way. Holding the lock
        # means no handler is between its savepoint and its suspend.
        async with self._lock:
            workflow.continue_as_new(args=[self._core.family, self._core.state])

    # -- handlers (the RFC's Virtual Actor Handlers) -----------------------
    #
    # Each is the same three lines: wait until the cache is seeded, take the
    # single-writer lock, drive the core handler. Everything the handler
    # *decides* - the no-op checks, the order of cluster operations, when the
    # cached state may be updated - is in poc/core/family.py.

    @workflow.update
    async def pause(self) -> str | None:
        await workflow.wait_condition(lambda: self._ready)
        async with self._lock:
            return await drive_cluster(self._core.pause(), ACTOR_CLUSTER)

    @workflow.update
    async def resume(self) -> bool:
        await workflow.wait_condition(lambda: self._ready)
        async with self._lock:
            return await drive_cluster(self._core.resume(), ACTOR_CLUSTER)

    @workflow.update
    async def patch_config(self, datatypes: list[str]) -> list[str]:
        await workflow.wait_condition(lambda: self._ready)
        async with self._lock:
            return await drive_cluster(
                self._core.patch_config(datatypes), ACTOR_CLUSTER
            )

    # -- introspection and lifecycle ---------------------------------------

    @workflow.query
    def current_state(self) -> FamilyState:
        return self._core.state

    @workflow.signal
    def terminate(self) -> None:
        """Stop the actor. Test cleanup only - production actors run forever."""
        self._terminate = True
