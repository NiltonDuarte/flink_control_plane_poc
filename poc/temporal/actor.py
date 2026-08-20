"""FlinkJobFamilyActor - the Virtual Object / Actor of the RFC, on Temporal.

Temporal has no Virtual Object primitive. This models one as a long-lived
*entity workflow*:

* ``workflow_id = "family:<name>"`` - Temporal's workflow-id uniqueness is what
  makes the actor a singleton per job family.
* Commands arrive as ``@workflow.update`` handlers, which (unlike signals) are
  request/response, so the caller gets a result and an error back.
* Every handler body runs under one ``asyncio.Lock``. **That lock is the
  single-writer guarantee.** Temporal delivers concurrent updates as concurrent
  tasks, so without it two sagas could interleave a pause and a resume on the
  same family. With it, a second command queues behind the first exactly as the
  RFC describes.
* State lives in the workflow, not in an external database - matching the RFC's
  "eliminating external database dependency for state resolution".

The cached state is seeded once from the cluster and then maintained in memory.
It is what normal precondition checks ("verify state != SUSPENDED") read. The
compensation-only restore handler refreshes it from the cluster under the lock.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from poc.common.domain import FamilyState, FamilyStatus
    from poc.temporal.activities import (
        patch_configmap,
        read_status,
        resume_job,
        suspend_job,
        trigger_savepoint,
    )
    from poc.temporal.application import TASK_QUEUE, app

# Permanent faults must not be retried: retrying cannot help, and every wasted
# attempt delays the compensation that does need to happen. Temporal matches
# these by exception *type name*.
CLUSTER_RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=100),
    backoff_coefficient=2.0,
    maximum_attempts=4,
    non_retryable_error_types=["PermanentClusterError"],
)

ACTIVITY_TIMEOUT = timedelta(seconds=10)

# Entity workflows live forever, so history has to be bounded. Low enough that
# the POC can actually exercise the rollover.
HISTORY_ROLLOVER = 1_000


def actor_id(family: str) -> str:
    """The workflow id for a family's actor. Uniqueness here *is* the actor."""
    return f"family:{family}"


@app.workflow(task_queue=TASK_QUEUE)
class FlinkJobFamilyActor:
    def __init__(self) -> None:
        self._family: str = ""
        self._state: FamilyState = FamilyState.RUNNING
        self._ready = False
        self._terminate = False
        # Serializes every command handler. See the module docstring.
        self._lock = asyncio.Lock()

    @workflow.run
    async def run(self, family: str, state: FamilyState | None = None) -> None:
        self._family = family
        if state is None:
            # First incarnation: adopt whatever the cluster currently says.
            status: FamilyStatus = await app.execute_activity(
                read_status,
                family,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=CLUSTER_RETRY,
            )
            self._state = status.state
        else:
            # Carried across a continue_as_new boundary.
            self._state = state
        self._ready = True

        await workflow.wait_condition(
            lambda: (
                self._terminate
                or workflow.info().get_current_history_length() > HISTORY_ROLLOVER
            )
        )
        if self._terminate:
            return

        # Take the lock before rolling over: continue_as_new ends this run, and
        # any command still in flight would be aborted mid-way. Holding the lock
        # means no handler is between its savepoint and its suspend.
        async with self._lock:
            workflow.continue_as_new(args=[self._family, self._state])

    # -- handlers (the RFC's Virtual Actor Handlers) -----------------------

    @workflow.update
    async def pause(self) -> str | None:
        """Trigger a savepoint, then suspend. No-op if already suspended.

        Returns the savepoint URI, or None if the family was already suspended -
        which the saga uses to know whether a compensating resume is owed.
        """
        await workflow.wait_condition(lambda: self._ready)
        async with self._lock:
            if self._state == FamilyState.SUSPENDED:
                return None
            uri: str = await app.execute_activity(
                trigger_savepoint,
                self._family,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=CLUSTER_RETRY,
            )
            await app.execute_activity(
                suspend_job,
                self._family,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=CLUSTER_RETRY,
            )
            self._state = FamilyState.SUSPENDED
            return uri

    @workflow.update
    async def resume(self) -> bool:
        """Return to RUNNING. No-op if already running."""
        await workflow.wait_condition(lambda: self._ready)
        async with self._lock:
            if self._state == FamilyState.RUNNING:
                return False
            await app.execute_activity(
                resume_job,
                self._family,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=CLUSTER_RETRY,
            )
            self._state = FamilyState.RUNNING
            return True

    @workflow.update
    async def patch_config(self, datatypes: list[str]) -> list[str]:
        """Replace the routing config; returns the previous value for callers."""
        await workflow.wait_condition(lambda: self._ready)
        async with self._lock:
            previous: list[str] = await app.execute_activity(
                patch_configmap,
                self._family,
                datatypes,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=CLUSTER_RETRY,
            )
            return previous

    @workflow.update
    async def restore(
        self,
        desired_state: FamilyState | None,
        datatypes: list[str] | None,
    ) -> bool:
        """Reconcile a compensation intent against authoritative cluster state.

        Normal commands deliberately trust the actor cache. Compensation is
        different: a forward activity may have committed and then lost its
        response, leaving that cache stale. The fresh read and any needed repair
        therefore happen under the same lock as every other actor command.
        """
        await workflow.wait_condition(lambda: self._ready)
        async with self._lock:
            status: FamilyStatus = await app.execute_activity(
                read_status,
                self._family,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=CLUSTER_RETRY,
            )
            self._state = status.state
            changed = False

            if desired_state is not None and status.state != desired_state:
                if desired_state == FamilyState.SUSPENDED:
                    await app.execute_activity(
                        trigger_savepoint,
                        self._family,
                        start_to_close_timeout=ACTIVITY_TIMEOUT,
                        retry_policy=CLUSTER_RETRY,
                    )
                    await app.execute_activity(
                        suspend_job,
                        self._family,
                        start_to_close_timeout=ACTIVITY_TIMEOUT,
                        retry_policy=CLUSTER_RETRY,
                    )
                else:
                    await app.execute_activity(
                        resume_job,
                        self._family,
                        start_to_close_timeout=ACTIVITY_TIMEOUT,
                        retry_policy=CLUSTER_RETRY,
                    )
                self._state = desired_state
                changed = True

            if datatypes is not None and status.datatypes != datatypes:
                await app.execute_activity(
                    patch_configmap,
                    self._family,
                    datatypes,
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=CLUSTER_RETRY,
                )
                changed = True

            return changed

    # -- introspection and lifecycle ---------------------------------------

    @workflow.query
    def current_state(self) -> FamilyState:
        return self._state

    @workflow.signal
    def terminate(self) -> None:
        """Stop the actor. Test cleanup only - production actors run forever."""
        self._terminate = True
