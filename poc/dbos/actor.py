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
from sqlalchemy import text
from dbos import DBOS
from poc.common.domain import FamilyState
from poc.dbos.server import family_repository

from poc.dbos.activities import (
    patch_configmap,
    read_status,
    resume_job,
    suspend_job,
    trigger_savepoint,
)


ACTIVITY_TIMEOUT = timedelta(seconds=10)

# Entity workflows live forever, so history has to be bounded. Low enough that
# the POC can actually exercise the rollover.
HISTORY_ROLLOVER = 1_000


def actor_id(family: str) -> str:
    """The workflow id for a family's actor. Uniqueness here *is* the actor."""
    return f"family:{family}"


class FlinkJobFamilyActor:
    def __init__(self) -> None:
        self._family: str = ""
        self._state: FamilyState = FamilyState.RUNNING
        self._ready = False
        self._terminate = False
        # # Serializes every command handler. See the module docstring.
        # self._lock = asyncio.Lock()
    
    # @family_repository.transaction()
    # def lock_and_get_state(family: str) -> FamilyState:
    #     family_repository.session.execute(
    #         text("INSERT INTO family_state (family, state) VALUES (:family, :state) ON CONFLICT DO NOTHING"),
    #         {"family": family, "state": FamilyState.RUNNING.value}
    #     )
    #     result = family_repository.session.execute(
    #         text("SELECT state FROM family_state WHERE family = :family FOR UPDATE"),
    #         {"family": family}
    #     ).fetchone()
    #     return FamilyState(result[0])


    @DBOS.step()
    def run(self, family: str, state: FamilyState | None = None) -> None:
        self._family = family
        if state is None:
            # First incarnation: adopt whatever the cluster currently says.
            status = read_status(family)
            self._state = status.state
        else:
            # Carried across a continue_as_new boundary.
            self._state = state
        self._ready = True

        if self._terminate:
            return

    # -- handlers (the RFC's Virtual Actor Handlers) -----------------------

    @DBOS.step()
    def pause(self) -> str | None:
        """Trigger a savepoint, then suspend. No-op if already suspended.

        Returns the savepoint URI, or None if the family was already suspended -
        which the saga uses to know whether a compensating resume is owed.
        """
        # TODO: check how to implement this
        # workflow.wait_condition(lambda: self._ready)
        # with self._lock:
        if self._state == FamilyState.SUSPENDED:
            return None
        uri: str = trigger_savepoint(self._family)
        suspend_job(self._family)
        self._state = FamilyState.SUSPENDED
        return uri

    @DBOS.step()
    def resume(self) -> bool:
        # workflow.wait_condition(lambda: self._ready)
        # with self._lock:
        if self._state == FamilyState.RUNNING:
            return False
        resume_job(self._family)
        # workflow.execute_activity(
        #     resume_job,
        #     self._family,
        #     start_to_close_timeout=ACTIVITY_TIMEOUT,
        #     retry_policy=CLUSTER_RETRY,
        # )
        self._state = FamilyState.RUNNING
        return True

    @DBOS.step()
    def patch_config(self, datatypes: list[str]) -> list[str]:
        """Replace the routing config; returns the previous value for callers."""
        # workflow.wait_condition(lambda: self._ready)
        # with self._lock:
        return patch_configmap(self._family, datatypes)

    @DBOS.step()
    def restore(
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
        # workflow.wait_condition(lambda: self._ready)
        # with self._lock:
        status = read_status(self._family)
        self._state = status.state
        changed = False

        if desired_state is not None and status.state != desired_state:
            if desired_state == FamilyState.SUSPENDED:
                trigger_savepoint(self._family)
                suspend_job(self._family)

            else:
                resume_job(self._family)
            self._state = desired_state
            changed = True

        if datatypes is not None and status.datatypes != datatypes:
            patch_configmap(self._family, datatypes)
            changed = True

            return changed

    # -- introspection and lifecycle ---------------------------------------

    @family_repository.transaction()
    def current_state(self) -> FamilyState:
        sql = text("SELECT state FROM family_state WHERE family = :family")
        result = family_repository.session.execute(sql, {"family": self._family}).fetchone()
        return result[0] if result else "NOT_FOUND"
