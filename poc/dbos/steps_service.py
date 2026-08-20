"""The saga -> actor hop.

**This module exists because of a Temporal limitation worth recording.**
`temporalio.workflow.ExternalWorkflowHandle` exposes only `signal` and `cancel` -
there is no `update`. A Temporal workflow therefore cannot make a
request/response call into another running workflow. Restate and DBOS invoke a
Virtual Object handler directly and get a return value; on Temporal the saga
needs this hop.

The workaround is an activity that holds a Temporal *client* and calls the actor
from outside the workflow sandbox. It is a class-based activity so the client can
be injected by the worker rather than reconnected per call.

The subtle part is retry safety. This activity can be retried - by a worker crash,
a task timeout, a transient connection error - and a naive retry would issue the
command a second time, pausing an already-paused family or double-patching a
config. `execute_update` accepts an `id`, and Temporal deduplicates updates by
that id, so a retry attaches to the original update and returns its result
instead of re-running it. The saga derives that id deterministically from its own
workflow id plus the step name, which is what makes crash recovery provably free
of duplicated side effects.
"""

from __future__ import annotations

from typing import Any

from dbos import DBOS
# from temporalio import activity
# from temporalio.client import Client, WorkflowUpdateFailedError
# from temporalio.common import WorkflowIDConflictPolicy
# from temporalio.exceptions import ApplicationError

from poc.baseline.actor import FamilyActor
from poc.common.domain import CommandRequest, CommandResult, FamilyCommand
from poc.temporal.actor import FlinkJobFamilyActor, actor_id

type WorkflowFamilies = list[str]

class DBOSActorService:
    def __init__(self, families: WorkflowFamilies):
        self._actors = {
            family: FamilyActor(family) for family in families
        }

    @DBOS.step()
    def resume(families: str): ...

    @DBOS.step()
    def patch_config(families: str): ...

    @DBOS.step()
    def restore(families: str): ...

    @DBOS.step()
    def pause(family: str):
        pass
