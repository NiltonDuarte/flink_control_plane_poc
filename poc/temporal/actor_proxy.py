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

from temporalio.client import Client, WorkflowHandle, WorkflowUpdateFailedError
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError

from poc.common.domain import CommandRequest, CommandResult, FamilyCommand
from poc.temporal.actor import FlinkJobFamilyActor, actor_id
from poc.temporal.application import TASK_QUEUE, app


class ActorProxy:
    """Activity implementation bound to a Temporal client and task queue."""

    def __init__(self, client: Client) -> None:
        self._client = client

    @app.activity(task_queue=TASK_QUEUE, name="execute_family_command")
    async def execute_family_command(self, request: CommandRequest) -> CommandResult:
        # USE_EXISTING makes this get-or-create: the first command for a family
        # starts its actor, every later one attaches to the running instance.
        handle = await app.client(self._client).start_workflow(
            FlinkJobFamilyActor.run,
            request.family,
            id=actor_id(request.family),
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )

        try:
            return await self._dispatch(handle, request)
        except WorkflowUpdateFailedError as err:
            # The command genuinely failed inside the actor - its own activity
            # retries are already exhausted. Retrying the proxy would only replay
            # a decided failure and delay compensation, so mark it terminal.
            raise ApplicationError(
                f"{request.command.value} failed on {request.family}: {err.cause}",
                non_retryable=True,
            ) from err

    async def _dispatch(
        self, handle: WorkflowHandle[Any, Any], request: CommandRequest
    ) -> CommandResult:
        if request.command is FamilyCommand.PAUSE:
            uri: str | None = await handle.execute_update(
                FlinkJobFamilyActor.pause, id=request.update_id
            )
            return CommandResult(changed=uri is not None, savepoint_uri=uri)

        if request.command is FamilyCommand.RESUME:
            changed: bool = await handle.execute_update(
                FlinkJobFamilyActor.resume, id=request.update_id
            )
            return CommandResult(changed=changed)

        if request.command is FamilyCommand.PATCH_CONFIG:
            await handle.execute_update(
                FlinkJobFamilyActor.patch_config,
                request.datatypes or [],
                id=request.update_id,
            )
            return CommandResult(changed=True)

        if request.command is FamilyCommand.RESTORE:
            changed: bool = await handle.execute_update(
                FlinkJobFamilyActor.restore,
                args=[request.desired_state, request.datatypes],
                id=request.update_id,
            )
            return CommandResult(changed=changed)

        raise ApplicationError(
            f"unknown command: {request.command}", non_retryable=True
        )
