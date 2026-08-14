"""Temporal's answers to the core's ports.

Everything the core deliberately refused to decide is decided here: what a
cluster operation costs before it is given up on, how a step name becomes a
deduplication key, and what carries a command across the saga -> actor hop.

Two `ClusterPort` instances exist, and the difference between them is load
bearing:

* `SAGA_READS` - the saga's pre-flight read of both families. Short timeout, and
  **no retry policy**, so Temporal's default applies.
* `ACTOR_CLUSTER` - every mutation an actor performs. Retries transient faults
  with backoff, and refuses to retry `PermanentClusterError` at all: retrying
  cannot help, and each wasted attempt delays the compensation that does need to
  happen. Temporal matches those by exception *type name*, which is why the
  taxonomy in `poc/core/domain.py` is expressed as distinct classes.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from pydantic import BaseModel

    from poc.adapters.temporal.activities import (
        patch_configmap,
        read_status,
        resume_job,
        suspend_job,
        trigger_savepoint,
    )
    from poc.core.domain import CommandResult, FamilyCommand, FamilyStatus
    from poc.core.ports import Command

# Referenced by name so workflow code never imports the Temporal *client* that
# poc/adapters/temporal/actor_proxy.py needs - the sandbox stays clean.
PROXY_ACTIVITY = "execute_family_command"

# The proxy is retried only for infrastructure blips. A command that genuinely
# failed inside the actor comes back as a non_retryable ApplicationError, so it
# lands immediately and compensation starts without delay.
PROXY_RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=100),
    backoff_coefficient=2.0,
    maximum_attempts=3,
)

CLUSTER_RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=100),
    backoff_coefficient=2.0,
    maximum_attempts=4,
    non_retryable_error_types=["PermanentClusterError"],
)

PROXY_TIMEOUT = timedelta(seconds=30)
READ_TIMEOUT = timedelta(seconds=10)
ACTIVITY_TIMEOUT = timedelta(seconds=10)


class CommandRequest(BaseModel):
    """The proxy activity's payload - a `Command` plus Temporal's dedup key.

    `update_id` is the field that makes this Temporal-specific, and it is why the
    type lives in the adapter rather than in the core's domain. It is derived
    deterministically from the execution and the step, so a retried proxy
    activity attaches to the same update instead of executing the command a
    second time. Without it, an activity timeout during a pause would pause twice.
    """

    family: str
    command: FamilyCommand
    update_id: str
    datatypes: list[str] | None = None


class ActivityClusterPort:
    """Runs each cluster operation as an activity, with one retry configuration."""

    def __init__(
        self, *, timeout: timedelta, retry_policy: RetryPolicy | None = None
    ) -> None:
        self._timeout = timeout
        self._retry = retry_policy

    async def read_status(self, family: str) -> FamilyStatus:
        return await self._call(read_status, family)

    async def trigger_savepoint(self, family: str) -> str:
        return await self._call(trigger_savepoint, family)

    async def suspend_job(self, family: str) -> None:
        await self._call(suspend_job, family)

    async def resume_job(self, family: str) -> None:
        await self._call(resume_job, family)

    async def patch_configmap(self, family: str, datatypes: list[str]) -> list[str]:
        return await workflow.execute_activity(
            patch_configmap,
            args=[family, datatypes],
            start_to_close_timeout=self._timeout,
            retry_policy=self._retry,
        )

    async def _call(self, activity, family: str):  # type: ignore[no-untyped-def]
        return await workflow.execute_activity(
            activity,
            family,
            start_to_close_timeout=self._timeout,
            retry_policy=self._retry,
        )


class ProxyCommandPort:
    """The saga -> actor hop, and the idempotency key that makes it retry-safe.

    The key is stable across replays and retries of *this* execution, which is
    what makes a retried proxy activity attach to the original update instead of
    issuing the command twice.

    It is keyed on the **run** id, not the workflow id. Actors outlive the sagas
    that call them, so a second execution of the same workflow id (a re-run, a
    retry) would otherwise dedupe onto the *previous* execution's updates and
    silently receive its stale results - including its failures. Keying on run id
    scopes the dedup to one execution, which is exactly the window in which it is
    wanted.
    """

    async def execute(self, command: Command) -> CommandResult:
        request = CommandRequest(
            family=command.family,
            command=command.command,
            update_id=self._update_id(command.step),
            datatypes=command.datatypes,
        )
        return await workflow.execute_activity(
            PROXY_ACTIVITY,
            request,
            result_type=CommandResult,
            start_to_close_timeout=PROXY_TIMEOUT,
            retry_policy=PROXY_RETRY,
        )

    @staticmethod
    def _update_id(step: str) -> str:
        info = workflow.info()
        return f"{info.workflow_id}:{info.run_id}:{step}"


SAGA_READS = ActivityClusterPort(timeout=READ_TIMEOUT)
ACTOR_CLUSTER = ActivityClusterPort(timeout=ACTIVITY_TIMEOUT, retry_policy=CLUSTER_RETRY)
SAGA_COMMANDS = ProxyCommandPort()
