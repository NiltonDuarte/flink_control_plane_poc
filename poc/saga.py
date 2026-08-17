"""MoveDatatypeWorkflow - the fail-fast saga.

Three phases across both job families: pause -> patch config -> resume. Every
step that succeeds pushes its inverse onto a compensation stack; the first
failure unwinds that stack LIFO and aborts the transaction.

The stack is worth a note. The RFC spells out four compensation scenarios, and
the fourth (failure during resume) needs a layered rollback: pause what was
resumed, revert every config, then resume everything. That falls out of a plain
LIFO unwind with no special-casing - the only difference from the RFC's written
order is which family is handled first within a phase, which is arbitrary. The
deep rollback needs no dedicated code path, which is a small piece of evidence
that the RFC's compensation model is structurally sound.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from poc.activities import read_status
    from poc.domain import (
        CommandRequest,
        CommandResult,
        FamilyCommand,
        FamilyStatus,
        MoveDatatypeRequest,
        SagaFailure,
    )

# Referenced by name so this module never imports the Temporal client that
# poc/actor_proxy.py needs - workflow code stays sandbox-friendly.
PROXY_ACTIVITY = "execute_family_command"

# The proxy is retried only for infrastructure blips. A command that genuinely
# failed inside the actor comes back as a non_retryable ApplicationError, so it
# lands here immediately and compensation starts without delay.
PROXY_RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=100),
    backoff_coefficient=2.0,
    maximum_attempts=3,
)

PROXY_TIMEOUT = timedelta(seconds=30)
READ_TIMEOUT = timedelta(seconds=10)


@dataclass
class _Compensation:
    """A queued inverse action. In-workflow memory only; replay rebuilds it."""

    label: str
    command: FamilyCommand
    family: str
    datatypes: list[str] | None = None


@workflow.defn
class MoveDatatypeWorkflow:
    @workflow.run
    async def run(self, request: MoveDatatypeRequest) -> list[str]:
        """Move one datatype between two families. Returns the executed steps."""
        source, target = request.source_family, request.target_family
        done: list[str] = []
        stack: list[_Compensation] = []

        statuses = await self._read_all(request.families)
        self._validate(request, statuses)

        # A move is two-sided: the datatype leaves the source and joins the
        # target, so both configs change and a partial apply breaks routing.
        new_config = {
            source: [d for d in statuses[source].datatypes if d != request.datatype],
            target: [*statuses[target].datatypes, request.datatype],
        }

        try:
            # -- Phase 1: pause ------------------------------------------
            for family in request.families:
                result = await self._call(FamilyCommand.PAUSE, family, f"pause:{family}")
                done.append(f"pause:{family}")
                if result.changed:
                    # Only owed if this call actually suspended something.
                    stack.append(
                        _Compensation(f"resume:{family}", FamilyCommand.RESUME, family)
                    )

            # -- Phase 2: patch config -----------------------------------
            for family in request.families:
                result = await self._call(
                    FamilyCommand.PATCH_CONFIG,
                    family,
                    f"patch:{family}",
                    datatypes=new_config[family],
                )
                done.append(f"patch:{family}")
                stack.append(
                    _Compensation(
                        f"revert:{family}",
                        FamilyCommand.PATCH_CONFIG,
                        family,
                        result.previous_datatypes or [],
                    )
                )

            # -- Phase 3: resume -----------------------------------------
            for family in request.families:
                result = await self._call(FamilyCommand.RESUME, family, f"resume:{family}")
                done.append(f"resume:{family}")
                if result.changed:
                    stack.append(
                        _Compensation(f"pause:{family}", FamilyCommand.PAUSE, family)
                    )

            return done

        except Exception as err:  # noqa: BLE001 - any failure triggers rollback
            failed_step = self._next_step(request, done)
            compensated, errors = await self._compensate(stack)
            raise ApplicationError(
                f"move of {request.datatype} aborted at {failed_step}: {err}",
                SagaFailure(
                    failed_step=failed_step,
                    reason=str(err),
                    compensated=compensated,
                    compensation_errors=errors,
                ),
                non_retryable=True,
            ) from err

    # -- helpers -----------------------------------------------------------

    async def _read_all(self, families: list[str]) -> dict[str, FamilyStatus]:
        """Read sequentially, so the audit log has a deterministic order."""
        statuses: dict[str, FamilyStatus] = {}
        for family in families:
            statuses[family] = await workflow.execute_activity(
                read_status,
                family,
                start_to_close_timeout=READ_TIMEOUT,
            )
        return statuses

    def _validate(
        self, request: MoveDatatypeRequest, statuses: dict[str, FamilyStatus]
    ) -> None:
        """Reject impossible moves before anything has been mutated.

        Failing here costs no compensation, which is the cheapest place to fail.
        """
        if request.source_family == request.target_family:
            raise ApplicationError("source and target family are the same", non_retryable=True)
        if request.datatype not in statuses[request.source_family].datatypes:
            raise ApplicationError(
                f"{request.datatype} is not owned by {request.source_family}",
                non_retryable=True,
            )
        if request.datatype in statuses[request.target_family].datatypes:
            raise ApplicationError(
                f"{request.datatype} is already owned by {request.target_family}",
                non_retryable=True,
            )

    async def _call(
        self,
        command: FamilyCommand,
        family: str,
        step: str,
        datatypes: list[str] | None = None,
    ) -> CommandResult:
        """Invoke one actor handler through the proxy activity.

        The update id is stable across replays and retries of *this* execution,
        which is what makes a retried proxy activity attach to the original
        update instead of issuing the command twice.

        It is keyed on the **run** id, not the workflow id. Actors outlive the
        sagas that call them, so a second execution of the same workflow id
        (a re-run, a retry) would otherwise dedupe onto the *previous*
        execution's updates and silently receive its stale results - including
        its failures. Keying on run id scopes the dedup to one execution, which
        is exactly the window in which it is wanted.
        """
        request = CommandRequest(
            family=family,
            command=command,
            update_id=self._update_id(step),
            datatypes=datatypes,
        )
        return await workflow.execute_activity(
            PROXY_ACTIVITY,
            request,
            result_type=CommandResult,
            start_to_close_timeout=PROXY_TIMEOUT,
            retry_policy=PROXY_RETRY,
        )

    async def _compensate(self, stack: list[_Compensation]) -> tuple[list[str], list[str]]:
        """Unwind LIFO, best effort.

        A failing compensation must not abort the unwind - stopping halfway would
        strand the cluster in a worse state than finishing the remaining ones.
        Errors are collected and reported instead.
        """
        compensated: list[str] = []
        errors: list[str] = []
        for index, item in enumerate(reversed(stack)):
            request = CommandRequest(
                family=item.family,
                command=item.command,
                update_id=self._update_id(f"comp:{index}:{item.label}"),
                datatypes=item.datatypes,
            )
            try:
                await workflow.execute_activity(
                    PROXY_ACTIVITY,
                    request,
                    result_type=CommandResult,
                    start_to_close_timeout=PROXY_TIMEOUT,
                    retry_policy=PROXY_RETRY,
                )
                compensated.append(item.label)
            except Exception as err:  # noqa: BLE001 - keep unwinding
                errors.append(f"{item.label}: {err}")
        return compensated, errors

    @staticmethod
    def _update_id(step: str) -> str:
        """Dedup key for one actor command, scoped to this workflow execution."""
        info = workflow.info()
        return f"{info.workflow_id}:{info.run_id}:{step}"

    @staticmethod
    def _next_step(request: MoveDatatypeRequest, done: list[str]) -> str:
        """The step that failed: the first one not in `done`."""
        expected = [
            *(f"pause:{f}" for f in request.families),
            *(f"patch:{f}" for f in request.families),
            *(f"resume:{f}" for f in request.families),
        ]
        for step in expected:
            if step not in done:
                return step
        return "unknown"
