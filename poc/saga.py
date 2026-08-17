"""MoveDatatypeWorkflow - the fail-fast saga.

Three phases across both job families: pause -> patch config -> resume. Before
each forward call, the workflow pushes a snapshot-derived restore intent onto a
compensation stack; the first failure unwinds that stack LIFO and aborts the
transaction. Registering first covers the ambiguous case where a mutation lands
but its response is lost.

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
        FamilyState,
        FamilyStatus,
        MoveDatatypeRequest,
        SagaFailure,
        SagaOutcome,
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

ERROR_TYPES = {
    SagaOutcome.REJECTED: "SagaRejectedError",
    SagaOutcome.COMPENSATED: "SagaCompensatedError",
    SagaOutcome.COMPENSATION_INCOMPLETE: "SagaCompensationIncompleteError",
}


@dataclass
class _Compensation:
    """A queued restore intent. In-workflow memory only; replay rebuilds it."""

    label: str
    family: str
    datatypes: list[str] | None = None
    desired_state: FamilyState | None = None


@workflow.defn
class MoveDatatypeWorkflow:
    @workflow.run
    async def run(self, request: MoveDatatypeRequest) -> list[str]:
        """Move one datatype between two families. Returns the executed steps."""
        source, target = request.source_family, request.target_family
        done: list[str] = []
        stack: list[_Compensation] = []

        statuses = await self._read_all(request.families)
        validation_error = self._validation_error(request, statuses)
        if validation_error is not None:
            raise self._failure(
                request,
                outcome=SagaOutcome.REJECTED,
                failed_step="validate",
                reason=validation_error,
            )

        # A move is two-sided: the datatype leaves the source and joins the
        # target, so both configs change and a partial apply breaks routing.
        new_config = {
            source: [d for d in statuses[source].datatypes if d != request.datatype],
            target: [*statuses[target].datatypes, request.datatype],
        }

        # Every rollback payload comes from the authoritative pre-saga snapshot
        # and is built before any mutation. A retry's return value is not safe
        # rollback data: an earlier attempt may already have applied the change.
        pause_restore = {
            family: _Compensation(
                f"resume:{family}",
                family,
                desired_state=statuses[family].state,
            )
            for family in request.families
        }
        config_restore = {
            family: _Compensation(
                f"revert:{family}",
                family,
                datatypes=list(statuses[family].datatypes),
            )
            for family in request.families
        }
        resume_restore = {
            family: _Compensation(
                f"pause:{family}",
                family,
                desired_state=FamilyState.SUSPENDED,
            )
            for family in request.families
        }

        try:
            # -- Phase 1: pause ------------------------------------------
            for family in request.families:
                stack.append(pause_restore[family])
                await self._call(FamilyCommand.PAUSE, family, f"pause:{family}")
                done.append(f"pause:{family}")

            # -- Phase 2: patch config -----------------------------------
            for family in request.families:
                stack.append(config_restore[family])
                await self._call(
                    FamilyCommand.PATCH_CONFIG,
                    family,
                    f"patch:{family}",
                    datatypes=new_config[family],
                )
                done.append(f"patch:{family}")

            # -- Phase 3: resume -----------------------------------------
            for family in request.families:
                stack.append(resume_restore[family])
                await self._call(FamilyCommand.RESUME, family, f"resume:{family}")
                done.append(f"resume:{family}")

            return done

        except Exception as err:  # noqa: BLE001 - any failure triggers rollback
            failed_step = self._next_step(request, done)
            compensated, noops, errors = await self._compensate(stack)
            outcome = (
                SagaOutcome.COMPENSATION_INCOMPLETE
                if errors
                else SagaOutcome.COMPENSATED
            )
            raise self._failure(
                request,
                outcome=outcome,
                failed_step=failed_step,
                reason=str(err),
                compensated=compensated,
                compensation_noops=noops,
                compensation_errors=errors,
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

    def _validation_error(
        self, request: MoveDatatypeRequest, statuses: dict[str, FamilyStatus]
    ) -> str | None:
        """Reject impossible moves before anything has been mutated.

        Failing here costs no compensation, which is the cheapest place to fail.
        """
        if request.source_family == request.target_family:
            return "source and target family are the same"
        if request.datatype not in statuses[request.source_family].datatypes:
            return f"{request.datatype} is not owned by {request.source_family}"
        if request.datatype in statuses[request.target_family].datatypes:
            return f"{request.datatype} is already owned by {request.target_family}"
        return None

    @staticmethod
    def _failure(
        request: MoveDatatypeRequest,
        *,
        outcome: SagaOutcome,
        failed_step: str,
        reason: str,
        compensated: list[str] | None = None,
        compensation_noops: list[str] | None = None,
        compensation_errors: list[str] | None = None,
    ) -> ApplicationError:
        """Build the stable Temporal error type, message, and detail together."""
        applied = compensated or []
        noops = compensation_noops or []
        errors = compensation_errors or []
        if outcome == SagaOutcome.REJECTED:
            message = (
                f"{outcome.value}: move of {request.datatype} rejected "
                f"at {failed_step}: {reason}"
            )
        else:
            message = (
                f"{outcome.value}: move of {request.datatype} failed "
                f"at {failed_step}: {reason}; rollback "
                f"applied={len(applied)} no_op={len(noops)} failed={len(errors)}"
            )
        return ApplicationError(
            message,
            SagaFailure(
                outcome=outcome,
                failed_step=failed_step,
                reason=reason,
                compensated=applied,
                compensation_noops=noops,
                compensation_errors=errors,
            ),
            type=ERROR_TYPES[outcome],
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

    async def _compensate(
        self, stack: list[_Compensation]
    ) -> tuple[list[str], list[str], list[str]]:
        """Unwind LIFO, best effort.

        A failing compensation must not abort the unwind - stopping halfway would
        strand the cluster in a worse state than finishing the remaining ones.
        Errors are collected and reported instead.
        """
        compensated: list[str] = []
        noops: list[str] = []
        errors: list[str] = []
        for index, item in enumerate(reversed(stack)):
            request = CommandRequest(
                family=item.family,
                command=FamilyCommand.RESTORE,
                update_id=self._update_id(f"comp:{index}:{item.label}"),
                datatypes=item.datatypes,
                desired_state=item.desired_state,
            )
            try:
                result = await workflow.execute_activity(
                    PROXY_ACTIVITY,
                    request,
                    result_type=CommandResult,
                    start_to_close_timeout=PROXY_TIMEOUT,
                    retry_policy=PROXY_RETRY,
                )
                if result.changed:
                    compensated.append(item.label)
                else:
                    noops.append(item.label)
            except Exception as err:  # noqa: BLE001 - keep unwinding
                errors.append(f"{item.label}: {err}")
        return compensated, noops, errors

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
