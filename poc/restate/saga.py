"""Restate Workflow implementation of the move-datatype saga."""

from __future__ import annotations

from dataclasses import dataclass

import restate
from restate import TerminalError

from poc.common.domain import (
    CommandResult,
    FamilyCommand,
    FamilyState,
    FamilyStatus,
    MoveDatatypeRequest,
    SagaFailure,
    SagaOutcome,
)
from poc.restate.actor import (
    CLUSTER_RUN_OPTIONS,
    patch_config,
    pause,
    restore,
    resume,
)
from poc.restate.cluster_steps import read_status
from poc.restate.errors import saga_terminal_error
from poc.restate.models import Empty, PatchConfigRequest, RestoreRequest

move_datatype = restate.Workflow(
    "MoveDatatypeWorkflow",
    description="Pause, patch, and resume two families with LIFO compensation.",
)


@dataclass
class _Compensation:
    label: str
    family: str
    datatypes: list[str] | None = None
    desired_state: FamilyState | None = None


@move_datatype.main()
async def run(
    ctx: restate.WorkflowContext, request: MoveDatatypeRequest
) -> list[str]:
    """Execute the shared three-phase contract as one Restate Workflow."""
    source, target = request.source_family, request.target_family
    done: list[str] = []
    stack: list[_Compensation] = []

    statuses = await _read_all(ctx, request.families)
    validation_error = _validation_error(request, statuses)
    if validation_error is not None:
        raise _failure(
            outcome=SagaOutcome.REJECTED,
            failed_step="validate",
            reason=validation_error,
        )

    new_config = {
        source: [d for d in statuses[source].datatypes if d != request.datatype],
        target: [*statuses[target].datatypes, request.datatype],
    }
    pause_restore = {
        family: _Compensation(
            f"resume:{family}", family, desired_state=statuses[family].state
        )
        for family in request.families
    }
    config_restore = {
        family: _Compensation(
            f"revert:{family}", family, datatypes=list(statuses[family].datatypes)
        )
        for family in request.families
    }
    resume_restore = {
        family: _Compensation(
            f"pause:{family}", family, desired_state=FamilyState.SUSPENDED
        )
        for family in request.families
    }

    try:
        for family in request.families:
            stack.append(pause_restore[family])
            await ctx.object_call(pause, key=family, arg=Empty())
            done.append(f"pause:{family}")

        for family in request.families:
            stack.append(config_restore[family])
            await ctx.object_call(
                patch_config,
                key=family,
                arg=PatchConfigRequest(datatypes=new_config[family]),
            )
            done.append(f"patch:{family}")

        for family in request.families:
            stack.append(resume_restore[family])
            await ctx.object_call(resume, key=family, arg=Empty())
            done.append(f"resume:{family}")
        return done
    except TerminalError as err:
        compensated, noops, errors = await _compensate(ctx, stack)
        outcome = (
            SagaOutcome.COMPENSATION_INCOMPLETE
            if errors
            else SagaOutcome.COMPENSATED
        )
        raise _failure(
            outcome=outcome,
            failed_step=_next_step(request, done),
            reason=err.message,
            compensated=compensated,
            compensation_noops=noops,
            compensation_errors=errors,
        ) from err


async def _read_all(
    ctx: restate.WorkflowContext, families: list[str]
) -> dict[str, FamilyStatus]:
    statuses: dict[str, FamilyStatus] = {}
    for family in families:
        statuses[family] = await ctx.run_typed(
            f"read snapshot for {family}",
            read_status,
            CLUSTER_RUN_OPTIONS,
            family,
        )
    return statuses


def _validation_error(
    request: MoveDatatypeRequest, statuses: dict[str, FamilyStatus]
) -> str | None:
    if request.source_family == request.target_family:
        return "source and target family are the same"
    if request.datatype not in statuses[request.source_family].datatypes:
        return f"{request.datatype} is not owned by {request.source_family}"
    if request.datatype in statuses[request.target_family].datatypes:
        return f"{request.datatype} is already owned by {request.target_family}"
    return None


async def _compensate(
    ctx: restate.WorkflowContext, stack: list[_Compensation]
) -> tuple[list[str], list[str], list[str]]:
    compensated: list[str] = []
    noops: list[str] = []
    errors: list[str] = []
    for item in reversed(stack):
        try:
            result: CommandResult = await ctx.object_call(
                restore,
                key=item.family,
                arg=RestoreRequest(
                    desired_state=item.desired_state,
                    datatypes=item.datatypes,
                ),
            )
            (compensated if result.changed else noops).append(item.label)
        except TerminalError as err:
            errors.append(f"{item.label}: {err.message}")
    return compensated, noops, errors


def _failure(
    *,
    outcome: SagaOutcome,
    failed_step: str,
    reason: str,
    compensated: list[str] | None = None,
    compensation_noops: list[str] | None = None,
    compensation_errors: list[str] | None = None,
) -> TerminalError:
    return saga_terminal_error(
        SagaFailure(
            outcome=outcome,
            failed_step=failed_step,
            reason=reason,
            compensated=compensated or [],
            compensation_noops=compensation_noops or [],
            compensation_errors=compensation_errors or [],
        )
    )


def _next_step(request: MoveDatatypeRequest, done: list[str]) -> str:
    expected = [
        *(f"pause:{f}" for f in request.families),
        *(f"patch:{f}" for f in request.families),
        *(f"resume:{f}" for f in request.families),
    ]
    return next((step for step in expected if step not in done), "unknown")
