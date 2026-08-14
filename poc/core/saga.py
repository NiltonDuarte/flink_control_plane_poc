"""The Move Datatype saga - the fail-fast transaction, with no engine in sight.

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

**Written as a generator.** `yield Command(...)` is "somebody run this and hand
me the result"; the failure of that command arrives back at the same `yield` as
an exception, which is why the `try/except` below reads exactly like ordinary
straight-line code despite the execution happening in an engine adapter. See
`poc/adapters/driver.py` for the other half.
"""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from poc.core.domain import (
    CommandResult,
    FamilyCommand,
    FamilyStatus,
    MoveDatatypeRequest,
    SagaAborted,
    SagaFailure,
    SagaRejected,
)
from poc.core.ports import Command, ReadStatus, SagaOp


@dataclass
class _Compensation:
    """A queued inverse action. In-memory only; a replay rebuilds it."""

    label: str
    command: FamilyCommand
    family: str
    datatypes: list[str] | None = None


def move_datatype(request: MoveDatatypeRequest) -> Generator[SagaOp, Any, list[str]]:
    """Move one datatype between two families. Returns the executed steps."""
    source, target = request.source_family, request.target_family
    done: list[str] = []
    stack: list[_Compensation] = []

    statuses = yield from _read_all(request.families)
    _validate(request, statuses)

    # A move is two-sided: the datatype leaves the source and joins the target,
    # so both configs change and a partial apply breaks routing.
    new_config = {
        source: [d for d in statuses[source].datatypes if d != request.datatype],
        target: [*statuses[target].datatypes, request.datatype],
    }

    try:
        # -- Phase 1: pause --------------------------------------------------
        for family in request.families:
            result: CommandResult = yield Command(
                FamilyCommand.PAUSE, family, step=f"pause:{family}"
            )
            done.append(f"pause:{family}")
            if result.changed:
                # Only owed if this call actually suspended something.
                stack.append(
                    _Compensation(f"resume:{family}", FamilyCommand.RESUME, family)
                )

        # -- Phase 2: patch config -------------------------------------------
        for family in request.families:
            result = yield Command(
                FamilyCommand.PATCH_CONFIG,
                family,
                step=f"patch:{family}",
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

        # -- Phase 3: resume --------------------------------------------------
        for family in request.families:
            result = yield Command(
                FamilyCommand.RESUME, family, step=f"resume:{family}"
            )
            done.append(f"resume:{family}")
            if result.changed:
                stack.append(
                    _Compensation(f"pause:{family}", FamilyCommand.PAUSE, family)
                )

        return done

    except Exception as err:  # noqa: BLE001 - any failure triggers rollback
        failed_step = _next_step(request, done)
        compensated, errors = yield from _compensate(stack)
        raise SagaAborted(
            f"move of {request.datatype} aborted at {failed_step}: {err}",
            SagaFailure(
                failed_step=failed_step,
                reason=str(err),
                compensated=compensated,
                compensation_errors=errors,
            ),
        ) from err


# -- helpers ---------------------------------------------------------------


def _read_all(families: list[str]) -> Generator[SagaOp, Any, dict[str, FamilyStatus]]:
    """Read sequentially, so the audit log has a deterministic order."""
    statuses: dict[str, FamilyStatus] = {}
    for family in families:
        statuses[family] = yield ReadStatus(family)
    return statuses


def _validate(
    request: MoveDatatypeRequest, statuses: dict[str, FamilyStatus]
) -> None:
    """Reject impossible moves before anything has been mutated.

    Failing here costs no compensation, which is the cheapest place to fail.
    """
    if request.source_family == request.target_family:
        raise SagaRejected("source and target family are the same")
    if request.datatype not in statuses[request.source_family].datatypes:
        raise SagaRejected(f"{request.datatype} is not owned by {request.source_family}")
    if request.datatype in statuses[request.target_family].datatypes:
        raise SagaRejected(
            f"{request.datatype} is already owned by {request.target_family}"
        )


def _compensate(
    stack: list[_Compensation],
) -> Generator[SagaOp, Any, tuple[list[str], list[str]]]:
    """Unwind LIFO, best effort.

    A failing compensation must not abort the unwind - stopping halfway would
    strand the cluster in a worse state than finishing the remaining ones. Errors
    are collected and reported instead.
    """
    compensated: list[str] = []
    errors: list[str] = []
    for index, item in enumerate(reversed(stack)):
        try:
            yield Command(
                item.command,
                item.family,
                step=f"comp:{index}:{item.label}",
                datatypes=item.datatypes,
            )
            compensated.append(item.label)
        except Exception as err:  # noqa: BLE001 - keep unwinding
            errors.append(f"{item.label}: {err}")
    return compensated, errors


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
