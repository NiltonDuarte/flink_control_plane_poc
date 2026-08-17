

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from poc.common.domain import (
        FamilyCommand,
        MoveDatatypeRequest,
        SagaFailure,
    )
from poc.baseline.actor import FamilyActor, UpdateStatus
from poc.common.domain import FamilyStatus


@dataclass
class _Compensation:
    """A queued inverse action. In-workflow memory only; replay rebuilds it."""

    label: str
    command: FamilyCommand
    family: str
    datatypes: list[str] | None = None

class MoveDatatypeWorkflow:
    def run(self, request: MoveDatatypeRequest) -> list[str]:
        """Move one datatype between two families. Returns the executed steps."""
        source, target = request.source_family, request.target_family
        done: list[str] = []
        stack: list[_Compensation] = []
        actors = {
            source: FamilyActor(source),
            target: FamilyActor(target)
        }

        statuses = self._read_all(request.families)
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
                result = actors[family].pause()
                done.append(f"pause:{family}")
                if result == UpdateStatus.CHANGED:
                    # Only owed if this call actually suspended something.
                    stack.append(
                        _Compensation(f"resume:{family}", FamilyCommand.RESUME, family)
                    )

            # -- Phase 2: patch config -----------------------------------
            for family in request.families:
                previous_datatypes = actors[family].patch_config(new_config[family])
                done.append(f"patch:{family}")
                stack.append(
                    _Compensation(
                        f"revert:{family}",
                        FamilyCommand.PATCH_CONFIG,
                        family,
                        previous_datatypes or [],
                    )
                )

            # -- Phase 3: resume -----------------------------------------
            for family in request.families:
                result = actors[family].resume()
                done.append(f"resume:{family}")
                if result == UpdateStatus.CHANGED:
                    stack.append(
                        _Compensation(f"pause:{family}", FamilyCommand.PAUSE, family)
                    )

            return done

        except Exception as err:  # noqa: BLE001 - any failure triggers rollback
            failed_step = self._next_step(request, done)
            compensated, errors = self._compensate(stack)
            raise RuntimeError(
                f"move of {request.datatype} aborted at {failed_step}: {err}",
                SagaFailure(
                    failed_step=failed_step,
                    reason=str(err),
                    compensated=compensated,
                    compensation_errors=errors,
                ),
            ) from err

    def _read_all(self, families: list[str]) -> dict[str, FamilyStatus]:
        """Read sequentially, so the audit log has a deterministic order."""
        statuses: dict[str, FamilyStatus] = {}
        for family in families:
            statuses[family] = FamilyActor(family).read_status()
        return statuses


    def _validate(
        self, request: MoveDatatypeRequest, statuses: dict[str, FamilyStatus]
    ) -> None:
        """Reject impossible moves before anything has been mutated.

        Failing here costs no compensation, which is the cheapest place to fail.
        """
        if request.source_family == request.target_family:
            raise RuntimeError("source and target family are the same")
        if request.datatype not in statuses[request.source_family].datatypes:
            raise RuntimeError(
                f"{request.datatype} is not owned by {request.source_family}",
            )
        if request.datatype in statuses[request.target_family].datatypes:
            raise RuntimeError(
                f"{request.datatype} is already owned by {request.target_family}",
            )

    def _compensate(self, stack: list[_Compensation]) -> tuple[list[str], list[str]]:
        """Unwind LIFO, best effort.

        A failing compensation must not abort the unwind - stopping halfway would
        strand the cluster in a worse state than finishing the remaining ones.
        Errors are collected and reported instead.
        """
        compensated: list[str] = []
        errors: list[str] = []
        for index, item in enumerate(reversed(stack)):
            actor = FamilyActor(item.family)
            try:
                match item.command:
                    case FamilyCommand.PAUSE:
                        actor.pause()
                    case FamilyCommand.RESUME:
                        actor.resume()
                    case FamilyCommand.PATCH_CONFIG:
                        actor.patch_config(item.datatypes)
                compensated.append(item.label)
            except Exception as err:  # noqa: BLE001 - keep unwinding
                errors.append(f"{item.label}: {err}")
        return compensated, errors

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
