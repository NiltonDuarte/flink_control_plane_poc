from __future__ import annotations

from dataclasses import dataclass

from poc.baseline.actor import FamilyActor, UpdateStatus
from poc.common.domain import (
    FamilyCommand,
    FamilyState,
    FamilyStatus,
    MoveDatatypeRequest,
    SagaFailure,
)


@dataclass
class _Compensation:
    """A queued restore intent. In-workflow memory only; replay rebuilds it."""

    label: str
    family: str
    datatypes: list[str] | None = None
    desired_state: FamilyState | None = None


class MoveDatatypeWorkflow:
    def run(self, request: MoveDatatypeRequest) -> list[str]:
        """Move one datatype between two families. Returns the executed steps."""
        source, target = request.source_family, request.target_family
        done: list[str] = []
        stack: list[_Compensation] = []
        actors = {source: FamilyActor(source), target: FamilyActor(target)}

        statuses = self._read_all(request.families)
        self._validate(request, statuses)

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
                actors[family].pause()
                done.append(f"pause:{family}")
            # -- Phase 2: patch config -----------------------------------
            for family in request.families:
                stack.append(config_restore[family])
                actors[family].patch_config(new_config[family])
                done.append(f"patch:{family}")

            # -- Phase 3: resume -----------------------------------------
            for family in request.families:
                stack.append(resume_restore[family])
                actors[family].resume()
                done.append(f"resume:{family}")

            return done

        except Exception as err:  # noqa: BLE001, RUF100
            failed_step = self._next_step(request, done)
            compensated, noops, errors = self._compensate(stack)
            raise RuntimeError(
                f"move of {request.datatype} aborted at {failed_step}: {err}",
                SagaFailure(
                    failed_step=failed_step,
                    reason=str(err),
                    compensated=compensated,
                    compensation_noops=noops,
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

    def _compensate(
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
        for item in reversed(stack):
            try:
                changed = FamilyActor(item.family).restore(
                    item.desired_state, item.datatypes
                )
                if changed:
                    compensated.append(item.label)
                else:
                    noops.append(item.label)
            except Exception as err:  # noqa: BLE001 - keep unwinding
                errors.append(f"{item.label}: {err}")
        return compensated, noops, errors

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
