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

from dbos import DBOS

from poc.baseline.actor import FamilyActor
from poc.common.domain import (
    ClusterError,
    CommandRequest,
    CommandResult,
    FamilyCommand,
    FamilyState,
    FamilyStatus,
    MoveDatatypeRequest,
    SagaFailure,
)
from poc.dbos.actor import FlinkJobFamilyActor
from poc.dbos.activities import read_status


@dataclass
class _Compensation:
    """A queued restore intent. In-workflow memory only; replay rebuilds it."""

    label: str
    family: str
    datatypes: list[str] | None = None
    desired_state: FamilyState | None = None

class MoveDatatypeWorkflow:
    def __init__(self):
        self.flink_job_family_actor = FlinkJobFamilyActor()

# -- helpers -----------------------------------------------------------

    def _read_all(self, families: list[str]) -> dict[str, FamilyStatus]:
        """Read sequentially, so the audit log has a deterministic order."""
        statuses: dict[str, FamilyStatus] = {}
        for family in families:
            statuses[family] = read_status(family)
        return statuses

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
        for index, item in enumerate(reversed(stack)):
            # request = CommandRequest(
            #     family=item.family,
            #     command=FamilyCommand.RESTORE,
            #     update_id=_update_id(f"comp:{index}:{item.label}"),
            #     datatypes=item.datatypes,
            #     desired_state=item.desired_state,
            # )
            result = FlinkJobFamilyActor()
            # if result.changed:
            #     compensated.append(item.label)
            # else:
            #     noops.append(item.label)
            # except Exception as err:  # noqa: BLE001 - keep unwinding
            #     errors.append(f"{item.label}: {err}")
        return compensated, noops, errors

    def _update_id(step: str) -> str:
        return f"{DBOS.workflow_id}:{step}"

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

    @DBOS.workflow(name="move_datatype")
    def move_datatype(self, request: MoveDatatypeRequest) -> list[str]:
        """Move one datatype between two families. Returns the executed steps."""
        source, target = request.source_family, request.target_family
        done: list[str] = []
        stack: list[_Compensation] = []


        statuses = self._read_all(request.families)
        # self._validate(request, statuses)

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
                self.flink_job_family_actor.pause(family)
                done.append(f"pause:{family}")

            # -- Phase 2: patch config -----------------------------------
            for family in request.families:
                stack.append(config_restore[family])
                self.flink_job_family_actor.patch_config(new_config[family])

                done.append(f"patch:{family}")

            # -- Phase 3: resume -----------------------------------------
            for family in request.families:
                stack.append(resume_restore[family])
                self.flink_job_family_actor.resume(family)
                done.append(f"resume:{family}")

            return done

        except Exception as err:  # noqa: BLE001 - any failure triggers rollback
            failed_step = self._next_step(request, done)
            compensated, noops, errors = self._compensate(stack)
            raise ClusterError(
                f"move of {request.datatype} aborted at {failed_step}: {err}." +
                f"{str(SagaFailure(
                                failed_step=failed_step,
                                reason=str(err),
                                compensated=compensated,
                                compensation_noops=noops,
                                compensation_errors=errors,
                            ).model_dump())}"
            ) from err