"""What a job family's single-writer actor actually *does*.

The RFC's Virtual Actor Handlers - `pause`, `resume`, `patch_config` - minus
every mechanism that makes them single-writer. Serialization, addressing and
lifetime are the engine's problem and live in the adapters: on Temporal an entity
workflow keyed `family:<name>` with a lock inside it, on Restate a Virtual Object
that serializes by construction. That difference is one of the things the
comparison is meant to surface, so it deliberately does not appear here.

What *is* here is the part both engines must agree on: the cached state, the
precondition checks the RFC calls for ("verify state != SUSPENDED"), and the
order of cluster operations within a handler. `changed`-ness comes from these
checks - a pause of an already-suspended family did nothing, so the saga is owed
no compensating resume, and recording one is how rollbacks end up resuming
families that were never running to begin with.

State is held in the actor, not in an external store, matching the RFC's
"eliminating external database dependency for state resolution".
"""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

from poc.core.domain import FamilyState, FamilyStatus
from poc.core.ports import (
    ClusterOp,
    PatchConfigmap,
    ReadStatus,
    ResumeJob,
    SuspendJob,
    TriggerSavepoint,
)


class FamilyCore:
    """One job family's cached state and the handlers that mutate it."""

    def __init__(self, family: str, state: FamilyState) -> None:
        self.family = family
        self.state = state

    @staticmethod
    def adopt(family: str) -> Generator[ClusterOp, Any, "FamilyCore"]:
        """Seed from the cluster - what a first incarnation of an actor does."""
        status: FamilyStatus = yield ReadStatus(family)
        return FamilyCore(family, status.state)

    # -- handlers (the RFC's Virtual Actor Handlers) -----------------------

    def pause(self) -> Generator[ClusterOp, Any, str | None]:
        """Trigger a savepoint, then suspend. No-op if already suspended.

        Returns the savepoint URI, or None if the family was already suspended -
        which the saga uses to know whether a compensating resume is owed.
        """
        if self.state == FamilyState.SUSPENDED:
            return None
        uri: str = yield TriggerSavepoint(self.family)
        yield SuspendJob(self.family)
        # Only once both operations have landed. A failure in either propagates
        # out of this generator with the cached state untouched, so the actor
        # still believes what the cluster believes.
        self.state = FamilyState.SUSPENDED
        return uri

    def resume(self) -> Generator[ClusterOp, Any, bool]:
        """Return to RUNNING. No-op if already running."""
        if self.state == FamilyState.RUNNING:
            return False
        yield ResumeJob(self.family)
        self.state = FamilyState.RUNNING
        return True

    def patch_config(self, datatypes: list[str]) -> Generator[ClusterOp, Any, list[str]]:
        """Replace the routing config; returns the previous value for rollback."""
        previous: list[str] = yield PatchConfigmap(self.family, datatypes)
        return previous
