"""Domain models and error taxonomy.

Deliberately free of any Temporal or infrastructure import. Everything here is
plain Python + Pydantic so it can be reused unchanged when the mock cluster is
replaced by real Flink/Kubernetes calls.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------
# The split between transient and permanent is the whole error taxonomy of the
# POC. It is enforced by configuration rather than by try/except plumbing:
# Temporal's RetryPolicy matches on the exception *type name*, so listing
# "PermanentClusterError" in non_retryable_error_types is enough to make
# permanent failures abort on the first attempt. See poc/saga.py.


class ClusterError(Exception):
    """Base class for anything the (mock) cluster can fail with."""


class TransientClusterError(ClusterError):
    """A retryable fault: the call may succeed if attempted again.

    Stands in for the real world's 503s, optimistic-concurrency conflicts on a
    resource patch, and API-server timeouts.
    """


class PermanentClusterError(ClusterError):
    """A non-retryable fault: retrying cannot help.

    Stands in for a malformed spec, a missing job family, or a rejected state
    transition. Retrying these only burns the retry budget and delays the
    compensation that actually needs to happen.
    """


# --------------------------------------------------------------------------
# Models
# --------------------------------------------------------------------------


class FamilyState(str, Enum):
    """Runtime state of a Flink job family.

    Instant transitions only: the POC has no SUSPENDING/STARTING intermediates
    because the mock applies changes synchronously (see POC_SCOPE.md).
    """

    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"


class FamilyStatus(BaseModel):
    """The full persisted state of one job family - the mock's unit of storage.

    `generation` increments on every mutation. It is not used for control flow;
    it exists so the audit log and the on-disk files make it obvious how many
    times a family was touched, including during compensation.
    """

    family: str
    state: FamilyState = FamilyState.RUNNING
    datatypes: list[str] = Field(default_factory=list)
    generation: int = 0
    savepoint_uri: str | None = None


class FamilyCommand(str, Enum):
    """The actor handlers, as named by the RFC's Virtual Actor Handlers section."""

    PAUSE = "pause"
    RESUME = "resume"
    PATCH_CONFIG = "patch_config"


class MoveDatatypeRequest(BaseModel):
    """Input contract for the saga.

    A move is two-sided: `datatype` leaves `source_family` and joins
    `target_family`, so both configs are patched in phase 2. That is precisely
    why the saga needs all-or-nothing semantics - a half-applied move leaves the
    datatype owned by nobody (or by both), and routing is broken either way.
    """

    datatype: str
    source_family: str
    target_family: str

    @property
    def families(self) -> list[str]:
        """Families touched by this move, in a stable order.

        Order is fixed so the audit log is deterministic and directly assertable.
        """
        return [self.source_family, self.target_family]


class CommandRequest(BaseModel):
    """What the saga asks an actor to do, via the proxy activity.

    `update_id` is the important field. It is derived deterministically from the
    workflow and step, so a retried proxy activity dedupes onto the same Temporal
    update instead of executing the command a second time. Without it, an
    activity timeout during a pause would pause twice.
    """

    family: str
    command: FamilyCommand
    update_id: str
    datatypes: list[str] | None = None


class CommandResult(BaseModel):
    """What an actor handler returns, flattened into one shape.

    `changed` is the field the saga actually cares about: a pause of an
    already-suspended family changed nothing, so no compensating resume is owed.
    Recording compensations for no-op commands is how rollbacks end up resuming
    families that were never running to begin with.
    """

    changed: bool = True
    savepoint_uri: str | None = None
    previous_datatypes: list[str] | None = None


class SagaFailure(BaseModel):
    """Reported back when a saga aborts, after compensation has run."""

    failed_step: str
    reason: str
    compensated: list[str]
    compensation_errors: list[str] = Field(default_factory=list)
