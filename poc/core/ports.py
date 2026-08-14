"""The boundary: what the core asks for, and who is obliged to provide it.

Two directions, both defined here.

**Outwards** - the values a core generator yields. They are inert descriptions of
an operation, not calls: a `SuspendJob(family)` is a request that somebody
suspend the job, and the generator is suspended until somebody does. Which is
what keeps the core free of I/O.

**Inwards** - the protocols an adapter satisfies. `ClusterPort` is the five
operations of the mock (and, one day, of the Flink Kubernetes Operator);
`CommandPort` is the saga's hop into a job family's single-writer actor.

The split between the two op families is not cosmetic. `ClusterOp` is
infrastructure - the adapter runs it wherever side effects are allowed to happen.
`Command` is a request to a *family actor*, and it carries a `step` rather than
an idempotency key, because deriving that key is engine-specific: Temporal builds
`workflow_id:run_id:step` for update dedup, Restate has its own. The core names
the step; the adapter turns the name into a key.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from poc.core.domain import CommandResult, FamilyCommand, FamilyStatus


# --------------------------------------------------------------------------
# Cluster operations - what the family handlers yield
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ReadStatus:
    family: str


@dataclass(frozen=True)
class TriggerSavepoint:
    family: str


@dataclass(frozen=True)
class SuspendJob:
    family: str


@dataclass(frozen=True)
class ResumeJob:
    family: str


@dataclass(frozen=True)
class PatchConfigmap:
    family: str
    datatypes: list[str]


ClusterOp = ReadStatus | TriggerSavepoint | SuspendJob | ResumeJob | PatchConfigmap


# --------------------------------------------------------------------------
# Family commands - what the saga yields
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Command:
    """One command for one job family's actor.

    `step` is the saga's own name for this call - ``"pause:family_a"``,
    ``"comp:0:resume:family_a"``. It is stable across replays and retries of the
    same execution, which is the property an adapter needs to build a dedup key
    from it. The core never sees the key itself.
    """

    command: FamilyCommand
    family: str
    step: str
    datatypes: list[str] | None = None


SagaOp = ReadStatus | Command


# --------------------------------------------------------------------------
# Ports - what an adapter must provide
# --------------------------------------------------------------------------


class ClusterPort(Protocol):
    """The infrastructure surface. `poc/cluster.py` is the mock behind it."""

    async def read_status(self, family: str) -> FamilyStatus: ...

    async def trigger_savepoint(self, family: str) -> str: ...

    async def suspend_job(self, family: str) -> None: ...

    async def resume_job(self, family: str) -> None: ...

    async def patch_configmap(self, family: str, datatypes: list[str]) -> list[str]: ...


class CommandPort(Protocol):
    """The saga -> family-actor hop.

    The implementation owns the idempotency-key derivation and whatever the
    engine needs to reach a running actor - on Temporal that is an activity
    holding a client, because a workflow cannot update another workflow directly.
    """

    async def execute(self, command: Command) -> CommandResult: ...
