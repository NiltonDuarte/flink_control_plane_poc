"""Restate-specific handler payloads.

The shared domain models remain engine-neutral. These small envelopes exist only
because a Restate handler accepts a single request value.
"""

from pydantic import BaseModel

from poc.common.domain import FamilyState


class Empty(BaseModel):
    """Explicit empty input for no-argument object commands."""


class PatchConfigRequest(BaseModel):
    datatypes: list[str]


class RestoreRequest(BaseModel):
    desired_state: FamilyState | None = None
    datatypes: list[str] | None = None


class ActorState(BaseModel):
    state: FamilyState
