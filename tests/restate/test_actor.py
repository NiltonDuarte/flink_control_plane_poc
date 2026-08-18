"""Virtual Object serialization, persisted state, and stale-cache repair."""

from __future__ import annotations

import asyncio

import pytest
import restate
from restate.types import HarnessEnvironment

from poc.common.cluster import ChaosRule
from poc.common.domain import FamilyState
from poc.restate.actor import current_state, pause, restore, resume
from poc.restate.models import Empty, RestoreRequest
from tests.restate.conftest import RestateCase

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_concurrent_commands_serialize_to_one_effective_mutation(
    restate_env: HarnessEnvironment, restate_case: RestateCase
) -> None:
    results = await asyncio.gather(
        *(
            restate_env.client.object_call(pause, restate_case.source, Empty())
            for _ in range(5)
        )
    )

    assert sum(result.changed for result in results) == 1
    assert [entry.op for entry in restate_case.cluster.audit()] == [
        "trigger_savepoint",
        "suspend_job",
    ]
    state = await restate_env.client.object_call(
        current_state, restate_case.source, Empty()
    )
    assert state.state == FamilyState.SUSPENDED


async def test_state_persists_across_calls(
    restate_env: HarnessEnvironment, restate_case: RestateCase
) -> None:
    first_pause = await restate_env.client.object_call(
        pause, restate_case.source, Empty()
    )
    second_pause = await restate_env.client.object_call(
        pause, restate_case.source, Empty()
    )
    first_resume = await restate_env.client.object_call(
        resume, restate_case.source, Empty()
    )
    second_resume = await restate_env.client.object_call(
        resume, restate_case.source, Empty()
    )

    assert first_pause.changed is True
    assert second_pause.changed is False
    assert first_resume.changed is True
    assert second_resume.changed is False
    assert [entry.op for entry in restate_case.cluster.audit()] == [
        "trigger_savepoint",
        "suspend_job",
        "resume_job",
    ]


async def test_restore_refreshes_stale_state_after_lost_response(
    restate_env: HarnessEnvironment, restate_case: RestateCase
) -> None:
    restate_case.cluster.set_chaos(
        {f"{restate_case.source}:suspend_job": ChaosRule(mode="lost_response", times=4)}
    )
    with pytest.raises(restate.HttpError):
        await restate_env.client.object_call(pause, restate_case.source, Empty())

    cached = await restate_env.client.object_call(
        current_state, restate_case.source, Empty()
    )
    assert cached.state == FamilyState.RUNNING
    assert restate_case.cluster.read(restate_case.source).state == FamilyState.SUSPENDED

    result = await restate_env.client.object_call(
        restore,
        restate_case.source,
        RestoreRequest(desired_state=FamilyState.RUNNING),
    )
    assert result.changed is True
    refreshed = await restate_env.client.object_call(
        current_state, restate_case.source, Empty()
    )
    assert refreshed.state == FamilyState.RUNNING
    assert restate_case.cluster.read(restate_case.source).state == FamilyState.RUNNING
