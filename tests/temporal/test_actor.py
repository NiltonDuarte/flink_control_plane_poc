"""The actor guarantee: one writer per job family.

Temporal has no Virtual Object, so the guarantee is built from two pieces - a
workflow id derived from the family name, and a lock inside the actor. This is
the test that the second piece is load-bearing.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from temporalio.client import Client, WorkflowUpdateFailedError

from poc.common.cluster import ChaosRule, MockCluster
from poc.common.domain import FamilyState
from poc.common.scenarios import SOURCE
from poc.temporal.actor import FlinkJobFamilyActor, actor_id
from poc.temporal.worker import build_worker
from tests.conftest import ops


async def test_concurrent_pauses_are_serialized(
    client: Client, cluster: MockCluster
) -> None:
    """Five simultaneous pauses must produce one savepoint, not five.

    Temporal delivers concurrent updates as concurrent tasks. Without the lock,
    all five handlers would read state == RUNNING before any of them wrote
    SUSPENDED, and all five would take a savepoint and suspend - the classic
    lost-update race. With it, the first wins and the rest observe SUSPENDED and
    no-op, which is what "queues concurrent commands implicitly" means in the RFC.
    """
    task_queue = f"tq-{uuid.uuid4()}"
    async with build_worker(client, task_queue):
        handle = await client.start_workflow(
            FlinkJobFamilyActor.run,
            args=[SOURCE],
            id=actor_id(SOURCE),
            task_queue=task_queue,
        )
        results = await asyncio.gather(
            *(
                handle.execute_update(FlinkJobFamilyActor.pause, id=f"pause-{index}")
                for index in range(5)
            )
        )
        state = await handle.query(FlinkJobFamilyActor.current_state)

    # Exactly one call did the work; the other four returned None (already suspended).
    assert sum(1 for result in results if result is not None) == 1
    assert ops(cluster) == [
        f"trigger_savepoint({SOURCE})",
        f"suspend_job({SOURCE})",
    ]
    assert state == FamilyState.SUSPENDED


async def test_actor_state_survives_across_commands(
    client: Client, cluster: MockCluster
) -> None:
    """The actor holds its own state - no external store is consulted between commands."""
    task_queue = f"tq-{uuid.uuid4()}"
    async with build_worker(client, task_queue):
        handle = await client.start_workflow(
            FlinkJobFamilyActor.run,
            args=[SOURCE],
            id=actor_id(SOURCE),
            task_queue=task_queue,
        )

        await handle.execute_update(FlinkJobFamilyActor.pause, id="p1")
        assert (
            await handle.query(FlinkJobFamilyActor.current_state)
            == FamilyState.SUSPENDED
        )

        # A second pause is a no-op precisely because the actor remembers.
        assert await handle.execute_update(FlinkJobFamilyActor.pause, id="p2") is None

        assert await handle.execute_update(FlinkJobFamilyActor.resume, id="r1") is True
        assert (
            await handle.query(FlinkJobFamilyActor.current_state) == FamilyState.RUNNING
        )

        # Likewise a redundant resume.
        assert await handle.execute_update(FlinkJobFamilyActor.resume, id="r2") is False

    assert ops(cluster) == [
        f"trigger_savepoint({SOURCE})",
        f"suspend_job({SOURCE})",
        f"resume_job({SOURCE})",
    ]


async def test_restore_refreshes_stale_cache_after_lost_response(
    client: Client, cluster: MockCluster
) -> None:
    """Compensation reads the cluster when a landed pause never returned."""
    cluster.set_chaos(
        {f"{SOURCE}:suspend_job": ChaosRule(mode="lost_response", times=4)}
    )
    task_queue = f"tq-{uuid.uuid4()}"
    async with build_worker(client, task_queue):
        handle = await client.start_workflow(
            FlinkJobFamilyActor.run,
            args=[SOURCE],
            id=actor_id(SOURCE),
            task_queue=task_queue,
        )

        with pytest.raises(WorkflowUpdateFailedError):
            await handle.execute_update(FlinkJobFamilyActor.pause, id="lost-pause")

        assert (
            await handle.query(FlinkJobFamilyActor.current_state) == FamilyState.RUNNING
        )
        assert cluster.read(SOURCE).state == FamilyState.SUSPENDED

        changed = await handle.execute_update(
            FlinkJobFamilyActor.restore,
            args=[FamilyState.RUNNING, None],
            id="restore-after-lost-pause",
        )
        assert changed is True
        assert (
            await handle.query(FlinkJobFamilyActor.current_state) == FamilyState.RUNNING
        )
        assert cluster.read(SOURCE).state == FamilyState.RUNNING
