"""Temporal activities: the only code that touches the cluster.

Each activity is a thin wrapper over `MockCluster`. Keeping them thin is what
makes the production swap cheap - the real implementation changes `cluster.py`,
and these signatures stay put.

The cluster root is resolved per call from the environment rather than captured
at import time, so a test can point each case at its own tmpdir.
"""

from __future__ import annotations

from poc.common.cluster import MockCluster
from poc.common.domain import FamilyStatus
from poc.temporal.application import TASK_QUEUE, app


@app.activity(task_queue=TASK_QUEUE)
async def read_status(family: str) -> FamilyStatus:
    """Read a family's current state - used once per actor to seed its cache."""
    return MockCluster.from_env().read(family)


@app.activity(task_queue=TASK_QUEUE)
async def trigger_savepoint(family: str, operation_id: str) -> str:
    return MockCluster.from_env().trigger_savepoint(family, operation_id)


@app.activity(task_queue=TASK_QUEUE)
async def suspend_job(family: str, operation_id: str) -> bool:
    return MockCluster.from_env().suspend_job(family, operation_id)


@app.activity(task_queue=TASK_QUEUE)
async def resume_job(family: str, operation_id: str) -> bool:
    return MockCluster.from_env().resume_job(family, operation_id)


@app.activity(task_queue=TASK_QUEUE)
async def patch_configmap(family: str, datatypes: list[str], operation_id: str) -> bool:
    """Replace routing config and report whether this attempt changed it."""
    return MockCluster.from_env().patch_configmap(family, datatypes, operation_id)
