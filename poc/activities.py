"""Temporal activities: the only code that touches the cluster.

Each activity is a thin wrapper over `MockCluster`. Keeping them thin is what
makes the production swap cheap - the real implementation changes `cluster.py`,
and these signatures stay put.

The cluster root is resolved per call from the environment rather than captured
at import time, so a test can point each case at its own tmpdir.
"""

from __future__ import annotations

from temporalio import activity

from poc.cluster import MockCluster
from poc.domain import FamilyStatus


@activity.defn
async def read_status(family: str) -> FamilyStatus:
    """Read a family's current state - used once per actor to seed its cache."""
    return MockCluster.from_env().read(family)


@activity.defn
async def trigger_savepoint(family: str) -> str:
    return MockCluster.from_env().trigger_savepoint(family)


@activity.defn
async def suspend_job(family: str) -> None:
    MockCluster.from_env().suspend_job(family)


@activity.defn
async def resume_job(family: str) -> None:
    MockCluster.from_env().resume_job(family)


@activity.defn
async def patch_configmap(family: str, datatypes: list[str]) -> list[str]:
    """Replace routing config; the return value is informational only."""
    return MockCluster.from_env().patch_configmap(family, datatypes)


ALL_ACTIVITIES = [
    read_status,
    trigger_savepoint,
    suspend_job,
    resume_job,
    patch_configmap,
]
