"""Shared fixtures.

Two things every test needs, and one that only the engine-parameterized ones do.

A clean **cluster** per test (its own tmpdir, pointed at by ``POC_CLUSTER_ROOT``)
and clean **actors** per test, since actors are long-lived by design and would
otherwise carry cached state into the next one.

The Temporal environment is session-scoped because starting one costs seconds and
the tests do not need isolation at that level. It lives here rather than in
`tests/temporal/` so that `tests/temporal/*` inherits it and the
engine-parameterized `engine` fixture can pull it on demand - see `engines.py`.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment

from poc.adapters.temporal.actor import actor_id
from poc.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.scenarios import SEED, Scenario
from tests.engines import HARNESSES, EngineHarness


@pytest.fixture(scope="session")
async def env() -> AsyncIterator[WorkflowEnvironment]:
    """Time-skipping environment: retry backoff costs no wall-clock time."""
    environment = await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter
    )
    yield environment
    await environment.shutdown()


@pytest.fixture
def cluster(tmp_path: Path) -> Iterator[MockCluster]:
    """A freshly seeded cluster, wired to the activities via the environment."""
    instance = MockCluster(tmp_path / "cluster")
    instance.seed(SEED)
    previous = os.environ.get(ENV_CLUSTER_ROOT)
    os.environ[ENV_CLUSTER_ROOT] = str(instance.root)
    yield instance
    if previous is None:
        os.environ.pop(ENV_CLUSTER_ROOT, None)
    else:
        os.environ[ENV_CLUSTER_ROOT] = previous


@pytest.fixture
async def client(env: WorkflowEnvironment) -> AsyncIterator[Client]:
    """The environment's client, with actors cleaned up afterwards."""
    yield env.client
    for family in SEED:
        try:
            await env.client.get_workflow_handle(actor_id(family)).terminate()
        except Exception:  # noqa: BLE001 - not started is the normal case
            pass


@pytest.fixture(params=list(HARNESSES), ids=lambda name: name)
def engine(request: pytest.FixtureRequest) -> EngineHarness:
    """One engine to run the scenario matrix against.

    Parameterized, so every scenario assertion is made once per registered
    engine. Today that is Temporal alone; the point is that adding Restate adds
    a column rather than a second test suite.
    """
    return HARNESSES[request.param](request)


async def run_scenario(
    engine: EngineHarness,
    cluster: MockCluster,
    scenario: Scenario,
) -> tuple[list[str] | None, Exception | None]:
    """Run a scenario end to end. Returns (steps, error) - exactly one is set."""
    cluster.set_chaos(scenario.chaos)
    return await engine.run_saga(scenario)


def ops(cluster: MockCluster, *, successful_only: bool = True) -> list[str]:
    """Audit log flattened to ``"op(family)"`` strings, for order assertions."""
    return [
        f"{entry.op}({entry.family})"
        for entry in cluster.audit(successful_only=successful_only)
    ]
