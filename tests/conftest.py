"""Shared fixtures.

One time-skipping Temporal environment for the whole session, because starting
one costs seconds and the tests do not need isolation at that level. What they do
need is a clean *cluster* and clean *actors* per test:

* each test gets its own tmpdir cluster, pointed at by ``POC_CLUSTER_ROOT``;
* actors are terminated after each test, since they are long-lived by design and
  would otherwise carry cached state into the next one.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment

from poc.common.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.common.domain import MoveDatatypeRequest, SagaFailure
from poc.common.scenarios import REQUEST, SEED, Scenario
from poc.temporal.actor import actor_id
from poc.temporal.application import app
from poc.temporal.saga import MoveDatatypeWorkflow
from poc.temporal.worker import build_worker


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
        except Exception:  # noqa: BLE001, S110 - not started is the normal case
            pass


async def run_scenario(
    client: Client,
    cluster: MockCluster,
    scenario: Scenario,
) -> tuple[list[str] | None, Exception | None]:
    """Run a scenario end to end. Returns (steps, error) - exactly one is set."""
    cluster.set_chaos(scenario.chaos)
    return await run_move(client, request=REQUEST, workflow_name=scenario.name)


async def run_move(
    client: Client,
    *,
    request: MoveDatatypeRequest,
    workflow_name: str,
) -> tuple[list[str] | None, Exception | None]:
    """Run one move request end to end and capture its expected domain failure."""
    async with build_worker(client):
        try:
            steps: list[str] = await app.client(client).execute_workflow(
                MoveDatatypeWorkflow.run,
                request,
                id=f"move-{workflow_name}-{uuid.uuid4()}",
            )
            return steps, None
        except Exception as err:  # noqa: BLE001 - failure scenarios expect this
            return None, err


def ops(cluster: MockCluster, *, successful_only: bool = True) -> list[str]:
    """Flatten effective mutations, or all attempts, for order assertions."""
    return [
        f"{entry.op}({entry.family})"
        for entry in cluster.audit(effective_only=successful_only)
    ]


def saga_application_error(err: Exception) -> ApplicationError:
    """Find the saga's ApplicationError through Temporal's exception wrappers."""
    cause: BaseException | None = err
    while cause is not None:
        if isinstance(cause, ApplicationError) and cause.details:
            try:
                SagaFailure.model_validate(cause.details[0])
            except ValueError:
                pass
            else:
                return cause
        cause = cause.__cause__ or getattr(cause, "cause", None)
    raise AssertionError(f"no SagaFailure detail found in {err!r}")


def saga_failure(err: Exception) -> SagaFailure:
    """Decode the structured failure detail through Temporal's exception wrapper."""
    return SagaFailure.model_validate(saga_application_error(err).details[0])
