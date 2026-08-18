"""Pinned Restate harness and isolated object-key fixtures."""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import restate
from restate.types import HarnessEnvironment

from poc.common.cluster import ENV_CLUSTER_ROOT, ChaosRule, MockCluster
from poc.common.domain import MoveDatatypeRequest
from poc.common.scenarios import REQUEST, SEED, SOURCE, TARGET, Scenario
from poc.restate.app import app

RESTATE_TEST_IMAGE = "docker.io/restatedev/restate:1.7.2"


@dataclass(frozen=True)
class RestateCase:
    cluster: MockCluster
    source: str
    target: str
    request: MoveDatatypeRequest

    def scenario(self, original: Scenario) -> Scenario:
        rules: dict[str, ChaosRule] = {}
        for key, rule in original.chaos.items():
            family, operation = key.split(":", 1)
            keyed_family = self.source if family == SOURCE else self.target
            rules[f"{keyed_family}:{operation}"] = rule
        return Scenario(
            name=original.name,
            description=original.description,
            chaos=rules,
            expect_failure=original.expect_failure,
        )

    def normalize(self, value: str) -> str:
        return value.replace(self.source, SOURCE).replace(self.target, TARGET)


@pytest.fixture(scope="session")
async def restate_env() -> AsyncIterator[HarnessEnvironment]:
    """One server, pinned to 1.7.2, forcing replay at suspension points."""
    # Colima exposes its socket from a host path that the Ryuk helper cannot
    # mount back into the VM. The harness already stops its container in a
    # context manager, so disabling that redundant reaper is safe here.
    previous = os.environ.get("TESTCONTAINERS_RYUK_DISABLED")
    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
    try:
        async with restate.create_test_harness(
            app,
            restate_image=RESTATE_TEST_IMAGE,
            always_replay=True,
        ) as environment:
            yield environment
    finally:
        if previous is None:
            os.environ.pop("TESTCONTAINERS_RYUK_DISABLED", None)
        else:
            os.environ["TESTCONTAINERS_RYUK_DISABLED"] = previous


@pytest.fixture
def restate_case(tmp_path: Path) -> Iterator[RestateCase]:
    """Use never-repeated object keys so server-side K/V cannot leak across tests."""
    suffix = uuid.uuid4().hex
    source = f"{SOURCE}-{suffix}"
    target = f"{TARGET}-{suffix}"
    cluster = MockCluster(tmp_path / "restate-cluster")
    cluster.seed({source: SEED[SOURCE], target: SEED[TARGET]})
    previous = os.environ.get(ENV_CLUSTER_ROOT)
    os.environ[ENV_CLUSTER_ROOT] = str(cluster.root)
    yield RestateCase(
        cluster=cluster,
        source=source,
        target=target,
        request=REQUEST.model_copy(
            update={"source_family": source, "target_family": target}
        ),
    )
    if previous is None:
        os.environ.pop(ENV_CLUSTER_ROOT, None)
    else:
        os.environ[ENV_CLUSTER_ROOT] = previous
