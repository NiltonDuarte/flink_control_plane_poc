"""Pinned Restate harness and isolated object-key fixtures."""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import tempfile
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
from hypercorn.asyncio import serve
from hypercorn.config import Config
from restate.client import Client

from poc.common.cluster import ENV_CLUSTER_ROOT, ChaosRule, MockCluster
from poc.common.domain import MoveDatatypeRequest
from poc.common.scenarios import REQUEST, SEED, SOURCE, TARGET, Scenario
from poc.restate.app import app


@dataclass
class HarnessEnvironment:
    client: Client
    ingress_url: str


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


async def _wait_for_port(port: int, timeout: float = 15.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.settimeout(0.1)
            if connection.connect_ex(("127.0.0.1", port)) == 0:
                return
        await asyncio.sleep(0.1)
    raise AssertionError(f"Port {port} did not open in time")


@pytest.fixture(scope="session")
async def restate_env() -> AsyncIterator[HarnessEnvironment]:
    """One server, running locally. App runs in-process to share os.environ."""
    restate_bin = shutil.which("restate-server") or shutil.which("restate")
    if not restate_bin:
        pytest.fail("restate-server or restate binary not found in PATH")

    config = Config()
    config.bind = ["127.0.0.1:9080"]
    shutdown_event = asyncio.Event()
    server_task = asyncio.create_task(
        serve(app, config, shutdown_trigger=shutdown_event.wait)
    )

    with tempfile.TemporaryDirectory() as db_dir:
        cmd = [restate_bin]
        if "restate-server" not in restate_bin:
            cmd.append("server")

        env = {**os.environ, "RESTATE_BIFROST__STORAGE__PATH": db_dir}
        restate_proc = subprocess.Popen(  # noqa: ASYNC220
            cmd,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        try:
            await _wait_for_port(9080)
            await _wait_for_port(8080)
            await _wait_for_port(9070)

            async with httpx.AsyncClient() as http:
                response = await http.post(
                    "http://127.0.0.1:9070/deployments",
                    json={"uri": "http://127.0.0.1:9080"},
                    headers={"content-type": "application/json"},
                    timeout=25.0,
                )
                response.raise_for_status()

            async with httpx.AsyncClient(base_url="http://127.0.0.1:8080") as http:
                client = Client(http)
                yield HarnessEnvironment(
                    client=client, ingress_url="http://127.0.0.1:8080"
                )

        finally:
            restate_proc.terminate()
            restate_proc.wait()
            shutdown_event.set()
            await server_task


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
