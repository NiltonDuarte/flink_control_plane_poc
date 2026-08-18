"""Restate service-process crash and journal recovery."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import subprocess
import sys
import uuid
from pathlib import Path

import httpx
import pytest
from restate.client import Client

from poc.common.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.common.scenarios import REQUEST, SEED, SOURCE, TARGET
from poc.restate.cluster_steps import ENV_STEP_DELAY
from poc.restate.saga import run
from tests.restate.conftest import RESTATE_TEST_IMAGE

pytestmark = [pytest.mark.crash, pytest.mark.restate_crash]

REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICE_PORT = 9080


def _spawn_service(cluster_root: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "poc.restate.run_service"],
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT),
            ENV_CLUSTER_ROOT: str(cluster_root),
            ENV_STEP_DELAY: "0.25",
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


async def _wait_for_service(
    process: subprocess.Popen[bytes], timeout: float = 20
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if process.poll() is not None:
            _, stderr = process.communicate()
            raise AssertionError(
                f"service exited early: {stderr.decode(errors='replace')}"
            )
        with socket.socket() as connection:
            connection.settimeout(0.1)
            if connection.connect_ex(("127.0.0.1", SERVICE_PORT)) == 0:
                return
        await asyncio.sleep(0.05)
    raise AssertionError("Restate ASGI service did not listen on port 9080")


async def _wait_for_ops(
    cluster: MockCluster, process: subprocess.Popen[bytes], count: int
) -> None:
    deadline = asyncio.get_running_loop().time() + 30
    while asyncio.get_running_loop().time() < deadline:
        if process.poll() is not None:
            _, stderr = process.communicate()
            raise AssertionError(
                f"service exited early: {stderr.decode(errors='replace')}"
            )
        if len(cluster.audit(successful_only=True)) >= count:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"saga did not reach {count} operations")


def _kill(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGKILL)
        process.wait(timeout=10)
    process.communicate()


async def test_saga_survives_asgi_service_kill(tmp_path: Path) -> None:
    """The server journals progress while the stateless SDK endpoint restarts."""
    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
    from restate.harness import create_restate_container

    cluster = MockCluster(tmp_path / "cluster")
    cluster.seed(SEED)
    cluster.set_chaos({})
    service = _spawn_service(cluster.root)

    try:
        await _wait_for_service(service)
        with create_restate_container(
            restate_image=RESTATE_TEST_IMAGE,
            always_replay=True,
        ) as runtime:
            response = runtime.get_admin_client().post(
                "/deployments",
                headers={"content-type": "application/json"},
                json={"uri": f"http://host.docker.internal:{SERVICE_PORT}"},
            )
            assert response.is_success, response.text

            # Restate may keep the ingress request open across endpoint recovery;
            # the SDK helper's default HTTP timeout is too short for that test.
            async with httpx.AsyncClient(
                base_url=runtime.ingress_url(),
                http2=True,
                timeout=130,
            ) as http_client:
                client = Client(http_client)
                result = asyncio.create_task(
                    client.workflow_call(
                        run,
                        key=f"crash-{uuid.uuid4()}",
                        arg=REQUEST,
                    )
                )
                await _wait_for_ops(cluster, service, 3)
                completed_before_kill = [
                    (entry.op, entry.family)
                    for entry in cluster.audit(successful_only=True)
                ]
                _kill(service)

                await asyncio.sleep(0.5)
                service = _spawn_service(cluster.root)
                await _wait_for_service(service)
                steps = await asyncio.wait_for(result, timeout=120)
    finally:
        _kill(service)

    assert steps == [
        f"pause:{SOURCE}",
        f"pause:{TARGET}",
        f"patch:{SOURCE}",
        f"patch:{TARGET}",
        f"resume:{SOURCE}",
        f"resume:{TARGET}",
    ]
    assert cluster.read(SOURCE).datatypes == ["impressions"]
    assert cluster.read(TARGET).datatypes == ["views", "clicks"]

    successful = [(entry.op, entry.family) for entry in cluster.audit(successful_only=True)]
    # The first family's completed pause is journaled and is never replayed.
    assert successful.count(("trigger_savepoint", SOURCE)) == 1
    assert successful.count(("suspend_job", SOURCE)) == 1
    assert successful[:2] == completed_before_kill[:2]
    # An operation killed between its external commit and journal write may run
    # once more, but completed phases do not restart from the beginning.
    assert 8 <= len(successful) <= 9, successful
