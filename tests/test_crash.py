"""Worker crash durability.

The one test that cannot use the time-skipping server. It proves the property
durable execution actually sells: kill the process running a half-finished saga,
start a new one, and the saga carries on from where it stopped rather than
restarting or stalling.

Marked `crash` and deselected by default - it needs a real server, a real
subprocess, and real seconds (the in-flight activity has to time out before the
server hands it to the new worker).

    uv run pytest -m crash

A note on what "no duplicated side effects" can honestly mean here. Temporal
gives activities *at-least-once* execution, so an activity interrupted by the
kill will run again on recovery; that is by design, and it is why the cluster
operations are idempotent. What must not happen is the saga replaying phases it
already completed. That is the assertion below: each operation appears about
once, not twice over.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment

from poc.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.saga import MoveDatatypeWorkflow
from poc.scenarios import REQUEST, SEED, SOURCE, TARGET

pytestmark = pytest.mark.crash

REPO_ROOT = Path(__file__).parent.parent


@pytest.fixture
async def local_env() -> AsyncIterator[WorkflowEnvironment]:
    """A real dev server. No virtual clock - timeouts must elapse for real."""
    env = await WorkflowEnvironment.start_local(data_converter=pydantic_data_converter)
    yield env
    await env.shutdown()


def _spawn_worker(address: str, task_queue: str, cluster_root: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "tests.worker_process", address, task_queue, str(cluster_root)],
        cwd=REPO_ROOT,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


async def _wait_for_ops(cluster: MockCluster, count: int, timeout: float = 60.0) -> None:
    """Block until the saga has made `count` successful cluster changes."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if len(cluster.audit(successful_only=True)) >= count:
            return
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"only {len(cluster.audit(successful_only=True))} ops after {timeout}s"
    )


async def test_saga_survives_worker_kill(local_env: WorkflowEnvironment, tmp_path: Path) -> None:
    cluster = MockCluster(tmp_path / "cluster")
    cluster.seed(SEED)
    cluster.set_chaos({})

    address = local_env.client.service_client.config.target_host
    task_queue = f"crash-{uuid.uuid4()}"
    worker = _spawn_worker(address, task_queue, cluster.root)

    try:
        handle = await local_env.client.start_workflow(
            MoveDatatypeWorkflow.run,
            REQUEST,
            id=f"crash-{uuid.uuid4()}",
            task_queue=task_queue,
        )

        # Let the saga get properly underway - past the first pause, into the
        # second - so the kill lands mid-transaction rather than before it began.
        await _wait_for_ops(cluster, 3)
        ops_before_kill = len(cluster.audit(successful_only=True))

        worker.send_signal(signal.SIGKILL)
        worker.wait(timeout=10)

        # Nothing can progress with no worker: the saga is parked in the server,
        # not lost, and not spinning.
        await asyncio.sleep(2)
        assert len(cluster.audit(successful_only=True)) == ops_before_kill

        worker = _spawn_worker(address, task_queue, cluster.root)
        steps = await asyncio.wait_for(handle.result(), timeout=120)
    finally:
        worker.send_signal(signal.SIGKILL)
        worker.wait(timeout=10)

    # It finished, and it finished correctly.
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

    # It resumed rather than restarted. A from-scratch replay would run the
    # 8-operation happy path twice; at-least-once activity semantics allow the
    # one interrupted operation to repeat, so the ceiling is 8 + a small margin.
    successful = cluster.audit(successful_only=True)
    assert 8 <= len(successful) <= 11, [str(entry) for entry in successful]
