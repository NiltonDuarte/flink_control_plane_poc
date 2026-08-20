"""Worker crash durability.

The one test that cannot use the time-skipping server. It proves the property
durable execution actually sells: kill the process running a half-finished saga,
start a new one, and the saga carries on from where it stopped rather than
restarting or stalling.

Marked `crash` so it can be selected alone or excluded from the fast subset. It
needs a real server, a real subprocess, and real seconds (the in-flight activity
has to time out before the server hands it to the new worker).

    uv run pytest -m crash

Temporal gives activities *at-least-once* execution, so an interrupted activity
may be attempted again. Stable mutation identities make the repeated attempt a
successful audited no-op. The assertion below therefore requires exactly one
effective side effect for every logical operation.
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

from poc.common.cluster import MockCluster
from poc.common.scenarios import REQUEST, SEED, SOURCE, TARGET
from poc.temporal.application import app
from poc.temporal.saga import MoveDatatypeWorkflow

pytestmark = pytest.mark.crash

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
async def local_env() -> AsyncIterator[WorkflowEnvironment]:
    """A real dev server. No virtual clock - timeouts must elapse for real."""
    env = await WorkflowEnvironment.start_local(data_converter=pydantic_data_converter)
    yield env
    await env.shutdown()


def _spawn_worker(address: str, cluster_root: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tests.temporal.worker_process",
            address,
            str(cluster_root),
        ],
        cwd=REPO_ROOT,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


async def _wait_for_ops(
    cluster: MockCluster,
    count: int,
    worker: subprocess.Popen[bytes],
    timeout: float = 60.0,
) -> None:
    """Block until the saga has made `count` successful cluster changes."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        return_code = worker.poll()
        if return_code is not None:
            _, stderr = worker.communicate()
            detail = stderr.decode(errors="replace").strip()
            raise AssertionError(
                f"worker exited early with code {return_code}: {detail or '<no stderr>'}"
            )
        if len(cluster.audit(effective_only=True)) >= count:
            return
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"only {len(cluster.audit(effective_only=True))} ops after {timeout}s"
    )


async def test_saga_survives_worker_kill(
    local_env: WorkflowEnvironment, tmp_path: Path
) -> None:
    cluster = MockCluster(tmp_path / "cluster")
    cluster.seed(SEED)
    cluster.set_chaos({})

    address = local_env.client.service_client.config.target_host
    client = local_env.client
    worker = _spawn_worker(address, cluster.root)

    try:
        handle = await app.client(client).start_workflow(
            MoveDatatypeWorkflow.run,
            REQUEST,
            id=f"crash-{uuid.uuid4()}",
        )

        # Let the saga get properly underway - past the first pause, into the
        # second - so the kill lands mid-transaction rather than before it began.
        await _wait_for_ops(cluster, 3, worker)
        ops_before_kill = len(cluster.audit(effective_only=True))

        worker.send_signal(signal.SIGKILL)
        worker.wait(timeout=10)
        worker.communicate()

        # Nothing can progress with no worker: the saga is parked in the server,
        # not lost, and not spinning.
        await asyncio.sleep(2)
        assert len(cluster.audit(effective_only=True)) == ops_before_kill

        worker = _spawn_worker(address, cluster.root)
        steps = await asyncio.wait_for(handle.result(), timeout=120)
    finally:
        if worker.poll() is None:
            worker.send_signal(signal.SIGKILL)
            worker.wait(timeout=10)
        worker.communicate()

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

    effective = [
        (entry.op, entry.family) for entry in cluster.audit(effective_only=True)
    ]
    assert effective == [
        ("trigger_savepoint", SOURCE),
        ("suspend_job", SOURCE),
        ("trigger_savepoint", TARGET),
        ("suspend_job", TARGET),
        ("patch_configmap", SOURCE),
        ("patch_configmap", TARGET),
        ("resume_job", SOURCE),
        ("resume_job", TARGET),
    ]
