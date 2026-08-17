"""Run any scenario by name against a local Temporal server.

    temporal server start-dev            # in another terminal
    uv run python -m poc.cli list
    uv run python -m poc.cli run fail-in-resume

Seeds a fresh cluster directory, installs the scenario's chaos rules, runs the
saga with an in-process worker, then prints the audit log and the resulting
cluster state.
"""

from __future__ import annotations

import logging
from pathlib import Path

# Failure scenarios are *supposed* to fail, and Temporal logs every failed
# activity attempt with a full traceback. That noise buries the audit log, which
# is the actual output, so the worker's activity logger is silenced here.
logging.getLogger("temporalio.activity").setLevel(logging.CRITICAL)

from temporalio.client import Client

from poc.temporal.actor import actor_id
from poc.common.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.common.scenarios import REQUEST, SCENARIOS, SEED
from poc.temporal.saga import MoveDatatypeWorkflow
from poc.temporal.worker import TASK_QUEUE, build_worker, connect

DEFAULT_ROOT = Path(".cluster")


async def _clear_actors(client: Client) -> None:
    """Terminate actors left over from a previous scenario run.

    Actors are long-lived by design, so they survive the saga that started them.
    Across CLI runs that is a problem: an actor still cached as SUSPENDED would
    see a freshly seeded RUNNING cluster and skip the pause it was asked for.
    Tests do not need this - each gets its own Temporal environment.
    """
    for family in SEED:
        try:
            await client.get_workflow_handle(actor_id(family)).terminate(
                reason="new scenario run"
            )
        except Exception:  # noqa: BLE001 - nothing to terminate is the normal case
            pass

async def _run_workflow(scenario):
    client = await connect()
    await _clear_actors(client)
    failed = False
    async with build_worker(client):
        try:
            steps = await client.execute_workflow(
                MoveDatatypeWorkflow.run,
                REQUEST,
                id=f"move-{scenario.name}",
                task_queue=TASK_QUEUE,
            )
            print(f"RESULT   : completed - {', '.join(steps)}\n")
        except Exception as err:  # noqa: BLE001 - expected for failure scenarios
            failed = True
            print(f"RESULT   : failed - {err}\n")
    return failed

