"""Running one scenario on a live Temporal server, for the CLI.

The counterpart of `tests/temporal/harness.py`: the same job, but against
`temporal server start-dev` rather than a test environment, and with the
long-lived-actor cleanup that only matters across repeated CLI runs.
"""

from __future__ import annotations

import logging

from temporalio.client import Client

# Failure scenarios are *supposed* to fail, and Temporal logs every failed
# activity attempt with a full traceback. That noise buries the audit log, which
# is the actual output, so the worker's activity logger is silenced here.
logging.getLogger("temporalio.activity").setLevel(logging.CRITICAL)

from poc.adapters.temporal.actor import actor_id
from poc.adapters.temporal.saga import MoveDatatypeWorkflow
from poc.adapters.temporal.worker import TASK_QUEUE, build_worker, connect
from poc.scenarios import REQUEST, SEED, Scenario


async def run_scenario(scenario: Scenario) -> tuple[list[str] | None, Exception | None]:
    """Run the move saga. Returns (steps, error) - exactly one is set."""
    client = await connect()
    await _clear_actors(client)
    async with build_worker(client):
        try:
            steps: list[str] = await client.execute_workflow(
                MoveDatatypeWorkflow.run,
                REQUEST,
                id=f"move-{scenario.name}",
                task_queue=TASK_QUEUE,
            )
            return steps, None
        except Exception as err:  # noqa: BLE001 - expected for failure scenarios
            return None, err


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
