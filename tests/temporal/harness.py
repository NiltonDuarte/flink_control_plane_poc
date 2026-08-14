"""Running the shared scenario matrix on Temporal.

The whole engine-specific cost of a scenario, in one class: a worker on a fresh
task queue, one workflow execution, and the failure surfacing as an exception.
Everything asserted about the result lives in `tests/test_saga.py` and is engine
agnostic.
"""

from __future__ import annotations

import uuid

from temporalio.client import Client

from poc.adapters.temporal.saga import MoveDatatypeWorkflow
from poc.adapters.temporal.worker import build_worker
from poc.scenarios import REQUEST, Scenario


class TemporalHarness:
    name = "temporal"

    def __init__(self, client: Client) -> None:
        self._client = client

    async def run_saga(
        self, scenario: Scenario
    ) -> tuple[list[str] | None, Exception | None]:
        # A task queue per run, so a worker from a previous test can never pick
        # up this one's work.
        task_queue = f"tq-{uuid.uuid4()}"
        async with build_worker(self._client, task_queue):
            try:
                steps: list[str] = await self._client.execute_workflow(
                    MoveDatatypeWorkflow.run,
                    REQUEST,
                    id=f"move-{scenario.name}-{uuid.uuid4()}",
                    task_queue=task_queue,
                )
                return steps, None
            except Exception as err:  # noqa: BLE001 - failure scenarios expect this
                return None, err
