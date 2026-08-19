"""Record workflow histories as replay fixtures.

    uv run python -m tests.temporal.record_histories

Run this whenever the saga's *intended* shape changes, and commit the result.
The recorded histories are the input to `test_replay.py`, which is the guard
against accidentally changing that shape: edit the saga in a way that reorders
or removes a step, and replaying yesterday's history against today's code fails.

Uses the time-skipping environment, so no server needs to be running.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path

from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment

from poc.common.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.common.scenarios import REQUEST, SCENARIOS, SEED
from poc.temporal.application import app
from poc.temporal.saga import MoveDatatypeWorkflow
from poc.temporal.worker import build_worker

HISTORY_DIR = Path(__file__).parent / "histories"

# One clean success and one deep compensation: between them they cover every
# branch the saga can take.
RECORD = ["happy", "fail-in-resume"]


async def record() -> None:
    HISTORY_DIR.mkdir(exist_ok=True)
    env = await WorkflowEnvironment.start_time_skipping(
        data_converter=pydantic_data_converter
    )
    client = env.client
    try:
        for name in RECORD:
            scenario = SCENARIOS[name]
            with tempfile.TemporaryDirectory() as tmp:
                cluster = MockCluster(Path(tmp) / "cluster")
                cluster.seed(SEED)
                cluster.set_chaos(scenario.chaos)
                os.environ[ENV_CLUSTER_ROOT] = str(cluster.root)

                workflow_id = f"record-{name}"
                async with build_worker(client):
                    try:
                        await app.client(client).execute_workflow(
                            MoveDatatypeWorkflow.run,
                            REQUEST,
                            id=workflow_id,
                        )
                    except Exception:  # noqa: BLE001, S110 - expected failure
                        pass

                    handle = client.get_workflow_handle(workflow_id)
                    history = await handle.fetch_history()

                target = HISTORY_DIR / f"{name}.json"
                target.write_text(json.dumps(json.loads(history.to_json()), indent=2))
                print(f"wrote {target.relative_to(Path.cwd())}")

                # Actors outlive the saga; clear them before the next recording.
                for family in SEED:
                    try:
                        await client.get_workflow_handle(f"family:{family}").terminate()
                    except Exception:  # noqa: BLE001, S110 - actor may not exist
                        pass
    finally:
        await env.shutdown()


if __name__ == "__main__":
    asyncio.run(record())
