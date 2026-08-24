"""Shared CLI adapter for the Restate engine."""

import os

import restate
from poc.common.scenarios import SEED, Scenario
from poc.restate.actor import reset
from poc.restate.client import (
    DEFAULT_RESTATE_INGRESS_URL,
    ENV_RESTATE_INGRESS_URL,
    run_scenario,
)
from poc.restate.models import Empty


async def _clear_actors() -> None:
    """Wipe K/V state left over from a previous scenario run."""
    ingress = os.environ.get(ENV_RESTATE_INGRESS_URL, DEFAULT_RESTATE_INGRESS_URL)
    async with restate.create_client(ingress) as client:
        for family in SEED:
            try:
                await client.object_call(reset, key=family, arg=Empty())
            except Exception:  # noqa: BLE001, S110
                pass


async def _run_workflow(
    scenario: Scenario,
) -> tuple[list[str] | None, Exception | None]:
    await _clear_actors()
    return await run_scenario(scenario)
