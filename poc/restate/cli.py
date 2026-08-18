"""Shared CLI adapter for the Restate engine."""

from poc.common.scenarios import Scenario
from poc.restate.client import run_scenario


async def _run_workflow(
    scenario: Scenario,
) -> tuple[list[str] | None, Exception | None]:
    return await run_scenario(scenario)
