"""Ingress client for running shared scenarios against a Restate server."""

from __future__ import annotations

import os
import uuid

import restate
from poc.common.scenarios import REQUEST, Scenario
from poc.restate.saga import run

ENV_RESTATE_INGRESS_URL = "RESTATE_INGRESS_URL"
DEFAULT_RESTATE_INGRESS_URL = "http://localhost:8080"


async def run_scenario(scenario: Scenario) -> tuple[list[str] | None, Exception | None]:
    ingress = os.environ.get(ENV_RESTATE_INGRESS_URL, DEFAULT_RESTATE_INGRESS_URL)
    try:
        async with restate.create_client(ingress) as client:
            steps = await client.workflow_call(
                run,
                key=f"move-{scenario.name}-{uuid.uuid4()}",
                arg=REQUEST,
            )
        return steps, None
    except Exception as err:  # Client-side transport and terminal domain failures.
        return None, err
