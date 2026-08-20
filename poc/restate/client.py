"""Ingress client for running shared scenarios against a Restate server."""

from __future__ import annotations

import os
import uuid

import httpx

import restate
from poc.common.scenarios import REQUEST, Scenario
from poc.restate.saga import run
from restate.client import Client

ENV_RESTATE_INGRESS_URL = "RESTATE_INGRESS_URL"
DEFAULT_RESTATE_INGRESS_URL = "http://localhost:8080"
INGRESS_TIMEOUT = httpx.Timeout(connect=5, read=120, write=10, pool=5)


async def run_scenario(scenario: Scenario) -> tuple[list[str] | None, Exception | None]:
    ingress = os.environ.get(ENV_RESTATE_INGRESS_URL, DEFAULT_RESTATE_INGRESS_URL)
    try:
        async with httpx.AsyncClient(
            base_url=ingress,
            http2=True,
            timeout=INGRESS_TIMEOUT,
        ) as http_client:
            client = Client(http_client)
            steps = await client.workflow_call(
                run,
                key=f"move-{scenario.name}-{uuid.uuid4()}",
                arg=REQUEST,
            )
        return steps, None
    except (httpx.HTTPError, restate.HttpError) as err:
        return None, err
