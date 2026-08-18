"""Replay determinism.

Named explicitly in the ticket, and the cheapest safety net in the whole suite:
no server, no worker, no cluster - just recorded histories fed back through the
current workflow code.

What it catches is the failure mode that makes durable execution dangerous. A
workflow that has been running for hours is resumed by replaying its history
against whatever code is deployed *now*. If that code issues a different
sequence of commands, the replay diverges and the in-flight workflow breaks.
This test surfaces that at commit time instead of at 3am.

**What it does not catch, and why that matters here.** Temporal compares the
*type* of each scheduled command, not its arguments. Both of these were verified
against this suite:

* inserting an extra activity call -> caught ("Activity type of scheduled event
  'execute_family_command' does not match activity type of activity command
  'read_status'");
* reversing the order families are resumed in -> **not caught**, because every
  actor command travels through the same `execute_family_command` activity, so
  the command *sequence* is identical and only the payload differs.

That blind spot is a direct cost of the proxy design
(see poc/temporal/actor_proxy.py):
funnelling every command through one activity type is what makes the saga
readable, and it is also what hides command-level changes from replay. The audit
-log assertions in tests/temporal/test_saga.py are what actually cover ordering. Replay covers
structure. Neither is sufficient alone, which is worth knowing before leaning on
replay as the primary regression gate for an engine comparison.

Regenerate the fixtures with
`uv run python -m tests.temporal.record_histories` when the saga's shape changes
on purpose, and review the diff as part of the change.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer

from poc.temporal.actor import FlinkJobFamilyActor
from poc.temporal.saga import MoveDatatypeWorkflow

HISTORY_DIR = Path(__file__).parent / "histories"
HISTORIES = sorted(HISTORY_DIR.glob("*.json"))


def test_fixtures_exist() -> None:
    """Guard against the suite silently passing with nothing to replay."""
    assert HISTORIES, (
        f"no replay fixtures in {HISTORY_DIR}; run tests.temporal.record_histories"
    )


@pytest.mark.parametrize("history_file", HISTORIES, ids=lambda p: p.stem)
async def test_history_replays_without_divergence(history_file: Path) -> None:
    replayer = Replayer(
        workflows=[MoveDatatypeWorkflow, FlinkJobFamilyActor],
        data_converter=pydantic_data_converter,
    )
    history = WorkflowHistory.from_json(history_file.stem, history_file.read_text())

    # Raises on any non-determinism between the history and current code.
    await replayer.replay_workflow(history)
