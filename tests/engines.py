"""The engine seam for the shared scenario matrix.

`tests/test_saga.py` asserts the *sequence of side effects* a scenario produces.
Nothing in those assertions is Temporal-specific - they are statements about the
saga, and the whole reason `poc/core/` exists is so a second engine has to
satisfy exactly the same ones. This module is where an engine plugs in.

A harness owns whatever running an engine costs (a worker, a server, a task
queue) and exposes one method. Adding Restate means writing one class and adding
one line to `HARNESSES`; `test_saga.py` does not change at all - which is the
property being protected here.

Factories take the pytest `request` so an engine can pull its own fixtures via
`getfixturevalue` - only the engine actually under test pays for its setup.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

import pytest

from poc.scenarios import Scenario


class EngineHarness(Protocol):
    """One engine, ready to run a scenario end to end."""

    name: str

    async def run_saga(
        self, scenario: Scenario
    ) -> tuple[list[str] | None, Exception | None]:
        """Run the move saga. Returns (steps, error) - exactly one is set."""
        ...


def _temporal(request: pytest.FixtureRequest) -> EngineHarness:
    from tests.temporal.harness import TemporalHarness

    return TemporalHarness(request.getfixturevalue("client"))


# Restate lands here in issue #11, and nothing else in the suite should need to
# change when it does.
HARNESSES: dict[str, Callable[[pytest.FixtureRequest], EngineHarness]] = {
    "temporal": _temporal,
}
