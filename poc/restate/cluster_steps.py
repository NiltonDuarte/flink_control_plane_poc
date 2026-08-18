"""Typed durable-step callables for the shared mock cluster."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import TypeVar

from poc.common.cluster import MockCluster
from poc.common.domain import FamilyStatus, PermanentClusterError
from restate import TerminalError

T = TypeVar("T")
ENV_STEP_DELAY = "RESTATE_STEP_DELAY"


def _call(operation: Callable[[], T]) -> T:
    """Translate only permanent infrastructure faults into terminal failures."""
    delay = float(os.environ.get(ENV_STEP_DELAY, "0"))
    if delay > 0:
        time.sleep(delay)
    try:
        return operation()
    except PermanentClusterError as err:
        raise TerminalError(str(err), status_code=422) from err


def read_status(family: str) -> FamilyStatus:
    return _call(lambda: MockCluster.from_env().read(family))


def trigger_savepoint(family: str) -> str:
    return _call(lambda: MockCluster.from_env().trigger_savepoint(family))


def suspend_job(family: str) -> None:
    return _call(lambda: MockCluster.from_env().suspend_job(family))


def resume_job(family: str) -> None:
    return _call(lambda: MockCluster.from_env().resume_job(family))


def patch_configmap(family: str, datatypes: list[str]) -> list[str]:
    return _call(lambda: MockCluster.from_env().patch_configmap(family, datatypes))
