"""Typed durable-step callables for the shared mock cluster."""

from __future__ import annotations

import os
import time
from collections.abc import Callable

from poc.common.cluster import MockCluster
from poc.common.domain import (
    FamilyStatus,
    InvalidFamilyIdentifierError,
    PermanentClusterError,
)
from restate import TerminalError

ENV_STEP_DELAY = "RESTATE_STEP_DELAY"


def _call[T](operation: Callable[[], T]) -> T:
    """Translate only permanent infrastructure faults into terminal failures."""
    delay = float(os.environ.get(ENV_STEP_DELAY, "0"))
    if delay > 0:
        time.sleep(delay)
    try:
        return operation()
    except (InvalidFamilyIdentifierError, PermanentClusterError) as err:
        raise TerminalError(str(err), status_code=422) from err


def read_status(family: str) -> FamilyStatus:
    return _call(lambda: MockCluster.from_env().read(family))


def trigger_savepoint(family: str, operation_id: str) -> str:
    return _call(lambda: MockCluster.from_env().trigger_savepoint(family, operation_id))


def suspend_job(family: str, operation_id: str) -> bool:
    return _call(lambda: MockCluster.from_env().suspend_job(family, operation_id))


def resume_job(family: str, operation_id: str) -> bool:
    return _call(lambda: MockCluster.from_env().resume_job(family, operation_id))


def patch_configmap(family: str, datatypes: list[str], operation_id: str) -> bool:
    return _call(
        lambda: MockCluster.from_env().patch_configmap(family, datatypes, operation_id)
    )
