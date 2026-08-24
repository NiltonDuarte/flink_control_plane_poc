"""Typed durable-step callables for the shared mock cluster."""

from __future__ import annotations

from collections.abc import Callable

from poc.common.cluster import MockCluster
from poc.common.domain import FamilyStatus, PermanentClusterError
from restate import TerminalError


def _call[T](operation: Callable[[], T]) -> T:
    """Translate only permanent infrastructure faults into terminal failures."""
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


def patch_configmap(family: str, datatypes: list[str]) -> None:
    return _call(lambda: MockCluster.from_env().patch_configmap(family, datatypes))
