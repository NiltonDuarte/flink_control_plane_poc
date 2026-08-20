from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import ClassVar, ParamSpec, Self, TypeVar, cast

from poc.common.cluster import MockCluster
from poc.common.domain import FamilyState, FamilyStatus, TransientClusterError

P = ParamSpec("P")
R = TypeVar("R")


def retryable(attempts: int) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decor(fn: Callable[P, R]) -> Callable[P, R]:
        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(attempts):
                try:
                    return fn(*args, **kwargs)
                except TransientClusterError:
                    if attempt == attempts - 1:
                        raise
            raise AssertionError("retryable requires at least one attempt")

        return wrapper

    return decor


class FamilyActor:
    _instances: ClassVar[dict[str, FamilyActor]] = {}

    def __new__(cls, name: str) -> Self:
        if name not in cls._instances:
            cls._instances[name] = super().__new__(cls)
        return cast(Self, cls._instances[name])

    def __init__(self, name: str) -> None:
        self.name = name
        self.cluster = MockCluster.from_env()

    def read_status(self) -> FamilyStatus:
        return self.cluster.read(self.name)

    @retryable(4)
    def _trigger_savepoint(self) -> None:
        self.cluster.trigger_savepoint(self.name)

    @retryable(4)
    def _suspend(self) -> None:
        self.cluster.suspend_job(self.name)

    def pause(self) -> None:
        self._trigger_savepoint()
        self._suspend()

    @retryable(4)
    def patch_config(self, config: list[str]) -> None:
        self.cluster.patch_configmap(self.name, config)

    @retryable(4)
    def resume(self) -> None:
        self.cluster.resume_job(self.name)

    def restore(
        self,
        desired_state: FamilyState | None,
        datatypes: list[str] | None,
    ) -> bool:
        """Reconcile a compensation intent against authoritative cluster state.

        Normal commands deliberately trust the actor cache. Compensation is
        different: a forward activity may have committed and then lost its
        response, leaving that cache stale. The fresh read and any needed repair
        therefore happen under the same lock as every other actor command.
        """

        status = self.read_status()
        changed = False

        if desired_state is not None and status.state != desired_state:
            if desired_state == FamilyState.SUSPENDED:
                self._trigger_savepoint()
                self._suspend()
            else:
                self.resume()
            changed = True

        if datatypes is not None and status.datatypes != datatypes:
            self.patch_config(datatypes)
            changed = True

        return changed
