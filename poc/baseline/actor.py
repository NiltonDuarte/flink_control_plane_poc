from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any, ClassVar, Self

from poc.common.cluster import MockCluster
from poc.common.domain import FamilyState, FamilyStatus, TransientClusterError


def retryable(attempts: int) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decor(fn: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(attempts):
                try:
                    return fn(*args, **kwargs)
                except TransientClusterError:
                    if attempt == attempts - 1:
                        raise

        return wrapper

    return decor


class FamilyActor:
    _instances: ClassVar[dict[str, FamilyActor]] = {}

    def __new__(cls, name: str) -> Self:
        if name not in cls._instances:
            cls._instances[name] = super().__new__(cls)
        return cls._instances[name]

    def __init__(self, name: str) -> None:
        self.name = name
        self.cluster = MockCluster.from_env()

    def read_status(self) -> FamilyStatus:
        return self.cluster.read(self.name)

    @retryable(4)
    def _trigger_savepoint(self, operation_id: str) -> str:
        return self.cluster.trigger_savepoint(self.name, operation_id)

    @retryable(4)
    def _suspend(self, operation_id: str) -> bool:
        return self.cluster.suspend_job(self.name, operation_id)

    def pause(self, operation_id: str) -> None:
        self._trigger_savepoint(f"{operation_id}:savepoint")
        self._suspend(f"{operation_id}:suspend")

    @retryable(4)
    def patch_config(self, config: list[str], operation_id: str) -> bool:
        return self.cluster.patch_configmap(self.name, config, operation_id)

    @retryable(4)
    def resume(self, operation_id: str) -> bool:
        return self.cluster.resume_job(self.name, operation_id)

    def restore(
        self,
        desired_state: FamilyState | None,
        datatypes: list[str] | None,
        operation_id: str,
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
                self._trigger_savepoint(f"{operation_id}:savepoint")
                self._suspend(f"{operation_id}:suspend")
            else:
                self.resume(f"{operation_id}:resume")
            changed = True

        if datatypes is not None and status.datatypes != datatypes:
            self.patch_config(datatypes, f"{operation_id}:config")
            changed = True

        return changed
