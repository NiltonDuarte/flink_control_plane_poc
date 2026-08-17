from enum import Enum

from poc_baseline.cluster import MockCluster
from poc_baseline.domain import FamilyState

class UpdateStatus(Enum):
    CHANGED = "CHANGED"
    UNCHANGED = "UNCHANGED"

def retryable(retries):
    def decor(fn):
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    return fn(*args, **kwargs)
                except:
                    attempt += 1
            raise RuntimeError("Too much failure for {fn}")
        return wrapper
    return decor


class FamilyActor:
    def __init__(self, name):
        self.name = name
        self.cluster = MockCluster.from_env()

    def read_status(self):
        return self.cluster.read(self.name)

    def _trigger_savepoint(self):
        self.cluster.trigger_savepoint(self.name)

    def _suspend(self):
        self.cluster.suspend_job(self.name)

    @retryable(4)
    def pause(self) -> UpdateStatus:
        if self.read_status().state == FamilyState.SUSPENDED:
            return UpdateStatus.UNCHANGED
        self._trigger_savepoint()
        self._suspend()
        return UpdateStatus.CHANGED

    @retryable(4)
    def patch_config(self, config):
        previous_datatypes = self.read_status().datatypes
        self.cluster.patch_configmap(self.name, config)
        return previous_datatypes

    @retryable(4)
    def resume(self):
        if self.read_status().state == FamilyState.RUNNING:
            return UpdateStatus.UNCHANGED
        self.cluster.resume_job(self.name)
        return UpdateStatus.CHANGED