"""Registry ownership for the Temporal PoC."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

import temporal_app as app_module
from poc.temporal import activities as activities_module  # noqa: F401
from poc.temporal.actor import FlinkJobFamilyActor
from poc.temporal.actor_proxy import ActorProxy
from poc.temporal.application import TASK_QUEUE, app
from poc.temporal.saga import MoveDatatypeWorkflow
from poc.temporal.worker import build_worker
from temporal_app import ActivityBindingError


class FakeWorker:
    created: ClassVar[list[FakeWorker]] = []

    def __init__(self, client: object, **options: Any) -> None:
        self.client = client
        self.options = options
        self.created.append(self)


def test_registry_owns_all_definitions_and_one_queue() -> None:
    assert app.name == "flink-control-plane"
    assert app.task_queues == (TASK_QUEUE,)
    assert app.workflows == {
        "FlinkJobFamilyActor": app.workflow_registration(FlinkJobFamilyActor),
        "MoveDatatypeWorkflow": app.workflow_registration(MoveDatatypeWorkflow),
    }
    assert set(app.activities) == {
        "read_status",
        "trigger_savepoint",
        "suspend_job",
        "resume_job",
        "patch_configmap",
        "execute_family_command",
    }
    assert all(item.task_queue == TASK_QUEUE for item in app.workflows.values())
    assert all(item.task_queue == TASK_QUEUE for item in app.activities.values())
    assert (
        app.activity_registration(ActorProxy.execute_family_command).name
        == "execute_family_command"
    )


def test_worker_binds_exact_proxy_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeWorker.created = []
    monkeypatch.setattr(app_module, "Worker", FakeWorker)
    native_client = object()
    returned_worker = build_worker(native_client)  # type: ignore[arg-type]

    assert len(FakeWorker.created) == 1
    worker = FakeWorker.created[0]
    assert returned_worker is worker
    assert worker.client is native_client
    bound_proxies = [
        activity
        for activity in worker.options["activities"]
        if getattr(activity, "__name__", "") == "execute_family_command"
    ]
    assert len(bound_proxies) == 1
    proxy = bound_proxies[0].__self__
    assert isinstance(proxy, ActorProxy)
    assert proxy._client is native_client


def test_worker_rejects_missing_proxy_instance() -> None:
    native_client = object()

    with pytest.raises(ActivityBindingError, match="execute_family_command"):
        app.create_workers(native_client)  # type: ignore[arg-type]
