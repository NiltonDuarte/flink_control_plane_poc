"""Application-bound Temporal registry and routing helpers.

Copy this file into a Python 3.11+ project that depends on
``temporalio>=1.30,<2``. The utility deliberately remains a registry/router over
the native Temporal SDK; it is not a dependency-injection container.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import MappingProxyType
from typing import Any, TypedDict, TypeVar, Unpack, cast

from temporalio import activity as temporal_activity
from temporalio import workflow as temporal_workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.common import (
    Priority,
    RetryPolicy,
    SearchAttributes,
    TypedSearchAttributes,
    VersioningBehavior,
    VersioningOverride,
    WorkflowIDConflictPolicy,
    WorkflowIDReusePolicy,
)
from temporalio.worker import Worker
from temporalio.workflow import (
    ActivityCancellationType,
    ChildWorkflowCancellationType,
    ChildWorkflowHandle,
    ParentClosePolicy,
    VersioningIntent,
)

__all__ = [
    "ActivityBindingError",
    "ActivityRegistration",
    "DefinitionConfigurationError",
    "DuplicateRegistrationError",
    "TemporalApp",
    "TemporalAppError",
    "TemporalClient",
    "UnknownRegistrationError",
    "WorkflowRegistration",
]

DefinitionT = TypeVar("DefinitionT")
ResultT = TypeVar("ResultT")
_UNSET: Any = object()


class TemporalAppError(Exception):
    """Base class for registry and routing errors."""


class DefinitionConfigurationError(TemporalAppError, ValueError):
    """Raised when app and native definition decorator options conflict."""


class DuplicateRegistrationError(TemporalAppError, ValueError):
    """Raised when a Temporal type name is registered more than once."""


class UnknownRegistrationError(TemporalAppError, LookupError):
    """Raised when routing is requested for an unregistered definition."""


class ActivityBindingError(TemporalAppError, ValueError):
    """Raised when instance activity methods cannot be bound unambiguously."""


@dataclass(frozen=True, slots=True)
class WorkflowRegistration:
    """A native Temporal workflow class and the queue that owns it."""

    name: str
    task_queue: str
    workflow: type[Any]
    run: Callable[..., Any]


@dataclass(frozen=True, slots=True)
class ActivityRegistration:
    """A native Temporal activity callable and the queue that owns it."""

    name: str
    task_queue: str
    activity: Callable[..., Any]
    requires_instance: bool


class ActivityOptions(TypedDict, total=False):
    """Native activity options; routing and arguments are registry-owned."""

    result_type: type[Any] | None
    schedule_to_close_timeout: timedelta | None
    schedule_to_start_timeout: timedelta | None
    start_to_close_timeout: timedelta | None
    heartbeat_timeout: timedelta | None
    retry_policy: RetryPolicy | None
    cancellation_type: ActivityCancellationType
    activity_id: str | None
    versioning_intent: VersioningIntent | None
    summary: str | None
    priority: Priority


class ChildWorkflowOptions(TypedDict, total=False):
    """Native child-workflow options; routing and arguments are registry-owned."""

    id: str | None
    result_type: type[Any] | None
    cancellation_type: ChildWorkflowCancellationType
    parent_close_policy: ParentClosePolicy
    execution_timeout: timedelta | None
    run_timeout: timedelta | None
    task_timeout: timedelta | None
    id_reuse_policy: WorkflowIDReusePolicy
    retry_policy: RetryPolicy | None
    cron_schedule: str
    memo: Mapping[str, Any] | None
    search_attributes: SearchAttributes | TypedSearchAttributes | None
    versioning_intent: VersioningIntent | None
    static_summary: str | None
    static_details: str | None
    priority: Priority


class ClientWorkflowOptions(TypedDict, total=False):
    """Native execute-workflow options; routing, arguments, and ID are owned."""

    result_type: type[Any] | None
    execution_timeout: timedelta | None
    run_timeout: timedelta | None
    task_timeout: timedelta | None
    id_reuse_policy: WorkflowIDReusePolicy
    id_conflict_policy: WorkflowIDConflictPolicy
    retry_policy: RetryPolicy | None
    cron_schedule: str
    memo: Mapping[str, Any] | None
    search_attributes: SearchAttributes | TypedSearchAttributes | None
    static_summary: str | None
    static_details: str | None
    start_delay: timedelta | None
    start_signal: str | None
    start_signal_args: Sequence[Any]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    request_eager_start: bool
    priority: Priority
    versioning_override: VersioningOverride | None


class StartClientWorkflowOptions(ClientWorkflowOptions, total=False):
    """Native start-workflow-only options."""

    callbacks: Sequence[Any]
    links: Sequence[Any]
    request_id: str | None
    stack_level: int


class TemporalClient:
    """Route registered workflow calls while retaining the native client."""

    def __init__(self, app: TemporalApp, client: Client) -> None:
        self._app = app
        self.client = client

    async def start_workflow(
        self,
        workflow: Callable[..., Awaitable[ResultT]],
        *workflow_args: Any,
        id: str,
        **options: Unpack[StartClientWorkflowOptions],
    ) -> WorkflowHandle[Any, ResultT]:
        """Start a registered workflow and return its native Temporal handle."""

        registration = self._app.workflow_registration(workflow)
        start = cast(
            Callable[..., Awaitable[WorkflowHandle[Any, ResultT]]],
            self.client.start_workflow,
        )
        return await start(
            workflow,
            args=workflow_args,
            id=id,
            task_queue=registration.task_queue,
            **options,
        )

    async def execute_workflow(
        self,
        workflow: Callable[..., Awaitable[ResultT]],
        *workflow_args: Any,
        id: str,
        **options: Unpack[ClientWorkflowOptions],
    ) -> ResultT:
        """Execute a registered workflow and await its result."""

        registration = self._app.workflow_registration(workflow)
        execute = cast(Callable[..., Awaitable[ResultT]], self.client.execute_workflow)
        result = await execute(
            workflow,
            args=workflow_args,
            id=id,
            task_queue=registration.task_queue,
            **options,
        )
        return result


class TemporalApp:
    """Own workflow/activity registrations for one host application.

    Modules register definitions when explicitly imported. The registry never scans
    packages or invents execution policy.
    """

    def __init__(self, name: str | None = None) -> None:
        self.name = name
        self._workflows_by_name: dict[str, WorkflowRegistration] = {}
        self._workflows_by_ref: dict[object, WorkflowRegistration] = {}
        self._activities_by_name: dict[str, ActivityRegistration] = {}
        self._activities_by_ref: dict[object, ActivityRegistration] = {}

    @property
    def workflows(self) -> Mapping[str, WorkflowRegistration]:
        """Read-only workflow registrations keyed by Temporal type name."""

        return MappingProxyType(self._workflows_by_name)

    @property
    def activities(self) -> Mapping[str, ActivityRegistration]:
        """Read-only activity registrations keyed by Temporal type name."""

        return MappingProxyType(self._activities_by_name)

    @property
    def task_queues(self) -> tuple[str, ...]:
        """All queues with registered work, in deterministic order."""

        queues = {registration.task_queue for registration in self._workflows_by_name.values()}
        queues.update(registration.task_queue for registration in self._activities_by_name.values())
        return tuple(sorted(queues))

    def workflow(
        self,
        *,
        task_queue: str,
        name: str | None = _UNSET,
        sandboxed: bool = _UNSET,
        failure_exception_types: Sequence[type[BaseException]] = _UNSET,
        versioning_behavior: VersioningBehavior = _UNSET,
    ) -> Callable[[type[DefinitionT]], type[DefinitionT]]:
        """Define and register a workflow class on ``task_queue``.

        Definition options are passed to native ``@workflow.defn``. Native
        ``@workflow.run``/signal/query/update decorators remain unchanged. A legacy
        class with ``@workflow.defn`` is accepted when explicitly repeated options
        match its existing definition; omitted options preserve its metadata.
        """

        self._validate_task_queue(task_queue)
        options = {
            "name": name,
            "sandboxed": sandboxed,
            "failure_exception_types": failure_exception_types,
            "versioning_behavior": versioning_behavior,
        }
        supplied_options = {key: value for key, value in options.items() if value is not _UNSET}

        def decorator(cls: type[DefinitionT]) -> type[DefinitionT]:
            definition = temporal_workflow._Definition.from_class(cls)
            if definition is None:
                cls = temporal_workflow.defn(cls, **supplied_options)
                definition = temporal_workflow._Definition.must_from_class(cls)

            definition_name = self._definition_name("workflow", definition.name)
            self._validate_definition_options(
                "workflow",
                supplied_options,
                {
                    "name": definition.name,
                    "sandboxed": definition.sandboxed,
                    "failure_exception_types": definition.failure_exception_types,
                    "versioning_behavior": definition.versioning_behavior,
                },
                default_name=cls.__name__,
            )
            if definition_name in self._workflows_by_name:
                # Sandboxed workflow modules are re-executed for validation and replay.
                # A passed-through application object therefore sees the decorator
                # again, but this is not a second host registration.
                if temporal_workflow.unsafe.in_sandbox():
                    return cls
                self._raise_duplicate("workflow", definition_name)

            registration = WorkflowRegistration(
                name=definition_name,
                task_queue=task_queue,
                workflow=cast(type[Any], cls),
                run=definition.run_fn,
            )
            self._workflows_by_name[definition_name] = registration
            self._workflows_by_ref[cls] = registration
            self._workflows_by_ref[definition.run_fn] = registration
            return cls

        return decorator

    def activity(
        self,
        *,
        task_queue: str,
        name: str | None = _UNSET,
        no_thread_cancel_exception: bool = _UNSET,
    ) -> Callable[[DefinitionT], DefinitionT]:
        """Define and register an activity callable on ``task_queue``.

        Methods are registered as definitions here, then bound to host-created
        dependency-bearing instances by :meth:`create_workers`. A legacy callable
        with ``@activity.defn`` is accepted when explicitly repeated options match
        its existing definition; omitted options preserve its metadata.
        """

        self._validate_task_queue(task_queue)
        options = {
            "name": name,
            "no_thread_cancel_exception": no_thread_cancel_exception,
        }
        supplied_options = {key: value for key, value in options.items() if value is not _UNSET}

        def decorator(fn: DefinitionT) -> DefinitionT:
            callable_fn = cast(Callable[..., Any], fn)
            definition = temporal_activity._Definition.from_callable(callable_fn)
            if definition is None:
                callable_fn = temporal_activity.defn(callable_fn, **supplied_options)
                definition = temporal_activity._Definition.must_from_callable(callable_fn)

            definition_name = self._definition_name("activity", definition.name)
            self._validate_definition_options(
                "activity",
                supplied_options,
                {
                    "name": definition.name,
                    "no_thread_cancel_exception": definition.no_thread_cancel_exception,
                },
                default_name=callable_fn.__name__,
            )
            if definition_name in self._activities_by_name:
                self._raise_duplicate("activity", definition_name)

            requires_instance = (
                "." in callable_fn.__qualname__ and "<locals>" not in callable_fn.__qualname__
            )
            registration = ActivityRegistration(
                name=definition_name,
                task_queue=task_queue,
                activity=callable_fn,
                requires_instance=requires_instance,
            )
            self._activities_by_name[definition_name] = registration
            self._activities_by_ref[callable_fn] = registration
            return cast(DefinitionT, callable_fn)

        return decorator

    def workflow_registration(self, workflow: object) -> WorkflowRegistration:
        """Return the registration for a workflow class or run-method reference."""

        registration = self._workflows_by_ref.get(workflow)
        if registration is None:
            definition = None
            if isinstance(workflow, type):
                definition = temporal_workflow._Definition.from_class(workflow)
            elif callable(workflow):
                definition = temporal_workflow._Definition.from_run_fn(workflow)
            if definition is not None:
                name = self._definition_name("workflow", definition.name)
                registration = self._workflows_by_name.get(name)
        if registration is None:
            raise UnknownRegistrationError(
                f"Workflow {self._display_name(workflow)!r} is not registered with this app"
            )
        return registration

    def activity_registration(self, activity: object) -> ActivityRegistration:
        """Return the registration for an activity reference."""

        original = getattr(activity, "__func__", activity)
        registration = self._activities_by_ref.get(original)
        if registration is None and callable(activity):
            definition = temporal_activity._Definition.from_callable(activity)
            if definition is not None:
                name = self._definition_name("activity", definition.name)
                registration = self._activities_by_name.get(name)
        if registration is None:
            raise UnknownRegistrationError(
                f"Activity {self._display_name(activity)!r} is not registered with this app"
            )
        return registration

    async def execute_activity(
        self,
        activity: Callable[..., ResultT] | Callable[..., Awaitable[ResultT]],
        *activity_args: Any,
        **options: Unpack[ActivityOptions],
    ) -> ResultT:
        """Execute an activity on its registered queue with native options."""

        registration = self.activity_registration(activity)
        execute = cast(Callable[..., Awaitable[ResultT]], temporal_workflow.execute_activity)
        result = await execute(
            activity,
            args=activity_args,
            task_queue=registration.task_queue,
            **options,
        )
        return result

    async def execute_child_workflow(
        self,
        workflow: Callable[..., Awaitable[ResultT]],
        *workflow_args: Any,
        **options: Unpack[ChildWorkflowOptions],
    ) -> ResultT:
        """Execute a child workflow on its registered queue with native options."""

        registration = self.workflow_registration(workflow)
        execute = cast(Callable[..., Awaitable[ResultT]], temporal_workflow.execute_child_workflow)
        result = await execute(
            workflow,
            args=workflow_args,
            task_queue=registration.task_queue,
            **options,
        )
        return result

    async def start_child_workflow(
        self,
        workflow: Callable[..., Awaitable[ResultT]],
        *workflow_args: Any,
        **options: Unpack[ChildWorkflowOptions],
    ) -> ChildWorkflowHandle[Any, ResultT]:
        """Start a child workflow and return its native Temporal handle."""

        registration = self.workflow_registration(workflow)
        start = cast(
            Callable[..., Awaitable[ChildWorkflowHandle[Any, ResultT]]],
            temporal_workflow.start_child_workflow,
        )
        return await start(
            workflow,
            args=workflow_args,
            task_queue=registration.task_queue,
            **options,
        )

    def client(self, client: Client) -> TemporalClient:
        """Create the small routing facade around a native Temporal client."""

        return TemporalClient(self, client)

    def create_workers(
        self,
        client: Client,
        *,
        activity_instances: Iterable[object] = (),
        worker_options: Mapping[str, Any] | None = None,
        worker_options_by_task_queue: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, Worker]:
        """Construct one combined worker per registered task queue.

        ``worker_options`` apply to every worker and queue-specific options override
        them. These dictionaries are passed directly to :class:`temporalio.worker.Worker`.
        The registry always owns ``task_queue``, ``workflows``, and ``activities``.
        """

        global_options = dict(worker_options or {})
        per_queue_options = worker_options_by_task_queue or {}
        reserved = {"task_queue", "workflows", "activities"}
        supplied_reserved = reserved.intersection(global_options)
        supplied_reserved.update(
            key for options in per_queue_options.values() for key in reserved.intersection(options)
        )
        if supplied_reserved:
            joined = ", ".join(sorted(supplied_reserved))
            raise ValueError(
                f"Worker routing options are registry-owned and cannot be set: {joined}"
            )

        activities = self._bind_activities(tuple(activity_instances))
        workflows_by_queue: dict[str, list[type[Any]]] = defaultdict(list)
        activities_by_queue: dict[str, list[Callable[..., Any]]] = defaultdict(list)
        for workflow_registration in self._workflows_by_name.values():
            workflows_by_queue[workflow_registration.task_queue].append(
                workflow_registration.workflow
            )
        for activity_registration, bound_activity in activities:
            activities_by_queue[activity_registration.task_queue].append(bound_activity)

        workers: dict[str, Worker] = {}
        for task_queue in self.task_queues:
            options = {**global_options, **dict(per_queue_options.get(task_queue, {}))}
            workers[task_queue] = Worker(
                client,
                task_queue=task_queue,
                workflows=workflows_by_queue[task_queue],
                activities=activities_by_queue[task_queue],
                **options,
            )
        return workers

    def _bind_activities(
        self, instances: Sequence[object]
    ) -> list[tuple[ActivityRegistration, Callable[..., Any]]]:
        bound_by_original: dict[Callable[..., Any], Callable[..., Any]] = {}
        provider_by_original: dict[Callable[..., Any], object] = {}
        for instance in instances:
            for cls in type(instance).__mro__:
                for attribute_name, attribute in vars(cls).items():
                    original_value = self._descriptor_callable(attribute)
                    if not callable(original_value):
                        continue
                    original = cast(Callable[..., Any], original_value)
                    if original not in self._activities_by_ref:
                        continue
                    bound_value = getattr(instance, attribute_name)
                    if not callable(bound_value):
                        registration = self._activities_by_ref[original]
                        raise ActivityBindingError(
                            f"Instance attribute for activity {registration.name!r} is not callable"
                        )
                    bound_activity = cast(Callable[..., Any], bound_value)
                    previous_provider = provider_by_original.get(original)
                    if previous_provider is not None and previous_provider is not instance:
                        registration = self._activities_by_ref[original]
                        raise ActivityBindingError(
                            f"Activity {registration.name!r} was provided by multiple instances"
                        )
                    bound_by_original[original] = bound_activity
                    provider_by_original[original] = instance

        result: list[tuple[ActivityRegistration, Callable[..., Any]]] = []
        missing: list[str] = []
        for registration in self._activities_by_name.values():
            if registration.requires_instance:
                resolved_activity = bound_by_original.get(registration.activity)
                if resolved_activity is None:
                    missing.append(registration.name)
                    continue
                result.append((registration, resolved_activity))
            else:
                result.append((registration, registration.activity))
        if missing:
            joined = ", ".join(repr(name) for name in missing)
            raise ActivityBindingError(f"No host instance was provided for activities: {joined}")
        return result

    @staticmethod
    def _descriptor_callable(attribute: object) -> object:
        if isinstance(attribute, (staticmethod, classmethod)):
            return attribute.__func__
        return attribute

    @staticmethod
    def _validate_task_queue(task_queue: str) -> None:
        if not task_queue or not task_queue.strip():
            raise ValueError("task_queue must be a non-empty string")

    @staticmethod
    def _definition_name(kind: str, name: str | None) -> str:
        if name is None:
            raise ValueError(f"Dynamic Temporal {kind} definitions cannot be registered")
        return name

    @classmethod
    def _validate_definition_options(
        cls,
        kind: str,
        supplied: Mapping[str, Any],
        existing: Mapping[str, Any],
        *,
        default_name: str,
    ) -> None:
        for field, requested_value in supplied.items():
            normalized_requested = cls._normalize_definition_option(
                field, requested_value, default_name=default_name
            )
            existing_value = existing[field]
            normalized_existing = cls._normalize_definition_option(
                field, existing_value, default_name=default_name
            )
            if normalized_requested != normalized_existing:
                raise DefinitionConfigurationError(
                    f"Temporal {kind} definition option {field!r} conflicts: "
                    f"app decorator requested {requested_value!r}, but the existing "
                    f"native definition has {existing_value!r}"
                )

    @staticmethod
    def _normalize_definition_option(field: str, value: Any, *, default_name: str) -> Any:
        if field == "name":
            return value or default_name
        if field == "failure_exception_types":
            return tuple(value)
        if field == "versioning_behavior" and value is None:
            return VersioningBehavior.UNSPECIFIED
        return value

    @staticmethod
    def _display_name(value: object) -> str:
        return cast(str, getattr(value, "__qualname__", getattr(value, "__name__", repr(value))))

    @staticmethod
    def _raise_duplicate(kind: str, name: str) -> None:
        raise DuplicateRegistrationError(
            f"Temporal {kind} type name {name!r} is already registered with this app"
        )
