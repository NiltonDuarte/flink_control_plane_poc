"""Restate Virtual Object implementing one single-writer Flink job family."""

from __future__ import annotations

from datetime import timedelta

import restate

from poc.common.domain import CommandResult, FamilyState
from poc.restate.cluster_steps import (
    patch_configmap,
    read_status,
    resume_job,
    suspend_job,
    trigger_savepoint,
)
from poc.restate.models import (
    ActorState,
    Empty,
    PatchConfigRequest,
    RestoreRequest,
)

flink_job_family = restate.VirtualObject(
    "FlinkJobFamilyActor",
    description="Single-writer runtime and routing commands for one Flink job family.",
)

STATE_KEY = "runtime_state"
CLUSTER_RUN_OPTIONS = restate.RunOptions(
    max_attempts=4,
    initial_retry_interval=timedelta(milliseconds=100),
    retry_interval_factor=2.0,
)


async def _cached_state(ctx: restate.ObjectContext) -> FamilyState:
    cached = await ctx.get(STATE_KEY, type_hint=str)
    if cached is not None:
        return FamilyState(cached)
    status = await ctx.run_typed(
        "read initial cluster state",
        read_status,
        CLUSTER_RUN_OPTIONS,
        ctx.key(),
    )
    ctx.set(STATE_KEY, status.state.value)
    return status.state


@flink_job_family.handler()
async def pause(ctx: restate.ObjectContext, _request: Empty) -> CommandResult:
    """Take a savepoint and suspend, or no-op when already suspended."""
    if await _cached_state(ctx) == FamilyState.SUSPENDED:
        return CommandResult(changed=False)
    uri = await ctx.run_typed(
        "trigger savepoint", trigger_savepoint, CLUSTER_RUN_OPTIONS, ctx.key()
    )
    await ctx.run_typed(
        "suspend job", suspend_job, CLUSTER_RUN_OPTIONS, ctx.key()
    )
    ctx.set(STATE_KEY, FamilyState.SUSPENDED.value)
    return CommandResult(changed=True, savepoint_uri=uri)


@flink_job_family.handler()
async def resume(ctx: restate.ObjectContext, _request: Empty) -> CommandResult:
    """Return to running, or no-op when the cached state is already running."""
    if await _cached_state(ctx) == FamilyState.RUNNING:
        return CommandResult(changed=False)
    await ctx.run_typed("resume job", resume_job, CLUSTER_RUN_OPTIONS, ctx.key())
    ctx.set(STATE_KEY, FamilyState.RUNNING.value)
    return CommandResult(changed=True)


@flink_job_family.handler()
async def patch_config(
    ctx: restate.ObjectContext, request: PatchConfigRequest
) -> CommandResult:
    """Replace routing configuration through a typed durable step."""
    await ctx.run_typed(
        "patch configmap",
        patch_configmap,
        CLUSTER_RUN_OPTIONS,
        ctx.key(),
        request.datatypes,
    )
    return CommandResult(changed=True)


@flink_job_family.handler()
async def restore(
    ctx: restate.ObjectContext, request: RestoreRequest
) -> CommandResult:
    """Reconcile a pre-saga intent against fresh authoritative cluster state."""
    status = await ctx.run_typed(
        "refresh cluster state", read_status, CLUSTER_RUN_OPTIONS, ctx.key()
    )
    ctx.set(STATE_KEY, status.state.value)
    changed = False

    if request.desired_state is not None and status.state != request.desired_state:
        if request.desired_state == FamilyState.SUSPENDED:
            await ctx.run_typed(
                "restore savepoint",
                trigger_savepoint,
                CLUSTER_RUN_OPTIONS,
                ctx.key(),
            )
            await ctx.run_typed(
                "restore suspended state",
                suspend_job,
                CLUSTER_RUN_OPTIONS,
                ctx.key(),
            )
        else:
            await ctx.run_typed(
                "restore running state",
                resume_job,
                CLUSTER_RUN_OPTIONS,
                ctx.key(),
            )
        ctx.set(STATE_KEY, request.desired_state.value)
        changed = True

    if request.datatypes is not None and status.datatypes != request.datatypes:
        await ctx.run_typed(
            "restore configmap",
            patch_configmap,
            CLUSTER_RUN_OPTIONS,
            ctx.key(),
            request.datatypes,
        )
        changed = True

    return CommandResult(changed=changed)


@flink_job_family.handler(kind="shared")
async def current_state(
    ctx: restate.ObjectSharedContext, _request: Empty
) -> ActorState:
    """Expose the persisted cache for tests and operational inspection."""
    cached = await ctx.get(STATE_KEY, type_hint=str)
    return ActorState(state=FamilyState(cached or FamilyState.RUNNING.value))
