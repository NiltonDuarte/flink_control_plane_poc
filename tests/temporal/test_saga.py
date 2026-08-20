"""Saga compensation: the four RFC scenarios plus the retry taxonomy.

Every failure test makes two assertions, and the first is the one that matters:

1. the audit log is exactly the expected **sequence** of side effects, and
2. the observable cluster state is back where it started.

The order assertion is the stronger of the two. A rollback that compensates in
the wrong order still lands in the right final state, so a final-state check
alone would wave it through.

`snapshot()` deliberately ignores `generation` and `savepoint_uri` - see the note
on `MockCluster.snapshot`. Rollback restores state and routing, not history.
"""

from __future__ import annotations

import pytest
from temporalio.client import Client

from poc.common.cluster import MockCluster
from poc.common.domain import MoveDatatypeRequest, SagaOutcome
from poc.common.scenarios import REQUEST, SCENARIOS, SEED, SOURCE, TARGET
from tests.conftest import (
    ops,
    run_move,
    run_scenario,
    saga_application_error,
    saga_failure,
)

FORWARD_PHASE_1 = [
    f"trigger_savepoint({SOURCE})",
    f"suspend_job({SOURCE})",
    f"trigger_savepoint({TARGET})",
    f"suspend_job({TARGET})",
]

FAILING_SCENARIOS = [
    name for name, scenario in SCENARIOS.items() if scenario.expect_failure
]


@pytest.mark.parametrize("scenario_name", FAILING_SCENARIOS)
async def test_failure_scenarios_expose_typed_verdicts(
    client: Client, cluster: MockCluster, scenario_name: str
) -> None:
    """Every operational failure has the same searchable four-part contract."""
    _, err = await run_scenario(client, cluster, SCENARIOS[scenario_name])

    assert err is not None
    failure = saga_failure(err)
    application_error = saga_application_error(err)
    expected_outcome = (
        SagaOutcome.COMPENSATION_INCOMPLETE
        if scenario_name == "compensation-unreachable"
        else SagaOutcome.COMPENSATED
    )
    expected_type = (
        "SagaCompensationIncompleteError"
        if expected_outcome == SagaOutcome.COMPENSATION_INCOMPLETE
        else "SagaCompensatedError"
    )

    assert failure.outcome == expected_outcome
    assert application_error.type == expected_type
    assert application_error.non_retryable is True
    assert application_error.message.startswith(f"{expected_outcome.value}:")
    assert f"applied={len(failure.compensated)}" in application_error.message
    assert f"no_op={len(failure.compensation_noops)}" in application_error.message
    assert f"failed={len(failure.compensation_errors)}" in application_error.message


VALIDATION_CASES = [
    pytest.param(
        MoveDatatypeRequest(
            datatype=REQUEST.datatype,
            source_family=SOURCE,
            target_family=SOURCE,
        ),
        None,
        "source and target family are the same",
        id="same-family",
    ),
    pytest.param(
        MoveDatatypeRequest(
            datatype="missing",
            source_family=SOURCE,
            target_family=TARGET,
        ),
        None,
        f"missing is not owned by {SOURCE}",
        id="missing-source-ownership",
    ),
    pytest.param(
        REQUEST,
        {SOURCE: SEED[SOURCE], TARGET: [*SEED[TARGET], REQUEST.datatype]},
        f"{REQUEST.datatype} is already owned by {TARGET}",
        id="datatype-already-at-target",
    ),
]


@pytest.mark.parametrize("move_request, seed, reason", VALIDATION_CASES)
async def test_validation_rejections_are_typed_and_do_not_mutate(
    client: Client,
    cluster: MockCluster,
    move_request: MoveDatatypeRequest,
    seed: dict[str, list[str]] | None,
    reason: str,
) -> None:
    if seed is not None:
        cluster.seed(seed)
    cluster.set_chaos({})
    before = cluster.snapshot()

    _, err = await run_move(client, request=move_request, workflow_name="validation")

    assert err is not None
    failure = saga_failure(err)
    application_error = saga_application_error(err)
    assert failure.outcome == SagaOutcome.REJECTED
    assert failure.failed_step == "validate"
    assert failure.reason == reason
    assert failure.compensated == []
    assert failure.compensation_noops == []
    assert failure.compensation_errors == []
    assert application_error.type == "SagaRejectedError"
    assert application_error.non_retryable is True
    assert application_error.message.startswith("REJECTED:")
    assert cluster.snapshot() == before
    assert ops(cluster, successful_only=False) == []


async def test_happy_path(client: Client, cluster: MockCluster) -> None:
    steps, err = await run_scenario(client, cluster, SCENARIOS["happy"])

    assert err is None
    assert steps == [
        f"pause:{SOURCE}",
        f"pause:{TARGET}",
        f"patch:{SOURCE}",
        f"patch:{TARGET}",
        f"resume:{SOURCE}",
        f"resume:{TARGET}",
    ]
    assert ops(cluster) == [
        *FORWARD_PHASE_1,
        f"patch_configmap({SOURCE})",
        f"patch_configmap({TARGET})",
        f"resume_job({SOURCE})",
        f"resume_job({TARGET})",
    ]
    # The datatype actually moved.
    assert cluster.read(SOURCE).datatypes == ["impressions"]
    assert cluster.read(TARGET).datatypes == ["views", "clicks"]


async def test_fail_in_pause_compensates(client: Client, cluster: MockCluster) -> None:
    """RFC scenario 2: resume only what was paused; no config was ever touched."""
    before = cluster.snapshot()

    _, err = await run_scenario(client, cluster, SCENARIOS["fail-in-pause"])

    assert err is not None
    assert ops(cluster) == [
        f"trigger_savepoint({SOURCE})",
        f"suspend_job({SOURCE})",
        f"trigger_savepoint({TARGET})",
        # suspend_job(TARGET) fails - not in the successful-only view
        f"resume_job({SOURCE})",
    ]
    assert cluster.snapshot() == before


async def test_fail_in_update_compensates(client: Client, cluster: MockCluster) -> None:
    """RFC scenario 3: revert the config that landed, then resume both."""
    before = cluster.snapshot()

    _, err = await run_scenario(client, cluster, SCENARIOS["fail-in-update"])

    assert err is not None
    assert ops(cluster) == [
        *FORWARD_PHASE_1,
        f"patch_configmap({SOURCE})",
        # patch_configmap(TARGET) fails
        f"patch_configmap({SOURCE})",  # reverted
        f"resume_job({TARGET})",
        f"resume_job({SOURCE})",
    ]
    assert cluster.snapshot() == before


async def test_fail_in_resume_compensates_in_layers(
    client: Client, cluster: MockCluster
) -> None:
    """RFC scenario 4: the deep rollback.

    Re-pause what was resumed, revert every config, then resume everything. This
    ordering is not special-cased anywhere - it is what a plain LIFO unwind of
    the compensation stack produces.
    """
    before = cluster.snapshot()

    _, err = await run_scenario(client, cluster, SCENARIOS["fail-in-resume"])

    assert err is not None
    failure = saga_failure(err)
    assert failure.compensated == [
        f"pause:{SOURCE}",
        f"revert:{TARGET}",
        f"revert:{SOURCE}",
        f"resume:{TARGET}",
        f"resume:{SOURCE}",
    ]
    assert failure.compensation_noops == [f"pause:{TARGET}"]
    assert failure.compensation_errors == []
    assert ops(cluster) == [
        *FORWARD_PHASE_1,
        f"patch_configmap({SOURCE})",
        f"patch_configmap({TARGET})",
        f"resume_job({SOURCE})",
        # resume_job(TARGET) exhausts its retries
        f"trigger_savepoint({SOURCE})",  # re-pause what was resumed
        f"suspend_job({SOURCE})",
        f"patch_configmap({TARGET})",  # revert both configs
        f"patch_configmap({SOURCE})",
        f"resume_job({TARGET})",  # restore both to running
        f"resume_job({SOURCE})",
    ]
    assert cluster.snapshot() == before


async def test_compensation_can_itself_fail(
    client: Client, cluster: MockCluster
) -> None:
    """The limitation worth knowing about.

    When the broken action *is* the compensating action, a fail-fast saga cannot
    deliver all-or-nothing. It reports the problem rather than resolving it, and
    the cluster is left partially rolled back - here, TARGET stays suspended.
    """
    _, err = await run_scenario(client, cluster, SCENARIOS["compensation-unreachable"])

    assert err is not None
    failure = saga_failure(err)
    assert failure.compensated == [
        f"pause:{SOURCE}",
        f"revert:{TARGET}",
        f"revert:{SOURCE}",
        f"resume:{SOURCE}",
    ]
    assert failure.compensation_noops == [f"pause:{TARGET}"]
    assert len(failure.compensation_errors) == 1
    assert failure.compensation_errors[0].startswith(f"resume:{TARGET}:")
    assert cluster.read(SOURCE).datatypes == ["clicks", "impressions"]
    assert cluster.read(TARGET).datatypes == ["views"]
    # Configs were restored, but the runtime state was not.
    assert cluster.read(SOURCE).state.value == "RUNNING"
    assert cluster.read(TARGET).state.value == "SUSPENDED"


async def test_transient_faults_are_retried(
    client: Client, cluster: MockCluster
) -> None:
    """Two transient failures are absorbed inside the actor; the saga never sees them."""
    _, err = await run_scenario(client, cluster, SCENARIOS["transient-retry"])

    assert err is None
    attempts = [
        entry
        for entry in cluster.audit()
        if entry.op == "suspend_job" and entry.family == TARGET
    ]
    assert [entry.outcome for entry in attempts] == ["transient", "transient", "ok"]
    # The move still completed.
    assert cluster.read(TARGET).datatypes == ["views", "clicks"]


async def test_permanent_faults_are_not_retried(
    client: Client, cluster: MockCluster
) -> None:
    """A permanent fault burns exactly one attempt.

    This is the point of the transient/permanent split: retrying a permanent
    failure cannot help, and every wasted attempt delays compensation.
    """
    before = cluster.snapshot()

    _, err = await run_scenario(client, cluster, SCENARIOS["permanent-first-step"])

    assert err is not None
    attempts = [entry for entry in cluster.audit() if entry.op == "trigger_savepoint"]
    assert len(attempts) == 1
    assert attempts[0].outcome == "permanent"
    # The pre-registered restore is evaluated but produces no mutation.
    assert ops(cluster) == []
    assert cluster.snapshot() == before


async def test_exhausted_transient_becomes_a_failure(
    client: Client, cluster: MockCluster
) -> None:
    """Transient does not mean forgiving: past the budget it compensates like any failure."""
    before = cluster.snapshot()

    _, err = await run_scenario(client, cluster, SCENARIOS["transient-exhausted"])

    assert err is not None
    attempts = [
        entry
        for entry in cluster.audit()
        if entry.op == "suspend_job" and entry.family == TARGET
    ]
    assert len(attempts) == 4  # CLUSTER_RETRY.maximum_attempts
    assert cluster.snapshot() == before


async def test_lost_suspend_response_is_reconciled(
    client: Client, cluster: MockCluster
) -> None:
    """A landed pause is restored even though its actor never cached the result."""
    before = cluster.snapshot()

    _, err = await run_scenario(client, cluster, SCENARIOS["lost-response-suspend"])

    assert err is not None
    failure = saga_failure(err)
    assert failure.compensated == [f"resume:{TARGET}", f"resume:{SOURCE}"]
    assert failure.compensation_noops == []
    assert failure.compensation_errors == []
    attempts = [
        entry
        for entry in cluster.audit()
        if entry.op == "suspend_job" and entry.family == TARGET
    ]
    assert len(attempts) == 4
    assert all(entry.outcome == "ok" for entry in attempts)
    assert cluster.snapshot() == before


async def test_lost_patch_response_restores_snapshot_config(
    client: Client, cluster: MockCluster
) -> None:
    """Rollback never trusts a retry's already-patched previous value."""
    before = cluster.snapshot()

    _, err = await run_scenario(client, cluster, SCENARIOS["lost-response-patch"])

    assert err is not None
    failure = saga_failure(err)
    assert failure.compensated == [
        f"revert:{SOURCE}",
        f"resume:{TARGET}",
        f"resume:{SOURCE}",
    ]
    assert failure.compensation_noops == []
    assert failure.compensation_errors == []
    patches = [
        entry
        for entry in cluster.audit()
        if entry.op == "patch_configmap" and entry.family == SOURCE
    ]
    assert len(patches) == 5  # four landed attempts, then one snapshot restore
    assert patches[-1].detail["to"] == ["clicks", "impressions"]
    assert cluster.snapshot() == before
