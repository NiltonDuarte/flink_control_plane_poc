"""Full shared-scenario parity through the Restate test harness."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
import restate
from restate.types import HarnessEnvironment

from poc.baseline.actor import FamilyActor
from poc.baseline.saga import MoveDatatypeWorkflow as BaselineWorkflow
from poc.common.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.common.domain import (
    FamilyState,
    MoveDatatypeRequest,
    SagaError,
    SagaFailure,
    SagaOutcome,
)
from poc.common.scenarios import REQUEST, SCENARIOS, SEED, SOURCE, TARGET, Scenario
from poc.restate.client import ENV_RESTATE_INGRESS_URL
from poc.restate.errors import SAGA_HTTP_STATUS, decode_saga_failure
from poc.restate.saga import run
from tests.restate.conftest import RestateCase

pytestmark = pytest.mark.asyncio(loop_scope="session")


def _audit(cluster: MockCluster, case: RestateCase) -> list[tuple[str, str, str]]:
    return [
        (entry.op, case.normalize(entry.family), entry.outcome)
        for entry in cluster.audit()
    ]


def _snapshot(
    cluster: MockCluster, case: RestateCase
) -> dict[str, tuple[FamilyState, tuple[str, ...]]]:
    return {
        case.normalize(family): value for family, value in cluster.snapshot().items()
    }


def _labels(labels: list[str], case: RestateCase) -> list[str]:
    return [case.normalize(label) for label in labels]


def _decode(error: restate.HttpError) -> tuple[SagaFailure, str]:
    decoded = decode_saga_failure(error.body or str(error))
    assert decoded is not None, error.body
    return decoded


def _run_baseline(
    root: Path, case: RestateCase, scenario: Scenario
) -> tuple[list[str] | None, SagaFailure | None, MockCluster]:
    cluster = MockCluster(root)
    cluster.seed({case.source: SEED[SOURCE], case.target: SEED[TARGET]})
    cluster.set_chaos(case.scenario(scenario).chaos)
    os.environ[ENV_CLUSTER_ROOT] = str(root)
    FamilyActor._instances.clear()
    try:
        return BaselineWorkflow().run(case.request), None, cluster
    except SagaError as err:
        return None, err.failure, cluster
    finally:
        FamilyActor._instances.clear()


@pytest.mark.parametrize("scenario_name", SCENARIOS)
async def test_complete_shared_scenario_matrix_matches_baseline(
    restate_env: HarnessEnvironment,
    restate_case: RestateCase,
    tmp_path: Path,
    scenario_name: str,
) -> None:
    """Compare verdicts, labels, attempts, order, and final state exactly."""
    scenario = SCENARIOS[scenario_name]
    baseline_steps, baseline_failure, baseline_cluster = _run_baseline(
        tmp_path / "baseline-cluster", restate_case, scenario
    )

    os.environ[ENV_CLUSTER_ROOT] = str(restate_case.cluster.root)
    restate_case.cluster.set_chaos(restate_case.scenario(scenario).chaos)
    try:
        restate_steps = await restate_env.client.workflow_call(
            run,
            key=f"matrix-{scenario_name}-{uuid.uuid4()}",
            arg=restate_case.request,
        )
        restate_error = None
    except restate.HttpError as err:
        restate_steps = None
        restate_error = err

    assert _audit(restate_case.cluster, restate_case) == _audit(
        baseline_cluster, restate_case
    )
    assert _snapshot(restate_case.cluster, restate_case) == _snapshot(
        baseline_cluster, restate_case
    )

    if baseline_failure is None:
        assert restate_error is None
        assert restate_steps is not None
        assert _labels(restate_steps, restate_case) == _labels(
            baseline_steps or [], restate_case
        )
        return

    assert restate_steps is None
    assert restate_error is not None
    failure, error_type = _decode(restate_error)
    assert failure.outcome == baseline_failure.outcome
    assert restate_error.status_code == SAGA_HTTP_STATUS[failure.outcome]
    assert error_type.endswith("Error")
    assert restate_case.normalize(failure.failed_step) == restate_case.normalize(
        baseline_failure.failed_step
    )
    assert _labels(failure.compensated, restate_case) == _labels(
        baseline_failure.compensated, restate_case
    )
    assert _labels(failure.compensation_noops, restate_case) == _labels(
        baseline_failure.compensation_noops, restate_case
    )
    assert len(failure.compensation_errors) == len(baseline_failure.compensation_errors)


@pytest.mark.parametrize(
    ("scenario_name", "operation", "family", "outcomes"),
    [
        ("transient-retry", "suspend_job", TARGET, ["transient"] * 2 + ["ok"]),
        ("transient-exhausted", "suspend_job", TARGET, ["transient"] * 4),
        ("permanent-first-step", "trigger_savepoint", SOURCE, ["permanent"]),
    ],
)
async def test_retry_attempts_and_permanent_errors(
    restate_env: HarnessEnvironment,
    restate_case: RestateCase,
    scenario_name: str,
    operation: str,
    family: str,
    outcomes: list[str],
) -> None:
    scenario = restate_case.scenario(SCENARIOS[scenario_name])
    restate_case.cluster.set_chaos(scenario.chaos)
    with pytest.raises(restate.HttpError) if scenario.expect_failure else _no_error():
        await restate_env.client.workflow_call(
            run,
            key=f"retry-{scenario_name}-{uuid.uuid4()}",
            arg=restate_case.request,
        )
    keyed_family = restate_case.source if family == SOURCE else restate_case.target
    attempts = [
        entry.outcome
        for entry in restate_case.cluster.audit()
        if entry.op == operation and entry.family == keyed_family
    ]
    assert attempts == outcomes


class _no_error:
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        return False


@pytest.mark.parametrize(
    ("request_factory", "reason"),
    [
        (
            lambda case: case.request.model_copy(update={"target_family": case.source}),
            "source and target family are the same",
        ),
        (
            lambda case: case.request.model_copy(update={"datatype": "missing"}),
            "missing is not owned by family_a",
        ),
    ],
)
async def test_validation_rejections_are_typed_and_do_not_mutate(
    restate_env: HarnessEnvironment,
    restate_case: RestateCase,
    request_factory,
    reason: str,
) -> None:
    before = restate_case.cluster.snapshot()
    with pytest.raises(restate.HttpError) as raised:
        await restate_env.client.workflow_call(
            run,
            key=f"validation-{uuid.uuid4()}",
            arg=request_factory(restate_case),
        )

    failure, error_type = _decode(raised.value)
    assert failure.outcome == SagaOutcome.REJECTED
    assert failure.failed_step == "validate"
    assert restate_case.normalize(failure.reason) == reason
    assert error_type == "SagaRejectedError"
    assert raised.value.status_code == 400
    assert restate_case.cluster.snapshot() == before
    assert restate_case.cluster.audit() == []


async def test_shared_cli_renders_restate_failure(
    restate_env: HarnessEnvironment,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from poc.cli import WorkflowEngine, _run

    monkeypatch.setenv(ENV_RESTATE_INGRESS_URL, restate_env.ingress_url)
    result = await _run(
        "permanent-first-step", tmp_path / "cli-cluster", WorkflowEngine.RESTATE
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "RESULT   : failed - COMPENSATED (SagaCompensatedError)" in output
    assert "step   : pause:family_a" in output
    assert "rollback: applied=0 no-op=1 failed=0" in output
