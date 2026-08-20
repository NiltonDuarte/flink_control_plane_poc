"""Baseline parity for saga verdicts, retries, and compensation."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from poc.baseline.actor import FamilyActor
from poc.baseline.saga import MoveDatatypeWorkflow
from poc.common.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.common.domain import (
    FamilyState,
    MoveDatatypeRequest,
    SagaError,
    SagaOutcome,
)
from poc.common.scenarios import REQUEST, SCENARIOS, SEED, SOURCE, TARGET, Scenario


@pytest.fixture
def cluster(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[MockCluster]:
    root = tmp_path / "cluster"
    monkeypatch.setenv(ENV_CLUSTER_ROOT, str(root))
    instance = MockCluster(root)
    instance.seed(SEED)
    FamilyActor._instances.clear()
    yield instance
    FamilyActor._instances.clear()


def run_scenario(
    cluster: MockCluster, scenario: Scenario
) -> tuple[list[str] | None, SagaError | None]:
    cluster.set_chaos(scenario.chaos)
    try:
        return MoveDatatypeWorkflow().run(REQUEST), None
    except SagaError as err:
        return None, err


@pytest.mark.parametrize("scenario_name", SCENARIOS)
def test_all_scenarios_have_expected_result_and_final_state(
    cluster: MockCluster, scenario_name: str
) -> None:
    scenario = SCENARIOS[scenario_name]
    before = cluster.snapshot()

    steps, err = run_scenario(cluster, scenario)

    if not scenario.expect_failure:
        assert err is None
        assert steps == [
            f"pause:{SOURCE}",
            f"pause:{TARGET}",
            f"patch:{SOURCE}",
            f"patch:{TARGET}",
            f"resume:{SOURCE}",
            f"resume:{TARGET}",
        ]
        assert cluster.read(SOURCE).state == FamilyState.RUNNING
        assert cluster.read(SOURCE).datatypes == ["impressions"]
        assert cluster.read(TARGET).state == FamilyState.RUNNING
        assert cluster.read(TARGET).datatypes == ["views", "clicks"]
        return

    assert steps is None
    assert err is not None
    expected_outcome = (
        SagaOutcome.COMPENSATION_INCOMPLETE
        if scenario_name == "compensation-unreachable"
        else SagaOutcome.COMPENSATED
    )
    assert err.failure.outcome == expected_outcome
    if expected_outcome == SagaOutcome.COMPENSATED:
        assert cluster.snapshot() == before
    else:
        assert cluster.read(SOURCE).state == FamilyState.RUNNING
        assert cluster.read(TARGET).state == FamilyState.SUSPENDED
        assert cluster.read(SOURCE).datatypes == SEED[SOURCE]
        assert cluster.read(TARGET).datatypes == SEED[TARGET]


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
def test_validation_rejections_are_typed_and_do_not_mutate(
    cluster: MockCluster,
    move_request: MoveDatatypeRequest,
    seed: dict[str, list[str]] | None,
    reason: str,
) -> None:
    if seed is not None:
        cluster.seed(seed)
    cluster.set_chaos({})
    before = cluster.snapshot()

    with pytest.raises(SagaError) as raised:
        MoveDatatypeWorkflow().run(move_request)

    err = raised.value
    assert err.type == "SagaRejectedError"
    assert err.failure.outcome == SagaOutcome.REJECTED
    assert err.failure.failed_step == "validate"
    assert err.failure.reason == reason
    assert err.failure.compensated == []
    assert err.failure.compensation_noops == []
    assert err.failure.compensation_errors == []
    assert cluster.snapshot() == before
    assert cluster.audit() == []


@pytest.mark.parametrize(
    ("scenario_name", "operation", "expected_outcomes"),
    [
        (
            "transient-retry",
            "suspend_job",
            ["transient", "transient", "ok"],
        ),
        ("transient-exhausted", "suspend_job", ["transient"] * 4),
        ("permanent-first-step", "trigger_savepoint", ["permanent"]),
    ],
)
def test_retry_counts_follow_the_error_taxonomy(
    cluster: MockCluster,
    scenario_name: str,
    operation: str,
    expected_outcomes: list[str],
) -> None:
    _, err = run_scenario(cluster, SCENARIOS[scenario_name])

    attempts = [
        entry
        for entry in cluster.audit()
        if entry.op == operation
        and entry.family
        == (SOURCE if scenario_name == "permanent-first-step" else TARGET)
    ]
    assert [entry.outcome for entry in attempts] == expected_outcomes
    assert (err is not None) == SCENARIOS[scenario_name].expect_failure
    if scenario_name == "transient-exhausted":
        assert err is not None
        assert "attempt 4/99" in err.failure.reason


def test_deep_compensation_reports_applied_and_noop_restores(
    cluster: MockCluster,
) -> None:
    before = cluster.snapshot()

    _, err = run_scenario(cluster, SCENARIOS["fail-in-resume"])

    assert err is not None
    assert err.failure.outcome == SagaOutcome.COMPENSATED
    assert err.failure.compensated == [
        f"pause:{SOURCE}",
        f"revert:{TARGET}",
        f"revert:{SOURCE}",
        f"resume:{TARGET}",
        f"resume:{SOURCE}",
    ]
    assert err.failure.compensation_noops == [f"pause:{TARGET}"]
    assert err.failure.compensation_errors == []
    assert cluster.snapshot() == before


def test_failed_compensation_is_reported_and_remaining_restores_continue(
    cluster: MockCluster,
) -> None:
    _, err = run_scenario(cluster, SCENARIOS["compensation-unreachable"])

    assert err is not None
    assert err.failure.outcome == SagaOutcome.COMPENSATION_INCOMPLETE
    assert err.failure.compensated == [
        f"pause:{SOURCE}",
        f"revert:{TARGET}",
        f"revert:{SOURCE}",
        f"resume:{SOURCE}",
    ]
    assert err.failure.compensation_noops == [f"pause:{TARGET}"]
    assert len(err.failure.compensation_errors) == 1
    assert err.failure.compensation_errors[0].startswith(f"resume:{TARGET}:")
