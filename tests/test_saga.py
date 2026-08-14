"""Saga compensation: the four RFC scenarios plus the retry taxonomy.

Every failure test makes two assertions, and the first is the one that matters:

1. the audit log is exactly the expected **sequence** of side effects, and
2. the observable cluster state is back where it started.

The order assertion is the stronger of the two. A rollback that compensates in
the wrong order still lands in the right final state, so a final-state check
alone would wave it through.

`snapshot()` deliberately ignores `generation` and `savepoint_uri` - see the note
on `MockCluster.snapshot`. Rollback restores state and routing, not history.

**Nothing below mentions an engine.** These are statements about the saga in
`poc/core/saga.py`, and the `engine` fixture runs them against every registered
adapter (see `tests/engines.py`). When Restate lands it has to satisfy this file
unchanged - which is the only way a comparison between the two means anything.
"""

from __future__ import annotations

from poc.cluster import MockCluster
from poc.scenarios import SCENARIOS, SOURCE, TARGET
from tests.conftest import ops, run_scenario
from tests.engines import EngineHarness

FORWARD_PHASE_1 = [
    f"trigger_savepoint({SOURCE})",
    f"suspend_job({SOURCE})",
    f"trigger_savepoint({TARGET})",
    f"suspend_job({TARGET})",
]


async def test_happy_path(engine: EngineHarness, cluster: MockCluster) -> None:
    steps, err = await run_scenario(engine, cluster, SCENARIOS["happy"])

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


async def test_fail_in_pause_compensates(
    engine: EngineHarness, cluster: MockCluster
) -> None:
    """RFC scenario 2: resume only what was paused; no config was ever touched."""
    before = cluster.snapshot()

    _, err = await run_scenario(engine, cluster, SCENARIOS["fail-in-pause"])

    assert err is not None
    assert ops(cluster) == [
        f"trigger_savepoint({SOURCE})",
        f"suspend_job({SOURCE})",
        f"trigger_savepoint({TARGET})",
        # suspend_job(TARGET) fails - not in the successful-only view
        f"resume_job({SOURCE})",
    ]
    assert cluster.snapshot() == before


async def test_fail_in_update_compensates(
    engine: EngineHarness, cluster: MockCluster
) -> None:
    """RFC scenario 3: revert the config that landed, then resume both."""
    before = cluster.snapshot()

    _, err = await run_scenario(engine, cluster, SCENARIOS["fail-in-update"])

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
    engine: EngineHarness, cluster: MockCluster
) -> None:
    """RFC scenario 4: the deep rollback.

    Re-pause what was resumed, revert every config, then resume everything. This
    ordering is not special-cased anywhere - it is what a plain LIFO unwind of
    the compensation stack produces.
    """
    before = cluster.snapshot()

    _, err = await run_scenario(engine, cluster, SCENARIOS["fail-in-resume"])

    assert err is not None
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
    engine: EngineHarness, cluster: MockCluster
) -> None:
    """The limitation worth knowing about.

    When the broken action *is* the compensating action, a fail-fast saga cannot
    deliver all-or-nothing. It reports the problem rather than resolving it, and
    the cluster is left partially rolled back - here, TARGET stays suspended.
    """
    _, err = await run_scenario(engine, cluster, SCENARIOS["compensation-unreachable"])

    assert err is not None
    assert cluster.read(SOURCE).datatypes == ["clicks", "impressions"]
    assert cluster.read(TARGET).datatypes == ["views"]
    # Configs were restored, but the runtime state was not.
    assert cluster.read(SOURCE).state.value == "RUNNING"
    assert cluster.read(TARGET).state.value == "SUSPENDED"


async def test_transient_faults_are_retried(
    engine: EngineHarness, cluster: MockCluster
) -> None:
    """Two transient failures are absorbed inside the actor; the saga never sees them."""
    _, err = await run_scenario(engine, cluster, SCENARIOS["transient-retry"])

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
    engine: EngineHarness, cluster: MockCluster
) -> None:
    """A permanent fault burns exactly one attempt.

    This is the point of the transient/permanent split: retrying a permanent
    failure cannot help, and every wasted attempt delays compensation.
    """
    before = cluster.snapshot()

    _, err = await run_scenario(engine, cluster, SCENARIOS["permanent-first-step"])

    assert err is not None
    attempts = [entry for entry in cluster.audit() if entry.op == "trigger_savepoint"]
    assert len(attempts) == 1
    assert attempts[0].outcome == "permanent"
    # Nothing succeeded, so there was nothing to compensate.
    assert ops(cluster) == []
    assert cluster.snapshot() == before


async def test_exhausted_transient_becomes_a_failure(
    engine: EngineHarness, cluster: MockCluster
) -> None:
    """Transient does not mean forgiving: past the budget it compensates like any failure."""
    before = cluster.snapshot()

    _, err = await run_scenario(engine, cluster, SCENARIOS["transient-exhausted"])

    assert err is not None
    attempts = [
        entry
        for entry in cluster.audit()
        if entry.op == "suspend_job" and entry.family == TARGET
    ]
    assert len(attempts) == 4  # CLUSTER_RETRY.maximum_attempts
    assert cluster.snapshot() == before
