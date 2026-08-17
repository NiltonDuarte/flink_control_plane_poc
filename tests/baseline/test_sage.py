from pathlib import Path


from poc.common.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.common.domain import FamilyState
from poc.common.scenarios import REQUEST, SCENARIOS, SEED, SOURCE, TARGET
from poc.baseline.actor import FamilyActor
from poc.baseline.saga import MoveDatatypeWorkflow

def test_baseline_executes_happy_path_with_shared_models(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "cluster"
    monkeypatch.setenv(ENV_CLUSTER_ROOT, str(root))
    cluster = MockCluster(root)
    cluster.seed(SEED)
    cluster.set_chaos(SCENARIOS["happy"].chaos)
    FamilyActor._instances.clear()

    try:
        steps = MoveDatatypeWorkflow().run(REQUEST)
    finally:
        FamilyActor._instances.clear()

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
