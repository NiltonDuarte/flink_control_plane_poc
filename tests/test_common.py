"""Shared-module boundaries and baseline integration smoke tests."""

from __future__ import annotations

import ast
from pathlib import Path

from poc import cli as temporal_cli
from poc.common.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.common.domain import FamilyState
from poc.common.scenarios import REQUEST, SCENARIOS, SEED, SOURCE, TARGET
from poc_baseline import cli as baseline_cli
from poc_baseline.actor import FamilyActor
from poc_baseline.saga import MoveDatatypeWorkflow

COMMON_ROOT = Path(__file__).parents[1] / "poc" / "common"


def test_common_package_has_no_engine_specific_imports() -> None:
    """Keep shared modules free of Temporal and implementation code."""
    forbidden: list[str] = []
    for path in COMMON_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                modules.append(node.module)

            for module in modules:
                engine_specific = module.startswith("temporalio") or module.startswith(
                    "poc_baseline"
                )
                non_common_poc = module.startswith("poc.") and not module.startswith(
                    "poc.common"
                )
                if engine_specific or non_common_poc:
                    forbidden.append(f"{path.name}: {module}")

    assert forbidden == []


def test_old_shared_module_paths_are_removed() -> None:
    project_root = COMMON_ROOT.parents[1]
    old_paths = [
        project_root / package / f"{module}.py"
        for package in ("poc", "poc_baseline")
        for module in ("cluster", "domain", "scenarios")
    ]

    assert [str(path) for path in old_paths if path.exists()] == []


def test_both_clis_list_the_shared_scenarios(capsys) -> None:
    temporal_cli._print_scenarios()
    temporal_output = capsys.readouterr().out

    baseline_cli._print_scenarios()
    baseline_output = capsys.readouterr().out

    assert baseline_output == temporal_output
    for name in SCENARIOS:
        assert name in temporal_output


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
