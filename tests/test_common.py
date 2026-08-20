"""Shared-module boundaries and baseline integration smoke tests."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from pydantic import ValidationError

from poc.common.cluster import MockCluster
from poc.common.domain import FamilyState, MoveDatatypeRequest
from poc.common.scenarios import SEED, SOURCE

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
                engine_specific = module.startswith(("temporalio", "poc_baseline"))
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


def test_repeated_operation_identity_deduplicates_savepoint_side_effect(
    tmp_path: Path,
) -> None:
    cluster = MockCluster(tmp_path / "cluster")
    cluster.seed(SEED)

    first_uri = cluster.trigger_savepoint(SOURCE, "savepoint-operation")
    first_generation = cluster.read(SOURCE).generation
    repeated_uri = cluster.trigger_savepoint(SOURCE, "savepoint-operation")

    assert repeated_uri == first_uri
    assert cluster.read(SOURCE).generation == first_generation
    assert len(list(cluster.savepoints_dir.glob("*.json"))) == 1
    assert [entry.changed for entry in cluster.audit()] == [True, False]

    distinct_uri = cluster.trigger_savepoint(SOURCE, "distinct-savepoint-operation")
    assert distinct_uri != first_uri
    assert cluster.read(SOURCE).generation == first_generation + 1
    assert len(list(cluster.savepoints_dir.glob("*.json"))) == 2


def test_state_mutations_are_noops_when_desired_state_already_exists(
    tmp_path: Path,
) -> None:
    cluster = MockCluster(tmp_path / "cluster")
    cluster.seed(SEED)

    assert cluster.suspend_job(SOURCE, "suspend-1") is True
    generation = cluster.read(SOURCE).generation
    assert cluster.suspend_job(SOURCE, "suspend-1") is False
    assert cluster.suspend_job(SOURCE, "suspend-2") is False
    assert cluster.read(SOURCE).generation == generation

    assert cluster.patch_configmap(SOURCE, ["clicks"], "patch-1") is True
    generation = cluster.read(SOURCE).generation
    assert cluster.patch_configmap(SOURCE, ["clicks"], "patch-1") is False
    assert cluster.patch_configmap(SOURCE, ["clicks"], "patch-2") is False
    assert cluster.read(SOURCE).generation == generation

    assert cluster.resume_job(SOURCE, "resume-1") is True
    generation = cluster.read(SOURCE).generation
    assert cluster.resume_job(SOURCE, "resume-1") is False
    assert cluster.resume_job(SOURCE, "resume-2") is False
    assert cluster.read(SOURCE).generation == generation

    attempts = cluster.audit()
    assert len(attempts) == 9
    assert sum(entry.changed for entry in attempts) == 3
    assert len(cluster.audit(effective_only=True)) == 3


@pytest.mark.parametrize(
    "family",
    ["", ".", "../escape", "nested/family", "nested\\family", "a" * 129],
)
def test_invalid_family_identifiers_fail_before_filesystem_access(
    tmp_path: Path, family: str
) -> None:
    root = tmp_path / "cluster"
    cluster = MockCluster(root)

    with pytest.raises(ValueError, match="family identifier"):
        cluster.seed({family: []})
    assert not root.exists()

    cluster.seed(SEED)
    before = sorted(path.relative_to(root) for path in root.rglob("*"))
    with pytest.raises(ValueError, match="family identifier"):
        cluster.read(family)
    with pytest.raises(ValueError, match="family identifier"):
        cluster.suspend_job(family, "invalid-family-operation")
    assert sorted(path.relative_to(root) for path in root.rglob("*")) == before
    assert cluster.audit() == []

    with pytest.raises(ValidationError, match="family identifier"):
        MoveDatatypeRequest(
            datatype="clicks",
            source_family=family,
            target_family="family_b",
        )


def test_effective_audit_filter_excludes_failed_and_deduplicated_attempts(
    tmp_path: Path,
) -> None:
    cluster = MockCluster(tmp_path / "cluster")
    cluster.seed(SEED)

    cluster.resume_job(SOURCE, "already-running")
    cluster.suspend_job(SOURCE, "suspend")
    cluster.suspend_job(SOURCE, "suspend")

    assert [entry.changed for entry in cluster.audit()] == [False, True, False]
    assert [entry.op for entry in cluster.audit(effective_only=True)] == ["suspend_job"]
    assert cluster.read(SOURCE).state == FamilyState.SUSPENDED
