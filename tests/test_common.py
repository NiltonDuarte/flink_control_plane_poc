"""Shared-module boundaries and baseline integration smoke tests."""

from __future__ import annotations

import ast
from pathlib import Path


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
