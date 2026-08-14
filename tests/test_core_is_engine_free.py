"""The rule that makes the comparison honest, enforced mechanically.

`poc/core/` may not know which engine is executing it, and neither may the mock
cluster. Documenting that in a docstring is worth something; failing a test when
somebody breaks it is worth more, because the first import of `temporalio` into
the core is exactly the point at which "the same saga on two engines" quietly
stops being true.

Two properties, checked by reading the source rather than by importing it - an
import test would pass simply because the dependency happens to be installed.

1. **No engine SDK, in either direction.** `restate` is banned as firmly as
   `temporalio`: once issue #11 lands, a Restate import in the core would break
   Temporal just as surely as the reverse.
2. **No `await`, anywhere.** This is the structural half of the guarantee. The
   core cannot open a socket, sleep, or call an engine even by accident, because
   it has no suspension point that isn't a `yield` handed to an adapter.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
CORE = REPO_ROOT / "poc" / "core"

# The mock cluster and the scenario matrix are shared by every adapter too, so
# they live under the same rule.
ENGINE_FREE = sorted(CORE.glob("*.py")) + [
    REPO_ROOT / "poc" / "cluster.py",
    REPO_ROOT / "poc" / "scenarios.py",
]

BANNED_ROOTS = {"temporalio", "restate"}


def _module_names(source: ast.Module) -> set[str]:
    """Every module named by an import statement, fully dotted."""
    names: set[str] = set()
    for node in ast.walk(source):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            names.add(node.module)
    return names


def _is_banned(module: str) -> bool:
    return module.split(".")[0] in BANNED_ROOTS or module.startswith("poc.adapters")


@pytest.mark.parametrize(
    "path", ENGINE_FREE, ids=lambda p: str(p.relative_to(REPO_ROOT))
)
def test_module_imports_no_engine(path: Path) -> None:
    offenders = sorted(
        module for module in _module_names(ast.parse(path.read_text())) if _is_banned(module)
    )
    assert not offenders, (
        f"{path.relative_to(REPO_ROOT)} imports {offenders}. Whatever needs it "
        f"belongs in poc/adapters/, behind a port in poc/core/ports.py."
    )


@pytest.mark.parametrize(
    "path", sorted(CORE.glob("*.py")), ids=lambda p: str(p.relative_to(REPO_ROOT))
)
def test_core_module_never_awaits(path: Path) -> None:
    """The core yields what it wants done; an adapter does it. No exceptions.

    (`async def` is allowed - `poc/core/ports.py` declares the protocols an
    adapter implements. Declaring the obligation is not performing it.)
    """
    tree = ast.parse(path.read_text())
    awaits = [node for node in ast.walk(tree) if isinstance(node, ast.Await)]
    assert not awaits, (
        f"{path.relative_to(REPO_ROOT)} awaits on line {awaits[0].lineno}. The "
        f"core is synchronous by construction - yield the operation instead."
    )
