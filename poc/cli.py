"""Run any scenario by name against a local Temporal server.

    temporal server start-dev            # in another terminal
    uv run python -m poc.cli list
    uv run python -m poc.cli run fail-in-resume

Seeds a fresh cluster directory, installs the scenario's chaos rules, runs the
saga with an in-process worker, then prints the audit log and the resulting
cluster state.
"""

from __future__ import annotations

import argparse
import asyncio
from enum import StrEnum
import os
import shutil
import sys
from pathlib import Path


from poc.common.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.common.scenarios import SCENARIOS, SEED

DEFAULT_ROOT = Path(".cluster")
DEFAULT_ENGINE = "None"


class WorkflowEngine(StrEnum):
    NONE = "None"
    TEMPORAL = "Temporal"


def _print_scenarios() -> None:
    width = max(len(name) for name in SCENARIOS)
    for name, scenario in SCENARIOS.items():
        marker = "fails" if scenario.expect_failure else "ok   "
        print(f"  {name:<{width}}  [{marker}]  {scenario.description}")


async def _run(name: str, root: Path, engine: WorkflowEngine) -> int:
    scenario = SCENARIOS.get(name)
    if scenario is None:
        print(f"unknown scenario: {name}\n", file=sys.stderr)
        _print_scenarios()
        return 2

    if root.exists():
        shutil.rmtree(root)
    os.environ[ENV_CLUSTER_ROOT] = str(root)
    cluster = MockCluster(root)
    cluster.seed(SEED)
    cluster.set_chaos(scenario.chaos)

    print(f"scenario : {scenario.name}")
    print(f"  {scenario.description}")
    print(f"cluster  : {root}/\n")

    match engine:
        case WorkflowEngine.NONE:
            from poc.baseline.cli import _run_workflow
        case WorkflowEngine.TEMPORAL:
            from poc.temporal.cli import _run_workflow
        case _:
            raise AttributeError(
                f"Chosen engine [{engine}] is not supported. Choose on of {list(map(lambda x: x.value, WorkflowEngine))}"
            )

    failed = await _run_workflow(scenario)

    print("audit log (every attempt, in order):")
    for entry in cluster.audit():
        print(f"  {entry.seq:>3}  {entry}")

    print("\nfinal cluster state:")
    for family in sorted(SEED):
        status = cluster.read(family)
        print(f"  {family}: {status.state.value:<9} datatypes={status.datatypes}")

    if scenario.expect_failure != failed:
        expected = "failure" if scenario.expect_failure else "success"
        print(
            f"\nWARNING: scenario expected {expected} but got the opposite",
            file=sys.stderr,
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="poc.cli", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="list available scenarios")
    run = sub.add_parser("run", help="run one scenario")
    run.add_argument("scenario")
    run.add_argument(
        "--root", type=Path, default=DEFAULT_ROOT, help="cluster directory"
    )
    run.add_argument(
        "--engine", type=str, default=DEFAULT_ENGINE, help="Run engine {None, Temporal}"
    )

    args = parser.parse_args()
    if args.command == "list":
        _print_scenarios()
        return 0
    return asyncio.run(_run(args.scenario, args.root, args.engine))


if __name__ == "__main__":
    raise SystemExit(main())
