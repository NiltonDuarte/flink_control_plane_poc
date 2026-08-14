"""Run any scenario by name, on any implemented engine.

    temporal server start-dev            # in another terminal
    uv run python -m poc.cli list
    uv run python -m poc.cli run fail-in-resume
    uv run python -m poc.cli run fail-in-resume --engine restate

Seeds a fresh cluster directory, installs the scenario's chaos rules, runs the
saga, then prints the audit log and the resulting cluster state.

Everything below the `--engine` switch is engine-agnostic on purpose: the
scenario, the seed, the chaos rules and the audit log are properties of the
saga, not of whatever is executing it. Comparing two engines means reading the
same output twice, so this file must not have a favourite.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import sys
from pathlib import Path

from poc.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.scenarios import SCENARIOS, SEED, Scenario

DEFAULT_ROOT = Path(".cluster")
ENGINES = ["temporal", "restate"]


async def _execute(
    engine: str, scenario: Scenario
) -> tuple[list[str] | None, Exception | None]:
    """Hand the scenario to an adapter.

    Imported late, so one engine's dependencies are never a prerequisite for
    running the other.
    """
    from poc.adapters.temporal.run import run_scenario

    return await run_scenario(scenario)


def _print_scenarios() -> None:
    width = max(len(name) for name in SCENARIOS)
    for name, scenario in SCENARIOS.items():
        marker = "fails" if scenario.expect_failure else "ok   "
        print(f"  {name:<{width}}  [{marker}]  {scenario.description}")


async def _run(name: str, root: Path, engine: str) -> int:
    scenario = SCENARIOS.get(name)
    if scenario is None:
        print(f"unknown scenario: {name}\n", file=sys.stderr)
        _print_scenarios()
        return 2

    if engine == "restate":
        # Before anything is seeded: an engine that cannot run the scenario
        # should not leave a cluster directory behind suggesting it tried.
        from poc.adapters.restate import unavailable

        print(f"{unavailable()}", file=sys.stderr)
        return 3

    if root.exists():
        shutil.rmtree(root)
    os.environ[ENV_CLUSTER_ROOT] = str(root)
    cluster = MockCluster(root)
    cluster.seed(SEED)
    cluster.set_chaos(scenario.chaos)

    print(f"scenario : {scenario.name}")
    print(f"  {scenario.description}")
    print(f"engine   : {engine}")
    print(f"cluster  : {root}/\n")

    steps, err = await _execute(engine, scenario)

    if err is None:
        print(f"RESULT   : completed - {', '.join(steps or [])}\n")
    else:
        print(f"RESULT   : failed - {err}\n")

    print("audit log (every attempt, in order):")
    for entry in cluster.audit():
        print(f"  {entry.seq:>3}  {entry}")

    print("\nfinal cluster state:")
    for family in sorted(SEED):
        status = cluster.read(family)
        print(f"  {family}: {status.state.value:<9} datatypes={status.datatypes}")

    if scenario.expect_failure != (err is not None):
        expected = "failure" if scenario.expect_failure else "success"
        print(f"\nWARNING: scenario expected {expected} but got the opposite", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="poc.cli", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="list available scenarios")
    run = sub.add_parser("run", help="run one scenario")
    run.add_argument("scenario")
    run.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="cluster directory")
    run.add_argument(
        "--engine",
        choices=ENGINES,
        default="temporal",
        help="durable execution engine to run the saga on (default: temporal)",
    )

    args = parser.parse_args()
    if args.command == "list":
        _print_scenarios()
        return 0
    return asyncio.run(_run(args.scenario, args.root, args.engine))


if __name__ == "__main__":
    raise SystemExit(main())
