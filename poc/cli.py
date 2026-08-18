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
import os
import shutil
import sys
import textwrap
from enum import StrEnum
from pathlib import Path

from temporalio.exceptions import ApplicationError

from poc.common.cluster import ENV_CLUSTER_ROOT, MockCluster
from poc.common.domain import SagaError, SagaFailure
from poc.common.scenarios import SCENARIOS, SEED

DEFAULT_ROOT = Path(".cluster")
DEFAULT_ENGINE = "None"


class WorkflowEngine(StrEnum):
    NONE = "None"
    TEMPORAL = "Temporal"
    RESTATE = "Restate"


def _print_scenarios() -> None:
    name_width = max(len(name) for name in SCENARIOS)

    # Calculate the exact number of spaces needed to align wrapped lines
    # "  " (2) + name_width + "  [" (3) + marker (5) + "]  " (3) = name_width + 13
    indent_spaces = " " * (name_width + 13)

    # Get the current terminal width (defaults to 80 if it can't detect it)
    terminal_width = shutil.get_terminal_size(fallback=(80, 24)).columns

    for name, scenario in SCENARIOS.items():
        marker = "fails" if scenario.expect_failure else "ok   "
        prefix = f"  {name:<{name_width}}  [{marker}]  "

        # Wrap the text dynamically based on terminal size
        formatted_text = textwrap.fill(
            scenario.description,
            width=terminal_width,
            initial_indent=prefix,  # The first line starts with our name/marker
            subsequent_indent=indent_spaces,  # Subsequent lines start with empty spaces
        )

        print(formatted_text)


def _print_engines() -> None:
    print("------  WorkflowEngines ------")
    print(f"WorkflowEngines: {', '.join([x.value for x in WorkflowEngine])}")
    print("None:      Runs with bare python, no engined backed")
    print("Temporal:  Run the workflow backed by temporal engine")
    print("Restate:   Run the workflow backed by Restate")


def _extract_saga_failure(
    err: BaseException,
) -> tuple[SagaFailure, str] | None:
    """Find a structured verdict through either engine's error wrappers."""
    cause: BaseException | None = err
    while cause is not None:
        if isinstance(cause, SagaError):
            return cause.failure, cause.type
        if isinstance(cause, ApplicationError) and cause.details:
            try:
                return SagaFailure.model_validate(cause.details[0]), cause.type
            except ValueError:
                pass
        try:
            from restate import HttpError
            from poc.restate.errors import decode_saga_failure

            if isinstance(cause, HttpError):
                decoded = decode_saga_failure(cause.body or str(cause))
                if decoded is not None:
                    return decoded
        except ImportError:
            pass
        cause = cause.__cause__ or getattr(cause, "cause", None)
    return None


def _print_failure(err: BaseException) -> None:
    """Render a stable saga verdict, falling back for unrelated exceptions."""
    extracted = _extract_saga_failure(err)
    if extracted is None:
        print(f"RESULT   : failed - {err}\n")
        return

    failure, error_type = extracted
    print(f"RESULT   : failed - {failure.outcome.value} ({error_type})")
    print(f"  step   : {failure.failed_step}")
    print(f"  reason : {failure.reason}")
    print(
        "  rollback: "
        f"applied={len(failure.compensated)} "
        f"no-op={len(failure.compensation_noops)} "
        f"failed={len(failure.compensation_errors)}\n"
    )


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
        case WorkflowEngine.RESTATE:
            from poc.restate.cli import _run_workflow
        case _:
            raise RuntimeError("Invalid Engine")

    steps, error = await _run_workflow(scenario)
    if error is None:
        assert steps is not None
        print(f"RESULT   : completed - {', '.join(steps)}\n")
    else:
        _print_failure(error)
    failed = error is not None

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
        "--engine",
        type=WorkflowEngine,
        default=DEFAULT_ENGINE,
        help=f"Run engine {[x.value for x in WorkflowEngine]}",
    )

    args = parser.parse_args()
    if args.command == "list":
        _print_scenarios()
        _print_engines()
        return 0
    return asyncio.run(_run(args.scenario, args.root, args.engine))


if __name__ == "__main__":
    raise SystemExit(main())
