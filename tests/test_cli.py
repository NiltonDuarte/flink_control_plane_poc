"""CLI rendering for typed saga verdicts and unrelated failures."""

from pathlib import Path

import pytest
from temporalio.exceptions import ApplicationError

from poc.cli import WorkflowEngine, _print_failure, _run
from poc.common.domain import SagaFailure, SagaOutcome


def test_cli_prints_structured_saga_verdict(
    capsys: pytest.CaptureFixture[str],
) -> None:
    saga_error = ApplicationError(
        "COMPENSATED: stable history message",
        SagaFailure(
            outcome=SagaOutcome.COMPENSATED,
            failed_step="resume:family_b",
            reason="resume failed",
            compensated=["revert:family_a"],
            compensation_noops=["pause:family_b"],
            compensation_errors=[],
        ),
        type="SagaCompensatedError",
        non_retryable=True,
    )
    error = RuntimeError("workflow failed")
    error.__cause__ = saga_error

    _print_failure(error)

    output = capsys.readouterr().out
    assert "RESULT   : failed - COMPENSATED (SagaCompensatedError)" in output
    assert "step   : resume:family_b" in output
    assert "rollback: applied=1 no-op=1 failed=0" in output
    assert "stable history message" not in output


def test_cli_falls_back_to_raw_unrelated_exception(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _print_failure(RuntimeError("transport disappeared"))

    assert capsys.readouterr().out == ("RESULT   : failed - transport disappeared\n\n")


async def test_baseline_failure_reaches_shared_structured_formatter(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    result = await _run(
        "permanent-first-step", tmp_path / "cluster", WorkflowEngine.NONE
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "RESULT   : failed - COMPENSATED (SagaCompensatedError)" in output
    assert "step   : pause:family_a" in output
    assert "rollback: applied=0 no-op=1 failed=0" in output
