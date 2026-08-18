"""CLI rendering for typed saga verdicts and unrelated failures."""

from pathlib import Path

from temporalio.exceptions import ApplicationError
from restate import HttpError

from poc.cli import WorkflowEngine, _print_failure, _run
from poc.common.domain import SagaFailure, SagaOutcome
from poc.restate.errors import encode_saga_failure


def test_cli_prints_structured_saga_verdict(capsys) -> None:
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


def test_cli_falls_back_to_raw_unrelated_exception(capsys) -> None:
    _print_failure(RuntimeError("transport disappeared"))

    assert capsys.readouterr().out == ("RESULT   : failed - transport disappeared\n\n")


def test_cli_decodes_restate_terminal_error_envelope(capsys) -> None:
    failure = SagaFailure(
        outcome=SagaOutcome.COMPENSATION_INCOMPLETE,
        failed_step="resume:family_b",
        reason="resume failed",
        compensated=["revert:family_a"],
        compensation_noops=["pause:family_b"],
        compensation_errors=["resume:family_b: still unavailable"],
    )
    error = HttpError(
        500,
        "Internal Server Error",
        body=f'{{"message":"{encode_saga_failure(failure)}"}}',
    )

    _print_failure(error)

    output = capsys.readouterr().out
    assert (
        "RESULT   : failed - COMPENSATION_INCOMPLETE "
        "(SagaCompensationIncompleteError)" in output
    )
    assert "step   : resume:family_b" in output
    assert "rollback: applied=1 no-op=1 failed=1" in output


async def test_baseline_failure_reaches_shared_structured_formatter(
    tmp_path: Path, capsys
) -> None:
    result = await _run(
        "permanent-first-step", tmp_path / "cluster", WorkflowEngine.NONE
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "RESULT   : failed - COMPENSATED (SagaCompensatedError)" in output
    assert "step   : pause:family_a" in output
    assert "rollback: applied=0 no-op=1 failed=0" in output
