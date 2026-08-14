"""The saga, with no engine underneath it at all.

`tests/test_saga.py` proves Temporal executes the saga correctly. This file
proves the saga is correct, which is a different claim and a much cheaper one to
check: no server, no worker, no time-skipping environment, no cluster on disk -
just the generator, driven by a dict.

That is the dividend of writing the core as a generator. It also reaches two
things the engine tests cannot see:

* the **payload** of a compensating patch. `ops()` flattens the audit log to
  ``"op(family)"``, so a revert that restored the *wrong* config would still
  read as ``patch_configmap(family_a)`` and pass. Here the exact datatypes are
  asserted.
* the ``changed`` branch - a pause of an already-suspended family owes no
  compensating resume. No scenario in the matrix starts a family suspended, so
  on an engine that branch is only exercised by accident.
"""

from __future__ import annotations

from typing import Any

from poc.core.domain import (
    CommandResult,
    FamilyCommand,
    FamilyState,
    FamilyStatus,
    MoveDatatypeRequest,
    SagaAborted,
    SagaRejected,
)
from poc.core.ports import Command, ReadStatus, SagaOp
from poc.core.saga import move_datatype

SOURCE, TARGET = "family_a", "family_b"
REQUEST = MoveDatatypeRequest(
    datatype="clicks", source_family=SOURCE, target_family=TARGET
)


class FakeEngine:
    """Everything an adapter does, in thirty lines and no I/O.

    Holds just enough family state to answer `changed` and `previous_datatypes`
    honestly, because a saga fed dishonest results proves nothing about
    compensation.
    """

    def __init__(
        self,
        datatypes: dict[str, list[str]],
        *,
        suspended: frozenset[str] | set[str] = frozenset(),
        fail_steps: dict[str, str] | None = None,
    ) -> None:
        self.datatypes = {family: list(values) for family, values in datatypes.items()}
        self.state = {
            family: FamilyState.SUSPENDED if family in suspended else FamilyState.RUNNING
            for family in datatypes
        }
        self.fail_steps = fail_steps or {}
        self.ops: list[SagaOp] = []

    def run(self, request: MoveDatatypeRequest) -> list[str]:
        """Drive the saga to completion. Core exceptions propagate to the caller."""
        generator = move_datatype(request)
        to_send: Any = None
        to_throw: BaseException | None = None
        while True:
            try:
                if to_throw is not None:
                    op = generator.throw(to_throw)
                else:
                    op = generator.send(to_send)
            except StopIteration as finished:
                return finished.value  # type: ignore[no-any-return]
            self.ops.append(op)
            to_send, to_throw = None, None
            try:
                to_send = self._execute(op)
            except Exception as err:  # noqa: BLE001 - handed back to the saga
                to_throw = err

    @property
    def steps(self) -> list[str]:
        """Every command the saga issued, by step name, in order."""
        return [op.step for op in self.ops if isinstance(op, Command)]

    def _execute(self, op: SagaOp) -> Any:
        if isinstance(op, ReadStatus):
            return FamilyStatus(
                family=op.family,
                state=self.state[op.family],
                datatypes=list(self.datatypes[op.family]),
            )

        if op.step in self.fail_steps:
            raise RuntimeError(self.fail_steps[op.step])

        if op.command is FamilyCommand.PAUSE:
            changed = self.state[op.family] is FamilyState.RUNNING
            self.state[op.family] = FamilyState.SUSPENDED
            return CommandResult(changed=changed)
        if op.command is FamilyCommand.RESUME:
            changed = self.state[op.family] is FamilyState.SUSPENDED
            self.state[op.family] = FamilyState.RUNNING
            return CommandResult(changed=changed)

        previous = list(self.datatypes[op.family])
        self.datatypes[op.family] = list(op.datatypes or [])
        return CommandResult(changed=True, previous_datatypes=previous)


def _engine(**kwargs: Any) -> FakeEngine:
    return FakeEngine({SOURCE: ["clicks", "impressions"], TARGET: ["views"]}, **kwargs)


def test_happy_path_issues_three_phases_in_order() -> None:
    engine = _engine()
    done = engine.run(REQUEST)

    assert done == [
        f"pause:{SOURCE}",
        f"pause:{TARGET}",
        f"patch:{SOURCE}",
        f"patch:{TARGET}",
        f"resume:{SOURCE}",
        f"resume:{TARGET}",
    ]
    # What it reports having done is what it actually asked for, in that order.
    assert engine.steps == done
    # Both sides of the move landed: the datatype left one family and joined the
    # other, which is the reason this needs to be transactional at all.
    assert engine.datatypes == {SOURCE: ["impressions"], TARGET: ["views", "clicks"]}


def test_failure_in_resume_unwinds_the_stack_lifo() -> None:
    """RFC scenario 4, asserted on the commands rather than on their side effects."""
    engine = _engine(fail_steps={f"resume:{TARGET}": "resume is broken"})

    try:
        engine.run(REQUEST)
    except SagaAborted as err:
        aborted = err
    else:
        raise AssertionError("expected the saga to abort")

    assert engine.steps == [
        f"pause:{SOURCE}",
        f"pause:{TARGET}",
        f"patch:{SOURCE}",
        f"patch:{TARGET}",
        f"resume:{SOURCE}",
        f"resume:{TARGET}",  # fails here
        f"comp:0:pause:{SOURCE}",  # re-pause what was resumed
        f"comp:1:revert:{TARGET}",  # revert both configs
        f"comp:2:revert:{SOURCE}",
        f"comp:3:resume:{TARGET}",  # restore both to running
        f"comp:4:resume:{SOURCE}",
    ]
    assert aborted.failure.failed_step == f"resume:{TARGET}"
    assert aborted.failure.compensation_errors == []
    # The thing the audit log cannot show: each revert carried the config that
    # family had *before* the saga touched it.
    assert engine.datatypes == {SOURCE: ["clicks", "impressions"], TARGET: ["views"]}


def test_a_pause_that_changed_nothing_owes_no_resume() -> None:
    """The `changed` branch: compensations are for side effects, not for calls.

    Recording one for a no-op is how rollbacks end up resuming families that were
    never running to begin with.
    """
    engine = _engine(
        suspended={SOURCE}, fail_steps={f"patch:{TARGET}": "patch is broken"}
    )

    try:
        engine.run(REQUEST)
    except SagaAborted:
        pass
    else:
        raise AssertionError("expected the saga to abort")

    # TARGET was running and was suspended, so its resume is owed. SOURCE was
    # already suspended, so it is not - and it is absent below.
    assert engine.steps == [
        f"pause:{SOURCE}",
        f"pause:{TARGET}",
        f"patch:{SOURCE}",
        f"patch:{TARGET}",  # fails here
        f"comp:0:revert:{SOURCE}",
        f"comp:1:resume:{TARGET}",
    ]


def test_a_failing_compensation_does_not_stop_the_unwind() -> None:
    """Stopping halfway would strand the cluster in a worse state than finishing."""
    engine = _engine(
        fail_steps={
            f"resume:{TARGET}": "resume is broken",
            f"comp:1:revert:{TARGET}": "and so is its revert",
        }
    )

    try:
        engine.run(REQUEST)
    except SagaAborted as err:
        aborted = err
    else:
        raise AssertionError("expected the saga to abort")

    # Every remaining compensation still ran.
    assert engine.steps[-3:] == [
        f"comp:2:revert:{SOURCE}",
        f"comp:3:resume:{TARGET}",
        f"comp:4:resume:{SOURCE}",
    ]
    assert aborted.failure.compensation_errors == [
        f"revert:{TARGET}: and so is its revert"
    ]
    assert f"revert:{TARGET}" not in aborted.failure.compensated


def test_an_impossible_move_is_rejected_before_anything_is_touched() -> None:
    """The cheapest place to fail: nothing happened, so nothing is owed."""
    engine = _engine()

    try:
        engine.run(
            MoveDatatypeRequest(
                datatype="nonexistent", source_family=SOURCE, target_family=TARGET
            )
        )
    except SagaRejected as err:
        assert "not owned by" in str(err)
    else:
        raise AssertionError("expected the saga to be rejected")

    # The two pre-flight reads, and not one command.
    assert engine.steps == []
    assert all(isinstance(op, ReadStatus) for op in engine.ops)
