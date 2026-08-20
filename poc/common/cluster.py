"""The mock cluster: a directory that stands in for Flink + Kubernetes.

This is the module you delete when going to production. Everything else in the
POC talks to it through the five operations below, so replacing it with real
Flink Kubernetes Operator calls means reimplementing this file and nothing else.
It therefore imports no Temporal and no POC workflow code.

Transitions are instant by design (see POC_SCOPE.md): an operation reads JSON,
mutates it, writes it back, and returns. There is no desired/observed split and
no polling.

On-disk layout::

    <root>/
      families/<name>.json    one file per job family
      savepoints/<uri>.json   one file per triggered savepoint
      audit.jsonl             append-only, one line per attempted operation
      chaos.json              declarative fault-injection rules (written by tests)
      chaos_hits.json         per-rule attempt counters (maintained here)
      completed_operations.json  stable operation identities and results

`chaos.json` is kept declarative and is never rewritten by this module, so a test
can write it once and assert against it afterwards; the mutable attempt counters
live in `chaos_hits.json` instead.
"""

from __future__ import annotations

import json
import os
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from poc.common.domain import (
    FamilyState,
    FamilyStatus,
    PermanentClusterError,
    TransientClusterError,
    validate_family_identifier,
)

ENV_CLUSTER_ROOT = "POC_CLUSTER_ROOT"

Outcome = Literal["ok", "transient", "permanent"]


class AuditEntry(BaseModel):
    """One attempted cluster operation.

    Failed and deduplicated attempts are recorded too. ``changed`` separates
    effective external mutations from successful retry no-ops.
    """

    seq: int
    op: str
    family: str
    operation_id: str
    outcome: Outcome
    changed: bool
    detail: dict[str, Any] = Field(default_factory=dict)

    def __str__(self) -> str:
        suffix = "" if self.outcome == "ok" else f" [{self.outcome}]"
        return f"{self.op}({self.family}){suffix}"


class ChaosRule(BaseModel):
    """A fault-injection rule, keyed by ``"<family>:<op>"`` in chaos.json.

    ``transient`` fails before mutation for the first ``times`` attempts and
    then succeeds. ``lost_response`` commits the mutation but raises a transient
    error before returning for the first ``times`` attempts. ``permanent``
    always fails before mutation.
    """

    mode: Literal["transient", "permanent", "lost_response"]
    times: int = 1


class _CompletedOperation(BaseModel):
    """Persisted result used to deduplicate an ambiguous retried mutation."""

    op: str
    family: str
    result: dict[str, Any] = Field(default_factory=dict)


class MockCluster:
    """File-backed stand-in for the Flink/Kubernetes control surface."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.families_dir = root / "families"
        self.savepoints_dir = root / "savepoints"
        self.audit_path = root / "audit.jsonl"
        self.chaos_path = root / "chaos.json"
        self.hits_path = root / "chaos_hits.json"
        self.operations_path = root / "completed_operations.json"

    # -- lifecycle ---------------------------------------------------------

    @classmethod
    def from_env(cls) -> MockCluster:
        """Build from ``POC_CLUSTER_ROOT``.

        Activities resolve the cluster this way so each test can point at its own
        tmpdir without any global state.
        """
        root = os.environ.get(ENV_CLUSTER_ROOT)
        if not root:
            raise RuntimeError(f"{ENV_CLUSTER_ROOT} is not set")
        return cls(Path(root))

    def seed(self, families: dict[str, list[str]]) -> None:
        """Create a fresh cluster: family -> the datatypes it owns."""
        validated_families = {
            validate_family_identifier(name): datatypes
            for name, datatypes in families.items()
        }
        self.families_dir.mkdir(parents=True, exist_ok=True)
        self.savepoints_dir.mkdir(parents=True, exist_ok=True)
        self.audit_path.write_text("")
        self.hits_path.write_text("{}")
        self.operations_path.write_text("{}")
        if not self.chaos_path.exists():
            self.chaos_path.write_text("{}")
        for name, datatypes in validated_families.items():
            self._write(
                FamilyStatus(
                    family=name,
                    state=FamilyState.RUNNING,
                    datatypes=list(datatypes),
                )
            )

    def set_chaos(self, rules: dict[str, ChaosRule]) -> None:
        """Install fault-injection rules and reset the attempt counters."""
        self.chaos_path.write_text(
            json.dumps({k: v.model_dump() for k, v in rules.items()}, indent=2)
        )
        self.hits_path.write_text("{}")

    # -- reads -------------------------------------------------------------

    def read(self, family: str) -> FamilyStatus:
        family = validate_family_identifier(family)
        path = self._family_path(family)
        if not path.exists():
            raise PermanentClusterError(f"unknown job family: {family}")
        return FamilyStatus.model_validate_json(path.read_text())

    def audit(
        self, *, successful_only: bool = False, effective_only: bool = False
    ) -> list[AuditEntry]:
        """Return attempted calls, optionally limited to successes or mutations."""
        if not self.audit_path.exists():
            return []
        entries = [
            AuditEntry.model_validate_json(line)
            for line in self.audit_path.read_text().splitlines()
            if line.strip()
        ]
        if successful_only:
            entries = [e for e in entries if e.outcome == "ok"]
        if effective_only:
            entries = [e for e in entries if e.changed]
        return entries

    def snapshot(self) -> dict[str, tuple[FamilyState, tuple[str, ...]]]:
        """The observable state of every family: runtime state plus routing.

        Compared before and after a failed saga to prove the rollback was
        complete. Deliberately *excludes* `generation` and `savepoint_uri`,
        because a correct rollback does not - and should not - restore those:

        * `generation` counts mutations and only ever moves forward, so a
          successful compensation necessarily leaves it higher than it started;
        * a savepoint taken on the way in is a real artifact on real storage.
          You do not un-take a savepoint, and pretending otherwise would hide
          the fact that rollback leaves cleanup work behind.

        What all-or-nothing actually promises is that state and routing end up
        where they began, which is exactly what this compares.
        """
        return {
            path.stem: (status.state, tuple(status.datatypes))
            for path in sorted(self.families_dir.glob("*.json"))
            for status in [FamilyStatus.model_validate_json(path.read_text())]
        }

    # -- operations (the port that real Flink would implement) -------------

    def trigger_savepoint(self, family: str, operation_id: str) -> str:
        """Patch the FKO resource to take a savepoint; return its URI."""
        family = validate_family_identifier(family)
        op = "trigger_savepoint"
        completed = self._completed(operation_id, family, op)
        if completed is not None:
            uri = str(completed.result["uri"])
            self._record(
                op,
                family,
                operation_id,
                "ok",
                changed=False,
                detail={"uri": uri, "deduplicated": True},
            )
            self._maybe_lose_response(family, op)
            return uri

        self._maybe_fail(family, op, operation_id)
        status = self.read(family)
        identity = sha256(operation_id.encode()).hexdigest()[:16]
        uri = f"s3://savepoints/{family}/sp-{identity}"
        (self.savepoints_dir / f"{family}-{identity}.json").write_text(
            json.dumps({"family": family, "uri": uri}, indent=2)
        )
        status.savepoint_uri = uri
        self._commit(
            status,
            op,
            operation_id,
            {"uri": uri},
            result={"uri": uri},
        )
        self._maybe_lose_response(family, op)
        return uri

    def suspend_job(self, family: str, operation_id: str) -> bool:
        """Patch the FKO resource state to SUSPENDED."""
        family = validate_family_identifier(family)
        op = "suspend_job"
        if self._deduplicated(operation_id, family, op):
            self._maybe_lose_response(family, op)
            return False
        self._maybe_fail(family, op, operation_id)
        status = self.read(family)
        if status.state == FamilyState.SUSPENDED:
            self._complete_noop(operation_id, status, op)
            self._maybe_lose_response(family, op)
            return False
        status.state = FamilyState.SUSPENDED
        self._commit(status, op, operation_id, {})
        self._maybe_lose_response(family, op)
        return True

    def resume_job(self, family: str, operation_id: str) -> bool:
        """Patch the FKO resource state to RUNNING."""
        family = validate_family_identifier(family)
        op = "resume_job"
        if self._deduplicated(operation_id, family, op):
            self._maybe_lose_response(family, op)
            return False
        self._maybe_fail(family, op, operation_id)
        status = self.read(family)
        if status.state == FamilyState.RUNNING:
            self._complete_noop(operation_id, status, op)
            self._maybe_lose_response(family, op)
            return False
        status.state = FamilyState.RUNNING
        self._commit(status, op, operation_id, {})
        self._maybe_lose_response(family, op)
        return True

    def patch_configmap(
        self, family: str, datatypes: list[str], operation_id: str
    ) -> bool:
        """Replace routing config, deduplicating retries and desired-state no-ops."""
        family = validate_family_identifier(family)
        op = "patch_configmap"
        if self._deduplicated(operation_id, family, op):
            self._maybe_lose_response(family, op)
            return False
        self._maybe_fail(family, op, operation_id)
        status = self.read(family)
        if status.datatypes == datatypes:
            self._complete_noop(operation_id, status, op)
            self._maybe_lose_response(family, op)
            return False
        previous = list(status.datatypes)
        status.datatypes = list(datatypes)
        self._commit(
            status,
            op,
            operation_id,
            {"from": previous, "to": list(datatypes)},
        )
        self._maybe_lose_response(family, op)
        return True

    # -- internals ---------------------------------------------------------

    def _family_path(self, family: str) -> Path:
        family = validate_family_identifier(family)
        return self.families_dir / f"{family}.json"

    def _write(self, status: FamilyStatus) -> None:
        self._family_path(status.family).write_text(status.model_dump_json(indent=2))

    def _commit(
        self,
        status: FamilyStatus,
        op: str,
        operation_id: str,
        detail: dict[str, Any],
        *,
        result: dict[str, Any] | None = None,
    ) -> None:
        status.generation += 1
        self._write(status)
        self._store_completed(operation_id, op, status.family, result or {})
        self._record(op, status.family, operation_id, "ok", changed=True, detail=detail)

    def _complete_noop(self, operation_id: str, status: FamilyStatus, op: str) -> None:
        self._store_completed(operation_id, op, status.family, {})
        self._record(
            op,
            status.family,
            operation_id,
            "ok",
            changed=False,
            detail={"desired_state_already_present": True},
        )

    def _record(
        self,
        op: str,
        family: str,
        operation_id: str,
        outcome: Outcome,
        *,
        changed: bool,
        detail: dict[str, Any],
    ) -> None:
        entry = AuditEntry(
            seq=self._next_seq(),
            op=op,
            family=family,
            operation_id=operation_id,
            outcome=outcome,
            changed=changed,
            detail=detail,
        )
        with self.audit_path.open("a") as handle:
            handle.write(entry.model_dump_json() + "\n")

    def _next_seq(self) -> int:
        if not self.audit_path.exists():
            return 0
        return sum(
            1 for line in self.audit_path.read_text().splitlines() if line.strip()
        )

    def _maybe_fail(self, family: str, op: str, operation_id: str) -> None:
        """Raise faults configured to happen before a mutation is committed."""
        rules = self._chaos_rules()
        rule = rules.get(f"{family}:{op}")
        if rule is None or rule.mode == "lost_response":
            return

        if rule.mode == "permanent":
            self._record(
                op, family, operation_id, "permanent", changed=False, detail={}
            )
            raise PermanentClusterError(f"{op} on {family} is permanently broken")

        hits = self._hits()
        key = f"{family}:{op}"
        seen = hits.get(key, 0)
        if seen < rule.times:
            hits[key] = seen + 1
            self.hits_path.write_text(json.dumps(hits, indent=2))
            self._record(
                op,
                family,
                operation_id,
                "transient",
                changed=False,
                detail={"attempt": seen + 1},
            )
            raise TransientClusterError(
                f"{op} on {family} failed transiently (attempt {seen + 1}/{rule.times})"
            )

    def _maybe_lose_response(self, family: str, op: str) -> None:
        """Raise after a committed mutation to model an ambiguous response loss."""
        rule = self._chaos_rules().get(f"{family}:{op}")
        if rule is None or rule.mode != "lost_response":
            return

        hits = self._hits()
        key = f"{family}:{op}"
        seen = hits.get(key, 0)
        if seen < rule.times:
            hits[key] = seen + 1
            self.hits_path.write_text(json.dumps(hits, indent=2))
            raise TransientClusterError(
                f"response from {op} on {family} was lost "
                f"after commit (attempt {seen + 1}/{rule.times})"
            )

    def _chaos_rules(self) -> dict[str, ChaosRule]:
        if not self.chaos_path.exists():
            return {}
        raw = json.loads(self.chaos_path.read_text() or "{}")
        return {key: ChaosRule.model_validate(value) for key, value in raw.items()}

    def _hits(self) -> dict[str, int]:
        if not self.hits_path.exists():
            return {}
        return json.loads(self.hits_path.read_text() or "{}")

    def _deduplicated(self, operation_id: str, family: str, op: str) -> bool:
        completed = self._completed(operation_id, family, op)
        if completed is None:
            return False
        self._record(
            op,
            family,
            operation_id,
            "ok",
            changed=False,
            detail={"deduplicated": True},
        )
        return True

    def _completed(
        self, operation_id: str, family: str, op: str
    ) -> _CompletedOperation | None:
        raw = self._completed_operations().get(operation_id)
        if raw is None:
            return None
        completed = _CompletedOperation.model_validate(raw)
        if completed.family != family or completed.op != op:
            self._record(
                op,
                family,
                operation_id,
                "permanent",
                changed=False,
                detail={
                    "operation_identity_conflict": {
                        "family": completed.family,
                        "op": completed.op,
                    }
                },
            )
            raise PermanentClusterError(
                f"operation identity {operation_id!r} was already used for "
                f"{completed.op} on {completed.family}"
            )
        return completed

    def _store_completed(
        self,
        operation_id: str,
        op: str,
        family: str,
        result: dict[str, Any],
    ) -> None:
        completed = self._completed_operations()
        completed[operation_id] = _CompletedOperation(
            op=op, family=family, result=result
        ).model_dump()
        self.operations_path.write_text(json.dumps(completed, indent=2))

    def _completed_operations(self) -> dict[str, dict[str, Any]]:
        if not self.operations_path.exists():
            return {}
        return json.loads(self.operations_path.read_text() or "{}")
