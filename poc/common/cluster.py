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

`chaos.json` is kept declarative and is never rewritten by this module, so a test
can write it once and assert against it afterwards; the mutable attempt counters
live in `chaos_hits.json` instead.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel

from poc.common.domain import (
    FamilyState,
    FamilyStatus,
    PermanentClusterError,
    TransientClusterError,
)

ENV_CLUSTER_ROOT = "POC_CLUSTER_ROOT"

Outcome = Literal["ok", "transient", "permanent"]


class AuditEntry(BaseModel):
    """One attempted cluster operation.

    Failed attempts are recorded too, which is what makes retry behaviour
    visible: a transient fault that retries twice leaves three lines, not one.
    """

    seq: int
    op: str
    family: str
    outcome: Outcome
    detail: dict[str, Any] = {}

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


class MockCluster:
    """File-backed stand-in for the Flink/Kubernetes control surface."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.families_dir = root / "families"
        self.savepoints_dir = root / "savepoints"
        self.audit_path = root / "audit.jsonl"
        self.chaos_path = root / "chaos.json"
        self.hits_path = root / "chaos_hits.json"

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
        self.families_dir.mkdir(parents=True, exist_ok=True)
        self.savepoints_dir.mkdir(parents=True, exist_ok=True)
        self.audit_path.write_text("")
        self.hits_path.write_text("{}")
        if not self.chaos_path.exists():
            self.chaos_path.write_text("{}")
        for name, datatypes in families.items():
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
        path = self._family_path(family)
        if not path.exists():
            raise PermanentClusterError(f"unknown job family: {family}")
        return FamilyStatus.model_validate_json(path.read_text())

    def audit(self, *, successful_only: bool = False) -> list[AuditEntry]:
        """The full attempt log, in order."""
        if not self.audit_path.exists():
            return []
        entries = [
            AuditEntry.model_validate_json(line)
            for line in self.audit_path.read_text().splitlines()
            if line.strip()
        ]
        if successful_only:
            entries = [e for e in entries if e.outcome == "ok"]
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

    def trigger_savepoint(self, family: str) -> str:
        """Patch the FKO resource to take a savepoint; return its URI."""
        self._maybe_fail(family, "trigger_savepoint")
        status = self.read(family)
        uri = f"s3://savepoints/{family}/sp-{status.generation + 1:04d}"
        (self.savepoints_dir / f"{family}-{status.generation + 1:04d}.json").write_text(
            json.dumps({"family": family, "uri": uri}, indent=2)
        )
        status.savepoint_uri = uri
        self._commit(status, "trigger_savepoint", {"uri": uri})
        self._maybe_lose_response(family, "trigger_savepoint")
        return uri

    def suspend_job(self, family: str) -> None:
        """Patch the FKO resource state to SUSPENDED."""
        self._maybe_fail(family, "suspend_job")
        status = self.read(family)
        status.state = FamilyState.SUSPENDED
        self._commit(status, "suspend_job", {})
        self._maybe_lose_response(family, "suspend_job")

    def resume_job(self, family: str) -> None:
        """Patch the FKO resource state to RUNNING."""
        self._maybe_fail(family, "resume_job")
        status = self.read(family)
        status.state = FamilyState.RUNNING
        self._commit(status, "resume_job", {})
        self._maybe_lose_response(family, "resume_job")

    def patch_configmap(self, family: str, datatypes: list[str]) -> list[str]:
        """Replace routing config; return the previous value for observability."""
        self._maybe_fail(family, "patch_configmap")
        status = self.read(family)
        previous = list(status.datatypes)
        status.datatypes = list(datatypes)
        self._commit(status, "patch_configmap", {"from": previous, "to": list(datatypes)})
        self._maybe_lose_response(family, "patch_configmap")
        return previous

    # -- internals ---------------------------------------------------------

    def _family_path(self, family: str) -> Path:
        return self.families_dir / f"{family}.json"

    def _write(self, status: FamilyStatus) -> None:
        self._family_path(status.family).write_text(status.model_dump_json(indent=2))

    def _commit(self, status: FamilyStatus, op: str, detail: dict[str, Any]) -> None:
        status.generation += 1
        self._write(status)
        self._record(op, status.family, "ok", detail)

    def _record(self, op: str, family: str, outcome: Outcome, detail: dict[str, Any]) -> None:
        entry = AuditEntry(
            seq=self._next_seq(), op=op, family=family, outcome=outcome, detail=detail
        )
        with self.audit_path.open("a") as handle:
            handle.write(entry.model_dump_json() + "\n")

    def _next_seq(self) -> int:
        if not self.audit_path.exists():
            return 0
        return sum(1 for line in self.audit_path.read_text().splitlines() if line.strip())

    def _maybe_fail(self, family: str, op: str) -> None:
        """Raise faults configured to happen before a mutation is committed."""
        rules = self._chaos_rules()
        rule = rules.get(f"{family}:{op}")
        if rule is None or rule.mode == "lost_response":
            return

        if rule.mode == "permanent":
            self._record(op, family, "permanent", {})
            raise PermanentClusterError(f"{op} on {family} is permanently broken")

        hits = self._hits()
        key = f"{family}:{op}"
        seen = hits.get(key, 0)
        if seen < rule.times:
            hits[key] = seen + 1
            self.hits_path.write_text(json.dumps(hits, indent=2))
            self._record(op, family, "transient", {"attempt": seen + 1})
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
