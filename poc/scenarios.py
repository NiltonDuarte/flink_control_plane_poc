"""The named execution scenarios - the ticket's "explicit toggles/hooks".

Shared by the CLI and the test suite so that `make scenario-fail-in-resume` and
the corresponding test exercise byte-identically the same fault configuration.
Each scenario is just a seed cluster plus a set of chaos rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from poc.cluster import ChaosRule
from poc.domain import MoveDatatypeRequest

SOURCE = "family_a"
TARGET = "family_b"

# family_a owns two datatypes, family_b one. "clicks" is the one that moves.
SEED: dict[str, list[str]] = {
    SOURCE: ["clicks", "impressions"],
    TARGET: ["views"],
}

REQUEST = MoveDatatypeRequest(datatype="clicks", source_family=SOURCE, target_family=TARGET)


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    chaos: dict[str, ChaosRule] = field(default_factory=dict)
    expect_failure: bool = False


SCENARIOS: dict[str, Scenario] = {
    "happy": Scenario(
        name="happy",
        description="Success path: pause both, patch both configs, resume both.",
    ),
    "fail-in-pause": Scenario(
        name="fail-in-pause",
        description=(
            "RFC scenario 2. family_a pauses, family_b's suspend fails permanently. "
            "Compensation resumes family_a only; no config was ever touched."
        ),
        chaos={f"{TARGET}:suspend_job": ChaosRule(mode="permanent")},
        expect_failure=True,
    ),
    "fail-in-update": Scenario(
        name="fail-in-update",
        description=(
            "RFC scenario 3. Both pause, family_a's config is patched, family_b's "
            "fails. Compensation reverts family_a's config and resumes both."
        ),
        chaos={f"{TARGET}:patch_configmap": ChaosRule(mode="permanent")},
        expect_failure=True,
    ),
    "fail-in-resume": Scenario(
        name="fail-in-resume",
        description=(
            "RFC scenario 4, the layered rollback. Both pause, both configs patch, "
            "family_a resumes, family_b's resume exhausts its retries. Compensation "
            "pauses family_a again, reverts both configs, then resumes both."
        ),
        # times == CLUSTER_RETRY.maximum_attempts: the forward resume burns the
        # whole retry budget and fails, but the compensating resume that follows
        # is attempt 5 and succeeds. That separation is deliberate - it isolates
        # "the saga rolled back correctly" from "the rollback action itself is
        # broken", which is the compensation-unreachable scenario below.
        chaos={f"{TARGET}:resume_job": ChaosRule(mode="transient", times=4)},
        expect_failure=True,
    ),
    "compensation-unreachable": Scenario(
        name="compensation-unreachable",
        description=(
            "The uncomfortable one: resume is permanently broken on family_b, so the "
            "action compensation itself depends on cannot succeed. The saga aborts, "
            "the rollback is only partial, and family_b is left SUSPENDED. Fail-fast "
            "sagas cannot guarantee all-or-nothing when the inverse action is the "
            "thing that is broken - the failure is reported, not resolved."
        ),
        chaos={f"{TARGET}:resume_job": ChaosRule(mode="permanent")},
        expect_failure=True,
    ),
    "transient-retry": Scenario(
        name="transient-retry",
        description=(
            "family_b's suspend fails twice then succeeds. The saga completes - the "
            "retries are absorbed inside the actor and never reach the saga."
        ),
        chaos={f"{TARGET}:suspend_job": ChaosRule(mode="transient", times=2)},
    ),
    "transient-exhausted": Scenario(
        name="transient-exhausted",
        description=(
            "family_b's suspend fails more times than the retry budget allows. "
            "Retries are exhausted, and the saga compensates as for a hard failure."
        ),
        chaos={f"{TARGET}:suspend_job": ChaosRule(mode="transient", times=99)},
        expect_failure=True,
    ),
    "permanent-first-step": Scenario(
        name="permanent-first-step",
        description=(
            "family_a's very first savepoint fails permanently. Nothing succeeded, "
            "so there is nothing to compensate - the cheapest possible abort."
        ),
        chaos={f"{SOURCE}:trigger_savepoint": ChaosRule(mode="permanent")},
        expect_failure=True,
    ),
}
