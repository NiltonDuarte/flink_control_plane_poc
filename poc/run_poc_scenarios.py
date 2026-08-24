import shutil
import subprocess
from pathlib import Path

from poc.cli import WorkflowEngine
from poc.common.scenarios import SCENARIOS


def generate_evidence_matrix() -> None:
    results_dir = Path("poc", "docs", "assessment_evidence")
    if results_dir.exists():
        shutil.rmtree(results_dir)

    cluster_root = Path(".cluster")
    total_scenarios = len(SCENARIOS)

    print("Starting Scenarios..")
    for engine in WorkflowEngine:
        for index, scenario in enumerate(SCENARIOS.keys(), start=1):
            print(f"Running {engine} scenario {scenario} [{index}/{total_scenarios}]")
            target = results_dir / engine.value.lower() / scenario
            target.mkdir(parents=True, exist_ok=True)

            cmd = [
                "uv",
                "run",
                "python",
                "-m",
                "poc.cli",
                "run",
                scenario,
                "--engine",
                engine.value,
                "--root",
                str(cluster_root),
            ]

            run = subprocess.run(cmd, capture_output=True, text=True, check=False)

            (target / "execution.log").write_text(run.stdout)
            if run.stderr:
                (target / "error.log").write_text(run.stderr)

            if cluster_root.exists():
                shutil.copytree(
                    cluster_root, target / "cluster_state", dirs_exist_ok=True
                )


if __name__ == "__main__":
    generate_evidence_matrix()
