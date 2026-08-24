import hashlib
import json
import shutil
import subprocess
from pathlib import Path

from poc.cli import WorkflowEngine
from poc.common.scenarios import SCENARIOS


def _get_audit_data(target: Path) -> tuple[str, str]:
    audit_file = target / "cluster_state" / "audit.jsonl"
    if not audit_file.exists():
        return "", "missing"

    normalized = []
    for line in audit_file.read_text().splitlines():
        if line.strip():
            normalized.append(json.dumps(json.loads(line), sort_keys=True))

    content = "\n".join(normalized)
    file_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()[:8]
    return content, file_hash


def _get_state_data(target: Path) -> tuple[str, str]:
    families_dir = target / "cluster_state" / "families"
    if not families_dir.exists():
        return "", "missing"

    state_dict = {}
    for f in sorted(families_dir.glob("*.json")):
        state_dict[f.name] = json.loads(f.read_text())

    content = json.dumps(state_dict, sort_keys=True, indent=2)
    file_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()[:8]
    return content, file_hash


def build_html_report(results_dir: Path) -> None:
    html = [
        "<!DOCTYPE html>",
        "<html><head>",
        "<meta charset='utf-8'>",
        "<style>",
        "body { font-family: system-ui, sans-serif; background: #fdfdfd; color: #111; margin: 20px; }",
        "table { border-collapse: collapse; width: 100%; margin-top: 20px; }",
        "th, td { border: 1px solid #aaa; padding: 12px; vertical-align: top; }",
        "th { background: #eee; text-align: left; }",
        ".match { border-left: 5px solid #333; padding: 8px; margin-bottom: 8px; background: #fafafa; }",
        ".mismatch { border-left: 5px dashed #333; padding: 8px; margin-bottom: 8px; background: #eaeaea; }",
        ".status-label { font-weight: bold; font-family: monospace; font-size: 1.1em; display: block; margin-bottom: 6px; }",
        "details { margin-top: 12px; background: #fff; padding: 8px; border: 1px solid #ccc; }",
        "summary { cursor: pointer; font-weight: bold; }",
        ".code-container { position: relative; margin-top: 8px; }",
        ".copy-btn { position: absolute; top: 8px; right: 8px; cursor: pointer; padding: 4px 8px; border: 1px solid #999; background: #e0e0e0; font-size: 11px; }",
        ".copy-btn:hover { background: #ccc; }",
        "pre { white-space: pre; overflow-x: auto; background: #f4f4f4; padding: 12px 12px 12px 12px; font-size: 13px; border: 1px solid #ddd; margin: 0; }",
        "</style></head><body>",
        "<h1>Scenario Comparison Matrix</h1>",
        "<table>",
    ]

    engines = [e.value for e in WorkflowEngine]
    html.append("<tr><th>Scenario</th>")
    for e in engines:
        html.append(f"<th>{e}</th>")
    html.append("</tr>")

    for scenario in SCENARIOS:
        html.append(f"<tr><td style='width: 15%;'><strong>{scenario}</strong></td>")

        scenario_data = {}
        for e in engines:
            target = results_dir / e.lower() / scenario
            audit_content, audit_hash = _get_audit_data(target)
            state_content, state_hash = _get_state_data(target)
            scenario_data[e] = {
                "audit_content": audit_content,
                "audit_hash": audit_hash,
                "state_content": state_content,
                "state_hash": state_hash,
            }

        audit_hashes = {
            d["audit_hash"]
            for d in scenario_data.values()
            if d["audit_hash"] != "missing"
        }
        state_hashes = {
            d["state_hash"]
            for d in scenario_data.values()
            if d["state_hash"] != "missing"
        }

        audit_match = len(audit_hashes) == 1
        state_match = len(state_hashes) == 1

        for e in engines:
            data = scenario_data[e]

            a_cls = "match" if audit_match else "mismatch"
            a_label = "[ MATCH ]" if audit_match else "[ MISMATCH ]"

            s_cls = "match" if state_match else "mismatch"
            s_label = "[ MATCH ]" if state_match else "[ MISMATCH ]"

            html.append("<td>")
            html.append(
                f"<div class='{a_cls}'><span class='status-label'>{a_label} Audit</span><strong>Hash:</strong> {data['audit_hash']}"
            )
            if data["audit_content"]:
                html.append(
                    "<details><summary>View JSON</summary><div class='code-container'>"
                )
                html.append(
                    "<button class='copy-btn' onclick='copyContent(this)'>Copy</button>"
                )
                html.append(f"<pre>{data['audit_content']}</pre></div></details>")
            html.append("</div>")

            html.append(
                f"<div class='{s_cls}'><span class='status-label'>{s_label} State</span><strong>Hash:</strong> {data['state_hash']}"
            )
            if data["state_content"]:
                html.append(
                    "<details><summary>View JSON</summary><div class='code-container'>"
                )
                html.append(
                    "<button class='copy-btn' onclick='copyContent(this)'>Copy</button>"
                )
                html.append(f"<pre>{data['state_content']}</pre></div></details>")
            html.append("</div>")
            html.append("</td>")
        html.append("</tr>")

    html.append("</table>")

    html.append("""
    <script>
    document.addEventListener('DOMContentLoaded', () => {
        const allDetails = document.querySelectorAll('details');
        allDetails.forEach(detail => {
            detail.addEventListener('toggle', (e) => {
                const tr = detail.closest('tr');
                if (!tr) return;
                const targetState = detail.open;
                const siblings = tr.querySelectorAll('details');
                siblings.forEach(sibling => {
                    if (sibling !== detail && sibling.open !== targetState) {
                        sibling.open = targetState;
                    }
                });
            });
        });
    });

    async function copyContent(button) {
        const pre = button.nextElementSibling;
        if (!pre) return;
        try {
            await navigator.clipboard.writeText(pre.textContent);
            const originalText = button.textContent;
            button.textContent = 'Copied!';
            setTimeout(() => {
                button.textContent = originalText;
            }, 2000);
        } catch (err) {
            console.error('Failed to copy text: ', err);
            button.textContent = 'Error';
        }
    }
    </script>
    """)
    html.append("</body></html>")

    report_path = results_dir / "comparison.html"
    report_path.write_text("\n".join(html), encoding="utf-8")
    print(f"Report generated at: {report_path}")


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

            (target / "execution.log").write_text(run.stdout, encoding="utf-8")
            if run.stderr:
                (target / "error.log").write_text(run.stderr, encoding="utf-8")

            if cluster_root.exists():
                shutil.copytree(
                    cluster_root, target / "cluster_state", dirs_exist_ok=True
                )

    print("Building HTML comparison matrix...")
    build_html_report(results_dir)


if __name__ == "__main__":
    generate_evidence_matrix()
