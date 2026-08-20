from poc.baseline.saga import MoveDatatypeWorkflow
from poc.common.scenarios import REQUEST, Scenario


async def _run_workflow(
    scenario: Scenario,
) -> tuple[list[str] | None, Exception | None]:
    try:
        return MoveDatatypeWorkflow().run(REQUEST), None
    except Exception as err:  # noqa: BLE001 - expected for failure scenarios
        return None, err
