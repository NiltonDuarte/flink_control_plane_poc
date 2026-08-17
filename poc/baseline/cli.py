from poc.baseline.saga import MoveDatatypeWorkflow
from poc.common.scenarios import REQUEST


async def _run_workflow(scenario):
    failed = False
    try:
        steps = MoveDatatypeWorkflow().run(REQUEST)
        print(f"RESULT   : completed - {', '.join(steps)}\n")
    except Exception as err:  # noqa: BLE001 - expected for failure scenarios
        failed = True
        print(f"RESULT   : failed - {err}\n")
    return failed
