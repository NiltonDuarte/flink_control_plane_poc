"""MoveDatatypeWorkflow - the core saga, driven as a Temporal workflow.

The transaction itself is in `poc/core/saga.py`. What is left here is the two
things Temporal needs and the core cannot know:

1. **Execution.** `drive_saga` pulls operations out of the generator and runs
   them as activities.
2. **Failure translation.** In Temporal Python, an exception that is not a
   `FailureError` fails the *workflow task*, which is retried forever - so a raw
   `SagaAborted` escaping this method would hang the saga instead of failing it.
   Both core failures are therefore mapped onto a non-retryable
   `ApplicationError`, and `SagaAborted` carries its `SagaFailure` report through
   as the error's detail so a caller can read what was rolled back.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from poc.adapters.driver import drive_saga
    from poc.adapters.temporal.ports import SAGA_COMMANDS, SAGA_READS
    from poc.core.domain import MoveDatatypeRequest, SagaAborted, SagaRejected
    from poc.core.saga import move_datatype


@workflow.defn
class MoveDatatypeWorkflow:
    @workflow.run
    async def run(self, request: MoveDatatypeRequest) -> list[str]:
        """Move one datatype between two families. Returns the executed steps."""
        try:
            return await drive_saga(
                move_datatype(request),
                reads=SAGA_READS,
                commands=SAGA_COMMANDS,
            )
        except SagaRejected as err:
            # Nothing was mutated, so there is nothing to report but the reason.
            raise ApplicationError(str(err), non_retryable=True) from err
        except SagaAborted as err:
            raise ApplicationError(
                str(err), err.failure, non_retryable=True
            ) from err.__cause__
