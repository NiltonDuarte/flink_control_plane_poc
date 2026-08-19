import os
import threading
from dbos import DBOS, DBOSConfig, WorkflowHandle
from fastapi import FastAPI
import uvicorn
from poc.common.domain import MoveDatatypeRequest
from poc.dbos.saga import run_move_datatype_request

app = FastAPI()

# Mounts DBOS onto the FastAPI app lifecycle
DBOS.fastapi(app)

@app.post("/move-datatype")
def trigger_workflow(request: MoveDatatypeRequest):
    # DBOS handles the reliable execution in the background or synchronously
    handle: WorkflowHandle = DBOS.start_workflow(run_move_datatype_request, request)

if __name__ == "__main__":
    system_database_url = os.environ.get(
        "DBOS_SYSTEM_DATABASE_URL", "sqlite:///dbos_queue_worker.sqlite"
    )
    config: DBOSConfig = {
        "name": "dbos-queue-worker",
        "application_version": "0.1.0",
        "system_database_url": system_database_url,
    }
    DBOS(config=config)
    DBOS.launch()
    # Define a queue on which the web server
    # can submit workflows for execution.
    DBOS.register_queue("workflow-queue")
    # After launching DBOS, the worker waits indefinitely,
    # dequeuing and executing workflows.
    threading.Event().wait()