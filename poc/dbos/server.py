import os
from pathlib import Path
from typing import List, Optional

import uvicorn
from dbos import DBOSClient, EnqueueOptions, SQLAlchemyDatasource, DBOS
from sqlalchemy import text
from fastapi import APIRouter, FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from poc.common.domain import MoveDatatypeRequest

# 1. Define a single shared database URL
SHARED_DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///app_and_dbos.sqlite")

# 2. Initialize DBOS system state in the shared database
DBOS(config={"system_database_url": SHARED_DATABASE_URL, "name": "flink_control_plane"})
DBOS_CLIENT = DBOSClient(system_database_url=SHARED_DATABASE_URL)

# 3. Initialize your application tables in the exact same database
family_repository = SQLAlchemyDatasource.create(SHARED_DATABASE_URL)

app = FastAPI()
api = APIRouter(prefix="/api")
DBOS_CLIENT = DBOSClient(system_database_url=SHARED_DATABASE_URL)

WF_PROGRESS_KEY = "workflow_progress"
WF_QUEUE_NAME = "workflow-queue"

frontend_dist = Path(__file__).parent / "frontend"


class WorkflowStatus(BaseModel):
    workflow_id: str
    workflow_status: str
    steps_completed: Optional[int]
    num_steps: Optional[int]


# Use the DBOS client to enqueue a workflow
# for execution on the worker.
@api.post("/workflows/move_datatype")
def enqueue_workflow(request: MoveDatatypeRequest):
    options: EnqueueOptions = {
        "queue_name": WF_QUEUE_NAME,
        "workflow_name": "move_datatype",
    }
    DBOS_CLIENT.enqueue(
        options, 
        request,
    )
    return {"status": "enqueued"}

# List all workflows and their progress to display on the frontend
@api.get("/workflows")
def list_workflows() -> List[WorkflowStatus]:
    # Use the DBOS client to list all workflows
    workflows = DBOS_CLIENT.list_workflows(name="workflow", sort_desc=True)
    statuses: List[WorkflowStatus] = []
    for workflow in workflows:
        # Query each workflow's progress event. This may not be available
        # if the workflow has not yet started executing.
        progress = DBOS_CLIENT.get_event(
            workflow.workflow_id, WF_PROGRESS_KEY, timeout_seconds=0
        )
        status = WorkflowStatus(
            workflow_id=workflow.workflow_id,
            workflow_status=workflow.status,
            steps_completed=progress.get("steps_completed") if progress else None,
            num_steps=progress.get("num_steps") if progress else None,
        )
        statuses.append(status)
    return statuses


# Serve the API router from the FastAPI app
app.include_router(api)

# app.frontend("/", directory=frontend_dist)

# # Serve index.html for root
# @app.get("/")
# async def serve_index():
#     return FileResponse(frontend_dist / "index.html")

# # Mount static frontend files last
# app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="static")


if __name__ == "__main__":
    with family_repository.sessionmaker() as session:
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS family_state (
                family TEXT PRIMARY KEY,
                state TEXT NOT NULL
            )
        """))
        session.commit()
        print("✅ Application schema initialized.")
    DBOS.launch()
    DBOS.register_queue(WF_QUEUE_NAME)
    uvicorn.run(app, host="0.0.0.0", port=8000)
