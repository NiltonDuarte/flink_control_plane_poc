"""ASGI deployment endpoint for the Restate services."""

import restate

from poc.restate.actor import flink_job_family
from poc.restate.saga import move_datatype

app = restate.app(services=[flink_job_family, move_datatype])
