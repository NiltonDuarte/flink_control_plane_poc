"""Application registry and canonical routing for the Temporal PoC."""

from temporal_app import TemporalApp

TASK_QUEUE = "flink-control-plane"

app = TemporalApp("flink-control-plane")
