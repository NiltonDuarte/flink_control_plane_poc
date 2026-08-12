# Wayfinder Map: Sink Layer Control Plane

## Destination

Build a resilient control plane service that orchestrates sink‑layer Flink jobs, synchronising desired configuration from the Ingestion Config Portal to local Flink deployments, supporting durable multi‑step workflows and guaranteeing eventual consistency.

## Notes

- **Reference RFC**: `RFC_Sink_Layer_Control_Plane_Architecture.md` outlines objectives, core tech, and workflow ideas.
- **Skills to consult**: `/grilling`, `/domain-modeling`, `/research`, `/prototype`, `/task`.
- **Static typing**: Enforce strict type checking in Python using `pyright‑strict` or `mypy --strict` across the codebase.
- **Durable execution engine candidates**: Temporal and Restate – the POC will simulate Flink interactions via journaling files to compare these engines in a standalone application, avoiding real Kubernetes or Flink deployments.
- **Kubernetes integration**: Design a mutator component translating workflow steps into Kubernetes API calls with robust retry/error handling.
- **Saga compensation**: Define policies for pause, update, and resume failure scenarios.
- **MVP scope**: Determine minimal workflow set (Stop, Resume, Move‑Datatype, Reconcile) to deliver early value.

## Decisions so far

<!-- Filled as tickets are closed -->

## Not yet specified

- **Select durable execution engine** – need a concrete decision on Temporal vs Restate vs DBOS.
- **Define MVP workflow set** – which of Stop/Resume/Move‑Datatype/Reconcile to implement first.
- **Static type‑checking strategy** – tooling, CI integration, and enforcement policy.
- **Kubernetes mutator design** – API contract and error‑handling approach.
- **Failure‑scenario compensation policies** – level of granularity for saga rollbacks.

## Out of scope

<!-- Items explicitly ruled out will be listed here -->
