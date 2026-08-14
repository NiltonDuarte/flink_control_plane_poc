"""The Temporal adapter: the core saga, executed as workflows and activities.

The mapping, in one table:

| Core concept            | Temporal mechanism                                    |
|-------------------------|-------------------------------------------------------|
| the saga                | `MoveDatatypeWorkflow`, a workflow                    |
| a `ClusterOp`           | an activity, retried per `CLUSTER_RETRY`              |
| a family's single writer| an entity workflow keyed `family:<name>` + a lock     |
| a `Command`             | a workflow *update*, reached via a proxy activity     |
| the idempotency key     | `workflow_id:run_id:step`, dedup'd by `execute_update`|

The two entries that cost something are the last two, and both are recorded in
detail where they live - `actor_proxy.py` for why a workflow cannot call another
workflow request/response, and `ports.py` for why the key is keyed on the run id.
"""
