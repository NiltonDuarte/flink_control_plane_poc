"""Worker wiring, shared by the CLI and the tests.

Everything runs on one task queue: the saga, the per-family actors, the cluster
activities, and the proxy activity. Keeping it to one queue is deliberate - the
serialization guarantee under test comes from the actor's lock, not from queue
topology, and a queue-per-family would quietly do the job for it.
"""

from __future__ import annotations

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from poc.adapters.temporal.activities import ALL_ACTIVITIES
from poc.adapters.temporal.actor import FlinkJobFamilyActor
from poc.adapters.temporal.actor_proxy import ActorProxy
from poc.adapters.temporal.saga import MoveDatatypeWorkflow

TASK_QUEUE = "flink-control-plane"


async def connect(target: str = "localhost:7233") -> Client:
    """Connect with the Pydantic data converter installed.

    Required: the workflows exchange Pydantic models, which the default JSON
    converter cannot round-trip.
    """
    return await Client.connect(target, data_converter=pydantic_data_converter)


def build_worker(client: Client, task_queue: str = TASK_QUEUE) -> Worker:
    proxy = ActorProxy(client, task_queue)
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[MoveDatatypeWorkflow, FlinkJobFamilyActor],
        activities=[*ALL_ACTIVITIES, proxy.execute_family_command],
    )


async def main() -> None:
    client = await connect()
    async with build_worker(client):
        print(f"worker running on task queue {TASK_QUEUE!r}; ctrl-c to stop")
        import asyncio

        await asyncio.Future()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
