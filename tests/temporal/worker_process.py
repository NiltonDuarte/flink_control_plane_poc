"""Standalone worker entrypoint, launched as a subprocess by the crash test.

Needs to be its own process so the test can SIGKILL it - killing a worker task
inside the test process would only prove that asyncio cancellation works.

    python -m tests.temporal.worker_process <server-address> <cluster-root>
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from poc.common.cluster import ENV_CLUSTER_ROOT
from poc.temporal.worker import build_worker, connect

logging.getLogger("temporalio").setLevel(logging.CRITICAL)


async def main() -> None:
    address, cluster_root = sys.argv[1], sys.argv[2]
    os.environ[ENV_CLUSTER_ROOT] = cluster_root
    client = await connect(address)
    async with build_worker(client):
        await asyncio.Future()  # run until killed


if __name__ == "__main__":
    asyncio.run(main())
