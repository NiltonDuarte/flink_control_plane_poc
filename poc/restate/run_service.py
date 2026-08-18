"""Run the Restate ASGI endpoint locally on port 9080."""

from __future__ import annotations

import asyncio

from hypercorn.asyncio import serve
from hypercorn.config import Config

from poc.restate.app import app


async def _serve() -> None:
    config = Config()
    config.bind = ["0.0.0.0:9080"]
    await serve(app, config)


def main() -> None:
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
