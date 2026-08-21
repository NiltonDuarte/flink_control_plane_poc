#!/bin/bash
source .env
uvx dbos-argus@latest --db-url "sqlite:////dbos_queue_worker.sqlite"
