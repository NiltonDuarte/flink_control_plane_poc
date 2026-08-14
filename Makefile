.PHONY: help sync test test-fast test-core test-crash server list histories clean

help:
	@echo "make sync         install dependencies"
	@echo "make test         run the full suite"
	@echo "make test-fast    run everything except the crash test"
	@echo "make test-core    run only the engine-free core tests (no server, instant)"
	@echo "make test-crash   run the worker-kill durability test only"
	@echo "make server       start a local Temporal dev server"
	@echo "make list         list the runnable scenarios"
	@echo "make histories    re-record the replay fixtures"
	@echo "make clean        remove the local cluster directory"
	@echo ""
	@echo "run one scenario (needs 'make server' in another terminal):"
	@echo "  uv run python -m poc.cli run fail-in-resume"
	@echo "  uv run python -m poc.cli run fail-in-resume --engine restate"

sync:
	uv sync

test:
	uv run pytest

test-fast:
	uv run pytest -m "not crash"

test-core:
	uv run pytest tests/test_core_saga.py tests/test_core_is_engine_free.py

test-crash:
	uv run pytest -m crash

server:
	temporal server start-dev

list:
	uv run python -m poc.cli list

histories:
	uv run python -m tests.temporal.record_histories

clean:
	rm -rf .cluster
