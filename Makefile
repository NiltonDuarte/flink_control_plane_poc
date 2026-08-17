.PHONY: help sync test test-fast test-crash server list histories clean

help:
	@echo "make sync         install dependencies"
	@echo "make test         run the full suite"
	@echo "make test-fast    run everything except the crash test"
	@echo "make test-crash   run the worker-kill durability test only"
	@echo "make server       start a local Temporal dev server"
	@echo "make list         list the runnable scenarios"
	@echo "make histories    re-record the replay fixtures"
	@echo "make clean        remove the local cluster directory"
	@echo ""
	@echo "run one scenario (needs 'make server' in another terminal):"
	@echo "  uv run python -m poc_baseline.cli run fail-in-resume"

sync:
	uv sync

test:
	uv run pytest

test-fast:
	uv run pytest -m "not crash"

test-crash:
	uv run pytest -m crash

server:
	temporal server start-dev

list:
	uv run python -m poc_baseline.cli list

histories:
	uv run python -m tests.record_histories

clean:
	rm -rf .cluster
