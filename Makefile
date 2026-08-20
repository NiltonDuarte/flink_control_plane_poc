.PHONY: help sync check lint format-check test test-fast test-crash server list histories clean \
	restate-server restate-service restate-register restate-test \
	restate-test-crash restate-scenario

help:
	@echo "make sync         install dependencies"
	@echo "make check        run all non-mutating quality gates"
	@echo "make lint         run Ruff lint checks"
	@echo "make format-check check Ruff formatting without rewriting files"
	@echo "make test         run the full suite"
	@echo "make test-fast    run everything except the crash test"
	@echo "make test-crash   run the worker-kill durability test only"
	@echo "make server       start a local Temporal dev server"
	@echo "make list         list the runnable scenarios"
	@echo "make histories    re-record the replay fixtures"
	@echo "make restate-server       start a local Restate server"
	@echo "make restate-service      serve the Restate SDK endpoint on :9080"
	@echo "make restate-register     register the local SDK endpoint"
	@echo "make restate-test         run Restate tests except crash recovery"
	@echo "make restate-test-crash   run Restate service-crash recovery"
	@echo "make restate-scenario SCENARIO=happy  run one Restate scenario"
	@echo "make clean        remove the local cluster directory"
	@echo ""
	@echo "run one scenario (needs 'make server' in another terminal):"
	@echo "  uv run python -m poc.cli run fail-in-resume"

sync:
	uv sync

check: lint format-check test-fast

lint:
	uv run ruff check .

format-check:
	uv run ruff format --check .

test:
	uv run pytest

test-fast:
	uv run pytest -m "not crash"

test-crash:
	uv run pytest -m crash

server:
	temporal server start-dev

list:
	uv run python -m poc.cli list

histories:
	uv run python -m tests.temporal.record_histories

restate-server:
	restate-server

restate-service:
	POC_CLUSTER_ROOT=.cluster uv run python -m poc.restate.run_service

restate-register:
	curl -sS --connect-timeout 5 --max-time 15 \
		-X POST http://localhost:9070/deployments \
		-H 'content-type: application/json' \
		-d '{"uri":"http://localhost:9080"}'

restate-test:
	uv run pytest tests/restate -m "not crash"

restate-test-crash:
	uv run pytest tests/restate/test_crash.py

restate-scenario:
	@test -n "$(SCENARIO)" || (echo "usage: make restate-scenario SCENARIO=happy"; exit 2)
	uv run python -m poc.cli run "$(SCENARIO)" --engine Restate

clean:
	rm -rf .cluster
