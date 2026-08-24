.PHONY: help sync check lint format-check test server list histories clean \
	restate-server restate-service restate-register restate-test \
	restate-scenario

help:
	@echo "make sync         install dependencies"
	@echo "make check        run all non-mutating quality gates"
	@echo "make lint         run Ruff lint checks"
	@echo "make format-check check Ruff formatting without rewriting files"
	@echo "make test         run the full suite"
	@echo "make server       start a local Temporal dev server"
	@echo "make list         list the runnable scenarios"
	@echo "make histories    re-record the replay fixtures"
	@echo "make restate-server       start a local Restate server"
	@echo "make restate-service      serve the Restate SDK endpoint on :9080"
	@echo "make restate-register     register the local SDK endpoint"
	@echo "make restate-test         run Restate tests except"
	@echo "make restate-scenario SCENARIO=happy  run one Restate scenario"
	@echo "make clean        remove the local cluster directory"
	@echo ""
	@echo "run one scenario (needs 'make server' in another terminal):"
	@echo "  uv run python -m poc.cli run fail-in-resume"

sync:
	uv sync

check: lint format test-fast

lint:
	uv run ruff check --fix .

format:
	uv run ruff format .

test:
	@restate-server --bind-ip 127.0.0.1 > /dev/null 2>&1 & RESTATE_PID=$$!; \
	sleep 2; \
	uv run pytest; \
	TEST_EXIT=$$?; \
	echo ""; \
	read -p "Tests finished. Press Enter to stop the Restate server... " dummy; \
	kill $$RESTATE_PID 2>/dev/null || true; \
	exit $$TEST_EXIT

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
	curl -sS -X POST http://localhost:9070/deployments \
		-H 'content-type: application/json' \
		-d '{"uri":"http://localhost:9080"}'

restate-test:
	uv run pytest tests/restate


restate-scenario:
	@test -n "$(SCENARIO)" || (echo "usage: make restate-scenario SCENARIO=happy"; exit 2)
	uv run python -m poc.cli run "$(SCENARIO)" --engine Restate

clean:
	rm -rf .cluster
