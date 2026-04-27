# Common operations for pilot-langgraph.
#
# Usage:  make <target>
# Run `make help` to see the full list.

# --- defaults ---
PYTHON      ?= .venv/bin/python
WORKER_PEER ?=

# --- meta ---
.PHONY: help
help:
	@echo "Common targets:"
	@echo "  test           — full test suite (set WORKER_PEER=<addr> for live-worker tests)"
	@echo "  test-no-daemon — pure-unit tests only (no daemon needed)"
	@echo "  test-no-worker — local-daemon tests, skip live-worker (no PILOT_WORKER_PEER)"
	@echo "  test-durable   — include the gated durability test (set PILOT_WORKER_RESTART_CMD)"
	@echo "  lint           — ruff + mypy"
	@echo "  build          — uv build a fresh wheel"
	@echo "  install-test   — verify the wheel installs in a fresh venv"
	@echo "  bench          — run tools/bench.py against WORKER_PEER"
	@echo "  discover       — run tools/discover.py against WORKER_PEER"

# --- test ---
.PHONY: test test-no-daemon test-no-worker test-durable
test:
	PILOT_WORKER_PEER=$(WORKER_PEER) $(PYTHON) -m pytest tests/ --ignore=tests/test_checkpoint_durability.py

test-no-daemon:
	PILOT_SOCKET=/nonexistent $(PYTHON) -m pytest tests/ --ignore=tests/test_checkpoint_durability.py

test-no-worker:
	$(PYTHON) -m pytest tests/ --ignore=tests/test_checkpoint_durability.py

test-durable:
	PILOT_WORKER_PEER=$(WORKER_PEER) $(PYTHON) -m pytest tests/

# --- lint / quality ---
.PHONY: lint
lint:
	$(PYTHON) -m ruff check src/ tests/ tools/ examples/ --select F,E702,E711,E712,E721
	$(PYTHON) -m mypy src/pilot_langgraph/ --ignore-missing-imports --no-strict-optional

# --- distribution ---
.PHONY: build install-test
build:
	rm -rf dist/
	uv build

install-test: build
	rm -rf /tmp/install-test
	uv venv --python 3.13 /tmp/install-test
	VIRTUAL_ENV=/tmp/install-test uv pip install dist/*.whl
	/tmp/install-test/bin/python -c "import pilot_langgraph; print('version', pilot_langgraph.__version__)"

# --- worker ops ---
.PHONY: discover bench
discover:
	@test -n "$(WORKER_PEER)" || (echo "set WORKER_PEER=<pilot-address>" && exit 1)
	$(PYTHON) tools/discover.py $(WORKER_PEER)

bench:
	@test -n "$(WORKER_PEER)" || (echo "set WORKER_PEER=<pilot-address>" && exit 1)
	$(PYTHON) tools/bench.py --peer $(WORKER_PEER) --concurrency 1,4,16 --calls 100
