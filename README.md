# langgraph-pilot

Run [LangGraph](https://github.com/langchain-ai/langgraph) nodes on remote machines and persist graph state across networks, over end-to-end encrypted [Pilot Protocol](https://github.com/TeoSlayer/pilotprotocol) tunnels — NAT-traversed, mutually-authenticated, peer-to-peer. No public ports, no API gateway, no central broker.

```python
from langgraph.graph import StateGraph
from pilot_langgraph import (
    PilotRemoteRunnable,        # streaming, typed errors, retries
    PilotFanoutRunnable,        # parallel multi-worker dispatch
    PilotCheckpointSaver,       # durable state, retry + idempotent
    PilotChannel,               # pub/sub for multi-agent coordination
    pilot_handler,              # decorator with per-handler ACL
    ensure_trust,               # one-call bootstrap of mutual trust
)

# A graph that fans out to 3 remote workers in parallel and persists
# state on a 4th, all over end-to-end encrypted Pilot tunnels.
graph = StateGraph(MyState)
graph.add_node("research", PilotFanoutRunnable({
    "broad":   PilotRemoteRunnable(node="search",  peer="worker-a"),
    "deep":    PilotRemoteRunnable(node="search",  peer="worker-b"),
    "novel":   PilotRemoteRunnable(node="search",  peer="worker-c"),
}))
app = graph.compile(checkpointer=PilotCheckpointSaver(peer="state-worker"))
```

Each worker can be a laptop, a phone, an on-prem server, a cloud VM — anywhere a `pilot-daemon` runs. Nothing public-facing; mutual trust gates every call.

## What's in the box

Type-annotated (`py.typed`), Python 3.11+, pure stdlib + `langchain-core` + `langgraph`. Optional extras: `[xai]`, `[twilio]`, `[dotenv]`, `[otel]`.

### Remote execution
| Symbol | What it does |
|---|---|
| `PilotRemoteRunnable` | LangChain `Runnable` that runs on a remote pilot peer. Async-first (`ainvoke`/`astream`), sync wrapper (`invoke`/`stream`). Multiplexed concurrent calls share one connection. Configurable retry (`max_retries`, `retry_backoff_secs`). Transparent reconnect on dead cached connections. Optional caller-side pydantic validation (`input_model`, `output_model`). `await PilotRemoteRunnable.adiscover(peer)` enumerates all handlers. |
| `PilotFanoutRunnable` | Dispatch one input to N targets in parallel. `ainvoke` collects all; `astream` yields `(label, result)` as each arrives. `return_exceptions=True` for partial-success. |
| `PilotEchoRunnable` | Round-trip via Pilot's built-in echo (port 7). Smoke test against any peer. |
| LangGraph `Send` API | Native parallel branching works: a node returns `[Send("worker", state), ...]` for dynamic data-dependent fanout. |

### Persistence + state
| Symbol | What it does |
|---|---|
| `PilotCheckpointSaver` | `BaseCheckpointSaver` subclass. Ships every checkpoint to a remote peer. Idempotent retries on every method (`UNIQUE` constraint + `INSERT OR IGNORE` for `put_writes`). SQLite-backed worker store (opt-in via `PILOT_CHECKPOINT_DB=/path.db`); in-memory default. |

### Pub/sub
| Symbol | What it does |
|---|---|
| `PilotChannel` / `PilotPublisher` | Subscribe/publish over Pilot's port-1002 broker. Multi-agent broadcast with no central server. `auto_reconnect=True` survives broker restarts with backoff. |
| `PilotEventSource` | Runnable that streams events into a graph node from a remote topic. |

### Worker server
| Symbol | What it does |
|---|---|
| `WorkerServer` | Async server. Auto-registers `_health` and `_handlers` introspection. Graceful SIGTERM drain. Async-gen handlers stream chunks. Per-handler counters (calls, errors, p50/p95). `add_periodic_task(interval, fn)` for scheduled background work. `middleware=[...]` for cross-cutting concerns. `clear_user_handlers()` for hot-reload. |
| `@pilot_handler("name", ...)` | Decorate a function in any module loaded by the worker CLI. Kwargs: `allow=[node_ids]` (ACL), `timeout_secs` (per-call wallclock), `rate_per_caller` + `rate_window_secs` (sliding window), `max_concurrent` (semaphore), `input_model` + `output_model` (pydantic validation). |
| `Context` | Optional second arg to handlers — opt in by declaring 2 positional args. Carries `caller_node_id`, `caller_addr`, `caller_port`, `request_id` (uuid4), `started_at`, `handler_name`. |
| `Middleware` | `(payload, ctx, next_fn)` async or sync; outer-first composition. Wraps every non-introspection handler. |

### Observability
| Symbol | What it does |
|---|---|
| `RunnableConfig` callbacks | `on_chain_start`/`end`/`error` fire for every remote call (LangSmith works). |
| `--metrics-port N` | Worker CLI flag exposes Prometheus `/metrics` + `/healthz` on stdlib HTTP. 8 metrics including per-handler p50/p95. |
| `--log-format json` | Worker CLI flag for JSON-line logs (k8s/ELK/Datadog). Per-call lines include `request_id`, `caller_node_id`. |
| `_handlers` introspection | Exposes per-handler `input_schema` + `output_schema` (pydantic JSON Schema), `timeout_secs`, `max_concurrent`, ACL, p50/p95 — discoverable over the wire. |
| OpenTelemetry | Client + server spans with W3C `traceparent` propagation across the wire. Optional dep `[otel]` — graceful no-op without it. |

### Trust + transport
| Symbol | What it does |
|---|---|
| `ensure_trust(node_id)` / `ensure_trust_sync` | One Python call to bootstrap mutual trust. Idempotent. Polls until `mutual=true`. |
| `PilotConnection` | Native async client speaking the daemon's binary IPC over its Unix socket. No `pilotctl` subprocess. Streams + datagrams + jsonRPC, multiplexed; safe across coroutines. `is_alive()` for explicit health check. |

### Errors
| Symbol | What it does |
|---|---|
| `PilotRemoteError` (base) + `PilotHandlerNotFoundError`, `PilotUnauthorizedError`, `PilotHandlerError`, `PilotRateLimitError` | Typed exceptions with `peer` and `node` attributes. All subclass `RuntimeError` so legacy `except RuntimeError` still works. Worker tags every error reply with structured `error_type` field. |

### Testing helpers
| Symbol | What it does |
|---|---|
| `pilot_langgraph.testing.invoke_handler(fn, payload, ctx=...)` | Drive a handler from a unit test without a daemon. Detects sync/async/async-gen + Context + pydantic validation. |
| `pilot_langgraph.testing.make_context(...)` | Build a fake `Context` with sensible defaults. |
| `pilot_langgraph.testing.collect_stream(fn, ...)` | Drain an async-gen handler into a list. |

### CLIs + tools
| Tool | What it does |
|---|---|
| `pilot-langgraph-worker --handlers <module> [--port N] [--metrics-port N] [--reload] [--log-format json] [--drain-timeout N]` | Long-running worker process. `--reload` watches the handler module for file changes and re-registers handlers atomically. |
| `pilot-langgraph-trust <node_id>` | Bootstrap mutual trust from the shell. |
| `tools/bench.py --peer <addr> --concurrency 1,4,16 --calls 200` | Latency + throughput benchmark. |
| `tools/discover.py <peer> [--schemas] [--json]` | Pretty-print a worker's handler list, ACLs, perf, and JSON schemas. |

## Quickstart

```bash
# 1. Install (one-time)
curl -fsSL https://pilotprotocol.network/install.sh | sh
pilotctl daemon start --hostname my-laptop --email you@example.com --trust-auto-approve

# 2. Plugin
uv venv --python 3.13 && uv pip install -e .

# 3. On a SECOND host (any cloud, any network), repeat (1) with a different
#    --hostname and --email, then deploy a worker:
cat > handlers.py <<'PY'
from pilot_langgraph import pilot_handler

@pilot_handler("greet")
def greet(payload):
    return {"hello": payload.get("name", "world"), "host": __import__("socket").gethostname()}
PY
python -m pilot_langgraph.worker --handlers handlers --port 5000

# 4. From the laptop, bootstrap trust (one-time):
python -m pilot_langgraph.trust <worker-node-id>

# 5. Call the remote function as a LangGraph node:
python -c "
from pilot_langgraph import PilotRemoteRunnable
r = PilotRemoteRunnable(node='greet', peer='<worker-address-or-hostname>')
print(r.invoke({'name': 'Pilot'}))
"
```

## Deploy

Three reference deployments live in `deploy/`:

- **`pilot-langgraph-worker.service`** — systemd unit for bare-metal/VM hosts.
- **`Dockerfile`** — container image (Python 3.13 base, plugin pre-installed, healthcheck on `/healthz`). Build with `uv build && docker build -t pilot-langgraph-worker -f deploy/Dockerfile .`.
- **`docker-compose.example.yaml`** — local `docker compose up` recipe. Bind-mounts `/tmp/pilot.sock` from the host daemon and exposes Prometheus on 9090.

The pilot daemon itself is expected to run on the host (or in a sidecar with host networking) — Pilot's NAT traversal needs UDP control over the host's network interface, which fights default Docker bridge networking.

## Examples

| File | What it shows |
|---|---|
| `examples/simple_distributed.py` | Smallest no-LLM demo: 3-node graph, input → remote `compute_hash` → output. No LLM dependency — start here to verify your setup. |
| `examples/distributed_demo.py` | Local Grok agent → public `agent-alpha` echo. Smallest "graph state crosses the network" demo. |
| `examples/distributed_grok_demo.py` | 3-node Grok graph with one remote `enrich` node on any pilot worker. |
| `examples/checkpoint_demo.py` | Graph state persists across process restarts — write in one process, retrieve in a fresh one. |
| `examples/multi_agent_research.py` | Fanout (3 parallel researchers, streaming) → ACL-restricted evaluator → Grok summary, all checkpointed, all callbacks fire. |
| `examples/cross_host_fanout.py` | One input fanned out across two remote VMs in parallel; each branch's reply includes its host + PID, proving distinct physical execution. |
| `examples/dynamic_send_demo.py` | LangGraph `Send` API: planner produces N queries at runtime, each dispatched to the same remote node in parallel, results aggregate via `Annotated[..., operator.add]`. |

## Architecture

```
                                          [remote pilot worker]
        local laptop                       ┌──────────────────────────────┐
        ┌─────────────────────┐            │  WorkerServer (port 5000)    │
        │  LangGraph (yours)  │            │   ├─ search()    [open]      │
        │   ├─ planner        │            │   ├─ evaluate()  [ACL: only  │
        │   ├─ remote_node    │  dial      │   │   trusted-orchestrator]  │
        │   │   (PilotRemote  │  ────►     │   ├─ research()  [streaming] │
        │   │    Runnable)    │  reply     │   └─ checkpoint_*  [SQLite]  │
        │   ├─ fanout         │  ◄────     │                              │
        │   │   (PilotFanout) │            │  Built-in pub/sub broker     │
        │   ├─ checkpointer   │            │   (port 1002)                │
        │   │   (PilotChkpt)  │            └──────────────────────────────┘
        │   └─ summarizer     │                       ▲
        └─────────┬───────────┘                       │
                  │                                   │ encrypted UDP
        ┌─────────▼─────────┐         ┌───────────────┴───────────────┐
        │ PilotConnection   ├─unix─── │ pilot-daemon (this host)      │
        │ (asyncio, native) │         └───────────────┬───────────────┘
        └───────────────────┘                         │
                                                NAT-traversed via
                                                Pilot rendezvous
```

Each Pilot daemon assigns the host a 48-bit virtual address (`N:NNNN.HHHH.LLLL`). Trusted peers reach each other by address or hostname; the rendezvous server only handles discovery and NAT hole-punching — your data flows directly between daemons.

## Reliability + safety

| Mechanism | Where | What it covers |
|---|---|---|
| 3x retry with backoff + connection drop | `PilotRemoteRunnable.ainvoke`/`astream` | Cold tunnels, transient connection failures, daemon-side socket divergence. |
| Transparent reconnect on cached-conn death | `_ConnectionRegistry.get` (calls `is_alive()` before reuse) | Long-running deployments survive daemon restarts with no caller error. |
| 3x retry per checkpoint method | `PilotCheckpointSaver` | Persistent state writes on flaky links. |
| Idempotent `aput_writes` | SQLite `UNIQUE(thread, ns, cid, task_id, task_path) + INSERT OR IGNORE`; in-memory dedup | Safe to retry mid-graph. |
| Per-handler `timeout_secs` | `WorkerServer` / `@pilot_handler` | Runaway handlers cancelled, caller gets typed error. |
| Per-caller `rate_per_caller` | sliding-window deque | Burst-abuse protection. |
| `max_concurrent` semaphore | per-handler | Resource-bound handlers (GPU, DB pool) protected from oversubscription. |
| Graceful SIGTERM drain | `WorkerServer` | Lossless rolling restarts. |
| Pre-close flush yield | `WorkerServer._handle_one` | Large reply truncation under network stress. |
| `PilotChannel` auto-reconnect | background task with backoff | Subscribers survive broker restarts. |

## Performance

`tools/bench.py` measures latency + throughput against any deployed worker.

```bash
python tools/bench.py --peer <addr> --handler enrich \
    --concurrency 1,4,16 --calls 200
```

Sample numbers from a laptop calling an `e2-micro`-class VM across the Atlantic (~150ms RTT), 100 calls per level on the `enrich` handler:

| concurrency | wall (s) | RPS | p50 ms | p95 ms | p99 ms |
|---:|---:|---:|---:|---:|---:|
| 1 | 26.9 | 3.7 | 136 | 1961 | 2097 |
| 4 | 15.8 | 6.3 | 388 | 2462 | 2581 |
| 16 | 13.5 | 7.4 | 2231 | 3254 | 6293 |
| 32 | 14.7 | 6.8 | 4490 | 4557 | 5464 |

Single-flight latency ~150ms (one RTT). Throughput saturates around the worker's CPU at ~7 RPS for this trivial handler on `e2-micro`. Real handlers with cheaper bodies and beefier workers should scale linearly with concurrency until the network or worker CPU becomes the bottleneck.

## Tests

```bash
uv pip install -e .[dev]

# All tests including live-worker integration (needs a deployed worker):
PILOT_WORKER_PEER=<addr-of-deployed-worker> python -m pytest tests/

# All tests that need a local daemon but no remote worker:
python -m pytest tests/         # 124 pass, 49 skip cleanly

# Pure-unit tests (no daemon, no network — for any CI):
PILOT_SOCKET=/nonexistent .venv/bin/python -m pytest tests/   # 102 pass, 71 skip
```

**173 tests** covering wire codec, transport, runnables, checkpointer, channel + reconnect, ACL, timeouts, rate limits, max_concurrent, callbacks, otel, metrics, health/introspection, handler context, middleware, pydantic validation, typed errors, trust bootstrap, fanout, streaming, dynamic Send, retry config, periodic tasks, hot-reload, discover, deprecation, aclose. Live tests skip cleanly if the matching infra isn't reachable. Default pytest config rerun-2 absorbs the rare cold-tunnel transient.

## Layout

```
src/pilot_langgraph/
├── _ipc.py                  # binary wire codec
├── _otel.py                 # optional OpenTelemetry integration
├── asyncio_client.py        # PilotConnection (with is_alive), Stream, Listener
├── runnables.py             # PilotRemoteRunnable, PilotFanoutRunnable, PilotEchoRunnable
├── checkpoint.py            # PilotCheckpointSaver
├── checkpoint_worker.py     # in-memory + SQLite checkpoint stores
├── channel.py               # PilotChannel (+ auto_reconnect), PilotPublisher, PilotEventSource
├── server.py                # WorkerServer + @pilot_handler + Context + per-handler ACL/timeout/rate/concurrency
├── metrics.py               # Prometheus /metrics + /healthz HTTP server
├── trust.py                 # ensure_trust + CLI
├── errors.py                # typed exception hierarchy
└── worker.py                # `python -m pilot_langgraph.worker` CLI
deploy/pilot-langgraph-worker.service
examples/
tests/
```

## Caveats

- **Hairpin NAT.** Two pilot daemons behind the same router can't tunnel to each other on most consumer NATs. For dev work with one laptop, point at the public `agent-alpha` echo peer or deploy a worker on a separate host (any cloud, any region — `e2-micro` on GCP free tier is enough).
- **Trust must be mutual.** Use `ensure_trust(node_id)` from Python, or `pilotctl handshake <peer>` from each side. `--trust-auto-approve` on workers handles this for unattended deploys.
- **Worker ACL is per-handler, not per-peer-globally.** Every trusted peer can call any open handler; restrict sensitive handlers explicitly with `@pilot_handler("name", allow=[node_ids])`.

## Changelog

- **0.6.1** — Reference deployments (Dockerfile + docker-compose). `aclose()` / `aclose_sync()` for explicit connection shutdown. Legacy `pilot_client` / `remote_node` shims emit `DeprecationWarning` (removal in 1.0).
- **0.6.0** — Benchmark harness (`tools/bench.py`). Cross-host fanout. `WorkerServer` middleware chain. Pydantic `input_model` / `output_model` on both ends. `_handlers` introspection exposes JSON schemas. `tools/discover.py`. JSON log format with `request_id` correlation. Configurable retry. Periodic worker tasks. `--reload`. `PilotRemoteRunnable.adiscover()`. `pilot_langgraph.testing` helpers.
- **0.5.0** — `_health` + `_handlers` introspection. Handler `Context`. Per-handler `timeout_secs`, `rate_per_caller`, `max_concurrent`. Prometheus `/metrics` + `/healthz`. OpenTelemetry with W3C trace propagation. Transparent reconnect. `PilotChannel` `auto_reconnect`. `PilotRateLimitError`.
- **0.3.0** — Native async IPC client. Streaming. SQLite-backed checkpoint store. Idempotent `put_writes`. `RunnableConfig` propagation. Trust bootstrap helper. Pub/sub channels. Graceful SIGTERM drain. Per-handler ACL. `PilotFanoutRunnable`. Typed exception hierarchy.
- **0.1.0** — Initial prototype: `PilotRemoteRunnable` + `WorkerRouter`.

## License

AGPL-3.0 (matches Pilot Protocol). See `LICENSE`.
