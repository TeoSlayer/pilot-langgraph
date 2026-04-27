"""Example handlers run by `python -m pilot_langgraph.worker --handlers worker_handlers`.

The worker process loads this module on the remote machine, registers each
handler by name, and dispatches incoming `PilotRemoteRunnable` calls to them.
Each handler takes the JSON-decoded payload (the LangGraph state slice the
caller sent) and returns the JSON-encodable result that becomes the node's
output back in the caller's graph.
"""
from __future__ import annotations

import hashlib
import os
import socket
from datetime import UTC, datetime


def _origin() -> dict:
    return {
        "processed_on_host": socket.gethostname(),
        "processed_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "worker_pid": os.getpid(),
    }


def enrich(payload):
    """Echo the payload back with a remote-processing receipt attached."""
    return {
        "input_payload": payload,
        "input_size_bytes": len(str(payload)),
        "remote_receipt": _origin(),
    }


def reverse_text(payload):
    """Reverse a string under the `text` key. Proves remote computation."""
    text = payload.get("text", "") if isinstance(payload, dict) else str(payload)
    return {"reversed": text[::-1], "remote_receipt": _origin()}


def compute_hash(payload):
    """SHA-256 the JSON-stringified payload. Proves remote work, not echo."""
    import json as _json
    blob = _json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return {
        "sha256": hashlib.sha256(blob.encode()).hexdigest(),
        "input_bytes": len(blob),
        "remote_receipt": _origin(),
    }


async def research(payload):
    """Streaming researcher: yields a few "findings" related to the topic.

    Used by examples/multi_agent_research.py to demonstrate fanout + streaming.
    """
    import asyncio as _aio
    topic = payload.get("topic", "the topic") if isinstance(payload, dict) else str(payload)
    angle = payload.get("angle", "general") if isinstance(payload, dict) else "general"
    findings = [
        f"[{angle}] {topic}: surface observation",
        f"[{angle}] {topic}: deeper claim derived from the surface observation",
        f"[{angle}] {topic}: edge case that the deeper claim doesn't cover",
    ]
    for f in findings:
        yield {"finding": f, "angle": angle, "host": _origin()["processed_on_host"]}
        await _aio.sleep(0.1)


# Restricted handler — only specific node_ids may invoke. Demo uses the local
# daemon's node_id, which the example reads at runtime; PILOT_TRUSTED_CALLERS
# in the worker env can override.
import os as _os
_TRUSTED_NODE_IDS_ENV = _os.environ.get("PILOT_TRUSTED_CALLERS", "")
_ALLOWED_EVALUATORS = (
    [int(s) for s in _TRUSTED_NODE_IDS_ENV.split(",") if s.strip()]
    if _TRUSTED_NODE_IDS_ENV
    else None  # default open if env unset
)


def evaluate(payload):
    """ACL-restricted handler: scores a list of findings."""
    findings = (payload or {}).get("findings") or []
    scored = []
    for i, f in enumerate(findings):
        text = f.get("finding", "") if isinstance(f, dict) else str(f)
        score = 1.0 / (1 + len(text) % 5)  # toy scoring
        scored.append({"finding": text, "score": round(score, 3), "rank": i})
    scored.sort(key=lambda x: x["score"], reverse=True)
    return {"scored": scored, "n_evaluated": len(scored), "evaluator_host": _origin()["processed_on_host"]}


def crash(payload):
    """Always raises ZeroDivisionError. For testing the typed-error wire."""
    return 1 / 0


async def slow_work(payload):
    """Handler that sleeps for `seconds` (default 5) then returns. For drain tests."""
    import asyncio as _aio
    secs = float(payload.get("seconds", 5)) if isinstance(payload, dict) else 5.0
    await _aio.sleep(secs)
    return {"slept_for": secs, "remote_receipt": _origin()}


async def stream_count(payload):
    """Streaming handler — yields N chunks, one per simulated step."""
    import asyncio as _aio
    n = int(payload.get("n", 5)) if isinstance(payload, dict) else 5
    for i in range(n):
        yield {"step": i, "host": _origin()["processed_on_host"]}
        await _aio.sleep(0.05)


def register(router):
    router.register("enrich", enrich)
    router.register("reverse_text", reverse_text)
    router.register("compute_hash", compute_hash)
    router.register("stream_count", stream_count)
    router.register("slow_work", slow_work)
    router.register("crash", crash)
    router.register("research", research)
    router.register("evaluate", evaluate, allow=_ALLOWED_EVALUATORS)
    # Co-host the checkpoint store on the same worker so a single process
    # serves both regular handlers and checkpoint persistence.
    from pilot_langgraph import checkpoint_worker
    checkpoint_worker.register(router)
