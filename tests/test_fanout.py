"""PilotFanoutRunnable: dispatch one input to N remote targets in parallel."""
from __future__ import annotations

import time

import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


def _build_fan(worker_peer: str, n: int):
    """Build a fanout that dispatches the same input to N copies of `enrich`
    on the same worker. In production the targets would be different peers."""
    from pilot_langgraph import PilotFanoutRunnable
    return PilotFanoutRunnable({
        f"branch-{i}": {"node": "enrich", "peer": worker_peer, "timeout_secs": 30}
        for i in range(n)
    })


async def test_fanout_collects_all_results(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    fan = _build_fan(worker_peer, 3)
    out = await fan.ainvoke({"x": 1})
    assert set(out.keys()) == {"branch-0", "branch-1", "branch-2"}
    for v in out.values():
        assert v["input_payload"] == {"x": 1}
        assert "remote_receipt" in v


async def test_fanout_runs_in_parallel(worker_peer):
    """Wall-clock proves parallelism: 5 calls in less time than 5x sequential."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    fan = _build_fan(worker_peer, 5)
    t0 = time.monotonic()
    await fan.ainvoke({"y": 2})
    elapsed = time.monotonic() - t0
    # Single ainvoke ~150ms each over a typical pilot tunnel; 5 sequential would be ~750ms.
    # Parallel should land well under that. Generous bound: <2x median single-call time.
    assert elapsed < 2.0, f"fanout took {elapsed:.2f}s, expected <2s for parallel"


async def test_fanout_streams_results_as_they_arrive(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    fan = _build_fan(worker_peer, 4)
    received: list[tuple[str, dict]] = []
    async for label, result in fan.astream({"z": 3}):
        received.append((label, result))
    assert {label for label, _ in received} == {"branch-0", "branch-1", "branch-2", "branch-3"}
    for _, v in received:
        assert v["input_payload"] == {"z": 3}


async def test_fanout_propagates_failures_by_default(worker_peer):
    """One bad target raises; rest may have already replied (best-effort gather)."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotFanoutRunnable
    fan = PilotFanoutRunnable({
        "ok":  {"node": "enrich",            "peer": worker_peer, "timeout_secs": 15},
        "bad": {"node": "no-such-handler",   "peer": worker_peer, "timeout_secs": 15},
    })
    with pytest.raises(RuntimeError, match="no handler"):
        await fan.ainvoke({})


async def test_fanout_return_exceptions_keeps_partial_results(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotFanoutRunnable
    fan = PilotFanoutRunnable(
        {
            "ok":  {"node": "enrich",          "peer": worker_peer, "timeout_secs": 15},
            "bad": {"node": "no-such-handler", "peer": worker_peer, "timeout_secs": 15},
        },
        return_exceptions=True,
    )
    out = await fan.ainvoke({})
    assert out["ok"]["input_payload"] == {}
    assert isinstance(out["bad"], Exception)
    assert "no handler" in str(out["bad"])


def test_empty_targets_rejected():
    from pilot_langgraph import PilotFanoutRunnable
    with pytest.raises(ValueError):
        PilotFanoutRunnable({})
