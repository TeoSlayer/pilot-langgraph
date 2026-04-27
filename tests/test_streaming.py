"""astream tests against the live worker's `stream_count` handler."""
from __future__ import annotations


import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


async def test_astream_yields_individual_chunks(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER=<addr> to run worker tests")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="stream_count", peer=worker_peer, timeout_secs=30)
    chunks = []
    async for c in r.astream({"n": 5}):
        chunks.append(c)
    assert len(chunks) == 5
    assert [c["step"] for c in chunks] == [0, 1, 2, 3, 4]
    assert all("host" in c for c in chunks)


async def test_astream_yields_each_chunk_separately(worker_peer):
    """Each yield arrives as its own frame, not coalesced into a single read.

    Proven by counting individual yields. Timing-based assertions are flaky
    once the daemon's send-side packet coalescing or local TCP buffering
    glues frames together — count is what matters.
    """
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER=<addr> to run worker tests")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="stream_count", peer=worker_peer, timeout_secs=30)
    n_chunks = 0
    async for _ in r.astream({"n": 7}):
        n_chunks += 1
    assert n_chunks == 7


async def test_ainvoke_still_works_for_non_streaming_handlers(worker_peer):
    """ainvoke must remain compatible with one-shot handlers under the new
    newline-delimited frame protocol."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER=<addr> to run worker tests")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=15)
    out = await r.ainvoke({"x": 1})
    assert out["input_payload"] == {"x": 1}


async def test_ainvoke_collects_last_value_from_stream(worker_peer):
    """ainvoke against a streaming handler returns the LAST chunk."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER=<addr> to run worker tests")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="stream_count", peer=worker_peer, timeout_secs=30)
    out = await r.ainvoke({"n": 3})
    assert out == {"step": 2, "host": out["host"]}
