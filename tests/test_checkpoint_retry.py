"""PilotCheckpointSaver retries transient failures.

The first attempt is forced to fail by monkey-patching the underlying
PilotRemoteRunnable; the retry then runs against the live worker.
"""
from __future__ import annotations


import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


async def test_aput_retries_after_transient_failure(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")

    from langchain_core.runnables import RunnableConfig
    from pilot_langgraph import PilotCheckpointSaver

    saver = PilotCheckpointSaver(peer=worker_peer, port=5000, timeout_secs=20)

    real_ainvoke = saver._call_put.ainvoke
    state = {"failures_left": 1}

    async def flaky(input, *a, **kw):
        if state["failures_left"] > 0:
            state["failures_left"] -= 1
            raise RuntimeError("simulated transient")
        return await real_ainvoke(input, *a, **kw)

    saver._call_put.ainvoke = flaky  # type: ignore[method-assign]

    cfg: RunnableConfig = {"configurable": {"thread_id": "retry-test", "checkpoint_ns": ""}}
    checkpoint = {
        "v": 1,
        "id": "retry-cp-1",
        "ts": 0,
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
        "pending_sends": [],
    }
    metadata = {"source": "input", "step": 0, "writes": {}, "parents": {}}
    new_versions: dict = {}

    out = await saver.aput(cfg, checkpoint, metadata, new_versions)
    assert out["configurable"]["checkpoint_id"] == "retry-cp-1"
    assert state["failures_left"] == 0  # the first attempt consumed the failure

    # Verify it actually landed
    fetched = await saver.aget_tuple({"configurable": {"thread_id": "retry-test", "checkpoint_id": "retry-cp-1"}})
    assert fetched is not None


async def test_aput_writes_dedupes_on_retry(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")

    from langchain_core.runnables import RunnableConfig
    from pilot_langgraph import PilotCheckpointSaver

    saver = PilotCheckpointSaver(peer=worker_peer, port=5000, timeout_secs=20)
    cfg: RunnableConfig = {
        "configurable": {
            "thread_id": "dedupe-test",
            "checkpoint_ns": "",
            "checkpoint_id": "dedupe-cp-1",
        }
    }
    # First, anchor a checkpoint so writes have a parent
    ckpt = {"v": 1, "id": "dedupe-cp-1", "ts": 0, "channel_values": {},
            "channel_versions": {}, "versions_seen": {}, "pending_sends": []}
    await saver.aput({"configurable": {"thread_id": "dedupe-test", "checkpoint_ns": ""}},
                     ckpt, {"source": "input", "step": 0, "writes": {}, "parents": {}}, {})

    # Issue same put_writes twice with the same task_id.
    await saver.aput_writes(cfg, [("foo", "bar")], task_id="task-abc")
    await saver.aput_writes(cfg, [("foo", "bar")], task_id="task-abc")

    # Fetch and confirm exactly one pending write made it (not two).
    fetched = await saver.aget_tuple(cfg)
    assert fetched is not None
    pending = fetched.pending_writes
    matches = [p for p in pending if p[0] == "task-abc"]
    assert len(matches) == 1, f"expected 1 dedupe'd write, got {len(matches)}: {matches}"
