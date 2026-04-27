"""RunnableConfig propagation: callbacks see remote calls.

Without these, LangSmith tracing and any user-supplied callback handler
silently miss every remote node — the graph appears to skip the work.
"""
from __future__ import annotations

from uuid import UUID

import pytest

from langchain_core.callbacks import AsyncCallbackHandler

from .conftest import requires_daemon


pytestmark = [requires_daemon]


class _Recorder(AsyncCallbackHandler):
    def __init__(self):
        self.starts: list[dict] = []
        self.ends: list[dict] = []
        self.errors: list[BaseException] = []

    async def on_chain_start(self, serialized, inputs, *, run_id, parent_run_id=None, tags=None, metadata=None, **kw):
        self.starts.append({"serialized": serialized, "inputs": inputs, "run_id": run_id, "tags": tags, "metadata": metadata})

    async def on_chain_end(self, outputs, *, run_id, parent_run_id=None, tags=None, **kw):
        self.ends.append({"outputs": outputs, "run_id": run_id})

    async def on_chain_error(self, error, *, run_id, parent_run_id=None, tags=None, **kw):
        self.errors.append(error)


async def test_callbacks_fire_on_ainvoke(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    rec = _Recorder()
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=15)
    await r.ainvoke(
        {"x": 1},
        config={"callbacks": [rec], "tags": ["pytest"], "metadata": {"foo": "bar"}},
    )
    assert len(rec.starts) == 1
    assert rec.starts[0]["serialized"]["name"] == "PilotRemoteRunnable:enrich"
    assert rec.starts[0]["tags"] == ["pytest"]
    assert rec.starts[0]["metadata"] == {"foo": "bar"}
    assert isinstance(rec.starts[0]["run_id"], UUID)
    assert len(rec.ends) == 1
    assert rec.ends[0]["outputs"]["output"]["input_payload"] == {"x": 1}
    assert not rec.errors


async def test_callbacks_fire_on_error(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    rec = _Recorder()
    r = PilotRemoteRunnable(node="missing", peer=worker_peer, timeout_secs=10)
    with pytest.raises(RuntimeError, match="no handler"):
        await r.ainvoke({}, config={"callbacks": [rec]})
    assert len(rec.starts) == 1
    assert len(rec.errors) == 1
    assert "no handler" in str(rec.errors[0])
    assert not rec.ends


async def test_callbacks_fire_on_astream(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    rec = _Recorder()
    r = PilotRemoteRunnable(node="stream_count", peer=worker_peer, timeout_secs=20)
    chunks = []
    async for c in r.astream({"n": 3}, config={"callbacks": [rec]}):
        chunks.append(c)
    assert len(chunks) == 3
    assert len(rec.starts) == 1
    assert rec.starts[0]["serialized"]["name"] == "PilotRemoteRunnable:stream_count:stream"
    assert len(rec.ends) == 1
    assert rec.ends[0]["outputs"]["n_chunks"] == 3
