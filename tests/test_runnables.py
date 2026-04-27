"""Live tests of PilotEchoRunnable and PilotRemoteRunnable.

PilotEchoRunnable runs against the public `agent-alpha` echo peer.
PilotRemoteRunnable tests require `PILOT_WORKER_PEER` set to a deployed
worker hosting the example handlers (`enrich`, etc.).
"""
from __future__ import annotations

import asyncio

import pytest

from .conftest import requires_daemon

pytestmark = [requires_daemon]


async def test_echo_runnable_round_trip(remote_peer):
    from pilot_langgraph import PilotEchoRunnable
    r = PilotEchoRunnable(peer=remote_peer)
    out = await r.ainvoke({"hello": "world"})
    assert out["round_trip_ok"] is True


async def test_remote_runnable_invokes_custom_handler(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER=<addr> to run worker tests")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=30)
    out = await r.ainvoke({"plan": "test"})
    assert out["input_payload"] == {"plan": "test"}
    assert "remote_receipt" in out
    assert "processed_on_host" in out["remote_receipt"]


async def test_concurrent_remote_calls(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER=<addr> to run worker tests")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=30)
    outs = await asyncio.gather(*[r.ainvoke({"idx": i}) for i in range(5)])
    assert {o["input_payload"]["idx"] for o in outs} == {0, 1, 2, 3, 4}


def test_runnable_invoke_sync(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER=<addr> to run worker tests")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=30)
    out = r.invoke({"sync": True})
    assert out["input_payload"] == {"sync": True}


async def test_unknown_handler_raises(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER=<addr> to run worker tests")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="does-not-exist", peer=worker_peer, timeout_secs=10)
    with pytest.raises(RuntimeError, match="no handler"):
        await r.ainvoke({})
