"""Transparent reconnect after a dead cached connection."""
from __future__ import annotations


import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


async def test_is_alive_true_for_fresh_connection():
    from pilot_langgraph.asyncio_client import PilotConnection
    async with await PilotConnection.connect() as c:
        assert c.is_alive() is True


async def test_is_alive_false_after_close():
    from pilot_langgraph.asyncio_client import PilotConnection
    c = await PilotConnection.connect()
    await c.close()
    assert c.is_alive() is False


async def test_is_alive_false_after_force_close():
    from pilot_langgraph.asyncio_client import PilotConnection
    c = await PilotConnection.connect()
    c.force_close_sync()
    assert c.is_alive() is False


async def test_registry_reconnects_when_cached_is_dead():
    """If we explicitly kill the cached connection, the next get() returns a fresh one."""
    from pilot_langgraph.runnables import _registry

    # Reset the per-loop slot for this test
    _registry._by_loop.clear()

    c1 = await _registry.get()
    assert c1.is_alive()

    # Kill it
    c1.force_close_sync()
    assert not c1.is_alive()

    # Get again — should be a NEW PilotConnection
    c2 = await _registry.get()
    assert c2 is not c1
    assert c2.is_alive()


async def test_remote_call_succeeds_after_cached_connection_dies(worker_peer):
    """End-to-end: invoke once, kill the cache, invoke again — second call succeeds transparently."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    from pilot_langgraph.runnables import _registry

    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=15)
    out1 = await r.ainvoke({"first": True})
    assert "remote_receipt" in out1

    # Force-close the cached connection (simulates daemon restart)
    for c in list(_registry._by_loop.values()):
        c.force_close_sync()

    out2 = await r.ainvoke({"second": True})
    assert "remote_receipt" in out2
