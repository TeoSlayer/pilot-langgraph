"""PilotRemoteRunnable.adiscover tests."""
from __future__ import annotations

import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


async def test_discover_returns_runnables_for_each_handler(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    handlers = await PilotRemoteRunnable.adiscover(worker_peer, timeout_secs=15)

    # The deployed worker has at least these user handlers
    expected = {"enrich", "reverse_text", "compute_hash", "stream_count"}
    assert expected.issubset(handlers.keys()), f"missing: {expected - handlers.keys()}"
    # Each entry is a real PilotRemoteRunnable
    for name, runnable in handlers.items():
        assert isinstance(runnable, PilotRemoteRunnable)
        assert runnable.node == name


async def test_discover_skips_introspection_by_default(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    handlers = await PilotRemoteRunnable.adiscover(worker_peer, timeout_secs=15)
    assert "_health" not in handlers
    assert "_handlers" not in handlers


async def test_discover_can_include_introspection(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    handlers = await PilotRemoteRunnable.adiscover(
        worker_peer, timeout_secs=15, include_introspection=True
    )
    assert "_health" in handlers
    assert "_handlers" in handlers


async def test_discovered_runnable_actually_invokes(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    handlers = await PilotRemoteRunnable.adiscover(worker_peer, timeout_secs=15)
    out = await handlers["enrich"].ainvoke({"discovered": True})
    assert out["input_payload"] == {"discovered": True}
