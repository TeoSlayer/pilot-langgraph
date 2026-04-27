"""Shared fixtures for pilot-langgraph integration tests.

Tests are skipped if the local pilot daemon socket isn't reachable, so the
suite is safe to run without infrastructure.
"""
from __future__ import annotations

import asyncio
import os

import pytest


def _daemon_reachable() -> bool:
    socket_path = os.environ.get("PILOT_SOCKET", "/tmp/pilot.sock")
    if not os.path.exists(socket_path):
        return False
    try:
        from pilot_langgraph.asyncio_client import PilotConnection
        async def go():
            async with await PilotConnection.connect() as c:
                return bool(await c.info())
        return asyncio.run(go())
    except Exception:
        return False


requires_daemon = pytest.mark.skipif(not _daemon_reachable(), reason="local pilot daemon not reachable")


@pytest.fixture(autouse=True)
def _reset_registry():
    """Drop and force-close any cached PilotConnection from previous tests.

    pytest-asyncio mode=auto creates a fresh event loop per test, but cached
    PilotConnections have reader tasks bound to the OLD loop. Just clearing
    the dict isn't enough — the underlying Unix socket stays open, and the
    daemon accumulates state per IPC connection that eventually trips up
    list/stream calls under load.

    `force_close_sync()` shuts the writer's transport without needing to
    await anything, which is necessary when the owning loop is already
    destroyed.
    """
    from pilot_langgraph.runnables import _registry
    yield
    for c in list(_registry._by_loop.values()):
        try:
            c.force_close_sync()
        except Exception:
            pass
    _registry._by_loop.clear()
    _registry._locks.clear()


@pytest.fixture
async def conn():
    from pilot_langgraph.asyncio_client import PilotConnection
    c = await PilotConnection.connect()
    try:
        yield c
    finally:
        await c.close()


@pytest.fixture
def remote_peer() -> str:
    """Address of a reachable peer with the standard echo service.

    Override with `PILOT_TEST_PEER` (e.g. a deployed pilot-langgraph worker).
    Defaults to the public `agent-alpha` demo node which always echoes.
    """
    return os.environ.get("PILOT_TEST_PEER", "agent-alpha")


@pytest.fixture
def worker_peer() -> str | None:
    """Address of a deployed pilot-langgraph worker (with custom handlers).

    If unset, tests requiring a custom worker are skipped.
    """
    return os.environ.get("PILOT_WORKER_PEER")
