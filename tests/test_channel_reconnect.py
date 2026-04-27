"""PilotChannel auto-reconnect across broker disconnects."""
from __future__ import annotations

import asyncio

import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


async def test_auto_reconnect_recovers_subscription_after_drop(worker_peer):
    """After force-closing the underlying conn, the subscriber recovers and receives later events."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotChannel

    sub = await PilotChannel.subscribe(
        "reconnect-test", peer=worker_peer, timeout_secs=15,
        auto_reconnect=True, backoff_secs=0.5,
    )
    async with sub:
        # Let the bg task get going
        await asyncio.sleep(0.3)

        # Verify normal delivery first
        await PilotChannel.publish_one("reconnect-test", b"before-drop", peer=worker_peer, timeout_secs=10)
        ev1 = await sub.recv(timeout=10)
        assert ev1.payload == b"before-drop"

        # Force-kill the underlying connection (simulates broker daemon dropping us)
        sub._conn.force_close_sync()

        # Wait for the bg task to notice + reconnect (one iteration of backoff + reconnect)
        await asyncio.sleep(2.5)

        # New event after reconnect should arrive
        await PilotChannel.publish_one("reconnect-test", b"after-reconnect", peer=worker_peer, timeout_secs=10)
        ev2 = await sub.recv(timeout=15)
        assert ev2.payload == b"after-reconnect"


async def test_auto_reconnect_disabled_by_default(worker_peer):
    """Without auto_reconnect, force-closing the conn breaks the channel as before."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotChannel

    sub = await PilotChannel.subscribe(
        "no-reconnect", peer=worker_peer, timeout_secs=15,
        # auto_reconnect defaults to False
    )
    try:
        await asyncio.sleep(0.3)
        sub._conn.force_close_sync()
        # The recv path will see EOF on the dead stream
        with pytest.raises((EOFError, Exception)):
            await sub.recv(timeout=5)
    finally:
        await sub.close()


async def test_close_cancels_reconnect_task(worker_peer):
    """close() must cancel the bg task — no leaked coroutines."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotChannel

    sub = await PilotChannel.subscribe(
        "cancel-test", peer=worker_peer, timeout_secs=15,
        auto_reconnect=True, backoff_secs=10.0,  # long backoff so we'd hang if not cancelled
    )
    bg = sub._reconnect_task
    assert bg is not None
    await sub.close()
    # Either done (cancelled) or about to be — give it a tick.
    await asyncio.sleep(0.05)
    assert bg.done()
