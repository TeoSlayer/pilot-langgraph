"""Tests against a live local pilot daemon.

Skipped if no daemon is running on `$PILOT_SOCKET`. Spin one up with:
    pilotctl daemon start --hostname pilot-langgraph-dev --email you@example.com
"""
from __future__ import annotations

import asyncio


from .conftest import requires_daemon


pytestmark = [requires_daemon]


async def test_info(conn):
    info = await conn.info()
    assert "address" in info
    assert "node_id" in info


async def test_health(conn):
    h = await conn.health()
    assert h.get("status") == "ok"


async def test_resolve_self_returns_address(conn):
    info = await conn.info()
    assert ":" in info["address"]


async def test_echo_via_dial(conn, remote_peer):
    """End-to-end stream round-trip against the public echo peer."""
    info = await conn.resolve_hostname(remote_peer)
    from pilot_langgraph._ipc import Addr
    peer = Addr.parse(info["address"])
    payload = b"plugin-test-" + b"x" * 32

    stream = await conn.dial(peer, port=7, timeout=30)
    try:
        await stream.write(payload)
        chunk = await stream.read(timeout=15)
        assert chunk == payload
    finally:
        await stream.close()


async def test_concurrent_dials_dont_cross(conn, remote_peer):
    """Two simultaneous dials must each get their own reply, not crossed."""
    info = await conn.resolve_hostname(remote_peer)
    from pilot_langgraph._ipc import Addr
    peer = Addr.parse(info["address"])

    async def one(label: bytes) -> bytes:
        s = await conn.dial(peer, port=7, timeout=30)
        try:
            await s.write(label)
            return await s.read(timeout=15)
        finally:
            await s.close()

    a, b = await asyncio.gather(one(b"ping-A"), one(b"ping-B"))
    assert a == b"ping-A"
    assert b == b"ping-B"
