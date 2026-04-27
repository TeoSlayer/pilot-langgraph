"""Per-handler concurrency cap tests."""
from __future__ import annotations

import asyncio
import json

import pytest

from pilot_langgraph._ipc import Addr
from pilot_langgraph.server import WorkerServer


class _FakeStream:
    def __init__(self, request: bytes, peer_addr: Addr):
        self._frames_in = [request, b""]
        self.peer = (peer_addr, 49000)
        self.frames_out: list[bytes] = []

    async def read(self, timeout=None) -> bytes:
        return self._frames_in.pop(0) if self._frames_in else b""

    async def write(self, data: bytes) -> None:
        self.frames_out.append(data)

    async def close(self) -> None:
        pass


def _last_reply(fake) -> dict:
    line = fake.frames_out[-1].rstrip(b"\n").split(b"\n")[-1]
    return json.loads(line)


@pytest.mark.asyncio
async def test_no_cap_allows_unbounded_concurrency():
    s = WorkerServer(port=0)

    async def slow(_p):
        await asyncio.sleep(0.05)
        return {"ok": True}

    s.register("slow", slow)  # no max_concurrent
    addr = Addr(network=0, node=1)

    async def one():
        f = _FakeStream(json.dumps({"node": "slow", "payload": None}).encode(), addr)
        await s._handle_one(f)
        return _last_reply(f)

    results = await asyncio.gather(*(one() for _ in range(10)))
    assert all(r["ok"] for r in results)


@pytest.mark.asyncio
async def test_cap_rejects_excess_callers_immediately():
    s = WorkerServer(port=0)
    inflight = 0
    peak = 0

    async def slow(_p):
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        try:
            await asyncio.sleep(0.1)
            return {"ok": True}
        finally:
            inflight -= 1

    s.register("slow", slow, max_concurrent=2)
    addr = Addr(network=0, node=1)

    async def one():
        f = _FakeStream(json.dumps({"node": "slow", "payload": None}).encode(), addr)
        await s._handle_one(f)
        return _last_reply(f)

    # Fire 5 in parallel; only 2 should get past the gate.
    results = await asyncio.gather(*(one() for _ in range(5)))
    ok_count = sum(1 for r in results if r["ok"])
    busy_count = sum(
        1 for r in results
        if not r["ok"] and r.get("error_type") == "rate_limited"
        and "concurrency" in r["error"]
    )
    # We launched 5 essentially concurrently; the first 2 took the slots,
    # the other 3 should have been rejected.
    assert ok_count == 2
    assert busy_count == 3
    assert peak == 2  # never exceeded the cap


@pytest.mark.asyncio
async def test_cap_releases_after_handler_finishes():
    """After the first batch completes, a fresh batch can run."""
    s = WorkerServer(port=0)

    async def slow(_p):
        await asyncio.sleep(0.05)
        return {"ok": True}

    s.register("slow", slow, max_concurrent=1)
    addr = Addr(network=0, node=1)

    async def one():
        f = _FakeStream(json.dumps({"node": "slow", "payload": None}).encode(), addr)
        await s._handle_one(f)
        return _last_reply(f)

    # Sequential — each should succeed
    for _ in range(3):
        r = await one()
        assert r["ok"] is True


def test_pilot_handler_decorator_carries_max_concurrent():
    from pilot_langgraph.server import consume_global_handlers, pilot_handler

    consume_global_handlers()  # clear

    @pilot_handler("gpu", max_concurrent=4)
    def gpu(p): return p

    handlers = consume_global_handlers()
    assert handlers["gpu"].max_concurrent == 4
