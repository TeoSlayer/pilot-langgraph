"""Per-handler timeout enforcement."""
from __future__ import annotations

import asyncio
import json
import time

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
async def test_handler_timeout_cancels_and_replies_error():
    s = WorkerServer(port=0)

    async def slow(_payload):
        await asyncio.sleep(5.0)  # would normally take 5s
        return {"slept": True}

    s.register("slow", slow, timeout_secs=0.2)
    fake = _FakeStream(json.dumps({"node": "slow", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    t0 = time.monotonic()
    await s._handle_one(fake)
    elapsed = time.monotonic() - t0
    # Should return very close to the timeout, NOT the handler's natural 5s.
    assert elapsed < 1.0
    reply = _last_reply(fake)
    assert reply["ok"] is False
    assert reply["error_type"] == "handler_error"
    assert "timeout after 0.2s" in reply["error"]


@pytest.mark.asyncio
async def test_no_timeout_allows_handler_to_complete():
    s = WorkerServer(port=0)

    async def quick(_payload):
        await asyncio.sleep(0.05)
        return {"done": True}

    s.register("quick", quick)  # no timeout
    fake = _FakeStream(json.dumps({"node": "quick", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    reply = _last_reply(fake)
    assert reply["ok"] is True
    assert reply["result"] == {"done": True}


@pytest.mark.asyncio
async def test_timeout_recorded_as_error_in_stats():
    s = WorkerServer(port=0)

    async def slow(_payload):
        await asyncio.sleep(2.0)

    s.register("slow", slow, timeout_secs=0.1)
    addr = Addr(network=0, node=1)
    for _ in range(2):
        f = _FakeStream(json.dumps({"node": "slow", "payload": None}).encode(), addr)
        await s._handle_one(f)
    f_h = _FakeStream(json.dumps({"node": "_handlers", "payload": None}).encode(), addr)
    await s._handle_one(f_h)
    handlers = {h["name"]: h for h in _last_reply(f_h)["result"]["handlers"]}
    assert handlers["slow"]["calls"] == 2
    assert handlers["slow"]["errors"] == 2


def test_pilot_handler_decorator_carries_timeout():
    from pilot_langgraph.server import consume_global_handlers, pilot_handler

    consume_global_handlers()  # clear

    @pilot_handler("op", timeout_secs=1.5)
    def op(p): return p

    handlers = consume_global_handlers()
    assert handlers["op"].timeout_secs == 1.5
