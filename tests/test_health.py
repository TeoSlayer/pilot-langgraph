"""Worker introspection (`_health`, `_handlers`) tests.

Pure-unit (no daemon): exercises the auto-registered handlers via the
WorkerServer dispatch path with fake streams.
"""
from __future__ import annotations

import json

import pytest

from pilot_langgraph._ipc import Addr
from pilot_langgraph.server import WorkerServer


class _FakeStream:
    def __init__(self, request: bytes, peer_addr: Addr, peer_port: int = 49000):
        self._frames_in = [request, b""]
        self.peer = (peer_addr, peer_port)
        self.frames_out: list[bytes] = []
        self.closed = False

    async def read(self, timeout: float | None = None) -> bytes:
        if self._frames_in:
            return self._frames_in.pop(0)
        return b""

    async def write(self, data: bytes) -> None:
        self.frames_out.append(data)

    async def close(self) -> None:
        self.closed = True


def _last_reply(fake) -> dict:
    """Parse the FINAL frame (which has done=true)."""
    for f in reversed(fake.frames_out):
        line = f.rstrip(b"\n").split(b"\n")[-1]
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise AssertionError(f"no JSON in {fake.frames_out!r}")


@pytest.mark.asyncio
async def test_health_reports_zero_calls_initially():
    s = WorkerServer(port=0)
    fake = _FakeStream(json.dumps({"node": "_health", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    reply = _last_reply(fake)
    assert reply["ok"] is True
    assert reply["result"]["ok"] is True
    assert reply["result"]["total_calls"] == 0  # _health itself counted AFTER reply
    assert reply["result"]["total_errors"] == 0
    assert reply["result"]["n_handlers"] >= 2  # at least _health + _handlers


@pytest.mark.asyncio
async def test_handlers_introspection_lists_all_registered():
    s = WorkerServer(port=0)
    s.register("greet", lambda p: {"hi": p})
    s.register("admin", lambda p: {"x": 1}, allow=[42, 100])
    fake = _FakeStream(json.dumps({"node": "_handlers", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    reply = _last_reply(fake)
    by_name = {h["name"]: h for h in reply["result"]["handlers"]}
    assert "greet" in by_name
    assert by_name["greet"]["allow"] is None
    assert by_name["admin"]["allow"] == [42, 100]
    assert "_health" in by_name and "_handlers" in by_name


@pytest.mark.asyncio
async def test_counters_increment_on_call_and_error():
    s = WorkerServer(port=0)
    s.register("ok",  lambda p: {"good": True})
    def boom(_): raise ValueError("nope")
    s.register("bad", boom)
    addr = Addr(network=0, node=1)

    # 2 ok, 1 bad
    for _ in range(2):
        f = _FakeStream(json.dumps({"node": "ok", "payload": None}).encode(), addr)
        await s._handle_one(f)
    f = _FakeStream(json.dumps({"node": "bad", "payload": None}).encode(), addr)
    await s._handle_one(f)

    # Now check stats via _handlers
    f_h = _FakeStream(json.dumps({"node": "_handlers", "payload": None}).encode(), addr)
    await s._handle_one(f_h)
    handlers = {h["name"]: h for h in _last_reply(f_h)["result"]["handlers"]}
    assert handlers["ok"]["calls"] == 2 and handlers["ok"]["errors"] == 0
    assert handlers["bad"]["calls"] == 1 and handlers["bad"]["errors"] == 1
    assert handlers["ok"]["p50_ms"] >= 0  # set, even if tiny
