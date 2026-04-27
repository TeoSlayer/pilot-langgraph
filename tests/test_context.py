"""Handler Context tests.

Verifies single-arg handlers stay single-arg (backward compat) and that
two-arg handlers receive a populated Context.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from pilot_langgraph._ipc import Addr
from pilot_langgraph.server import Context, WorkerServer, _wants_context


class _FakeStream:
    def __init__(self, request: bytes, peer_addr: Addr, peer_port: int = 49000):
        self._frames_in = [request, b""]
        self.peer = (peer_addr, peer_port)
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


class TestWantsContext:
    def test_single_arg_doesnt_want_ctx(self):
        assert _wants_context(lambda payload: None) is False

    def test_two_arg_wants_ctx(self):
        assert _wants_context(lambda payload, ctx: None) is True

    def test_def_with_two_positional(self):
        def fn(payload, ctx): return None
        assert _wants_context(fn) is True

    def test_var_args_wants_ctx(self):
        def fn(*args): return None
        assert _wants_context(fn) is True


@pytest.mark.asyncio
async def test_legacy_handler_unchanged():
    """Single-arg handlers receive only the payload — no breakage."""
    s = WorkerServer(port=0)
    captured: list[Any] = []
    s.register("legacy", lambda p: captured.append(p) or {"ok": 1})
    fake = _FakeStream(json.dumps({"node": "legacy", "payload": {"hi": "there"}}).encode(),
                       peer_addr=Addr(network=0, node=42))
    await s._handle_one(fake)
    assert captured == [{"hi": "there"}]
    assert _last_reply(fake)["ok"] is True


@pytest.mark.asyncio
async def test_handler_with_ctx_receives_metadata():
    s = WorkerServer(port=0)
    captured: list[Context] = []

    def with_ctx(payload, ctx):
        captured.append(ctx)
        return {"caller": ctx.caller_node_id, "name": ctx.handler_name}

    s.register("introspect", with_ctx)
    fake = _FakeStream(json.dumps({"node": "introspect", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=12345),
                       peer_port=51000)
    await s._handle_one(fake)
    reply = _last_reply(fake)
    assert reply["ok"] is True
    assert reply["result"]["caller"] == 12345
    assert reply["result"]["name"] == "introspect"
    assert len(captured) == 1
    ctx = captured[0]
    assert ctx.caller_node_id == 12345
    assert ctx.caller_addr.node == 12345
    assert ctx.caller_port == 51000
    assert ctx.handler_name == "introspect"
    assert isinstance(ctx.request_id, str) and len(ctx.request_id) >= 32  # uuid4


@pytest.mark.asyncio
async def test_async_handler_with_ctx():
    s = WorkerServer(port=0)
    seen_calls: list[int] = []

    async def fn(payload, ctx):
        seen_calls.append(ctx.caller_node_id)
        return {"got": payload, "from": ctx.caller_node_id}

    s.register("async_ctx", fn)
    for caller in (1, 2, 3):
        fake = _FakeStream(json.dumps({"node": "async_ctx", "payload": "p"}).encode(),
                           peer_addr=Addr(network=0, node=caller))
        await s._handle_one(fake)
    assert seen_calls == [1, 2, 3]


@pytest.mark.asyncio
async def test_async_gen_handler_with_ctx():
    s = WorkerServer(port=0)

    async def gen(payload, ctx):
        for i in range(3):
            yield {"i": i, "from": ctx.caller_node_id}

    s.register("gen_ctx", gen)
    fake = _FakeStream(json.dumps({"node": "gen_ctx", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=77))
    await s._handle_one(fake)
    # 4 frames written: 3 chunks + 1 done
    assert len(fake.frames_out) == 4
    parsed = [json.loads(f.rstrip(b"\n")) for f in fake.frames_out]
    chunks = [p for p in parsed if not p.get("done")]
    assert [c["result"]["i"] for c in chunks] == [0, 1, 2]
    assert all(c["result"]["from"] == 77 for c in chunks)
