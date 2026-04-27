"""Worker middleware chain tests."""
from __future__ import annotations

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
async def test_middleware_runs_around_handler():
    log: list[str] = []

    async def logger(payload, ctx, next_fn):
        log.append(f"before {ctx.handler_name}")
        result = await next_fn(payload, ctx)
        log.append(f"after {ctx.handler_name}")
        return result

    s = WorkerServer(port=0, middleware=[logger])
    s.register("hi", lambda p: {"echoed": p})
    fake = _FakeStream(json.dumps({"node": "hi", "payload": "world"}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    assert _last_reply(fake)["result"] == {"echoed": "world"}
    assert log == ["before hi", "after hi"]


@pytest.mark.asyncio
async def test_middleware_chain_order():
    """Outer (registered first) wraps inner (registered later)."""
    log: list[str] = []

    async def outer(p, c, next_fn):
        log.append("outer-pre")
        r = await next_fn(p, c)
        log.append("outer-post")
        return r

    async def inner(p, c, next_fn):
        log.append("inner-pre")
        r = await next_fn(p, c)
        log.append("inner-post")
        return r

    s = WorkerServer(port=0, middleware=[outer, inner])
    s.register("hi", lambda p: "ok")
    fake = _FakeStream(json.dumps({"node": "hi", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    assert log == ["outer-pre", "inner-pre", "inner-post", "outer-post"]


@pytest.mark.asyncio
async def test_middleware_can_short_circuit():
    """A middleware that returns without calling next_fn skips the handler."""
    handler_called = []

    async def auth(payload, ctx, next_fn):
        if not (isinstance(payload, dict) and payload.get("token") == "secret"):
            return {"denied": True}
        return await next_fn(payload, ctx)

    s = WorkerServer(port=0, middleware=[auth])
    s.register("op", lambda p: handler_called.append(p) or {"ok": True})

    addr = Addr(network=0, node=1)

    # Bad token — handler not called
    fake = _FakeStream(json.dumps({"node": "op", "payload": {"token": "wrong"}}).encode(), addr)
    await s._handle_one(fake)
    assert _last_reply(fake)["result"] == {"denied": True}
    assert handler_called == []

    # Good token — handler called
    fake = _FakeStream(json.dumps({"node": "op", "payload": {"token": "secret"}}).encode(), addr)
    await s._handle_one(fake)
    assert _last_reply(fake)["result"] == {"ok": True}
    assert handler_called == [{"token": "secret"}]


@pytest.mark.asyncio
async def test_middleware_can_transform_result():
    async def wrap(payload, ctx, next_fn):
        result = await next_fn(payload, ctx)
        return {"wrapped": result, "by": "middleware"}

    s = WorkerServer(port=0, middleware=[wrap])
    s.register("hi", lambda p: {"raw": p})
    fake = _FakeStream(json.dumps({"node": "hi", "payload": "x"}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    assert _last_reply(fake)["result"] == {"wrapped": {"raw": "x"}, "by": "middleware"}


@pytest.mark.asyncio
async def test_introspection_handlers_bypass_middleware():
    """_health and _handlers must NOT be wrapped — monitors should be lightweight."""
    log: list[str] = []

    async def chatty(p, c, next_fn):
        log.append(c.handler_name)
        return await next_fn(p, c)

    s = WorkerServer(port=0, middleware=[chatty])
    fake = _FakeStream(json.dumps({"node": "_health", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    assert _last_reply(fake)["ok"] is True
    assert log == []  # middleware never ran for _health


@pytest.mark.asyncio
async def test_use_method_appends_middleware():
    log: list[str] = []
    s = WorkerServer(port=0)
    s.register("op", lambda p: "ok")
    s.use(lambda p, c, next_fn: log.append("called") or next_fn(p, c))
    fake = _FakeStream(json.dumps({"node": "op", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    assert log == ["called"]


@pytest.mark.asyncio
async def test_middleware_works_with_ctx_handler():
    """A handler that takes ctx still gets ctx; middleware sees the same one."""
    seen_in_mw: list[int] = []
    seen_in_handler: list[int] = []

    async def mw(p, ctx, next_fn):
        seen_in_mw.append(ctx.caller_node_id)
        return await next_fn(p, ctx)

    def h(payload, ctx):
        seen_in_handler.append(ctx.caller_node_id)
        return {"caller": ctx.caller_node_id}

    s = WorkerServer(port=0, middleware=[mw])
    s.register("h", h)
    fake = _FakeStream(json.dumps({"node": "h", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=42))
    await s._handle_one(fake)
    assert seen_in_mw == [42]
    assert seen_in_handler == [42]
    assert _last_reply(fake)["result"] == {"caller": 42}
