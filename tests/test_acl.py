"""Per-handler ACL tests.

Pure-unit (no daemon) — exercise WorkerServer's authorization logic by
faking the inbound stream peer. Live ACL test exercised via worker_handlers
(would require deploying with restricted handlers; covered by the unit
behaviour here).
"""
from __future__ import annotations

import json

import pytest

from pilot_langgraph._ipc import Addr
from pilot_langgraph.server import WorkerServer, pilot_handler, consume_global_handlers


class _FakeStream:
    """Behaves like asyncio_client.Stream for WorkerServer._handle_one."""

    def __init__(self, request: bytes, peer_addr: Addr, peer_port: int = 49000):
        self._frames_in = [request, b""]  # request + EOF
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


@pytest.mark.asyncio
async def test_open_handler_accepts_any_caller():
    s = WorkerServer(port=0)
    s.register("greet", lambda payload: {"hi": payload})
    fake = _FakeStream(json.dumps({"node": "greet", "payload": "world"}).encode(),
                       peer_addr=Addr(network=0, node=999))
    await s._handle_one(fake)
    reply = json.loads(fake.frames_out[0])
    assert reply["ok"] is True
    assert reply["result"] == {"hi": "world"}


@pytest.mark.asyncio
async def test_acl_denies_caller_not_in_allowlist():
    s = WorkerServer(port=0)
    s.register("admin_op", lambda p: {"done": True}, allow=[42])
    fake = _FakeStream(json.dumps({"node": "admin_op", "payload": {}}).encode(),
                       peer_addr=Addr(network=0, node=999))  # caller != 42
    await s._handle_one(fake)
    reply = json.loads(fake.frames_out[0])
    assert reply["ok"] is False
    assert "unauthorized" in reply["error"]
    assert "999" in reply["error"]


@pytest.mark.asyncio
async def test_acl_allows_caller_in_allowlist():
    s = WorkerServer(port=0)
    called = []
    s.register("admin_op", lambda p: called.append(p) or {"done": True}, allow=[42, 100])
    fake = _FakeStream(json.dumps({"node": "admin_op", "payload": {"x": 1}}).encode(),
                       peer_addr=Addr(network=0, node=42))
    await s._handle_one(fake)
    reply = json.loads(fake.frames_out[0])
    assert reply["ok"] is True
    assert called == [{"x": 1}]


@pytest.mark.asyncio
async def test_authorize_method_updates_acl():
    s = WorkerServer(port=0)
    s.register("op", lambda p: {})
    s.authorize("op", allow=[100])
    fake_denied = _FakeStream(json.dumps({"node": "op", "payload": {}}).encode(),
                              peer_addr=Addr(network=0, node=42))
    await s._handle_one(fake_denied)
    assert json.loads(fake_denied.frames_out[0])["ok"] is False

    fake_allowed = _FakeStream(json.dumps({"node": "op", "payload": {}}).encode(),
                               peer_addr=Addr(network=0, node=100))
    await s._handle_one(fake_allowed)
    assert json.loads(fake_allowed.frames_out[0])["ok"] is True


def test_pilot_handler_decorator_carries_allow():
    consume_global_handlers()  # clear

    @pilot_handler("op_open")
    def open_op(p): return p

    @pilot_handler("op_locked", allow=[7, 8])
    def locked_op(p): return p

    handlers = consume_global_handlers()
    assert handlers["op_open"].allow is None
    assert handlers["op_locked"].allow == {7, 8}
