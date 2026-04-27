"""Schema introspection: _handlers exposes pydantic input/output JSON schemas."""
from __future__ import annotations

import json

import pytest
from pydantic import BaseModel, Field

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


class GreetIn(BaseModel):
    name: str = Field(description="who to greet")
    times: int = 1


class GreetOut(BaseModel):
    message: str


@pytest.mark.asyncio
async def test_handlers_introspection_exposes_input_schema():
    s = WorkerServer(port=0)
    s.register("greet", lambda p: {"message": "hi"}, input_model=GreetIn)
    fake = _FakeStream(json.dumps({"node": "_handlers", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    handlers = {h["name"]: h for h in _last_reply(fake)["result"]["handlers"]}
    schema = handlers["greet"]["input_schema"]
    assert schema is not None
    assert schema["title"] == "GreetIn"
    assert "name" in schema["properties"]
    assert "times" in schema["properties"]
    assert handlers["greet"]["output_schema"] is None  # only input_model set


@pytest.mark.asyncio
async def test_handlers_introspection_exposes_output_schema():
    s = WorkerServer(port=0)
    s.register("greet", lambda p: {"message": "hi"}, output_model=GreetOut)
    fake = _FakeStream(json.dumps({"node": "_handlers", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    handlers = {h["name"]: h for h in _last_reply(fake)["result"]["handlers"]}
    out = handlers["greet"]["output_schema"]
    assert out is not None
    assert out["title"] == "GreetOut"
    assert "message" in out["properties"]


@pytest.mark.asyncio
async def test_handlers_introspection_includes_timeout_and_concurrency():
    """The new fields surface for runtime API discovery."""
    s = WorkerServer(port=0)
    s.register("op", lambda p: None, timeout_secs=15.0, max_concurrent=4)
    fake = _FakeStream(json.dumps({"node": "_handlers", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    handlers = {h["name"]: h for h in _last_reply(fake)["result"]["handlers"]}
    assert handlers["op"]["timeout_secs"] == 15.0
    assert handlers["op"]["max_concurrent"] == 4


@pytest.mark.asyncio
async def test_handlers_without_models_have_null_schemas():
    s = WorkerServer(port=0)
    s.register("loose", lambda p: p)
    fake = _FakeStream(json.dumps({"node": "_handlers", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    handlers = {h["name"]: h for h in _last_reply(fake)["result"]["handlers"]}
    assert handlers["loose"]["input_schema"] is None
    assert handlers["loose"]["output_schema"] is None
