"""Pydantic input/output validation on handlers."""
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
    name: str = Field(min_length=1)
    times: int = 1


class GreetOut(BaseModel):
    greeting: str
    repeated: int


@pytest.mark.asyncio
async def test_input_validation_passes_typed_model_to_handler():
    s = WorkerServer(port=0)
    captured: list = []

    def greet(payload: GreetIn) -> dict:
        captured.append(payload)
        return {"greeting": f"hi {payload.name}", "repeated": payload.times}

    s.register("greet", greet, input_model=GreetIn)
    fake = _FakeStream(json.dumps({"node": "greet", "payload": {"name": "Pilot", "times": 3}}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    assert _last_reply(fake)["ok"] is True
    assert isinstance(captured[0], GreetIn)
    assert captured[0].name == "Pilot"


@pytest.mark.asyncio
async def test_input_validation_rejects_bad_payload():
    s = WorkerServer(port=0)

    def greet(payload: GreetIn) -> dict:
        return {"greeting": "should not run"}

    s.register("greet", greet, input_model=GreetIn)
    # Missing required `name`
    fake = _FakeStream(json.dumps({"node": "greet", "payload": {"times": 5}}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    reply = _last_reply(fake)
    assert reply["ok"] is False
    assert reply["error_type"] == "handler_error"
    assert "input validation" in reply["error"]


@pytest.mark.asyncio
async def test_output_validation_passes_dump_over_wire():
    s = WorkerServer(port=0)

    def greet(payload):
        # Returns a dict that matches GreetOut
        return {"greeting": f"hi {payload['name']}", "repeated": 1}

    s.register("greet", greet, output_model=GreetOut)
    fake = _FakeStream(json.dumps({"node": "greet", "payload": {"name": "x"}}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    reply = _last_reply(fake)
    assert reply["ok"] is True
    assert reply["result"] == {"greeting": "hi x", "repeated": 1}


@pytest.mark.asyncio
async def test_output_validation_rejects_bad_handler_return():
    s = WorkerServer(port=0)

    def greet(payload):
        return {"oops": "wrong shape"}  # missing required fields

    s.register("greet", greet, output_model=GreetOut)
    fake = _FakeStream(json.dumps({"node": "greet", "payload": None}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    reply = _last_reply(fake)
    assert reply["ok"] is False
    assert reply["error_type"] == "handler_error"
    assert "output validation" in reply["error"]


@pytest.mark.asyncio
async def test_no_validation_by_default():
    s = WorkerServer(port=0)
    s.register("loose", lambda p: p)
    fake = _FakeStream(json.dumps({"node": "loose", "payload": {"anything": "goes"}}).encode(),
                       peer_addr=Addr(network=0, node=1))
    await s._handle_one(fake)
    assert _last_reply(fake)["result"] == {"anything": "goes"}


def test_pilot_handler_decorator_carries_models():
    from pilot_langgraph.server import consume_global_handlers, pilot_handler

    consume_global_handlers()  # clear

    @pilot_handler("greet", input_model=GreetIn, output_model=GreetOut)
    def greet(p: GreetIn): return {"greeting": "x", "repeated": 1}

    handlers = consume_global_handlers()
    assert handlers["greet"].input_model is GreetIn
    assert handlers["greet"].output_model is GreetOut
