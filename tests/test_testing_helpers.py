"""Tests for the `pilot_langgraph.testing` helpers."""
from __future__ import annotations

import pytest
from pydantic import BaseModel

from pilot_langgraph.testing import collect_stream, invoke_handler, make_context


class TestMakeContext:
    def test_defaults(self):
        ctx = make_context()
        assert ctx.caller_node_id == 1
        assert ctx.caller_addr.node == 1
        assert ctx.handler_name == "test-handler"
        assert isinstance(ctx.request_id, str)

    def test_overrides(self):
        ctx = make_context(caller_node_id=42, handler_name="my-handler")
        assert ctx.caller_node_id == 42
        assert ctx.caller_addr.node == 42
        assert ctx.handler_name == "my-handler"


@pytest.mark.asyncio
async def test_invoke_sync_handler():
    def fn(payload):
        return {"echo": payload}
    out = await invoke_handler(fn, {"x": 1})
    assert out == {"echo": {"x": 1}}


@pytest.mark.asyncio
async def test_invoke_async_handler():
    async def fn(payload):
        return {"echo": payload}
    out = await invoke_handler(fn, {"x": 1})
    assert out == {"echo": {"x": 1}}


@pytest.mark.asyncio
async def test_invoke_handler_with_ctx():
    seen = []
    def fn(payload, ctx):
        seen.append(ctx.caller_node_id)
        return ctx.handler_name
    out = await invoke_handler(fn, None, ctx=make_context(caller_node_id=99, handler_name="h"))
    assert seen == [99]
    assert out == "h"


@pytest.mark.asyncio
async def test_invoke_handler_synthesizes_ctx_when_missing():
    """If handler wants ctx but caller didn't pass one, we make a default."""
    def fn(payload, ctx):
        return ctx.caller_node_id
    out = await invoke_handler(fn, None)
    assert out == 1  # default make_context()


@pytest.mark.asyncio
async def test_invoke_async_gen_collects_chunks():
    async def gen(payload):
        for i in range(3):
            yield {"i": i}
    out = await collect_stream(gen, None)
    assert out == [{"i": 0}, {"i": 1}, {"i": 2}]


class _In(BaseModel):
    name: str


class _Out(BaseModel):
    greeting: str


@pytest.mark.asyncio
async def test_invoke_with_input_validation():
    def fn(payload: _In):
        return {"greeting": f"hi {payload.name}"}
    out = await invoke_handler(fn, {"name": "Pilot"}, input_model=_In)
    assert out == {"greeting": "hi Pilot"}


@pytest.mark.asyncio
async def test_invoke_with_input_validation_rejects():
    def fn(payload: _In):
        return None
    with pytest.raises(ValueError, match="input validation"):
        await invoke_handler(fn, {"wrong_field": "x"}, input_model=_In)


@pytest.mark.asyncio
async def test_invoke_with_output_validation_dumps():
    def fn(payload):
        return {"greeting": "hi"}
    out = await invoke_handler(fn, None, output_model=_Out)
    assert out == {"greeting": "hi"}


@pytest.mark.asyncio
async def test_invoke_with_output_validation_rejects():
    def fn(payload):
        return {"oops": "wrong"}
    with pytest.raises(ValueError, match="output validation"):
        await invoke_handler(fn, None, output_model=_Out)
