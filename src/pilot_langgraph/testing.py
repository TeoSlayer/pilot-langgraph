"""Helpers for writing unit tests against your own handlers.

Lets users test handler functions without standing up a real Pilot daemon
or worker process. Drives the same dispatch logic the real `WorkerServer`
does (pydantic validation, Context, sync/async/async-gen detection) but in
isolation.

Usage:

    from pilot_langgraph.testing import invoke_handler, make_context

    async def my_handler(payload, ctx):
        return {"got": payload, "from": ctx.caller_node_id}

    async def test_my_handler():
        ctx = make_context(caller_node_id=42)
        result = await invoke_handler(my_handler, {"x": 1}, ctx=ctx)
        assert result == {"got": {"x": 1}, "from": 42}

For a more integrated test, use a `WorkerServer()` instance directly with a
fake stream — see `tests/test_acl.py` for examples.
"""
from __future__ import annotations

import inspect
import time
import uuid
from typing import Any

from ._ipc import Addr
from .server import Context, _wants_context


def make_context(
    *,
    caller_node_id: int = 1,
    caller_addr: Addr | None = None,
    caller_port: int = 49000,
    request_id: str | None = None,
    started_at: float | None = None,
    handler_name: str = "test-handler",
) -> Context:
    """Build a `Context` for tests.

    All args are optional; sensible defaults make the simplest case
    `make_context()` for handlers that just need *some* ctx.
    """
    return Context(
        caller_node_id=caller_node_id,
        caller_addr=caller_addr or Addr(network=0, node=caller_node_id),
        caller_port=caller_port,
        request_id=request_id or str(uuid.uuid4()),
        started_at=started_at if started_at is not None else time.monotonic(),
        handler_name=handler_name,
    )


async def invoke_handler(
    handler: Any,
    payload: Any,
    *,
    ctx: Context | None = None,
    input_model: Any = None,
    output_model: Any = None,
) -> Any:
    """Invoke `handler` with the right calling convention for tests.

    Detects whether the handler wants a Context (2-arg signature), runs sync
    or coroutine or async-generator handlers correctly, and applies optional
    pydantic input/output validation. Async generators are collected into a
    list of yielded values.

    Returns the handler's result (or list of yielded chunks for async-gen).
    Raises whatever the handler raises (or a `ValueError` for validation
    failures, matching the worker's behavior).
    """
    handler_p = payload
    if input_model is not None:
        try:
            handler_p = input_model.model_validate(payload)
        except Exception as e:
            raise ValueError(f"input validation: {e}") from e

    wants_ctx = _wants_context(handler)
    if wants_ctx and ctx is None:
        ctx = make_context()
    args = (handler_p, ctx) if wants_ctx else (handler_p,)

    if inspect.isasyncgenfunction(handler):
        chunks = []
        async for c in handler(*args):
            chunks.append(c)
        result: Any = chunks
    elif inspect.iscoroutinefunction(handler):
        result = await handler(*args)
    else:
        result = handler(*args)

    if output_model is not None and not isinstance(result, list):
        try:
            return output_model.model_validate(result).model_dump()
        except Exception as e:
            raise ValueError(f"output validation: {e}") from e
    return result


async def collect_stream(handler: Any, payload: Any, *, ctx: Context | None = None) -> list:
    """Convenience: drain an async-gen handler into a list of yielded chunks."""
    result = await invoke_handler(handler, payload, ctx=ctx)
    if not isinstance(result, list):
        raise TypeError(f"{handler.__name__} is not an async generator; got {type(result).__name__}")
    return result
