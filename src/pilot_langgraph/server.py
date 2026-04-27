"""Async worker server for pilot-langgraph.

Listens on a Pilot port, accepts inbound streams, parses each as a JSON
`{"node": <name>, "payload": ...}` request, runs the registered handler, and
writes back `{"ok": true, "result": ...}` (or `{"ok": false, "error": ...}`)
on the same stream before closing it.

Two introspection handlers are auto-registered (always):

    `_health`     -> {ok, version, uptime_secs, in_flight, total_calls, total_errors}
    `_handlers`   -> {handlers: [{name, allow, calls, errors, p50_ms, p95_ms}, ...]}

These are open to any mutually-trusted peer (callers can be ACL'd by the
network operator via Pilot's trust model — workers just expose them).

Public API:
    pilot_handler("name", allow=[node_ids])  # decorator
    WorkerServer(port=...).register("name", fn, allow=[node_ids])
    await WorkerServer.serve_forever()

Sync entry point:
    python -m pilot_langgraph.worker --handlers <module>
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from . import _otel
from ._ipc import Addr
from .asyncio_client import PilotConnection, Stream

log = logging.getLogger(__name__)

DEFAULT_PORT = 5000


"""Middleware type signature.

A middleware is `async def mw(payload, ctx, next_fn) -> result` (or sync). It
gets the payload + Context + a call-next callable; it MUST `await next_fn(payload, ctx)`
once to actually invoke the handler (or any inner middleware), and may
transform either the payload going in or the result coming out.

Order of execution: outermost first (the first middleware in the list runs
its pre-`next_fn` code first and its post-`next_fn` code last).

Middleware applies to ALL handlers on the server EXCEPT introspection
(`_health`, `_handlers`) — those bypass to keep monitoring lightweight.
"""

Middleware = Callable[..., Any]  # (payload, ctx, next_fn) -> Any


@dataclass(frozen=True, slots=True)
class Context:
    """Optional second arg passed to handlers that declare it.

    A handler defined as `def fn(payload, ctx)` (or `async def`) receives a
    Context with metadata about the caller and request. Handlers defined as
    `def fn(payload)` are unchanged — they continue to receive only the payload.

    Attributes:
        caller_node_id: 32-bit Pilot node_id of the peer that invoked us.
        caller_addr: full Addr (network + node) of the peer.
        caller_port: ephemeral source port the peer dialed from.
        request_id: per-call uuid4, distinct from any client correlation.
        started_at: monotonic-clock seconds when the request began on the worker.
        handler_name: the registered handler name being dispatched.
    """
    caller_node_id: int
    caller_addr: Addr
    caller_port: int
    request_id: str
    started_at: float
    handler_name: str


Handler = Callable[..., Any]  # 1 or 2 positional args (payload[, ctx])


class _Stats:
    __slots__ = ("calls", "errors", "latencies_ms")

    def __init__(self) -> None:
        self.calls = 0
        self.errors = 0
        # Bounded ring of recent latencies for p50/p95.
        self.latencies_ms: deque[float] = deque(maxlen=256)

    def percentile(self, p: float) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_lat = sorted(self.latencies_ms)
        idx = max(0, min(len(sorted_lat) - 1, int(round(p * (len(sorted_lat) - 1)))))
        return round(sorted_lat[idx], 2)


class _NoopAsyncCtx:
    """Helper: an async context manager that does nothing.

    Lets `async with (sem or _NoopAsyncCtx())` work uniformly whether or not
    a semaphore is configured.
    """
    async def __aenter__(self): return None
    async def __aexit__(self, *_): return None


def _wants_context(fn: Handler) -> bool:
    """Heuristic: does this handler take a second positional `ctx` argument?

    Inspects the signature once at registration. Tolerates *args by treating
    it as "yes, two args is fine." Handlers declared as 1-arg keep the old
    payload-only signature.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    positional = [
        p for p in sig.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    has_var_positional = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
    return len(positional) >= 2 or has_var_positional


class _RateLimiter:
    """Sliding-window per-caller rate limiter.

    Per (handler, caller_node_id) keeps a deque of recent call timestamps.
    On each `consume()`, prunes timestamps older than `window_secs`, then
    checks deque length against `limit`. If under: append now, return True
    (allowed). If at/over: return False (denied).

    Memory bound: O(callers * limit) timestamps. For a handler with 100
    distinct callers each under a 1000-call/min limit, that's 100k floats —
    ~1.6MB. Bounded by the limit, not by call rate.
    """
    __slots__ = ("limit", "window_secs", "windows")

    def __init__(self, limit: int, window_secs: float):
        self.limit = limit
        self.window_secs = float(window_secs)
        self.windows: dict[int, deque[float]] = {}

    def consume(self, caller_node_id: int, now: float) -> bool:
        window = self.windows.get(caller_node_id)
        if window is None:
            window = deque()
            self.windows[caller_node_id] = window
        cutoff = now - self.window_secs
        while window and window[0] < cutoff:
            window.popleft()
        if len(window) >= self.limit:
            return False
        window.append(now)
        return True


class _Registration:
    __slots__ = ("fn", "allow", "stats", "wants_ctx", "timeout_secs",
                 "rate_limiter", "max_concurrent", "_semaphore",
                 "input_model", "output_model")

    def __init__(
        self,
        fn: Handler,
        allow: set[int] | None,
        timeout_secs: float | None = None,
        rate_per_caller: int | None = None,
        rate_window_secs: float = 60.0,
        max_concurrent: int | None = None,
        input_model: Any = None,
        output_model: Any = None,
    ):
        self.fn = fn
        # `None` means "open to any mutually-trusted peer" (backward compat).
        # An empty set means "deny all" (rare, but explicit).
        self.allow = allow
        self.stats = _Stats()
        self.wants_ctx = _wants_context(fn)
        # `None` means no per-handler timeout — caller-side timeout is the bound.
        # If set, a wall-clock timer cancels the handler at expiry and the
        # caller gets a PilotHandlerError("timeout after Xs").
        self.timeout_secs = timeout_secs
        # `None` disables rate limiting; otherwise N calls per window per caller.
        self.rate_limiter = _RateLimiter(rate_per_caller, rate_window_secs) if rate_per_caller else None
        # `None` allows unlimited concurrent invocations (default).
        # If set, a Semaphore caps it; busy callers get rate_limited error fast
        # rather than starving the worker.
        self.max_concurrent = max_concurrent
        self._semaphore: asyncio.Semaphore | None = None
        # Pydantic models for input/output validation. None = no validation.
        # On invalid input/output the caller gets a typed PilotHandlerError.
        self.input_model = input_model
        self.output_model = output_model

    def semaphore(self) -> asyncio.Semaphore | None:
        """Lazy-init the semaphore on the loop that first uses it."""
        if self.max_concurrent is None:
            return None
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrent)
        return self._semaphore

    def authorized(self, caller_node_id: int) -> bool:
        if self.allow is None:
            return True
        return caller_node_id in self.allow


_GLOBAL_HANDLERS: dict[str, _Registration] = {}


def pilot_handler(
    name: str,
    *,
    allow: list[int] | set[int] | None = None,
    timeout_secs: float | None = None,
    rate_per_caller: int | None = None,
    rate_window_secs: float = 60.0,
    max_concurrent: int | None = None,
    input_model: Any = None,
    output_model: Any = None,
):
    """Decorator to register a handler in the module-global registry.

    Args:
        name: handler name as called by `PilotRemoteRunnable(node=name, ...)`.
        allow: list/set of caller node_ids permitted to invoke this handler.
            `None` (default) means any mutually-trusted peer may call it.
            An empty list means "deny all" — useful for temporarily disabling
            a handler without removing it.
        timeout_secs: wall-clock budget for one invocation; `None` = no limit.
            On expiry the handler task is cancelled and the caller receives
            `PilotHandlerError("timeout after Xs")`.
        rate_per_caller: max invocations per caller per `rate_window_secs`;
            `None` (default) disables. Exceeding callers receive
            `PilotRateLimitError("rate limit: N/Ws")`.
        rate_window_secs: sliding window for rate limiting (default 60s).
        max_concurrent: cap on simultaneous invocations of this handler across
            ALL callers; `None` (default) is unlimited. Excess callers get
            `PilotRateLimitError("concurrency limit ...")` immediately rather
            than queuing — useful for resource-bound handlers (GPU, DB conns).
        input_model: optional pydantic `BaseModel` subclass. The wire payload
            is parsed through `model_validate(payload)`; the validated model
            is passed to the handler. Invalid input produces
            `PilotHandlerError("input validation: ...")` and the handler is
            never called.
        output_model: optional pydantic `BaseModel` subclass. The handler's
            return value is parsed through `model_validate(...)`; the wire
            ships `.model_dump()`. Invalid output produces
            `PilotHandlerError("output validation: ...")`.
    """
    allow_set = None if allow is None else set(allow)

    def _wrap(fn: Handler) -> Handler:
        _GLOBAL_HANDLERS[name] = _Registration(
            fn, allow_set,
            timeout_secs=timeout_secs,
            rate_per_caller=rate_per_caller,
            rate_window_secs=rate_window_secs,
            max_concurrent=max_concurrent,
            input_model=input_model,
            output_model=output_model,
        )
        return fn
    return _wrap


def consume_global_handlers() -> dict[str, _Registration]:
    out = dict(_GLOBAL_HANDLERS)
    _GLOBAL_HANDLERS.clear()
    return out


class WorkerServer:
    def __init__(
        self,
        *,
        port: int = DEFAULT_PORT,
        socket_path: str | None = None,
        drain_timeout_secs: float = 30.0,
        middleware: list[Middleware] | None = None,
    ):
        self.port = port
        self.socket_path = socket_path
        self.drain_timeout_secs = drain_timeout_secs
        self._handlers: dict[str, _Registration] = {}
        self._conn: PilotConnection | None = None
        self._inflight: set[asyncio.Task] = set()
        self._stopping = asyncio.Event()
        self._started_at: float | None = None
        self._total_calls = 0
        self._total_errors = 0
        # Middleware applies to all NON-introspection handlers.
        self._middleware: list[Middleware] = list(middleware) if middleware else []
        # Periodic background tasks: list of (interval_secs, async_callable, name).
        # Started in serve_forever, cancelled in _drain_inflight on shutdown.
        self._periodic: list[tuple[float, Callable[[], Any], str]] = []
        self._periodic_tasks: list[asyncio.Task] = []
        self._register_introspection()

    def add_periodic_task(
        self,
        interval_secs: float,
        fn: Callable[[], Any],
        *,
        name: str | None = None,
    ) -> None:
        """Schedule `fn()` to run every `interval_secs` while the server is up.

        `fn` may be sync or async. It runs after the first interval (no
        immediate first-fire). Exceptions are logged but never crash the
        loop — periodic tasks must not take down the worker.

        All scheduled tasks are cancelled cleanly on graceful shutdown
        (SIGTERM / `request_stop()`). Add tasks before `serve_forever()`.
        """
        if self._started_at is not None:
            raise RuntimeError("add_periodic_task must be called before serve_forever()")
        self._periodic.append((float(interval_secs), fn, name or fn.__name__))

    def use(self, mw: Middleware) -> None:
        """Append a middleware. Outer (registered first) wraps inner."""
        self._middleware.append(mw)

    def _register_introspection(self) -> None:
        """Auto-register `_health` and `_handlers` on every WorkerServer."""
        def _health(_payload: Any) -> dict:
            from . import __version__
            uptime = (time.monotonic() - self._started_at) if self._started_at else 0.0
            return {
                "ok": True,
                "version": __version__,
                "uptime_secs": round(uptime, 1),
                "in_flight": sum(1 for t in self._inflight if not t.done()),
                "total_calls": self._total_calls,
                "total_errors": self._total_errors,
                "n_handlers": len(self._handlers),
            }

        def _handlers_introspect(_payload: Any) -> dict:
            def _schema_of(model: Any) -> dict | None:
                if model is None:
                    return None
                # Pydantic v2 BaseModel has `model_json_schema()`
                fn = getattr(model, "model_json_schema", None)
                if fn is None:
                    return None
                try:
                    return fn()
                except Exception:
                    return None

            return {
                "handlers": [
                    {
                        "name": name,
                        "allow": sorted(reg.allow) if reg.allow is not None else None,
                        "calls": reg.stats.calls,
                        "errors": reg.stats.errors,
                        "p50_ms": reg.stats.percentile(0.50),
                        "p95_ms": reg.stats.percentile(0.95),
                        "timeout_secs": reg.timeout_secs,
                        "max_concurrent": reg.max_concurrent,
                        "input_schema": _schema_of(reg.input_model),
                        "output_schema": _schema_of(reg.output_model),
                    }
                    for name, reg in sorted(self._handlers.items())
                ]
            }

        self._handlers["_health"] = _Registration(_health, allow=None)
        self._handlers["_handlers"] = _Registration(_handlers_introspect, allow=None)

    def register(
        self,
        name: str,
        fn: Handler,
        *,
        allow: list[int] | set[int] | None = None,
        timeout_secs: float | None = None,
        rate_per_caller: int | None = None,
        rate_window_secs: float = 60.0,
        max_concurrent: int | None = None,
        input_model: Any = None,
        output_model: Any = None,
    ) -> None:
        """Register a handler.

        See :func:`pilot_handler` for full kwarg documentation.
        """
        self._handlers[name] = _Registration(
            fn,
            None if allow is None else set(allow),
            timeout_secs=timeout_secs,
            rate_per_caller=rate_per_caller,
            rate_window_secs=rate_window_secs,
            max_concurrent=max_concurrent,
            input_model=input_model,
            output_model=output_model,
        )

    def register_many(self, mapping: dict[str, Handler] | dict[str, "_Registration"]) -> None:
        for name, val in mapping.items():
            if isinstance(val, _Registration):
                self._handlers[name] = val
            else:
                self._handlers[name] = _Registration(val, None)

    def clear_user_handlers(self) -> None:
        """Drop every registered handler EXCEPT auto-registered introspection.

        Used by the `--reload` worker mode to swap a fresh handler set in
        place after the user's source file changes.
        """
        for name in list(self._handlers):
            if not name.startswith("_"):
                del self._handlers[name]

    @property
    def handlers(self) -> dict[str, Handler]:
        return {name: reg.fn for name, reg in self._handlers.items()}

    def authorize(self, name: str, allow: list[int] | set[int]) -> None:
        """Update the ACL for an already-registered handler."""
        if name not in self._handlers:
            raise KeyError(f"no such handler: {name}")
        self._handlers[name].allow = set(allow)

    def request_stop(self) -> None:
        """Signal the accept loop to stop after the next iteration."""
        self._stopping.set()

    async def serve_forever(self) -> None:
        """Run until `request_stop()` is called; drain in-flight handlers before returning."""
        self._conn = await PilotConnection.connect(self.socket_path)
        self._started_at = time.monotonic()
        info = await self._conn.info()
        log.info("worker: pilot daemon hostname=%s address=%s", info.get("hostname"), info.get("address"))
        listener = await self._conn.listen(self.port)
        log.info("worker: listening port=%d handlers=%s", self.port, sorted(self._handlers))

        # Spin up periodic background tasks bound to the server lifecycle.
        for interval, fn, name in self._periodic:
            self._periodic_tasks.append(
                asyncio.create_task(self._run_periodic(interval, fn, name),
                                    name=f"pilot-periodic-{name}")
            )

        accept_task: asyncio.Task | None = None
        try:
            while not self._stopping.is_set():
                accept_task = asyncio.create_task(listener.accept())
                stop_task = asyncio.create_task(self._stopping.wait())
                done, pending = await asyncio.wait(
                    {accept_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if stop_task in done and accept_task not in done:
                    accept_task.cancel()
                    try:
                        await accept_task
                    except (asyncio.CancelledError, Exception):
                        pass
                    break
                stop_task.cancel()
                try:
                    await stop_task
                except asyncio.CancelledError:
                    pass
                stream = accept_task.result()
                t = asyncio.create_task(
                    self._handle_one(stream), name=f"pilot-call-{stream.conn_id}"
                )
                self._inflight.add(t)
                t.add_done_callback(self._inflight.discard)
        finally:
            await listener.close()
            await self._drain_inflight()
            await self._conn.close()

    async def _run_periodic(self, interval: float, fn: Callable[[], Any], name: str) -> None:
        log.info("worker: periodic task `%s` scheduled every %.1fs", name, interval)
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=interval)
                return  # stopping was signalled — exit cleanly
            except asyncio.TimeoutError:
                pass  # interval elapsed, run the task
            try:
                if inspect.iscoroutinefunction(fn):
                    await fn()
                else:
                    fn()
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("worker: periodic task `%s` raised; continuing", name)

    async def _drain_inflight(self) -> None:
        # First, cancel periodic tasks — they'd never finish on their own.
        for t in self._periodic_tasks:
            t.cancel()
        if self._periodic_tasks:
            await asyncio.gather(*self._periodic_tasks, return_exceptions=True)
            self._periodic_tasks.clear()
        if not self._inflight:
            return
        log.info("worker: draining %d in-flight handler(s) (up to %.0fs)",
                 len(self._inflight), self.drain_timeout_secs)
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._inflight, return_exceptions=True),
                timeout=self.drain_timeout_secs,
            )
            log.info("worker: drain complete")
        except asyncio.TimeoutError:
            log.warning("worker: drain timeout — %d task(s) still running, cancelling",
                        sum(1 for t in self._inflight if not t.done()))
            for t in self._inflight:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*self._inflight, return_exceptions=True)

    async def _handle_one(self, stream: Stream) -> None:
        peer = stream.peer
        caller_node_id: int = peer[0].node if peer else 0
        try:
            buf = b""
            while True:
                chunk = await stream.read(timeout=30.0)
                if not chunk:
                    return
                buf += chunk
                try:
                    req = json.loads(buf)
                    break
                except json.JSONDecodeError:
                    continue

            node = req.get("node")
            payload = req.get("payload")
            # Generate request_id up-front so it's available to log + ctx.
            _request_id = str(uuid.uuid4())
            log.info(
                "worker: call node=%s caller_node_id=%d bytes=%d request_id=%s",
                node, caller_node_id, len(buf), _request_id,
                extra={"node": node, "caller_node_id": caller_node_id,
                       "bytes": len(buf), "request_id": _request_id},
            )

            reg = self._handlers.get(node)
            if reg is None:
                await self._send_chunk(stream, {
                    "ok": False,
                    "error_type": "handler_not_found",
                    "error": f"no handler for `{node}`",
                    "done": True,
                })
                return

            if not reg.authorized(caller_node_id):
                log.warning("worker: DENY node=%s caller_node_id=%d (not in ACL)",
                            node, caller_node_id)
                await self._send_chunk(stream, {
                    "ok": False,
                    "error_type": "unauthorized",
                    "error": f"unauthorized: caller node_id {caller_node_id} not allowed to invoke `{node}`",
                    "done": True,
                })
                return

            if reg.rate_limiter is not None:
                if not reg.rate_limiter.consume(caller_node_id, now=time.monotonic()):
                    rl = reg.rate_limiter
                    log.info("worker: RATE-LIMIT node=%s caller_node_id=%d (limit %d/%.0fs)",
                             node, caller_node_id, rl.limit, rl.window_secs)
                    await self._send_chunk(stream, {
                        "ok": False,
                        "error_type": "rate_limited",
                        "error": f"rate limit: {rl.limit}/{rl.window_secs:g}s for handler `{node}` per caller",
                        "done": True,
                    })
                    return

            sem = reg.semaphore()
            if sem is not None and sem.locked():
                log.info("worker: BUSY node=%s caller_node_id=%d (max_concurrent %d reached)",
                         node, caller_node_id, reg.max_concurrent)
                await self._send_chunk(stream, {
                    "ok": False,
                    "error_type": "rate_limited",
                    "error": f"concurrency limit: {reg.max_concurrent} simultaneous invocations of `{node}` already in flight",
                    "done": True,
                })
                return

            handler = reg.fn
            t_start = time.perf_counter()

            ctx = Context(
                caller_node_id=caller_node_id,
                caller_addr=peer[0] if peer else Addr(network=0, node=0),
                caller_port=peer[1] if peer else 0,
                request_id=_request_id,
                started_at=t_start,
                handler_name=node,
            )
            ctx_args: tuple = (ctx,) if reg.wants_ctx else ()

            timeout = reg.timeout_secs
            traceparent = req.get("traceparent") if isinstance(req, dict) else None

            async def _invoke_handler(p, _c):
                """Adapter: invoke the registered handler with the right number of args.

                Wraps the call with optional pydantic input/output validation.
                Streaming handlers (async generators) are NOT validated for
                output (each yielded chunk would need its own validation pass —
                deferred for v1).
                """
                # Input validation: parse the wire payload through the model.
                handler_p = p
                if reg.input_model is not None:
                    try:
                        handler_p = reg.input_model.model_validate(p)
                    except Exception as e:
                        raise ValueError(f"input validation: {e}") from e

                if reg.wants_ctx:
                    if inspect.isasyncgenfunction(handler):
                        return handler(handler_p, _c)
                    if inspect.iscoroutinefunction(handler):
                        result = await handler(handler_p, _c)
                    else:
                        result = handler(handler_p, _c)
                else:
                    if inspect.isasyncgenfunction(handler):
                        return handler(handler_p)
                    if inspect.iscoroutinefunction(handler):
                        result = await handler(handler_p)
                    else:
                        result = handler(handler_p)

                # Output validation: parse the result through the model and
                # ship its dumped dict over the wire.
                if reg.output_model is not None:
                    try:
                        return reg.output_model.model_validate(result).model_dump()
                    except Exception as e:
                        raise ValueError(f"output validation: {e}") from e
                return result

            # Compose the middleware chain. Skip middleware for introspection handlers.
            mws = self._middleware if not node.startswith("_") else []

            async def _through_middleware(p, c):
                idx = 0
                async def _next(_p, _c):
                    nonlocal idx
                    if idx >= len(mws):
                        return await _invoke_handler(_p, _c)
                    mw = mws[idx]
                    idx += 1
                    if inspect.iscoroutinefunction(mw):
                        return await mw(_p, _c, _next)
                    return mw(_p, _c, _next)
                return await _next(p, c)

            async def _drive() -> None:
                if inspect.isasyncgenfunction(handler) and not mws:
                    # Fast path: streaming handler with no middleware.
                    async for chunk_result in handler(payload, *ctx_args):
                        await self._send_chunk(stream, {"ok": True, "result": chunk_result, "done": False})
                    await self._send_chunk(stream, {"ok": True, "result": None, "done": True})
                else:
                    # Streaming + middleware is supported by collecting the
                    # generator into a list inside the middleware chain — middleware
                    # can post-process the entire stream as a unit. For simple
                    # request/reply this is the same as a single result.
                    result = await _through_middleware(payload, ctx)
                    if inspect.isasyncgen(result):
                        async for chunk_result in result:
                            await self._send_chunk(stream, {"ok": True, "result": chunk_result, "done": False})
                        await self._send_chunk(stream, {"ok": True, "result": None, "done": True})
                    else:
                        await self._send_chunk(stream, {"ok": True, "result": result, "done": True})

            try:
                # Acquire the concurrency semaphore (if configured) just for
                # the dispatch window. We've already short-circuited the
                # "would block" case above, so this is non-blocking in
                # practice — the locked() check + acquire is racy but safe:
                # a second waiter just gets in line briefly.
                async with (sem if sem is not None else _NoopAsyncCtx()):
                    with _otel.server_span(node, caller_node_id, traceparent):
                        if timeout is not None:
                            await asyncio.wait_for(_drive(), timeout=timeout)
                        else:
                            await _drive()
                self._total_calls += 1
                reg.stats.calls += 1
                reg.stats.latencies_ms.append((time.perf_counter() - t_start) * 1000.0)
            except asyncio.TimeoutError:
                log.warning("worker: handler `%s` timed out after %ss", node, timeout)
                self._total_calls += 1
                self._total_errors += 1
                reg.stats.calls += 1
                reg.stats.errors += 1
                reg.stats.latencies_ms.append((time.perf_counter() - t_start) * 1000.0)
                await self._send_chunk(stream, {
                    "ok": False,
                    "error_type": "handler_error",
                    "error": f"timeout after {timeout}s",
                    "done": True,
                })
            except Exception as e:
                log.exception("worker: handler raised")
                self._total_calls += 1
                self._total_errors += 1
                reg.stats.calls += 1
                reg.stats.errors += 1
                reg.stats.latencies_ms.append((time.perf_counter() - t_start) * 1000.0)
                await self._send_chunk(stream, {
                    "ok": False,
                    "error_type": "handler_error",
                    "error": f"{type(e).__name__}: {e}",
                    "done": True,
                })
        finally:
            # Brief yield so the daemon can forward any buffered cmdSend frames
            # over the wire before our cmdClose tells it to tear down the
            # connection. Without this, replies large enough to fragment can
            # be truncated when close races the send. 150ms is comfortable for
            # WAN-bound replies; with a no-op or single-frame reply the caller
            # has already received and processed the result well before we
            # actually close, so the 'latency' is post-completion.
            await asyncio.sleep(0.15)
            await stream.close()

    @staticmethod
    async def _send_chunk(stream: Stream, frame: dict) -> None:
        # Newline delimiter so the caller can split on it without a length prefix.
        wire = json.dumps(frame, separators=(",", ":"), default=str).encode() + b"\n"
        await stream.write(wire)
