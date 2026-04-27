"""LangChain Runnables that execute on a remote Pilot peer.

Async-first, stream-based. Each invocation opens its own pilot stream, writes
the request as JSON, reads the JSON reply on the same stream, closes. Many
calls run concurrently — each gets a fresh connID at the daemon level, so
there's no per-port multiplexing to manage.

Usage in a LangGraph node:

    from pilot_langgraph import PilotRemoteRunnable
    graph.add_node("worker", PilotRemoteRunnable(node="enrich", peer="my-worker"))
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import AsyncIterator, Iterator
from typing import Any

from langchain_core.callbacks.manager import (
    AsyncCallbackManager,
)
from langchain_core.runnables import Runnable
from langchain_core.runnables.config import RunnableConfig

from . import _otel
from ._ipc import Addr
from .asyncio_client import PilotConnection
from .errors import from_reply as _error_from_reply

log = logging.getLogger(__name__)

DEFAULT_PORT = 5000


# ---- shared connection cache ----

class _ConnectionRegistry:
    def __init__(self) -> None:
        self._by_loop: "dict[int, PilotConnection]" = {}
        # Per-loop lock map. asyncio.Lock instances bind to the loop that
        # first awaits them, so a single shared lock would break the moment
        # two different loops touch the registry. Keyed by loop id().
        self._locks: dict[int, asyncio.Lock] = {}
        self._sync_loop: asyncio.AbstractEventLoop | None = None
        self._sync_thread: threading.Thread | None = None
        self._sync_lock = threading.Lock()

    def _lock_for_loop(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        lid = id(loop)
        lock = self._locks.get(lid)
        if lock is None:
            lock = asyncio.Lock()  # binds to the current loop on first await
            self._locks[lid] = lock
        return lock

    async def get(self, socket_path: str | None = None) -> PilotConnection:
        loop = asyncio.get_running_loop()
        async with self._lock_for_loop(loop):
            conn = self._by_loop.get(id(loop))
            if conn is None or not conn.is_alive():
                # Cached connection is missing, closed, or the daemon went away
                # since we last used it. Replace transparently.
                if conn is not None:
                    log.info("PilotConnection cached for this loop is no longer alive; reconnecting")
                    try:
                        await conn.close()
                    except Exception:
                        pass
                conn = await PilotConnection.connect(socket_path)
                self._by_loop[id(loop)] = conn
            return conn

    async def aclose_all(self) -> None:
        """Close every cached PilotConnection across all event loops AND stop
        the background sync-loop thread (if one was ever started).

        Best-effort — connections bound to event loops other than the caller's
        are force-closed synchronously (the alternative is leaving them open
        until interpreter exit). Call once at the end of long-running scripts
        for a clean shutdown.
        """
        current_loop = None
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        async with self._lock_for_loop(current_loop) if current_loop else _NoLockCtx():
            conns = list(self._by_loop.items())
            self._by_loop.clear()
        for lid, conn in conns:
            if current_loop is not None and id(current_loop) == lid:
                try:
                    await conn.close()
                except Exception:
                    pass
            else:
                # Different loop — can't await close on it. Force-close the
                # transport synchronously; the cancelled reader task gets GC'd
                # with its loop.
                try:
                    conn.force_close_sync()
                except Exception:
                    pass
        # Note: stopping the sync_loop is done by aclose_sync() AFTER this
        # async function returns — we can't stop a loop while running on it.

    async def drop_current(self) -> None:
        """Drop the cached connection ONLY if it's no longer alive.

        Under concurrency, a single retry caller killing the shared connection
        would cascade-fail every other in-flight call. So we only drop when
        the connection is actually dead — otherwise the next `get()` returns
        the same healthy one and the retry hits a fresh stream attempt on
        the live socket.
        """
        loop = asyncio.get_running_loop()
        async with self._lock_for_loop(loop):
            conn = self._by_loop.get(id(loop))
            if conn is None:
                return
            if conn.is_alive():
                # Still healthy — leave it alone, let the retry just dial again.
                return
            self._by_loop.pop(id(loop), None)
        try:
            await conn.close()
        except Exception:
            pass

    def sync_loop(self) -> asyncio.AbstractEventLoop:
        with self._sync_lock:
            if self._sync_loop is None:
                self._sync_loop = asyncio.new_event_loop()
                t = threading.Thread(
                    target=self._sync_loop.run_forever,
                    name="pilot-langgraph-sync-loop",
                    daemon=True,
                )
                t.start()
                self._sync_thread = t
            return self._sync_loop


class _NoLockCtx:
    """Used by aclose_all when there's no running loop to acquire a lock on."""
    async def __aenter__(self): return None
    async def __aexit__(self, *_): return None


_registry = _ConnectionRegistry()


async def aclose() -> None:
    """Close every cached PilotConnection.

    Call at the end of a long-running async script for a clean shutdown.
    The atexit hook does best-effort cleanup, but calling this explicitly
    from your event loop avoids the "Task was destroyed but it is pending"
    warnings on interpreter exit.
    """
    await _registry.aclose_all()


def aclose_sync() -> None:
    """Synchronous wrapper for `aclose()`. Also stops the sync_loop thread.

    After this returns, the plugin has no active background work. Call from
    a normal sync `try: ... finally: pilot_langgraph.aclose_sync()` block.
    """
    # Run the async close on the sync loop (drains all cached connections).
    _run_sync(aclose())
    # Now stop the sync loop itself from OUTSIDE.
    with _registry._sync_lock:
        sync_loop = _registry._sync_loop
        _registry._sync_loop = None
        thread = _registry._sync_thread
        _registry._sync_thread = None
    if sync_loop is not None and sync_loop.is_running():
        try:
            sync_loop.call_soon_threadsafe(sync_loop.stop)
        except Exception:
            pass
    # Best-effort wait for the thread to exit so callers see is_running()=False.
    if thread is not None:
        thread.join(timeout=2.0)


def _run_sync(coro):
    loop = _registry.sync_loop()
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    return fut.result()


def _shutdown_sync_loop() -> None:
    """Best-effort cleanup of the background sync loop at interpreter exit.

    Calls aclose_sync() if the sync loop is still up. Users who call
    aclose() explicitly during their normal shutdown won't trigger this path.
    """
    loop = _registry._sync_loop
    if loop is None or not loop.is_running():
        return
    try:
        aclose_sync()
    except Exception:
        pass


import atexit as _atexit
_atexit.register(_shutdown_sync_loop)


# ---- public Runnables ----

class PilotRemoteRunnable(Runnable[Any, Any]):
    """Run a graph node on a remote Pilot peer.

    Args:
        node: handler name registered on the remote `WorkerServer`.
        peer: pilot address (`"0:XXXX.YYYY.ZZZZ"`) or hostname.
        port: TCP-equivalent stream port the worker listens on.
        timeout_secs: wall-clock budget for one invocation.
        socket_path: override `$PILOT_SOCKET`.
    """

    def __init__(
        self,
        *,
        node: str,
        peer: str | Addr,
        port: int = DEFAULT_PORT,
        timeout_secs: float = 60.0,
        socket_path: str | None = None,
        input_model: Any = None,
        output_model: Any = None,
        max_retries: int = 2,
        retry_backoff_secs: float = 1.5,
    ):
        super().__init__()
        self.node = node
        self.peer = Addr.parse(peer) if isinstance(peer, str) and ":" in peer else peer
        self.port = port
        self.timeout_secs = timeout_secs
        self.socket_path = socket_path
        # Optional caller-side pydantic validation mirroring worker's.
        # On invoke: input is validated and dumped to dict before send.
        # On result: response is validated and returned as a dumped dict.
        self.input_model = input_model
        self.output_model = output_model
        # Retry config for transient connection failures (cold tunnel, dead
        # cached connection). `max_retries=0` disables retries entirely.
        # `max_retries=N` allows N+1 total attempts (1 initial + N retries).
        # Backoff doubles each attempt up to a 30s cap.
        self.max_retries = max_retries
        self.retry_backoff_secs = retry_backoff_secs

    async def _resolve_peer(self, conn: PilotConnection) -> Addr:
        if isinstance(self.peer, Addr):
            return self.peer
        info = await conn.resolve_hostname(self.peer)
        return Addr.parse(info["address"])

    async def ainvoke(
        self,
        input: Any,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        cm = AsyncCallbackManager.configure(
            inheritable_callbacks=(config or {}).get("callbacks"),
            inheritable_tags=(config or {}).get("tags"),
            inheritable_metadata=(config or {}).get("metadata"),
        )
        run_managers = await cm.on_chain_start(
            serialized={"name": f"PilotRemoteRunnable:{self.node}"},
            inputs={"input": input, "peer": str(self.peer), "port": self.port},
            run_id=(config or {}).get("run_id"),
        )
        run_manager = run_managers[0] if isinstance(run_managers, list) else run_managers
        # Caller-side input validation: catch type errors BEFORE the network round-trip.
        wire_input = input
        if self.input_model is not None:
            try:
                wire_input = self.input_model.model_validate(input).model_dump()
            except Exception as e:
                err = ValueError(f"client input validation: {e}")
                await run_manager.on_chain_error(err)
                raise err
        with _otel.client_span(self.node, str(self.peer)):
            try:
                result = await self._ainvoke_with_retries(wire_input)
            except BaseException as e:
                await run_manager.on_chain_error(e)
                raise
            # Caller-side output validation: enforce the contract on what we got back.
            if self.output_model is not None:
                try:
                    result = self.output_model.model_validate(result).model_dump()
                except Exception as e:
                    err = ValueError(f"client output validation: {e}")
                    await run_manager.on_chain_error(err)
                    raise err
            await run_manager.on_chain_end({"output": result})
            return result

    async def _ainvoke_with_retries(self, input: Any) -> Any:
        # Cold pilot tunnels (first dial after either daemon restarts) sometimes
        # close immediately with no data. Configurable retry with exponential
        # backoff (capped at 30s) drops the cached connection in case the
        # daemon-side socket state went sideways.
        total_attempts = 1 + max(0, self.max_retries)
        last_err: Exception | None = None
        for attempt in range(total_attempts):
            try:
                return await self._ainvoke_once(input)
            except (PilotConnectionErrorWrapper, TimeoutError) as e:
                last_err = e
                if attempt < total_attempts - 1:
                    delay = min(self.retry_backoff_secs * (2 ** attempt), 30.0)
                    log.info("PilotRemoteRunnable: retry %d/%d after %s (sleep %.1fs)",
                             attempt + 1, total_attempts - 1, e, delay)
                    await _registry.drop_current()
                    await asyncio.sleep(delay)
                    continue
                raise
        if last_err:
            raise last_err

    async def _ainvoke_once(self, input: Any) -> Any:
        # Drain the streaming protocol until `done`; return the last non-null result.
        # For non-streaming handlers this is exactly one frame.
        last_result: Any = None
        async for frame in self._aframes(input):
            if frame.get("ok") is False:
                raise _error_from_reply(frame, peer=frame.get("_peer"), node=self.node)
            if frame.get("result") is not None:
                last_result = frame["result"]
            if frame.get("done"):
                break
        return last_result

    async def _aframes(self, input: Any) -> AsyncIterator[dict[str, Any]]:
        """Yield each newline-delimited JSON frame from the remote handler.

        Closes the stream after the `done` frame or on error.
        """
        conn = await _registry.get(self.socket_path)
        peer_addr = await self._resolve_peer(conn)

        # OTel: attach a client span and inject W3C traceparent into the request
        # so the worker can continue the trace as a server span.
        traceparent: dict[str, str] = {}
        _otel.inject_traceparent(traceparent)
        wire_req: dict[str, Any] = {"node": self.node, "payload": input}
        if traceparent:
            wire_req["traceparent"] = traceparent
        req = json.dumps(wire_req, separators=(",", ":"), default=str)
        log.info("PilotRemoteRunnable: dial peer=%s port=%d node=%s bytes=%d",
                 peer_addr, self.port, self.node, len(req))

        stream = await conn.dial(peer_addr, self.port, timeout=self.timeout_secs)
        try:
            await stream.write(req.encode())
            buf = b""
            deadline = asyncio.get_event_loop().time() + self.timeout_secs
            saw_any = False
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    raise TimeoutError(f"no reply from `{peer_addr}` within {self.timeout_secs}s")
                chunk = await stream.read(timeout=remaining)
                if not chunk:
                    if not saw_any:
                        raise PilotConnectionErrorWrapper(
                            f"stream from `{peer_addr}` closed before any reply"
                        )
                    return
                buf += chunk
                # Newline-delimited JSON frames; tolerate partial trailing frame.
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line:
                        continue
                    try:
                        frame = json.loads(line)
                    except json.JSONDecodeError:
                        # Possibly an old non-streaming handler that wrote one JSON
                        # without a newline. Try parsing the whole buf as one object.
                        try:
                            frame = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                    saw_any = True
                    frame["_peer"] = str(peer_addr)
                    yield frame
                    if frame.get("done"):
                        return
                # Legacy fallback: if no newline yet but the whole buf parses, emit and stop.
                if buf:
                    try:
                        frame = json.loads(buf)
                        saw_any = True
                        frame["_peer"] = str(peer_addr)
                        if "done" not in frame:
                            frame["done"] = True
                        yield frame
                        return
                    except json.JSONDecodeError:
                        pass
        finally:
            await stream.close()

    async def astream(
        self,
        input: Any,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """Yield each chunk from a streaming remote handler.

        Pairs with a worker handler defined as `async def fn(payload): yield ...`.
        Non-streaming handlers yield a single value. Cold-tunnel resilient: a
        first-frame EOF before any chunk is yielded triggers a retry; once
        chunks have started flowing, mid-stream errors propagate to the caller.
        """
        cm = AsyncCallbackManager.configure(
            inheritable_callbacks=(config or {}).get("callbacks"),
            inheritable_tags=(config or {}).get("tags"),
            inheritable_metadata=(config or {}).get("metadata"),
        )
        run_managers = await cm.on_chain_start(
            serialized={"name": f"PilotRemoteRunnable:{self.node}:stream"},
            inputs={"input": input, "peer": str(self.peer), "port": self.port},
            run_id=(config or {}).get("run_id"),
        )
        run_manager = run_managers[0] if isinstance(run_managers, list) else run_managers

        async def _attempt_stream() -> AsyncIterator[Any]:
            async for frame in self._aframes(input):
                if frame.get("ok") is False:
                    raise _error_from_reply(frame, peer=frame.get("_peer"), node=self.node)
                if frame.get("result") is not None:
                    yield frame["result"]
                if frame.get("done"):
                    return

        chunks: list[Any] = []
        try:
            for attempt in range(3):
                started = False
                try:
                    async for v in _attempt_stream():
                        started = True
                        chunks.append(v)
                        yield v
                    break  # done frame reached cleanly
                except (PilotConnectionErrorWrapper, TimeoutError) as e:
                    if started or attempt == 2:
                        raise
                    log.info("PilotRemoteRunnable.astream: retry %d after %s", attempt + 1, e)
                    await _registry.drop_current()
                    await asyncio.sleep(1.5)
                    continue
        except BaseException as e:
            await run_manager.on_chain_error(e)
            raise
        await run_manager.on_chain_end({"chunks": chunks, "n_chunks": len(chunks)})

    def stream(
        self,
        input: Any,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """Sync wrapper over astream — collects chunks via a queue across loops."""
        loop = _registry.sync_loop()
        queue: "asyncio.Queue[Any]" = asyncio.Queue()
        SENTINEL = object()

        async def _drain():
            try:
                async for v in self.astream(input, config, **kwargs):
                    queue.put_nowait(v)
            except Exception as e:
                queue.put_nowait(("__error__", e))
            finally:
                queue.put_nowait(SENTINEL)

        asyncio.run_coroutine_threadsafe(_drain(), loop)
        while True:
            v = asyncio.run_coroutine_threadsafe(queue.get(), loop).result()
            if v is SENTINEL:
                return
            if isinstance(v, tuple) and len(v) == 2 and v[0] == "__error__":
                raise v[1]
            yield v

    def invoke(
        self,
        input: Any,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        return _run_sync(self.ainvoke(input, config, **kwargs))

    @classmethod
    async def adiscover(
        cls,
        peer: str | Addr,
        *,
        port: int = DEFAULT_PORT,
        timeout_secs: float = 30.0,
        socket_path: str | None = None,
        include_introspection: bool = False,
    ) -> dict[str, "PilotRemoteRunnable"]:
        """Discover a peer's handlers and return one Runnable per handler.

        Calls the peer's auto-registered `_handlers` introspection once,
        then constructs a `PilotRemoteRunnable` for each handler so callers
        can invoke any of them by name without repeating the boilerplate.

        Args:
            peer: pilot address or hostname.
            port: handler port on the peer.
            timeout_secs: budget for both the discovery call and each child runnable.
            include_introspection: if True, also include `_health` and `_handlers`.

        Returns:
            Dict mapping handler name → ready-to-use PilotRemoteRunnable.

        Example:
            handlers = await PilotRemoteRunnable.adiscover("my-worker")
            result = await handlers["search"].ainvoke({"query": "..."})
        """
        listing = await cls(
            node="_handlers", peer=peer, port=port, timeout_secs=timeout_secs,
            socket_path=socket_path, max_retries=1,
        ).ainvoke(None)
        out: dict[str, "PilotRemoteRunnable"] = {}
        for h in listing.get("handlers", []):
            name = h.get("name", "")
            if not include_introspection and name.startswith("_"):
                continue
            out[name] = cls(
                node=name, peer=peer, port=port, timeout_secs=timeout_secs,
                socket_path=socket_path,
            )
        return out

    @classmethod
    def discover(
        cls,
        peer: str | Addr,
        **kwargs: Any,
    ) -> dict[str, "PilotRemoteRunnable"]:
        """Synchronous wrapper around :meth:`adiscover`."""
        return _run_sync(cls.adiscover(peer, **kwargs))


class PilotFanoutRunnable(Runnable[Any, Any]):
    """Send the same input to N remote targets in parallel; collect all results.

    Each target is a `(label, PilotRemoteRunnable | dict)` pair. Dicts can use
    keys `node`, `peer`, `port`, `timeout_secs`, `socket_path` — same as the
    PilotRemoteRunnable constructor.

    `ainvoke(input)` returns `{label: result}` once every target replies.
    `astream(input)` yields `(label, result)` tuples as each target replies,
    so a graph can react to fast workers immediately without waiting for
    slow ones. Failures from any target raise unless `return_exceptions=True`,
    in which case the exception appears in the result dict in place of a value.

    Usage:
        fan = PilotFanoutRunnable({
            "search":  PilotRemoteRunnable(node="search",  peer="worker-a"),
            "analyse": PilotRemoteRunnable(node="analyse", peer="worker-b"),
            "score":   {"node": "score", "peer": "worker-c"},
        })
        out = await fan.ainvoke({"query": "..."})
        # out == {"search": ..., "analyse": ..., "score": ...}
    """

    def __init__(
        self,
        targets: dict[str, "PilotRemoteRunnable | dict[str, Any]"],
        *,
        return_exceptions: bool = False,
    ):
        super().__init__()
        if not targets:
            raise ValueError("PilotFanoutRunnable requires at least one target")
        self.targets: dict[str, PilotRemoteRunnable] = {}
        for label, t in targets.items():
            if isinstance(t, PilotRemoteRunnable):
                self.targets[label] = t
            elif isinstance(t, dict):
                self.targets[label] = PilotRemoteRunnable(**t)
            else:
                raise TypeError(f"target {label!r}: expected PilotRemoteRunnable or dict, got {type(t).__name__}")
        self.return_exceptions = return_exceptions

    async def ainvoke(self, input: Any, config: RunnableConfig | None = None, **kwargs: Any) -> dict[str, Any]:
        labels = list(self.targets.keys())
        coros = [self.targets[label].ainvoke(input, config=config) for label in labels]
        results = await asyncio.gather(*coros, return_exceptions=self.return_exceptions)
        return dict(zip(labels, results))

    async def astream(self, input: Any, config: RunnableConfig | None = None, **kwargs: Any) -> AsyncIterator[tuple[str, Any]]:
        """Yield (label, result) as each target replies (out of order)."""
        async def _wrapped(label: str):
            try:
                return label, await self.targets[label].ainvoke(input, config=config)
            except Exception as e:
                if self.return_exceptions:
                    return label, e
                raise

        pending = {asyncio.create_task(_wrapped(label)) for label in self.targets}
        try:
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for t in done:
                    yield t.result()
        except BaseException:
            for t in pending:
                t.cancel()
            raise

    def invoke(self, input: Any, config: RunnableConfig | None = None, **kwargs: Any) -> dict[str, Any]:
        return _run_sync(self.ainvoke(input, config, **kwargs))


class PilotEchoRunnable(Runnable[Any, Any]):
    """Round-trip a payload through a peer's port-7 echo service via streams.

    Useful as a smoke test against any pilot peer (e.g. public `agent-alpha`).
    """

    def __init__(
        self,
        *,
        peer: str | Addr,
        port: int = 7,
        timeout_secs: float = 30.0,
        socket_path: str | None = None,
    ):
        super().__init__()
        self.peer = Addr.parse(peer) if isinstance(peer, str) and ":" in peer else peer
        self.port = port
        self.timeout_secs = timeout_secs
        self.socket_path = socket_path

    async def ainvoke(self, input: Any, config: RunnableConfig | None = None, **kwargs: Any) -> dict[str, Any]:
        conn = await _registry.get(self.socket_path)
        peer_addr: Addr
        if isinstance(self.peer, Addr):
            peer_addr = self.peer
        else:
            info = await conn.resolve_hostname(self.peer)
            peer_addr = Addr.parse(info["address"])

        wire_str = json.dumps(input, separators=(",", ":"), default=str)
        stream = await conn.dial(peer_addr, self.port, timeout=self.timeout_secs)
        try:
            await stream.write(wire_str.encode())
            chunk = await stream.read(timeout=self.timeout_secs)
        finally:
            await stream.close()
        echoed = chunk.decode(errors="replace")
        return {
            "peer": str(peer_addr),
            "echoed_payload": echoed,
            "echoed_bytes": len(echoed),
            "round_trip_ok": echoed == wire_str,
        }

    def invoke(self, input: Any, config: RunnableConfig | None = None, **kwargs: Any) -> dict[str, Any]:
        return _run_sync(self.ainvoke(input, config, **kwargs))


class PilotConnectionErrorWrapper(RuntimeError):
    pass
