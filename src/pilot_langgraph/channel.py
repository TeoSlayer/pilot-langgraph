"""Pub/sub channels over Pilot Protocol's built-in event stream broker (port 1002).

Each daemon runs an event broker on port 1002. To subscribe, dial the peer
hosting the broker, send a subscribe frame for the topic, then read events
forever. To publish, write events on the same kind of connection.

Use `"*"` as the topic to subscribe to all events on the peer.

Usage:

    # Subscriber
    async with PilotChannel.subscribe("alerts", peer="my-broker") as ch:
        async for ev in ch:
            print(ev.topic, ev.payload)

    # Publisher (one-shot)
    await PilotChannel.publish_one("alerts", b"system online", peer="my-broker")

    # Persistent publisher
    async with PilotChannel.publisher(peer="my-broker") as pub:
        await pub.publish("alerts", b"event 1")
        await pub.publish("alerts", b"event 2")

A `PilotEventSource` Runnable wraps subscribe + astream so a LangGraph node
can be driven by external events.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from langchain_core.runnables import Runnable
from langchain_core.runnables.config import RunnableConfig

from . import _ipc
from ._ipc import Addr, Event
from .asyncio_client import PilotConnection, Stream

log = logging.getLogger(__name__)


class PilotChannel:
    """Async subscription handle. Iterate to receive events; close when done.

    With `auto_reconnect=True` (default), the channel keeps a background task
    that re-establishes the subscription if the broker connection drops.
    Events delivered during the gap are unrecoverable (Pilot's broker has no
    offline buffer), but the subscriber keeps working past transient broker
    restarts without raising.
    """

    def __init__(
        self,
        conn: PilotConnection,
        stream: Stream,
        topic: str,
        owns_conn: bool,
        *,
        auto_reconnect: bool = False,
        peer_addr: Addr | None = None,
        socket_path: str | None = None,
        backoff_secs: float = 1.0,
        timeout_secs: float = 30.0,
    ):
        self._conn = conn
        self._stream = stream
        self._owns_conn = owns_conn
        self.topic = topic
        self._buf = b""
        self._closed = False
        # Auto-reconnect machinery
        self.auto_reconnect = auto_reconnect
        self._peer_addr = peer_addr
        self._socket_path = socket_path
        self._backoff_secs = backoff_secs
        self._timeout_secs = timeout_secs
        self._reconnect_queue: asyncio.Queue[Event] | None = None
        self._reconnect_task: asyncio.Task | None = None
        if auto_reconnect:
            self._reconnect_queue = asyncio.Queue()
            self._reconnect_task = asyncio.create_task(
                self._reconnect_loop(), name=f"pilot-channel-reconnect-{topic}"
            )

    @classmethod
    async def subscribe(
        cls,
        topic: str,
        *,
        peer: str | Addr,
        socket_path: str | None = None,
        timeout_secs: float = 30.0,
        auto_reconnect: bool = False,
        backoff_secs: float = 1.0,
    ) -> "PilotChannel":
        """Open a subscription stream to `peer`'s event broker for `topic`.

        Args:
            topic: pass `"*"` to receive every event the broker publishes.
            auto_reconnect: if True, reconnect on broker disconnect with backoff.
            backoff_secs: initial backoff between reconnect attempts.
        Use `async with` so the underlying stream is closed when done.
        """
        conn = await PilotConnection.connect(socket_path)
        try:
            peer_addr = peer if isinstance(peer, Addr) else (
                Addr.parse(peer) if ":" in peer else
                Addr.parse((await conn.resolve_hostname(peer))["address"])
            )
            stream = await conn.dial(peer_addr, _ipc.PORT_EVENT_STREAM, timeout=timeout_secs)
            await stream.write(_ipc.encode_event(topic, b""))
            return cls(
                conn, stream, topic, owns_conn=True,
                auto_reconnect=auto_reconnect, peer_addr=peer_addr,
                socket_path=socket_path, backoff_secs=backoff_secs,
                timeout_secs=timeout_secs,
            )
        except Exception:
            await conn.close()
            raise

    async def _reconnect_loop(self) -> None:
        """Background task: keep the subscription alive across broker drops."""
        while not self._closed:
            try:
                # Drain events from current stream into the queue.
                events_seen = 0
                async for ev in self._iterate_current_stream():
                    if self._closed:
                        return
                    if self._reconnect_queue is not None:
                        self._reconnect_queue.put_nowait(ev)
                    events_seen += 1
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning("PilotChannel: subscription dropped (%s); reconnecting in %.1fs",
                            e, self._backoff_secs)

            if self._closed:
                return

            # Reconnect with backoff
            try:
                await self._stream.close()
            except Exception:
                pass
            try:
                if self._owns_conn:
                    await self._conn.close()
            except Exception:
                pass
            await asyncio.sleep(self._backoff_secs)
            try:
                self._conn = await PilotConnection.connect(self._socket_path)
                self._stream = await self._conn.dial(
                    self._peer_addr, _ipc.PORT_EVENT_STREAM, timeout=self._timeout_secs
                )
                await self._stream.write(_ipc.encode_event(self.topic, b""))
                self._buf = b""
                log.info("PilotChannel: reconnected to %s topic=%s", self._peer_addr, self.topic)
            except Exception as e:
                log.warning("PilotChannel: reconnect attempt failed (%s)", e)

    async def _iterate_current_stream(self):
        """Yield Events from `self._stream` until EOF or close."""
        while not self._closed:
            events, self._buf = _ipc.split_events(self._buf)
            for ev in events:
                yield ev
            if self._closed:
                return
            chunk = await self._stream.read(timeout=30.0)
            if not chunk:
                raise EOFError("event stream closed by broker")
            self._buf += chunk

    @classmethod
    async def publisher(
        cls,
        *,
        peer: str | Addr,
        socket_path: str | None = None,
        timeout_secs: float = 30.0,
    ) -> "PilotPublisher":
        """Open a long-lived publishing connection. Use `async with`."""
        conn = await PilotConnection.connect(socket_path)
        try:
            peer_addr = peer if isinstance(peer, Addr) else (
                Addr.parse(peer) if ":" in peer else
                Addr.parse((await conn.resolve_hostname(peer))["address"])
            )
            stream = await conn.dial(peer_addr, _ipc.PORT_EVENT_STREAM, timeout=timeout_secs)
            # Publishers also need to subscribe first per the broker protocol;
            # use a wildcard so they don't accidentally drop their own messages.
            await stream.write(_ipc.encode_event("*", b""))
            return PilotPublisher(conn, stream)
        except Exception:
            await conn.close()
            raise

    @classmethod
    async def publish_one(
        cls,
        topic: str,
        payload: bytes | str,
        *,
        peer: str | Addr,
        socket_path: str | None = None,
        timeout_secs: float = 30.0,
    ) -> None:
        """Open, publish one event, close. For when you publish rarely."""
        async with await cls.publisher(peer=peer, socket_path=socket_path, timeout_secs=timeout_secs) as pub:
            await pub.publish(topic, payload)

    async def recv(self, timeout: float | None = None) -> Event:
        if self._reconnect_queue is not None:
            # auto_reconnect path: pull from the queue fed by the bg task.
            if timeout is not None:
                return await asyncio.wait_for(self._reconnect_queue.get(), timeout=timeout)
            return await self._reconnect_queue.get()

        deadline = None if timeout is None else asyncio.get_event_loop().time() + timeout
        while True:
            events, self._buf = _ipc.split_events(self._buf)
            if events:
                if len(events) > 1:
                    rest = b"".join(_ipc.encode_event(e.topic, e.payload) for e in events[1:])
                    self._buf = rest + self._buf
                return events[0]
            remaining = None if deadline is None else deadline - asyncio.get_event_loop().time()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"no event on `{self.topic}` within {timeout}s")
            chunk = await self._stream.read(timeout=remaining if remaining is not None else 30.0)
            if not chunk:
                raise EOFError("event stream closed by broker")
            self._buf += chunk

    def __aiter__(self) -> "PilotChannel":
        return self

    async def __anext__(self) -> Event:
        try:
            return await self.recv()
        except (EOFError, asyncio.CancelledError):
            raise StopAsyncIteration

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await self._stream.close()
        except Exception:
            pass
        if self._owns_conn:
            try:
                await self._conn.close()
            except Exception:
                pass

    async def __aenter__(self) -> "PilotChannel":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()


class PilotPublisher:
    def __init__(self, conn: PilotConnection, stream: Stream):
        self._conn = conn
        self._stream = stream
        self._closed = False

    async def publish(self, topic: str, payload: bytes | str) -> None:
        if isinstance(payload, str):
            payload = payload.encode()
        await self._stream.write(_ipc.encode_event(topic, payload))

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._stream.close()
        except Exception:
            pass
        await self._conn.close()

    async def __aenter__(self) -> "PilotPublisher":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()


class PilotEventSource(Runnable[Any, Any]):
    """A LangGraph-shaped Runnable that streams events from a remote broker.

    `astream(input)` ignores `input` and yields `{topic, payload}` dicts as
    they arrive. Use as a graph node that drives downstream nodes from an
    external event stream.

    Args:
        topic: topic to subscribe to (`"*"` for all).
        peer: address or hostname of the broker.
        max_events: stop after this many events (None = run forever).
        timeout_secs: per-event timeout (None = wait forever).
    """

    def __init__(
        self,
        *,
        topic: str,
        peer: str | Addr,
        max_events: int | None = None,
        timeout_secs: float | None = None,
        socket_path: str | None = None,
    ):
        super().__init__()
        self.topic = topic
        self.peer = peer
        self.max_events = max_events
        self.timeout_secs = timeout_secs
        self.socket_path = socket_path

    async def ainvoke(self, input: Any, config: RunnableConfig | None = None, **kwargs: Any) -> dict:
        """Wait for one event and return it. Use astream() for many."""
        async with await PilotChannel.subscribe(
            self.topic, peer=self.peer, socket_path=self.socket_path,
        ) as ch:
            ev = await ch.recv(timeout=self.timeout_secs)
            return {"topic": ev.topic, "payload": ev.payload.decode(errors="replace")}

    async def astream(self, input: Any, config: RunnableConfig | None = None, **kwargs: Any) -> AsyncIterator[dict]:
        n = 0
        async with await PilotChannel.subscribe(
            self.topic, peer=self.peer, socket_path=self.socket_path,
        ) as ch:
            async for ev in ch:
                yield {"topic": ev.topic, "payload": ev.payload.decode(errors="replace")}
                n += 1
                if self.max_events is not None and n >= self.max_events:
                    return

    def invoke(self, input: Any, config: RunnableConfig | None = None, **kwargs: Any) -> dict:
        from .runnables import _run_sync
        return _run_sync(self.ainvoke(input, config, **kwargs))
