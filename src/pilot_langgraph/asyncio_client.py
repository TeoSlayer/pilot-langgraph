"""Async Pilot daemon IPC client — streams, datagrams, and JSON-RPC.

Speaks the daemon's binary protocol directly over its Unix socket. No
`pilotctl` subprocess. Concurrent operations multiplex onto a single
connection: jsonRPC waiters are FIFO-routed by reply-command byte; per-
connection-id recv queues fan inbound stream data to the right `Stream`;
per-port accept queues feed pending `Listener.accept()` calls.

Public API:
    PilotConnection.connect(socket_path) -> PilotConnection
    .info() / .health() / .resolve_hostname() / .set_hostname()
    .handshake_send() / .handshake_approve()
    .dial(addr, port) -> Stream                  # outbound stream
    .listen(port) -> Listener                    # inbound stream listener
    .send_to(addr, port, data) / .subscribe(port)  # datagrams (rarely used)
    .close()
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import defaultdict, deque
from typing import Any

from . import _ipc
from ._ipc import Addr, Datagram

log = logging.getLogger(__name__)


class PilotConnectionError(RuntimeError):
    pass


class PilotConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, socket_path: str):
        self._reader = reader
        self._writer = writer
        self.socket_path = socket_path
        self._waiters: dict[int, deque[asyncio.Future[bytes]]] = defaultdict(deque)
        self._port_dgram_queues: dict[int, list[asyncio.Queue[Datagram]]] = defaultdict(list)
        self._stream_recv_queues: dict[int, asyncio.Queue[bytes | None]] = {}
        # cmdRecv frames that arrive before the queue is registered get buffered
        # here, then drained into the queue on registration. Same idea as
        # ipcClient.pendRecv in pilotprotocol/pkg/driver/ipc.go.
        self._pending_recv: dict[int, list[bytes | None]] = {}
        self._listener_accept_queues: dict[int, asyncio.Queue[_ipc.AcceptedConn]] = {}
        self._write_lock = asyncio.Lock()
        self._closed = asyncio.Event()
        self._reader_task = asyncio.create_task(self._read_loop(), name="pilot-ipc-reader")

    # ---- lifecycle ----
    @classmethod
    async def connect(cls, socket_path: str | None = None) -> "PilotConnection":
        path = socket_path or os.environ.get("PILOT_SOCKET", "/tmp/pilot.sock")
        try:
            reader, writer = await asyncio.open_unix_connection(path)
        except (FileNotFoundError, ConnectionRefusedError) as e:
            raise PilotConnectionError(f"daemon socket {path}: {e}") from e
        return cls(reader, writer, path)

    async def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass
        self._reader_task.cancel()
        for q in self._waiters.values():
            while q:
                fut = q.popleft()
                if not fut.done():
                    fut.set_exception(PilotConnectionError("connection closed"))
        for srq in self._stream_recv_queues.values():
            try:
                srq.put_nowait(None)
            except Exception:
                pass

    def force_close_sync(self) -> None:
        """Synchronously tear down the writer without awaiting.

        Used when a connection's owning loop has already been destroyed (e.g.
        between pytest-asyncio tests) — calling `await close()` is impossible
        because there's no live loop to schedule it on. We just shut the
        underlying socket; the cancelled reader task gets garbage-collected
        with the loop.

        Also notifies outstanding stream readers and dgram subscribers via
        EOF markers, so any background tasks parked on `await q.get()` wake
        up immediately rather than waiting for their per-read timeouts.
        """
        if self._closed.is_set():
            return
        self._closed.set()
        try:
            transport = getattr(self._writer, "_transport", None)
            if transport is not None:
                transport.close()
        except Exception:
            pass
        # Wake up parked readers — None signals EOF.
        for srq in self._stream_recv_queues.values():
            try:
                srq.put_nowait(None)
            except Exception:
                pass
        # Datagram subscribers loop on get(); dropping references via close
        # event isn't enough, but a cancelled-future will surface in their
        # own loop if they check, so we leave them alone here.

    async def __aenter__(self) -> "PilotConnection":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    def is_alive(self) -> bool:
        """True if this connection still appears usable for further requests.

        Dead means: explicitly closed, reader task finished (daemon disconnect
        or unhandled exception), or the underlying writer's transport is gone.
        """
        if self._closed.is_set():
            return False
        if self._reader_task.done():
            return False
        transport = getattr(self._writer, "_transport", None)
        if transport is None:
            return False
        if hasattr(transport, "is_closing") and transport.is_closing():
            return False
        return True

    # ---- read loop ----
    async def _read_loop(self) -> None:
        try:
            while not self._closed.is_set():
                length_bytes = await self._reader.readexactly(4)
                length = int.from_bytes(length_bytes, "big")
                if length == 0 or length > _ipc.MAX_MESSAGE_SIZE:
                    raise PilotConnectionError(f"invalid frame length: {length}")
                msg = await self._reader.readexactly(length)
                if not msg:
                    continue
                cmd = msg[0]
                payload = msg[1:]

                if cmd == _ipc.CMD_RECV:
                    parsed = _ipc.parse_recv(payload)
                    if parsed is None:
                        continue
                    conn_id, data = parsed
                    q = self._stream_recv_queues.get(conn_id)
                    if q is not None:
                        q.put_nowait(data)
                    else:
                        self._pending_recv.setdefault(conn_id, []).append(data)
                elif cmd == _ipc.CMD_ACCEPT:
                    accepted = _ipc.parse_accept(payload)
                    if accepted is None:
                        continue
                    aq = self._listener_accept_queues.get(accepted.local_port)
                    if aq is not None:
                        aq.put_nowait(accepted)
                elif cmd == _ipc.CMD_CLOSE_OK:
                    parsed_close = _ipc.parse_close_ok(payload)
                    if parsed_close is not None:
                        q = self._stream_recv_queues.pop(parsed_close, None)
                        if q is not None:
                            q.put_nowait(None)
                        else:
                            # Close-ack arrived before the queue was registered;
                            # mark EOF so a later registrant sees end-of-stream.
                            self._pending_recv.setdefault(parsed_close, []).append(None)
                    # also dispatch to any caller awaiting cmdCloseOK (unusual)
                    self._deliver_to_waiter(cmd, payload)
                elif cmd == _ipc.CMD_RECV_FROM:
                    dg = _ipc.parse_datagram(payload)
                    if dg is None:
                        continue
                    # Distinct local name from `q` above so mypy doesn't
                    # narrow the dgram queue type from earlier stream-queue use.
                    for dq in list(self._port_dgram_queues.get(dg.dst_port, ())):
                        dq.put_nowait(dg)
                elif cmd == _ipc.CMD_ERROR:
                    err = _ipc.parse_error(payload)
                    self._fail_oldest_waiter(err)
                else:
                    self._deliver_to_waiter(cmd, payload)
        except asyncio.IncompleteReadError:
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("pilot ipc read loop exiting")
        finally:
            await self.close()

    def _deliver_to_waiter(self, cmd: int, payload: bytes) -> None:
        q = self._waiters.get(cmd)
        if not q:
            return
        fut = q.popleft()
        if not fut.done():
            fut.set_result(payload)

    def _fail_oldest_waiter(self, err: str) -> None:
        for q in self._waiters.values():
            if q:
                fut = q.popleft()
                if not fut.done():
                    fut.set_exception(PilotConnectionError(f"daemon: {err}"))
                return

    # ---- raw I/O ----
    async def _send_raw_locked(self, payload: bytes) -> None:
        length = len(payload).to_bytes(4, "big")
        self._writer.write(length + payload)
        await self._writer.drain()

    async def _send_raw(self, payload: bytes) -> None:
        async with self._write_lock:
            await self._send_raw_locked(payload)

    async def _request(self, payload: bytes, expect: int, timeout: float | None = None) -> bytes:
        # Critical: register the waiter and send the request under the same
        # lock, so FIFO waiter order matches FIFO send order — otherwise two
        # concurrent _request calls can race the future enqueue against the
        # write, and the dispatcher will return reply A to caller B.
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[bytes] = loop.create_future()
        async with self._write_lock:
            self._waiters[expect].append(fut)
            await self._send_raw_locked(payload)
        try:
            if timeout:
                return await asyncio.wait_for(fut, timeout=timeout)
            return await fut
        except asyncio.TimeoutError:
            try:
                self._waiters[expect].remove(fut)
            except ValueError:
                pass
            raise

    async def _request_json(self, payload: bytes, expect: int, timeout: float | None = None) -> dict[str, Any]:
        body = await self._request(payload, expect, timeout=timeout)
        return json.loads(body) if body else {}

    # ---- jsonRPC ----
    async def info(self, timeout: float = 10.0) -> dict[str, Any]:
        return await self._request_json(_ipc.encode_info(), _ipc.CMD_INFO_OK, timeout=timeout)

    async def health(self, timeout: float = 10.0) -> dict[str, Any]:
        return await self._request_json(_ipc.encode_health(), _ipc.CMD_HEALTH_OK, timeout=timeout)

    async def resolve_hostname(self, hostname: str, timeout: float = 10.0) -> dict[str, Any]:
        return await self._request_json(
            _ipc.encode_resolve_hostname(hostname), _ipc.CMD_RESOLVE_HOSTNAME_OK, timeout=timeout
        )

    async def set_hostname(self, hostname: str, timeout: float = 10.0) -> dict[str, Any]:
        return await self._request_json(
            _ipc.encode_set_hostname(hostname), _ipc.CMD_SET_HOSTNAME_OK, timeout=timeout
        )

    async def handshake_send(self, node_id: int, justification: str = "", timeout: float = 10.0) -> dict[str, Any]:
        return await self._request_json(
            _ipc.encode_handshake_send(node_id, justification), _ipc.CMD_HANDSHAKE_OK, timeout=timeout
        )

    async def handshake_approve(self, node_id: int, timeout: float = 10.0) -> dict[str, Any]:
        return await self._request_json(
            _ipc.encode_handshake_approve(node_id), _ipc.CMD_HANDSHAKE_OK, timeout=timeout
        )

    async def trust_list(self, timeout: float = 10.0) -> list[dict[str, Any]]:
        d = await self._request_json(_ipc.encode_handshake_trusted(), _ipc.CMD_HANDSHAKE_OK, timeout=timeout)
        return d.get("trusted", []) if isinstance(d, dict) else (d or [])

    async def pending(self, timeout: float = 10.0) -> list[dict[str, Any]]:
        d = await self._request_json(_ipc.encode_handshake_pending(), _ipc.CMD_HANDSHAKE_OK, timeout=timeout)
        return d.get("pending", []) if isinstance(d, dict) else (d or [])

    # ---- streams ----
    def _register_recv_queue(self, conn_id: int) -> asyncio.Queue:
        recv_q: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._stream_recv_queues[conn_id] = recv_q
        # Drain anything that arrived before the queue was registered.
        pending = self._pending_recv.pop(conn_id, None)
        if pending:
            for item in pending:
                recv_q.put_nowait(item)
        return recv_q

    async def dial(self, dst: Addr | str, port: int, timeout: float = 30.0) -> "Stream":
        if isinstance(dst, str):
            dst = Addr.parse(dst)
        body = await self._request(_ipc.encode_dial(dst, port), _ipc.CMD_DIAL_OK, timeout=timeout)
        conn_id = _ipc.parse_dial_ok(body)
        if conn_id is None:
            raise PilotConnectionError("dial: malformed cmdDialOK")
        recv_q = self._register_recv_queue(conn_id)
        return Stream(self, conn_id, recv_q, peer=(dst, port))

    async def listen(self, port: int, timeout: float = 10.0) -> "Listener":
        body = await self._request(_ipc.encode_bind(port), _ipc.CMD_BIND_OK, timeout=timeout)
        bound = _ipc.parse_bind_ok(body)
        if bound is None:
            raise PilotConnectionError("bind: malformed cmdBindOK")
        accept_q: asyncio.Queue[_ipc.AcceptedConn] = asyncio.Queue()
        self._listener_accept_queues[bound] = accept_q
        return Listener(self, bound, accept_q)

    # ---- datagrams (kept for completeness, not used by Runnables) ----
    async def send_to(self, dst: Addr | str, port: int, data: bytes | str) -> None:
        if isinstance(dst, str):
            dst = Addr.parse(dst)
        if isinstance(data, str):
            data = data.encode()
        await self._send_raw(_ipc.encode_send_to(dst, port, data))

    def subscribe(self, port: int) -> "_PortSubscription":
        return _PortSubscription(self, port)


class Stream:
    """Single Pilot stream. Wraps a connID. .write/.read/.close are async."""

    def __init__(self, conn: PilotConnection, conn_id: int, recv_q: asyncio.Queue, peer: tuple[Addr, int] | None = None):
        self._conn = conn
        self.conn_id = conn_id
        self._recv_q = recv_q
        self.peer = peer
        self._closed = False

    async def write(self, data: bytes) -> None:
        if self._closed:
            raise PilotConnectionError("stream closed")
        for i in range(0, len(data), _ipc.MAX_SEND_CHUNK):
            chunk = data[i:i + _ipc.MAX_SEND_CHUNK]
            await self._conn._send_raw(_ipc.encode_send(self.conn_id, chunk))

    async def read(self, timeout: float | None = None) -> bytes:
        """Read one frame as delivered by the daemon. Returns b'' on EOF."""
        if timeout is not None:
            data = await asyncio.wait_for(self._recv_q.get(), timeout=timeout)
        else:
            data = await self._recv_q.get()
        if data is None:
            return b""
        return data

    async def read_until_eof(self, timeout: float | None = None) -> bytes:
        chunks: list[bytes] = []
        while True:
            chunk = await self.read(timeout=timeout)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)

    async def read_one(self, timeout: float | None = None) -> bytes:
        """Convenience: read first frame (typical req/reply pattern)."""
        return await self.read(timeout=timeout)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._conn._send_raw(_ipc.encode_close(self.conn_id))
        except Exception:
            pass
        # The daemon will eventually push cmdCloseOK which removes the queue;
        # remove now too in case the daemon's already gone.
        self._conn._stream_recv_queues.pop(self.conn_id, None)

    async def __aenter__(self) -> "Stream":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()


class Listener:
    """Pilot port listener. .accept() yields one inbound Stream at a time."""

    def __init__(self, conn: PilotConnection, port: int, accept_q: asyncio.Queue):
        self._conn = conn
        self.port = port
        self._accept_q = accept_q
        self._closed = False

    async def accept(self, timeout: float | None = None) -> Stream:
        if timeout is not None:
            accepted = await asyncio.wait_for(self._accept_q.get(), timeout=timeout)
        else:
            accepted = await self._accept_q.get()
        recv_q = self._conn._register_recv_queue(accepted.conn_id)
        return Stream(self._conn, accepted.conn_id, recv_q,
                      peer=(accepted.remote_addr, accepted.remote_port))

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._conn._listener_accept_queues.pop(self.port, None)

    async def __aenter__(self) -> "Listener":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()


class _PortSubscription:
    def __init__(self, conn: PilotConnection, port: int):
        self._conn = conn
        self.port = port
        self._queue: asyncio.Queue[Datagram] = asyncio.Queue()

    async def __aenter__(self) -> asyncio.Queue[Datagram]:
        self._conn._port_dgram_queues[self.port].append(self._queue)
        return self._queue

    async def __aexit__(self, *_: Any) -> None:
        try:
            self._conn._port_dgram_queues[self.port].remove(self._queue)
        except ValueError:
            pass
