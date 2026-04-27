"""Pilot Protocol transport layer.

`PilotClient` is a thin shell over `pilotctl --json`. `WorkerRouter` is the
remote-side dispatcher: it receives wire-protocol requests on a Pilot port,
runs a registered handler, and ships the reply back to the sender.

Both sides of the wire run their own pilot daemon. Trust must be established
out-of-band (`pilotctl handshake <peer>`) before any send/recv works.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from . import wire

log = logging.getLogger(__name__)

Handler = Callable[[Any], Any]


class PilotError(RuntimeError):
    def __init__(self, code: str, message: str, hint: str | None = None):
        super().__init__(f"{code}: {message}" + (f" (hint: {hint})" if hint else ""))
        self.code = code
        self.message = message
        self.hint = hint


@dataclass
class PilotInfo:
    address: str
    hostname: str
    node_id: int
    public_key: str
    peers: int
    uptime_secs: int
    version: str


class PilotClient:
    """Wrapper over `pilotctl --json`. Pass `socket=` to address a specific daemon."""

    def __init__(self, socket: str | None = None, binary: str = "pilotctl"):
        self.binary = binary
        self.socket = socket

    def _run(self, *args: str, timeout: float = 30.0) -> dict[str, Any]:
        env = os.environ.copy()
        if self.socket:
            env["PILOT_SOCKET"] = self.socket
        proc = subprocess.run(
            [self.binary, "--json", *args],
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout,
        )
        if not proc.stdout.strip():
            raise PilotError("no_output", proc.stderr.strip() or "pilotctl returned no output")
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError as e:
            raise PilotError("invalid_json", f"could not parse pilotctl output: {e}\n{proc.stdout}")
        if payload.get("status") == "error":
            raise PilotError(payload.get("code", "unknown"), payload.get("message", ""), payload.get("hint"))
        return payload.get("data", {})

    def info(self) -> PilotInfo:
        d = self._run("info")
        return PilotInfo(
            address=d["address"],
            hostname=d.get("hostname", ""),
            node_id=d["node_id"],
            public_key=d.get("public_key", ""),
            peers=d.get("peers", 0),
            uptime_secs=d.get("uptime_secs", 0),
            version=d.get("version", ""),
        )

    def is_running(self) -> bool:
        try:
            self.info()
            return True
        except PilotError as e:
            if e.code == "not_running":
                return False
            raise

    def find(self, hostname: str) -> dict[str, Any]:
        return self._run("find", hostname)

    def set_hostname(self, hostname: str) -> dict[str, Any]:
        return self._run("set-hostname", hostname)

    def handshake(self, peer: str, justification: str = "pilot-langgraph") -> dict[str, Any]:
        return self._run("handshake", peer, justification)

    def trust_list(self) -> list[dict[str, Any]]:
        d = self._run("trust")
        if isinstance(d, list):
            return d
        return d.get("trusted", []) if isinstance(d, dict) else []

    def trusts(self, peer: str) -> bool:
        for entry in self.trust_list():
            if peer in (entry.get("hostname"), entry.get("address"), str(entry.get("node_id", ""))):
                return True
        return False

    def send(self, peer: str, port: int, data: str, timeout: str = "30s") -> dict[str, Any]:
        # subprocess timeout has to outlive the daemon's own --timeout
        sec = float(timeout.rstrip("s"))
        return self._run("send", peer, str(port), "--data", data, "--timeout", timeout, timeout=sec + 5)

    def recv(self, port: int, count: int = 1, timeout: str = "60s") -> list[dict[str, Any]]:
        sec = float(timeout.rstrip("s"))
        d = self._run("recv", str(port), "--count", str(count), "--timeout", timeout, timeout=sec + 5)
        if isinstance(d, dict):
            return d.get("messages") or []
        return []

    def recv_async(self, port: int, count: int = 1, timeout: str = "60s") -> "subprocess.Popen[str]":
        """Start `pilotctl recv` in the background. Caller must `.communicate()` to collect.

        Use this to win the race when you need to listen for a reply that arrives
        immediately after a `send`. Start `recv_async` first, then `send`, then
        block on the popen until it returns.
        """
        env = os.environ.copy()
        if self.socket:
            env["PILOT_SOCKET"] = self.socket
        return subprocess.Popen(
            [self.binary, "--json", "recv", str(port), "--count", str(count), "--timeout", timeout],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

    @staticmethod
    def collect_recv(proc: "subprocess.Popen[str]", wait_secs: float) -> list[dict[str, Any]]:
        try:
            out, _ = proc.communicate(timeout=wait_secs)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
        if not out.strip():
            return []
        try:
            payload = json.loads(out)
        except json.JSONDecodeError:
            return []
        if payload.get("status") == "error":
            return []
        d = payload.get("data") or {}
        return d.get("messages") or []

    def ping(self, peer: str, count: int = 1) -> dict[str, Any]:
        return self._run("ping", peer, "--count", str(count))


def from_env() -> PilotClient:
    return PilotClient(socket=os.environ.get("PILOT_SOCKET"))


class WorkerRouter:
    """Receives wire-protocol requests on a pilot port and dispatches to handlers.

    Run on the remote machine. Register handlers by name; the matching
    `PilotRemoteRunnable(node="X", peer=...)` on the caller side ships its
    state to handler `X` and gets the return value back as graph state.
    """

    def __init__(self, *, request_port: int = wire.DEFAULT_REQUEST_PORT, client: PilotClient | None = None):
        self.request_port = request_port
        self.client = client or from_env()
        self.handlers: dict[str, Handler] = {}

    def register(self, name: str, fn: Handler) -> None:
        self.handlers[name] = fn

    def serve_one(self, timeout_secs: int = 60) -> bool:
        try:
            msgs = self.client.recv(self.request_port, count=1, timeout=f"{timeout_secs}s")
        except PilotError as e:
            if e.code in {"timeout", "no_messages"}:
                return False
            raise
        if not msgs:
            return False

        try:
            req = wire.decode(msgs[0]["data"])
        except (ValueError, KeyError):
            log.warning("worker: malformed request, dropping")
            return True

        node = req.get("node")
        call_id = req.get("call_id")
        sender = req.get("from")
        reply_port = req.get("reply_port")
        log.info("worker: recv call_id=%s node=%s from=%s", call_id, node, sender)

        handler = self.handlers.get(node)
        if handler is None:
            reply = wire.encode_reply(call_id=call_id, ok=False, error=f"no handler for `{node}`")
        else:
            try:
                result = handler(req.get("payload"))
                reply = wire.encode_reply(call_id=call_id, ok=True, result=result)
            except Exception as e:
                log.exception("worker: handler raised")
                reply = wire.encode_reply(call_id=call_id, ok=False, error=f"{type(e).__name__}: {e}")

        try:
            self.client.send(sender, reply_port, reply, timeout=f"{timeout_secs}s")
        except PilotError as e:
            log.error("worker: failed to deliver reply call_id=%s to %s:%s: %s",
                      call_id, sender, reply_port, e)
        return True

    def serve_forever(self, idle_timeout_secs: int = 60) -> None:
        log.info("worker: listening port=%d handlers=%s", self.request_port, list(self.handlers))
        while True:
            self.serve_one(timeout_secs=idle_timeout_secs)
