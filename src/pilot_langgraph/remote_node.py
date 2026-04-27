"""Backward-compat shim. Use `pilot_langgraph.runnables` instead.

`remote_node()` and `remote_echo_node()` remain as thin function wrappers around
the new `PilotRemoteRunnable` / `PilotEchoRunnable` classes.

Deprecated since 0.6.0; will be removed in 1.0.
"""
from __future__ import annotations

import warnings
from collections.abc import Callable
from typing import Any

from . import wire
from .runnables import PilotEchoRunnable, PilotRemoteRunnable
from .transport import PilotClient, WorkerRouter  # noqa: F401

warnings.warn(
    "pilot_langgraph.remote_node is deprecated since 0.6.0; "
    "import PilotRemoteRunnable / PilotEchoRunnable from pilot_langgraph instead. "
    "This shim will be removed in 1.0.",
    DeprecationWarning,
    stacklevel=2,
)

WIRE_VERSION = wire.WIRE_VERSION
DEFAULT_REQUEST_PORT = wire.DEFAULT_REQUEST_PORT
DEFAULT_REPLY_PORT = wire.DEFAULT_REPLY_PORT
PILOT_ECHO_PORT = wire.PILOT_ECHO_PORT


def remote_node(
    node_name: str,
    peer: str,
    *,
    request_port: int = DEFAULT_REQUEST_PORT,
    reply_port: int = DEFAULT_REPLY_PORT,  # noqa: ARG001  kept for backward compat
    timeout_secs: int = 60,
    client: PilotClient | None = None,  # noqa: ARG001  kept for backward compat
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Legacy shim — prefer ``PilotRemoteRunnable`` directly.

    `client` and `reply_port` are accepted but ignored: the modern Runnable
    uses a shared `_ConnectionRegistry` and the wire protocol no longer
    needs a separate reply port.
    """
    runnable = PilotRemoteRunnable(
        node=node_name, peer=peer,
        port=request_port, timeout_secs=timeout_secs,
    )
    return lambda state: runnable.invoke(state)


def remote_echo_node(
    peer: str,
    *,
    state_key: str = "round_trip",
    port: int = PILOT_ECHO_PORT,
    timeout_secs: int = 30,
    client: PilotClient | None = None,  # noqa: ARG001  kept for backward compat
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Legacy shim — prefer ``PilotEchoRunnable`` directly. `client` is ignored."""
    runnable = PilotEchoRunnable(peer=peer, port=port, timeout_secs=timeout_secs)
    return lambda state: {state_key: runnable.invoke(state)}
