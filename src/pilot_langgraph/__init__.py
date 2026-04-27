"""LangGraph distributed across networks via Pilot Protocol tunnels."""
from __future__ import annotations

from ._ipc import Addr, Datagram, Event
from .asyncio_client import Listener, PilotConnection, PilotConnectionError, Stream
from .errors import (
    PilotHandlerError,
    PilotHandlerNotFoundError,
    PilotRateLimitError,
    PilotRemoteError,
    PilotUnauthorizedError,
)
from .server import Context, Middleware, WorkerServer, pilot_handler
from . import testing  # noqa: F401  expose pilot_langgraph.testing.{invoke_handler, make_context}
from .trust import ensure_trust, ensure_trust_sync

# Channel API; depends on langchain_core for PilotEventSource Runnable.
try:
    from .channel import PilotChannel, PilotEventSource, PilotPublisher  # noqa: F401
except ImportError:
    PilotChannel = None  # type: ignore[assignment,misc]
    PilotEventSource = None  # type: ignore[assignment,misc]
    PilotPublisher = None  # type: ignore[assignment,misc]

# langchain_core only required for caller-side Runnables; tolerate absence on workers.
try:
    from .runnables import (  # noqa: F401
        PilotEchoRunnable,
        PilotFanoutRunnable,
        PilotRemoteRunnable,
        aclose,
        aclose_sync,
    )
    from .checkpoint import PilotCheckpointSaver  # noqa: F401
except ImportError:
    PilotEchoRunnable = None  # type: ignore[assignment,misc]
    PilotFanoutRunnable = None  # type: ignore[assignment,misc]
    PilotRemoteRunnable = None  # type: ignore[assignment,misc]
    PilotCheckpointSaver = None  # type: ignore[assignment,misc]
    aclose = None  # type: ignore[assignment,misc]
    aclose_sync = None  # type: ignore[assignment,misc]

__version__ = "0.6.1"

__all__ = [
    "PilotConnection",
    "PilotConnectionError",
    "Stream",
    "Listener",
    "Addr",
    "Datagram",
    "Event",
    "WorkerServer",
    "pilot_handler",
    "Context",
    "Middleware",
    "PilotRemoteRunnable",
    "PilotEchoRunnable",
    "PilotFanoutRunnable",
    "PilotCheckpointSaver",
    "PilotChannel",
    "PilotPublisher",
    "PilotEventSource",
    "PilotRemoteError",
    "PilotHandlerError",
    "PilotHandlerNotFoundError",
    "PilotUnauthorizedError",
    "PilotRateLimitError",
    "ensure_trust",
    "ensure_trust_sync",
    "aclose",
    "aclose_sync",
]
