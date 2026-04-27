"""Backward-compat shim. Use `pilot_langgraph.PilotConnection` (async) instead.

Deprecated since 0.6.0; will be removed in 1.0.
"""
import warnings

from .transport import PilotClient, PilotError, PilotInfo, from_env  # noqa: F401

warnings.warn(
    "pilot_langgraph.pilot_client is deprecated since 0.6.0; "
    "use pilot_langgraph.PilotConnection (native async) instead. "
    "This subprocess-based shim will be removed in 1.0.",
    DeprecationWarning,
    stacklevel=2,
)
