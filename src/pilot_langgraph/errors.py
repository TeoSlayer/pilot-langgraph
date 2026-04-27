"""Typed exceptions raised by remote handler failures.

The worker tags every error reply with an `error_type` string; the caller
maps that into a typed Python exception so user code can write specific
`except` clauses:

    try:
        await runnable.ainvoke(payload)
    except PilotUnauthorizedError:
        # ACL denial — escalate, don't retry
        ...
    except PilotHandlerNotFoundError:
        # Misconfiguration — log and skip
        ...
    except PilotHandlerError as e:
        # Handler raised an exception — retry or report
        ...

`PilotRemoteError` is the common base for everything that came back from a
remote worker. Connection-layer errors (tunnel down, daemon disconnect)
remain `PilotConnectionError` from `pilot_langgraph.asyncio_client`.
"""
from __future__ import annotations


class PilotRemoteError(RuntimeError):
    """Base class for any error returned by a remote handler."""

    error_type: str = "remote_error"

    def __init__(self, message: str, *, peer: str | None = None, node: str | None = None):
        super().__init__(message)
        self.peer = peer
        self.node = node


class PilotHandlerNotFoundError(PilotRemoteError):
    """The remote worker has no handler with the requested name."""
    error_type = "handler_not_found"


class PilotUnauthorizedError(PilotRemoteError):
    """The remote worker's ACL denied the caller for this handler."""
    error_type = "unauthorized"


class PilotHandlerError(PilotRemoteError):
    """The remote handler ran but raised an exception."""
    error_type = "handler_error"


class PilotRateLimitError(PilotRemoteError):
    """The caller exceeded the rate limit configured for this handler.

    Callers that catch this can back off and retry; it's not a permanent
    failure. The error message includes the limit + window for diagnosis.
    """
    error_type = "rate_limited"


_ERROR_TYPE_MAP: dict[str, type[PilotRemoteError]] = {
    "handler_not_found": PilotHandlerNotFoundError,
    "unauthorized": PilotUnauthorizedError,
    "handler_error": PilotHandlerError,
    "rate_limited": PilotRateLimitError,
    "remote_error": PilotRemoteError,
}


def from_reply(reply: dict, *, peer: str | None = None, node: str | None = None) -> PilotRemoteError:
    """Construct the right typed exception for a `{ok: False, ...}` reply frame."""
    err_msg = reply.get("error") or "remote error"
    err_type = reply.get("error_type") or "remote_error"
    cls = _ERROR_TYPE_MAP.get(err_type, PilotRemoteError)
    return cls(err_msg, peer=peer, node=node)
