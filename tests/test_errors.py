"""Typed remote-exception tests.

Verifies that PilotRemoteRunnable raises the right subclass for each
worker error category, with `peer` and `node` attached.
"""
from __future__ import annotations

import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


async def test_handler_not_found_raises_typed(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotHandlerNotFoundError, PilotRemoteRunnable
    r = PilotRemoteRunnable(node="does-not-exist", peer=worker_peer, timeout_secs=10)
    with pytest.raises(PilotHandlerNotFoundError) as exc:
        await r.ainvoke({})
    assert exc.value.error_type == "handler_not_found"
    assert exc.value.node == "does-not-exist"
    assert exc.value.peer == worker_peer


async def test_handler_error_raises_typed(worker_peer):
    """A handler that raises produces PilotHandlerError on the caller.

    The deployed worker has a `crash` handler that does `1/0` — invoking it
    must come back as a typed PilotHandlerError carrying the type+message.
    """
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotHandlerError, PilotRemoteRunnable
    r = PilotRemoteRunnable(node="crash", peer=worker_peer, timeout_secs=10)
    with pytest.raises(PilotHandlerError) as exc:
        await r.ainvoke(None)
    assert exc.value.error_type == "handler_error"
    assert exc.value.node == "crash"
    assert "ZeroDivisionError" in str(exc.value)


async def test_unauthorized_raises_typed(worker_peer):
    """A handler with a restrictive ACL produces PilotUnauthorizedError."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    # Same caveat — needs a worker handler with a non-matching ACL.
    # The unit-level WorkerServer test already covers the dispatch path;
    # here we only need to verify the caller-side mapping. Use a fake
    # error_type-tagged reply via direct codec inspection.
    from pilot_langgraph.errors import from_reply, PilotUnauthorizedError
    e = from_reply({"ok": False, "error_type": "unauthorized", "error": "denied"},
                   peer=worker_peer, node="some-handler")
    assert isinstance(e, PilotUnauthorizedError)
    assert e.error_type == "unauthorized"
    assert "denied" in str(e)


def test_error_factory_falls_back_to_base():
    from pilot_langgraph.errors import from_reply, PilotRemoteError
    e = from_reply({"ok": False, "error": "weird thing"}, peer="x", node="y")
    assert type(e) is PilotRemoteError
    assert e.error_type == "remote_error"


def test_typed_errors_are_subclasses_of_remote_error():
    from pilot_langgraph import (
        PilotHandlerError,
        PilotHandlerNotFoundError,
        PilotRemoteError,
        PilotUnauthorizedError,
    )
    assert issubclass(PilotHandlerNotFoundError, PilotRemoteError)
    assert issubclass(PilotUnauthorizedError, PilotRemoteError)
    assert issubclass(PilotHandlerError, PilotRemoteError)
