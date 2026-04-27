"""Configurable retry policy on PilotRemoteRunnable."""
from __future__ import annotations


import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


async def test_max_retries_zero_fails_fast(worker_peer, monkeypatch):
    """With max_retries=0, a transient failure raises immediately."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    from pilot_langgraph.runnables import PilotConnectionErrorWrapper

    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=10,
                            max_retries=0, retry_backoff_secs=0.1)

    attempts = 0

    async def always_fail(input):
        nonlocal attempts
        attempts += 1
        raise PilotConnectionErrorWrapper("forced")

    monkeypatch.setattr(r, "_ainvoke_once", always_fail)
    with pytest.raises(PilotConnectionErrorWrapper):
        await r.ainvoke({})
    assert attempts == 1  # exactly one attempt


async def test_max_retries_one_does_two_attempts(worker_peer, monkeypatch):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    from pilot_langgraph.runnables import PilotConnectionErrorWrapper

    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=10,
                            max_retries=1, retry_backoff_secs=0.05)

    attempts = 0
    async def always_fail(input):
        nonlocal attempts
        attempts += 1
        raise PilotConnectionErrorWrapper("forced")

    monkeypatch.setattr(r, "_ainvoke_once", always_fail)
    with pytest.raises(PilotConnectionErrorWrapper):
        await r.ainvoke({})
    assert attempts == 2  # 1 initial + 1 retry


async def test_retry_eventually_succeeds(worker_peer, monkeypatch):
    """If the Nth attempt succeeds, the call returns its result."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    from pilot_langgraph.runnables import PilotConnectionErrorWrapper

    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=15,
                            max_retries=3, retry_backoff_secs=0.05)

    real_once = r._ainvoke_once
    attempts = 0
    async def flaky(input):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PilotConnectionErrorWrapper("forced")
        return await real_once(input)

    monkeypatch.setattr(r, "_ainvoke_once", flaky)
    out = await r.ainvoke({"x": 1})
    assert attempts == 3
    assert out["input_payload"] == {"x": 1}


async def test_default_retry_config():
    """Default behaviour: 2 retries (3 total attempts)."""
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="x", peer="0:0000.0000.0001")
    assert r.max_retries == 2
    assert r.retry_backoff_secs == 1.5


async def test_backoff_grows_exponentially_capped(worker_peer, monkeypatch):
    """Verify the sleep delays double per attempt and cap at 30s."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    import asyncio as _asyncio
    from pilot_langgraph import PilotRemoteRunnable
    from pilot_langgraph.runnables import PilotConnectionErrorWrapper

    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=10,
                            max_retries=4, retry_backoff_secs=2.0)

    async def always_fail(input):
        raise PilotConnectionErrorWrapper("forced")

    monkeypatch.setattr(r, "_ainvoke_once", always_fail)

    sleeps: list[float] = []
    async def fake_sleep(secs):
        sleeps.append(secs)
        # don't actually wait — the test would take forever
    monkeypatch.setattr(_asyncio, "sleep", fake_sleep)

    with pytest.raises(PilotConnectionErrorWrapper):
        await r.ainvoke({})

    # 4 retries → 4 sleeps with delays 2, 4, 8, 16
    assert sleeps == [2.0, 4.0, 8.0, 16.0]
