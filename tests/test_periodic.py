"""WorkerServer.add_periodic_task tests."""
from __future__ import annotations

import asyncio

import pytest

from pilot_langgraph.server import WorkerServer


@pytest.mark.asyncio
async def test_periodic_task_runs_repeatedly():
    """Task fires after each interval until the server stops."""
    s = WorkerServer(port=0)
    fires: list[int] = []

    async def beat():
        fires.append(len(fires))

    s.add_periodic_task(0.05, beat)

    # Drive the lifecycle directly without binding to a real Pilot socket.
    s._started_at = 0.0
    task = asyncio.create_task(s._run_periodic(0.05, beat, "beat"))
    await asyncio.sleep(0.18)
    s._stopping.set()
    await asyncio.wait_for(task, timeout=0.5)
    assert len(fires) >= 3  # at least 3 beats in 180ms with 50ms interval


@pytest.mark.asyncio
async def test_periodic_errors_dont_kill_the_loop():
    """A task that raises is logged but the loop keeps ticking."""
    s = WorkerServer(port=0)
    fires: list[bool] = []

    async def flaky():
        fires.append(True)
        if len(fires) % 2 == 0:
            raise RuntimeError("expected")

    s._started_at = 0.0
    task = asyncio.create_task(s._run_periodic(0.03, flaky, "flaky"))
    await asyncio.sleep(0.20)
    s._stopping.set()
    await asyncio.wait_for(task, timeout=0.5)
    # Even with periodic exceptions, multiple fires happened
    assert len(fires) >= 4


@pytest.mark.asyncio
async def test_periodic_stops_on_request_stop():
    """When _stopping fires, the task exits within one interval."""
    s = WorkerServer(port=0)
    fires: list[int] = []

    async def beat():
        fires.append(1)

    s._started_at = 0.0
    task = asyncio.create_task(s._run_periodic(0.05, beat, "beat"))
    await asyncio.sleep(0.12)
    s._stopping.set()
    # Should exit in well under one full interval
    await asyncio.wait_for(task, timeout=0.2)
    final_count = len(fires)
    # Confirm no further fires after stop signal
    await asyncio.sleep(0.1)
    assert len(fires) == final_count


@pytest.mark.asyncio
async def test_add_periodic_task_rejected_after_start():
    s = WorkerServer(port=0)
    s._started_at = 0.0  # simulate already-started
    with pytest.raises(RuntimeError, match="before serve_forever"):
        s.add_periodic_task(1.0, lambda: None)


@pytest.mark.asyncio
async def test_sync_periodic_function_supported():
    s = WorkerServer(port=0)
    fires: list[int] = []
    def beat_sync():
        fires.append(1)

    s._started_at = 0.0
    task = asyncio.create_task(s._run_periodic(0.04, beat_sync, "beat"))
    await asyncio.sleep(0.15)
    s._stopping.set()
    await asyncio.wait_for(task, timeout=0.5)
    assert len(fires) >= 2
