"""Explicit registry shutdown via pilot_langgraph.aclose()."""
from __future__ import annotations


from .conftest import requires_daemon


pytestmark = [requires_daemon]


async def test_aclose_drops_all_cached_connections():
    """After aclose(), the registry should be empty and a new get() opens fresh."""
    import pilot_langgraph
    from pilot_langgraph.runnables import _registry

    # Establish a cached connection
    c1 = await _registry.get()
    assert c1.is_alive()
    assert len(_registry._by_loop) >= 1

    await pilot_langgraph.aclose()
    assert len(_registry._by_loop) == 0
    # The previously-cached conn was closed
    assert not c1.is_alive()

    # Subsequent get() returns a fresh, alive connection
    c2 = await _registry.get()
    assert c2 is not c1
    assert c2.is_alive()


async def test_aclose_is_idempotent():
    """Calling aclose() twice is a no-op the second time."""
    import pilot_langgraph
    from pilot_langgraph.runnables import _registry

    await _registry.get()
    await pilot_langgraph.aclose()
    await pilot_langgraph.aclose()  # must not raise
    assert len(_registry._by_loop) == 0


async def test_aclose_with_no_active_connections():
    """aclose() works even if the registry has nothing cached."""
    import pilot_langgraph
    from pilot_langgraph.runnables import _registry
    _registry._by_loop.clear()
    await pilot_langgraph.aclose()  # must not raise
    assert len(_registry._by_loop) == 0


def test_aclose_sync_works_outside_event_loop():
    """The sync wrapper drives a private background loop."""
    import pilot_langgraph
    pilot_langgraph.aclose_sync()  # must not raise


def test_aclose_stops_the_sync_loop_thread():
    """A single aclose_sync() call also tears down the sync_loop background thread."""
    import pilot_langgraph
    from pilot_langgraph.runnables import _registry
    # Touch the sync loop so it gets created.
    loop = _registry.sync_loop()
    assert loop is not None and loop.is_running()
    pilot_langgraph.aclose_sync()
    # The shutdown is async via call_soon_threadsafe; give it a beat.
    import time
    for _ in range(20):
        if not loop.is_running():
            break
        time.sleep(0.05)
    assert not loop.is_running()
