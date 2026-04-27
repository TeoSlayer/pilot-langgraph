"""PilotChannel pub/sub tests.

Codec tests are pure-unit. Live tests use the worker's built-in event broker
on port 1002 (PortEventStream) — no extra worker handlers needed.
"""
from __future__ import annotations

import asyncio

import pytest

from pilot_langgraph._ipc import encode_event, split_events

from .conftest import requires_daemon


# ---- pure codec ----

class TestEventCodec:
    def test_round_trip_one_event(self):
        out, leftover = split_events(encode_event("alerts", b"hello"))
        assert len(out) == 1
        assert out[0].topic == "alerts"
        assert out[0].payload == b"hello"
        assert leftover == b""

    def test_round_trip_multiple_events(self):
        buf = encode_event("a", b"1") + encode_event("bb", b"22") + encode_event("ccc", b"333")
        out, leftover = split_events(buf)
        assert [(e.topic, e.payload) for e in out] == [("a", b"1"), ("bb", b"22"), ("ccc", b"333")]
        assert leftover == b""

    def test_partial_event_returned_as_leftover(self):
        full = encode_event("topic", b"payload")
        out, leftover = split_events(full[:5])  # halfway through topic
        assert out == []
        assert leftover == full[:5]

    def test_partial_after_complete(self):
        complete = encode_event("a", b"x")
        partial = encode_event("b", b"yy")[:4]
        out, leftover = split_events(complete + partial)
        assert len(out) == 1
        assert leftover == partial

    def test_empty_payload(self):
        out, _ = split_events(encode_event("subscribe", b""))
        assert out[0].topic == "subscribe"
        assert out[0].payload == b""


# ---- live pub/sub against worker's broker ----

pytestmark_live = [requires_daemon, pytest.mark.asyncio]


@pytest.mark.asyncio
async def test_publish_subscribe_round_trip(worker_peer):
    """Publish to peer's broker, receive on a subscription to the same peer/topic."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotChannel
    sub = await PilotChannel.subscribe("pytest-channel", peer=worker_peer, timeout_secs=20)
    async with sub:
        # Tiny gap so the subscribe registers before the publish fires.
        await asyncio.sleep(0.5)

        async def _delayed_publish():
            await asyncio.sleep(0.2)
            await PilotChannel.publish_one("pytest-channel", b"hi-from-test", peer=worker_peer, timeout_secs=15)

        pub_task = asyncio.create_task(_delayed_publish())
        ev = await sub.recv(timeout=10)
        assert ev.topic == "pytest-channel"
        assert ev.payload == b"hi-from-test"
        await pub_task


@pytest.mark.asyncio
async def test_wildcard_subscription_receives_any_topic(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotChannel
    async with await PilotChannel.subscribe("*", peer=worker_peer, timeout_secs=20) as sub:
        await asyncio.sleep(0.5)
        await PilotChannel.publish_one("topic-a", b"alpha", peer=worker_peer, timeout_secs=15)
        ev = await sub.recv(timeout=10)
        assert ev.payload == b"alpha"


@pytest.mark.asyncio
async def test_publisher_sends_multiple_events(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotChannel
    async with await PilotChannel.subscribe("multi", peer=worker_peer, timeout_secs=20) as sub:
        await asyncio.sleep(0.5)
        async with await PilotChannel.publisher(peer=worker_peer, timeout_secs=15) as pub:
            for i in range(3):
                await pub.publish("multi", f"event-{i}".encode())
        received = []
        for _ in range(3):
            received.append((await sub.recv(timeout=5)).payload)
        assert received == [b"event-0", b"event-1", b"event-2"]


@pytest.mark.asyncio
async def test_event_source_runnable(worker_peer):
    """PilotEventSource as a LangGraph-shaped Runnable."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotChannel, PilotEventSource
    src = PilotEventSource(topic="rs-test", peer=worker_peer, max_events=2)

    async def _publish():
        await asyncio.sleep(0.7)
        async with await PilotChannel.publisher(peer=worker_peer, timeout_secs=15) as pub:
            await pub.publish("rs-test", b"a")
            await pub.publish("rs-test", b"b")

    pub_task = asyncio.create_task(_publish())
    out = []
    async for ev in src.astream(None):
        out.append(ev)
    await pub_task
    assert len(out) == 2
    assert out[0]["payload"] == "a"
    assert out[1]["payload"] == "b"
