"""Per-caller rate limiting tests."""
from __future__ import annotations

import json

import pytest

from pilot_langgraph._ipc import Addr
from pilot_langgraph.server import WorkerServer, _RateLimiter


class _FakeStream:
    def __init__(self, request: bytes, peer_addr: Addr):
        self._frames_in = [request, b""]
        self.peer = (peer_addr, 49000)
        self.frames_out: list[bytes] = []

    async def read(self, timeout=None) -> bytes:
        return self._frames_in.pop(0) if self._frames_in else b""

    async def write(self, data: bytes) -> None:
        self.frames_out.append(data)

    async def close(self) -> None:
        pass


def _last_reply(fake) -> dict:
    line = fake.frames_out[-1].rstrip(b"\n").split(b"\n")[-1]
    return json.loads(line)


class TestSlidingWindow:
    def test_allows_under_limit(self):
        rl = _RateLimiter(limit=3, window_secs=10.0)
        for i in range(3):
            assert rl.consume(caller_node_id=42, now=100.0 + i * 0.1) is True

    def test_denies_at_limit(self):
        rl = _RateLimiter(limit=3, window_secs=10.0)
        now = 100.0
        for _ in range(3):
            rl.consume(42, now)
        assert rl.consume(42, now) is False

    def test_window_slides_to_allow_new_calls(self):
        rl = _RateLimiter(limit=3, window_secs=10.0)
        for _ in range(3):
            rl.consume(42, 100.0)
        # 11s later, the old 3 should have aged out
        assert rl.consume(42, 111.0) is True

    def test_per_caller_isolation(self):
        rl = _RateLimiter(limit=2, window_secs=10.0)
        rl.consume(1, 100.0)
        rl.consume(1, 100.0)
        assert rl.consume(1, 100.0) is False
        # caller 2 has its own deque
        assert rl.consume(2, 100.0) is True
        assert rl.consume(2, 100.0) is True
        assert rl.consume(2, 100.0) is False


@pytest.mark.asyncio
async def test_rate_limit_denies_typed_error():
    s = WorkerServer(port=0)
    s.register("hot", lambda p: {"ok": True}, rate_per_caller=2, rate_window_secs=60)
    addr = Addr(network=0, node=42)

    # First two pass
    for _ in range(2):
        f = _FakeStream(json.dumps({"node": "hot", "payload": None}).encode(), addr)
        await s._handle_one(f)
        assert _last_reply(f)["ok"] is True

    # Third hits the limit
    f = _FakeStream(json.dumps({"node": "hot", "payload": None}).encode(), addr)
    await s._handle_one(f)
    reply = _last_reply(f)
    assert reply["ok"] is False
    assert reply["error_type"] == "rate_limited"
    assert "2/60s" in reply["error"]


@pytest.mark.asyncio
async def test_rate_limit_per_caller_independent():
    """A caller hitting the limit doesn't affect a different caller."""
    s = WorkerServer(port=0)
    s.register("hot", lambda p: {"ok": True}, rate_per_caller=1, rate_window_secs=60)

    a = Addr(network=0, node=1)
    b = Addr(network=0, node=2)

    fa1 = _FakeStream(json.dumps({"node": "hot", "payload": None}).encode(), a)
    await s._handle_one(fa1)
    assert _last_reply(fa1)["ok"] is True

    fa2 = _FakeStream(json.dumps({"node": "hot", "payload": None}).encode(), a)
    await s._handle_one(fa2)
    assert _last_reply(fa2)["error_type"] == "rate_limited"

    fb1 = _FakeStream(json.dumps({"node": "hot", "payload": None}).encode(), b)
    await s._handle_one(fb1)
    assert _last_reply(fb1)["ok"] is True


@pytest.mark.asyncio
async def test_no_rate_limit_by_default():
    s = WorkerServer(port=0)
    s.register("free", lambda p: {"ok": True})
    addr = Addr(network=0, node=1)
    for _ in range(20):
        f = _FakeStream(json.dumps({"node": "free", "payload": None}).encode(), addr)
        await s._handle_one(f)
        assert _last_reply(f)["ok"] is True


def test_pilot_handler_decorator_carries_rate_limit():
    from pilot_langgraph.server import consume_global_handlers, pilot_handler

    consume_global_handlers()  # clear

    @pilot_handler("hot", rate_per_caller=5, rate_window_secs=30)
    def hot(p): return p

    handlers = consume_global_handlers()
    rl = handlers["hot"].rate_limiter
    assert rl is not None
    assert rl.limit == 5
    assert rl.window_secs == 30


def test_typed_error_subclass():
    from pilot_langgraph import PilotRateLimitError, PilotRemoteError
    assert issubclass(PilotRateLimitError, PilotRemoteError)
    assert PilotRateLimitError.error_type == "rate_limited"
