"""Prometheus metrics endpoint tests."""
from __future__ import annotations

import json
import time
import urllib.request

import pytest

from pilot_langgraph._ipc import Addr
from pilot_langgraph.metrics import free_port, render, serve
from pilot_langgraph.server import WorkerServer


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


def _build_server_with_traffic() -> WorkerServer:
    """Synthetic server with a handful of canned-stat handlers."""
    s = WorkerServer(port=0)
    s._started_at = time.monotonic() - 30  # pretend 30s uptime
    s._total_calls = 17
    s._total_errors = 2

    s.register("enrich", lambda p: p)
    s._handlers["enrich"].stats.calls = 10
    s._handlers["enrich"].stats.errors = 1
    s._handlers["enrich"].stats.latencies_ms.extend([5.0, 10.0, 20.0, 50.0, 100.0])

    s.register("search", lambda p: p)
    s._handlers["search"].stats.calls = 5
    s._handlers["search"].stats.errors = 0
    return s


class TestRender:
    def test_includes_uptime_and_totals(self):
        text = render(_build_server_with_traffic())
        assert "pilot_worker_uptime_seconds" in text
        assert "pilot_worker_calls_total 17" in text
        assert "pilot_worker_errors_total 2" in text

    def test_per_handler_labels(self):
        text = render(_build_server_with_traffic())
        assert 'pilot_handler_calls_total{handler="enrich"} 10' in text
        assert 'pilot_handler_calls_total{handler="search"} 5' in text
        assert 'pilot_handler_errors_total{handler="enrich"} 1' in text

    def test_p50_p95_present(self):
        text = render(_build_server_with_traffic())
        assert 'pilot_handler_latency_ms_p50{handler="enrich"}' in text
        assert 'pilot_handler_latency_ms_p95{handler="enrich"}' in text

    def test_includes_help_and_type_lines(self):
        text = render(_build_server_with_traffic())
        # Standard prom format requires HELP and TYPE before each metric
        assert text.count("# HELP ") >= 6
        assert text.count("# TYPE ") >= 6


class TestHttpServer:
    def test_metrics_endpoint_serves_text(self):
        s = _build_server_with_traffic()
        port = free_port()
        httpd = serve(s, host="127.0.0.1", port=port)
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as r:
                assert r.status == 200
                body = r.read().decode()
                assert "pilot_worker_calls_total" in body
                assert "Content-Type" in r.headers and "text/plain" in r.headers["Content-Type"]
        finally:
            httpd.shutdown()

    def test_healthz_returns_200(self):
        s = _build_server_with_traffic()
        port = free_port()
        httpd = serve(s, host="127.0.0.1", port=port)
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2) as r:
                assert r.status == 200
                assert r.read() == b"ok\n"
        finally:
            httpd.shutdown()

    def test_unknown_path_returns_404(self):
        s = _build_server_with_traffic()
        port = free_port()
        httpd = serve(s, host="127.0.0.1", port=port)
        try:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/unknown", timeout=2)
                assert False, "expected 404"
            except urllib.error.HTTPError as e:
                assert e.code == 404
        finally:
            httpd.shutdown()


@pytest.mark.asyncio
async def test_metrics_reflect_real_traffic():
    """Drive the server with synthetic calls; verify counters update."""
    s = WorkerServer(port=0)
    s._started_at = time.monotonic()

    def ok(_p): return {"x": 1}
    def fail(_p): raise ValueError("nope")

    s.register("ok", ok)
    s.register("fail", fail)

    addr = Addr(network=0, node=1)
    for _ in range(3):
        f = _FakeStream(json.dumps({"node": "ok", "payload": None}).encode(), addr)
        await s._handle_one(f)
    f = _FakeStream(json.dumps({"node": "fail", "payload": None}).encode(), addr)
    await s._handle_one(f)

    text = render(s)
    assert 'pilot_handler_calls_total{handler="ok"} 3' in text
    assert 'pilot_handler_calls_total{handler="fail"} 1' in text
    assert 'pilot_handler_errors_total{handler="fail"} 1' in text
    assert "pilot_worker_calls_total 4" in text
    assert "pilot_worker_errors_total 1" in text
