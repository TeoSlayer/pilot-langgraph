"""Prometheus-format exposition of WorkerServer counters.

Spawned by the worker CLI when `--metrics-port N` is set. Stdlib `http.server`,
no new deps. Single endpoint `/metrics` returns the counters; `/healthz`
returns 200 OK for k8s liveness probes.

Format: standard Prometheus text exposition, e.g.

    # HELP pilot_worker_uptime_seconds Worker process uptime in seconds.
    # TYPE pilot_worker_uptime_seconds gauge
    pilot_worker_uptime_seconds 1234.5
    # HELP pilot_handler_calls_total Total handler invocations.
    # TYPE pilot_handler_calls_total counter
    pilot_handler_calls_total{handler="enrich"} 42
    pilot_handler_calls_total{handler="search"} 17
    pilot_handler_errors_total{handler="enrich"} 0
    pilot_handler_latency_ms_p50{handler="enrich"} 12.3
    pilot_handler_latency_ms_p95{handler="enrich"} 45.7

The endpoint is intentionally read-only and free of authentication — bind to
loopback or behind a sidecar if exposed off-host.
"""
from __future__ import annotations

import logging
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .server import WorkerServer

log = logging.getLogger(__name__)


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render(server: "WorkerServer") -> str:
    """Render the current worker state as Prometheus text exposition."""
    lines: list[str] = []
    uptime = (time.monotonic() - server._started_at) if server._started_at else 0.0

    def metric(name: str, help_text: str, mtype: str, samples: list[tuple[dict[str, str], float]]) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {mtype}")
        for labels, value in samples:
            if labels:
                label_str = "{" + ",".join(
                    f'{k}="{_escape_label(v)}"' for k, v in sorted(labels.items())
                ) + "}"
            else:
                label_str = ""
            # Prometheus accepts both int and float; emit floats as-is, ints unsuffixed
            if isinstance(value, float) and value.is_integer():
                rendered = f"{int(value)}"
            elif isinstance(value, int):
                rendered = f"{value}"
            else:
                rendered = f"{value:.6g}"
            lines.append(f"{name}{label_str} {rendered}")

    metric("pilot_worker_uptime_seconds",
           "Worker process uptime in seconds.", "gauge",
           [({}, round(uptime, 1))])
    metric("pilot_worker_inflight_handlers",
           "Currently-executing handler tasks.", "gauge",
           [({}, sum(1 for t in server._inflight if not t.done()))])
    metric("pilot_worker_calls_total",
           "Total handler invocations across all handlers.", "counter",
           [({}, server._total_calls)])
    metric("pilot_worker_errors_total",
           "Total handler errors across all handlers.", "counter",
           [({}, server._total_errors)])

    handler_samples_calls: list[tuple[dict[str, str], float]] = []
    handler_samples_errors: list[tuple[dict[str, str], float]] = []
    handler_samples_p50: list[tuple[dict[str, str], float]] = []
    handler_samples_p95: list[tuple[dict[str, str], float]] = []
    for name, reg in sorted(server._handlers.items()):
        labels = {"handler": name}
        handler_samples_calls.append((labels, float(reg.stats.calls)))
        handler_samples_errors.append((labels, float(reg.stats.errors)))
        handler_samples_p50.append((labels, reg.stats.percentile(0.50)))
        handler_samples_p95.append((labels, reg.stats.percentile(0.95)))

    metric("pilot_handler_calls_total",
           "Total invocations per handler.", "counter", handler_samples_calls)
    metric("pilot_handler_errors_total",
           "Total errors per handler.", "counter", handler_samples_errors)
    metric("pilot_handler_latency_ms_p50",
           "Median latency over the last 256 calls (ms).", "gauge", handler_samples_p50)
    metric("pilot_handler_latency_ms_p95",
           "P95 latency over the last 256 calls (ms).", "gauge", handler_samples_p95)

    return "\n".join(lines) + "\n"


def serve(server: "WorkerServer", *, host: str = "127.0.0.1", port: int = 9090) -> ThreadingHTTPServer:
    """Start a background HTTP server exposing /metrics + /healthz.

    Returns the started server so the caller can `shutdown()` it on worker exit.
    """
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # silence default access log
            log.debug("metrics: " + fmt, *args)

        def do_GET(self):  # noqa: N802
            if self.path == "/metrics":
                body = render(server).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif self.path == "/healthz":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"ok\n")
            else:
                self.send_response(404)
                self.end_headers()

    httpd = ThreadingHTTPServer((host, port), _Handler)
    t = threading.Thread(target=httpd.serve_forever, name="pilot-metrics", daemon=True)
    t.start()
    log.info("metrics: serving on http://%s:%d/metrics", host, port)
    return httpd


def free_port() -> int:
    """Test helper: pick an OS-assigned free port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
