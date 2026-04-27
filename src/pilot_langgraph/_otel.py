"""Optional OpenTelemetry integration.

Imports `opentelemetry-api` lazily. If unavailable, every helper here is a
no-op so the rest of the plugin works unchanged. When installed, every
remote call emits a CLIENT span on the caller side and a SERVER span on
the worker side; W3C trace context (`traceparent`) is propagated in the
request frame so the two spans link into one trace.

Configure your own SDK + exporter once at process start (e.g. via
`opentelemetry-sdk` + `opentelemetry-exporter-otlp`) and spans flow.
"""
from __future__ import annotations

import logging
from typing import Any, Iterator
from contextlib import contextmanager

log = logging.getLogger(__name__)

try:
    from opentelemetry import context as _otel_context
    from opentelemetry import trace as _otel_trace
    from opentelemetry.propagate import extract as _otel_extract, inject as _otel_inject

    _AVAILABLE = True
    _tracer: Any = _otel_trace.get_tracer("pilot_langgraph")
    _SpanKind: Any = _otel_trace.SpanKind
    _Status: Any = _otel_trace.Status
    _StatusCode: Any = _otel_trace.StatusCode
except ImportError:  # opentelemetry-api not installed
    _AVAILABLE = False
    _otel_context = None  # type: ignore[assignment]
    _otel_trace = None  # type: ignore[assignment]
    _otel_extract = None  # type: ignore[assignment]
    _otel_inject = None  # type: ignore[assignment]
    _tracer = None
    _SpanKind = None
    _Status = None
    _StatusCode = None


def is_available() -> bool:
    return _AVAILABLE


def inject_traceparent(carrier: dict[str, str]) -> None:
    """Mutate `carrier` to add the active trace's W3C `traceparent` (and `tracestate`).

    No-op if otel-api isn't installed or no span is active.
    """
    if _AVAILABLE:
        _otel_inject(carrier)


def extract_context(carrier: dict[str, str]):
    """Return an otel Context from a carrier dict, or None if otel unavailable."""
    if not _AVAILABLE:
        return None
    return _otel_extract(carrier)


@contextmanager
def client_span(node: str, peer: str, attributes: dict | None = None) -> Iterator[Any]:
    """Start a CLIENT span around a remote pilot call. Yields the span (or None)."""
    if not _AVAILABLE:
        yield None
        return
    attrs: dict[str, Any] = {
        "pilot.node": node,
        "pilot.peer": peer,
        "rpc.system": "pilot",
        "rpc.service": "pilot_langgraph",
        "rpc.method": node,
    }
    if attributes:
        attrs.update(attributes)
    with _tracer.start_as_current_span(
        f"pilot.call {node}",
        kind=_SpanKind.CLIENT,
        attributes=attrs,
    ) as span:
        try:
            yield span
        except BaseException as e:
            span.set_status(_Status(_StatusCode.ERROR, str(e)))
            span.record_exception(e)
            raise


@contextmanager
def server_span(node: str, caller_node_id: int, parent_carrier: dict[str, str] | None) -> Iterator[Any]:
    """Start a SERVER span as a child of the propagated parent context."""
    if not _AVAILABLE:
        yield None
        return
    parent_ctx = _otel_extract(parent_carrier) if parent_carrier else None
    attrs = {
        "pilot.node": node,
        "pilot.caller_node_id": caller_node_id,
        "rpc.system": "pilot",
        "rpc.service": "pilot_langgraph",
        "rpc.method": node,
    }
    token = _otel_context.attach(parent_ctx) if parent_ctx else None
    try:
        with _tracer.start_as_current_span(
            f"pilot.handle {node}",
            kind=_SpanKind.SERVER,
            attributes=attrs,
        ) as span:
            try:
                yield span
            except BaseException as e:
                span.set_status(_Status(_StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise
    finally:
        if token is not None:
            _otel_context.detach(token)
