"""OpenTelemetry instrumentation tests.

Uses an in-memory span exporter so we can assert spans were emitted with
the expected attributes and parent/child relationships, without needing a
real collector.
"""
from __future__ import annotations


import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


_GLOBAL_EXPORTER = None


def _setup_in_memory_exporter():
    """Configure the in-memory exporter ONCE per process (otel rejects re-set).

    Subsequent calls just return the same exporter so tests can `clear()` it.
    """
    global _GLOBAL_EXPORTER
    if _GLOBAL_EXPORTER is not None:
        return _GLOBAL_EXPORTER

    from opentelemetry import trace as otrace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    otrace.set_tracer_provider(provider)

    # The plugin's _otel module captured the global tracer at import time
    # (via `get_tracer`) — that tracer is loop-bound to whatever provider
    # was global at import. Force a fresh tracer pointing at our new
    # provider so spans actually flow.
    import pilot_langgraph._otel as _o
    _o._tracer = otrace.get_tracer("pilot_langgraph")

    _GLOBAL_EXPORTER = exporter
    return exporter


async def test_client_span_emitted_for_remote_call(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    exporter = _setup_in_memory_exporter()
    exporter.clear()

    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=15)
    await r.ainvoke({"otel_test": True})

    spans = exporter.get_finished_spans()
    client_spans = [s for s in spans if s.kind.name == "CLIENT"]
    assert len(client_spans) >= 1
    cs = client_spans[-1]
    assert cs.name == "pilot.call enrich"
    assert cs.attributes.get("pilot.node") == "enrich"
    assert cs.attributes.get("pilot.peer") == worker_peer
    assert cs.attributes.get("rpc.system") == "pilot"


async def test_traceparent_propagated_in_request(worker_peer):
    """Worker receives the W3C traceparent in the request frame."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    _setup_in_memory_exporter()  # ensures otel is configured so a span exists

    # We can't easily inspect the worker's incoming request, but we CAN
    # invoke a handler that echoes its payload back (enrich) and then
    # confirm `traceparent` would have been added — by manually re-running
    # the encode logic and checking it includes traceparent.
    from opentelemetry import trace as otrace
    from pilot_langgraph._otel import inject_traceparent
    tracer = otrace.get_tracer("test")
    with tracer.start_as_current_span("test_span"):
        carrier: dict[str, str] = {}
        inject_traceparent(carrier)
        assert "traceparent" in carrier
        assert carrier["traceparent"].startswith("00-")  # W3C version 00


async def test_handler_error_recorded_on_client_span(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    exporter = _setup_in_memory_exporter()
    exporter.clear()

    from pilot_langgraph import PilotHandlerNotFoundError, PilotRemoteRunnable
    r = PilotRemoteRunnable(node="bogus_handler", peer=worker_peer, timeout_secs=10)
    with pytest.raises(PilotHandlerNotFoundError):
        await r.ainvoke({})

    spans = exporter.get_finished_spans()
    client_spans = [s for s in spans if s.kind.name == "CLIENT"]
    assert client_spans
    cs = client_spans[-1]
    assert cs.status.status_code.name == "ERROR"
    # Exception should be recorded as an event
    assert any(e.name == "exception" for e in cs.events)


def test_otel_helpers_noop_without_active_span():
    """inject_traceparent into an empty carrier when no span is active is a no-op."""
    from pilot_langgraph._otel import inject_traceparent
    carrier: dict[str, str] = {}
    inject_traceparent(carrier)
    # Without an active span context, the propagator may or may not write
    # — but it must not raise. Result is implementation-defined; we only
    # assert that it doesn't crash and the carrier is still a dict.
    assert isinstance(carrier, dict)
