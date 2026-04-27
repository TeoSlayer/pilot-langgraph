"""Live test of PilotCheckpointSaver round-tripping a real LangGraph thread."""
from __future__ import annotations

from typing import TypedDict

import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


class _State(TypedDict, total=False):
    n: int
    log: list[str]


def _step1(s: _State) -> dict:
    return {"n": s.get("n", 0) + 1, "log": [*s.get("log", []), "step1"]}


def _step2(s: _State) -> dict:
    return {"n": s.get("n", 0) * 2, "log": [*s.get("log", []), "step2"]}


def test_checkpoint_persists_across_graph_instances(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER=<addr> to run worker tests")

    from langgraph.graph import END, START, StateGraph

    from pilot_langgraph import PilotCheckpointSaver

    saver = PilotCheckpointSaver(peer=worker_peer, port=5000, timeout_secs=30)
    cfg = {"configurable": {"thread_id": "pytest-thread-1"}}

    def build():
        g = StateGraph(_State)
        g.add_node("a", _step1)
        g.add_node("b", _step2)
        g.add_edge(START, "a")
        g.add_edge("a", "b")
        g.add_edge("b", END)
        return g.compile(checkpointer=saver)

    g1 = build()
    final = g1.invoke({"n": 3, "log": []}, cfg)
    assert final["n"] == 8

    g2 = build()  # fresh instance, simulates new process
    snap = g2.get_state(cfg)
    assert snap.values["n"] == 8
    assert "step1" in snap.values["log"]
    assert "step2" in snap.values["log"]


def test_checkpoint_history_orders_newest_first(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER=<addr> to run worker tests")

    from langgraph.graph import END, START, StateGraph

    from pilot_langgraph import PilotCheckpointSaver

    saver = PilotCheckpointSaver(peer=worker_peer, port=5000, timeout_secs=30)
    cfg = {"configurable": {"thread_id": "pytest-thread-history"}}

    def build():
        g = StateGraph(_State)
        g.add_node("a", _step1)
        g.add_node("b", _step2)
        g.add_edge(START, "a")
        g.add_edge("a", "b")
        g.add_edge("b", END)
        return g.compile(checkpointer=saver)

    g = build()
    g.invoke({"n": 1, "log": []}, cfg)

    history = list(g.get_state_history(cfg))
    assert len(history) >= 2
    # First entry should be the latest (highest n value after both steps)
    assert history[0].values.get("n") == 4
