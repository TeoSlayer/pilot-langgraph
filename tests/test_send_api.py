"""LangGraph `Send` API + PilotRemoteRunnable: dynamic parallel dispatch.

Send is the canonical LangGraph idiom for spawning N parallel branches at
runtime (vs. compile-time fanout via PilotFanoutRunnable). A node returns
`[Send("target_node", state), ...]` and LangGraph dispatches each to
"target_node" in parallel. We verify PilotRemoteRunnable works as that
target — proving the standard LangGraph parallel pattern composes naturally
with remote execution.
"""
from __future__ import annotations

import operator
from typing import Annotated, TypedDict

import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


class _State(TypedDict, total=False):
    queries: list[str]
    # Annotated with operator.add so each parallel branch's append accumulates
    # in the final state instead of overwriting.
    results: Annotated[list[dict], operator.add]


async def test_send_dispatches_parallel_to_pilot_runnable(worker_peer):
    """Planner returns N Sends; each goes to the remote node concurrently."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")

    from langgraph.types import Send
    from langgraph.graph import END, START, StateGraph

    from pilot_langgraph import PilotRemoteRunnable

    remote = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=30)

    # The remote handler returns its result as a dict; wrap so it merges into
    # the accumulated `results` list.
    async def call_remote(state) -> dict:
        out = await remote.ainvoke(state)
        return {"results": [out]}

    def planner(state: _State):
        # Dynamic fanout: one Send per query.
        return [Send("worker", {"q": q}) for q in state["queries"]]

    g = StateGraph(_State)
    g.add_node("worker", call_remote)
    g.add_conditional_edges(START, planner, ["worker"])
    g.add_edge("worker", END)
    app = g.compile()

    final = await app.ainvoke({"queries": ["alpha", "beta", "gamma"]})
    assert len(final["results"]) == 3
    # Each result should echo back the query it was sent
    received_queries = sorted([r["input_payload"]["q"] for r in final["results"]])
    assert received_queries == ["alpha", "beta", "gamma"]
    # Every branch ran on the worker
    assert all("remote_receipt" in r for r in final["results"])
