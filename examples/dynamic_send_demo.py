"""LangGraph `Send` dynamic parallel dispatch to a remote pilot worker.

Demonstrates the canonical multi-agent pattern: a planner produces N tasks
at runtime, each dispatched to the SAME worker node in parallel via Send.
The aggregator reduces all results back into a single graph state.

Topology:
    planner    ──[ Send(worker, q1), Send(worker, q2), ... ]──►   worker
                                                                  worker     ──► END
                                                                  worker
                                                                    │
                                                                    └─ all branches reduce
                                                                       into one accumulated
                                                                       results[] via operator.add
"""
from __future__ import annotations

import asyncio
import operator
import os
import sys
from typing import Annotated, TypedDict

from langgraph.types import Send
from langgraph.graph import END, START, StateGraph

from pilot_langgraph import PilotRemoteRunnable


WORKER = os.environ.get("PILOT_REMOTE_PEER", "").strip()
if not WORKER:
    sys.exit("set PILOT_REMOTE_PEER to a pilot worker address")


class State(TypedDict, total=False):
    topic: str
    queries: list[str]
    results: Annotated[list[dict], operator.add]


def split_into_queries(state: State) -> dict:
    """Sync planner: turn one topic into multiple search queries."""
    queries = [
        f"{state['topic']}: surface look",
        f"{state['topic']}: deep dive",
        f"{state['topic']}: skeptical critique",
        f"{state['topic']}: synthesis",
    ]
    return {"queries": queries}


def fan_out(state: State):
    """Edge: emit one Send per query for parallel remote dispatch."""
    return [Send("worker", {"q": q}) for q in state["queries"]]


async def call_remote_worker(state) -> dict:
    """Invoke the remote handler. The state arg here is whatever the Send carried."""
    runnable = PilotRemoteRunnable(node="enrich", peer=WORKER, timeout_secs=30)
    result = await runnable.ainvoke(state)
    return {"results": [result]}


async def main() -> None:
    g = StateGraph(State)
    g.add_node("plan", split_into_queries)
    g.add_node("worker", call_remote_worker)
    g.add_edge(START, "plan")
    g.add_conditional_edges("plan", fan_out, ["worker"])
    g.add_edge("worker", END)
    app = g.compile()

    print(f"target worker: {WORKER}\n")
    final = await app.ainvoke({"topic": "Pilot Protocol"})
    print(f"planner produced {len(final['queries'])} queries; got {len(final['results'])} parallel results\n")
    for r in final["results"]:
        q = r["input_payload"]["q"]
        host = r["remote_receipt"]["processed_on_host"]
        print(f"  - {q!r} -> processed on {host}")


if __name__ == "__main__":
    asyncio.run(main())
