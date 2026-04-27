"""End-to-end LangGraph extension demo against a real remote worker.

Topology:
    [local laptop]                                  [remote pilot worker]
       Grok planner                                  WorkerRouter
            |                                         (port 5000)
            v                                            |
       PilotRemoteRunnable -- encrypted UDP tunnel -->  enrich
            ^                                            |
            |                                            v
       Grok summarizer  <--- reply over Pilot tunnel ---'

Proves:
- LangGraph state crosses the public internet via Pilot tunnels
- A *custom* handler runs on the remote (not just an echo service)
- The remote's processing receipt (hostname, pid, timestamp) appears in the
  caller's graph state, attesting that the work happened on the worker
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, TypedDict

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_xai import ChatXAI
from langgraph.graph import END, START, StateGraph

from pilot_langgraph import PilotConnection, PilotRemoteRunnable

load_dotenv()

REMOTE_PEER = os.environ.get("PILOT_REMOTE_PEER", "").strip()
if not REMOTE_PEER:
    sys.exit("set PILOT_REMOTE_PEER to a pilot worker address")
REMOTE_HANDLER = os.environ.get("PILOT_REMOTE_HANDLER", "enrich")


class GraphState(TypedDict, total=False):
    user_question: str
    plan: str
    remote_result: dict[str, Any]
    answer: str


def llm() -> ChatXAI:
    return ChatXAI(model="grok-3-mini", api_key=os.environ["XAI_API_KEY"], temperature=0.2)


def planner(state: GraphState) -> dict:
    msg = llm().invoke([
        SystemMessage("Output a one-sentence research plan. No prose, just the plan."),
        HumanMessage(state["user_question"]),
    ])
    return {"plan": msg.content.strip()}


def summarizer(state: GraphState) -> dict:
    rr = state.get("remote_result", {})
    receipt = rr.get("remote_receipt", {})
    msg = llm().invoke([
        SystemMessage(
            "You are a concise answerer. The user asked a question. A planner produced a plan. "
            f"That plan was sent over a Pilot Protocol encrypted UDP tunnel to a remote worker "
            f"(host={receipt.get('processed_on_host')}, pid={receipt.get('worker_pid')}, "
            f"at={receipt.get('processed_at_utc')}). The worker enriched it with "
            f"input_size_bytes={rr.get('input_size_bytes')}. "
            "Now produce a 2-sentence answer to the user's question, then a third sentence "
            "noting the distributed execution succeeded across the network."
        ),
        HumanMessage(f"Question: {state['user_question']}\nPlan: {state['plan']}"),
    ])
    return {"answer": msg.content.strip()}


def build_graph():
    remote = PilotRemoteRunnable(node=REMOTE_HANDLER, peer=REMOTE_PEER, timeout_secs=90)

    def remote_worker(state: GraphState) -> dict:
        # Send only the plan over the wire, nest result under remote_result.
        result = remote.invoke({"plan": state.get("plan", "")})
        return {"remote_result": result}

    g = StateGraph(GraphState)
    g.add_node("planner", planner)
    g.add_node("remote_worker", remote_worker)
    g.add_node("summarizer", summarizer)

    g.add_edge(START, "planner")
    g.add_edge("planner", "remote_worker")
    g.add_edge("remote_worker", "summarizer")
    g.add_edge("summarizer", END)
    return g.compile()


def main():
    import asyncio
    async def _info():
        async with await PilotConnection.connect() as c:
            return await c.info()
    info = asyncio.run(_info())
    print(f"local daemon: {info.get('hostname')} @ {info.get('address')}")
    print(f"remote peer:  {REMOTE_PEER}  handler: {REMOTE_HANDLER}")

    graph = build_graph()
    initial: GraphState = {
        "user_question": "In one sentence, what does Pilot Protocol give AI agents that the public internet does not?"
    }

    print("\n--- invoking distributed graph ---")
    final = graph.invoke(initial)

    print("\n--- result ---")
    print("plan:         ", final.get("plan"))
    rr = final.get("remote_result", {})
    print(f"remote_result.input_payload  : {rr.get('input_payload')!r:.120}")
    print(f"remote_result.input_size_bytes: {rr.get('input_size_bytes')}")
    print(f"remote_result.remote_receipt : {rr.get('remote_receipt')}")
    print(f"answer:        {final.get('answer')}")
    return final


if __name__ == "__main__":
    out = main()
    print("\n--- raw ---")
    print(json.dumps(out, indent=2, default=str))
