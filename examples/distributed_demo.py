"""End-to-end demo: a 3-node LangGraph whose middle node round-trips state
across a real Pilot Protocol tunnel to the public `agent-alpha` peer.

Flow:
    user_question -> planner (Grok, local)
                  -> remote_round_trip (Pilot tunnel to agent-alpha echo)
                  -> summarizer (Grok, local)
                  -> final_answer

This proves graph state survives serialization, transit over Pilot's
encrypted UDP tunnel to a daemon on a different machine, and round-trip
back into the local LangGraph runtime.
"""
from __future__ import annotations

import asyncio
import json
import os
from typing import TypedDict

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_xai import ChatXAI
from langgraph.graph import END, START, StateGraph

from pilot_langgraph import PilotConnection, PilotEchoRunnable

load_dotenv()


class GraphState(TypedDict, total=False):
    user_question: str
    plan: str
    round_trip: dict
    answer: str


def build_llm() -> ChatXAI:
    return ChatXAI(model="grok-3-mini", api_key=os.environ["XAI_API_KEY"], temperature=0.3)


def planner(state: GraphState) -> dict:
    llm = build_llm()
    msg = llm.invoke([
        SystemMessage("You are a planner. Given a user question, output a one-sentence research plan. No prose, just the plan."),
        HumanMessage(state["user_question"]),
    ])
    return {"plan": msg.content.strip()}


def summarizer(state: GraphState) -> dict:
    llm = build_llm()
    rt = state.get("round_trip", {})
    msg = llm.invoke([
        SystemMessage(
            "You are a concise answerer. The user asked a question. A planner "
            "produced a plan. The plan was then transmitted across a real Pilot "
            "Protocol tunnel to peer `{peer}` (address {addr}) and round-tripped "
            "back ({bytes} bytes, integrity={ok}). Now produce a 2-sentence final "
            "answer to the user's original question, and append one sentence noting "
            "the distributed round-trip completed successfully.".format(
                peer=rt.get("peer", "?"),
                addr=rt.get("target_address", "?"),
                bytes=rt.get("echoed_bytes", 0),
                ok=rt.get("round_trip_ok", False),
            )
        ),
        HumanMessage(f"Question: {state['user_question']}\nPlan that travelled the tunnel: {state['plan']}"),
    ])
    return {"answer": msg.content.strip()}


def build_graph(peer: str = "agent-alpha"):
    echo = PilotEchoRunnable(peer=peer)

    def round_trip(state: GraphState) -> dict:
        return {"round_trip": echo.invoke(state)}

    g = StateGraph(GraphState)
    g.add_node("planner", planner)
    g.add_node("remote_round_trip", round_trip)
    g.add_node("summarizer", summarizer)

    g.add_edge(START, "planner")
    g.add_edge("planner", "remote_round_trip")
    g.add_edge("remote_round_trip", "summarizer")
    g.add_edge("summarizer", END)
    return g.compile()


def main():
    async def _info():
        async with await PilotConnection.connect() as c:
            return await c.info()
    info = asyncio.run(_info())
    print(f"local daemon: {info['hostname']} @ {info['address']} (peers={info.get('peers', 0)})")

    peer = os.environ.get("PILOT_REMOTE_PEER", "agent-alpha")
    print(f"remote peer:  {peer}")

    graph = build_graph(peer=peer)
    initial: GraphState = {"user_question": "In one sentence, what does Pilot Protocol give AI agents that the public internet does not?"}

    print("\n--- invoking graph ---")
    final = graph.invoke(initial)

    print("\n--- result ---")
    print("plan:        ", final.get("plan"))
    rt = final.get("round_trip", {})
    print(f"round_trip:   peer={rt.get('peer')} addr={rt.get('target_address')} bytes={rt.get('echoed_bytes')} ok={rt.get('round_trip_ok')}")
    print("answer:      ", final.get("answer"))

    return final


if __name__ == "__main__":
    out = main()
    print("\n--- raw final state ---")
    print(json.dumps(out, indent=2, default=str))
