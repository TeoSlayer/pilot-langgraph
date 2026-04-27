"""Smallest no-LLM distributed-LangGraph demo.

A 3-node LangGraph: input → remote `compute_hash` (on the worker) → output.
The graph state crosses a real Pilot tunnel to the worker, gets SHA-256'd
remotely (with the worker's hostname + pid attached), and comes back.

No Grok / xAI / langchain-xai dependency — only `langgraph` + this plugin.
Run as the first thing after installing the plugin to verify your setup.
"""
from __future__ import annotations

import asyncio
import os
import sys
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from pilot_langgraph import PilotRemoteRunnable


WORKER = os.environ.get("PILOT_REMOTE_PEER", "").strip()
if not WORKER:
    sys.exit("set PILOT_REMOTE_PEER to a pilot worker address")


class State(TypedDict, total=False):
    text: str
    digest: str
    on_host: str


async def hash_remotely(state: State) -> dict:
    runnable = PilotRemoteRunnable(node="compute_hash", peer=WORKER, timeout_secs=15)
    result = await runnable.ainvoke({"text": state["text"]})
    return {
        "digest": result["sha256"],
        "on_host": result["remote_receipt"]["processed_on_host"],
    }


def report(state: State) -> dict:
    print(f"\nInput:    {state['text']!r}")
    print(f"SHA-256:  {state['digest']}")
    print(f"Computed on remote host: {state['on_host']}")
    return {}


async def main() -> None:
    g = StateGraph(State)
    g.add_node("hash", hash_remotely)
    g.add_node("report", report)
    g.add_edge(START, "hash")
    g.add_edge("hash", "report")
    g.add_edge("report", END)
    app = g.compile()

    print(f"Worker: {WORKER}\n")
    await app.ainvoke({"text": "Pilot Protocol gives AI agents addresses, ports, and trust."})


if __name__ == "__main__":
    asyncio.run(main())
