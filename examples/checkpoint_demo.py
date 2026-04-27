"""Demo: a LangGraph compiled with `checkpointer=PilotCheckpointSaver(...)`.

The graph runs locally; its state is persisted on a remote pilot peer. Two
process-level invocations are simulated: the first writes a checkpoint, the
second resumes from it. State survives because it lives across the network
on the worker, not in this process.
"""
from __future__ import annotations

import os
import sys
from typing import TypedDict

from dotenv import load_dotenv
from langgraph.graph import END, START, StateGraph

from pilot_langgraph import PilotCheckpointSaver

load_dotenv()

REMOTE_PEER = os.environ.get("PILOT_REMOTE_PEER", "").strip()
if not REMOTE_PEER:
    sys.exit("set PILOT_REMOTE_PEER to a pilot worker address (with checkpoint handlers)")


class State(TypedDict, total=False):
    value: int
    log: list[str]


def step_a(state: State) -> dict:
    log = list(state.get("log", []))
    log.append("step_a ran")
    return {"value": state.get("value", 0) + 1, "log": log}


def step_b(state: State) -> dict:
    log = list(state.get("log", []))
    log.append("step_b ran")
    return {"value": state.get("value", 0) * 10, "log": log}


def build_graph(saver):
    g = StateGraph(State)
    g.add_node("a", step_a)
    g.add_node("b", step_b)
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    return g.compile(checkpointer=saver)


def main():
    saver = PilotCheckpointSaver(peer=REMOTE_PEER, port=5000, timeout_secs=30)

    cfg = {"configurable": {"thread_id": "demo-thread-1"}}

    print("--- run 1: invoke from scratch ---")
    g = build_graph(saver)
    out = g.invoke({"value": 5, "log": []}, cfg)
    print("final state:", out)

    # Re-instantiate the graph (simulates a fresh process) and ask for the latest checkpoint.
    print("\n--- run 2: a fresh graph instance reads the same thread ---")
    g2 = build_graph(saver)
    snap = g2.get_state(cfg)
    print("retrieved state:", snap.values)
    print("retrieved metadata:", dict(snap.metadata))

    # List all checkpoints saved for this thread
    print("\n--- checkpoint history ---")
    for tup in g2.get_state_history(cfg):
        print(f"  cid={tup.config['configurable']['checkpoint_id']!s:.40} value={tup.values.get('value')!r}")


if __name__ == "__main__":
    main()
