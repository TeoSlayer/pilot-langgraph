"""End-to-end multi-agent research demo composing every feature.

What this exercises:

1. **Fanout** (`PilotFanoutRunnable`): one research question is dispatched to
   three remote researcher *angles* in parallel — fast, slow, contrarian —
   each running on the same worker but with different payloads.
2. **Streaming** (`PilotRemoteRunnable.astream`): each researcher streams its
   findings as it produces them, so partial results are visible immediately.
3. **ACL** (`@pilot_handler(..., allow=[...])`): the `evaluate` handler is
   restricted to specific caller node_ids — demonstrating that not every
   trusted peer gets to ask for a score.
4. **Checkpointer** (`PilotCheckpointSaver`): the graph state is persisted on
   the remote worker, so this script can be re-run with the same thread_id
   to resume.
5. **Callbacks**: a tiny LoggingCallbackHandler shows that every remote call
   emits `on_chain_start` / `on_chain_end` (so LangSmith would see them).
6. **Grok** (`langchain_xai`) composes the final answer using the ranked
   findings as ground truth.

Topology:
                                          [remote pilot worker]
        local laptop                       ┌────────────────────────────┐
        ┌─────────────────────┐            │  research(topic, angle=X)  │
        │  graph orchestrator │  ───┐      │  research(topic, angle=Y)  │
        │  ─ fanout to 3      │  ───┼──►  │  research(topic, angle=Z)  │
        │  ─ stream findings  │  ───┘      │  evaluate(findings) [ACL]  │
        │  ─ checkpoint state │            │  checkpoint_*              │
        │  ─ Grok summary     │            └────────────────────────────┘
        └─────────────────────┘
"""
from __future__ import annotations

import asyncio
import os
import sys
from typing import TypedDict

from dotenv import load_dotenv
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_xai import ChatXAI
from langgraph.graph import END, START, StateGraph

from pilot_langgraph import (
    PilotCheckpointSaver,
    PilotRemoteRunnable,
    PilotUnauthorizedError,
)

load_dotenv()

WORKER = os.environ.get("PILOT_REMOTE_PEER", "").strip()
if not WORKER:
    sys.exit("set PILOT_REMOTE_PEER to a pilot worker address (with checkpoint handlers)")
THREAD_ID = os.environ.get("PILOT_THREAD_ID", "research-demo-1")
QUESTION = os.environ.get(
    "PILOT_QUESTION",
    "What does Pilot Protocol give AI agents that the public internet does not?",
)


class State(TypedDict, total=False):
    question: str
    findings_by_angle: dict[str, list[dict]]
    ranked: list[dict]
    answer: str


class _LoggingCallback(AsyncCallbackHandler):
    """Demonstrates callback propagation; in production this would be LangSmith."""
    async def on_chain_start(self, serialized, inputs, *, run_id, **kw):
        name = (serialized or {}).get("name", "?")
        print(f"  [callback] chain_start name={name}")

    async def on_chain_end(self, outputs, *, run_id, **kw):
        keys = list(outputs.keys()) if isinstance(outputs, dict) else type(outputs).__name__
        print(f"  [callback] chain_end keys={keys}")


def _llm() -> ChatXAI:
    return ChatXAI(model="grok-3-mini", api_key=os.environ["XAI_API_KEY"], temperature=0.3)


# ---- graph nodes ----

async def fanout_research(state: State) -> dict:
    """Iter-10 PilotFanoutRunnable: 3 angles in parallel via streaming.

    Each branch calls `research` with its own angle. We use `astream` to
    receive findings as they're produced, then re-aggregate by angle.
    """
    angles = ["fast", "slow", "contrarian"]
    # Per-branch runnables (constructed manually because each branch needs a
    # different payload — PilotFanoutRunnable sends the same input to all).
    runnables = {a: PilotRemoteRunnable(node="research", peer=WORKER, timeout_secs=30) for a in angles}

    async def _stream_one(angle: str) -> tuple[str, list[dict]]:
        chunks: list[dict] = []
        async for c in runnables[angle].astream({"topic": state["question"], "angle": angle}):
            chunks.append(c)
            print(f"  [{angle}] {c['finding']}")
        return angle, chunks

    results = await asyncio.gather(*[_stream_one(a) for a in angles])
    return {"findings_by_angle": dict(results)}


async def evaluate_findings(state: State) -> dict:
    flat = [f for angle_findings in state["findings_by_angle"].values() for f in angle_findings]
    runnable = PilotRemoteRunnable(node="evaluate", peer=WORKER, timeout_secs=30)
    try:
        out = await runnable.ainvoke({"findings": flat})
    except PilotUnauthorizedError as e:
        print(f"  [evaluate] DENIED: {e}")
        # Demonstrate typed-error handling: degrade gracefully without scoring.
        return {"ranked": [{"finding": f.get("finding", "?"), "score": None, "rank": i}
                           for i, f in enumerate(flat)]}
    print(f"  [evaluate] scored {out['n_evaluated']} findings on {out['evaluator_host']}")
    return {"ranked": out["scored"]}


def summarize(state: State) -> dict:
    top = state["ranked"][:3]
    bullets = "\n".join(f"  - ({f.get('score')}) {f['finding']}" for f in top)
    msg = _llm().invoke([
        SystemMessage(
            "You are a concise answerer. The user asked a question. Multiple "
            "remote researcher agents produced findings, and an evaluator agent "
            "ranked them. Answer the user's question in 2 sentences using the "
            "top-ranked findings as ground truth, then add one sentence noting "
            "the workflow ran across distributed Pilot peers."
        ),
        HumanMessage(f"Question: {state['question']}\nTop findings:\n{bullets}"),
    ])
    return {"answer": msg.content.strip()}


def build_graph(saver: PilotCheckpointSaver):
    g = StateGraph(State)
    g.add_node("research", fanout_research)
    g.add_node("evaluate", evaluate_findings)
    g.add_node("summarize", summarize)
    g.add_edge(START, "research")
    g.add_edge("research", "evaluate")
    g.add_edge("evaluate", "summarize")
    g.add_edge("summarize", END)
    return g.compile(checkpointer=saver)


async def main():
    saver = PilotCheckpointSaver(peer=WORKER, port=5000, timeout_secs=30)
    graph = build_graph(saver)
    cfg = {"configurable": {"thread_id": THREAD_ID}, "callbacks": [_LoggingCallback()]}

    print(f"\n=== thread_id={THREAD_ID} ===")
    print(f"Question: {QUESTION}\n")

    final = await graph.ainvoke({"question": QUESTION, "findings_by_angle": {}}, cfg)

    print("\n=== ranked findings ===")
    for f in final["ranked"][:5]:
        print(f"  [{f.get('score','?'):>5}] {f['finding']}")

    print("\n=== answer ===")
    print(final["answer"])

    print("\n=== checkpoint replay ===")
    snap = await build_graph(saver).aget_state(cfg)
    print(f"Latest snapshot has {len(snap.values.get('ranked', []))} ranked findings; ",
          f"answer is {len(snap.values.get('answer', ''))} chars")


if __name__ == "__main__":
    asyncio.run(main())
