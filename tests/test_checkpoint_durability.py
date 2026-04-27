"""End-to-end durability test for the SQLite checkpoint store.

Requires:
  * `PILOT_WORKER_PEER` set to a deployed worker
  * Worker started with `PILOT_CHECKPOINT_DB=/some/path.db`
  * `PILOT_WORKER_RESTART_CMD` set to a shell command that restarts the
    worker process (e.g. `ssh worker-host "sudo systemctl restart
    pilot-langgraph-worker"`). Without it, this test is skipped.
"""
from __future__ import annotations

import os
import subprocess
import time
from typing import TypedDict

import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


class _State(TypedDict, total=False):
    n: int


def _step(s: _State) -> dict:
    return {"n": s.get("n", 0) + 100}


def _restart_worker():
    cmd = os.environ.get("PILOT_WORKER_RESTART_CMD")
    if not cmd:
        return False
    subprocess.run(cmd, shell=True, check=True, timeout=60)
    time.sleep(3)
    return True


def test_state_survives_worker_restart(worker_peer):
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    if not os.environ.get("PILOT_WORKER_RESTART_CMD"):
        pytest.skip("set PILOT_WORKER_RESTART_CMD to enable durability test")

    from langgraph.graph import END, START, StateGraph
    from pilot_langgraph import PilotCheckpointSaver

    saver = PilotCheckpointSaver(peer=worker_peer, port=5000, timeout_secs=30)
    cfg = {"configurable": {"thread_id": f"durability-{int(time.time())}"}}

    def build():
        g = StateGraph(_State)
        g.add_node("step", _step)
        g.add_edge(START, "step")
        g.add_edge("step", END)
        return g.compile(checkpointer=saver)

    g1 = build()
    final = g1.invoke({"n": 7}, cfg)
    assert final["n"] == 107

    assert _restart_worker(), "PILOT_WORKER_RESTART_CMD must succeed"

    # Fresh saver + fresh graph after worker restart.
    saver2 = PilotCheckpointSaver(peer=worker_peer, port=5000, timeout_secs=30)
    g2 = build()
    g2.checkpointer = saver2  # type: ignore[attr-defined]
    snap = g2.get_state(cfg)
    assert snap.values["n"] == 107
