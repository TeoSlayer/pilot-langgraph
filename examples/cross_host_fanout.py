"""Cross-host fanout: one input dispatched to N remote workers in parallel.

Proves the multi-host story end-to-end. Each branch lands on a different
physical VM; the response includes the worker's hostname so you can SEE
the work was distributed.

Topology:

         local laptop                           ┌──────────────────────────┐
         ┌──────────────────┐    ──── dial ──►  │  worker-1 (any host)     │
         │                  │                   │                          │
         │  PilotFanout     │    ──── dial ──►  │                          │
         │   ─ branch-0 ────┤                   ┌──────────────────────────┐
         │   ─ branch-1 ────┘                   │  worker-2 (any host)     │
         │                  │                   │                          │
         └──────────────────┘                   └──────────────────────────┘

Set:
    PILOT_WORKER_PEERS="<addr-w1>,<addr-w2>"
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

from pilot_langgraph import PilotFanoutRunnable, PilotRemoteRunnable


_peers = os.environ.get("PILOT_WORKER_PEERS", "").strip()
if not _peers:
    sys.exit("set PILOT_WORKER_PEERS to a comma-separated list of pilot addresses")
WORKERS = _peers.split(",")


async def main() -> None:
    targets = {
        f"branch-{i}": PilotRemoteRunnable(node="enrich", peer=peer.strip(), timeout_secs=30)
        for i, peer in enumerate(WORKERS)
    }
    print(f"fanning out across {len(targets)} workers: {[t.peer for t in targets.values()]}\n")

    fan = PilotFanoutRunnable(targets)

    t0 = time.monotonic()
    results = await fan.ainvoke({"task": "find the meaning of life"})
    elapsed = time.monotonic() - t0

    print(f"all branches returned in {elapsed:.2f}s\n")
    for label, out in results.items():
        receipt = out["remote_receipt"]
        print(f"  {label}:")
        print(f"    host: {receipt['processed_on_host']}")
        print(f"    pid:  {receipt['worker_pid']}")
        print(f"    at:   {receipt['processed_at_utc']}")

    hosts = {out["remote_receipt"]["processed_on_host"] for out in results.values()}
    print(f"\ndistinct hosts that processed branches: {len(hosts)}")
    if len(hosts) == len(targets):
        print("✓ each branch ran on a different physical worker")
    else:
        print("⚠ multiple branches collapsed to the same host")


if __name__ == "__main__":
    asyncio.run(main())
