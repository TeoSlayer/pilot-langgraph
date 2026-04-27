"""Discover a remote pilot-langgraph worker's full surface from the shell.

Calls the worker's auto-registered `_health` and `_handlers` introspection
handlers and prints a human-readable summary of:
  - worker version / uptime / in-flight count / total throughput
  - every handler with ACL, timeout, concurrency cap, perf stats
  - input/output JSON schemas (if pydantic models registered)

Usage:
    python tools/discover.py <pilot-address-or-hostname>
    python tools/discover.py <peer> --schemas       # show full schemas
    python tools/discover.py <peer> --json          # raw JSON output
"""
from __future__ import annotations

import argparse
import asyncio
import json

from pilot_langgraph import PilotRemoteRunnable


async def _call(peer: str, node: str, timeout: float = 15.0):
    r = PilotRemoteRunnable(node=node, peer=peer, timeout_secs=timeout)
    return await r.ainvoke(None)


def _fmt_handler_row(h: dict) -> str:
    """One line per handler: name, ACL, timeout, concurrency, p50, p95, calls."""
    name = h["name"]
    allow = "open" if h["allow"] is None else f"{len(h['allow'])} peers"
    timeout = "-" if h["timeout_secs"] is None else f"{h['timeout_secs']:g}s"
    concur = "-" if h["max_concurrent"] is None else str(h["max_concurrent"])
    return (
        f"  {name:<25} acl={allow:<10} timeout={timeout:<6} concurrent={concur:<4} "
        f"calls={h['calls']:<5} errors={h['errors']:<3} "
        f"p50={h['p50_ms']:>5}ms p95={h['p95_ms']:>5}ms"
    )


async def main() -> int:
    p = argparse.ArgumentParser(description="Introspect a remote pilot-langgraph worker.")
    p.add_argument("peer", help="pilot address (N:NNNN.HHHH.LLLL) or hostname")
    p.add_argument("--schemas", action="store_true",
                   help="print full JSON schemas for typed handlers")
    p.add_argument("--json", action="store_true",
                   help="print raw JSON output (for piping to jq)")
    p.add_argument("--timeout", type=float, default=15.0)
    args = p.parse_args()

    health = await _call(args.peer, "_health", timeout=args.timeout)
    handlers = await _call(args.peer, "_handlers", timeout=args.timeout)

    if args.json:
        print(json.dumps({"health": health, "handlers": handlers}, indent=2, default=str))
        return 0

    print(f"\nWorker: {args.peer}")
    print(f"  version:     {health.get('version', '?')}")
    print(f"  uptime:      {health.get('uptime_secs', 0):.0f}s")
    print(f"  in-flight:   {health.get('in_flight', 0)}")
    print(f"  total calls: {health.get('total_calls', 0)} (errors {health.get('total_errors', 0)})")
    print(f"  handlers:    {health.get('n_handlers', 0)}\n")

    print("Handlers:")
    user_handlers = [h for h in handlers["handlers"] if not h["name"].startswith("_")]
    intro_handlers = [h for h in handlers["handlers"] if h["name"].startswith("_")]
    for h in user_handlers:
        print(_fmt_handler_row(h))
    if intro_handlers:
        print("\nIntrospection:")
        for h in intro_handlers:
            print(_fmt_handler_row(h))

    if args.schemas:
        typed = [h for h in user_handlers
                 if h.get("input_schema") or h.get("output_schema")]
        if typed:
            print("\nSchemas:")
            for h in typed:
                if h.get("input_schema"):
                    print(f"\n  {h['name']}.input_schema:")
                    print(json.dumps(h["input_schema"], indent=4))
                if h.get("output_schema"):
                    print(f"\n  {h['name']}.output_schema:")
                    print(json.dumps(h["output_schema"], indent=4))
        else:
            print("\n(no typed handlers — nothing to show with --schemas)")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
