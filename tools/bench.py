"""Benchmark harness for PilotRemoteRunnable.

Measures latency (p50/p95/p99) and throughput against a deployed worker.
Designed to give realistic numbers under multiple concurrency levels.

Usage:
    python tools/bench.py --peer <pilot-address> --handler enrich \\
        --concurrency 1,4,16 --calls 200

The handler should be cheap and deterministic (the standard `enrich`
example handler is ideal — it just echoes back with metadata).
"""
from __future__ import annotations

import argparse
import asyncio
import statistics
import time

from pilot_langgraph import PilotRemoteRunnable


async def _one_call(r: PilotRemoteRunnable, payload: dict) -> float:
    t0 = time.perf_counter()
    await r.ainvoke(payload)
    return (time.perf_counter() - t0) * 1000.0  # ms


async def _bench(peer: str, handler: str, concurrency: int, total: int, timeout: float) -> dict:
    r = PilotRemoteRunnable(node=handler, peer=peer, timeout_secs=timeout)

    # Warm up the tunnel + cache
    await r.ainvoke({"warmup": True})

    sem = asyncio.Semaphore(concurrency)
    latencies: list[float] = []

    async def _bounded(i: int):
        async with sem:
            ms = await _one_call(r, {"i": i})
            latencies.append(ms)

    t0 = time.perf_counter()
    await asyncio.gather(*(_bounded(i) for i in range(total)))
    wall = time.perf_counter() - t0

    latencies.sort()
    return {
        "peer": peer,
        "handler": handler,
        "concurrency": concurrency,
        "total_calls": total,
        "wall_secs": round(wall, 2),
        "throughput_per_sec": round(total / wall, 1),
        "p50_ms": round(latencies[len(latencies) // 2], 1),
        "p95_ms": round(latencies[int(len(latencies) * 0.95)], 1),
        "p99_ms": round(latencies[int(len(latencies) * 0.99)], 1),
        "min_ms": round(min(latencies), 1),
        "max_ms": round(max(latencies), 1),
        "mean_ms": round(statistics.fmean(latencies), 1),
    }


async def main() -> int:
    p = argparse.ArgumentParser(description="Benchmark a deployed pilot-langgraph worker")
    p.add_argument("--peer", required=True, help="pilot address or hostname of the worker")
    p.add_argument("--handler", default="enrich", help="handler name to call (default: enrich)")
    p.add_argument("--concurrency", default="1,4,16",
                   help="comma-separated concurrency levels to test")
    p.add_argument("--calls", type=int, default=200, help="calls per concurrency level")
    p.add_argument("--timeout", type=float, default=30.0, help="per-call timeout secs")
    args = p.parse_args()

    levels = [int(c.strip()) for c in args.concurrency.split(",") if c.strip()]
    print(f"# Bench: peer={args.peer} handler={args.handler} calls={args.calls}\n")
    header = f"{'concurrency':>11} {'wall_s':>7} {'rps':>7} {'p50_ms':>8} {'p95_ms':>8} {'p99_ms':>8} {'min':>6} {'max':>6}"
    print(header)
    print("-" * len(header))
    rows = []
    for c in levels:
        result = await _bench(args.peer, args.handler, c, args.calls, args.timeout)
        rows.append(result)
        print(f"{c:>11} {result['wall_secs']:>7.2f} {result['throughput_per_sec']:>7.1f} "
              f"{result['p50_ms']:>8.1f} {result['p95_ms']:>8.1f} {result['p99_ms']:>8.1f} "
              f"{result['min_ms']:>6.1f} {result['max_ms']:>6.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
