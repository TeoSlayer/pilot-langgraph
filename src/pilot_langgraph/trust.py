"""Trust bootstrap helpers — establish mutual Pilot trust from one Python call.

Usage:
    from pilot_langgraph import ensure_trust
    await ensure_trust(89702)               # async
    from pilot_langgraph.trust import ensure_trust_sync
    ensure_trust_sync(89702)                # sync
    # CLI:  python -m pilot_langgraph.trust 89702

Both daemons must be running. The remote daemon should be started with
`--trust-auto-approve` for unattended setups; otherwise trust will reach
"sent" but stay non-mutual until the remote operator approves it.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time

from .asyncio_client import PilotConnection

log = logging.getLogger(__name__)


async def _is_mutual(conn: PilotConnection, node_id: int) -> bool:
    for entry in await conn.trust_list():
        if int(entry.get("node_id", -1)) == node_id and entry.get("mutual") is True:
            return True
    return False


async def ensure_trust(
    node_id: int,
    *,
    justification: str = "pilot-langgraph",
    timeout_secs: float = 30.0,
    poll_interval: float = 1.0,
    socket_path: str | None = None,
) -> dict:
    """Establish mutual trust with `node_id`. Idempotent — returns immediately if already mutual.

    Sends a handshake (if not yet trusted) and polls the trust list until
    mutual=true appears, raising TimeoutError on expiry. Returns the trust
    record once mutual.
    """
    conn = await PilotConnection.connect(socket_path)
    try:
        if await _is_mutual(conn, node_id):
            log.info("ensure_trust: already mutual with node_id=%d", node_id)
            for entry in await conn.trust_list():
                if int(entry.get("node_id", -1)) == node_id:
                    return entry

        log.info("ensure_trust: sending handshake to node_id=%d", node_id)
        await conn.handshake_send(node_id, justification=justification)

        deadline = time.monotonic() + timeout_secs
        while time.monotonic() < deadline:
            for entry in await conn.trust_list():
                if int(entry.get("node_id", -1)) == node_id and entry.get("mutual") is True:
                    log.info("ensure_trust: mutual trust established with node_id=%d", node_id)
                    return entry
            await asyncio.sleep(poll_interval)
        raise TimeoutError(
            f"trust with node_id={node_id} did not become mutual within {timeout_secs}s "
            "— is the remote daemon running with --trust-auto-approve, or has the "
            "remote operator approved the pending handshake?"
        )
    finally:
        await conn.close()


def ensure_trust_sync(node_id: int, **kwargs) -> dict:
    return asyncio.run(ensure_trust(node_id, **kwargs))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pilot-langgraph-trust",
                                description="Bootstrap mutual Pilot trust with a peer.")
    p.add_argument("node_id", type=int, help="Numeric node_id of the peer")
    p.add_argument("--justification", default="pilot-langgraph")
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument("--socket", default=None, help="override $PILOT_SOCKET")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper(), format="%(message)s")
    try:
        entry = ensure_trust_sync(
            args.node_id, justification=args.justification,
            timeout_secs=args.timeout, socket_path=args.socket,
        )
    except TimeoutError as e:
        print(f"timeout: {e}", file=sys.stderr)
        return 2
    print(f"mutual trust established: node_id={entry['node_id']} pubkey={entry.get('public_key','?')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
