"""Trust bootstrap helper tests.

Idempotent path is tested live (peer already trusted).
The full untrust→handshake→wait-for-mutual loop is exercised via the CLI
fixture; unset by default to avoid disrupting other tests.
"""
from __future__ import annotations


import pytest

from .conftest import requires_daemon


pytestmark = [requires_daemon]


async def test_ensure_trust_idempotent_when_already_mutual(worker_peer):
    """Calling ensure_trust on a peer we already trust returns the record without re-handshaking."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    # The worker_peer fixture is an address; we need a node_id. Resolve it.
    from pilot_langgraph import Addr, ensure_trust, PilotConnection
    addr = Addr.parse(worker_peer)
    async with await PilotConnection.connect() as c:
        for entry in await c.trust_list():
            if entry.get("node_id") and Addr(network=entry["network"], node=entry["node_id"]) == addr:
                node_id = entry["node_id"]
                break
        else:
            # Try lookup by address string; fall back to any-mutual entry
            trust = await c.trust_list()
            mutual = [t for t in trust if t.get("mutual")]
            if not mutual:
                pytest.skip("no mutually-trusted peers; bootstrap manually first")
            node_id = mutual[0]["node_id"]

    out = await ensure_trust(node_id, timeout_secs=10)
    assert out["mutual"] is True
    assert out["node_id"] == node_id


async def test_ensure_trust_fails_on_unknown_node():
    """A bogus node_id should produce an error (either daemon rejection or wait timeout)."""
    from pilot_langgraph import ensure_trust, PilotConnectionError
    # Either the daemon refuses immediately (bogus id) or the handshake gets
    # sent and we time out waiting for an approval that never comes.
    with pytest.raises((TimeoutError, PilotConnectionError)):
        await ensure_trust(999_999_999, timeout_secs=3, poll_interval=0.5)


def test_trust_cli_help():
    """The trust CLI exposes the documented args."""
    from pilot_langgraph.trust import main
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
