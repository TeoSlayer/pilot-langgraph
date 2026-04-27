"""Caller-side pydantic validation on PilotRemoteRunnable."""
from __future__ import annotations

import pytest
from pydantic import BaseModel, Field

from .conftest import requires_daemon


pytestmark = [requires_daemon]


class EnrichIn(BaseModel):
    plan: str = Field(min_length=1)


class EnrichOut(BaseModel):
    input_payload: dict
    input_size_bytes: int
    remote_receipt: dict


async def test_client_input_validation_rejects_before_send(worker_peer):
    """Bad input is caught client-side; the network round-trip never happens."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=10,
                             input_model=EnrichIn)
    with pytest.raises(ValueError, match="client input validation"):
        await r.ainvoke({"wrong_field": "missing plan"})


async def test_client_input_dumps_validated_model_to_wire(worker_peer):
    """Validated input is dumped to dict before sending."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=15,
                             input_model=EnrichIn)
    out = await r.ainvoke({"plan": "ship the rocket"})
    # Worker echoes input_payload; the wire shape should be the dumped EnrichIn
    assert out["input_payload"] == {"plan": "ship the rocket"}


async def test_client_output_validation_passes(worker_peer):
    """Caller-side output validation succeeds when worker returns matching shape."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=15,
                             input_model=EnrichIn, output_model=EnrichOut)
    out = await r.ainvoke({"plan": "test"})
    # The output_model.model_dump() result is what we get back
    assert "input_payload" in out
    assert "input_size_bytes" in out
    assert "remote_receipt" in out


async def test_client_output_validation_rejects_unexpected_shape(worker_peer):
    """If the worker's response doesn't match the expected output_model, raise."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")

    class WrongOut(BaseModel):
        # Worker returns input_payload/etc, not a `headline` field.
        headline: str

    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=15,
                             output_model=WrongOut)
    with pytest.raises(ValueError, match="client output validation"):
        await r.ainvoke({"plan": "x"})


async def test_no_validation_by_default(worker_peer):
    """Without input/output models, runnable behavior is unchanged."""
    if not worker_peer:
        pytest.skip("set PILOT_WORKER_PEER")
    from pilot_langgraph import PilotRemoteRunnable
    r = PilotRemoteRunnable(node="enrich", peer=worker_peer, timeout_secs=15)
    out = await r.ainvoke({"anything": "goes"})
    assert out["input_payload"] == {"anything": "goes"}
