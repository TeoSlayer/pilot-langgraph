"""Wire protocol for distributed LangGraph execution over Pilot Protocol.

A request travels caller -> remote on `request_port`:

    {"v": 1,
     "kind": "invoke" | "subgraph",
     "call_id": "<uuid>",
     "node": "<handler_name>",
     "from": "<sender_hostname>",
     "reply_port": <int>,
     "payload": <json>}

A reply travels remote -> caller on `reply_port`:

    {"v": 1,
     "call_id": "<uuid>",
     "ok": <bool>,
     "result": <json>,
     "error": <str|null>}

Only JSON-serializable payloads survive the tunnel. The transport layer is
responsible for ensuring `payload` is JSON-encodable.
"""
from __future__ import annotations

import json
from typing import Any, Literal

# Pilot daemon reserves ports 7, 444, 1001, 1002, 1003 for its built-in
# services (echo, handshake, dataexchange, eventstream, tasksubmit). Pick
# user ports outside that range so the WorkerRouter can bind cleanly.
WIRE_VERSION = 1
DEFAULT_REQUEST_PORT = 5000
DEFAULT_REPLY_PORT = 5001
PILOT_ECHO_PORT = 7

Kind = Literal["invoke", "subgraph"]


def encode_request(
    *,
    call_id: str,
    node: str,
    sender_hostname: str,
    reply_port: int,
    payload: Any,
    kind: Kind = "invoke",
) -> str:
    return json.dumps(
        {
            "v": WIRE_VERSION,
            "kind": kind,
            "call_id": call_id,
            "node": node,
            "from": sender_hostname,
            "reply_port": reply_port,
            "payload": payload,
        },
        separators=(",", ":"),
        default=str,
    )


def encode_reply(
    *,
    call_id: str,
    ok: bool,
    result: Any = None,
    error: str | None = None,
) -> str:
    return json.dumps(
        {
            "v": WIRE_VERSION,
            "call_id": call_id,
            "ok": ok,
            "result": result,
            "error": error,
        },
        separators=(",", ":"),
        default=str,
    )


def decode(blob: str) -> dict[str, Any]:
    return json.loads(blob)
