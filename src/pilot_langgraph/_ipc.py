"""Low-level Pilot IPC wire format.

Reverse-engineered from `pilotprotocol/pkg/driver/{ipc,driver,conn,listener}.go`
and `pilotprotocol/pkg/protocol/address.go`. Pure constants + codecs, no I/O.

Framing:
    [4-byte big-endian length][payload]

Request/response payload always starts with a 1-byte command. The rest of the
shape depends on the command — see encode_*/parse_* helpers below.
"""
from __future__ import annotations

import re
import struct
from dataclasses import dataclass

# ---- framing ----
LENGTH_PREFIX = struct.Struct(">I")
MAX_MESSAGE_SIZE = 1 << 20
MAX_SEND_CHUNK = MAX_MESSAGE_SIZE - 64  # leave room for [cmd(1)][connID(4)] + safety

# ---- commands ----
CMD_BIND = 0x01
CMD_BIND_OK = 0x02
CMD_DIAL = 0x03
CMD_DIAL_OK = 0x04
CMD_ACCEPT = 0x05
CMD_SEND = 0x06
CMD_RECV = 0x07
CMD_CLOSE = 0x08
CMD_CLOSE_OK = 0x09
CMD_ERROR = 0x0A
CMD_SEND_TO = 0x0B
CMD_RECV_FROM = 0x0C
CMD_INFO = 0x0D
CMD_INFO_OK = 0x0E
CMD_HANDSHAKE = 0x0F
CMD_HANDSHAKE_OK = 0x10
CMD_RESOLVE_HOSTNAME = 0x11
CMD_RESOLVE_HOSTNAME_OK = 0x12
CMD_SET_HOSTNAME = 0x13
CMD_SET_HOSTNAME_OK = 0x14
CMD_HEALTH = 0x21
CMD_HEALTH_OK = 0x22

# Built-in service ports (from pilotprotocol/pkg/protocol/header.go)
PORT_ECHO = 7
PORT_DATA_EXCHANGE = 1001
PORT_EVENT_STREAM = 1002
PORT_TASK_SUBMIT = 1003

# Handshake sub-commands
SUB_HANDSHAKE_SEND = 0x01
SUB_HANDSHAKE_APPROVE = 0x02
SUB_HANDSHAKE_REJECT = 0x03
SUB_HANDSHAKE_PENDING = 0x04
SUB_HANDSHAKE_TRUSTED = 0x05
SUB_HANDSHAKE_REVOKE = 0x06

# ---- address ----
ADDR_SIZE = 6
ADDR_RE = re.compile(r"^(\d+):([0-9A-Fa-f]{4})\.([0-9A-Fa-f]{4})\.([0-9A-Fa-f]{4})$")


@dataclass(frozen=True, slots=True)
class Addr:
    network: int
    node: int

    def pack(self) -> bytes:
        return struct.pack(">HI", self.network, self.node)

    @classmethod
    def unpack(cls, buf: bytes, offset: int = 0) -> "Addr":
        network, node = struct.unpack_from(">HI", buf, offset)
        return cls(network=network, node=node)

    @classmethod
    def parse(cls, s: str) -> "Addr":
        m = ADDR_RE.match(s)
        if not m:
            raise ValueError(f"invalid pilot address: {s!r}")
        net_dec = int(m.group(1))
        net_hex = int(m.group(2), 16)
        if net_dec != net_hex:
            raise ValueError(f"network mismatch in {s!r}: {net_dec} vs 0x{net_hex:04X}")
        node_high = int(m.group(3), 16)
        node_low = int(m.group(4), 16)
        return cls(network=net_dec, node=(node_high << 16) | node_low)

    def __str__(self) -> str:
        return f"{self.network}:{self.network:04X}.{(self.node >> 16) & 0xFFFF:04X}.{self.node & 0xFFFF:04X}"


# ---- request encoders ----

def encode_info() -> bytes:
    return bytes([CMD_INFO])


def encode_health() -> bytes:
    return bytes([CMD_HEALTH])


def encode_resolve_hostname(hostname: str) -> bytes:
    return bytes([CMD_RESOLVE_HOSTNAME]) + hostname.encode()


def encode_set_hostname(hostname: str) -> bytes:
    return bytes([CMD_SET_HOSTNAME]) + hostname.encode()


def encode_handshake_send(node_id: int, justification: str) -> bytes:
    return (
        bytes([CMD_HANDSHAKE, SUB_HANDSHAKE_SEND])
        + struct.pack(">I", node_id)
        + justification.encode()
    )


def encode_handshake_approve(node_id: int) -> bytes:
    return bytes([CMD_HANDSHAKE, SUB_HANDSHAKE_APPROVE]) + struct.pack(">I", node_id)


def encode_handshake_pending() -> bytes:
    return bytes([CMD_HANDSHAKE, SUB_HANDSHAKE_PENDING])


def encode_handshake_trusted() -> bytes:
    return bytes([CMD_HANDSHAKE, SUB_HANDSHAKE_TRUSTED])


def encode_send_to(dst: Addr, port: int, data: bytes) -> bytes:
    return bytes([CMD_SEND_TO]) + dst.pack() + struct.pack(">H", port) + data


# ---- stream encoders ----

def encode_dial(dst: Addr, port: int) -> bytes:
    return bytes([CMD_DIAL]) + dst.pack() + struct.pack(">H", port)


def encode_bind(port: int) -> bytes:
    return bytes([CMD_BIND]) + struct.pack(">H", port)


def encode_send(conn_id: int, data: bytes) -> bytes:
    return bytes([CMD_SEND]) + struct.pack(">I", conn_id) + data


def encode_close(conn_id: int) -> bytes:
    return bytes([CMD_CLOSE]) + struct.pack(">I", conn_id)


# ---- response/push parsers ----

@dataclass(frozen=True, slots=True)
class Datagram:
    src_addr: Addr
    src_port: int
    dst_port: int
    data: bytes


def parse_datagram(payload: bytes) -> Datagram | None:
    if len(payload) < ADDR_SIZE + 4:
        return None
    src_addr = Addr.unpack(payload, 0)
    src_port, dst_port = struct.unpack_from(">HH", payload, ADDR_SIZE)
    return Datagram(
        src_addr=src_addr,
        src_port=src_port,
        dst_port=dst_port,
        data=bytes(payload[ADDR_SIZE + 4:]),
    )


@dataclass(frozen=True, slots=True)
class AcceptedConn:
    local_port: int
    conn_id: int
    remote_addr: Addr
    remote_port: int


def parse_accept(payload: bytes) -> AcceptedConn | None:
    """cmdAccept payload: [2B local_port][4B conn_id][6B remote_addr][2B remote_port]."""
    if len(payload) < 2 + 4 + ADDR_SIZE + 2:
        return None
    local_port = struct.unpack_from(">H", payload, 0)[0]
    conn_id = struct.unpack_from(">I", payload, 2)[0]
    remote_addr = Addr.unpack(payload, 6)
    remote_port = struct.unpack_from(">H", payload, 6 + ADDR_SIZE)[0]
    return AcceptedConn(local_port=local_port, conn_id=conn_id,
                        remote_addr=remote_addr, remote_port=remote_port)


def parse_recv(payload: bytes) -> tuple[int, bytes] | None:
    """cmdRecv payload: [4B conn_id][data]."""
    if len(payload) < 4:
        return None
    conn_id = struct.unpack_from(">I", payload, 0)[0]
    return conn_id, bytes(payload[4:])


def parse_dial_ok(payload: bytes) -> int | None:
    """cmdDialOK payload: [4B conn_id]."""
    if len(payload) < 4:
        return None
    return struct.unpack_from(">I", payload, 0)[0]


def parse_bind_ok(payload: bytes) -> int | None:
    """cmdBindOK payload: [2B bound_port]."""
    if len(payload) < 2:
        return None
    return struct.unpack_from(">H", payload, 0)[0]


def parse_close_ok(payload: bytes) -> int | None:
    """cmdCloseOK payload: [4B conn_id]."""
    if len(payload) < 4:
        return None
    return struct.unpack_from(">I", payload, 0)[0]


def parse_error(payload: bytes) -> str:
    """cmdError payload: 2 reserved bytes + ASCII error text."""
    if len(payload) < 2:
        return "unknown daemon error"
    return payload[2:].decode(errors="replace")


# ---- event stream codec (port 1002 broker) ----
# Wire format per event: [2B topic_len][topic][4B payload_len][payload]

@dataclass(frozen=True, slots=True)
class Event:
    topic: str
    payload: bytes


def encode_event(topic: str, payload: bytes) -> bytes:
    t = topic.encode()
    return (
        struct.pack(">H", len(t))
        + t
        + struct.pack(">I", len(payload))
        + payload
    )


def split_events(buf: bytes) -> tuple[list[Event], bytes]:
    """Pull every complete event from `buf`. Returns (events, leftover).

    The eventstream framing is length-prefixed but doesn't align to TCP frame
    boundaries, so any bytes left over are an in-progress event the caller
    should retain for the next read.
    """
    events: list[Event] = []
    pos = 0
    while pos + 2 <= len(buf):
        topic_len = struct.unpack_from(">H", buf, pos)[0]
        if topic_len > 1024:
            raise ValueError(f"topic too long: {topic_len}")
        if pos + 2 + topic_len + 4 > len(buf):
            break  # incomplete header
        payload_len = struct.unpack_from(">I", buf, pos + 2 + topic_len)[0]
        if payload_len > (1 << 24):
            raise ValueError(f"payload too large: {payload_len}")
        end = pos + 2 + topic_len + 4 + payload_len
        if end > len(buf):
            break  # incomplete payload
        topic = buf[pos + 2:pos + 2 + topic_len].decode(errors="replace")
        payload = bytes(buf[pos + 2 + topic_len + 4:end])
        events.append(Event(topic=topic, payload=payload))
        pos = end
    return events, bytes(buf[pos:])
