"""Pure-codec tests — no daemon required."""
from __future__ import annotations

import pytest

from pilot_langgraph._ipc import (
    Addr,
    encode_dial,
    encode_send,
    encode_send_to,
    parse_accept,
    parse_datagram,
    parse_dial_ok,
    parse_recv,
)


class TestAddr:
    def test_round_trip_text(self):
        a = Addr.parse("0:0000.0001.ABCD")
        assert str(a) == "0:0000.0001.ABCD"
        assert a.network == 0
        assert a.node == 0x1ABCD

    def test_round_trip_bytes(self):
        a = Addr(network=2, node=0xDEADBEEF)
        assert Addr.unpack(a.pack()) == a

    def test_invalid_format(self):
        with pytest.raises(ValueError):
            Addr.parse("not-an-address")

    def test_network_mismatch(self):
        # decimal 1 != hex 0
        with pytest.raises(ValueError):
            Addr.parse("1:0000.0000.0001")


class TestEncoders:
    def test_encode_dial(self):
        a = Addr.parse("0:0000.0000.0042")
        msg = encode_dial(a, port=5000)
        assert msg[0] == 0x03  # cmdDial
        assert len(msg) == 1 + 6 + 2

    def test_encode_send(self):
        msg = encode_send(conn_id=0x12345678, data=b"hello")
        assert msg[0] == 0x06  # cmdSend
        assert msg[1:5] == b"\x12\x34\x56\x78"
        assert msg[5:] == b"hello"

    def test_encode_send_to(self):
        a = Addr.parse("0:0000.0000.0001")
        msg = encode_send_to(a, port=7, data=b"ping")
        assert msg[0] == 0x0B  # cmdSendTo
        assert msg[-4:] == b"ping"


class TestParsers:
    def test_parse_dial_ok(self):
        assert parse_dial_ok(b"\x00\x00\x10\x00") == 0x1000

    def test_parse_recv(self):
        payload = b"\x00\x00\x00\x05hello"
        out = parse_recv(payload)
        assert out is not None
        assert out[0] == 5
        assert out[1] == b"hello"

    def test_parse_datagram(self):
        # [6B addr][2B src_port][2B dst_port][data]
        payload = bytes.fromhex("000000000042" + "0007" + "1388") + b"data!"
        dg = parse_datagram(payload)
        assert dg is not None
        assert dg.src_addr.node == 0x42
        assert dg.src_port == 7
        assert dg.dst_port == 5000
        assert dg.data == b"data!"

    def test_parse_accept(self):
        # [2B local_port][4B conn_id][6B remote_addr][2B remote_port]
        payload = (
            bytes.fromhex("1388")
            + bytes.fromhex("00000007")
            + bytes.fromhex("0000000000FF")
            + bytes.fromhex("C001")
        )
        ac = parse_accept(payload)
        assert ac is not None
        assert ac.local_port == 5000
        assert ac.conn_id == 7
        assert ac.remote_addr.node == 0xFF
        assert ac.remote_port == 0xC001
