"""JSON log format tests."""
from __future__ import annotations

import json
import logging


from pilot_langgraph.worker import _JsonFormatter, _configure_logging


def test_json_formatter_basic():
    f = _JsonFormatter()
    rec = logging.LogRecord(
        name="my.logger", level=logging.INFO, pathname="x.py", lineno=1,
        msg="hello %s", args=("world",), exc_info=None,
    )
    out = json.loads(f.format(rec))
    assert out["level"] == "INFO"
    assert out["logger"] == "my.logger"
    assert out["message"] == "hello world"
    assert "ts" in out


def test_json_formatter_includes_extras():
    f = _JsonFormatter()
    rec = logging.LogRecord(
        name="x", level=logging.INFO, pathname="x.py", lineno=1,
        msg="m", args=(), exc_info=None,
    )
    rec.request_id = "abc-123"
    rec.caller_node_id = 42
    out = json.loads(f.format(rec))
    assert out["request_id"] == "abc-123"
    assert out["caller_node_id"] == 42


def test_json_formatter_skips_non_serializable():
    f = _JsonFormatter()
    rec = logging.LogRecord(
        name="x", level=logging.INFO, pathname="x.py", lineno=1,
        msg="m", args=(), exc_info=None,
    )
    rec.weird = object()  # not JSON-serializable
    out = json.loads(f.format(rec))
    # Stored as repr() — readable but stable
    assert "weird" in out
    assert isinstance(out["weird"], str)


def test_configure_logging_plain(capsys):
    _configure_logging("INFO", "plain")
    log = logging.getLogger("pilot_langgraph.test_plain")
    log.info("hello")
    captured = capsys.readouterr()
    # Plain format includes timestamp, level, logger, message
    assert "INFO" in captured.err and "hello" in captured.err


def test_configure_logging_json(capsys):
    _configure_logging("INFO", "json")
    log = logging.getLogger("pilot_langgraph.test_json")
    log.info("structured", extra={"key": "val"})
    captured = capsys.readouterr()
    line = captured.err.strip().splitlines()[-1]
    parsed = json.loads(line)
    assert parsed["message"] == "structured"
    assert parsed["key"] == "val"
    assert parsed["level"] == "INFO"
