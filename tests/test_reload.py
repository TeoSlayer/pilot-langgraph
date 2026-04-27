"""Hot-reload of handler module."""
from __future__ import annotations

import importlib
import os
import sys
import textwrap
import tempfile

import pytest

from pilot_langgraph.server import WorkerServer
from pilot_langgraph.worker import _reload_handler_module, _watch_module_file


@pytest.fixture
def temp_handler_module():
    """Create a throwaway handler module file we can edit between tests."""
    d = tempfile.mkdtemp()
    sys.path.insert(0, d)
    mod_name = f"_test_handlers_{os.getpid()}"
    path = os.path.join(d, mod_name + ".py")
    with open(path, "w") as f:
        f.write(textwrap.dedent("""
            def greet(payload):
                return {"version": "v1", "echo": payload}

            def register(server):
                server.register("greet", greet)
        """))
    yield mod_name, path
    sys.path.remove(d)
    sys.modules.pop(mod_name, None)


def test_reload_swaps_in_new_handler_implementation(temp_handler_module, caplog):
    import logging
    import time as _time
    log = logging.getLogger("test")

    mod_name, path = temp_handler_module
    s = WorkerServer(port=0)

    # Initial register
    mod = importlib.import_module(mod_name)
    mod.register(s)
    assert s.handlers["greet"](None)["version"] == "v1"

    # Edit the file: bump version. Bump mtime explicitly so importlib's
    # bytecode cache doesn't shadow the change on fast tests (filesystems
    # can have second-granularity mtime).
    _time.sleep(0.05)
    with open(path, "w") as f:
        f.write(textwrap.dedent("""
            def greet(payload):
                return {"version": "v2", "echo": payload}

            def register(server):
                server.register("greet", greet)
        """))
    new_mtime = _time.time() + 5.0
    os.utime(path, (new_mtime, new_mtime))

    _reload_handler_module(mod_name, s, log)
    # Same handler name, NEW implementation
    assert s.handlers["greet"](None)["version"] == "v2"


def test_reload_preserves_introspection_handlers(temp_handler_module):
    import logging
    log = logging.getLogger("test")

    mod_name, path = temp_handler_module
    s = WorkerServer(port=0)
    mod = importlib.import_module(mod_name)
    mod.register(s)

    assert "_health" in s._handlers
    assert "_handlers" in s._handlers

    _reload_handler_module(mod_name, s, log)

    # Introspection handlers must survive reloads
    assert "_health" in s._handlers
    assert "_handlers" in s._handlers
    assert "greet" in s._handlers


def test_reload_handles_broken_module_gracefully(temp_handler_module, caplog):
    import logging
    log = logging.getLogger("pilot_langgraph.worker")

    mod_name, path = temp_handler_module
    s = WorkerServer(port=0)
    mod = importlib.import_module(mod_name)
    mod.register(s)

    # Break the file
    with open(path, "w") as f:
        f.write("this is not valid python !!@!@\n")

    with caplog.at_level(logging.ERROR, logger="pilot_langgraph.worker"):
        _reload_handler_module(mod_name, s, log)

    # The OLD handler must still be present and working
    assert "greet" in s._handlers
    assert s.handlers["greet"](None)["version"] == "v1"


def test_watch_detects_mtime_change(temp_handler_module):
    import logging
    import time

    mod_name, path = temp_handler_module
    s = WorkerServer(port=0)
    mod = importlib.import_module(mod_name)
    mod.register(s)

    log = logging.getLogger("test")
    check = _watch_module_file(mod_name, s, log, interval_secs=0.05)
    assert check is not None

    # First call: no change
    check()
    assert s.handlers["greet"](None)["version"] == "v1"

    # Touch the file (mtime changes); bump beyond second granularity.
    time.sleep(0.05)
    with open(path, "w") as f:
        f.write(textwrap.dedent("""
            def greet(payload):
                return {"version": "v3", "echo": payload}

            def register(server):
                server.register("greet", greet)
        """))
    new_mtime = time.time() + 5.0
    os.utime(path, (new_mtime, new_mtime))

    check()
    assert s.handlers["greet"](None)["version"] == "v3"
