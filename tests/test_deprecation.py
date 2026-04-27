"""Verify legacy shim modules emit DeprecationWarning."""
from __future__ import annotations

import importlib
import sys
import warnings


def _fresh_import(mod_name: str):
    """Re-import a module so its top-level warnings.warn fires again."""
    sys.modules.pop(mod_name, None)
    importlib.invalidate_caches()
    return importlib.import_module(mod_name)


def test_remote_node_emits_deprecation_warning():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _fresh_import("pilot_langgraph.remote_node")
    matches = [x for x in w if issubclass(x.category, DeprecationWarning)]
    assert matches
    assert "pilot_langgraph.remote_node is deprecated" in str(matches[0].message)


def test_pilot_client_emits_deprecation_warning():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _fresh_import("pilot_langgraph.pilot_client")
    matches = [x for x in w if issubclass(x.category, DeprecationWarning)]
    assert matches
    assert "pilot_langgraph.pilot_client is deprecated" in str(matches[0].message)


def test_modern_imports_dont_emit_deprecation():
    """The supported public API must NOT emit DeprecationWarning on import."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _fresh_import("pilot_langgraph")
    matches = [x for x in w if issubclass(x.category, DeprecationWarning)]
    # The package import may transitively import deprecated modules in
    # downstream deps — but pilot_langgraph itself shouldn't.
    own = [x for x in matches if "pilot_langgraph" in str(x.message)]
    assert not own, f"unexpected deprecations from public API: {own}"
