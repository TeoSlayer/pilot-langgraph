"""Worker CLI: `python -m pilot_langgraph.worker --handlers <module>`.

The named module either:
  (a) exposes a `register(server)` function, or
  (b) uses `@pilot_handler("name")` decorators and gets auto-collected.

Either is valid.

SIGTERM and SIGINT trigger a graceful shutdown: the listener stops accepting,
in-flight handlers drain (up to `--drain-timeout` seconds), then the process
exits 0. systemd's default 90-second TimeoutStopSec is plenty of headroom.

`--reload` enables dev-mode hot reload: the handler module's source file is
polled for mtime changes; on change the module is re-imported and the
server's user handlers are swapped atomically. In-flight requests complete
under the OLD handler set; new requests use the NEW one. Introspection
handlers are unaffected.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import json as _json
import logging
import signal
import sys
import time

from .server import DEFAULT_PORT, WorkerServer, consume_global_handlers


class _JsonFormatter(logging.Formatter):
    """Single-line JSON log formatter for machine-scrapable logs.

    Emits one object per record: {ts, level, logger, message, ...extras}.
    Extra fields attached via `logger.info("...", extra={"k": v})` flow through.
    """

    _STD_ATTRS = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "asctime", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
                  + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Surface user-supplied `extra={...}` fields.
        for k, v in record.__dict__.items():
            if k in self._STD_ATTRS or k.startswith("_"):
                continue
            try:
                _json.dumps(v)  # keep only JSON-able fields
                payload[k] = v
            except (TypeError, ValueError):
                payload[k] = repr(v)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return _json.dumps(payload, separators=(",", ":"), default=str)


def _configure_logging(level: str, fmt: str) -> None:
    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())


def _reload_handler_module(mod_name: str, server: WorkerServer, log: logging.Logger) -> None:
    """Re-import `mod_name` and re-register its handlers on `server`.

    Errors are caught and logged — a broken file shouldn't kill the worker.
    """
    try:
        mod = importlib.import_module(mod_name)
        importlib.reload(mod)
    except Exception:
        log.exception("reload: failed to re-import %s; keeping old handlers", mod_name)
        return
    try:
        server.clear_user_handlers()
        decorated = consume_global_handlers()
        server.register_many(decorated)
        if hasattr(mod, "register"):
            mod.register(server)
        log.info("reload: re-registered %d handler(s) from %s", len(server.handlers), mod_name)
    except Exception:
        log.exception("reload: failed to re-register handlers; worker may be in inconsistent state")


def _watch_module_file(mod_name: str, server: WorkerServer, log: logging.Logger,
                       interval_secs: float = 0.5):
    """Periodic-task body: poll the handler module's source for changes."""
    import os as _os
    mod = importlib.import_module(mod_name)
    path = getattr(mod, "__file__", None)
    if path is None:
        log.warning("reload: cannot watch %s (no __file__)", mod_name)
        return None
    try:
        last_mtime = _os.stat(path).st_mtime
    except OSError:
        last_mtime = 0.0

    def _check():
        nonlocal last_mtime
        try:
            current = _os.stat(path).st_mtime
        except OSError:
            return
        if current != last_mtime:
            last_mtime = current
            log.info("reload: detected change in %s, reloading handlers", path)
            _reload_handler_module(mod_name, server, log)

    return _check


def _install_signal_handlers(server: WorkerServer, log: logging.Logger) -> None:
    loop = asyncio.get_event_loop()

    def _handler(signame: str) -> None:
        log.info("worker: received %s — beginning graceful drain", signame)
        server.request_stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handler, sig.name)
        except NotImplementedError:
            # Windows or restricted env — fall back to default behaviour.
            pass


async def _serve(server: WorkerServer, log: logging.Logger, metrics_port: int | None) -> int:
    _install_signal_handlers(server, log)
    metrics_httpd = None
    if metrics_port:
        from .metrics import serve as _metrics_serve
        metrics_httpd = _metrics_serve(server, host="0.0.0.0", port=metrics_port)
    try:
        await server.serve_forever()
    finally:
        if metrics_httpd is not None:
            metrics_httpd.shutdown()
    log.info("worker: exited cleanly")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pilot-langgraph-worker", description=__doc__.splitlines()[0])
    p.add_argument("--handlers", required=True, help="dotted-path of a module exposing register(server) and/or @pilot_handler-decorated functions")
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help="pilot port to listen on")
    p.add_argument("--drain-timeout", type=float, default=30.0,
                   help="seconds to wait for in-flight handlers on shutdown")
    p.add_argument("--metrics-port", type=int, default=None,
                   help="if set, expose /metrics + /healthz on this HTTP port")
    p.add_argument("--reload", action="store_true",
                   help="dev-mode: hot-reload the handler module on file change")
    p.add_argument("--reload-interval", type=float, default=0.5,
                   help="seconds between handler-file mtime polls when --reload is set")
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--log-format", default="plain", choices=["plain", "json"],
                   help="plain text (default) or single-line JSON for k8s/ELK/Datadog scrapers")
    args = p.parse_args(argv)

    _configure_logging(args.log_level, args.log_format)
    log = logging.getLogger("pilot_langgraph.worker")

    sys.path.insert(0, ".")
    mod = importlib.import_module(args.handlers)

    server = WorkerServer(port=args.port, drain_timeout_secs=args.drain_timeout)
    decorated = consume_global_handlers()
    server.register_many(decorated)
    if hasattr(mod, "register"):
        mod.register(server)
    if not server.handlers:
        log.error("module %s registered no handlers (expose register(server) or use @pilot_handler)", args.handlers)
        return 2

    if args.reload:
        check = _watch_module_file(args.handlers, server, log, args.reload_interval)
        if check is not None:
            server.add_periodic_task(args.reload_interval, check, name="reload-watch")
            log.warning("reload: dev-mode hot-reload enabled (NOT for production)")

    try:
        return asyncio.run(_serve(server, log, args.metrics_port))
    except KeyboardInterrupt:
        log.info("worker: shutdown via KeyboardInterrupt")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
