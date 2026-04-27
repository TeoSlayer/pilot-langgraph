"""Worker-side checkpoint store handlers.

Runs on the remote pilot peer that backs `PilotCheckpointSaver`. Implements
the wire protocol described in `pilot_langgraph.checkpoint`.

Two store backends:

  * In-memory dict — the default. Fast, loses everything on restart.
  * SQLite-backed — opt in by setting `PILOT_CHECKPOINT_DB=/path/to/file.db`
    in the worker process environment. Durable across restarts. Stdlib only.

Usage on the worker:

    python -m pilot_langgraph.worker --port 5001 \
        --handlers pilot_langgraph.checkpoint_worker
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from collections import defaultdict
from typing import Any, Protocol


class CheckpointStore(Protocol):
    def put(self, payload: dict) -> dict: ...
    def get_tuple(self, payload: dict) -> dict: ...
    def list(self, payload: dict) -> dict: ...
    def put_writes(self, payload: dict) -> dict: ...


class _CheckpointStore:
    """Thread-safe in-memory store keyed by (thread, ns, checkpoint_id)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # (thread, ns) -> dict[checkpoint_id, record]
        self._by_thread: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
        # (thread, ns, checkpoint_id) -> list[{task_id, task_path, writes_b64}]
        self._writes: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        # (thread, ns) -> ordered list of checkpoint_ids (newest last)
        self._order: dict[tuple[str, str], list[str]] = defaultdict(list)

    def put(self, payload: dict) -> dict:
        thread = payload["thread"]
        ns = payload.get("checkpoint_ns", "") or ""
        cid = payload["checkpoint_id"]
        with self._lock:
            key = (thread, ns)
            record = {
                "checkpoint_id": cid,
                "parent_checkpoint_id": payload.get("parent_checkpoint_id"),
                "checkpoint_b64": payload["checkpoint_b64"],
                "metadata_b64": payload["metadata_b64"],
                "new_versions": payload.get("new_versions") or {},
            }
            self._by_thread[key][cid] = record
            order = self._order[key]
            if cid in order:
                order.remove(cid)
            order.append(cid)
        return {"ok": True}

    def get_tuple(self, payload: dict) -> dict:
        thread = payload["thread"]
        ns = payload.get("checkpoint_ns", "") or ""
        cid = payload.get("checkpoint_id")
        key = (thread, ns)
        with self._lock:
            records = self._by_thread.get(key)
            if not records:
                return {"found": False}
            if cid is None:
                # latest
                cid = self._order[key][-1] if self._order[key] else None
                if cid is None:
                    return {"found": False}
            record = records.get(cid)
            if record is None:
                return {"found": False}
            writes = self._writes.get((thread, ns, cid), [])
            pending: list[dict[str, Any]] = []
            for w in writes:
                # The wire keeps the writes blob as-is; the caller will
                # re-decode each (channel, value) tuple inside.
                pending.append({
                    "task_id": w["task_id"],
                    "channel": w.get("channel", ""),
                    "value_b64": w["writes_b64"],
                })
            return {
                "found": True,
                "checkpoint_id": cid,
                "parent_checkpoint_id": record["parent_checkpoint_id"],
                "checkpoint_b64": record["checkpoint_b64"],
                "metadata_b64": record["metadata_b64"],
                "pending_writes": pending,
            }

    def list(self, payload: dict) -> dict:
        thread = payload.get("thread")
        ns = payload.get("checkpoint_ns") or ""
        before_cid = payload.get("before_checkpoint_id")
        limit = payload.get("limit")
        items: list[dict[str, Any]] = []
        with self._lock:
            keys = [(thread, ns)] if thread is not None else list(self._by_thread.keys())
            for key in keys:
                if key not in self._order:
                    continue
                ordered_ids = list(reversed(self._order[key]))  # newest first
                if before_cid:
                    if before_cid in ordered_ids:
                        idx = ordered_ids.index(before_cid)
                        ordered_ids = ordered_ids[idx + 1:]
                for cid in ordered_ids:
                    record = self._by_thread[key].get(cid)
                    if record is None:
                        continue
                    items.append({
                        "thread": key[0],
                        "checkpoint_ns": key[1],
                        "checkpoint_id": cid,
                        "parent_checkpoint_id": record["parent_checkpoint_id"],
                        "checkpoint_b64": record["checkpoint_b64"],
                        "metadata_b64": record["metadata_b64"],
                    })
                    if limit is not None and len(items) >= limit:
                        break
                if limit is not None and len(items) >= limit:
                    break
        return {"items": items}

    def put_writes(self, payload: dict) -> dict:
        key = (payload["thread"], payload.get("checkpoint_ns", "") or "", payload["checkpoint_id"])
        # Dedupe by (task_id, task_path) so retries are idempotent.
        ident = (payload["task_id"], payload.get("task_path", ""))
        with self._lock:
            existing = self._writes[key]
            for w in existing:
                if (w["task_id"], w.get("task_path", "")) == ident:
                    return {"ok": True}
            existing.append({
                "task_id": payload["task_id"],
                "task_path": payload.get("task_path", ""),
                "writes_b64": payload["writes_b64"],
                "channel": "",
            })
        return {"ok": True}


class _SqliteCheckpointStore:
    """Durable store backed by sqlite3 from the standard library.

    Schema (auto-created):
        checkpoints(thread, ns, cid, parent_cid, blob_dict_json, meta_dict_json, new_versions_json, seq INTEGER PRIMARY KEY AUTOINCREMENT)
            UNIQUE(thread, ns, cid) — checkpoint_id is unique per thread/ns
        writes(thread, ns, cid, task_id, task_path, value_dict_json, seq INTEGER PRIMARY KEY AUTOINCREMENT)

    Order is preserved by the autoincrement `seq` column. The library expects
    the typed-dict (`{"type":..., "blob":...}`) coming over the wire, so we
    just JSON-encode it once for storage.
    """

    def __init__(self, path: str):
        self.path = path
        # Per-thread connection cache; sqlite3 connections aren't safe to
        # share across threads by default, so we open per-thread on demand.
        self._tls = threading.local()
        # Bootstrap schema on init (use a short-lived connection).
        with sqlite3.connect(path) as boot:
            boot.execute("PRAGMA journal_mode=WAL")
            boot.executescript("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread TEXT NOT NULL,
                    ns TEXT NOT NULL DEFAULT '',
                    cid TEXT NOT NULL,
                    parent_cid TEXT,
                    blob_dict_json TEXT NOT NULL,
                    meta_dict_json TEXT NOT NULL,
                    new_versions_json TEXT NOT NULL DEFAULT '{}',
                    UNIQUE(thread, ns, cid)
                );
                CREATE INDEX IF NOT EXISTS ix_checkpoints_thread
                    ON checkpoints(thread, ns, seq DESC);

                CREATE TABLE IF NOT EXISTS writes (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread TEXT NOT NULL,
                    ns TEXT NOT NULL DEFAULT '',
                    cid TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    task_path TEXT NOT NULL DEFAULT '',
                    value_dict_json TEXT NOT NULL,
                    UNIQUE(thread, ns, cid, task_id, task_path)
                );
                CREATE INDEX IF NOT EXISTS ix_writes_checkpoint
                    ON writes(thread, ns, cid, seq);
            """)

    def _conn(self) -> sqlite3.Connection:
        c = getattr(self._tls, "conn", None)
        if c is None:
            c = sqlite3.connect(self.path, isolation_level=None)  # autocommit
            c.execute("PRAGMA journal_mode=WAL")
            self._tls.conn = c
        return c

    def put(self, payload: dict) -> dict:
        thread = payload["thread"]
        ns = payload.get("checkpoint_ns") or ""
        cid = payload["checkpoint_id"]
        c = self._conn()
        c.execute(
            "INSERT OR REPLACE INTO checkpoints(thread, ns, cid, parent_cid, blob_dict_json, meta_dict_json, new_versions_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (thread, ns, cid, payload.get("parent_checkpoint_id"),
             json.dumps(payload["checkpoint_b64"]), json.dumps(payload["metadata_b64"]),
             json.dumps(payload.get("new_versions") or {})),
        )
        return {"ok": True}

    def get_tuple(self, payload: dict) -> dict:
        thread = payload["thread"]
        ns = payload.get("checkpoint_ns") or ""
        cid = payload.get("checkpoint_id")
        c = self._conn()
        if cid is None:
            row = c.execute(
                "SELECT cid, parent_cid, blob_dict_json, meta_dict_json FROM checkpoints "
                "WHERE thread=? AND ns=? ORDER BY seq DESC LIMIT 1",
                (thread, ns),
            ).fetchone()
        else:
            row = c.execute(
                "SELECT cid, parent_cid, blob_dict_json, meta_dict_json FROM checkpoints "
                "WHERE thread=? AND ns=? AND cid=?",
                (thread, ns, cid),
            ).fetchone()
        if row is None:
            return {"found": False}
        actual_cid, parent_cid, blob_json, meta_json = row
        write_rows = c.execute(
            "SELECT task_id, value_dict_json FROM writes WHERE thread=? AND ns=? AND cid=? ORDER BY seq",
            (thread, ns, actual_cid),
        ).fetchall()
        pending = [
            {"task_id": tid, "channel": "", "value_b64": json.loads(vj)}
            for tid, vj in write_rows
        ]
        return {
            "found": True,
            "checkpoint_id": actual_cid,
            "parent_checkpoint_id": parent_cid,
            "checkpoint_b64": json.loads(blob_json),
            "metadata_b64": json.loads(meta_json),
            "pending_writes": pending,
        }

    def list(self, payload: dict) -> dict:
        thread = payload.get("thread")
        ns = payload.get("checkpoint_ns") or ""
        before_cid = payload.get("before_checkpoint_id")
        limit = payload.get("limit")
        c = self._conn()

        sql = "SELECT thread, ns, cid, parent_cid, blob_dict_json, meta_dict_json, seq FROM checkpoints WHERE 1=1"
        args: list = []
        if thread is not None:
            sql += " AND thread=?"
            args.append(thread)
            if payload.get("checkpoint_ns") is not None:
                sql += " AND ns=?"
                args.append(ns)
        if before_cid:
            row = c.execute(
                "SELECT seq FROM checkpoints WHERE thread=? AND ns=? AND cid=?",
                (thread, ns, before_cid),
            ).fetchone()
            if row:
                sql += " AND seq<?"
                args.append(row[0])
        sql += " ORDER BY seq DESC"
        if limit is not None:
            sql += " LIMIT ?"
            args.append(limit)

        items = []
        for r in c.execute(sql, args):
            t, n, cid, parent_cid, blob_json, meta_json, _seq = r
            items.append({
                "thread": t,
                "checkpoint_ns": n,
                "checkpoint_id": cid,
                "parent_checkpoint_id": parent_cid,
                "checkpoint_b64": json.loads(blob_json),
                "metadata_b64": json.loads(meta_json),
            })
        return {"items": items}

    def put_writes(self, payload: dict) -> dict:
        c = self._conn()
        # OR IGNORE: if the same (thread, ns, cid, task_id, task_path) row already
        # exists, the retry is a no-op. Lets the caller retry safely on transient
        # tunnel failures without ending up with duplicate writes in history.
        c.execute(
            "INSERT OR IGNORE INTO writes(thread, ns, cid, task_id, task_path, value_dict_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (payload["thread"], payload.get("checkpoint_ns") or "",
             payload["checkpoint_id"], payload["task_id"],
             payload.get("task_path", ""), json.dumps(payload["writes_b64"])),
        )
        return {"ok": True}


def make_store() -> CheckpointStore:
    db_path = os.environ.get("PILOT_CHECKPOINT_DB", "").strip()
    if db_path:
        return _SqliteCheckpointStore(db_path)
    return _CheckpointStore()


_STORE: CheckpointStore | None = None


def register(server) -> None:
    global _STORE
    if _STORE is None:
        _STORE = make_store()
    server.register("checkpoint_put", _STORE.put)
    server.register("checkpoint_get_tuple", _STORE.get_tuple)
    server.register("checkpoint_list", _STORE.list)
    server.register("checkpoint_put_writes", _STORE.put_writes)
