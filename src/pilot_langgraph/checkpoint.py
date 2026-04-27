"""LangGraph BaseCheckpointSaver backed by a remote Pilot peer.

A graph that uses `checkpointer=PilotCheckpointSaver(peer="my-store")` will
ship every checkpoint, every metadata blob, and every intermediate write to
that peer over a Pilot tunnel. The peer runs the matching handlers (see
`pilot_langgraph.checkpoint_worker`) which back onto an in-memory or
persistent store. This makes graph state survive process death and lets
multiple processes drive the same thread by talking to the same store.

Checkpoint blobs and metadata are serialized via LangGraph's standard
`JsonPlusSerializer` and base64-encoded for JSON-safe transport.

Wire protocol (handler name → request → reply):
    checkpoint_put         {thread,checkpoint_ns,checkpoint_id,parent_id,
                            checkpoint_b64,metadata_b64,new_versions} -> {ok:true}
    checkpoint_get_tuple   {thread,checkpoint_ns,checkpoint_id?} ->
                            {found:bool, checkpoint_b64?, metadata_b64?,
                             checkpoint_id?, parent_checkpoint_id?, pending_writes?}
    checkpoint_list        {thread?,checkpoint_ns?,filter?,before?,limit?} ->
                            {items: [<same shape as get_tuple result>...]}
    checkpoint_put_writes  {thread,checkpoint_ns,checkpoint_id,task_id,
                            task_path,writes_b64} -> {ok:true}
"""
from __future__ import annotations

import base64
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    ChannelVersions,
)
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from ._ipc import Addr
from .runnables import PilotRemoteRunnable, _run_sync

import asyncio as _asyncio
import logging as _logging

_log = _logging.getLogger(__name__)


async def _aretry(coro_factory, *, attempts: int = 3, base_delay: float = 0.5, op: str = "checkpoint op"):
    """Retry a coroutine factory on transient failures with exponential backoff.

    `coro_factory` is a zero-arg callable returning a fresh coroutine each
    call (so we can retry without re-awaiting a consumed coroutine).
    """
    last: Exception | None = None
    for i in range(attempts):
        try:
            return await coro_factory()
        except Exception as e:
            last = e
            if i == attempts - 1:
                raise
            delay = base_delay * (2 ** i)
            _log.info("%s: retry %d/%d after %s (sleep %.1fs)", op, i + 1, attempts, e, delay)
            await _asyncio.sleep(delay)
    if last:
        raise last


def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def _ub64(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"))


class PilotCheckpointSaver(BaseCheckpointSaver):
    """Checkpoint saver that persists graph state on a remote Pilot peer.

    Args:
        peer: pilot address or hostname running the checkpoint handlers.
        port: pilot port the checkpoint handlers listen on (default 5001).
        timeout_secs: per-call wall-clock budget.
        socket_path: override `$PILOT_SOCKET`.
    """

    def __init__(
        self,
        *,
        peer: str | Addr,
        port: int = 5001,
        timeout_secs: float = 30.0,
        socket_path: str | None = None,
    ):
        super().__init__(serde=JsonPlusSerializer())
        self.peer = peer
        self.port = port
        self.timeout_secs = timeout_secs
        self._call_put = PilotRemoteRunnable(node="checkpoint_put", peer=peer, port=port,
                                             timeout_secs=timeout_secs, socket_path=socket_path)
        self._call_get = PilotRemoteRunnable(node="checkpoint_get_tuple", peer=peer, port=port,
                                             timeout_secs=timeout_secs, socket_path=socket_path)
        self._call_list = PilotRemoteRunnable(node="checkpoint_list", peer=peer, port=port,
                                              timeout_secs=timeout_secs, socket_path=socket_path)
        self._call_writes = PilotRemoteRunnable(node="checkpoint_put_writes", peer=peer, port=port,
                                                timeout_secs=timeout_secs, socket_path=socket_path)

    # ---- (de)serialization ----
    # JsonPlusSerializer uses different type tags ("msgpack", "json") depending
    # on the input — round-trip the tag through the wire as part of the blob.
    def _encode_typed(self, value: Any) -> dict[str, str]:
        type_, blob = self.serde.dumps_typed(value)
        return {"type": type_, "blob": _b64(blob)}

    def _decode_typed(self, blob_dict: dict[str, str]) -> Any:
        return self.serde.loads_typed((blob_dict["type"], _ub64(blob_dict["blob"])))

    def _encode_checkpoint(self, c: Checkpoint) -> dict[str, str]:
        return self._encode_typed(c)

    def _decode_checkpoint(self, blob_dict: dict[str, str]) -> Checkpoint:
        return self._decode_typed(blob_dict)

    def _encode_metadata(self, m: CheckpointMetadata) -> dict[str, str]:
        return self._encode_typed(m)

    def _decode_metadata(self, blob_dict: dict[str, str]) -> CheckpointMetadata:
        return self._decode_typed(blob_dict)

    def _encode_writes(self, writes: Sequence[tuple[str, Any]]) -> dict[str, str]:
        return self._encode_typed(list(writes))

    def _decode_writes(self, blob_dict: dict[str, str]) -> list[tuple[str, Any]]:
        return self._decode_typed(blob_dict)

    # ---- async API (primary) ----
    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        thread = config["configurable"]["thread_id"]
        ns = config["configurable"].get("checkpoint_ns", "")
        cid = checkpoint["id"]
        parent = config["configurable"].get("checkpoint_id")
        payload = {
            "thread": thread,
            "checkpoint_ns": ns,
            "checkpoint_id": cid,
            "parent_checkpoint_id": parent,
            "checkpoint_b64": self._encode_checkpoint(checkpoint),
            "metadata_b64": self._encode_metadata(metadata),
            "new_versions": dict(new_versions),
        }
        # checkpoint_put is idempotent (INSERT OR REPLACE on the same cid).
        await _aretry(lambda: self._call_put.ainvoke(payload), op=f"aput cid={cid}")
        return {
            "configurable": {
                **config["configurable"],
                "thread_id": thread,
                "checkpoint_ns": ns,
                "checkpoint_id": cid,
            }
        }

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        cfgable = config["configurable"]
        thread = cfgable["thread_id"]
        ns = cfgable.get("checkpoint_ns", "")
        cid = cfgable.get("checkpoint_id")
        # Read is idempotent.
        result = await _aretry(
            lambda: self._call_get.ainvoke({"thread": thread, "checkpoint_ns": ns, "checkpoint_id": cid}),
            op=f"aget_tuple thread={thread}",
        )
        if not result.get("found"):
            return None
        ckpt = self._decode_checkpoint(result["checkpoint_b64"])
        meta = self._decode_metadata(result["metadata_b64"])
        pending: list[tuple[str, str, Any]] = []
        for raw in result.get("pending_writes") or []:
            pending.append((raw["task_id"], raw["channel"], self._decode_writes(raw["value_b64"])[0][1]))
        out_cid = result.get("checkpoint_id", ckpt["id"])
        parent_cid = result.get("parent_checkpoint_id")
        out_cfg: RunnableConfig = {
            "configurable": {
                **cfgable,
                "thread_id": thread,
                "checkpoint_ns": ns,
                "checkpoint_id": out_cid,
            }
        }
        parent_cfg: RunnableConfig | None = None
        if parent_cid:
            parent_cfg = {
                "configurable": {
                    "thread_id": thread,
                    "checkpoint_ns": ns,
                    "checkpoint_id": parent_cid,
                }
            }
        return CheckpointTuple(out_cfg, ckpt, meta, parent_cfg, pending)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        cfgable = (config or {}).get("configurable", {}) if config else {}
        before_cfgable = (before or {}).get("configurable", {}) if before else {}
        list_payload = {
            "thread": cfgable.get("thread_id"),
            "checkpoint_ns": cfgable.get("checkpoint_ns"),
            "filter": filter,
            "before_checkpoint_id": before_cfgable.get("checkpoint_id") if before_cfgable else None,
            "limit": limit,
        }
        # Read is idempotent.
        result = await _aretry(lambda: self._call_list.ainvoke(list_payload), op="alist")
        for item in result.get("items", []):
            ckpt = self._decode_checkpoint(item["checkpoint_b64"])
            meta = self._decode_metadata(item["metadata_b64"])
            cid = item.get("checkpoint_id", ckpt["id"])
            parent_cid = item.get("parent_checkpoint_id")
            thread = item.get("thread", cfgable.get("thread_id"))
            ns = item.get("checkpoint_ns", cfgable.get("checkpoint_ns", ""))
            out_cfg: RunnableConfig = {
                "configurable": {"thread_id": thread, "checkpoint_ns": ns, "checkpoint_id": cid}
            }
            parent_cfg: RunnableConfig | None = None
            if parent_cid:
                parent_cfg = {
                    "configurable": {"thread_id": thread, "checkpoint_ns": ns, "checkpoint_id": parent_cid}
                }
            yield CheckpointTuple(out_cfg, ckpt, meta, parent_cfg, [])

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        cfgable = config["configurable"]
        payload = {
            "thread": cfgable["thread_id"],
            "checkpoint_ns": cfgable.get("checkpoint_ns", ""),
            "checkpoint_id": cfgable["checkpoint_id"],
            "task_id": task_id,
            "task_path": task_path,
            "writes_b64": self._encode_writes(writes),
        }
        # The worker dedupes by (thread, ns, cid, task_id, task_path) so retries
        # are idempotent and won't append duplicate write rows.
        await _aretry(lambda: self._call_writes.ainvoke(payload),
                      op=f"aput_writes task={task_id}")

    # ---- sync wrappers ----
    def put(self, config, checkpoint, metadata, new_versions):
        return _run_sync(self.aput(config, checkpoint, metadata, new_versions))

    def get_tuple(self, config):
        return _run_sync(self.aget_tuple(config))

    def list(self, config, *, filter=None, before=None, limit=None) -> Iterator[CheckpointTuple]:
        async def _collect():
            out = []
            async for t in self.alist(config, filter=filter, before=before, limit=limit):
                out.append(t)
            return out
        return iter(_run_sync(_collect()))

    def put_writes(self, config, writes, task_id, task_path=""):
        return _run_sync(self.aput_writes(config, writes, task_id, task_path))
