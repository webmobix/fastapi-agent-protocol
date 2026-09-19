"""Checkpointer-backed thread-state reads for the protocol endpoints.

State/history reads always go through the last-assistant graph (checkpoint
channels only decode through a matching schema); untouched threads have no
checkpoints and return empty state without requiring any graph (no probe —
a probe would need a lib-owned saver, which is out of scope).

Message objects are serialized into the flat wire shape the Agent Server
returns. Graphs that checkpoint delta channels (e.g. DeepAgents) leave
``channel_values`` for delta channels empty and store each superstep's output
in the checkpoint's pending writes instead; history reconstruction walks
oldest-first accumulating pending writes to rebuild cumulative values.
"""

import copy
import logging
from typing import Any

from langchain_core.messages import message_to_dict

logger = logging.getLogger(__name__)


def _to_jsonable(value: Any) -> Any:
    """Recursively coerce a value into something JSON-serializable, flattening
    LangChain Messages, dataclasses, and LangGraph objects encountered inside
    state values."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {k: _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_to_jsonable(v) for v in value]
    if hasattr(value, "__dict__") and not isinstance(value, type):
        return {k: _to_jsonable(v) for k, v in vars(value).items() if not k.startswith("_")}
    return str(value)


def serialize_values(values: dict[str, Any] | None) -> dict[str, Any]:
    """Convert a raw state values dict into the JSON-safe wire shape."""
    if not values:
        return {}
    out: dict[str, Any] = {}
    for key, value in values.items():
        if key == "messages" and isinstance(value, (list, tuple)):
            out[key] = [serialize_message(m) for m in value]
        else:
            out[key] = _to_jsonable(value)
    return out


def serialize_message(message: Any) -> dict[str, Any]:
    """Flatten a LangChain Message into the `{type, content, ...}` wire shape."""
    try:
        serialized = message_to_dict(message)
        data = dict(serialized["data"])
        data.setdefault("type", serialized.get("type"))
        return data
    except Exception:  # not a message or unserializable
        return {"type": "unknown", "content": str(message)}


def empty_state(thread_id: str) -> dict[str, Any]:
    """Wire-shaped state for untouched threads (no checkpoints, no graph)."""
    return {
        "values": {},
        "next": [],
        "tasks": [],
        "metadata": {},
        "created_at": None,
        "checkpoint": {
            "checkpoint_id": None,
            "thread_id": thread_id,
            "checkpoint_ns": "",
        },
        "parent_checkpoint": None,
        "interrupts": [],
        "checkpoint_id": None,
        "parent_checkpoint_id": None,
    }


async def read_thread_history(
    graph: Any,
    config: dict[str, Any],
    *,
    limit: int = 10,
    before: str | None = None,
) -> list[dict[str, Any]]:
    """Materialize past states for history endpoints, newest first.

    Returns checkpointed snapshots in the protocol's ThreadState wire shape
    (``{checkpoint: {checkpoint_id}, values, messages, metadata}``);
    ``before`` is a checkpoint id cursor for pagination.
    """
    before_config = (
        {
            "configurable": {
                "thread_id": config["configurable"]["thread_id"],
                "checkpoint_ns": config["configurable"].get("checkpoint_ns", ""),
                "checkpoint_id": before,
            }
        }
        if before
        else None
    )
    checkpointer = getattr(graph, "checkpointer", None)
    if checkpointer is None:
        raise ValueError("No checkpointer set")
    tuples = [t async for t in checkpointer.alist(config, before=before_config, limit=limit)]
    # (checkpoint_id, values-so-far) accumulated oldest -> newest
    cumulative: dict[str, Any] = {}
    reconstructed: list[tuple[str | None, dict[str, Any], dict[str, Any] | None]] = []
    for checkpoint_tuple in reversed(tuples):  # oldest first
        for _task_id, channel, value in checkpoint_tuple.pending_writes or []:
            if channel in ("__interrupt__", "__error__", "__resume__") or channel.startswith(
                "branch:"
            ):
                continue
            if isinstance(value, list):
                cumulative.setdefault(channel, []).extend(value)
            else:
                cumulative[channel] = value
        reconstructed.append(
            (
                (checkpoint_tuple.config or {}).get("configurable", {}).get("checkpoint_id"),
                copy.deepcopy(dict(cumulative)),
                checkpoint_tuple.metadata,
            )
        )
    out: list[dict[str, Any]] = []
    for checkpoint_id, values, metadata in reversed(reconstructed):  # newest first
        entry: dict[str, Any] = {
            "checkpoint": {"checkpoint_id": checkpoint_id},
            "values": serialize_values(values),
            "metadata": metadata,
        }
        out.append(entry)
    return out


async def read_thread_state(graph: Any, config: dict[str, Any]) -> dict[str, Any]:
    """Materialize the full state snapshot a state endpoint returns."""
    snapshot = await graph.aget_state(config)

    def interrupt_dict(interrupt: Any) -> dict[str, Any]:
        return {
            "id": getattr(interrupt, "id", None),
            "value": _to_jsonable(getattr(interrupt, "value", None)),
        }

    values = serialize_values(snapshot.values)
    interrupts = [interrupt_dict(i) for i in snapshot.interrupts]
    tasks = [
        {
            "id": getattr(task, "id", None),
            "name": getattr(task, "name", None),
            "path": list(getattr(task, "path", ()) or ()),
            "error": _to_jsonable(getattr(task, "error", None)),
            "interrupts": [interrupt_dict(i) for i in getattr(task, "interrupts", ())],
        }
        for task in snapshot.tasks
    ]
    thread_config = config.get("configurable") or {}
    return {
        "values": values,
        "next": list(snapshot.next),
        "tasks": tasks,
        "metadata": snapshot.metadata,
        "created_at": snapshot.created_at
        if isinstance(snapshot.created_at, str)
        else (snapshot.created_at.isoformat() if snapshot.created_at else None),
        "checkpoint": {
            "checkpoint_id": thread_config.get("checkpoint_id"),
            "thread_id": thread_config.get("thread_id"),
            "checkpoint_ns": thread_config.get("checkpoint_ns", ""),
        },
        "parent_checkpoint": None,
        "interrupts": interrupts,
        "checkpoint_id": thread_config.get("checkpoint_id"),
        "parent_checkpoint_id": None,
    }


__all__ = [
    "empty_state",
    "read_thread_history",
    "read_thread_state",
    "serialize_message",
    "serialize_values",
]
