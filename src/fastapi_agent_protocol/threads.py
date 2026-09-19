"""Pluggable thread storage: protocol needs existence + opaque metadata +
last-assistant binding only; app FKs stay outside (no SQLAlchemy in lib).

Thread row: ``thread_id + metadata (opaque) + assistant_id + timestamps``.
Ships ``memory`` (default, zero deps) and ``sqlite(path)`` built-ins; custom
stores implement :class:`ThreadStore`.
"""

import json
import sqlite3
import threading
from dataclasses import dataclass, field
from typing import Any, Protocol

from fastapi_agent_protocol.uuid7 import utcnow, uuid7


@dataclass
class ThreadRecord:
    thread_id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    assistant_id: str | None = None
    created_at: str = ""
    updated_at: str = ""


class ThreadStore(Protocol):
    """Minimal persistence surface the protocol runtime needs."""

    def create(self, metadata: dict[str, Any] | None = None) -> ThreadRecord:
        """Provision a thread row with a uuidv7 id; returns the record."""
        ...

    def get(self, thread_id: str) -> ThreadRecord | None:
        """Look up a thread by id, or None when unknown."""
        ...

    def set_assistant(self, thread_id: str, assistant_id: str) -> None:
        """Persist the last graph that ran on a thread (restart-safe hydration)."""
        ...


class MemoryThreadStore:
    """In-process default: zero deps; threads vanish on restart."""

    def __init__(self) -> None:
        self._threads: dict[str, ThreadRecord] = {}
        self._lock = threading.Lock()

    def create(self, metadata: dict[str, Any] | None = None) -> ThreadRecord:
        now = utcnow().isoformat()
        record = ThreadRecord(
            thread_id=uuid7(),
            metadata=dict(metadata or {}),
            assistant_id=None,
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._threads[record.thread_id] = record
        return record

    def get(self, thread_id: str) -> ThreadRecord | None:
        with self._lock:
            record = self._threads.get(thread_id)
            if record is None:
                return None
            return ThreadRecord(
                thread_id=record.thread_id,
                metadata=dict(record.metadata),
                assistant_id=record.assistant_id,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )

    def set_assistant(self, thread_id: str, assistant_id: str) -> None:
        with self._lock:
            record = self._threads.get(thread_id)
            if record is not None:
                record.assistant_id = assistant_id
                record.updated_at = utcnow().isoformat()


class SqliteThreadStore:
    """Durable built-in over stdlib ``sqlite3`` (no ORM).

    ``assistant_id`` survives restarts so state/history/interrupt resume use
    the matching graph schema with the user's own saver.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS threads ("
            "thread_id TEXT PRIMARY KEY, "
            "metadata_json TEXT NOT NULL DEFAULT '{}', "
            "assistant_id TEXT, "
            "created_at TEXT NOT NULL, "
            "updated_at TEXT NOT NULL)"
        )
        self._conn.commit()

    def create(self, metadata: dict[str, Any] | None = None) -> ThreadRecord:
        now = utcnow().isoformat()
        record = ThreadRecord(
            thread_id=uuid7(),
            metadata=dict(metadata or {}),
            assistant_id=None,
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._conn.execute(
                "INSERT INTO threads (thread_id, metadata_json, assistant_id,"
                " created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (record.thread_id, json.dumps(record.metadata), None, now, now),
            )
            self._conn.commit()
        return record

    def get(self, thread_id: str) -> ThreadRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT thread_id, metadata_json, assistant_id, created_at, updated_at"
                " FROM threads WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
        if row is None:
            return None
        tid, metadata_json, assistant_id, created_at, updated_at = row
        try:
            metadata = json.loads(metadata_json) if metadata_json else {}
        except (json.JSONDecodeError, TypeError):
            metadata = {}
        return ThreadRecord(
            thread_id=tid,
            metadata=dict(metadata) if isinstance(metadata, dict) else {},
            assistant_id=assistant_id,
            created_at=created_at,
            updated_at=updated_at,
        )

    def set_assistant(self, thread_id: str, assistant_id: str) -> None:
        now = utcnow().isoformat()
        with self._lock:
            self._conn.execute(
                "UPDATE threads SET assistant_id = ?, updated_at = ? WHERE thread_id = ?",
                (assistant_id, now, thread_id),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def memory() -> MemoryThreadStore:
    """Zero-config default thread store (in-process)."""
    return MemoryThreadStore()


def sqlite(path: str) -> SqliteThreadStore:
    """Durable SQLite-backed thread store at ``path``."""
    return SqliteThreadStore(path)


__all__ = [
    "MemoryThreadStore",
    "SqliteThreadStore",
    "ThreadRecord",
    "ThreadStore",
    "memory",
    "sqlite",
]
