"""Lib-owned RFC 9562 UUIDv7 (stdlib ``uuid.uuid7`` arrives in 3.14).

Combines a 48-bit Unix-ms timestamp with 74 random bits: monotonic at
millisecond resolution, unique across threads, sortable by creation time —
the id shape the Agent Server also uses for threads.
"""

import os
import uuid
from datetime import UTC, datetime


def utcnow() -> datetime:
    return datetime.now(UTC)


def uuid7() -> str:
    raw = os.urandom(16)
    now_ms = int(utcnow().timestamp() * 1000) & ((1 << 48) - 1)
    raw = (
        now_ms.to_bytes(6, "big")
        + (0b0111 << 12 | (int.from_bytes(raw[6:8], "big") & 0x0FFF)).to_bytes(2, "big")
        + (0b10 << 14 | (int.from_bytes(raw[8:10], "big") & 0x3FFF)).to_bytes(2, "big")
        + raw[10:]
    )
    return uuid.UUID(bytes=raw).hex


__all__ = ["utcnow", "uuid7"]
