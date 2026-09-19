"""Per-thread event hub: seq-stamped envelopes, bounded replay, filtered fan-out.

Every protocol event published on a thread gets a thread-scoped monotonically
increasing ``seq`` and is retained in a bounded ring buffer (default ~500
events) so an SSE subscription opened with ``since: <seq>`` can replay exactly
the missed, matching events in order before switching to live delivery.
Subscribers filter by channel (``messages``/``values``/``lifecycle``/``input``/...)
and namespace prefix; unknown channel names are ignored per extensibility.
"""

import asyncio
import contextlib
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

DEFAULT_REPLAY_SIZE = 500
MAX_QUEUE = 4096

# method (or method family) -> channel name on the wire
_METHOD_CHANNELS = {
    "lifecycle": "lifecycle",
    "messages": "messages",
    "tools": "tools",
    "values": "values",
    "updates": "updates",
    "checkpoints": "checkpoints",
    "custom": "custom",
    "tasks": "tasks",
    # the input channel's only event is named input.requested on the wire
    "input.requested": "input",
}


def channel_for_method(method: str) -> str:
    """Map a wire method to its channel; unknown methods map to themselves."""
    return _METHOD_CHANNELS.get(method, method)


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(eq=False)  # identity hash: mutable cursor, set membership
class Subscription:
    """One SSE consumer's view into a thread hub; ``last_seq`` is the dedup
    cursor advanced by replay/live delivery to guarantee exact-once order."""

    queue: asyncio.Queue[dict[str, Any]]
    channels: frozenset[str] | None  # None/empty = all channels
    namespaces: tuple[tuple[str, ...], ...]  # empty tuple = all namespaces
    last_seq: int

    def matches(self, envelope: dict[str, Any]) -> bool:
        if envelope["seq"] <= self.last_seq:
            return False
        if self.channels and channel_for_method(envelope["method"]) not in self.channels:
            return False
        if self.namespaces:
            # an explicitly empty prefix ([[]]) selects root-namespace events
            # only; non-empty prefixes are ordinary path prefix matches
            ns = tuple(envelope["params"]["namespace"])
            for prefix in self.namespaces:
                if prefix:
                    if ns[: len(prefix)] == prefix:
                        break
                elif ns == ():
                    break
            else:
                return False
        return True


@dataclass
class ThreadEventHub:
    """Seq assignment + ring buffer + fan-out for one thread's protocol events."""

    replay_size: int = DEFAULT_REPLAY_SIZE
    _seq: int = 0
    _buffer: deque[dict[str, Any]] = field(default_factory=deque)
    _subscribers: set[Subscription] = field(default_factory=set)

    def __post_init__(self) -> None:
        self._buffer = deque(maxlen=self.replay_size)

    @property
    def current_seq(self) -> int:
        return self._seq

    def publish(
        self,
        method: str,
        data: dict[str, Any],
        *,
        namespace: list[str] | None = None,
        timestamp: int | None = None,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        """Stamp, retain, and fan out one protocol event; returns the envelope."""
        self._seq += 1
        ts = timestamp if timestamp is not None else now_ms()
        envelope = {
            "type": "event",
            "event_id": event_id or f"{ts}-{self._seq}",
            "seq": self._seq,
            "method": method,
            "params": {
                "namespace": list(namespace or []),
                "timestamp": ts,
                "data": data,
            },
        }
        self._buffer.append(envelope)
        for subscriber in list(self._subscribers):
            if not subscriber.matches(envelope):
                continue
            with contextlib.suppress(asyncio.QueueFull):  # drop slow consumers
                subscriber.queue.put_nowait(envelope)
        return envelope

    def subscribe(
        self,
        channels: list[str] | None = None,
        namespaces: list[list[str]] | None = None,
        since: int | None = None,
    ) -> Subscription:
        """Register a filtered subscriber starting after ``since`` (0 = start)."""
        subscription = Subscription(
            queue=asyncio.Queue(maxsize=MAX_QUEUE),
            channels=frozenset(channels) if channels else None,
            namespaces=tuple(tuple(ns) for ns in (namespaces or [])),
            last_seq=max(since or 0, 0),
        )
        self._subscribers.add(subscription)
        return subscription

    def unsubscribe(self, subscription: Subscription) -> None:
        self._subscribers.discard(subscription)

    def replay(self, subscription: Subscription) -> list[dict[str, Any]]:
        """Matching buffered events newer than the subscriber's cursor, in order.

        Marks them delivered so the live stream skips duplicates at handoff.
        """
        matched = [env for env in self._buffer if subscription.matches(env)]
        if matched:
            subscription.last_seq = matched[-1]["seq"]
        return matched


class HubRegistry:
    """Thread-id keyed container of hubs; hubs live for the process lifetime."""

    def __init__(self, replay_size: int = DEFAULT_REPLAY_SIZE) -> None:
        self._replay_size = replay_size
        self._hubs: dict[str, ThreadEventHub] = {}

    def hub(self, thread_id: str) -> ThreadEventHub:
        hub = self._hubs.get(thread_id)
        if hub is None:
            hub = ThreadEventHub(replay_size=self._replay_size)
            self._hubs[thread_id] = hub
        return hub

    def drop(self, thread_id: str) -> None:
        self._hubs.pop(thread_id, None)


__all__ = [
    "DEFAULT_REPLAY_SIZE",
    "HubRegistry",
    "Subscription",
    "ThreadEventHub",
    "channel_for_method",
    "now_ms",
]
