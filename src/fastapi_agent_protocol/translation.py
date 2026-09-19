"""LangGraph stream-tuple -> Agent Streaming Protocol v2 event translation.

Two pure-state machines driven by the run manager:

- :class:`MessagesTranslator` consumes ``("messages", chunk, meta)`` tuples and
  frames each message as ``message-start`` -> ``content-block-*`` deltas
  (``text-delta`` for text, accumulated ``tool_call_chunk`` argument fragments
  as ``block-delta``) -> ``content-block-finish`` (parsed tool call) ->
  ``message-finish``. v1 scope is root-namespace content only.

- :func:`values_event_data` wraps full-state snapshots for the ``values``
  channel; :func:`interrupts_from_snapshot` extracts pending LangGraph
  interrupts (``__interrupt__``) as ``(interrupt_id, payload)`` pairs for
  ``input.requested`` emission.
"""

import json
from typing import Any

_ROLE_BY_TYPE = {
    "aimessage": "ai",
    "aimessagechunk": "ai",
    "humanmessage": "human",
    "humanmessagechunk": "human",
    "systemmessage": "system",
    "systemmessagechunk": "system",
    "toolmessage": "tool",
    "toolmessagechunk": "tool",
}


def _role(chunk: Any) -> str | None:
    return _ROLE_BY_TYPE.get(str(getattr(chunk, "type", "")).lower())


class _BlockState:
    """Accumulator for one streamed tool-call argument block."""

    def __init__(self, index: int, call_id: str | None, name: str | None) -> None:
        self.index = index
        self.call_id = call_id
        self.name = name
        self.args = ""

    def parse_args(self) -> dict[str, Any]:
        raw = self.args or "{}"
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        except json.JSONDecodeError:
            return {"raw": raw}


class MessageFrameEmitter:
    """Per-message-id frame state: start/deltas/blocks/finish bookkeeping."""

    def __init__(self, message_id: str, role: str) -> None:
        self.message_id = message_id
        self.role = role
        self.started = False
        self.finished = False
        self.text_index: int | None = None
        self.blocks: dict[int, _BlockState] = {}
        self.next_index = 0
        self.usage: dict[str, Any] | None = None

    def allocate_index(self) -> int:
        index = self.next_index
        self.next_index += 1
        return index


class MessagesTranslator:
    """Translate ``("messages", chunk, meta)`` tuples into protocol frames.

    Emits onto a callable sink (usually the thread hub's ``publish`` bound to
    the ``messages`` channel). One translator instance serves one run; state
    is keyed by streamed chunk id so concurrent messages interleave safely.
    """

    def __init__(self, sink: Any) -> None:
        self._sink = sink
        self._open: dict[str, MessageFrameEmitter] = {}

    def feed(self, chunk: Any, meta: dict[str, Any] | None = None) -> None:
        # v1 scope is root-namespace-only streams: every streamed chunk is
        # emitted at namespace=[] (subagent-scoped projection is a non-goal).
        del meta
        role = _role(chunk)
        if role is None:
            return
        message_id = getattr(chunk, "id", None) or f"msg-{id(chunk)}"
        emitter = self._open.get(message_id)
        if emitter is None:
            # sequential stream: a brand-new message terminates any still-open ones
            self.finish(None)
            emitter = MessageFrameEmitter(message_id, role)
            self._open[message_id] = emitter
        if emitter.finished:
            return
        self._emit_start(emitter)
        self._emit_text_delta(emitter, chunk)
        self._emit_tool_call_chunks(emitter, chunk)

    def finish(self, chunk: Any, meta: dict[str, Any] | None = None) -> None:
        """Terminate frames for this chunk's message (or all open messages)."""
        targets: list[MessageFrameEmitter]
        if chunk is None:
            targets = [e for e in self._open.values() if not e.finished]
        else:
            message_id = getattr(chunk, "id", None)
            emitter = self._open.get(message_id) if message_id else None
            targets = [] if emitter is None or emitter.finished else [emitter]

        usage = getattr(chunk, "usage_metadata", None) if chunk is not None else None
        for emitter in targets:
            if usage:
                emitter.usage = dict(usage)
            self._finalize_blocks(emitter)
            data: dict[str, Any] = {"event": "message-finish"}
            if emitter.usage:
                data["usage"] = emitter.usage
            self._sink(data)
            emitter.finished = True

    # --- internals -----------------------------------------------------------

    def _emit_start(self, emitter: MessageFrameEmitter) -> None:
        if emitter.started:
            return
        emitter.started = True
        self._sink({"event": "message-start", "role": emitter.role, "id": emitter.message_id})

    def _ensure_text_index(self, emitter: MessageFrameEmitter) -> int:
        if emitter.text_index is None:
            emitter.text_index = emitter.allocate_index()
        return emitter.text_index

    def _emit_text_delta(self, emitter: MessageFrameEmitter, chunk: Any) -> None:
        text = self._text_content(chunk)
        if not text:
            return
        index = self._ensure_text_index(emitter)
        self._sink(
            {
                "event": "content-block-delta",
                "index": index,
                "delta": {"type": "text-delta", "text": text},
            }
        )

    @staticmethod
    def _text_content(chunk: Any) -> str:
        """Plain-text fragment of a streamed message chunk."""
        content = getattr(chunk, "content", None)
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for block in content:
                if isinstance(block, str):
                    parts.append(block)
                elif isinstance(block, dict) and block.get("type") in ("text", "text_delta"):
                    parts.append(str(block.get("text") or ""))
            return "".join(parts)
        return ""

    def _emit_tool_call_chunks(self, emitter: MessageFrameEmitter, chunk: Any) -> None:
        fragments = list(getattr(chunk, "tool_call_chunks", None) or [])
        for fragment in fragments:
            index = fragment.get("index")
            if index is None:
                index = emitter.allocate_index()
            state = emitter.blocks.get(index)
            if state is None:
                state = _BlockState(index, fragment.get("id"), fragment.get("name"))
                emitter.blocks[index] = state
                self._sink(
                    {
                        "event": "content-block-start",
                        "index": index,
                        "content": {
                            "type": "tool_call_chunk",
                            "id": state.call_id,
                            "name": state.name,
                            "args": "",
                            "index": index,
                        },
                    }
                )
            else:
                state.call_id = state.call_id or fragment.get("id")
                state.name = state.name or fragment.get("name")
            args_fragment = fragment.get("args")
            if isinstance(args_fragment, str) and args_fragment:
                state.args += args_fragment
                self._sink(
                    {
                        "event": "content-block-delta",
                        "index": index,
                        "delta": {
                            "type": "block-delta",
                            "fields": {
                                "type": "tool_call_chunk",
                                "id": state.call_id,
                                "name": state.name,
                                "args": args_fragment,
                            },
                        },
                    }
                )

    def _finalize_blocks(self, emitter: MessageFrameEmitter) -> None:
        for index in sorted(emitter.blocks):
            state = emitter.blocks[index]
            self._sink(
                {
                    "event": "content-block-finish",
                    "index": index,
                    "content": {
                        "type": "tool_call",
                        "id": state.call_id,
                        "name": state.name,
                        "args": state.parse_args(),
                        "index": index,
                    },
                }
            )


# --- values channel + interrupts ---------------------------------------------


def values_event_data(state: dict[str, Any]) -> dict[str, Any]:
    """Full-state snapshot payload for a ``values`` event (data verbatim)."""
    return dict(state or {})


def interrupts_from_snapshot(snapshot_values: dict[str, Any]) -> list[tuple[str, Any]]:
    """Extract ``(interrupt_id, payload)`` pairs from state holding
    ``__interrupt__`` entries (LangGraph Interrupt objects or plain dicts).
    """
    entries = (snapshot_values or {}).get("__interrupt__") or []
    pairs: list[tuple[str, Any]] = []
    for entry in entries:
        interrupt_id = getattr(entry, "id", None) or (
            entry.get("id") if isinstance(entry, dict) else None
        )
        payload = entry.get("value") if isinstance(entry, dict) else getattr(entry, "value", None)
        pairs.append((str(interrupt_id), payload))
    return pairs


__all__ = [
    "MessageFrameEmitter",
    "MessagesTranslator",
    "interrupts_from_snapshot",
    "values_event_data",
]
