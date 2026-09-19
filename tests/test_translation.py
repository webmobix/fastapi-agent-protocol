"""Task 2.2: messages translation + values/interrupt helpers vs golden shapes."""

from typing import Any

from langchain_core.messages import AIMessageChunk

from fastapi_agent_protocol.translation import (
    MessagesTranslator,
    interrupts_from_snapshot,
    values_event_data,
)


def _text_chunk(text: str, msg_id: str = "m1") -> AIMessageChunk:
    return AIMessageChunk(content=text, id=msg_id)


def test_frame_sequence_text() -> None:
    frames: list[dict] = []
    tr = MessagesTranslator(frames.append)
    tr.feed(_text_chunk("hel"))
    tr.feed(_text_chunk("lo"))
    tr.finish(None)
    kinds = [f["event"] for f in frames]
    assert kinds[0] == "message-start"
    assert kinds[-1] == "message-finish"
    deltas = [f for f in frames if f["event"] == "content-block-delta"]
    assert "".join(d["delta"]["text"] for d in deltas) == "hello"
    # golden shape of start frame
    assert frames[0] == {"event": "message-start", "role": "ai", "id": "m1"}


def test_tool_call_chunk_accumulation() -> None:
    frames: list[dict] = []
    tr = MessagesTranslator(frames.append)
    c1 = AIMessageChunk(content="", id="m2")
    c1.tool_call_chunks = [{"index": 0, "id": "call1", "name": "search", "args": '{"q":'}]  # type: ignore[attr-defined]
    c2 = AIMessageChunk(content="", id="m2")
    c2.tool_call_chunks = [{"index": 0, "id": "call1", "name": "search", "args": '"x"}'}]  # type: ignore[attr-defined]
    tr.feed(c1)
    tr.feed(c2)
    tr.finish(None)
    finish_blocks = [f for f in frames if f["event"] == "content-block-finish"]
    assert len(finish_blocks) == 1
    content = finish_blocks[0]["content"]
    assert content["type"] == "tool_call"
    assert content["args"] == {"q": "x"}


def test_values_event_data_verbatim() -> None:
    state = {"messages": [], "count": 3}
    assert values_event_data(state) == state


def test_interrupts_from_snapshot_dicts_and_objects() -> None:
    class FakeInterrupt:
        def __init__(self) -> None:
            self.id = "i1"
            self.value = {"q": 1}

    pairs = interrupts_from_snapshot(
        {"__interrupt__": [FakeInterrupt(), {"id": "i2", "value": "v"}]}
    )
    assert pairs == [("i1", {"q": 1}), ("i2", "v")]
    assert interrupts_from_snapshot({}) == []


def test_unknown_chunk_type_ignored() -> None:
    frames: list[Any] = []
    tr = MessagesTranslator(frames.append)

    class Weird:  # noqa: D106
        type = "nonsense"

    tr.feed(Weird())
    tr.finish(None)
    assert frames == []
