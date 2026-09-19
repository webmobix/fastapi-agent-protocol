"""Task 2.4: state serialization + delta-aware history reconstruction."""

from typing import Any

from conftest import echo_factory
from langchain_core.messages import HumanMessage

from fastapi_agent_protocol.state import (
    empty_state,
    read_thread_history,
    read_thread_state,
    serialize_message,
    serialize_values,
)


def test_serialize_values_messages_wire_shape() -> None:
    out = serialize_values({"messages": [HumanMessage(content="hi")], "n": 1})
    assert isinstance(out["messages"], list)
    assert out["messages"][0]["content"] == "hi"
    assert out["n"] == 1
    assert serialize_values(None) == {}
    assert serialize_values({}) == {}


def test_serialize_message_fallback() -> None:
    assert serialize_message(object())["type"] == "unknown"


def test_empty_state_for_untouched_thread() -> None:
    st = empty_state("t1")
    assert st["values"] == {}
    assert st["checkpoint"]["thread_id"] == "t1"


async def test_read_state_after_run() -> None:
    graph = echo_factory(None)
    cfg = {"configurable": {"thread_id": "t-state", "checkpoint_ns": ""}}
    await graph.ainvoke({"messages": [HumanMessage(content="yo")]}, config=cfg)
    snapshot = await read_thread_state(graph, cfg)
    assert snapshot["values"]["messages"]
    assert snapshot["checkpoint"]["thread_id"] == "t-state"


async def test_history_delta_aware_reconstruction() -> None:
    """Fake a DeepAgents-style delta graph: channel values live in pending writes."""

    class FakeTuple:
        def __init__(self, cid: str, writes: list, metadata: Any = None) -> None:
            self.config = {"configurable": {"checkpoint_id": cid}}
            self.pending_writes = writes
            self.metadata: Any = metadata or {}

    class FakeCheckpointer:
        def __init__(self, tuples: list) -> None:  # type: ignore[no-untyped-def]
            self._tuples = tuples

        async def alist(self, config: Any, before: Any = None, limit: int = 10):  # type: ignore[no-untyped-def]
            items = self._tuples[:limit]
            for t in items:
                yield t

    class FakeGraph:
        def __init__(self) -> None:
            self.checkpointer = FakeCheckpointer(
                [
                    FakeTuple("c2", [("t1", "todos", ["b"]), ("t1", "__interrupt__", [])]),
                    FakeTuple("c1", [("t0", "todos", ["a"])]),
                ]
            )

    history = await read_thread_history(
        FakeGraph(), {"configurable": {"thread_id": "t", "checkpoint_ns": ""}}
    )
    assert len(history) == 2
    # newest first; cumulative: c1 has [a], c2 has [a, b]
    assert history[0]["checkpoint"] == {"checkpoint_id": "c2"}
    assert history[0]["values"]["todos"] == ["a", "b"]
    assert history[1]["values"]["todos"] == ["a"]


async def test_history_requires_checkpointer() -> None:
    import pytest

    class NoSaver:
        checkpointer = None

    with pytest.raises(ValueError, match="No checkpointer"):
        await read_thread_history(
            NoSaver(), {"configurable": {"thread_id": "t", "checkpoint_ns": ""}}
        )
