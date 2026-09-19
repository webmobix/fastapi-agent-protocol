"""Shared fixture graphs for protocol tests (user-owned, lib never builds these)."""

import asyncio
from typing import Any

import pytest
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.types import interrupt


def echo_factory(config: dict[str, Any] | None = None) -> Any:
    """Minimal compiled graph: echoes last human message; per-test saver."""

    def node(state: MessagesState) -> dict[str, Any]:
        last = state["messages"][-1].content if state["messages"] else ""
        return {"messages": [HumanMessage(content=f"echo:{last}")]}

    g = StateGraph(MessagesState)
    g.add_node("n", node)
    g.add_edge(START, "n")
    g.add_edge("n", END)
    return g.compile(checkpointer=MemorySaver())


def slow_echo_factory(delay: float = 0.3) -> Any:
    def factory(config: dict[str, Any] | None = None) -> Any:
        async def node(state: MessagesState) -> dict[str, Any]:
            await asyncio.sleep(delay)
            last = state["messages"][-1].content if state["messages"] else ""
            return {"messages": [HumanMessage(content=f"slow:{last}")]}

        g = StateGraph(MessagesState)
        g.add_node("n", node)
        g.add_edge(START, "n")
        g.add_edge("n", END)
        return g.compile(checkpointer=MemorySaver())

    return factory


def interrupt_factory(config: dict[str, Any] | None = None) -> Any:
    """Graph that interrupts mid-run (ask-human pattern)."""

    def node(state: MessagesState) -> dict[str, Any]:
        answer = interrupt({"question": "proceed?"})
        return {"messages": [HumanMessage(content=f"got:{answer}")]}

    g = StateGraph(MessagesState)
    g.add_node("n", node)
    g.add_edge(START, "n")
    g.add_edge("n", END)
    return g.compile(checkpointer=MemorySaver())


def make_echo_factory() -> Any:
    """Echo graph factory closing over ONE shared saver (multi-resolve tests)."""
    saver = MemorySaver()

    def factory(config: dict[str, Any] | None = None) -> Any:
        def node(state: MessagesState) -> dict[str, Any]:
            last = state["messages"][-1].content if state["messages"] else ""
            return {"messages": [HumanMessage(content=f"echo:{last}")]}

        g = StateGraph(MessagesState)
        g.add_node("n", node)
        g.add_edge(START, "n")
        g.add_edge("n", END)
        return g.compile(checkpointer=saver)

    return factory


def make_chat_factory() -> Any:
    """Model-backed factory: exercises the messages translation path end to end.

    The node calls a (fake) chat model so ``messages`` stream mode emits real
    ``AIMessageChunk`` tuples; plain state-returning nodes emit none.
    """
    import itertools

    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

    saver = MemorySaver()

    def factory(config: dict[str, Any] | None = None) -> Any:
        model = GenericFakeChatModel(messages=itertools.repeat("chat-stream"))

        async def node(state: MessagesState) -> dict[str, Any]:
            resp = await model.ainvoke(list(state["messages"]))
            return {"messages": [resp]}

        g = StateGraph(MessagesState)
        g.add_node("n", node)
        g.add_edge(START, "n")
        g.add_edge("n", END)
        return g.compile(checkpointer=saver)

    return factory


def make_interrupt_factory() -> Any:
    """Interrupt graph factory closing over ONE shared saver.

    Resume only works when every ``resolve`` returns a graph over the same
    checkpointer — exactly how real hosts wire a persistent saver. Plain
    :func:`interrupt_factory` builds a fresh saver per call (fine for
    single-run tests, wrong for resume tests).
    """
    saver = MemorySaver()

    def factory(config: dict[str, Any] | None = None) -> Any:
        def node(state: MessagesState) -> dict[str, Any]:
            answer = interrupt({"question": "proceed?"})
            return {"messages": [HumanMessage(content=f"got:{answer}")]}

        g = StateGraph(MessagesState)
        g.add_node("n", node)
        g.add_edge(START, "n")
        g.add_edge("n", END)
        return g.compile(checkpointer=saver)

    return factory


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
