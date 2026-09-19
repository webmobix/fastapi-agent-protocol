"""Minimal host app: mount the protocol runtime with zero config.

Run with::

    uv run uvicorn examples.minimal:app --port 8123

Then (in another terminal)::

    curl -s -X POST localhost:8123/threads -H 'content-type: application/json' \
      -d '{"metadata": {}}'
"""

from typing import Any

from fastapi import FastAPI
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, MessagesState, StateGraph

from fastapi_agent_protocol import create_protocol

_checkpointer = MemorySaver()  # process-lifetime saver; use a file-backed
# saver (e.g. AsyncSqliteSaver) in production so state survives restarts


def support_factory(config: dict[str, Any] | None = None) -> Any:
    """User-owned graph: the library never builds this for you.

    Close over your own checkpointer here (file-backed in production) so
    state/history/interrupt resume survive restarts. The saver MUST be shared
    across factory calls — a fresh saver per call would strand checkpoints
    where state reads can never find them.
    """

    def node(state: MessagesState) -> dict[str, Any]:
        last = state["messages"][-1].content if state["messages"] else ""
        return {"messages": [HumanMessage(content=f"supported:{last}")]}

    graph = StateGraph(MessagesState)
    graph.add_node("support", node)
    graph.add_edge(START, "support")
    graph.add_edge("support", END)
    return graph.compile(checkpointer=_checkpointer)


protocol = create_protocol(agents={"support": support_factory})

app = FastAPI(title="minimal protocol host", lifespan=protocol.lifespan)
app.include_router(protocol.router)
