"""Reusable FastAPI runtime exposing user-owned LangGraph graphs over the
Agent Protocol streaming surface.

Mount in any FastAPI app::

    from fastapi import FastAPI
    from fastapi_agent_protocol import create_protocol

    protocol = create_protocol(agents={"support": support_factory})
    app = FastAPI(lifespan=protocol.lifespan)
    app.include_router(protocol.router)

The library never builds graphs, never owns checkpointers, and never imports
app-specific agent code — hosts register factories of shape
``factory(config) -> compiled graph``. Thread storage is pluggable
(``memory`` default, ``sqlite`` built-in).
"""

from fastapi_agent_protocol.factory import ProtocolFactory, create_protocol
from fastapi_agent_protocol.hub import HubRegistry, ThreadEventHub
from fastapi_agent_protocol.registry import AgentRegistry, UnknownAssistantError
from fastapi_agent_protocol.runner import (
    RunActiveError,
    RunError,
    RunManager,
    UnknownInterruptError,
)
from fastapi_agent_protocol.state import (
    empty_state,
    read_thread_history,
    read_thread_state,
    serialize_message,
    serialize_values,
)
from fastapi_agent_protocol.threads import (
    MemoryThreadStore,
    SqliteThreadStore,
    ThreadRecord,
    ThreadStore,
    memory,
    sqlite,
)
from fastapi_agent_protocol.translation import (
    MessagesTranslator,
    interrupts_from_snapshot,
    values_event_data,
)
from fastapi_agent_protocol.uuid7 import utcnow, uuid7

__all__ = [
    "AgentRegistry",
    "HubRegistry",
    "MemoryThreadStore",
    "MessagesTranslator",
    "ProtocolFactory",
    "RunActiveError",
    "RunError",
    "RunManager",
    "SqliteThreadStore",
    "ThreadEventHub",
    "ThreadRecord",
    "ThreadStore",
    "UnknownAssistantError",
    "UnknownInterruptError",
    "create_protocol",
    "empty_state",
    "interrupts_from_snapshot",
    "memory",
    "read_thread_history",
    "read_thread_state",
    "serialize_message",
    "serialize_values",
    "sqlite",
    "utcnow",
    "uuid7",
    "values_event_data",
]
