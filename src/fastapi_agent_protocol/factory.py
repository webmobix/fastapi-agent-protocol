"""Top-level factory: zero-config mount with escape hatches.

``create_protocol(agents={...}, thread_store=...)`` returns a
:class:`ProtocolFactory` holding a mountable FastAPI router plus the runtime
handles (hubs, registry, run manager). Wire the lifespan into the host app and
mount the router::

    protocol = create_protocol(agents={"support": support_factory})
    app = FastAPI(lifespan=protocol.lifespan)
    app.include_router(protocol.router)

Late registration works without a restart via ``protocol.register(id, factory)``.
Replay size and heartbeat are the factory escape hatches; CORS/auth stay with
the host app.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from fastapi_agent_protocol.hub import DEFAULT_REPLAY_SIZE, HubRegistry
from fastapi_agent_protocol.registry import AgentRegistry, GraphFactory
from fastapi_agent_protocol.router import DEFAULT_HEARTBEAT_S, build_router
from fastapi_agent_protocol.runner import RunManager
from fastapi_agent_protocol.threads import MemoryThreadStore, ThreadStore


class ProtocolFactory:
    """Mountable protocol runtime returned by :func:`create_protocol`."""

    def __init__(
        self,
        *,
        registry: AgentRegistry,
        thread_store: ThreadStore,
        hubs: HubRegistry,
        run_manager: RunManager,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_S,
    ) -> None:
        self.registry = registry
        self.thread_store = thread_store
        self.hubs = hubs
        self.run_manager = run_manager
        self.heartbeat_seconds = heartbeat_seconds
        self.router = build_router(
            thread_store,
            registry,
            hubs,
            run_manager,
            heartbeat_seconds=heartbeat_seconds,
        )

    def register(self, assistant_id: str, factory: GraphFactory) -> None:
        """Late agent registration (usable after mount, no restart)."""
        self.registry.register(assistant_id, factory)

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncIterator[None]:
        """Host-app lifespan: publishes runtime handles on ``app.state``."""
        app.state.thread_store = self.thread_store
        app.state.agent_registry = self.registry
        app.state.graph_registry = self.registry  # alias for tmp-parity readers
        app.state.hubs = self.hubs
        app.state.run_manager = self.run_manager
        app.state.heartbeat_seconds = self.heartbeat_seconds
        yield


def create_protocol(
    agents: dict[str, GraphFactory] | None = None,
    *,
    thread_store: ThreadStore | None = None,
    replay_size: int = DEFAULT_REPLAY_SIZE,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_S,
) -> ProtocolFactory:
    """Create a mountable protocol runtime with sensible defaults.

    :param agents: ``{assistant_id: factory}`` with
        ``factory(config) -> compiled graph`` (sync or async).
    :param thread_store: custom store; defaults to in-process ``memory()``.
    :param replay_size: per-thread SSE ring-buffer retention.
    :param heartbeat_seconds: SSE heartbeat interval.
    """
    store: ThreadStore = thread_store if thread_store is not None else MemoryThreadStore()
    registry = AgentRegistry(dict(agents or {}))
    hubs = HubRegistry(replay_size=replay_size)
    run_manager = RunManager(registry, hubs)
    return ProtocolFactory(
        registry=registry,
        thread_store=store,
        hubs=hubs,
        run_manager=run_manager,
        heartbeat_seconds=heartbeat_seconds,
    )


__all__ = ["ProtocolFactory", "create_protocol"]
