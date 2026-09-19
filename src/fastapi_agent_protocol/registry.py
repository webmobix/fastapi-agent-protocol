"""Agent registration without graph ownership.

Host apps register named agents via factories of shape
``factory(config) -> compiled graph``; the library resolves the graph per run
without constructing checkpointers or importing agent code. Works for
``StateGraph`` and DeepAgents alike — the checkpointer is a
graph-construction concern closed over in user code.
"""

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

GraphFactory = Callable[[dict[str, Any] | None], Any | Awaitable[Any]]


class UnknownAssistantError(LookupError):
    """No graph is registered under this assistant id."""

    def __init__(self, assistant_id: str, known: list[str]) -> None:
        super().__init__(f"unknown assistant_id {assistant_id!r} (registered: {', '.join(known)})")
        self.assistant_id = assistant_id


class AgentRegistry:
    """Named agent factories; graphs resolved lazily per run."""

    def __init__(self, agents: dict[str, GraphFactory] | None = None) -> None:
        self._factories: dict[str, GraphFactory] = dict(agents or {})

    @property
    def assistant_ids(self) -> list[str]:
        return sorted(self._factories)

    def register(self, assistant_id: str, factory: GraphFactory) -> None:
        """Add or override a graph factory (usable after mount, no restart)."""
        self._factories[assistant_id] = factory

    async def resolve(self, assistant_id: str, config: dict[str, Any] | None = None) -> Any:
        """Build the compiled graph for this run's config."""
        factory = self._factories.get(assistant_id)
        if factory is None:
            raise UnknownAssistantError(assistant_id, self.assistant_ids)
        built = factory(config)
        if inspect.isawaitable(built):
            built = await built
        return built


__all__ = ["AgentRegistry", "GraphFactory", "UnknownAssistantError"]
