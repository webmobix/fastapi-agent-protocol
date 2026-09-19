"""Task 1.4: AgentRegistry register/resolve + unknown-id error path."""

import pytest
from conftest import echo_factory

from fastapi_agent_protocol.registry import AgentRegistry, UnknownAssistantError


async def test_register_and_resolve() -> None:
    registry = AgentRegistry({"support": echo_factory})
    graph = await registry.resolve("support", config=None)
    assert graph is not None
    assert registry.assistant_ids == ["support"]


async def test_unknown_id_raises() -> None:
    registry = AgentRegistry({})
    with pytest.raises(UnknownAssistantError):
        await registry.resolve("nope")


def test_late_register() -> None:
    registry = AgentRegistry()
    assert registry.assistant_ids == []
    registry.register("x", echo_factory)
    assert registry.assistant_ids == ["x"]


async def test_async_factory_supported() -> None:
    async def afactory(config: dict | None = None):  # type: ignore[no-untyped-def]
        return echo_factory(config)

    registry = AgentRegistry({"a": afactory})
    graph = await registry.resolve("a")
    assert graph is not None
