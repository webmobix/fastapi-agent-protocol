"""Task 2.3: run manager — parallel threads, busy rejection, race, lifecycle."""

import asyncio

import pytest
from conftest import echo_factory, make_interrupt_factory, slow_echo_factory
from langchain_core.messages import HumanMessage

from fastapi_agent_protocol.hub import HubRegistry
from fastapi_agent_protocol.registry import AgentRegistry
from fastapi_agent_protocol.runner import (
    RunActiveError,
    RunManager,
    UnknownInterruptError,
)


def _manager(factory_dict=None, **kw):  # type: ignore[no-untyped-def]
    registry = AgentRegistry(factory_dict or {"echo": echo_factory})
    hubs = HubRegistry()
    return RunManager(registry, hubs), hubs


async def _lifecycle_events(hubs: HubRegistry, thread_id: str) -> list[dict]:
    hub = hubs.hub(thread_id)
    sub = hub.subscribe(channels=["lifecycle"], since=0)
    return hub.replay(sub)


async def test_parallel_threads_stream_independently() -> None:
    manager, hubs = _manager()
    r1 = await manager.start("A", "echo", {"messages": [HumanMessage(content="a")]})
    r2 = await manager.start("B", "echo", {"messages": [HumanMessage(content="b")]})
    assert r1 != r2
    await manager.wait("A")
    await manager.wait("B")
    for tid in ("A", "B"):
        events = [e["params"]["data"]["event"] for e in await _lifecycle_events(hubs, tid)]
        assert events == ["started", "running", "completed"]


async def test_second_start_on_busy_thread_rejected() -> None:
    manager, _ = _manager({"slow": slow_echo_factory(0.5)})
    await manager.start("T", "slow", {"messages": [HumanMessage(content="x")]})
    with pytest.raises(RunActiveError):
        await manager.start("T", "slow", {"messages": [HumanMessage(content="y")]})
    await manager.wait("T")


async def test_concurrent_start_race_yields_exactly_one_success() -> None:
    manager, _ = _manager({"slow": slow_echo_factory(0.5)})
    results = await asyncio.gather(
        manager.start("T", "slow", {"messages": [HumanMessage(content="1")]}),
        manager.start("T", "slow", {"messages": [HumanMessage(content="2")]}),
        return_exceptions=True,
    )
    successes = [r for r in results if isinstance(r, str)]
    failures = [r for r in results if isinstance(r, RunActiveError)]
    assert len(successes) == 1
    assert len(failures) == 1
    await manager.wait("T")


async def test_exactly_one_terminal_event_per_run() -> None:
    manager, hubs = _manager()
    await manager.start("T", "echo", {"messages": [HumanMessage(content="hi")]})
    await manager.wait("T")
    events = [e["params"]["data"]["event"] for e in await _lifecycle_events(hubs, "T")]
    terminals = [e for e in events if e in ("completed", "failed", "interrupted")]
    assert len(terminals) == 1


async def test_cancel_reports_interrupted() -> None:
    manager, hubs = _manager({"slow": slow_echo_factory(5.0)})
    await manager.start("T", "slow", {"messages": [HumanMessage(content="x")]})
    await asyncio.sleep(0.05)
    assert manager.cancel("T") is True
    await manager.wait("T")
    events = [e["params"]["data"]["event"] for e in await _lifecycle_events(hubs, "T")]
    assert events[-1] == "interrupted"


async def test_interrupt_and_resume() -> None:
    manager, hubs = _manager({"ask": make_interrupt_factory()})
    await manager.start("T", "ask", {"messages": [HumanMessage(content="go")]})
    await manager.wait("T")
    pending = manager.pending_interrupts("T")
    interrupt_ids = [k for k in pending if k != "assistant_id"]
    assert len(interrupt_ids) == 1
    hub = hubs.hub("T")
    sub = hub.subscribe(channels=["input"], since=0)
    requested = hub.replay(sub)
    assert requested and requested[0]["method"] == "input.requested"

    run_id = await manager.respond("T", interrupt_ids[0], "yes")
    assert run_id
    await manager.wait("T")
    events = [e["params"]["data"]["event"] for e in await _lifecycle_events(hubs, "T")]
    assert events[-1] == "completed"


async def test_respond_unknown_interrupt_rejected_and_pending_intact() -> None:
    manager, _ = _manager({"ask": make_interrupt_factory()})
    await manager.start("T", "ask", {"messages": [HumanMessage(content="go")]})
    await manager.wait("T")
    before = manager.pending_interrupts("T")
    with pytest.raises(UnknownInterruptError):
        await manager.respond("T", "bogus", "x", assistant_id="ask")
    assert manager.pending_interrupts("T") == before


async def test_resume_works_after_pending_map_loss() -> None:
    """Restart-safety: with the in-memory map wiped (simulated restart), a
    caller-supplied assistant id must still resume the checkpointed interrupt."""
    manager, hubs = _manager({"ask": make_interrupt_factory()})
    await manager.start("T", "ask", {"messages": [HumanMessage(content="go")]})
    await manager.wait("T")
    interrupt_ids = [k for k in manager.pending_interrupts("T") if k != "assistant_id"]
    assert interrupt_ids
    manager._pending_inputs.pop("T")  # simulate a process restart
    run_id = await manager.respond("T", interrupt_ids[0], "yes", assistant_id="ask")
    assert run_id
    await manager.wait("T")
    events = [e["params"]["data"]["event"] for e in await _lifecycle_events(hubs, "T")]
    assert events[-1] == "completed"


async def test_wait_propagates_caller_cancellation() -> None:
    manager, _ = _manager({"slow": slow_echo_factory(5.0)})
    await manager.start("T", "slow", {"messages": [HumanMessage(content="x")]})
    waiter = asyncio.create_task(manager.wait("T"))
    await asyncio.sleep(0.05)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    # run keeps going; cancel it to settle
    assert manager.cancel("T") is True
    await manager.wait("T")


async def test_concurrent_respond_yields_one_success_one_active_error() -> None:
    manager, _ = _manager({"ask": make_interrupt_factory()})
    await manager.start("T", "ask", {"messages": [HumanMessage(content="go")]})
    await manager.wait("T")
    interrupt_ids = [k for k in manager.pending_interrupts("T") if k != "assistant_id"]
    results = await asyncio.gather(
        manager.respond("T", interrupt_ids[0], "a", assistant_id="ask"),
        manager.respond("T", interrupt_ids[0], "b", assistant_id="ask"),
        return_exceptions=True,
    )
    successes = [r for r in results if isinstance(r, str)]
    failures = [r for r in results if isinstance(r, RunActiveError)]
    assert len(successes) == 1
    assert len(failures) == 1
    await manager.wait("T")


async def test_failed_start_preserves_pending_interrupts() -> None:
    manager, _ = _manager({"ask": make_interrupt_factory()})
    await manager.start("T", "ask", {"messages": [HumanMessage(content="go")]})
    await manager.wait("T")
    before = manager.pending_interrupts("T")
    assert [k for k in before if k != "assistant_id"]
    # unknown assistant start must fail without clearing pending
    from fastapi_agent_protocol.registry import UnknownAssistantError

    with pytest.raises(UnknownAssistantError):
        await manager.start("T", "missing", {"messages": []})
    assert manager.pending_interrupts("T") == before
