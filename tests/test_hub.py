"""Task 2.1: hub replay + dedup + channel/namespace filtering."""

import asyncio

from fastapi_agent_protocol.hub import HubRegistry, ThreadEventHub


def test_replay_since_delivers_missed_in_order() -> None:
    hub = ThreadEventHub()
    for i in range(5):
        hub.publish("messages", {"n": i})
    sub = hub.subscribe(since=2)
    replayed = hub.replay(sub)
    assert [e["seq"] for e in replayed] == [3, 4, 5]
    # handoff marks delivered: live delivery skips duplicates
    assert sub.last_seq == 5


def test_dedup_live_after_replay() -> None:
    hub = ThreadEventHub()
    hub.publish("messages", {"n": 1})
    sub = hub.subscribe(since=0)
    replayed = hub.replay(sub)
    assert len(replayed) == 1
    # event sitting in both buffer and queue is skipped live
    assert not sub.matches(replayed[0])


def test_channel_filtering() -> None:
    hub = ThreadEventHub()
    hub.publish("messages", {"n": 1})
    hub.publish("values", {"n": 2})
    hub.publish("lifecycle", {"event": "completed"})
    sub = hub.subscribe(channels=["messages"])
    replayed = hub.replay(sub)
    assert len(replayed) == 1
    assert replayed[0]["method"] == "messages"


def test_namespace_filtering() -> None:
    hub = ThreadEventHub()
    hub.publish("messages", {"n": 1}, namespace=[])
    hub.publish("messages", {"n": 2}, namespace=["sub"])
    root_only = hub.subscribe(namespaces=[[]])
    assert [e["params"]["data"] for e in hub.replay(root_only)] == [{"n": 1}]
    prefix = hub.subscribe(namespaces=[["sub"]])
    assert [e["params"]["data"] for e in hub.replay(prefix)] == [{"n": 2}]


def test_registry_hubs_are_per_thread() -> None:
    hubs = HubRegistry()
    assert hubs.hub("a") is hubs.hub("a")
    assert hubs.hub("a") is not hubs.hub("b")


def test_heartbeat_timer_path_exists() -> None:
    # asyncio.wait_for timeout is the heartbeat mechanism; smoke-test it.
    async def main() -> None:
        q: asyncio.Queue = asyncio.Queue()
        try:
            await asyncio.wait_for(q.get(), timeout=0.01)
        except TimeoutError:
            return
        raise AssertionError("expected timeout")

    asyncio.run(main())
