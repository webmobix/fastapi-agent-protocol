"""Tasks 1.5 / 3.1 / 3.2 / 3.3: factory mount + HTTP surface + compat.

Finite endpoints use ``TestClient``. Infinite SSE streams go over a live
uvicorn socket (``tests/live_server.py``) because this repo's ``TestClient``
and httpx's ``ASGITransport`` buffer the full response body.
"""

import time

import httpx
import pytest
from conftest import echo_factory, make_echo_factory, make_interrupt_factory, slow_echo_factory
from fastapi import FastAPI
from fastapi.testclient import TestClient
from langchain_core.messages import HumanMessage
from live_server import LiveServer, read_sse, strip_heartbeats

from fastapi_agent_protocol import create_protocol, memory


def _app(**kw):  # type: ignore[no-untyped-def]
    kw.setdefault("heartbeat_seconds", 0.1)
    protocol = create_protocol(
        agents={"echo": make_echo_factory(), "ask": make_interrupt_factory()},
        thread_store=memory(),
        **kw,
    )
    app = FastAPI(lifespan=protocol.lifespan)
    app.include_router(protocol.router)
    return app, protocol


@pytest.fixture
def client():  # type: ignore[no-untyped-def]
    app, _ = _app()
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="module")
def live():  # type: ignore[no-untyped-def]
    with LiveServer() as server:
        yield server


def test_zero_config_mount_streams(live: LiveServer) -> None:
    with httpx.Client(base_url=live.url, timeout=30.0) as ac:
        tid = ac.post("/threads", json={"metadata": {}}).json()["thread_id"]
        r = ac.post(
            f"/threads/{tid}/commands",
            json={
                "id": 1,
                "method": "run.start",
                "params": {
                    "assistant_id": "echo",
                    "input": {"messages": [{"type": "human", "content": "hi"}]},
                },
            },
        ).json()
        assert r["type"] == "success"
        # stream canonical path, collect lifecycle frames until terminal
        body = read_sse(ac, f"/threads/{tid}/stream", {"channels": ["lifecycle"]})
        assert "lifecycle" in body


def test_late_registration_without_restart() -> None:
    app, protocol = _app()
    protocol.register("late", echo_factory)
    with TestClient(app) as client:
        tid = client.post("/threads", json={}).json()["thread_id"]
        r = client.post(
            f"/threads/{tid}/commands",
            json={"id": 1, "method": "run.start", "params": {"assistant_id": "late", "input": {}}},
        ).json()
        assert r["type"] == "success"


def test_command_envelopes_success_and_errors(client: TestClient) -> None:  # type: ignore[no-untyped-def]
    tid = client.post("/threads", json={"metadata": {"k": "v"}}).json()["thread_id"]
    # unknown method
    r = client.post(
        f"/threads/{tid}/commands", json={"id": 2, "method": "nope", "params": {}}
    ).json()
    assert r == {
        "type": "error",
        "id": 2,
        "error": "unknown_command",
        "message": "unsupported method: nope",
    }
    # unknown assistant
    r = client.post(
        f"/threads/{tid}/commands",
        json={"id": 3, "method": "run.start", "params": {"assistant_id": "missing", "input": {}}},
    ).json()
    assert r["type"] == "error" and r["error"] == "unknown_command"
    # unknown thread -> 404
    resp = client.post(
        "/threads/nope/commands", json={"id": 1, "method": "state.get", "params": {}}
    )
    assert resp.status_code == 404


def test_run_start_stamps_session_metadata_and_binds_assistant(client: TestClient) -> None:  # type: ignore[no-untyped-def]
    seen: dict = {}

    def spy_factory(config):  # type: ignore[no-untyped-def]
        seen.update(config.get("configurable", {}))
        return echo_factory(config)

    from fastapi import FastAPI as FA
    from fastapi.testclient import TestClient as TC

    protocol = create_protocol(agents={"spy": spy_factory}, thread_store=memory())
    app = FA(lifespan=protocol.lifespan)
    app.include_router(protocol.router)
    with TC(app) as c:
        tid = c.post("/threads", json={"metadata": {"proj": "p"}}).json()["thread_id"]
        c.post(
            f"/threads/{tid}/commands",
            json={"id": 1, "method": "run.start", "params": {"assistant_id": "spy", "input": {}}},
        )
        assert seen.get("session_metadata") == {"proj": "p"}
        assert protocol.thread_store.get(tid).assistant_id == "spy"  # type: ignore[union-attr]


def test_stream_alias_identical_replay(live: LiveServer) -> None:
    with httpx.Client(base_url=live.url, timeout=30.0) as ac:
        tid = ac.post("/threads", json={}).json()["thread_id"]
        ac.post(
            f"/threads/{tid}/commands",
            json={
                "id": 1,
                "method": "run.start",
                "params": {
                    "assistant_id": "echo",
                    "input": {"messages": [{"type": "human", "content": "x"}]},
                },
            },
        )
        time.sleep(0.5)  # let the run publish
        bodies = [
            strip_heartbeats(read_sse(ac, path, {"since": 0}))
            for path in (f"/threads/{tid}/stream", f"/threads/{tid}/stream/events")
        ]
        assert bodies[0] == bodies[1]
        assert "event: lifecycle" in bodies[0]


def test_info_stub_no_404(client: TestClient) -> None:  # type: ignore[no-untyped-def]
    r = client.get("/info")
    assert r.status_code == 200
    assert "agents" in r.json()


def test_state_history_404_and_cursor_normalization(client: TestClient) -> None:  # type: ignore[no-untyped-def]
    assert client.get("/threads/nope/state").status_code == 404
    assert client.get("/threads/nope/history").status_code == 404
    assert client.post("/threads/nope/history", json={}).status_code == 404
    tid = client.post("/threads", json={}).json()["thread_id"]
    # untouched thread: empty state, empty history
    assert client.get(f"/threads/{tid}/state").json()["values"] == {}
    assert client.get(f"/threads/{tid}/history").json() == []
    # limit validation
    assert client.get(f"/threads/{tid}/history", params={"limit": 0}).status_code == 422
    # run then paginate; POST accepts full-config cursor
    client.post(
        f"/threads/{tid}/commands",
        json={
            "id": 1,
            "method": "run.start",
            "params": {
                "assistant_id": "echo",
                "input": {"messages": [{"type": "human", "content": "a"}]},
            },
        },
    )
    import time

    time.sleep(0.5)
    hist = client.get(f"/threads/{tid}/history", params={"limit": 5}).json()
    assert isinstance(hist, list) and len(hist) >= 1
    # SDK contract: messages live inside values (state.values.messages), same
    # shape as the GET state form — not hoisted to a top-level key.
    assert all("messages" not in h for h in hist)
    assert all(h["values"].get("messages") is not None or h["values"] == {} for h in hist)
    first_ckpt = hist[0]["checkpoint"]["checkpoint_id"]
    post_hist = client.post(
        f"/threads/{tid}/history",
        json={"limit": 5, "before": {"configurable": {"checkpoint_id": first_ckpt}}},
    ).json()
    assert isinstance(post_hist, list)


def test_state_get_unknown_assistant_is_error_envelope_not_500() -> None:
    app, protocol = _app()
    with TestClient(app) as c:
        tid = c.post("/threads", json={}).json()["thread_id"]
        # Simulate a deploy where the persisted assistant no longer exists.
        protocol.thread_store.set_assistant(tid, "vanished")
        r = c.post(
            f"/threads/{tid}/commands",
            json={"id": 7, "method": "state.get", "params": {}},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["type"] == "error"
        assert body["error"] == "unknown_command"
        assert "not registered" in body["message"]


def test_run_cancel_command_scopes_run_id() -> None:
    app, _ = _app()
    with TestClient(app) as c:
        tid = c.post("/threads", json={}).json()["thread_id"]
        # stale run_id must not kill a newer run
        assert (
            c.post(
                f"/threads/{tid}/commands",
                json={"id": 1, "method": "run.cancel", "params": {"run_id": "stale-run-id"}},
            ).json()["type"]
            == "success"
        )


def test_rest_run_cancel_and_interrupt_resume(client: TestClient) -> None:  # type: ignore[no-untyped-def]
    from fastapi import FastAPI as FA
    from fastapi.testclient import TestClient as TC

    protocol = create_protocol(
        agents={"ask": make_interrupt_factory(), "slow": slow_echo_factory(5.0)},
        thread_store=memory(),
    )
    app = FA(lifespan=protocol.lifespan)
    app.include_router(protocol.router)
    with TC(app) as c:
        # interrupt/resume via commands
        tid = c.post("/threads", json={}).json()["thread_id"]
        c.post(
            f"/threads/{tid}/commands",
            json={
                "id": 1,
                "method": "run.start",
                "params": {
                    "assistant_id": "ask",
                    "input": {"messages": [{"type": "human", "content": "go"}]},
                },
            },
        )
        import time

        deadline = time.time() + 10
        iid = None
        while time.time() < deadline:
            snap = c.get(f"/threads/{tid}/state").json()
            if snap.get("interrupts"):
                iid = snap["interrupts"][0]["id"]
                break
            time.sleep(0.1)
        assert iid, "expected a pending interrupt"
        r = c.post(
            f"/threads/{tid}/commands",
            json={
                "id": 2,
                "method": "input.respond",
                "params": {"interrupt_id": iid, "response": "yes"},
            },
        ).json()
        assert r["type"] == "success"
        # unknown interrupt rejected
        r = c.post(
            f"/threads/{tid}/commands",
            json={
                "id": 3,
                "method": "input.respond",
                "params": {"interrupt_id": "bogus", "response": 1},
            },
        ).json()
        assert r["type"] == "error" and r["error"] == "no_such_interrupt"
        # REST cancel of a slow run
        tid2 = c.post("/threads", json={}).json()["thread_id"]
        run_id = c.post(
            f"/threads/{tid2}/commands",
            json={
                "id": 1,
                "method": "run.start",
                "params": {
                    "assistant_id": "slow",
                    "input": {"messages": [{"type": "human", "content": "z"}]},
                },
            },
        ).json()["result"]["run_id"]
        time.sleep(0.2)
        resp = c.post(f"/threads/{tid2}/runs/{run_id}/cancel").json()
        assert resp["run_id"] == run_id


def test_input_messages_round_trip_via_human_dict(live: LiveServer) -> None:
    """Human-message dict input streams an echo containing the text."""
    with httpx.Client(base_url=live.url, timeout=30.0) as ac:
        tid = ac.post("/threads", json={}).json()["thread_id"]
        ac.post(
            f"/threads/{tid}/commands",
            json={
                "id": 1,
                "method": "run.start",
                "params": {
                    "assistant_id": "echo",
                    "input": {"messages": [HumanMessage(content="hello").model_dump()]},
                },
            },
        )
        body = read_sse(
            ac,
            f"/threads/{tid}/stream",
            {"channels": ["values"]},
            want=(b"echo:hello",),
        )
        assert "echo:hello" in body or "hello" in body


def test_thread_create_echoes_metadata_and_unknown_stream_404(client: TestClient) -> None:  # type: ignore[no-untyped-def]
    r = client.post("/threads", json={"metadata": {"a": 1}}).json()
    assert r["metadata"] == {"a": 1}
    with client.stream("POST", "/threads/nope/stream", json={}) as resp:
        assert resp.status_code == 404
