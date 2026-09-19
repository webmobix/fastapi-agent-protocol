"""Agent Protocol HTTP endpoints (streaming + threads + commands).

Served at SDK-default paths without an ``/api`` prefix:

- ``POST /threads`` — provision a thread
- ``GET /threads/{thread_id}/state`` — checkpoint-backed state hydration
- ``GET /threads/{thread_id}/history`` — past checkpointed states (newest first)
- ``POST /threads/{thread_id}/history`` — SDK getHistory form (JSON body)
- ``POST /threads/{thread_id}/commands`` — run.start / input.respond /
  run.cancel / state.get with ``{id, method, params}`` envelopes
- ``POST /threads/{thread_id}/runs/{run_id}/cancel`` — REST cancel
- ``POST /threads/{thread_id}/stream`` — canonical SSE subscription w/ replay
- ``POST /threads/{thread_id}/stream/events`` — deprecated alias, same handler
- ``GET /info`` — minimal descriptor so ``langgraph-sdk-js`` init won't 404

``run.cancel`` is a documented extension absent from upstream protocol CDDL.
"""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from fastapi_agent_protocol.hub import HubRegistry
from fastapi_agent_protocol.registry import AgentRegistry, UnknownAssistantError
from fastapi_agent_protocol.runner import (
    RunActiveError,
    RunManager,
    UnknownInterruptError,
)
from fastapi_agent_protocol.state import (
    empty_state,
    read_thread_history,
    read_thread_state,
)
from fastapi_agent_protocol.threads import ThreadStore

logger = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_S = 15.0


def error_envelope(cmd_id: int | None, code: str, message: str) -> JSONResponse:
    """JSON-RPC-style error envelope; rides inside HTTP 200 per the protocol."""
    return JSONResponse(
        status_code=200,
        content={"type": "error", "id": cmd_id, "error": code, "message": message},
    )


def success_envelope(cmd_id: int | None, result: dict[str, Any]) -> JSONResponse:
    return JSONResponse(
        content={"type": "success", "id": cmd_id, "result": result},
    )


def sse_block(envelope: dict[str, Any]) -> str:
    data = json.dumps(envelope, separators=(",", ":"))
    return f"event: {envelope['method']}\ndata: {data}\nid: {envelope['seq']}\n\n"


def _thread_config(thread_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


class _Deps:
    """Per-router runtime handles (closed over by endpoint handlers)."""

    def __init__(
        self,
        thread_store: ThreadStore,
        registry: AgentRegistry,
        hubs: HubRegistry,
        run_manager: RunManager,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_S,
    ) -> None:
        self.thread_store = thread_store
        self.registry = registry
        self.hubs = hubs
        self.run_manager = run_manager
        self.heartbeat_seconds = heartbeat_seconds


def build_router(
    thread_store: ThreadStore,
    registry: AgentRegistry,
    hubs: HubRegistry,
    run_manager: RunManager,
    *,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_S,
) -> APIRouter:
    deps = _Deps(thread_store, registry, hubs, run_manager, heartbeat_seconds)
    router = APIRouter()

    @router.post("/threads")
    async def create_thread(request: Request) -> dict[str, Any]:
        """Provision a thread row and return it."""
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        body = payload if isinstance(payload, dict) else {}
        raw_metadata = body.get("metadata")
        metadata: dict[str, Any] = dict(raw_metadata) if isinstance(raw_metadata, dict) else {}
        record = deps.thread_store.create(metadata)
        return {
            "thread_id": record.thread_id,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "state_updated_at": None,
            "metadata": record.metadata,
            "status": "idle",
            "config": {},
            "values": None,
        }

    async def _state_graph(thread_id: str) -> Any | None:
        """Graph able to read this thread's checkpointed channels.

        Checkpoint channels only decode through a graph built with the same
        state schema, so prefer the assistant that last ran here (persisted
        on the thread row so it survives restarts). Untouched threads have no
        checkpoints — return None and serve empty state.
        """
        record = deps.thread_store.get(thread_id)
        assistant = record.assistant_id if record is not None else None
        if assistant is None:
            return None
        return await deps.registry.resolve(assistant)

    @router.get("/threads/{thread_id}/state")
    async def get_state(thread_id: str) -> Any:
        if deps.thread_store.get(thread_id) is None:
            return JSONResponse(status_code=404, content={"detail": "thread not found"})
        try:
            graph = await _state_graph(thread_id)
        except UnknownAssistantError:
            return JSONResponse(status_code=404, content={"detail": "thread not found"})
        if graph is None:
            return empty_state(thread_id)
        return await read_thread_state(graph, _thread_config(thread_id))

    async def _history(thread_id: str, limit: int, before: Any | None) -> Any:
        if deps.thread_store.get(thread_id) is None:
            return JSONResponse(status_code=404, content={"detail": "thread not found"})
        if limit < 1:
            return JSONResponse(status_code=422, content={"detail": "limit must be >= 1"})
        if isinstance(before, dict):
            # SDK may pass a full Config; extract its checkpoint id.
            before = (before.get("configurable") or {}).get("checkpoint_id")
        if before is not None and not isinstance(before, str):
            before = None
        try:
            graph = await _state_graph(thread_id)
        except UnknownAssistantError:
            return JSONResponse(status_code=404, content={"detail": "thread not found"})
        if graph is None:
            return []
        return await read_thread_history(
            graph, _thread_config(thread_id), limit=limit, before=before
        )

    @router.get("/threads/{thread_id}/history")
    async def get_history(
        thread_id: str,
        limit: int = 10,
        before: str | None = None,
    ) -> Any:
        """Past checkpointed states for the thread, newest first."""
        return await _history(thread_id, limit, before)

    @router.post("/threads/{thread_id}/history")
    async def search_history(request: Request, thread_id: str) -> Any:
        """POST variant of history (SDK ``threads.getHistory`` shape)."""
        body: dict[str, Any] = {}
        try:
            payload = await request.json()
            if isinstance(payload, dict):
                body = payload
        except Exception:
            body = {}
        raw_limit = body.get("limit")
        limit: int = raw_limit if isinstance(raw_limit, int) else 10
        return await _history(thread_id, limit, body.get("before"))

    @router.post("/threads/{thread_id}/commands")
    async def dispatch_command(request: Request, thread_id: str) -> Any:
        record = deps.thread_store.get(thread_id)
        if record is None:
            return JSONResponse(status_code=404, content={"detail": "thread not found"})

        try:
            body = await request.json()
        except Exception:
            return error_envelope(None, "invalid_argument", "invalid JSON body")
        if not isinstance(body, dict):
            return error_envelope(None, "invalid_argument", "invalid command body")
        cmd_id = body.get("id")
        method = body.get("method")
        params = body.get("params") or {}
        manager = deps.run_manager

        if method == "run.start":
            assistant_id = params.get("assistant_id")
            if not assistant_id:
                return error_envelope(cmd_id, "invalid_argument", "missing assistant_id")
            # stamp stored metadata into configurable.session_metadata
            config = dict(params.get("config") or {})
            configurable = dict(config.get("configurable") or {})
            configurable["session_metadata"] = dict(record.metadata or {})
            config["configurable"] = configurable
            try:
                run_id = await manager.start(
                    thread_id,
                    assistant_id,
                    params.get("input"),
                    config=config,
                    metadata=params.get("metadata"),
                )
            except RunActiveError as exc:
                return error_envelope(cmd_id, exc.code, exc.message)
            except UnknownAssistantError as exc:
                return error_envelope(cmd_id, "unknown_command", str(exc))
            deps.thread_store.set_assistant(thread_id, assistant_id)
            return success_envelope(cmd_id, {"run_id": run_id})

        if method == "input.respond":
            interrupt_id = str(params.get("interrupt_id") or "")
            if not interrupt_id:
                return error_envelope(cmd_id, "invalid_argument", "missing interrupt_id")
            # Validate against the checkpointer so a pending interrupt
            # survives a restart (the in-memory run manager is not durable).
            graph = await _state_graph(thread_id)
            if graph is None:
                return error_envelope(
                    cmd_id, "no_such_interrupt", f"no pending interrupt {interrupt_id}"
                )
            snapshot = await read_thread_state(graph, _thread_config(thread_id))
            pending_ids = {i["id"] for i in snapshot["interrupts"]}
            if interrupt_id not in pending_ids:
                return error_envelope(
                    cmd_id, "no_such_interrupt", f"no pending interrupt {interrupt_id}"
                )
            stored = deps.thread_store.get(thread_id)
            try:
                run_id = await manager.respond(
                    thread_id,
                    interrupt_id,
                    params.get("response"),
                    assistant_id=(stored.assistant_id if stored else None),
                )
            except UnknownInterruptError as exc:
                return error_envelope(cmd_id, exc.code, exc.message)
            except UnknownAssistantError as exc:
                return error_envelope(cmd_id, "unknown_command", str(exc))
            except RunActiveError as exc:
                # A concurrent respond reserved the run slot first (its graph
                # was still resolving); surface it as a typed envelope, not 500.
                return error_envelope(cmd_id, exc.code, exc.message)
            return success_envelope(cmd_id, {"run_id": run_id})

        if method == "run.cancel":  # extension: absent from protocol v0.0.18
            manager.cancel(thread_id, params.get("run_id"))
            return success_envelope(cmd_id, {})

        if method == "state.get":
            try:
                graph = await _state_graph(thread_id)
            except UnknownAssistantError:
                return error_envelope(
                    cmd_id,
                    "unknown_command",
                    f"assistant for thread {thread_id} is not registered",
                )
            if graph is None:
                return success_envelope(cmd_id, empty_state(thread_id))
            snapshot = await read_thread_state(graph, _thread_config(thread_id))
            return success_envelope(cmd_id, snapshot)

        return error_envelope(cmd_id, "unknown_command", f"unsupported method: {method}")

    @router.post("/threads/{thread_id}/runs/{run_id}/cancel")
    async def cancel_run(thread_id: str, run_id: str) -> Any:
        if deps.thread_store.get(thread_id) is None:
            return JSONResponse(status_code=404, content={"detail": "thread not found"})
        manager = deps.run_manager
        manager.cancel(thread_id, run_id)
        status = "interrupted" if manager.active_run(thread_id) is None else "cancelling"
        return {"run_id": run_id, "status": status}

    async def _stream(request: Request, thread_id: str) -> Any:
        if deps.thread_store.get(thread_id) is None:
            return JSONResponse(status_code=404, content={"detail": "thread not found"})

        body: dict[str, Any] = {}
        try:
            payload = await asyncio.wait_for(request.json(), timeout=5.0)
            if isinstance(payload, dict):
                body = payload
        except Exception:
            body = {}
        channels = body.get("channels") if isinstance(body.get("channels"), list) else None
        namespaces = (
            [list(ns) for ns in body["namespaces"] if isinstance(ns, list)]
            if isinstance(body.get("namespaces"), list)
            else None
        )
        since = body.get("since") if isinstance(body.get("since"), int) else None

        hub = deps.hubs.hub(thread_id)
        subscription = hub.subscribe(channels=channels, namespaces=namespaces, since=since)

        async def gen() -> AsyncIterator[str]:
            heartbeat = deps.heartbeat_seconds
            try:
                yield ": connected\n\n"  # flush headers immediately
                # exact-once replay: buffered events newer than the cursor, in order
                for envelope in hub.replay(subscription):
                    yield sse_block(envelope)
                while True:
                    try:
                        envelope = await asyncio.wait_for(
                            subscription.queue.get(), timeout=heartbeat
                        )
                    except TimeoutError:
                        yield ": heartbeat\n\n"
                        continue
                    # dedup: an event published between subscribe and the replay
                    # pass sits in both the buffer and this queue
                    if not subscription.matches(envelope):
                        continue
                    subscription.last_seq = envelope["seq"]
                    yield sse_block(envelope)
            finally:
                hub.unsubscribe(subscription)

        return StreamingResponse(gen(), media_type="text/event-stream")

    @router.post("/threads/{thread_id}/stream")
    async def stream_canonical(request: Request, thread_id: str) -> Any:
        return await _stream(request, thread_id)

    @router.post(
        "/threads/{thread_id}/stream/events",
        deprecated=True,
        summary="Deprecated alias of POST /threads/{thread_id}/stream",
    )
    async def stream_events_alias(request: Request, thread_id: str) -> Any:
        return await _stream(request, thread_id)

    @router.get("/info")
    async def info() -> dict[str, Any]:
        """Minimal descriptor so ``langgraph-sdk-js`` client init won't 404."""
        return {"agents": [{"assistant_id": aid} for aid in deps.registry.assistant_ids]}

    return router


__all__ = [
    "DEFAULT_HEARTBEAT_S",
    "build_router",
    "error_envelope",
    "sse_block",
    "success_envelope",
]
