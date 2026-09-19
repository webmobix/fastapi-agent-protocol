"""Run manager: in-process graph execution with lifecycle + interrupt tracking.

Each accepted ``run.start`` spawns one asyncio task streaming the registered
graph with ``stream_mode=["messages", "values"]`` against
``configurable.thread_id``. Exactly one run may be active per thread; a second
concurrent start is a typed protocol error. Terminal transitions map to
exactly one lifecycle event per run: ``completed``, ``failed`` (with error
detail), or ``interrupted`` (followed by an ``input.requested`` event per
pending interrupt). User cancellation checkpoints mid-flight and reports
``interrupted`` — the protocol has no distinct cancelled terminal status.

Concurrency safety: the active-run check and slot reservation happen
synchronously before graph resolution (no ``await`` between check and reserve),
so two concurrent ``run.start`` calls on one thread can never both slip
through. Failed starts release the slot. Pending interrupts are consumed only
when the replacement run actually begins executing, not when ``run.start`` is
dispatched — a rejected start (busy thread, unknown assistant) leaves the
pending map intact.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Any

from fastapi_agent_protocol.state import serialize_values
from fastapi_agent_protocol.translation import (
    MessagesTranslator,
    interrupts_from_snapshot,
)

logger = logging.getLogger(__name__)

STREAM_MODES = ["messages", "values"]


class RunError(Exception):
    """Typed protocol error surfaced as an ErrorResponse envelope."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class RunActiveError(RunError):
    def __init__(self, thread_id: str, run_id: str) -> None:
        super().__init__(
            "invalid_argument",
            f"thread {thread_id} already has an active run ({run_id})",
        )
        self.run_id = run_id


class UnknownInterruptError(RunError):
    def __init__(self, thread_id: str, interrupt_id: str) -> None:
        super().__init__(
            "no_such_interrupt",
            f"thread {thread_id} has no pending interrupt {interrupt_id}",
        )


@dataclass
class ActiveRun:
    run_id: str
    thread_id: str
    assistant_id: str
    task: asyncio.Task[None] | None


def _lifecycle(graph_name: str, status: str, **extra: Any) -> dict[str, Any]:
    return {"event": status, "graph_name": graph_name, **extra}


class RunManager:
    """Owns per-thread active runs and their lifecycle publications."""

    def __init__(self, registry: Any, hubs: Any) -> None:
        self._registry = registry  # registry.AgentRegistry
        self._hubs = hubs  # hub.HubRegistry
        self._active: dict[str, ActiveRun] = {}
        # thread_id -> pending {interrupt_id: payload}; survives until resumed.
        # "assistant_id" key holds the graph to resume with.
        self._pending_inputs: dict[str, dict[str, Any]] = {}
        # thread_id -> last assistant id that ran (used for state hydration,
        # since a graph's checkpointed channels are only readable through a
        # graph built with the same state schema)
        self._last_assistant: dict[str, str] = {}

    def last_assistant(self, thread_id: str) -> str | None:
        return self._last_assistant.get(thread_id)

    # --- public API ------------------------------------------------------------

    def active_run(self, thread_id: str) -> ActiveRun | None:
        return self._active.get(thread_id)

    async def start(
        self,
        thread_id: str,
        assistant_id: str,
        input: Any,
        *,
        config: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Dispatch a run on ``assistant_id``; returns its run id.

        Raises :class:`RunActiveError` when the thread already runs, and
        propagates the registry's ``UnknownAssistantError`` for unregistered
        ids. The slot is reserved synchronously before graph resolution so
        concurrent starts race-safe; failed starts release it and leave
        pending interrupts intact.
        """
        # Synchronous check-and-reserve: no await before this completes, so
        # concurrent coroutines cannot both pass the guard.
        existing = self._active.get(thread_id)
        if existing is not None and (existing.task is None or not existing.task.done()):
            raise RunActiveError(thread_id, existing.run_id)

        run_id = uuid.uuid4().hex
        reserved = ActiveRun(
            run_id=run_id,
            thread_id=thread_id,
            assistant_id=assistant_id,
            task=None,
        )
        # Reserve synchronously (no await between the check above and here) so
        # a concurrent starter sees this thread as active even before the real
        # asyncio task exists.
        self._active[thread_id] = reserved
        try:
            graph = await self._registry.resolve(assistant_id, config=config)
        except Exception:
            # Failed start: release the slot, preserve pending interrupts.
            if self._active.get(thread_id) is reserved:
                self._active.pop(thread_id, None)
            raise

        run_config: dict[str, Any] = {
            "configurable": {
                "thread_id": thread_id,
                **((config or {}).get("configurable") or {}),
            },
            **{k: v for k, v in (config or {}).items() if k != "configurable"},
        }
        if metadata:
            run_config["metadata"] = {**run_config.get("metadata", {}), **metadata}

        # Success path only: a fresh run supersedes prior pending interrupts.
        # (Consumed here — after resolution succeeded — so rejected starts
        # preserve them.)
        self._pending_inputs.pop(thread_id, None)

        task = asyncio.create_task(
            self._execute(thread_id, run_id, assistant_id, graph, input, run_config),
            name=f"agent-run:{thread_id}:{run_id}",
        )
        self._last_assistant[thread_id] = assistant_id
        active = ActiveRun(run_id=run_id, thread_id=thread_id, assistant_id=assistant_id, task=task)

        def _clear(_: asyncio.Task[None]) -> None:
            if self._active.get(thread_id) is active:
                self._active.pop(thread_id, None)

        task.add_done_callback(_clear)
        self._active[thread_id] = active
        return run_id

    async def respond(
        self, thread_id: str, interrupt_id: str, response: Any, *, assistant_id: str | None = None
    ) -> str:
        """Resume a pending interrupt with the supplied value.

        ``assistant_id`` is the graph to resume; callers should pass the
        persisted assistant id so resume works across restarts. When the
        caller supplies one, the in-memory pending map is not required — the
        interrupt lives in the thread's checkpoints, so a restart (which wipes
        the map) must not block resume. When no assistant id is supplied,
        the id must be in the pending map. Raises :class:`UnknownInterruptError`
        without creating a run when neither source knows the id; a failed
        start (e.g. busy thread) leaves the pending map intact.
        """
        pending = self._pending_inputs.get(thread_id)
        if pending is not None and interrupt_id in pending:
            resolved_assistant = assistant_id or pending.get("assistant_id") or ""
        elif pending is None and assistant_id:
            # Durable path: no in-memory pending map for this thread (e.g. it
            # was lost in a restart) but the caller supplies the persisted
            # assistant id. The interrupt lives in the thread's checkpoints,
            # so resume proceeds; the router validates the id against the
            # checkpointer before calling this.
            resolved_assistant = assistant_id
        else:
            # Map exists for this thread (fresh) and the id is not in it, or
            # no assistant id was supplied: a stale/unknown interrupt id.
            raise UnknownInterruptError(thread_id, interrupt_id)
        from langgraph.types import Command

        return await self.start(thread_id, resolved_assistant, Command(resume=response))

    def cancel(self, thread_id: str, run_id: str | None = None) -> bool:
        """Cancel the active run if any; returns whether one was running.

        ``run_id`` scopes the cancellation to a specific run so a stale Stop
        click cannot kill a newer run that already replaced it.
        """
        active = self._active.get(thread_id)
        if active is None or active.task is None:
            return False
        if run_id is not None and active.run_id != run_id:
            return False
        active.task.cancel()
        return True

    async def wait(self, thread_id: str) -> None:
        """Await terminal settlement of the thread's active run, if any."""
        active = self._active.get(thread_id)
        if active is None or active.task is None:
            return
        try:
            # Shielded: cancelling this waiter must not cancel the observed
            # run — wait() is an observation helper, not an owner.
            await asyncio.shield(active.task)
        except asyncio.CancelledError:
            # The shield re-raises the inner task's cancellation here too;
            # re-raise only when THIS waiter was cancelled (cancellation
            # count > 0), otherwise the run itself was cancelled/failed and
            # that is its expected terminal state.
            waiter = asyncio.current_task()
            if waiter is not None and waiter.cancelling() > 0:
                raise
        except Exception:
            pass  # run failure is already reported on the hub's lifecycle
        await asyncio.sleep(0)  # let the done-callback clear _active

    def pending_interrupts(self, thread_id: str) -> dict[str, Any]:
        return dict(self._pending_inputs.get(thread_id) or {})

    # --- execution ---------------------------------------------------------------

    async def _execute(
        self,
        thread_id: str,
        run_id: str,
        assistant_id: str,
        graph: Any,
        input: Any,
        config: dict[str, Any],
    ) -> None:
        hub = self._hubs.hub(thread_id)
        translator = MessagesTranslator(lambda data: hub.publish("messages", data))
        hub.publish("lifecycle", _lifecycle(assistant_id, "started"))
        hub.publish("lifecycle", _lifecycle(assistant_id, "running"))
        try:
            last_values: dict[str, Any] | None = None
            async for mode, chunk in graph.astream(input, config=config, stream_mode=STREAM_MODES):
                if mode == "messages":
                    stream_chunk, meta = chunk
                    translator.feed(stream_chunk, meta)
                    if getattr(stream_chunk, "usage_metadata", None):
                        translator.finish(stream_chunk, meta)
                elif mode == "values":
                    last_values = chunk
                    hub.publish("values", serialize_values(chunk))
            interrupts = interrupts_from_snapshot(
                last_values
                if last_values is not None
                else await self._snapshot_values(graph, config)
            )
            if interrupts:
                self._register_pending(thread_id, assistant_id, interrupts)
                hub.publish("lifecycle", _lifecycle(assistant_id, "interrupted"))
                for interrupt_id, payload in interrupts:
                    hub.publish(
                        "input.requested",
                        {"interrupt_id": interrupt_id, "payload": payload},
                    )
            else:
                translator.finish(None)
                hub.publish("lifecycle", _lifecycle(assistant_id, "completed"))
        except asyncio.CancelledError:
            translator.finish(None)
            hub.publish("lifecycle", _lifecycle(assistant_id, "interrupted"))
            raise
        except Exception as exc:
            translator.finish(None)
            logger.exception("run %s on thread %s failed", run_id, thread_id)
            hub.publish(
                "lifecycle",
                _lifecycle(assistant_id, "failed", error=f"{type(exc).__name__}: {exc}"),
            )

    async def _snapshot_values(self, graph: Any, config: dict[str, Any]) -> dict[str, Any]:
        """Checkpointer-backed state fallback when no values tuple was streamed."""
        getter = getattr(graph, "aget_state", None)
        if getter is None:
            return {}
        try:
            snapshot = await getter(config)
        except Exception:  # uncheckpointed graphs simply have no snapshot
            logger.debug("state snapshot unavailable for config %s", config.get("configurable"))
            return {}
        return dict(getattr(snapshot, "values", None) or {})

    def _register_pending(
        self, thread_id: str, assistant_id: str, interrupts: list[tuple[str, Any]]
    ) -> None:
        pending: dict[str, Any] = {"assistant_id": assistant_id}
        for interrupt_id, payload in interrupts:
            pending[interrupt_id] = payload
        self._pending_inputs[thread_id] = pending


__all__ = [
    "ActiveRun",
    "RunActiveError",
    "RunError",
    "RunManager",
    "STREAM_MODES",
    "UnknownInterruptError",
]
