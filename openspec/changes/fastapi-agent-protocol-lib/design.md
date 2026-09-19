## Context

See `proposal.md` for motivation. The reference implementation lives in `tmp/orchestrator/agent_protocol/` (`hub`, `translation`, `runner`, `state`, `router`, `registry`, `store`) plus app wiring in `tmp/orchestrator/app.py`. It serves Assistant-UI `useStreamRuntime` (built on `langgraph-sdk-js`) against in-process graphs with a SQLite checkpointer, but is coupled to orchestrator models (`SessionRow`, `ProjectRow`, `AgentThreadRow` with session FK), settings, and hardcoded `interview`/`chat` factories. The lib extracts only the protocol runtime.

Confirmed decisions from exploration: library owns no checkpointer and never builds graphs; it owns the `ThreadStore` (memory default, sqlite built-in, custom pluggable); factory-with-defaults composition; official streaming paths with `stream/events` alias; one active run per protocol thread, parallel across threads via asyncio tasks.

## Goals / Non-Goals

**Goals:**
- Mountable protocol runtime for any FastAPI app wrapping user-compiled LangGraph / DeepAgents graphs.
- Correct streaming semantics: seq-ordered SSE, replay from `since`, filtered fan-out, heartbeat, exact-once handoff.
- Durable interrupts + restart-safe hydration via last-`assistant_id` + user graph's own saver.

**Non-Goals:**
- No graph builders, model resolution, saver provisioning, session/workspace/git business logic.
- No background-run queue, crons, store APIs, MCP/A2A, Studio, auth/rate-limiting, multi-worker fan-out, Postgres in v0.1.
- No subagent namespace projection in v0.1 (root namespace only, accepted but flattened).

## Decisions

- **Agent interface `factory(config) -> compiled graph` over `factory(config, checkpointer)`.** Rationale: checkpointer is a graph-construction concern; closing over it in user code removes the saver from lib entirely. Alternative (pass saver through lib) keeps lib in persistence business — rejected per confirmed scope.
- **Untouched threads return empty state without a probe graph.** Rationale: `tmp` probe needs a lib-owned saver; without it, no graph is needed until first run. Alternative (require caller-supplied probe) adds API surface for zero value — rejected.
- **State/history reads always via last-assistant graph.** Rationale: checkpoint channels only decode through matching schema; persisting `assistant_id` on the thread row makes resume restart-safe. Alternative (single global reader) breaks DeepAgents delta channels — rejected.
- **`ThreadStore` as small interface (`create/get/set_assistant/get_assistant`, metadata opaque) with `memory` + `sqlite(path)` built-ins.** Rationale: protocol needs existence + metadata + last-assistant only; app FKs stay outside. SQLAlchemy explicitly excluded to keep the lib light. Alternative (bring-your-own-ORM) pushes durability work to every user — rejected for v0.1.
- **Top factory `create_protocol(agents={...}, thread_store=..., ...)` returning router + lifespan state, plus `register(id, factory)`.** Rationale: zero-config mount (`agents` only) with escape hatches (replay size, heartbeat, cors left to host app). Alternative (bare router + manual `app.state` wiring) repeats `tmp/app.py` boilerplate per project — rejected.
- **Official `POST /threads/{id}/stream` canonical, `POST /threads/{id}/stream/events` alias; `POST /threads/{id}/history` alongside `GET history` for SDK `getHistory`.** Rationale: `langgraph-sdk-js` emits both shapes; alias preserves current frontend while converging on spec. Alternative (only new paths) breaks existing client day one — rejected.
- **Run slot reserved before any await.** The active-run check and registration happen synchronously before graph resolution, so two concurrent `run.start`s on one thread can never both slip through (the reference `runner.py` checks `_active`, then awaits the graph build, then registers — a race the port must close). Failed starts release the slot.
- **Failed `start` preserves pending interrupts.** Pending interrupts are consumed only when the replacement run actually begins executing, not when `run.start` is dispatched — a rejected start (busy thread, unknown assistant) leaves the pending map intact.
- **Minimal `GET /info` stub.** `langgraph-sdk-js` probes `GET /info` at client init; the lib serves a static `{}`-plus-agents stub so SDK construction does not 404. Full agent-server `/info` parity stays out of scope.
- **`Last-Event-ID` header reconnect is a v0.1 non-goal.** `langgraph-sdk-js` streams via fetch (not `EventSource`), so the browser auto-reconnect header is not on the hot path; body `since` is the only replay cursor. Alternative (honor the header) adds an untested surface — deferred.

## Risks / Trade-offs

- [SDK drift] `langgraph-sdk-js` / `useStream` envelope shapes change often → Mitigation: golden tests against pinned SDK versions, not just OpenAPI.
- [Event-loop starvation] sync-blocking nodes stall all concurrent protocol threads → Mitigation: document async nodes + offload; lib never blocks serving endpoints on a run.
- [Restart loss with memory store] hubs + active runs are in-memory; memory threads vanish → Mitigation: sqlite store for threads + file saver in user graphs documented as production pairing; interrupts validated against checkpointer, not run-manager memory.
- [History reconstruction for delta graphs] DeepAgents store superstep outputs in pending writes → Mitigation: carry over `tmp/state.py` oldest-first accumulation logic verbatim.
- [Path divergence] alias doubles SSE surface → Mitigation: alias marked deprecated, same handler, removed in a later major.
- [Seq reset on restart] hubs are in-memory, so after a restart a reconnecting client's `since` cursor exceeds the fresh hub's seq and replay silently yields nothing → Mitigation: document that `since` cursors are valid only within one process lifetime; clients must use `since: 0` (or state hydration) after detecting a fresh server. Epoch-stamped envelopes deferred to v0.2.
- [Probe surface] `langgraph-sdk-js` expects `GET /info` at client init → Mitigation: minimal static stub served by the router; covered by the SDK end-to-end test.

## Migration Plan

- v0.1 ships lib; `tmp/` reference app untouched.
- Later consumer change mounts lib in orchestrator, implements `ThreadStore` over existing `agent_threads` if desired, moves `interview`/`chat` factories to `register()`, flips frontend to canonical `/stream`, removes alias.

## Open Questions

- None blocking specs/tasks. Deferred: `POST /threads/search` + thread patch/delete/copy scope for v0.2; background `POST /runs*` + `agents/*` + `store/*` only if SDK drop-in without streaming client is required.
