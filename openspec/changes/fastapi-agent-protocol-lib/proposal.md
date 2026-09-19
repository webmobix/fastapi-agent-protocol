## Why

Every project that serves LangGraph / DeepAgents to a web frontend re-implements the same FastAPI + SSE + run-lifecycle glue, or runs a separate langgraph-server. A tiny reusable library that wraps user-owned graphs with the Agent Protocol streaming surface removes that repetition while staying self-hosted.

## What Changes

- Add standalone `fastapi-agent-protocol` library mounted in any FastAPI app via `include_router` + factory with defaults.
- Provide protocol runtime only: thread endpoints, `POST /threads/{id}/commands` (`run.start`, `input.respond`, `run.cancel`, `state.get`), SSE stream with seq replay + heartbeat, state/history hydration via user graphs.
- Provide `AgentRegistry` with user factories of shape `factory(config) -> compiled graph`; library never builds graphs, never owns checkpointers, never imports app-specific agent code. Works for `StateGraph` and DeepAgents alike.
- Provide `ThreadStore` interface with `memory` default (zero deps) and `sqlite` built-in; custom stores pluggable. No SQLAlchemy / app models in lib; thread row is `thread_id + metadata (opaque) + assistant_id + timestamps`.
- Serve official paths `POST /threads/{id}/stream` + `POST /threads/{id}/commands` and keep `POST /threads/{id}/stream/events` as deprecated alias for the current frontend.
- Enforce one active run per protocol thread; different protocol threads run as concurrent asyncio tasks.
- Extract portable behavior from `tmp/orchestrator/agent_protocol/` (`hub`, `translation`, runner lifecycle, state serialization + history reconstruction) as guide, not verbatim copy.

## Capabilities

### New Capabilities

- `protocol-library`: reusable FastAPI Agent Protocol runtime — thread provisioning, command dispatch, SSE streaming with replay, state/history reads, run lifecycle + interrupts + cancel, agent registry, thread-store backends.
- `protocol-compat`: langgraph-sdk-js / Assistant-UI `useStreamRuntime` compatibility — official streaming paths + envelopes, error shapes, cancel semantics, history shape the SDK expects.

### Modified Capabilities

- None (greenfield library; `tmp/` reference app unchanged in this change).

## Impact

- Affects: new package under `src/fastapi_agent_protocol/` (router, runner, hub, translation, state, registry, thread stores, factory); no changes to `tmp/` in this change.
- APIs: new public factory + router; frontend migrates from `/stream/events` to `/stream` at its own pace via alias.
- Dependencies: FastAPI + SSE via `StreamingResponse`; LangGraph types only at the compiled-graph interface boundary (no server, no cloud, no ORM in lib).
- Systems: single-process asyncio runtime; Postgres / multi-worker fan-out explicitly out of scope for v0.1.
