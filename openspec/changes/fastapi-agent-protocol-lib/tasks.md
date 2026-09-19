## 1. Library skeleton and factory

- [x] 1.1 Create package layout (`router`, `runner`, `hub`, `translation`, `state`, `registry`, `thread stores`, `factory`) and verify imports resolve without app models.
- [x] 1.2 Implement `ThreadStore` interface + `memory` store and verify create/get/set-assistant round-trip via unit test.
- [x] 1.3 Implement `sqlite(path)` thread store and verify persistence across instances via unit test.
- [x] 1.4 Implement `AgentRegistry` (`register`, `resolve`) with `factory(config)` signature and verify unknown-id error path via unit test.
- [x] 1.5 Implement `create_protocol(agents={...}, thread_store=...)` factory returning router + lifespan state with `register(id, factory)` attached, and verify zero-config `include_router` mount streams against a fixture graph via API test.
- [x] 1.6 Declare runtime dependencies (`fastapi`, `langchain-core`, `langgraph`), dev tooling (`ruff`, `mypy`, `pytest`, `pytest-asyncio`), and ship a lib-owned `uuid7` (stdlib `uuid7` arrives in 3.14; `requires-python >= 3.13`).

## 2. Protocol runtime extraction

- [x] 2.1 Port hub (seq, replay, fan-out, heartbeat) and verify replay + dedup + channel/namespace filtering via unit test.
- [x] 2.2 Port messages translation + values/interrupt helpers and verify frame sequence against golden shapes via unit test.
- [x] 2.3 Generalize run manager (per-thread active map, respond via resume, cancel, terminal lifecycle), reserve the run slot before graph resolution, preserve pending interrupts on failed starts, and verify parallel-threads + busy-thread rejection + race (concurrent `run.start` yields exactly one success) via async test.
- [x] 2.4 Port state serialization + delta-aware history reconstruction (no probe; empty state for untouched threads) and verify against DeepAgents-style pending writes via unit test.

## 3. HTTP surface and compat

- [x] 3.1 Implement thread endpoints + command dispatch envelopes (`run.start`, `input.respond`, `run.cancel`, `state.get`) and verify success/error envelopes via API test.
- [x] 3.2 Implement canonical SSE `POST /threads/{id}/stream` plus deprecated `/stream/events` alias, a minimal `GET /info` stub, and verify identical replay behavior on both paths + SDK init probes `/info` without 404 via API test.
- [x] 3.3 Implement state/history GET + `POST history` SDK shape and REST run cancel, verify 404s and cursor normalization via API test.
- [x] 3.4 Set up a JS test harness with pinned `langgraph-sdk-js` + `useStreamRuntime` (node deps, fixture app wiring).
- [x] 3.5 Verify end-to-end with the pinned SDK + `useStreamRuntime` against a fixture graph (stream, interrupt/resume, cancel, reload hydration).

## 4. Packaging and handoff

- [x] 4.1 Write README (mount example, factory defaults, custom store example, non-goals) and verify example app boots and streams.
- [x] 4.2 Run `ruff`, `mypy`, and full pytest suite; verify green.
- [x] 4.3 Run `openspec validate fastapi-agent-protocol-lib` and verify no errors.
