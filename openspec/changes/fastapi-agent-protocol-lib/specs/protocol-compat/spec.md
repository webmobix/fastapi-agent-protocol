## Purpose

Wire compatibility with langchain-ai agent-protocol streaming plus langgraph-sdk-js and Assistant-UI useStreamRuntime clients, so frontends stream agent sessions without a separate langgraph-server.

## ADDED Requirements

### Requirement: Canonical streaming endpoints with alias

The system SHALL serve `POST /threads/{thread_id}/stream` as the canonical SSE endpoint and SHALL keep `POST /threads/{thread_id}/stream/events` as a deprecated alias with identical behavior, including channel/namespace filtering, `since` replay, heartbeat, and exact-once handoff.

#### Scenario: Subscribe with replay

- **WHEN** a client opens SSE with `channels: ["messages"]` and `since: 12` after events 13-15 were published
- **THEN** it receives 13-15 in order followed by live events with no duplicates

#### Scenario: Legacy path still streams

- **WHEN** an existing frontend posts to `/stream/events`
- **THEN** it receives the same envelope sequence as the canonical path

### Requirement: SDK init probe

The system SHALL serve `GET /info` with a minimal descriptor so `langgraph-sdk-js` client initialization does not fail against the mounted runtime.

#### Scenario: SDK constructs against the runtime

- **WHEN** `langgraph-sdk-js` probes `GET /info` at client init
- **THEN** the runtime responds 200 without a 404 and streaming proceeds normally

#### Scenario: Last-Event-ID reconnect out of scope

- **WHEN** a client reconnects without a body `since` cursor
- **THEN** replay is driven solely by `since` in the request body (the `Last-Event-ID` header is a documented v0.1 non-goal)

### Requirement: Streaming command envelope

The system SHALL serve `POST /threads/{thread_id}/commands` accepting `{id, method, params}` and SHALL respond with `{type: success, id, result}` or `{type: error, id, error, message}` inside HTTP 200 for `run.start`, `input.respond`, `run.cancel`, and `state.get`.

#### Scenario: Start returns run id

- **WHEN** a client sends `run.start` with `assistant_id`, `input`, and `config`
- **THEN** the system stamps stored thread metadata into `configurable.session_metadata`, starts the run, persists the assistant binding, and returns `{run_id}`

#### Scenario: History POST shape for SDK

- **WHEN** the SDK calls `POST /threads/{id}/history` with `{limit, before}` where `before` may be a checkpoint id or full config
- **THEN** the system normalizes the cursor and returns history in the same shape as the GET form
