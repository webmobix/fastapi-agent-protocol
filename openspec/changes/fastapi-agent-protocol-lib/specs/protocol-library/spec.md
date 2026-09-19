## Purpose

Reusable FastAPI runtime that exposes user-owned LangGraph and DeepAgents graphs over the Agent Protocol streaming surface, with pluggable agent registration and thread storage.

## ADDED Requirements

### Requirement: Mountable protocol factory

The system SHALL provide a `create_protocol(agents=..., thread_store=...)` factory returning a mountable FastAPI router plus lifespan state with sensible defaults, and the factory object SHALL expose `register(id, factory)` for late agent registration.

#### Scenario: Zero-config mount

- **WHEN** a host app calls `create_protocol(agents={"support": factory})`, mounts the returned router via `include_router`, and wires the returned lifespan
- **THEN** thread provisioning, command dispatch, and streaming work without additional configuration

#### Scenario: Late registration

- **WHEN** a host calls `register(id, factory)` on the returned factory object after mount
- **THEN** runs may start against the newly registered agent id without restarting the app

### Requirement: Agent registration without graph ownership

The system SHALL allow host apps to register named agents via factories of shape `factory(config) -> compiled graph` and SHALL resolve the graph per run without constructing checkpointers or importing agent code.

#### Scenario: Register and run a custom agent

- **WHEN** a host registers `support` with a factory returning an already-compiled graph and starts a run with `assistant_id: support`
- **THEN** the run executes that graph with `configurable.thread_id` set and streams its events

#### Scenario: Unknown assistant rejected

- **WHEN** a run starts with an unregistered `assistant_id`
- **THEN** the command responds with a typed `unknown_command` error envelope and no run is created

### Requirement: Thread lifecycle with pluggable store

The system SHALL provision threads with uuidv7 ids, persist opaque metadata plus last `assistant_id` and timestamps, return 404 for unknown threads, and SHALL ship `memory` (default) and `sqlite(path)` stores while accepting custom `ThreadStore` implementations.

#### Scenario: Create then use thread

- **WHEN** a client posts to `POST /threads` with metadata
- **THEN** the system returns a thread object with id, metadata echo, and timestamps, usable for subsequent commands and streams

#### Scenario: Unknown thread guarded

- **WHEN** a client calls state, history, commands, or stream with an unknown id
- **THEN** the system returns 404 and starts no run

#### Scenario: Assistant binding survives restart

- **WHEN** a thread ran `assistant_id: sales` and the process restarts with the same sqlite store and user saver
- **THEN** state, history, and interrupt resume use the `sales` graph schema

### Requirement: Run lifecycle with per-thread serialization

The system SHALL allow at most one active run per protocol thread, SHALL run different protocol threads as concurrent asyncio tasks, SHALL support `run.start`, `input.respond`, `run.cancel` plus REST cancel, and SHALL emit exactly one terminal lifecycle event per run (`completed`, `failed`, `interrupted`).

#### Scenario: Parallel threads stream independently

- **WHEN** runs start on thread A and thread B concurrently
- **THEN** both stream lifecycle, message, and values events on their own hubs without interleaving

#### Scenario: Second start on busy thread rejected

- **WHEN** a run is active on a thread and another `run.start` arrives for the same thread
- **THEN** the system responds with a typed active-run error and keeps the first run

#### Scenario: Interrupt and resume

- **WHEN** a graph emits `__interrupt__` entries and the client responds with `input.respond` carrying a pending `interrupt_id`
- **THEN** the system resumes the same thread via `Command(resume=...)` and publishes `interrupted` followed by `input.requested` per pending interrupt

#### Scenario: Respond to unknown interrupt rejected

- **WHEN** `input.respond` carries an `interrupt_id` with no pending interrupt on the thread
- **THEN** the command responds with a typed `no_such_interrupt` error envelope, no run is created, and any previously pending interrupts remain intact

#### Scenario: Failed start preserves pending interrupts

- **WHEN** `run.start` is rejected on a thread with pending interrupts (busy thread or unknown assistant)
- **THEN** the pending interrupts remain registered and remain resumable afterward

#### Scenario: Cancel checkpoints and reports interrupted

- **WHEN** a client cancels via command or `POST /threads/{id}/runs/{run_id}/cancel`
- **THEN** the active task is cancelled, checkpoints mid-flight, and publishes `interrupted` with no distinct cancelled terminal status

### Requirement: State and history hydration

The system SHALL serve `GET /threads/{id}/state`, `GET` and `POST /threads/{id}/history` from the user graph's checkpointer, SHALL serialize messages to the flat wire shape, SHALL reconstruct cumulative values for delta-channel graphs, and SHALL return empty state for untouched threads without requiring a graph.

#### Scenario: Hydrate after reload

- **WHEN** a client fetches state for a thread with checkpoints
- **THEN** values, interrupts, tasks, and checkpoint pointers reflect the latest snapshot in protocol shape

#### Scenario: Paginate history newest-first

- **WHEN** a client requests history with `limit` and `before` cursor
- **THEN** the system returns checkpoint states newest-first honoring the cursor
