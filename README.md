# fastapi-agent-protocol

Reusable FastAPI runtime that exposes user-owned LangGraph / DeepAgents graphs
over the Agent Protocol streaming surface — no separate langgraph-server needed.

```python
from fastapi import FastAPI
from fastapi_agent_protocol import create_protocol

protocol = create_protocol(agents={"support": support_factory})
app = FastAPI(lifespan=protocol.lifespan)
app.include_router(protocol.router)
```

See `examples/minimal.py` for a runnable host (`uv run uvicorn examples.minimal:app`).

## What you get

- **Thread provisioning**: `POST /threads` (uuidv7 ids, opaque metadata echo).
- **Command dispatch**: `POST /threads/{id}/commands` with `{id, method, params}`
  envelopes → `{type: success, id, result}` / `{type: error, id, error, message}`
  over HTTP 200 for `run.start`, `input.respond`, `run.cancel`, `state.get`.
- **SSE streaming**: canonical `POST /threads/{id}/stream` plus deprecated alias
  `POST /threads/{id}/stream/events` — seq-stamped envelopes, `since` replay,
  channel/namespace filtering, heartbeat, exact-once handoff.
- **State/history hydration**: `GET /threads/{id}/state`, `GET` + `POST`
  `/threads/{id}/history` (the POST form matches SDK `getHistory`), `GET /info`
  stub so `langgraph-sdk-js` client init won't 404.
- **Run lifecycle**: one active run per protocol thread, concurrent across
  threads; terminal `completed` / `failed` / `interrupted` (cancel reports
  `interrupted` — the protocol has no cancelled status); interrupts resume via
  `Command(resume=...)`.

## Factory defaults

```python
protocol = create_protocol(
    agents={"support": support_factory},  # factory(config) -> compiled graph
    thread_store=None,      # default: in-process memory()
    replay_size=500,        # per-thread SSE ring-buffer retention
    heartbeat_seconds=15.0, # SSE heartbeat interval
)
protocol.register("sales", sales_factory)  # late registration, no restart
```

`factory(config)` may be sync or async. **Close over your own checkpointer
inside the factory** (file-backed in production) — the library never builds
graphs, never owns checkpointers, never imports app code. The saver instance
must be shared across factory calls, otherwise state reads can't find the
run's checkpoints. The factory stamps stored thread metadata into
`configurable.session_metadata` on every `run.start`.

## Thread stores

```python
from fastapi_agent_protocol import create_protocol, memory, sqlite

memory_store = memory()          # default, zero deps, vanishes on restart
sqlite_store = sqlite("threads.sqlite")  # durable assistant binding

protocol = create_protocol(agents={...}, thread_store=sqlite_store)
```

Custom stores implement the `ThreadStore` protocol (`create` / `get` /
`set_assistant`); the thread row is `thread_id + metadata (opaque) +
assistant_id + timestamps` — app FKs stay outside, no ORM in the lib. Pair a
durable store with a file-backed graph saver for restart-safe resume.

## Frontend

Frontends migrate from `/stream/events` to canonical `/stream` at their own
pace (alias serves identical bytes). `since` replay cursors are valid only
within one process lifetime — after detecting a fresh server, reconnect with
`since: 0` and rehydrate via state. The `Last-Event-ID` header is ignored.

## Non-goals (v0.1)

Graph builders, model resolution, saver provisioning, session/workspace
business logic, background-run queue, crons, store APIs, MCP/A2A, Studio,
auth/rate-limiting, multi-worker fan-out, Postgres, subagent namespace
projection (root namespace only), `Last-Event-ID` reconnect, full `/info`
parity, thread search/patch/delete.

## Development

```bash
uv sync --group dev        # ruff, mypy, pytest, uvicorn, ...
uv run pytest              # full suite (incl. live-socket SSE + JS SDK e2e)
uv run ruff check src tests
uv run mypy src
cd js && npm ci && npm run check-runtime && node e2e.mjs <server-url>
```
