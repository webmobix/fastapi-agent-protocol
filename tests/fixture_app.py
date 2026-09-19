"""Shared fixture protocol app (echo/ask/slow graphs over one saver each).

Used by the pytest live server and the JS SDK end-to-end harness.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from conftest import (
    make_chat_factory,
    make_echo_factory,
    make_interrupt_factory,
    slow_echo_factory,
)
from fastapi import FastAPI

from fastapi_agent_protocol import create_protocol, memory


def build_fixture_app(**kw):  # type: ignore[no-untyped-def]
    kw.setdefault("heartbeat_seconds", 0.1)
    protocol = create_protocol(
        agents={
            "echo": make_echo_factory(),
            "chat": make_chat_factory(),
            "ask": make_interrupt_factory(),
            "slow": slow_echo_factory(30.0),
        },
        thread_store=memory(),
        **kw,
    )
    app = FastAPI(lifespan=protocol.lifespan)
    app.include_router(protocol.router)
    return app, protocol


if __name__ == "__main__":
    import uvicorn

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8123
    app, _ = build_fixture_app()
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
