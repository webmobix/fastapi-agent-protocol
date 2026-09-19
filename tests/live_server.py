"""Live uvicorn server for SSE streaming tests.

This repo's ``TestClient`` and httpx's ``ASGITransport`` both buffer the full
response body, so infinite SSE streams can only be exercised over a real
socket. This helper boots the fixture protocol app on localhost.
"""

import socket
import threading
import time

import httpx
import uvicorn
from fixture_app import build_fixture_app


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class LiveServer:
    """Context manager: uvicorn on a background thread, ready when /info 200s."""

    def __init__(self, **kw) -> None:  # type: ignore[no-untyped-def]
        self.app, self.protocol = build_fixture_app(**kw)
        self.port = _free_port()
        self._server = uvicorn.Server(
            uvicorn.Config(self.app, host="127.0.0.1", port=self.port, log_level="warning")
        )
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> "LiveServer":
        self._thread.start()
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                r = httpx.get(self.url + "/info", timeout=1.0)
                if r.status_code == 200:
                    return self
            except Exception:
                pass
            time.sleep(0.05)
        raise RuntimeError("live server did not start")

    def __exit__(self, *exc: object) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=10)


def read_sse(  # type: ignore[no-untyped-def]
    client: httpx.Client,
    path: str,
    payload: dict,
    *,
    want: tuple[bytes, ...] = (b"completed", b"failed", b"interrupted"),
    max_bytes: int = 65536,
    timeout_s: float = 20.0,
) -> str:
    """Read a bounded prefix of an infinite SSE stream until a marker appears."""
    buf = b""
    deadline = time.time() + timeout_s
    with client.stream("POST", path, json=payload, timeout=timeout_s + 5) as resp:
        assert resp.status_code == 200
        for chunk in resp.iter_bytes():
            buf += chunk
            if any(w in buf for w in want) or len(buf) >= max_bytes:
                break
            assert time.time() < deadline, f"SSE read timed out waiting for {want}"
    return buf.decode()


def strip_heartbeats(body: str) -> str:
    return "\n".join(line for line in body.splitlines() if line != ": heartbeat")
