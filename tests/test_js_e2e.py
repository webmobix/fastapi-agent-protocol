"""Tasks 3.4/3.5: pinned langgraph-sdk-js + useStreamRuntime wiring, end to end.

Boots the fixture protocol app as a subprocess and runs the ``js/`` harness
(``e2e.mjs``: stream, interrupt/resume, cancel, reload hydration;
``check_runtime.mjs``: Assistant-UI runtime wiring proof). Skips when node
deps cannot be installed (offline).
"""

import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
JS = ROOT / "js"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _npm_available() -> bool:
    return shutil.which("node") is not None and shutil.which("npm") is not None


@pytest.mark.slow
def test_js_sdk_e2e() -> None:
    if not _npm_available():
        pytest.skip("node/npm not available")
    if not (JS / "node_modules").exists():
        r = subprocess.run(
            ["npm", "ci", "--no-audit", "--no-fund"],
            cwd=JS,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if r.returncode != 0:
            pytest.skip(f"npm ci failed (offline?): {r.stderr[-500:]}")

    port = _free_port()
    server = subprocess.Popen(
        [sys.executable, "tests/fixture_app.py", str(port)],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        url = f"http://127.0.0.1:{port}"
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if httpx.get(url + "/info", timeout=1.0).status_code == 200:
                    break
            except Exception:
                time.sleep(0.1)
        else:
            pytest.fail("fixture server did not start")

        for script in ("check_runtime.mjs", "e2e.mjs"):
            args = ["node", script] + ([url] if script == "e2e.mjs" else [])
            r = subprocess.run(args, cwd=JS, capture_output=True, text=True, timeout=120)
            assert r.returncode == 0, f"{script} failed:\n{r.stdout}\n{r.stderr}"
    finally:
        server.terminate()
        server.wait(timeout=15)
