"""Task 1.3: sqlite thread store persistence across instances."""

from fastapi_agent_protocol.threads import sqlite


def test_sqlite_persists_across_instances(tmp_path) -> None:  # type: ignore[no-untyped-def]
    path = str(tmp_path / "threads.sqlite")
    store = sqlite(path)
    record = store.create({"project": "p1"})
    store.set_assistant(record.thread_id, "sales")
    store.close()

    reopened = sqlite(path)
    try:
        fetched = reopened.get(record.thread_id)
        assert fetched is not None
        assert fetched.metadata == {"project": "p1"}
        assert fetched.assistant_id == "sales"
        assert fetched.created_at == record.created_at
    finally:
        reopened.close()


def test_sqlite_unknown_returns_none(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = sqlite(str(tmp_path / "t.sqlite"))
    try:
        assert store.get("missing") is None
    finally:
        store.close()
