"""Task 1.2: ThreadStore interface + memory store round-trip."""

from fastapi_agent_protocol.threads import MemoryThreadStore, memory


def test_create_get_round_trip() -> None:
    store = memory()
    record = store.create({"session_id": "s1"})
    assert record.thread_id
    assert record.metadata == {"session_id": "s1"}
    assert record.assistant_id is None
    assert record.created_at and record.updated_at

    fetched = store.get(record.thread_id)
    assert fetched is not None
    assert fetched.thread_id == record.thread_id
    assert fetched.metadata == {"session_id": "s1"}


def test_set_assistant_round_trip() -> None:
    store = MemoryThreadStore()
    record = store.create({})
    assert store.get(record.thread_id) is not None
    assert store.get(record.thread_id).assistant_id is None  # type: ignore[union-attr]
    store.set_assistant(record.thread_id, "sales")
    fetched = store.get(record.thread_id)
    assert fetched is not None
    assert fetched.assistant_id == "sales"


def test_unknown_thread_returns_none() -> None:
    store = memory()
    assert store.get("does-not-exist") is None
    # set_assistant on unknown is a no-op, must not raise
    store.set_assistant("does-not-exist", "x")


def test_create_defaults_to_empty_metadata() -> None:
    store = memory()
    record = store.create()
    assert record.metadata == {}
