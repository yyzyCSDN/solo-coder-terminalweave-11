from datetime import datetime, timezone
from threading import Thread

import pytest

from terminalweave.core.errors import ConflictError, ValidationError
from terminalweave.persistence.event_store import InMemoryEventStore
from terminalweave.persistence.snapshot import SnapshotStore


UTC = timezone.utc
D = datetime(2026, 1, 1, tzinfo=UTC)


def test_events_and_snapshots_do_not_leak_mutable_state_or_accept_same_sequence_change():
    events = InMemoryEventStore()
    payload = {"plan": {"status": "draft"}}
    events.append("P1", 0, "created", payload, D)
    payload["plan"]["status"] = "external-change"
    assert events.read("P1")[0].payload["plan"]["status"] == "draft"

    returned = events.read("P1")[0]
    returned.payload["plan"]["status"] = "reader-change"
    assert events.read("P1")[0].payload["plan"]["status"] == "draft"

    snapshots = SnapshotStore()
    snapshots.put("P1", 1, {"status": "draft"}, D)
    with pytest.raises(ConflictError):
        snapshots.put("P1", 1, {"status": "published"}, D)


def test_event_store_deep_isolates_nested_mutation_on_append_and_read():
    events = InMemoryEventStore()
    payload = {"plan": {"status": "draft"}, "items": [1, 2]}
    events.append("P1", 0, "created", payload, D)
    payload["plan"]["status"] = "external-change"
    payload["items"].append(3)

    stored = events.read("P1")[0]
    assert stored.payload == {"plan": {"status": "draft"}, "items": [1, 2]}

    stored.payload["plan"]["status"] = "reader-change"
    stored.payload["items"].append(99)
    assert events.read("P1")[0].payload == {"plan": {"status": "draft"}, "items": [1, 2]}

    appended = events.append("P1", 1, "updated", {"n": {"v": 1}}, D)
    appended.payload["n"]["v"] = 42
    assert events.read("P1")[1].payload == {"n": {"v": 1}}


def test_event_store_rejects_old_version_and_duplicate_submission():
    events = InMemoryEventStore()
    events.append("P1", 0, "created", {"v": 1}, D)
    events.append("P1", 1, "updated", {"v": 2}, D)

    # Stale expected sequence (old version)
    with pytest.raises(ConflictError):
        events.append("P1", 0, "stale", {"v": 3}, D)
    # Repeating a successful submit is a conflict, not a silent second event
    with pytest.raises(ConflictError):
        events.append("P1", 1, "duplicate", {"v": 3}, D)

    assert [e.sequence for e in events.read("P1")] == [1, 2]
    assert events.read("P1")[1].payload == {"v": 2}


def test_snapshot_same_sequence_identical_value_is_idempotent():
    snapshots = SnapshotStore()
    first = snapshots.put("P1", 1, {"status": "draft"}, D)
    # A resubmit with the identical state returns the stored snapshot...
    second = snapshots.put("P1", 1, {"status": "draft"}, D)
    assert second.sequence == first.sequence
    assert second.state == {"status": "draft"}
    assert second.created_at == first.created_at
    # ...and mutating either returned snapshot does not affect the store
    first.state["status"] = "changed-by-caller"
    second.state["status"] = "changed-by-caller"
    assert snapshots.get("P1").state == {"status": "draft"}


def test_snapshot_rejects_older_sequence_and_never_leaks_state():
    snapshots = SnapshotStore()
    snapshots.put("P1", 2, {"status": "published"}, D)

    with pytest.raises(ConflictError):
        snapshots.put("P1", 1, {"status": "draft"}, D)

    incoming = {"nested": {"a": [1]}}
    snapshots.put("P2", 5, incoming, D)
    incoming["nested"]["a"].append(2)
    assert snapshots.get("P2").state == {"nested": {"a": [1]}}

    got = snapshots.get("P2")
    got.state["nested"]["a"].append(99)
    assert snapshots.get("P2").state == {"nested": {"a": [1]}}

    with pytest.raises(ValidationError):
        snapshots.put("P3", -1, {}, D)


def test_concurrent_appoints_have_one_winner_and_stable_conflicts():
    events = InMemoryEventStore()
    outcomes: list[str] = []

    def worker():
        try:
            events.append("S", 0, "race", {"x": 1}, D)
            outcomes.append("ok")
        except ConflictError:
            outcomes.append("conflict")

    threads = [Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(outcomes).count("ok") == 1
    assert sorted(outcomes).count("conflict") == 19
    assert events.current_sequence("S") == 1
    assert events.read("S")[0].payload == {"x": 1}


def test_concurrent_identical_snapshot_submissions_all_stable():
    snapshots = SnapshotStore()
    outcomes: list[str] = []

    def worker():
        try:
            snapshots.put("Q", 1, {"status": "draft"}, D)
            outcomes.append("ok")
        except ConflictError:
            outcomes.append("conflict")

    threads = [Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes == ["ok"] * 20
    assert snapshots.get("Q").state == {"status": "draft"}
