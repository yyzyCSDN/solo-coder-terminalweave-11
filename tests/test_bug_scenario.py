from datetime import datetime, timezone

import pytest

from terminalweave.core.errors import ConflictError
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

    returned = events.read("P1")[0].payload
    returned["plan"]["status"] = "reader-change"
    assert events.read("P1")[0].payload["plan"]["status"] == "draft"

    snapshots = SnapshotStore()
    snapshots.put("P1", 1, {"status": "draft"}, D)
    with pytest.raises(ConflictError):
        snapshots.put("P1", 1, {"status": "published"}, D)
