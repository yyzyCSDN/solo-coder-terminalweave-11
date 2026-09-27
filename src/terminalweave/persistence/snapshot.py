from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from copy import deepcopy
from threading import RLock
from terminalweave.core.clock import ensure_utc, utc_now
from terminalweave.core.errors import ValidationError, ConflictError

@dataclass(frozen=True)
class Snapshot:
    stream_id: str
    sequence: int
    state: dict[str, object]
    created_at: datetime

    def __post_init__(self):
        object.__setattr__(self, 'created_at', ensure_utc(self.created_at))
        if self.sequence < 0:
            raise ValidationError('snapshot sequence cannot be negative')

class SnapshotStore:

    def __init__(self):
        self._items: dict[str, Snapshot] = {}
        self._lock = RLock()

    def put(self, stream_id: str, sequence: int, state: dict[str, object], created_at: datetime | None=None) -> Snapshot:
        with self._lock:
            current = self._items.get(stream_id)
            if current:
                if sequence < current.sequence:
                    raise ConflictError('cannot replace snapshot with older sequence')
                if sequence == current.sequence:
                    if current.state == state:
                        return _detach(current)
                    raise ConflictError('snapshot already exists at this sequence with different state')
            snap = Snapshot(stream_id, sequence, deepcopy(state), created_at or utc_now())
            self._items[stream_id] = snap
            return _detach(snap)

    def get(self, stream_id: str) -> Snapshot | None:
        with self._lock:
            snap = self._items.get(stream_id)
            return _detach(snap) if snap else None

    def delete_before(self, sequence: int) -> int:
        with self._lock:
            keys = [k for k, v in self._items.items() if v.sequence < sequence]
            for k in keys:
                del self._items[k]
            return len(keys)

def _detach(snap: Snapshot) -> Snapshot:
    return Snapshot(snap.stream_id, snap.sequence, deepcopy(snap.state), snap.created_at)
