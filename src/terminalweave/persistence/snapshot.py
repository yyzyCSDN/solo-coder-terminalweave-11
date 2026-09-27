from __future__ import annotations
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
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
        # Deep copy so nested state is never shared with callers.
        object.__setattr__(self, 'state', deepcopy(self.state))
        object.__setattr__(self, 'created_at', ensure_utc(self.created_at))
        if self.sequence < 0:
            raise ValidationError('snapshot sequence cannot be negative')

    def detached(self) -> 'Snapshot':
        """Return an independent copy safe to hand to callers."""
        return Snapshot(self.stream_id, self.sequence, self.state, self.created_at)

class SnapshotStore:

    def __init__(self):
        self._items: dict[str, Snapshot] = {}
        self._lock = RLock()

    def put(self, stream_id: str, sequence: int, state: dict[str, object], created_at: datetime | None=None) -> Snapshot:
        if sequence < 0:
            raise ValidationError('snapshot sequence cannot be negative')
        # Copy once, before validation, so concurrent caller mutation cannot
        # slip in between the check and the store.
        incoming = deepcopy(state)
        with self._lock:
            current = self._items.get(stream_id)
            if current is not None:
                if sequence < current.sequence:
                    raise ConflictError('cannot replace snapshot with older sequence')
                if sequence == current.sequence:
                    # Same sequence is immutable: a different value is a
                    # conflict, an identical resubmit is idempotent.
                    if incoming != current.state:
                        raise ConflictError(
                            f'snapshot for stream {stream_id} at sequence '
                            f'{sequence} already exists with different state')
                    return current.detached()
            snap = Snapshot(stream_id, sequence, incoming, created_at or utc_now())
            self._items[stream_id] = snap
            return snap.detached()

    def get(self, stream_id: str) -> Snapshot | None:
        with self._lock:
            snap = self._items.get(stream_id)
            return snap.detached() if snap else None

    def delete_before(self, sequence: int) -> int:
        with self._lock:
            keys = [k for k, v in self._items.items() if v.sequence < sequence]
            for k in keys:
                del self._items[k]
            return len(keys)
