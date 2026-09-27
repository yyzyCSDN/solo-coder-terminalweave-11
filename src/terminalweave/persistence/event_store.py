from __future__ import annotations
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from threading import RLock
from terminalweave.core.clock import ensure_utc, utc_now
from terminalweave.core.errors import ValidationError, ConflictError

@dataclass(frozen=True)
class StoredEvent:
    stream_id: str
    sequence: int
    event_type: str
    payload: dict[str, object]
    occurred_at: datetime

    def __post_init__(self):
        # Deep copy so nested payload data is never shared with callers.
        object.__setattr__(self, 'payload', deepcopy(self.payload))
        object.__setattr__(self, 'occurred_at', ensure_utc(self.occurred_at))
        if self.sequence < 1:
            raise ValidationError('sequence must be positive')
        if not self.event_type.strip():
            raise ValidationError('event type required')

    def detached(self) -> 'StoredEvent':
        """Return an independent copy safe to hand to callers."""
        return StoredEvent(self.stream_id, self.sequence, self.event_type,
                           self.payload, self.occurred_at)

class InMemoryEventStore:

    def __init__(self):
        self._streams: dict[str, list[StoredEvent]] = {}
        self._lock = RLock()

    def append(self, stream_id: str, expected_sequence: int, event_type: str, payload: dict[str, object], occurred_at: datetime | None=None) -> StoredEvent:
        with self._lock:
            stream = self._streams.setdefault(stream_id, [])
            current = stream[-1].sequence if stream else 0
            if current != expected_sequence:
                raise ConflictError(f'stream {stream_id} expected {expected_sequence}, actual {current}')
            event = StoredEvent(stream_id, current + 1, event_type, payload, occurred_at or utc_now())
            stream.append(event)
            return event.detached()

    def read(self, stream_id: str, after_sequence: int=0) -> tuple[StoredEvent, ...]:
        if after_sequence < 0:
            raise ValidationError('after_sequence cannot be negative')
        with self._lock:
            return tuple(e.detached()
                         for e in self._streams.get(stream_id, ())
                         if e.sequence > after_sequence)

    def current_sequence(self, stream_id: str) -> int:
        with self._lock:
            stream = self._streams.get(stream_id, ())
            return stream[-1].sequence if stream else 0

    def all_streams(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._streams))
