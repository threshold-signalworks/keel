"""
Keel Write-Ahead Log (WAL)

Append-only JSONL event log. Every state change in Keel produces a WAL entry.
Hash-chained: each event includes the SHA-256 of the previous event,
matching the provenance chain pattern.

Design properties:
- Append-only: existing entries are never modified
- Hash-chained: tamper-evident audit trail
- Flushed after every write: no lost events on crash
- One line per event: JSONL format
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from keel.core.schemas import WALEvent, WALEventType, canonical_hash, _now


class WAL:
    """
    Append-only write-ahead log with SHA-256 hash chaining.
    
    Compatible with external provenance tooling.
    """

    # Genesis hash — the chain starts here
    GENESIS_HASH = "0" * 64

    def __init__(self, wal_path: Optional[Path] = None, session_id: str = ""):
        self._wal_path = wal_path
        self._session_id = session_id
        self._last_hash: str = self.GENESIS_HASH
        self._event_count: int = 0

        # If WAL file exists, recover chain state
        if wal_path and wal_path.exists():
            self._recover_chain_state()

    def _recover_chain_state(self):
        """Read existing WAL to recover the last hash for chain continuity."""
        last_event = None
        count = 0
        with open(self._wal_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    last_event = json.loads(line)
                    count += 1
        if last_event:
            self._last_hash = last_event.get("event_hash", self.GENESIS_HASH)
            self._event_count = count

    def append(self, event: WALEvent) -> WALEvent:
        """
        Append an event to the WAL. Sets the chain hash and flushes to disk.
        Returns the event with computed hashes.
        """
        # Set chain link
        event.session_id = event.session_id or self._session_id
        event.prev_hash = self._last_hash
        event.event_hash = event.compute_hash()

        # Write
        if self._wal_path:
            self._wal_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._wal_path, "a") as f:
                f.write(json.dumps(event.to_dict(), default=str) + "\n")
                f.flush()  # No buffering. No lost events.

        # Update chain state
        self._last_hash = event.event_hash
        self._event_count += 1

        return event

    def log(self, event_type: str, payload: dict) -> WALEvent:
        """Convenience: create and append a WAL event in one call."""
        event = WALEvent(
            event_type=event_type,
            payload=payload,
            session_id=self._session_id,
        )
        return self.append(event)

    def read_all(self) -> List[WALEvent]:
        """Read all events from the WAL."""
        if not self._wal_path or not self._wal_path.exists():
            return []
        events = []
        with open(self._wal_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(WALEvent.from_dict(json.loads(line)))
        return events

    def read_since(self, since: str) -> List[WALEvent]:
        """Read events since a given ISO timestamp."""
        all_events = self.read_all()
        return [e for e in all_events if e.timestamp >= since]

    def read_by_type(self, event_type: str) -> List[WALEvent]:
        """Read events of a specific type."""
        all_events = self.read_all()
        return [e for e in all_events if e.event_type == event_type]

    def verify_chain(self) -> bool:
        """
        Verify the integrity of the hash chain.
        Returns True if the chain is intact, False if any link is broken.
        """
        events = self.read_all()
        if not events:
            return True

        expected_prev = self.GENESIS_HASH
        for event in events:
            if event.prev_hash != expected_prev:
                return False
            # Recompute the event hash to verify
            recomputed = event.compute_hash()
            if event.event_hash != recomputed:
                return False
            expected_prev = event.event_hash

        return True

    @property
    def last_hash(self) -> str:
        return self._last_hash

    @property
    def event_count(self) -> int:
        return self._event_count

    def __len__(self) -> int:
        return self._event_count

    def __bool__(self) -> bool:
        """A WAL always evaluates to True — an empty WAL is still a WAL."""
        return True

    def __repr__(self) -> str:
        return f"<WAL events={self._event_count} chain={self._last_hash[:12]}...>"
