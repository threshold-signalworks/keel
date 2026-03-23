"""
Keel Quarantine Manager

Tracks quarantined items with their original state, enforces minimum
delay windows before permanent deletion, and provides rollback.

Design principle: every destructive action passes through quarantine first.
Hard-delete requires:
1. Item is already quarantined
2. Minimum delay has elapsed since quarantine
3. Second structured approval referencing the quarantine record

This means accidental deletions always have a recovery window.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Set

from keel.core.schemas import WALEventType, _now


# ---------------------------------------------------------------------------
# Quarantine record
# ---------------------------------------------------------------------------

@dataclass
class QuarantineRecord:
    """
    Tracks a quarantined item's original state for rollback.
    """
    item_id: str
    surface: str                        # "gmail", "gcal", etc.
    original_labels: List[str]          # labels before quarantine
    reason: str = ""                    # why it was quarantined
    quarantined_at: float = 0.0         # time.time() when quarantined
    quarantined_at_iso: str = ""        # human-readable timestamp
    released: bool = False              # True if unquarantined
    deleted: bool = False               # True if permanently deleted
    released_at: str = ""
    deleted_at: str = ""

    def __post_init__(self):
        if self.quarantined_at == 0.0:
            self.quarantined_at = time.time()
        if not self.quarantined_at_iso:
            self.quarantined_at_iso = _now()

    @property
    def age_seconds(self) -> float:
        return time.time() - self.quarantined_at

    @property
    def is_active(self) -> bool:
        """Still quarantined (not released or deleted)."""
        return not self.released and not self.deleted

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Quarantine Manager
# ---------------------------------------------------------------------------

# Default minimum delay: 5 minutes (300 seconds)
# In production this would be hours or days; short for testing
DEFAULT_MIN_DELAY = 300


class QuarantineManager:
    """
    Manages quarantine state for all surfaces.
    
    Provides:
    - Record creation when items are quarantined
    - Delay window enforcement before permanent deletion
    - Rollback: restore original labels/state
    - Audit trail via WAL
    """

    def __init__(self, min_delay: float = DEFAULT_MIN_DELAY, wal=None):
        self._records: Dict[str, QuarantineRecord] = {}
        self._min_delay = min_delay
        self._wal = wal

    # -----------------------------------------------------------------
    # Quarantine
    # -----------------------------------------------------------------

    def quarantine(
        self,
        item_id: str,
        surface: str,
        original_labels: List[str],
        reason: str = "",
    ) -> QuarantineRecord:
        """
        Record an item as quarantined, preserving its original state.
        
        If already quarantined, returns the existing record (idempotent).
        """
        if item_id in self._records and self._records[item_id].is_active:
            return self._records[item_id]

        record = QuarantineRecord(
            item_id=item_id,
            surface=surface,
            original_labels=list(original_labels),
            reason=reason,
        )
        self._records[item_id] = record

        if self._wal:
            self._wal.log("QUARANTINED", {
                "item_id": item_id,
                "surface": surface,
                "original_labels": list(original_labels),
                "reason": reason,
            })

        return record

    # -----------------------------------------------------------------
    # Release (rollback)
    # -----------------------------------------------------------------

    def release(self, item_id: str) -> Optional[QuarantineRecord]:
        """
        Mark an item as released from quarantine.
        
        Returns the record with original_labels for the caller
        to restore. Returns None if not found or already released.
        """
        record = self._records.get(item_id)
        if record is None or not record.is_active:
            return None

        record.released = True
        record.released_at = _now()

        if self._wal:
            self._wal.log("QUARANTINE_RELEASED", {
                "item_id": item_id,
                "original_labels": record.original_labels,
            })

        return record

    # -----------------------------------------------------------------
    # Permanent deletion
    # -----------------------------------------------------------------

    def can_delete(self, item_id: str) -> tuple[bool, str]:
        """
        Check whether an item can be permanently deleted.
        
        Returns (allowed, reason).
        Requires:
        1. Item is quarantined and active
        2. Minimum delay has elapsed
        """
        record = self._records.get(item_id)
        if record is None:
            return False, f"Item '{item_id}' has no quarantine record. Quarantine before deleting."

        if not record.is_active:
            if record.released:
                return False, f"Item '{item_id}' was released from quarantine."
            if record.deleted:
                return False, f"Item '{item_id}' is already deleted."

        elapsed = record.age_seconds
        if elapsed < self._min_delay:
            remaining = self._min_delay - elapsed
            return False, (
                f"Minimum delay not met. "
                f"Quarantined {elapsed:.0f}s ago, need {self._min_delay:.0f}s. "
                f"{remaining:.0f}s remaining."
            )

        return True, "Delay window satisfied. Second approval required."

    def mark_deleted(self, item_id: str) -> Optional[QuarantineRecord]:
        """
        Mark an item as permanently deleted after approval.
        
        Caller must verify can_delete() first and obtain second approval.
        """
        record = self._records.get(item_id)
        if record is None or not record.is_active:
            return None

        record.deleted = True
        record.deleted_at = _now()

        if self._wal:
            self._wal.log("QUARANTINE_DELETED", {
                "item_id": item_id,
                "surface": record.surface,
                "age_seconds": record.age_seconds,
            })

        return record

    # -----------------------------------------------------------------
    # Query
    # -----------------------------------------------------------------

    def get_record(self, item_id: str) -> Optional[QuarantineRecord]:
        return self._records.get(item_id)

    def list_active(self) -> List[QuarantineRecord]:
        """All items currently in quarantine."""
        return [r for r in self._records.values() if r.is_active]

    def list_eligible_for_deletion(self) -> List[QuarantineRecord]:
        """Items that have passed the minimum delay and can be deleted."""
        return [
            r for r in self._records.values()
            if r.is_active and r.age_seconds >= self._min_delay
        ]

    def list_all(self) -> List[QuarantineRecord]:
        return list(self._records.values())

    @property
    def active_count(self) -> int:
        return sum(1 for r in self._records.values() if r.is_active)

    @property
    def min_delay(self) -> float:
        return self._min_delay

    # -----------------------------------------------------------------
    # Bulk operations
    # -----------------------------------------------------------------

    def release_all(self) -> List[QuarantineRecord]:
        """Release all active quarantine records. Returns released records."""
        released = []
        for record in self._records.values():
            if record.is_active:
                record.released = True
                record.released_at = _now()
                released.append(record)
        if self._wal and released:
            self._wal.log("QUARANTINE_BULK_RELEASE", {
                "count": len(released),
                "item_ids": [r.item_id for r in released],
            })
        return released
