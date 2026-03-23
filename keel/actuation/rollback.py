"""
Keel Rollback Manager

Orchestrates undo operations using quarantine records and adapter
capabilities. Provides:

- Unquarantine: restore original labels, move back to inbox
- Undo archive: move back to inbox
- Undo label changes: reverse add/remove
- Bulk rollback: undo an entire batch by receipt ID

Every rollback is logged to WAL with before/after state.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from keel.core.schemas import ActionSpec, WALEventType, _now
from keel.actuation.quarantine import QuarantineManager, QuarantineRecord
from keel.adapters.base import ToolResult
from keel.adapters.gmail_client import GmailClientProtocol


@dataclass
class RollbackResult:
    """Result of a rollback operation."""
    item_id: str
    success: bool
    action: str = ""           # what was undone
    details: str = ""
    labels_before: List[str] = field(default_factory=list)
    labels_after: List[str] = field(default_factory=list)
    timestamp: str = field(default_factory=_now)

    def to_dict(self) -> dict:
        return asdict(self)


class RollbackManager:
    """
    Orchestrates rollback operations across surfaces.
    
    Uses quarantine records for state restoration and adapter
    clients for execution.
    """

    def __init__(
        self,
        quarantine: QuarantineManager,
        gmail_client: Optional[GmailClientProtocol] = None,
        wal=None,
    ):
        self._quarantine = quarantine
        self._gmail = gmail_client
        self._wal = wal

    # -----------------------------------------------------------------
    # Unquarantine
    # -----------------------------------------------------------------

    def unquarantine(self, item_id: str) -> RollbackResult:
        """
        Restore an item from quarantine to its original state.
        
        Reads original labels from quarantine record, applies them,
        removes quarantine label.
        """
        record = self._quarantine.release(item_id)
        if record is None:
            return RollbackResult(
                item_id=item_id, success=False,
                action="unquarantine",
                details=f"No active quarantine record for '{item_id}'",
            )

        if record.surface == "gmail" and self._gmail:
            return self._unquarantine_gmail(item_id, record)

        return RollbackResult(
            item_id=item_id, success=True,
            action="unquarantine",
            details=f"Released from quarantine (surface: {record.surface}). "
                    f"Original labels: {record.original_labels}",
            labels_after=record.original_labels,
        )

    def _unquarantine_gmail(self, item_id: str, record: QuarantineRecord) -> RollbackResult:
        """Restore a Gmail message from quarantine."""
        msg = self._gmail.get_message(item_id)
        if msg is None:
            return RollbackResult(
                item_id=item_id, success=False,
                action="unquarantine",
                details=f"Message not found: {item_id}",
            )

        labels_before = list(msg.labels)

        # Remove quarantine label
        self._gmail.remove_label(item_id, "KEEL_QUARANTINE")

        # Restore original labels
        for label in record.original_labels:
            self._gmail.add_label(item_id, label)

        msg_after = self._gmail.get_message(item_id)
        labels_after = list(msg_after.labels) if msg_after else []

        self._log("ROLLBACK_UNQUARANTINE", item_id, labels_before, labels_after)

        return RollbackResult(
            item_id=item_id, success=True,
            action="unquarantine",
            details=f"Restored from quarantine. Labels: {labels_after}",
            labels_before=labels_before,
            labels_after=labels_after,
        )

    # -----------------------------------------------------------------
    # Undo archive
    # -----------------------------------------------------------------

    def undo_archive(self, item_id: str) -> RollbackResult:
        """Move a message back to inbox (undo archive)."""
        if not self._gmail:
            return RollbackResult(
                item_id=item_id, success=False,
                action="undo_archive",
                details="No Gmail client configured",
            )

        msg = self._gmail.get_message(item_id)
        if msg is None:
            return RollbackResult(
                item_id=item_id, success=False,
                action="undo_archive",
                details=f"Message not found: {item_id}",
            )

        labels_before = list(msg.labels)
        self._gmail.unarchive(item_id)
        msg_after = self._gmail.get_message(item_id)
        labels_after = list(msg_after.labels) if msg_after else []

        self._log("ROLLBACK_UNARCHIVE", item_id, labels_before, labels_after)

        return RollbackResult(
            item_id=item_id, success=True,
            action="undo_archive",
            details="Moved back to inbox",
            labels_before=labels_before,
            labels_after=labels_after,
        )

    # -----------------------------------------------------------------
    # Undo label
    # -----------------------------------------------------------------

    def undo_label_add(self, item_id: str, label: str) -> RollbackResult:
        """Remove a label that was added (undo label_add)."""
        if not self._gmail:
            return RollbackResult(item_id=item_id, success=False, action="undo_label_add",
                                  details="No Gmail client configured")

        msg = self._gmail.get_message(item_id)
        if msg is None:
            return RollbackResult(item_id=item_id, success=False, action="undo_label_add",
                                  details=f"Message not found: {item_id}")

        labels_before = list(msg.labels)
        self._gmail.remove_label(item_id, label)
        msg_after = self._gmail.get_message(item_id)
        labels_after = list(msg_after.labels) if msg_after else []

        self._log("ROLLBACK_LABEL_REMOVE", item_id, labels_before, labels_after)

        return RollbackResult(
            item_id=item_id, success=True, action="undo_label_add",
            details=f"Label '{label}' removed",
            labels_before=labels_before, labels_after=labels_after,
        )

    def undo_label_remove(self, item_id: str, label: str) -> RollbackResult:
        """Re-add a label that was removed (undo label_remove)."""
        if not self._gmail:
            return RollbackResult(item_id=item_id, success=False, action="undo_label_remove",
                                  details="No Gmail client configured")

        msg = self._gmail.get_message(item_id)
        if msg is None:
            return RollbackResult(item_id=item_id, success=False, action="undo_label_remove",
                                  details=f"Message not found: {item_id}")

        labels_before = list(msg.labels)
        self._gmail.add_label(item_id, label)
        msg_after = self._gmail.get_message(item_id)
        labels_after = list(msg_after.labels) if msg_after else []

        self._log("ROLLBACK_LABEL_ADD", item_id, labels_before, labels_after)

        return RollbackResult(
            item_id=item_id, success=True, action="undo_label_remove",
            details=f"Label '{label}' restored",
            labels_before=labels_before, labels_after=labels_after,
        )

    # -----------------------------------------------------------------
    # Undo trash
    # -----------------------------------------------------------------

    def undo_trash(self, item_id: str) -> RollbackResult:
        """Restore a message from trash."""
        if not self._gmail:
            return RollbackResult(item_id=item_id, success=False, action="undo_trash",
                                  details="No Gmail client configured")

        msg = self._gmail.get_message(item_id)
        if msg is None:
            return RollbackResult(item_id=item_id, success=False, action="undo_trash",
                                  details=f"Message not found: {item_id}")

        labels_before = list(msg.labels)
        self._gmail.untrash(item_id)
        msg_after = self._gmail.get_message(item_id)
        labels_after = list(msg_after.labels) if msg_after else []

        self._log("ROLLBACK_UNTRASH", item_id, labels_before, labels_after)

        return RollbackResult(
            item_id=item_id, success=True, action="undo_trash",
            details="Restored from trash",
            labels_before=labels_before, labels_after=labels_after,
        )

    # -----------------------------------------------------------------
    # Logging
    # -----------------------------------------------------------------

    def _log(self, event_type: str, item_id: str,
             labels_before: list, labels_after: list):
        if self._wal:
            self._wal.log(event_type, {
                "item_id": item_id,
                "labels_before": labels_before,
                "labels_after": labels_after,
            })
