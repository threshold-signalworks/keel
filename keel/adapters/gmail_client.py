"""
Gmail Client — Protocol and Implementations

Defines the interface for Gmail operations and provides:
- MockGmailClient: in-memory simulation for testing and demos
- LiveGmailClient: real Google API wrapper (requires credentials)

The mock is full-featured enough to demonstrate every safety property
without touching a real inbox.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, Set


# ---------------------------------------------------------------------------
# Message representation
# ---------------------------------------------------------------------------

@dataclass
class GmailMessage:
    """Representation of a Gmail message."""
    id: str
    thread_id: str = ""
    subject: str = ""
    sender: str = ""
    snippet: str = ""
    labels: Set[str] = field(default_factory=lambda: {"INBOX"})
    is_read: bool = False
    timestamp: str = ""
    headers: Dict[str, str] = field(default_factory=dict)

    @property
    def is_archived(self) -> bool:
        return "INBOX" not in self.labels

    @property
    def is_trashed(self) -> bool:
        return "TRASH" in self.labels

    @property
    def is_quarantined(self) -> bool:
        return "KEEL_QUARANTINE" in self.labels

    def to_preview(self) -> str:
        """Human-readable preview for receipts."""
        status = []
        if self.is_trashed:
            status.append("TRASH")
        if self.is_quarantined:
            status.append("QUARANTINE")
        if self.is_archived:
            status.append("ARCHIVED")
        labels_str = ", ".join(sorted(self.labels - {"INBOX", "TRASH", "KEEL_QUARANTINE"}))
        parts = [
            f"From: {self.sender}" if self.sender else "",
            f"Subject: {self.subject}" if self.subject else "",
            f"Labels: [{labels_str}]" if labels_str else "",
            f"Status: {', '.join(status)}" if status else "",
        ]
        return " | ".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Client protocol
# ---------------------------------------------------------------------------

class GmailClientProtocol(Protocol):
    """Interface for Gmail operations. Implemented by mock and live clients."""

    def get_message(self, message_id: str) -> Optional[GmailMessage]: ...
    def get_messages(self, message_ids: List[str]) -> Dict[str, Optional[GmailMessage]]: ...
    def add_label(self, message_id: str, label: str) -> bool: ...
    def remove_label(self, message_id: str, label: str) -> bool: ...
    def archive(self, message_id: str) -> bool: ...
    def unarchive(self, message_id: str) -> bool: ...
    def move_to_trash(self, message_id: str) -> bool: ...
    def untrash(self, message_id: str) -> bool: ...
    def get_labels(self, message_id: str) -> Optional[Set[str]]: ...


# ---------------------------------------------------------------------------
# Mock client
# ---------------------------------------------------------------------------

class MockGmailClient:
    """
    In-memory Gmail simulation.
    
    Tracks all operations for verification. Supports idempotency testing
    (adding a label that already exists returns success but logs as no-op).
    """

    def __init__(self, messages: Optional[List[GmailMessage]] = None):
        self._messages: Dict[str, GmailMessage] = {}
        self._operation_log: List[Dict[str, Any]] = []
        if messages:
            for msg in messages:
                self._messages[msg.id] = msg

    def seed_inbox(self, messages: List[GmailMessage]):
        """Add messages to the mock inbox."""
        for msg in messages:
            self._messages[msg.id] = msg

    def get_message(self, message_id: str) -> Optional[GmailMessage]:
        return self._messages.get(message_id)

    def get_messages(self, message_ids: List[str]) -> Dict[str, Optional[GmailMessage]]:
        return {mid: self._messages.get(mid) for mid in message_ids}

    def add_label(self, message_id: str, label: str) -> bool:
        msg = self._messages.get(message_id)
        if msg is None:
            self._log("add_label", message_id, False, f"Message not found: {message_id}")
            return False
        was_present = label in msg.labels
        msg.labels.add(label)
        self._log("add_label", message_id, True,
                  f"Label '{label}' {'already present (no-op)' if was_present else 'added'}",
                  idempotent_noop=was_present)
        return True

    def remove_label(self, message_id: str, label: str) -> bool:
        msg = self._messages.get(message_id)
        if msg is None:
            self._log("remove_label", message_id, False, f"Message not found: {message_id}")
            return False
        was_present = label in msg.labels
        msg.labels.discard(label)
        self._log("remove_label", message_id, True,
                  f"Label '{label}' {'removed' if was_present else 'not present (no-op)'}",
                  idempotent_noop=not was_present)
        return True

    def archive(self, message_id: str) -> bool:
        msg = self._messages.get(message_id)
        if msg is None:
            self._log("archive", message_id, False, f"Message not found: {message_id}")
            return False
        was_archived = msg.is_archived
        msg.labels.discard("INBOX")
        self._log("archive", message_id, True,
                  "Already archived (no-op)" if was_archived else "Archived",
                  idempotent_noop=was_archived)
        return True

    def unarchive(self, message_id: str) -> bool:
        msg = self._messages.get(message_id)
        if msg is None:
            self._log("unarchive", message_id, False, f"Message not found: {message_id}")
            return False
        msg.labels.add("INBOX")
        self._log("unarchive", message_id, True, "Unarchived")
        return True

    def move_to_trash(self, message_id: str) -> bool:
        msg = self._messages.get(message_id)
        if msg is None:
            self._log("move_to_trash", message_id, False, f"Message not found: {message_id}")
            return False
        was_trashed = msg.is_trashed
        msg.labels.add("TRASH")
        msg.labels.discard("INBOX")
        self._log("move_to_trash", message_id, True,
                  "Already in trash (no-op)" if was_trashed else "Moved to trash",
                  idempotent_noop=was_trashed)
        return True

    def untrash(self, message_id: str) -> bool:
        msg = self._messages.get(message_id)
        if msg is None:
            self._log("untrash", message_id, False, f"Message not found: {message_id}")
            return False
        msg.labels.discard("TRASH")
        msg.labels.add("INBOX")
        self._log("untrash", message_id, True, "Restored from trash")
        return True

    def get_labels(self, message_id: str) -> Optional[Set[str]]:
        msg = self._messages.get(message_id)
        return set(msg.labels) if msg else None

    # ---- Mock introspection ----

    @property
    def operation_log(self) -> List[Dict[str, Any]]:
        return list(self._operation_log)

    @property
    def noop_count(self) -> int:
        return sum(1 for op in self._operation_log if op.get("idempotent_noop"))

    def _log(self, operation: str, message_id: str, success: bool,
             detail: str, idempotent_noop: bool = False):
        self._operation_log.append({
            "operation": operation,
            "message_id": message_id,
            "success": success,
            "detail": detail,
            "idempotent_noop": idempotent_noop,
            "timestamp": time.time(),
        })


# ---------------------------------------------------------------------------
# Live client (stub — requires google-api-python-client)
# ---------------------------------------------------------------------------

class LiveGmailClient:
    """
    Real Gmail API client.
    
    Requires google-api-python-client and valid OAuth2 credentials.
    This is a Phase 3b deliverable — the mock is sufficient for Phase 3a.
    """

    def __init__(self, credentials_path: Optional[str] = None):
        self._service = None
        # TODO: OAuth2 flow, build service
        raise NotImplementedError(
            "LiveGmailClient requires google-api-python-client. "
            "Use MockGmailClient for testing and demos."
        )

    def get_message(self, message_id: str) -> Optional[GmailMessage]:
        raise NotImplementedError

    def get_messages(self, message_ids: List[str]) -> Dict[str, Optional[GmailMessage]]:
        raise NotImplementedError

    def add_label(self, message_id: str, label: str) -> bool:
        raise NotImplementedError

    def remove_label(self, message_id: str, label: str) -> bool:
        raise NotImplementedError

    def archive(self, message_id: str) -> bool:
        raise NotImplementedError

    def unarchive(self, message_id: str) -> bool:
        raise NotImplementedError

    def move_to_trash(self, message_id: str) -> bool:
        raise NotImplementedError

    def untrash(self, message_id: str) -> bool:
        raise NotImplementedError

    def get_labels(self, message_id: str) -> Optional[Set[str]]:
        raise NotImplementedError
