"""
Keel Gmail Adapter

First real-world tool surface. Reversible actions only in this phase:
label_add, label_remove, archive, quarantine, move_to_trash.

No hard-delete. That requires Phase 4 (quarantine + reversibility windows).

Safety properties:
- Object existence check before every action
- Idempotency: label already present → no-op, logged
- Per-hour rate limiting (configurable cap)
- Preview with message subjects/snippets in receipts
- Deadman: any error → PAUSE, not retry
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from keel.core.schemas import ActionSpec, Reversibility, _now
from keel.adapters.base import BaseAdapter, ToolResult
from keel.adapters.gmail_client import GmailClientProtocol, GmailMessage


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

@dataclass
class RateLimit:
    """Simple sliding-window rate limiter."""
    max_per_hour: int = 100
    _timestamps: List[float] = field(default_factory=list)

    def check(self) -> bool:
        """Return True if under the rate limit."""
        now = time.time()
        cutoff = now - 3600
        self._timestamps = [t for t in self._timestamps if t > cutoff]
        return len(self._timestamps) < self.max_per_hour

    def record(self):
        """Record an action."""
        self._timestamps.append(time.time())

    @property
    def remaining(self) -> int:
        now = time.time()
        cutoff = now - 3600
        recent = sum(1 for t in self._timestamps if t > cutoff)
        return max(0, self.max_per_hour - recent)

    @property
    def used(self) -> int:
        now = time.time()
        cutoff = now - 3600
        return sum(1 for t in self._timestamps if t > cutoff)


# ---------------------------------------------------------------------------
# Gmail adapter
# ---------------------------------------------------------------------------

QUARANTINE_LABEL = "KEEL_QUARANTINE"

# Actions this adapter supports and their reversibility
_GMAIL_ACTIONS = {
    "label_add": Reversibility.REVERSIBLE.value,
    "label_remove": Reversibility.REVERSIBLE.value,
    "archive": Reversibility.REVERSIBLE.value,
    "quarantine": Reversibility.REVERSIBLE.value,
    "move_to_trash": Reversibility.REVERSIBLE_WITHIN_WINDOW.value,
    # Deliberately absent: delete_hard, send
}


class GmailAdapter(BaseAdapter):
    """
    Gmail adapter for Keel.
    
    Executes reversible Gmail operations with safety checks:
    - Existence verification before action
    - Idempotency (no-op if already in target state)
    - Per-hour rate limiting
    - Preview generation for receipts
    - Hard stop on any error (deadman)
    """

    def __init__(
        self,
        client: GmailClientProtocol,
        rate_limit: Optional[RateLimit] = None,
    ):
        self._client = client
        self._rate_limit = rate_limit or RateLimit()
        self._paused = False
        self._pause_reason = ""

    @property
    def surface_name(self) -> str:
        return "gmail"

    @property
    def supported_actions(self) -> Dict[str, str]:
        return dict(_GMAIL_ACTIONS)

    @property
    def is_paused(self) -> bool:
        return self._paused

    @property
    def pause_reason(self) -> str:
        return self._pause_reason

    def resume(self):
        """Resume after a deadman pause."""
        self._paused = False
        self._pause_reason = ""

    # -----------------------------------------------------------------
    # Execute
    # -----------------------------------------------------------------

    def execute(self, action: ActionSpec) -> ToolResult:
        """Execute a Gmail action with all safety checks."""

        # Deadman check
        if self._paused:
            return ToolResult(
                action_id=action.action_id,
                status="blocked",
                details=f"Adapter paused: {self._pause_reason}. Call resume() to continue.",
            )

        # Action type check
        if action.action_type not in _GMAIL_ACTIONS:
            if action.action_type in ("delete_hard", "send"):
                return ToolResult(
                    action_id=action.action_id,
                    status="blocked",
                    details=f"Action '{action.action_type}' is not supported by the Gmail adapter "
                            f"in this phase. Hard deletes require Phase 4 quarantine workflow.",
                )
            return ToolResult(
                action_id=action.action_id,
                status="error",
                details=f"Unsupported action type: {action.action_type}",
            )

        # Rate limit check
        if not self._rate_limit.check():
            self._pause("Rate limit exceeded")
            return ToolResult(
                action_id=action.action_id,
                status="blocked",
                details=f"Rate limit exceeded ({self._rate_limit.max_per_hour}/hour). "
                        f"Adapter paused.",
            )

        # Execute per target
        results = []
        for target_id in action.target_ids:
            result = self._execute_single(action, target_id)
            results.append(result)
            if result["status"] == "error":
                # Deadman: stop on first error
                self._pause(f"Error on {target_id}: {result['detail']}")
                return ToolResult(
                    action_id=action.action_id,
                    status="error",
                    details=f"Error on target {target_id}: {result['detail']}. "
                            f"Adapter paused. {len(results)}/{len(action.target_ids)} targets processed.",
                    tool_receipt={
                        "surface": "gmail",
                        "action_type": action.action_type,
                        "results": results,
                        "paused": True,
                    },
                )

        # All targets processed
        noops = sum(1 for r in results if r.get("noop"))
        actual = len(results) - noops

        return ToolResult(
            action_id=action.action_id,
            status="success",
            details=f"{action.action_type} on {len(results)} target(s) "
                    f"({actual} modified, {noops} no-op)",
            tool_receipt={
                "surface": "gmail",
                "action_type": action.action_type,
                "targets_processed": len(results),
                "targets_modified": actual,
                "targets_noop": noops,
                "results": results,
            },
        )

    # -----------------------------------------------------------------
    # Per-target execution
    # -----------------------------------------------------------------

    def _execute_single(self, action: ActionSpec, target_id: str) -> Dict[str, Any]:
        """Execute action on a single target with existence check."""

        # Existence check
        msg = self._client.get_message(target_id)
        if msg is None:
            return {
                "target_id": target_id,
                "status": "error",
                "detail": f"Message not found: {target_id}",
                "noop": False,
            }

        # Dispatch
        handler = {
            "label_add": self._do_label_add,
            "label_remove": self._do_label_remove,
            "archive": self._do_archive,
            "quarantine": self._do_quarantine,
            "move_to_trash": self._do_move_to_trash,
        }.get(action.action_type)

        if handler is None:
            return {
                "target_id": target_id,
                "status": "error",
                "detail": f"No handler for {action.action_type}",
                "noop": False,
            }

        result = handler(msg, action)
        self._rate_limit.record()
        return result

    def _do_label_add(self, msg: GmailMessage, action: ActionSpec) -> Dict[str, Any]:
        label = action.params.get("label", "")
        if not label:
            return {"target_id": msg.id, "status": "error", "detail": "No label specified", "noop": False}

        was_present = label in msg.labels
        success = self._client.add_label(msg.id, label)
        return {
            "target_id": msg.id,
            "status": "success" if success else "error",
            "detail": f"Label '{label}' {'already present' if was_present else 'added'}",
            "noop": was_present,
            "preview": msg.to_preview(),
        }

    def _do_label_remove(self, msg: GmailMessage, action: ActionSpec) -> Dict[str, Any]:
        label = action.params.get("label", "")
        if not label:
            return {"target_id": msg.id, "status": "error", "detail": "No label specified", "noop": False}

        was_present = label in msg.labels
        success = self._client.remove_label(msg.id, label)
        return {
            "target_id": msg.id,
            "status": "success" if success else "error",
            "detail": f"Label '{label}' {'removed' if was_present else 'not present'}",
            "noop": not was_present,
            "preview": msg.to_preview(),
        }

    def _do_archive(self, msg: GmailMessage, action: ActionSpec) -> Dict[str, Any]:
        was_archived = msg.is_archived
        success = self._client.archive(msg.id)
        return {
            "target_id": msg.id,
            "status": "success" if success else "error",
            "detail": "Already archived" if was_archived else "Archived",
            "noop": was_archived,
            "preview": msg.to_preview(),
        }

    def _do_quarantine(self, msg: GmailMessage, action: ActionSpec) -> Dict[str, Any]:
        was_quarantined = msg.is_quarantined
        self._client.add_label(msg.id, QUARANTINE_LABEL)
        self._client.remove_label(msg.id, "INBOX")
        return {
            "target_id": msg.id,
            "status": "success",
            "detail": "Already quarantined" if was_quarantined else "Quarantined",
            "noop": was_quarantined,
            "preview": msg.to_preview(),
        }

    def _do_move_to_trash(self, msg: GmailMessage, action: ActionSpec) -> Dict[str, Any]:
        was_trashed = msg.is_trashed
        success = self._client.move_to_trash(msg.id)
        return {
            "target_id": msg.id,
            "status": "success" if success else "error",
            "detail": "Already in trash" if was_trashed else "Moved to trash",
            "noop": was_trashed,
            "preview": msg.to_preview(),
        }

    # -----------------------------------------------------------------
    # Preview
    # -----------------------------------------------------------------

    def preview(self, action: ActionSpec) -> str:
        """Generate preview with message subjects/snippets."""
        lines = [f"[gmail] {action.action_type}:"]
        messages = self._client.get_messages(action.target_ids)
        for tid in action.target_ids:
            msg = messages.get(tid)
            if msg:
                lines.append(f"  {tid}: {msg.to_preview()}")
            else:
                lines.append(f"  {tid}: (message not found)")
        return "\n".join(lines)

    # -----------------------------------------------------------------
    # Deadman
    # -----------------------------------------------------------------

    def _pause(self, reason: str):
        """Pause the adapter. Requires explicit resume() to continue."""
        self._paused = True
        self._pause_reason = reason
