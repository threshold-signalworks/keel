"""
Keel Approval Parser

Parses user approval input against a pending Receipt.
Strict syntax only: "APPROVE batch {batch_id} actions {action_ids}"

Anything else — "yes", "go ahead", "do it", "sure", "approved",
"APPROVE batch B123" (missing action IDs) — is rejected.

This is deliberately friction-ful. The friction is the safety feature.
Accidental authorisation is the failure mode this prevents.
"""

from __future__ import annotations

import re
from typing import Optional

from keel.core.schemas import (
    ApprovalResult,
    Receipt,
    WALEventType,
)


# ---------------------------------------------------------------------------
# Ambiguous patterns to reject
# ---------------------------------------------------------------------------

_AMBIGUOUS_PATTERNS = [
    r"^yes$",
    r"^y$",
    r"^ok$",
    r"^okay$",
    r"^sure$",
    r"^go ahead$",
    r"^do it$",
    r"^proceed$",
    r"^confirmed?$",
    r"^approved?$",
    r"^accept(ed)?$",
    r"^lgtm$",
    r"^looks good$",
    r"^fine$",
    r"^yep$",
    r"^yeah$",
    r"^absolutely$",
    r"^affirmative$",
]

_AMBIGUOUS_RE = re.compile(
    "|".join(_AMBIGUOUS_PATTERNS),
    re.IGNORECASE,
)

# The exact approval format
# APPROVE batch {BATCH_ID} actions {ID1},{ID2},...
_APPROVAL_RE = re.compile(
    r"^APPROVE\s+batch\s+(\S+)\s+actions\s+(\S+)$",
    re.IGNORECASE,
)


class ApprovalParser:
    """
    Parses structured approval input against a pending receipt.
    
    Strict syntax only. Every deviation is rejected with an explanation
    of the required format. This creates unambiguous audit entries and
    prevents accidental authorisation.
    """

    def __init__(self, wal=None):
        self._wal = wal

    def parse(self, user_input: str, pending_receipt: Receipt) -> ApprovalResult:
        """
        Parse user input against the pending receipt.
        
        Returns ApprovalResult with accepted=True only if:
        1. Input matches the exact APPROVE syntax
        2. Batch ID matches the pending receipt
        3. All referenced action IDs are in the receipt
        """
        raw = user_input.strip()

        # Check for empty input
        if not raw:
            return self._reject(raw, "Empty input. " + self._format_hint(pending_receipt))

        # Check for ambiguous input FIRST
        if _AMBIGUOUS_RE.match(raw):
            return self._reject(
                raw,
                f"Ambiguous approval rejected. Keel requires explicit structured approval.\n"
                f"{self._format_hint(pending_receipt)}"
            )

        # Try to parse the approval syntax
        match = _APPROVAL_RE.match(raw)
        if not match:
            return self._reject(
                raw,
                f"Unrecognised approval format.\n{self._format_hint(pending_receipt)}"
            )

        # Extract batch ID and action IDs
        input_batch_id = match.group(1)
        input_action_ids = [aid.strip() for aid in match.group(2).split(",") if aid.strip()]

        # Verify batch ID
        if input_batch_id != pending_receipt.batch_id:
            return self._reject(
                raw,
                f"Batch ID mismatch. Expected '{pending_receipt.batch_id}', "
                f"got '{input_batch_id}'."
            )

        # Verify action IDs exist in the receipt
        receipt_short_ids = {a.action_id[:8] for a in pending_receipt.actions}
        receipt_full_ids = {a.action_id for a in pending_receipt.actions}

        unrecognised = []
        matched_ids = []
        for aid in input_action_ids:
            if aid in receipt_short_ids or aid in receipt_full_ids:
                matched_ids.append(aid)
            else:
                unrecognised.append(aid)

        if unrecognised:
            return self._reject(
                raw,
                f"Unrecognised action ID(s): {', '.join(unrecognised)}. "
                f"Valid IDs: {', '.join(sorted(receipt_short_ids))}"
            )

        if not matched_ids:
            return self._reject(raw, "No action IDs provided.")

        # Success
        result = ApprovalResult(
            accepted=True,
            batch_id=input_batch_id,
            approved_action_ids=matched_ids,
            raw_input=raw,
        )

        self._log(pending_receipt.batch_id, True, "")
        return result

    def _reject(self, raw: str, reason: str) -> ApprovalResult:
        """Build a rejection result."""
        result = ApprovalResult(
            accepted=False,
            rejection_reason=reason,
            raw_input=raw,
        )
        # Only log if we have a WAL — batch_id might not be known for malformed input
        return result

    def _format_hint(self, receipt: Receipt) -> str:
        """Format the expected approval command as a hint."""
        action_ids_short = [a.action_id[:8] for a in receipt.actions]
        return (
            f"Required format:\n"
            f"  APPROVE batch {receipt.batch_id} actions {','.join(action_ids_short)}"
        )

    def _log(self, batch_id: str, accepted: bool, reason: str):
        """Log approval result to WAL."""
        if self._wal:
            event_type = WALEventType.APPROVED.value if accepted else WALEventType.APPROVAL_REJECTED.value
            self._wal.log(event_type, {
                "batch_id": batch_id,
                "accepted": accepted,
                "reason": reason,
            })
