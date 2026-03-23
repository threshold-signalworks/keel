"""
Keel Telemetry — Integration Surface

Structured event definitions and emission for external consumption.
Keel emits events to a configurable target (file, stdout, or external
endpoint).

The event envelope is compatible with the provenance chain format.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional, Protocol

from keel.core.schemas import (
    KeelTelemetryEvent,
    FidelityResult,
    CompactionEvent,
    ValidationResult,
    WALEvent,
    _now,
    canonical_hash,
)


# ---------------------------------------------------------------------------
# Emitter interface
# ---------------------------------------------------------------------------

class TelemetryEmitter(Protocol):
    """Protocol for telemetry emission targets."""
    def emit(self, event: KeelTelemetryEvent) -> None: ...


# ---------------------------------------------------------------------------
# Built-in emitters
# ---------------------------------------------------------------------------

class FileEmitter:
    """Emit telemetry events to a JSONL file."""

    def __init__(self, path: Path):
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: KeelTelemetryEvent) -> None:
        with open(self._path, "a") as f:
            f.write(json.dumps(event.to_dict(), default=str) + "\n")
            f.flush()


class StdoutEmitter:
    """Emit telemetry events to stdout (development/debug)."""

    def emit(self, event: KeelTelemetryEvent) -> None:
        print(json.dumps(event.to_dict(), default=str), file=sys.stderr)


class NullEmitter:
    """Discard events. Used when telemetry is disabled."""

    def emit(self, event: KeelTelemetryEvent) -> None:
        pass


# ---------------------------------------------------------------------------
# Event factory
# ---------------------------------------------------------------------------

class KeelTelemetry:
    """
    Factory for Keel telemetry events. Creates structured events
    from Keel internal results and emits them to configured targets.
    """

    def __init__(self, session_id: str, emitter: Optional[TelemetryEmitter] = None):
        self._session_id = session_id
        self._emitter = emitter or NullEmitter()
        self._last_hash = "0" * 64

    def _emit(self, event_type: str, payload: dict) -> KeelTelemetryEvent:
        """Create and emit a telemetry event."""
        event = KeelTelemetryEvent(
            event_type=event_type,
            session_id=self._session_id,
            payload=payload,
            provenance_hash=self._last_hash,
        )
        # Chain the provenance
        self._last_hash = canonical_hash(event.to_dict())
        self._emitter.emit(event)
        return event

    # ----- Specific event emitters -----

    def fidelity_check(self, result: FidelityResult) -> KeelTelemetryEvent:
        """Emit a fidelity check result."""
        return self._emit("fidelity_check", result.to_dict())

    def compaction(self, event: CompactionEvent) -> KeelTelemetryEvent:
        """Emit a compaction event."""
        return self._emit("compaction", event.to_dict())

    def validation(self, result: ValidationResult) -> KeelTelemetryEvent:
        """Emit a validation result."""
        return self._emit("validation", result.to_dict())

    def constraint_promoted(
        self, policy_id: str, from_tier: int, to_tier: int, trigger_text: str
    ) -> KeelTelemetryEvent:
        """Emit a constraint promotion event."""
        return self._emit("constraint_promoted", {
            "policy_id": policy_id,
            "from_tier": from_tier,
            "to_tier": to_tier,
            "trigger_text": trigger_text,
        })

    def approval_requested(
        self, batch_id: str, action_count: int, risk_summary: str
    ) -> KeelTelemetryEvent:
        """Emit an approval request event."""
        return self._emit("approval_requested", {
            "batch_id": batch_id,
            "action_count": action_count,
            "risk_summary": risk_summary,
        })

    def approval_result(
        self, batch_id: str, accepted: bool, reason: str = ""
    ) -> KeelTelemetryEvent:
        """Emit an approval result event."""
        return self._emit("approval_result", {
            "batch_id": batch_id,
            "accepted": accepted,
            "reason": reason,
        })
